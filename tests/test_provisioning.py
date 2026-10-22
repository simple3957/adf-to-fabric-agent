"""Fabric target provisioning: planning, approval, apply, retries, partial failure, verification, permissions.

A stateful in-memory fake stands in for Fabric REST and Azure Resource Manager. Tests assert what was SENT (the fake records every
call), so "planning never writes", "no duplicates" and "nothing is deleted" are checked on the wire, not assumed.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from adf2fabric.agent.conversion.compute_plan import build_compute_plan
from adf2fabric.agent.provisioning import requirements as rq
from adf2fabric.agent.provisioning import service
from adf2fabric.agent.provisioning.executor import ApplyRefused, apply_plan
from adf2fabric.agent.provisioning.planner import make_plan
from adf2fabric.agent.provisioning.transport import ArmGateway, FabricGateway, ProvisionError, token_tenant
from adf2fabric.agent.provisioning.verifier import verify_plan
from adf2fabric.project.provisioning import Action, ProvisioningPlan, ProvisioningRun, ResourceType as T, StepStatus, TargetSpec
from tests.test_discovery_activity_inventory import _discover, _project
from tests.test_workloads import ESTATE, LAKEHOUSE, LOCAL, SECRETS, ARTIFACTS  # a Databricks/Synapse estate with Spark compute and workloads

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
TENANT = "1680abbe-1a01-4cb0-99e9-452fe05bf615"
WS_ID = "53d67e72-8124-4ca3-ae55-c4b266935faa"
CAP_ID = "aaaaaaaa-0000-0000-0000-00000000000a"
OTHER_CAP = "bbbbbbbb-0000-0000-0000-00000000000b"
SECRET_TOKEN = "TOKEN-SECRET-VALUE-123"


# =====================================================================================================================
# the fake service
# =====================================================================================================================


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._b, self.headers = status, body, headers or {}
        self.content = b"x" if body is not None else b""

    def json(self):
        return self._b


class FakeService:
    """Fabric REST + ARM in one session; ``host`` of the URL says which. Failures are injected per (method, path regex)."""

    def __init__(self):
        self.calls: list[tuple[str, str]] = []
        self.workspaces: dict[str, dict] = {}
        self.items: dict[str, list[dict]] = {}  # workspace -> items
        self.pools: dict[str, list[dict]] = {}
        self.roles: dict[str, list[dict]] = {}
        self.connections: list[dict] = []
        self.capacities = [{"id": CAP_ID, "displayName": "capone", "sku": "F64", "region": "West Europe", "state": "Active"},
                           {"id": OTHER_CAP, "displayName": "captwo", "sku": "F2", "region": "West Europe", "state": "Active"}]
        self.arm_caps: dict[str, dict] = {}
        self.caller_roles = {"Admin"}
        self.provider_state = "Registered"
        self.skus = {"F64": ["westeurope"], "F4": ["westeurope"], "F2": ["westeurope", "northeurope"]}
        self.quota = (0, 128)
        self.name_available = True
        self.fail: list[dict] = []  # {"method","pattern","status","times","effect":bool,"body"}
        self.hooks: list = []  # callables(method, path) run before handling (to change the world mid-flight)
        self.ops: dict[str, str] = {}

    # -- failure injection ------------------------------------------------------------------------------------------------
    def inject(self, method, pattern, status, times=1, effect=False, body=None, headers=None):
        self.fail.append({"method": method, "pattern": pattern, "status": status, "times": times, "effect": effect, "body": body, "headers": headers or {}})

    def _injected(self, method, path):
        for f in self.fail:
            if f["method"] == method and re.search(f["pattern"], path) and f["times"] > 0:
                f["times"] -= 1
                return f
        return None

    # -- the session interface ------------------------------------------------------------------------------------------------
    def request(self, method, url, headers=None, json=None, timeout=None):
        assert headers and headers["Authorization"].startswith("Bearer "), "every call is authenticated"
        host_fab, host_arm = "https://api.fabric.microsoft.com/v1/", "https://management.azure.com/"
        if url.startswith(host_fab):
            path = url[len(host_fab):]
            kind = "fab"
        elif url.startswith(host_arm):
            path = url[len(host_arm):]
            kind = "arm"
        else:
            raise AssertionError(f"a call went to an unexpected host: {url}")
        clean = path.split("?")[0]
        self.calls.append((method, clean if kind == "fab" else "ARM:" + clean))
        for h in list(self.hooks):
            h(method, clean)
        f = self._injected(method, path)
        if f is not None and not f["effect"]:
            return Resp(f["status"], f["body"], f["headers"])
        out = self._fab(method, path, json) if kind == "fab" else self._arm(method, path, json)
        if f is not None and f["effect"]:  # the write happened, but the caller sees a failure
            return Resp(f["status"], f["body"], f["headers"])
        return out

    # -- Fabric ------------------------------------------------------------------------------------------------------------------
    def _page(self, rows):
        return Resp(200, {"value": rows})

    def _fab(self, method, path, body):
        p, _, q = path.partition("?")
        if p == "capacities" and method == "GET":
            return self._page(self.capacities)
        if p == "workspaces" and method == "GET":
            m = re.search(r"roles=(\w+)", q)
            rows = list(self.workspaces.values())
            if m:
                rows = rows if m.group(1) in self.caller_roles else []
            return self._page([{k: v for k, v in w.items() if k not in ("workspaceIdentity", "capacityAssignmentProgress")} for w in rows])
        if p == "workspaces" and method == "POST":
            if any(w["displayName"].lower() == body["displayName"].lower() for w in self.workspaces.values()):
                return Resp(409, {"errorCode": "WorkspaceNameAlreadyExists", "message": "dup"})
            wid = str(uuid.uuid4())
            w = {"id": wid, "displayName": body["displayName"], "type": "Workspace"}
            if body.get("capacityId"):
                w.update({"capacityId": body["capacityId"], "capacityRegion": "West Europe"})
            self.workspaces[wid] = w
            self.items[wid], self.pools[wid], self.roles[wid] = [], [], []
            return Resp(201, {k: v for k, v in w.items()})
        m = re.fullmatch(r"workspaces/([^/]+)", p)
        if m and method == "GET":
            w = self.workspaces.get(m.group(1))
            return Resp(200, dict(w)) if w else Resp(404, {"errorCode": "WorkspaceNotFound"})
        m = re.fullmatch(r"workspaces/([^/]+)/(\w+)(?:/(.*))?", p)
        if not m:
            if p == "connections" and method == "GET":
                return self._page(self.connections)
            if p.startswith("operations/"):
                return Resp(200, {"status": "Succeeded"})
            return Resp(404, {"errorCode": "UnknownRoute", "message": path})
        wid, sub, rest = m.groups()
        w = self.workspaces.get(wid)
        if w is None:
            return Resp(404, {"errorCode": "WorkspaceNotFound"})
        if sub == "assignToCapacity" and method == "POST":
            w["capacityId"], w["capacityRegion"], w["capacityAssignmentProgress"] = body["capacityId"], "West Europe", "Completed"
            return Resp(202, None)
        if sub == "provisionIdentity" and method == "POST":
            w["workspaceIdentity"] = {"applicationId": "app", "servicePrincipalId": "sp"}
            return Resp(200, {"applicationId": "app", "servicePrincipalId": "sp"})
        if sub == "roleAssignments":
            if method == "GET":
                return self._page(self.roles[wid])
            r = {"id": str(uuid.uuid4()), "principal": body["principal"], "role": body["role"]}
            self.roles[wid].append(r)
            return Resp(201, r)
        if sub == "items" and method == "GET":
            t = re.search(r"type=(\w+)", q)
            return self._page([i for i in self.items[wid] if not t or i["type"] == t.group(1)])
        kinds = {"lakehouses": "Lakehouse", "warehouses": "Warehouse", "environments": "Environment"}
        if sub in kinds and method == "POST":
            if any(i["displayName"].lower() == body["displayName"].lower() for i in self.items[wid]):
                return Resp(409, {"errorCode": "ItemDisplayNameAlreadyInUse"})
            item = {"id": str(uuid.uuid4()), "displayName": body["displayName"], "type": kinds[sub]}
            self.items[wid].append(item)
            return Resp(201, item)
        if sub == "spark" and rest == "pools":
            if method == "GET":
                return self._page(self.pools[wid])
            pool = {"id": str(uuid.uuid4()), **body}
            self.pools[wid].append(pool)
            return Resp(201, pool)
        return Resp(404, {"errorCode": "UnknownRoute", "message": path})

    # -- ARM -------------------------------------------------------------------------------------------------------------------------
    def _arm(self, method, path, body):
        p = path.split("?")[0]
        if re.fullmatch(r"subscriptions/[^/]+", p):
            return Resp(200, {"displayName": "sub", "state": "Enabled", "tenantId": TENANT})
        if p.endswith("/providers/Microsoft.Fabric"):
            return Resp(200, {"registrationState": self.provider_state})
        if p.endswith("/Microsoft.Fabric/skus"):
            return Resp(200, {"value": [{"name": k, "locations": v} for k, v in self.skus.items()]})
        if p.endswith("/usages"):
            return Resp(200, {"value": [{"name": {"value": "CapacityQuota"}, "currentValue": self.quota[0], "limit": self.quota[1]}]})
        if p.endswith("/checkNameAvailability"):
            return Resp(200, {"nameAvailable": self.name_available, "reason": None if self.name_available else "AlreadyExists"})
        m = re.search(r"/capacities/([^/]+)$", p)
        if m:
            name = m.group(1)
            if method == "GET":
                c = self.arm_caps.get(name)
                return Resp(200, c) if c else Resp(404, {"error": {"code": "ResourceNotFound"}})
            if method == "PUT":
                c = {"id": "/subscriptions/s/.../capacities/" + name, "name": name, "location": body["location"], "sku": body["sku"],
                     "properties": {"provisioningState": "Succeeded", "state": "Active"}}
                self.arm_caps[name] = c
                self.capacities.append({"id": str(uuid.uuid4()), "displayName": name, "sku": body["sku"]["name"], "region": "West Europe", "state": "Active"})
                return Resp(201, c)
        return Resp(404, {"error": {"code": "UnknownRoute"}})

    # -- inspection ----------------------------------------------------------------------------------------------------------------------
    def writes(self):
        return [(m, p) for m, p in self.calls if m != "GET"]


def _gateways(svc: FakeService, with_arm=False):
    sleeps: list[float] = []
    fab = FabricGateway(lambda: SECRET_TOKEN, session=svc, sleep=sleeps.append)
    arm = ArmGateway(lambda: SECRET_TOKEN, session=svc, sleep=sleeps.append) if with_arm else None
    return fab, arm, sleeps


def _workspace(svc: FakeService, name="target-ws", capacity=CAP_ID):
    wid = WS_ID
    svc.workspaces[wid] = {"id": wid, "displayName": name, "type": "Workspace", **({"capacityId": capacity, "capacityRegion": "West Europe"} if capacity else {})}
    svc.items[wid], svc.pools[wid], svc.roles[wid] = [], [], []
    return wid


@pytest.fixture(scope="module")
def project(tmp_path_factory):
    store, pid = _project(tmp_path_factory.mktemp("prov"), ESTATE)
    d = _discover(store)
    plan = build_compute_plan(d)
    store.conversion_dir(pid).mkdir(parents=True, exist_ok=True)
    (store.conversion_dir(pid) / "compute-plan.json").write_text(plan.model_dump_json(), encoding="utf-8")
    from adf2fabric.agent.dependencies import discover_dependencies

    discover_dependencies(store, pid)  # linked services that need a Fabric connection come from the dependency inventory
    return store, pid


@pytest.fixture()
def prov(project, tmp_path):
    """A fresh copy of the project's records for each test (provisioning records are written next to the project)."""
    import shutil

    store, pid = project
    shutil.rmtree(store.provisioning_dir(pid), ignore_errors=True)
    return store, pid


def spec(**kw) -> TargetSpec:
    base = dict(workspace_name="target-ws", capacity_name="capone", tenant_id=TENANT, lakehouse_names=["lh_main"])
    base.update(kw)
    return TargetSpec(**base)


def _step(plan, type_, name=None):
    return next(s for s in plan.steps if s.type is type_ and (name is None or s.name.lower() == name.lower()))


# =====================================================================================================================
# 1. requirements: only what the migration needs
# =====================================================================================================================


def test_requirements_come_from_the_actual_migration(project):
    store, pid = project
    reqs, notes = rq.build_requirements(spec(), rq.load_inputs(store, pid))
    types = {r.type for r in reqs}
    assert {T.workspace, T.capacity_assignment, T.lakehouse, T.connection} <= types
    assert T.warehouse not in types and T.azure_capacity not in types and T.workspace_identity not in types  # nothing was asked for or implied
    assert all(r.needed_by or r.type in (T.workspace, T.capacity_assignment, T.lakehouse) for r in reqs)


def test_a_project_without_spark_compute_gets_no_pool_or_environment(tmp_path):
    store, pid = _project(tmp_path, {"pipeline/P.json": {"name": "P", "properties": {"activities": [{"name": "W", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}]}}})
    _discover(store)
    reqs, notes = rq.build_requirements(spec(), rq.load_inputs(store, pid))
    assert not {r.type for r in reqs} & {T.spark_pool, T.environment, T.notebook, T.spark_job_definition, T.connection}


def test_capacity_estimate_follows_the_documented_ratio():
    assert rq.sku_units("F64") == 64 and rq.sku_units("FTL64") is None and rq.sku_units("P1") is None
    assert rq.recommended_sku(12) == "F16" and rq.recommended_sku(0.5) == "F2" and rq.recommended_sku(5000) == "F2048"


# =====================================================================================================================
# 2. planning is read-only; existing resources are reused; conflicts are not overwritten
# =====================================================================================================================


def test_planning_never_writes(prov):
    store, pid = prov
    svc = FakeService()
    _workspace(svc)
    fab, arm, _ = _gateways(svc, True)
    service.plan(store, pid, spec(create_capacity=False), fab, arm, NOW)
    assert svc.writes() == []


def test_new_workspace_is_planned_with_the_capacity_and_dependencies_in_order(prov):
    store, pid = prov
    svc = FakeService()
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(provision_workspace_identity=True, role_assignments=[{"principal_id": "u1", "principal_type": "User", "role": "Contributor"}]), fab, arm, NOW)
    ws = _step(p, T.workspace)
    assert ws.action is Action.create and ws.request == {"displayName": "target-ws", "capacityId": CAP_ID}
    ids = [s.id for s in p.steps]
    assert ids.index(ws.id) < ids.index(_step(p, T.workspace_identity).id) < ids.index(_step(p, T.lakehouse).id)
    assert all(ids.index(d) < ids.index(s.id) for s in p.steps for d in s.depends_on if d in ids)
    assert _step(p, T.capacity_assignment).action is Action.reuse  # assigned by the create call itself
    assert p.plan_hash and p.summary.to_create >= 3 and svc.writes() == []


def test_existing_resources_are_reused_not_recreated(prov):
    store, pid = prov
    svc = FakeService()
    wid = _workspace(svc)
    svc.items[wid].append({"id": "lh-1", "displayName": "lh_main", "type": "Lakehouse"})
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(), fab, arm, NOW)
    assert _step(p, T.workspace).action is Action.reuse and _step(p, T.lakehouse).action is Action.reuse
    assert _step(p, T.lakehouse).existing.id == "lh-1" and _step(p, T.capacity_assignment).action is Action.reuse
    assert not [s for s in p.steps if s.action is Action.create and s.type in (T.workspace, T.lakehouse, T.capacity_assignment)]
    assert "workspace_role" in {c.name for c in p.checks}


def test_a_workspace_on_another_capacity_is_a_conflict_never_moved(prov):
    store, pid = prov
    svc = FakeService()
    _workspace(svc, capacity=OTHER_CAP)
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(), fab, arm, NOW)
    a = _step(p, T.capacity_assignment)
    assert a.action is Action.conflict and "never moved" in a.blockers[0]
    assert _step(p, T.lakehouse).action is Action.blocked  # what depends on a conflict does not proceed


def test_an_unassigned_existing_workspace_is_configured_additively(prov):
    store, pid = prov
    svc = FakeService()
    _workspace(svc, capacity=None)
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(), fab, arm, NOW)
    a = _step(p, T.capacity_assignment)
    assert a.action is Action.configure and a.request == {"capacityId": CAP_ID} and a.requires_approval


def test_role_assignment_conflict_and_item_type_collision(prov):
    store, pid = prov
    svc = FakeService()
    wid = _workspace(svc)
    svc.roles[wid].append({"id": "r1", "principal": {"id": "u1", "type": "User"}, "role": "Viewer"})
    svc.items[wid].append({"id": "n1", "displayName": "lh_main", "type": "Notebook"})
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(role_assignments=[{"principal_id": "u1", "principal_type": "User", "role": "Contributor"}]), fab, arm, NOW)
    assert _step(p, T.role_assignment).action is Action.conflict and "not changed" in _step(p, T.role_assignment).blockers[0]
    assert _step(p, T.lakehouse).action is Action.conflict and "different type" in _step(p, T.lakehouse).blockers[0]


def test_a_spark_pool_is_reused_when_identical_and_a_same_name_different_pool_is_a_conflict(prov):
    store, pid = prov
    svc = FakeService()
    wid = _workspace(svc)
    base = service.plan(store, pid, spec(), *_gateways(svc)[:2], NOW)
    pools = [s for s in base.steps if s.type is T.spark_pool]
    assert pools and all(s.action is Action.create for s in pools)
    body = pools[0].request
    svc.pools[wid].append({"id": "pool-1", **body})
    again = service.plan(store, pid, spec(), *_gateways(svc)[:2], NOW)
    assert _step(again, T.spark_pool, pools[0].name).action is Action.reuse
    svc.pools[wid][0] = {"id": "pool-1", **body, "nodeSize": "XXLarge", "autoScale": {"enabled": True, "minNodeCount": 1, "maxNodeCount": 9}}
    clash = service.plan(store, pid, spec(), *_gateways(svc)[:2], NOW)
    assert _step(clash, T.spark_pool, pools[0].name).action is Action.conflict


def test_connections_notebooks_and_jobs_are_delegated_never_created_here(prov):
    store, pid = prov
    svc = FakeService()
    _workspace(svc)
    # a generated, approved workload makes notebook / SJD requirements
    from adf2fabric.agent.workloads.engine import Options, build_manifest

    wm = build_manifest(store, pid, Options(local_files=dict(LOCAL), secret_map=dict(SECRETS), lakehouse_id=LAKEHOUSE, artifacts=dict(ARTIFACTS), generate=True), NOW)
    store.save_workload_manifest(pid, wm)
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(), fab, arm, NOW)
    delegated = [s for s in p.steps if s.action is Action.delegate]
    assert {s.type for s in delegated} >= {T.notebook}
    assert all(s.api.value == "delegated" and s.delegated_to and not s.requires_approval for s in delegated)
    store.provisioning_dir(pid)  # noqa: B018
    run = service.apply  # the executor must never write for them: checked in the apply tests below


# =====================================================================================================================
# 3. permissions, tenant and unreadable state
# =====================================================================================================================


def test_the_token_tenant_is_read_from_the_token_only():
    import base64

    payload = base64.urlsafe_b64encode(json.dumps({"tid": TENANT, "secret": "x"}).encode()).decode().rstrip("=")
    assert token_tenant(f"h.{payload}.s") == TENANT and token_tenant("garbage") is None


def test_a_token_for_another_tenant_blocks_the_plan_and_approval(prov):
    import base64

    store, pid = prov
    svc = FakeService()
    _workspace(svc)
    payload = base64.urlsafe_b64encode(json.dumps({"tid": "other-tenant"}).encode()).decode().rstrip("=")
    fab = FabricGateway(lambda: f"h.{payload}.s", session=svc, sleep=lambda s: None)
    p = service.plan(store, pid, spec(), fab, None, NOW)
    assert any(c.name == "tenant" and c.blocking for c in p.checks) and p.blockers
    with pytest.raises(ApplyRefused, match="Blocking checks"):
        service.approve(store, pid, None, "pat")


def test_viewer_cannot_create_so_item_steps_are_blocked_not_attempted(prov):
    store, pid = prov
    svc = FakeService()
    wid = _workspace(svc)
    svc.caller_roles = {"Viewer"}
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(provision_workspace_identity=True, role_assignments=[{"principal_id": "u1", "principal_type": "User", "role": "Viewer"}]), fab, arm, NOW)
    assert _step(p, T.workspace_identity).action is Action.blocked and "Admin" in _step(p, T.workspace_identity).blockers[0]
    assert _step(p, T.role_assignment).action is Action.blocked and "Member" in _step(p, T.role_assignment).blockers[0]


def test_no_role_in_an_existing_workspace_is_a_blocking_failure(prov):
    store, pid = prov
    svc = FakeService()
    _workspace(svc)
    svc.caller_roles = set()
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(), fab, arm, NOW)
    assert any(c.name == "workspace_role" and c.blocking and c.status.value == "failed" for c in p.checks)


def test_an_unreadable_state_is_unknown_never_absent(prov):
    store, pid = prov
    svc = FakeService()
    _workspace(svc)
    svc.inject("GET", r"workspaces/[^/]+/items", 403, times=99, body={"errorCode": "InsufficientPrivileges"})
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(), fab, arm, NOW)
    lh = _step(p, T.lakehouse)
    assert lh.action is Action.unknown and "could not be read" in lh.blockers[0] and not lh.requires_approval
    assert any("items" in n for n in p.notes)
    assert service.approvable(p) == [] or lh.id not in service.approvable(p)


def test_workspace_creation_permission_is_reported_unknown_not_assumed(prov):
    store, pid = prov
    svc = FakeService()
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(), fab, arm, NOW)
    c = next(c for c in p.checks if c.name == "workspace_creation")
    assert c.status.value == "unknown" and c.not_proved


def test_a_capacity_the_account_cannot_see_blocks_workspace_creation(prov):
    store, pid = prov
    svc = FakeService()
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(capacity_name="not-visible"), fab, arm, NOW)
    assert _step(p, T.workspace).action is Action.blocked and any(c.name == "capacity" and c.blocking for c in p.checks)


def test_a_too_small_capacity_is_a_warning_with_the_numbers(prov):
    store, pid = prov
    svc = FakeService()
    _workspace(svc, capacity=OTHER_CAP)
    fab, arm, _ = _gateways(svc)
    p = service.plan(store, pid, spec(capacity_name="captwo"), fab, arm, NOW)
    size = next((c for c in p.checks if c.name == "capacity_size"), None)
    if p.estimates.get("largest_pool_capacity_units", 0) > 2:
        assert size is not None and size.status.value == "failed" and not size.blocking and "capacity units" in size.detail


# =====================================================================================================================
# 4. Azure capacity (ARM): a different API with its own checks
# =====================================================================================================================


def arm_spec(**kw):
    base = dict(capacity_name=None, create_capacity=True, azure_subscription_id="sub-1", azure_resource_group="rg", azure_capacity_name="newcap",
                azure_region="westeurope", azure_sku="F4", capacity_admins=["pat@contoso.com"])
    base.update(kw)
    return spec(**base)


def test_azure_capacity_creation_is_planned_with_arm_checks(prov):
    store, pid = prov
    svc = FakeService()
    fab, arm, _ = _gateways(svc, True)
    p = service.plan(store, pid, arm_spec(), fab, arm, NOW)
    cap = _step(p, T.azure_capacity)
    assert cap.action is Action.create and cap.api.value == "arm" and cap.request["sku"] == {"name": "F4", "tier": "Fabric"}
    names = {c.name: c.status.value for c in p.checks}
    assert names["azure_subscription"] == names["resource_provider"] == names["sku_availability"] == names["quota"] == names["capacity_name"] == "passed"
    assert _step(p, T.workspace).request["capacityId"] != "" and _step(p, T.workspace).depends_on == [cap.id]
    assert [m for m, _ in svc.writes()] == ["POST"] and svc.writes()[0][1].endswith("checkNameAvailability")  # the one read-only query


@pytest.mark.parametrize("mutate,check", [
    (lambda s: setattr(s, "provider_state", "NotRegistered"), "resource_provider"),
    (lambda s: setattr(s, "name_available", False), "capacity_name"),
    (lambda s: setattr(s, "quota", (126, 128)), "quota"),
])
def test_arm_blockers_stop_the_plan(prov, mutate, check):
    store, pid = prov
    svc = FakeService()
    mutate(svc)
    fab, arm, _ = _gateways(svc, True)
    p = service.plan(store, pid, arm_spec(), fab, arm, NOW)
    c = next(c for c in p.checks if c.name == check)
    assert c.status.value == "failed" and c.blocking and p.blockers


def test_a_sku_not_offered_in_the_region_blocks_the_capacity_step(prov):
    store, pid = prov
    svc = FakeService()
    fab, arm, _ = _gateways(svc, True)
    p = service.plan(store, pid, arm_spec(azure_region="japaneast"), fab, arm, NOW)
    assert _step(p, T.azure_capacity).action is Action.blocked and "not offered" in _step(p, T.azure_capacity).blockers[0]
    assert _step(p, T.workspace).action is Action.blocked  # depends on it


def test_incomplete_capacity_details_are_listed(prov):
    store, pid = prov
    svc = FakeService()
    fab, arm, _ = _gateways(svc, True)
    s = arm_spec()
    s = s.model_copy(update={"azure_sku": None, "capacity_admins": []})
    p = service.plan(store, pid, s, fab, arm, NOW)
    b = _step(p, T.azure_capacity).blockers[0]
    assert "--azure-sku" in b and "--capacity-admin" in b


def test_an_existing_azure_capacity_of_another_size_is_a_conflict(prov):
    store, pid = prov
    svc = FakeService()
    svc.arm_caps["newcap"] = {"id": "x", "name": "newcap", "location": "westeurope", "sku": {"name": "F2"}, "properties": {"provisioningState": "Succeeded", "state": "Active"}}
    fab, arm, _ = _gateways(svc, True)
    p = service.plan(store, pid, arm_spec(), fab, arm, NOW)
    assert _step(p, T.azure_capacity).action is Action.conflict


def test_the_arm_and_fabric_tokens_only_go_to_their_own_hosts():
    svc = FakeService()
    fab, arm, _ = _gateways(svc, True)
    with pytest.raises(ProvisionError):
        fab.request("GET", "https://management.azure.com/subscriptions")
    with pytest.raises(ProvisionError):
        arm.request("GET", "https://api.fabric.microsoft.com/v1/capacities")


# =====================================================================================================================
# 5. approval and apply
# =====================================================================================================================


def _planned(prov, svc, **spec_kw):
    store, pid = prov
    fab, arm, sleeps = _gateways(svc, spec_kw.pop("arm", False))
    p = service.plan(store, pid, spec(**spec_kw), fab, arm, NOW)
    return store, pid, fab, arm, sleeps, p


def test_apply_refuses_without_approval_and_writes_nothing(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc)
    with pytest.raises(ApplyRefused, match="no approval"):
        service.apply(store, pid, fab, arm, do_apply=True)
    assert svc.writes() == []


def test_approval_is_bound_to_named_steps_and_the_plan_hash(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc)
    with pytest.raises(ApplyRefused, match="cannot be approved"):
        service.approve(store, pid, ["connection:whatever"], "pat")  # delegated: nothing to approve
    a = service.approve(store, pid, [_step(p, T.workspace).id], "pat", "go")
    assert a.step_ids == [_step(p, T.workspace).id] and a.plan_hash == p.plan_hash
    # a changed plan invalidates it
    svc2 = FakeService()
    svc2.capacities = svc.capacities[:1]
    _workspace(svc2)
    fab2, _, _ = _gateways(svc2)
    service.plan(store, pid, spec(), fab2, None, NOW)
    with pytest.raises(ApplyRefused, match="different plan"):
        service.apply(store, pid, fab2, None, do_apply=True)


def test_dry_run_changes_nothing_and_reports_what_would_happen(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc)
    service.approve(store, pid, None, "pat")
    run, report = service.apply(store, pid, fab, arm, do_apply=False)
    assert run.dry_run and report is None and svc.writes() == []
    assert any("dry run: would create" in r.detail for r in run.results)


def test_only_approved_steps_are_applied(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc, warehouse_names=["wh1"])
    service.approve(store, pid, [_step(p, T.workspace).id, _step(p, T.lakehouse).id], "pat")
    run, report = service.apply(store, pid, fab, arm, do_apply=True)
    by = {r.step_id: r for r in run.results}
    assert by[_step(p, T.warehouse).id].status is StepStatus.skipped and by[_step(p, T.warehouse).id].detail == "not approved"
    assert by[_step(p, T.workspace).id].status is StepStatus.verified and by[_step(p, T.lakehouse).id].status is StepStatus.verified
    assert not any("warehouses" in p_ for _, p_ in svc.writes())


def test_full_apply_creates_in_order_and_is_verified_by_reading_back(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc, provision_workspace_identity=True, warehouse_names=["wh1"],
                                          role_assignments=[{"principal_id": "u1", "principal_type": "User", "role": "Contributor"}])
    service.approve(store, pid, None, "pat")
    run, report = service.apply(store, pid, fab, arm, do_apply=True)
    assert report.all_verified and all(r.status in (StepStatus.verified, StepStatus.skipped) for r in run.results)
    kinds = [path.split("/")[-1] if "/" in path else path for m, path in svc.writes()]
    assert kinds[0] == "workspaces" and "provisionIdentity" in kinds and "roleAssignments" in kinds and "lakehouses" in kinds and "warehouses" in kinds
    assert [k for k in kinds].count("lakehouses") == 1
    wid = next(iter(svc.workspaces))
    assert {i["displayName"] for i in svc.items[wid]} >= {"lh_main", "wh1"} and svc.workspaces[wid]["capacityId"] == CAP_ID
    assert all(m in ("GET", "POST", "PUT") for m, _ in svc.calls)  # never a DELETE / PATCH


def test_applied_is_not_verified_until_read_back(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc)
    service.approve(store, pid, None, "pat")
    run = apply_plan(store, pid, p, store.load_provisioning(pid, "approval", __import__("adf2fabric.project.provisioning", fromlist=["x"]).ProvisioningApproval), p.target,
                     fab, arm, apply=True, sleep=lambda s: None)
    assert {r.status for r in run.results if r.resource_id} == {StepStatus.applied}  # the executor alone never says verified
    assert all("Not yet verified" in r.detail for r in run.results if r.status is StepStatus.applied)


def test_reapplying_creates_nothing_a_second_time(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc)
    service.approve(store, pid, None, "pat")
    service.apply(store, pid, fab, arm, do_apply=True)
    n = len(svc.writes())
    p2 = service.plan(store, pid, spec(), fab, arm, NOW)
    assert p2.summary.to_create == 0 and _step(p2, T.lakehouse).action is Action.reuse
    with pytest.raises(ApplyRefused):  # the old approval described a world that no longer exists
        service.apply(store, pid, fab, arm, do_apply=True)
    assert len(svc.writes()) == n


def test_a_changed_target_is_refused_before_anything_is_written(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc)
    service.approve(store, pid, None, "pat")
    wid = str(uuid.uuid4())
    svc.workspaces[wid] = {"id": wid, "displayName": "target-ws", "type": "Workspace", "capacityId": CAP_ID}  # someone created it meanwhile
    svc.items[wid], svc.pools[wid], svc.roles[wid] = [], [], []
    with pytest.raises(ApplyRefused, match="changed since the plan"):
        service.apply(store, pid, fab, arm, do_apply=True)
    assert svc.writes() == []


def test_a_resource_appearing_between_check_and_write_is_reused_not_duplicated(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, _, p = _planned(prov, svc)
    service.approve(store, pid, [_step(p, T.lakehouse).id], "pat")
    fired = []

    def hook(method, path):  # another actor creates the same lakehouse after our last look and before our write lands
        if method == "POST" and path.endswith("/lakehouses") and not fired:
            fired.append(1)
            svc.items[wid].append({"id": "lh-other", "displayName": "lh_main", "type": "Lakehouse"})

    svc.hooks.append(hook)
    run, report = service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: None)
    r = next(r for r in run.results if r.step_id == _step(p, T.lakehouse).id)
    assert r.status is StepStatus.verified and r.resource_id == "lh-other" and "reused" in r.detail.lower()
    assert [i["displayName"] for i in svc.items[wid]] == ["lh_main"]  # one lakehouse, the other actor's; nothing overwritten or duplicated


# =====================================================================================================================
# 6. retries, throttling, ambiguity
# =====================================================================================================================


def test_throttling_is_retried_after_retry_after(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, sleeps, p = _planned(prov, svc)
    service.approve(store, pid, [_step(p, T.lakehouse).id], "pat")
    svc.inject("POST", r"/lakehouses", 429, times=2, headers={"Retry-After": "7"})
    run, report = service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: sleeps.append(s))
    assert report.all_verified and 7.0 in sleeps
    assert len([w for w in svc.writes() if w[1].endswith("/lakehouses")]) == 3  # two throttled attempts never took effect, one succeeded
    assert len([i for i in svc.items[wid] if i["displayName"] == "lh_main"]) == 1


def test_a_5xx_that_did_not_take_effect_is_retried_once_state_is_reread(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, sleeps, p = _planned(prov, svc)
    service.approve(store, pid, [_step(p, T.lakehouse).id], "pat")
    svc.inject("POST", r"/lakehouses", 503, times=1, effect=False)
    run, report = service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: sleeps.append(s))
    r = next(r for r in run.results if r.step_id == _step(p, T.lakehouse).id)
    assert r.status is StepStatus.verified and r.attempts == 2
    assert len([i for i in svc.items[wid] if i["displayName"] == "lh_main"]) == 1


def test_a_5xx_after_the_write_took_effect_is_recovered_not_repeated(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, sleeps, p = _planned(prov, svc)
    service.approve(store, pid, [_step(p, T.lakehouse).id], "pat")
    svc.inject("POST", r"/lakehouses", 504, times=1, effect=True)  # created, but the caller saw a gateway timeout
    run, report = service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: sleeps.append(s))
    r = next(r for r in run.results if r.step_id == _step(p, T.lakehouse).id)
    assert r.status is StepStatus.verified and "ambiguous failure" in r.detail
    assert len([w for w in svc.writes() if w[1].endswith("/lakehouses")]) == 1  # exactly one POST: never repeated
    assert len([i for i in svc.items[wid] if i["displayName"] == "lh_main"]) == 1


def test_a_clear_refusal_is_not_retried(prov):
    svc = FakeService()
    _workspace(svc)
    store, pid, fab, arm, sleeps, p = _planned(prov, svc)
    service.approve(store, pid, [_step(p, T.lakehouse).id], "pat")
    svc.inject("POST", r"/lakehouses", 403, times=9, body={"errorCode": "InsufficientPrivileges", "message": "no"})
    run, _ = service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: sleeps.append(s))
    r = next(r for r in run.results if r.step_id == _step(p, T.lakehouse).id)
    assert r.status in (StepStatus.failed, StepStatus.mismatch) and r.attempts == 1 and "permission denied" in r.detail
    assert len([w for w in svc.writes() if w[1].endswith("/lakehouses")]) == 1


def test_persistent_5xx_gives_up_without_duplicating(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, sleeps, p = _planned(prov, svc)
    service.approve(store, pid, [_step(p, T.lakehouse).id], "pat")
    svc.inject("POST", r"/lakehouses", 503, times=99, effect=False)
    run, _ = service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: sleeps.append(s))
    r = next(r for r in run.results if r.step_id == _step(p, T.lakehouse).id)
    assert r.attempts == 3 and r.status in (StepStatus.failed, StepStatus.mismatch) and "Gave up" in r.detail
    assert not [i for i in svc.items[wid] if i["displayName"] == "lh_main"]


# =====================================================================================================================
# 7. partial failure, journal, safe recovery
# =====================================================================================================================


def test_a_failed_step_skips_only_its_dependents_and_the_rest_completes(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, sleeps, p = _planned(prov, svc, warehouse_names=["wh1"])
    pools = [s for s in p.steps if s.type is T.spark_pool and s.action is Action.create]
    assert pools
    service.approve(store, pid, None, "pat")
    svc.inject("POST", r"/spark/pools", 403, times=99, body={"errorCode": "Forbidden"})
    run, report = service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: sleeps.append(s))
    by = {r.step_id: r for r in run.results}
    assert by[pools[0].id].status in (StepStatus.failed, StepStatus.mismatch)
    for env in [s for s in p.steps if s.type is T.environment and pools[0].id in s.depends_on]:
        assert by[env.id].status is StepStatus.skipped and "did not complete" in by[env.id].detail
    assert by[_step(p, T.lakehouse).id].status is StepStatus.verified and by[_step(p, T.warehouse).id].status is StepStatus.verified  # independent work finished
    assert report.all_verified is False
    assert not any(i["displayName"].startswith("ENV") for i in svc.items[wid])  # the dependent was never attempted


def test_the_journal_is_written_after_every_step_and_survives_a_crash(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, sleeps, p = _planned(prov, svc, warehouse_names=["wh1"])
    service.approve(store, pid, None, "pat")
    seen = []
    orig = store.save_provisioning

    def spy(project_id, name, model):
        if name == "run":
            seen.append(len([r for r in model.results if r.status is not StepStatus.skipped or r.detail]))
        return orig(project_id, name, model)

    store.save_provisioning = spy
    try:
        service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: None)
    finally:
        store.save_provisioning = orig
    assert seen == sorted(seen) and seen[0] < seen[-1]  # grew step by step
    run = store.load_provisioning(pid, "run", ProvisioningRun)
    assert run.finished_at is not None and run.plan_hash == p.plan_hash


def test_recovery_after_a_partial_run_completes_only_what_is_missing(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, sleeps, p = _planned(prov, svc, warehouse_names=["wh1"])
    service.approve(store, pid, [_step(p, T.lakehouse).id, _step(p, T.warehouse).id], "pat")
    svc.inject("POST", r"/warehouses", 403, times=99, body={"errorCode": "Forbidden"})
    service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: None)
    assert [i["displayName"] for i in svc.items[wid]] == ["lh_main"]
    svc.fail.clear()  # the permission problem is fixed
    p2 = service.plan(store, pid, spec(warehouse_names=["wh1"]), fab, arm, NOW)  # fresh plan sees the lakehouse as existing
    assert _step(p2, T.lakehouse).action is Action.reuse and _step(p2, T.warehouse).action is Action.create
    service.approve(store, pid, [_step(p2, T.warehouse).id], "pat")
    service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: None)
    assert sorted(i["displayName"] for i in svc.items[wid]) == ["lh_main", "wh1"]  # one of each, nothing recreated


def test_an_unfinished_previous_run_is_noted(prov):
    svc = FakeService()
    _workspace(svc)
    store, pid, fab, arm, _, p = _planned(prov, svc)
    store.save_provisioning(pid, "run", ProvisioningRun(plan_hash="old", dry_run=False, started_at=NOW))  # never finished
    service.approve(store, pid, [_step(p, T.lakehouse).id], "pat")
    run, _ = service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: None)
    assert run.interrupted is True and run.finished_at is not None


def test_a_new_capacity_flows_through_workspace_creation(prov):
    svc = FakeService()
    store, pid, fab, arm, sleeps, p = _planned(prov, svc, arm=True, capacity_name=None, create_capacity=True, azure_subscription_id="sub-1",
                                               azure_resource_group="rg", azure_capacity_name="newcap", azure_region="westeurope", azure_sku="F4",
                                               capacity_admins=["pat@contoso.com"])
    service.approve(store, pid, None, "pat")
    run, report = service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: sleeps.append(s))
    assert report.all_verified, [(r.step_id, r.status.value, r.detail) for r in run.results]
    new_id = next(c["id"] for c in svc.capacities if c["displayName"] == "newcap")
    ws = next(iter(svc.workspaces.values()))
    assert ws["capacityId"] == new_id and "newcap" in svc.arm_caps
    order = [p_ for _, p_ in svc.writes() if not p_.endswith("checkNameAvailability")]
    assert order[0].startswith("ARM:") and order[1] == "workspaces"  # capacity before workspace


def test_arm_capacity_is_not_created_twice(prov):
    svc = FakeService()
    store, pid, fab, arm, sleeps, p = _planned(prov, svc, arm=True, capacity_name=None, create_capacity=True, azure_subscription_id="sub-1",
                                               azure_resource_group="rg", azure_capacity_name="newcap", azure_region="westeurope", azure_sku="F4",
                                               capacity_admins=["pat@contoso.com"])
    service.approve(store, pid, [_step(p, T.azure_capacity).id], "pat")
    svc.inject("PUT", r"capacities/newcap", 503, times=1, effect=True)
    service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: sleeps.append(s))
    assert len([w for w in svc.writes() if w[0] == "PUT"]) == 1 and list(svc.arm_caps) == ["newcap"]


# =====================================================================================================================
# 8. verification
# =====================================================================================================================


def test_verification_catches_a_resource_that_is_gone_after_apply(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, _, p = _planned(prov, svc)
    service.approve(store, pid, [_step(p, T.lakehouse).id], "pat")
    service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: None)
    assert service.verify(store, pid, fab, arm, NOW).all_verified
    svc.items[wid].clear()  # removed by someone else
    report = service.verify(store, pid, fab, arm, NOW)
    assert not report.all_verified
    assert next(r for r in report.results if r.step_id == _step(p, T.lakehouse).id).status is StepStatus.mismatch


def test_verification_never_writes(prov):
    svc = FakeService()
    _workspace(svc)
    store, pid, fab, arm, _, p = _planned(prov, svc)
    n = len(svc.writes())
    service.verify(store, pid, fab, arm, NOW)
    assert len(svc.writes()) == n


def test_planned_but_unapplied_creates_are_not_claimed_to_exist(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc)
    report = verify_plan(p, p.target, fab, arm, None, NOW)
    assert not report.all_verified and all(r.status is not StepStatus.verified for r in report.results if "delegated" not in r.detail)


def test_delegated_resources_are_only_reported(prov):
    svc = FakeService()
    wid = _workspace(svc)
    store, pid, fab, arm, _, p = _planned(prov, svc)
    report = verify_plan(p, p.target, fab, arm, None, NOW)
    conn = [r for r in report.results if r.step_id.startswith("connection:")]
    assert conn and all(r.status is StepStatus.skipped and r.detail.startswith("delegated") for r in conn)


# =====================================================================================================================
# 9. nothing sensitive is persisted; nothing is deleted; discovery and tests create nothing
# =====================================================================================================================


def test_no_token_or_secret_reaches_any_record(prov):
    svc = FakeService()
    store, pid, fab, arm, _, p = _planned(prov, svc, provision_workspace_identity=True)
    service.approve(store, pid, None, "pat")
    service.apply(store, pid, fab, arm, do_apply=True, sleep=lambda s: None)
    blob = "".join(f.read_text(encoding="utf-8") for f in store.provisioning_dir(pid).glob("*") if f.is_file())
    assert SECRET_TOKEN not in blob and "Bearer" not in blob


def test_an_error_message_never_contains_the_token():
    svc = FakeService()
    fab, _, _ = _gateways(svc)
    svc.inject("GET", r"capacities", 500, times=99, body={"message": f"Bearer {SECRET_TOKEN} leaked"})
    with pytest.raises(ProvisionError) as e:
        fab.paged("capacities", "listing")
    assert SECRET_TOKEN not in str(e.value)


def test_provisioning_modules_issue_no_delete_or_patch():
    src = "".join(p.read_text(encoding="utf-8") for p in (Path(__file__).parents[1] / "src/adf2fabric/agent/provisioning").glob("*.py"))
    assert not re.search(r'"(DELETE|PATCH)"', src) and "deprovision" not in src.lower().replace("deprovisioning", "")


def test_state_discovery_is_get_only():
    from adf2fabric.agent.provisioning.state import read_state

    svc = FakeService()
    _workspace(svc)
    fab, arm, _ = _gateways(svc, True)
    read_state(fab, arm, arm_spec(workspace_name="target-ws"), NOW)
    assert svc.writes() == []
