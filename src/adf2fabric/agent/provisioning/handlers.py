"""One handler per resource type: how to PLAN it from the state that was read, FIND it live, APPLY it, and VERIFY it.

The split matters. ``plan`` is pure (it reads the state discovery recorded, never the network). ``find`` and ``verify`` only
GET. ``apply`` is the only place a write is sent, and every ``apply`` first re-reads the live state (``find``): a plan can be
hours old, so an existing resource is never overwritten and a duplicate never created because the plan said "create".

Operations used, by API (none assumed to exist on another):

  Azure Resource Manager (api-version 2023-11-01)
    GET/PUT  /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Fabric/capacities/{name}
  Fabric REST (/v1)
    POST /workspaces                       create workspace (201)             POST /workspaces/{id}/assignToCapacity   (202)
    POST /workspaces/{id}/provisionIdentity (200 or 202)                      POST /workspaces/{id}/roleAssignments    (201)
    POST /workspaces/{id}/lakehouses | /warehouses (201 or 202)               POST /workspaces/{id}/spark/pools        (201)
    POST /workspaces/{id}/environments (201 or 202; definition parts)
  Delegated (not written here): connections (credentials) -> connections apply; notebooks / Spark job definitions ->
  workloads-deploy (approved workloads only); shortcuts -> create-shortcut.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from adf2fabric.agent.deployment import compute_deploy
from adf2fabric.agent.provisioning import requirements as rq
from adf2fabric.agent.provisioning.state import ARM_CAPACITY_API, arm_capacity_path
from adf2fabric.agent.provisioning.transport import ArmGateway, FabricGateway, ProvisionError
from adf2fabric.project.compute import FabricEnvironmentProposal, FabricPoolProposal
from adf2fabric.project.provisioning import (Action, Api, Existence, ExistingResource, ProvisionStep, Requirement, ResourceType as T,
                                             StepStatus, TargetSpec, TargetState, VerifiedResource)

_CAP_NAME = re.compile(r"^[a-z][a-z0-9]{2,62}$")
ROLE_RANK = {"Viewer": 0, "Contributor": 1, "Member": 2, "Admin": 3}
PENDING_CAPACITY = "<resolved after the capacity is created>"


@dataclass
class Ctx:
    spec: TargetSpec
    state: TargetState
    fab: FabricGateway
    arm: Optional[ArmGateway] = None
    workspace_id: Optional[str] = None  # known once the workspace exists (found at planning, or created by a step)
    capacity_id: Optional[str] = None
    created: dict[str, str] = field(default_factory=dict)  # step id -> the id the resource got
    sku_regions: Optional[dict[str, set]] = None  # SKU -> locations, when the ARM SKU list was readable
    sleep: Any = None

    def __post_init__(self) -> None:
        if self.workspace_id is None and self.state.workspace is not None:
            self.workspace_id = self.state.workspace.id
        if self.sleep is None:
            import time

            self.sleep = time.sleep

    # -- what planning may conclude from the recorded state ---------------------------------------------------------------
    def workspace_will_exist_after_plan(self) -> bool:
        return self.state.workspace is not None or "workspace" in self.state.unread

    def children_readable(self, key: str) -> Optional[str]:
        """None if children of this kind can be judged from the state; else the reason they cannot (so they are 'unknown')."""
        if self.state.workspace is None:
            return None if "workspace" not in self.state.unread else "the workspace could not be read"
        return self.state.unread.get(key)


@dataclass
class Outcome:
    resource_id: Optional[str] = None
    detail: str = ""
    created: bool = True  # False: it already existed when the write was about to be sent


def _step(req: Requirement, action: Action, api: Api, *, endpoint: str = "", request: Optional[dict] = None, existing: Optional[ExistingResource] = None,
          blockers=(), estimate: Optional[dict] = None, delegated_to: str = "", notes=()) -> ProvisionStep:
    return ProvisionStep(
        id=req.id, type=req.type, name=req.name, action=action, api=api, depends_on=list(req.depends_on), request=request or {}, endpoint=endpoint,
        existing=existing, reason=req.reason, needed_by=list(req.needed_by), estimate=estimate or {}, blockers=list(blockers),
        requires_approval=action in (Action.create, Action.configure), delegated_to=delegated_to, notes=list(notes), spec=dict(req.spec))


def _by_name(items: list[ExistingResource], name: str) -> Optional[ExistingResource]:
    return next((i for i in items if i.name.lower() == name.lower()), None)


class Handler:
    type: T

    def plan(self, ctx: Ctx, req: Requirement) -> ProvisionStep:  # pragma: no cover - interface
        raise NotImplementedError

    def find(self, ctx: Ctx, step: ProvisionStep) -> Optional[str]:
        """The id of the live resource, or None when it is absent. Raises ProvisionError when it cannot be read (never 'absent')."""
        raise NotImplementedError

    def apply(self, ctx: Ctx, step: ProvisionStep) -> Outcome:
        raise NotImplementedError

    def verify(self, ctx: Ctx, step: ProvisionStep) -> VerifiedResource:
        try:
            found = self.find(ctx, step)
        except ProvisionError as exc:
            return VerifiedResource(step_id=step.id, status=StepStatus.failed, detail=f"could not read the resource: {exc}")
        if found is None:
            return VerifiedResource(step_id=step.id, status=StepStatus.mismatch, detail="the resource was not found when re-read")
        return VerifiedResource(step_id=step.id, status=StepStatus.verified, resource_id=found, detail=self._verified_detail(ctx, step))

    def _verified_detail(self, ctx: Ctx, step: ProvisionStep) -> str:
        return "found by reading it back from the service"


def _need_ws(ctx: Ctx) -> str:
    wid = ctx.workspace_id or ctx.created.get(rq.rid(T.workspace, ctx.spec.workspace_name or ctx.spec.workspace_id or ""))
    if not wid:
        raise ProvisionError("The workspace does not exist yet, so this step cannot run.")
    return wid


def _write(ctx: Ctx, method: str, path: str, body: Optional[dict], what: str):
    """A write: throttling is retried (it never took effect); an ambiguous failure is raised so the caller re-reads state first."""
    resp = ctx.fab.request(method, path, body, ambiguous_retry=False)
    return resp


# =====================================================================================================================
# Azure Fabric capacity (ARM)
# =====================================================================================================================


class AzureCapacityHandler(Handler):
    type = T.azure_capacity

    def plan(self, ctx, req):
        s = ctx.spec
        missing = [n for n, v in (("--azure-subscription", s.azure_subscription_id), ("--azure-resource-group", s.azure_resource_group),
                                  ("--azure-capacity-name", s.azure_capacity_name), ("--azure-region", s.azure_region), ("--azure-sku", s.azure_sku),
                                  ("--capacity-admin", s.capacity_admins)) if not v]
        blockers = [f"Missing for the new capacity: {', '.join(missing)}."] if missing else []
        if s.azure_capacity_name and not _CAP_NAME.match(s.azure_capacity_name):
            blockers.append("A capacity name must be 3-63 characters, lower-case letters and digits, starting with a letter.")
        units = rq.sku_units(s.azure_sku)
        if s.azure_sku and units is None:
            blockers.append(f"'{s.azure_sku}' is not an F SKU (F2 ... F2048).")
        if ctx.sku_regions is not None and s.azure_sku and s.azure_region and units:
            where = ctx.sku_regions.get(s.azure_sku.upper())
            if where is None:
                blockers.append(f"SKU {s.azure_sku} is not in the list of Fabric SKUs offered to this subscription.")
            elif s.azure_region.lower().replace(" ", "") not in {w.lower().replace(" ", "") for w in where}:
                blockers.append(f"SKU {s.azure_sku} is not offered in region '{s.azure_region}' for this subscription.")
        body = {"location": s.azure_region, "sku": {"name": (s.azure_sku or "").upper(), "tier": "Fabric"},
                "properties": {"administration": {"members": list(s.capacity_admins)}}}
        est = {"sku": (s.azure_sku or "").upper(), "capacity_units": units, "billing": "A running Fabric capacity is billed until it is paused."}
        ep = f"PUT {arm_capacity_path(s) or 'subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Fabric/capacities/{name}'}"
        if blockers:
            return _step(req, Action.blocked, Api.arm, endpoint=ep, request=body, blockers=blockers, estimate=est)
        if "azure_capacity" in ctx.state.unread:
            return _step(req, Action.unknown, Api.arm, endpoint=ep, request=body, estimate=est,
                         blockers=[f"The existing Azure capacity could not be read ({ctx.state.unread['azure_capacity']}); nothing is assumed."])
        ex = _by_name(ctx.state.azure_capacities, s.azure_capacity_name or "")
        if ex is not None:
            have = str(ex.properties.get("sku") or "").upper()
            if have and have != est["sku"]:
                return _step(req, Action.conflict, Api.arm, endpoint=ep, request=body, existing=ex, estimate=est,
                             blockers=[f"A capacity named '{ex.name}' exists as {have}, not {est['sku']}. It is not resized here."])
            return _step(req, Action.reuse, Api.arm, endpoint=ep, request=body, existing=ex, estimate=est,
                         notes=[f"State {ex.properties.get('state') or ex.properties.get('provisioningState') or 'unknown'}."])
        return _step(req, Action.create, Api.arm, endpoint=ep, request=body, estimate=est)

    def _path(self, ctx):
        p = arm_capacity_path(ctx.spec)
        if p is None or ctx.arm is None:
            raise ProvisionError("No Azure Resource Manager credential or capacity address is available.")
        return p

    def find(self, ctx, step):
        r = ctx.arm.request("GET", self._path(ctx)) if ctx.arm else None
        if r is None:
            raise ProvisionError("No Azure Resource Manager credential was supplied.")
        if r.status == 200 and isinstance(r.body, dict):
            return str(r.body.get("id") or ctx.spec.azure_capacity_name)
        if r.status == 404:
            return None
        raise ctx.arm.error(r, "Reading the Azure capacity")

    def apply(self, ctx, step):
        live = self.find(ctx, step)
        if live:
            return Outcome(live, "already exists (found immediately before creating); not created", created=False)
        r = ctx.arm.request("PUT", self._path(ctx), step.request, ambiguous_retry=False)
        if r.status not in (200, 201):
            raise ctx.arm.error(r, "Creating the Azure capacity")
        for _ in range(60):  # provisioningState is on the resource itself
            ctx.sleep(10)
            g = ctx.arm.request("GET", self._path(ctx))
            if g.status != 200:
                raise ctx.arm.error(g, "Reading the new Azure capacity")
            state = str(((g.body or {}).get("properties") or {}).get("provisioningState", ""))
            if state.lower() == "succeeded":
                return Outcome(str((g.body or {}).get("id") or ""), "created")
            if state.lower() in ("failed", "canceled", "deleting"):
                raise ProvisionError(f"The Azure capacity ended in provisioning state '{state}'.")
        raise ProvisionError("The Azure capacity did not finish provisioning in time; check the portal before retrying.", ambiguous=True)

    def _verified_detail(self, ctx, step):
        return "the Azure capacity was read back from Azure Resource Manager"


def resolve_capacity(ctx: Ctx) -> tuple[Optional[str], Optional[str], str]:
    """(capacity id, region, why-not). Uses only the recorded capacity list, or the created one when ``created_name`` is known."""
    s = ctx.spec
    caps = ctx.state.capacities
    if "capacities" in ctx.state.unread:
        return None, None, f"the capacity list could not be read ({ctx.state.unread['capacities']})"
    target = None
    if s.capacity_id:
        target = next((c for c in caps if (c.id or "").lower() == s.capacity_id.lower()), None)
        why = f"capacity {s.capacity_id} is not visible to this account (it must be a capacity you administer or contribute to)"
    elif s.capacity_name or (s.create_capacity and s.azure_capacity_name):
        name = s.capacity_name or s.azure_capacity_name or ""
        target = _by_name(caps, name)
        why = (f"capacity '{name}' does not exist yet" if s.create_capacity else f"no capacity named '{name}' is visible to this account")
        if target is None and s.create_capacity:
            return None, None, PENDING_CAPACITY
    else:
        return None, None, "no capacity was chosen (--capacity-id / --capacity-name, or --create-capacity)"
    if target is None:
        return None, None, why
    if str(target.properties.get("state", "Active")).lower() != "active":
        return target.id, str(target.properties.get("region") or ""), f"capacity '{target.name}' is {target.properties.get('state')}, not Active"
    return target.id, str(target.properties.get("region") or ""), ""


# =====================================================================================================================
# Workspace, capacity assignment, identity, roles
# =====================================================================================================================


class WorkspaceHandler(Handler):
    type = T.workspace

    def plan(self, ctx, req):
        s, st = ctx.spec, ctx.state
        ep = "POST /v1/workspaces"
        if "workspace" in st.unread:
            return _step(req, Action.unknown, Api.fabric, endpoint=ep, blockers=[f"The workspace could not be read ({st.unread['workspace']}); nothing is assumed."])
        if st.workspace is not None:
            roles = st.workspace.properties.get("caller_roles") or []
            notes = [f"Your roles here: {', '.join(roles) or 'none detected'}."]
            return _step(req, Action.reuse, Api.fabric, endpoint=ep, existing=st.workspace, notes=notes)
        if not s.workspace_name:
            return _step(req, Action.blocked, Api.fabric, endpoint=ep, blockers=[
                f"Workspace id {s.workspace_id} was not found or is not visible to this account, and no name was given to create one." if s.workspace_id
                else "No workspace was chosen (--workspace-name or --workspace-id)."])
        cap_id, _, why = resolve_capacity(ctx)
        body: dict[str, Any] = {"displayName": s.workspace_name}
        blockers = []
        if cap_id:
            body["capacityId"] = cap_id
        elif why == PENDING_CAPACITY:
            body["capacityId"] = PENDING_CAPACITY
        else:
            blockers.append(f"No usable capacity: {why}.")
        if len(s.workspace_name) > 256:
            blockers.append("A workspace name is at most 256 characters.")
        return _step(req, Action.blocked if blockers else Action.create, Api.fabric, endpoint=ep, request=body, blockers=blockers,
                     notes=["Needs the tenant setting that lets you create workspaces (it cannot be read without admin rights; a refusal is reported when applied)."])

    def find(self, ctx, step):
        s = ctx.spec
        if ctx.workspace_id:
            r = ctx.fab.request("GET", f"workspaces/{ctx.workspace_id}")
            if r.status == 200:
                return ctx.workspace_id
            if r.status == 404:
                return None
            raise ctx.fab.error(r, "Reading the workspace")
        for w in ctx.fab.paged("workspaces", "listing workspaces"):
            if str(w.get("displayName", "")).lower() == (s.workspace_name or "").lower() and w.get("type") != "Personal":
                return str(w["id"])
        return None

    def apply(self, ctx, step):
        live = self.find(ctx, step)
        if live:
            ctx.workspace_id = live
            return Outcome(live, "already exists (found immediately before creating); not created", created=False)
        body = dict(step.request)
        if body.get("capacityId") == PENDING_CAPACITY:
            cid = resolve_created_capacity(ctx)
            body["capacityId"] = cid
        r = _write(ctx, "POST", "workspaces", body, "Creating the workspace")
        if r.status == 409:
            now = self.find(ctx, step)
            if now:
                ctx.workspace_id = now
                return Outcome(now, "the name was taken just before the write; the existing workspace was reused", created=False)
        if r.status not in (200, 201, 202) or not isinstance(r.body, dict) and r.status != 202:
            raise ctx.fab.error(r, "Creating the workspace")
        wid = str((r.body or {}).get("id") or "") or self.find(ctx, step)
        if not wid:
            raise ProvisionError("The workspace was created but its id could not be read; check the tenant before retrying.", ambiguous=True)
        ctx.workspace_id = wid
        return Outcome(wid, "created")


def resolve_created_capacity(ctx: Ctx) -> str:
    """The Fabric id of the capacity this plan created in Azure: a Fabric capacity's id is not the ARM id, so it is looked up by name."""
    name = (ctx.spec.azure_capacity_name or "").lower()
    for _ in range(12):
        for c in ctx.fab.paged("capacities", "listing capacities"):
            if str(c.get("displayName", "")).lower() == name and str(c.get("state", "")).lower() == "active":
                return str(c["id"])
        ctx.sleep(15)
    raise ProvisionError(f"The new capacity '{name}' does not appear (as Active) in the Fabric capacity list yet. Re-run apply later; the capacity itself is not recreated.")


class CapacityAssignmentHandler(Handler):
    type = T.capacity_assignment

    def plan(self, ctx, req):
        st = ctx.state
        ep = "POST /v1/workspaces/{id}/assignToCapacity"
        cap_id, _, why = resolve_capacity(ctx)
        if "workspace" in st.unread:
            return _step(req, Action.unknown, Api.fabric, endpoint=ep, blockers=["The workspace could not be read, so its capacity is unknown."])
        if st.workspace is None:  # the workspace create call carries the capacity
            return _step(req, Action.reuse if (cap_id or why == PENDING_CAPACITY) else Action.blocked, Api.fabric, endpoint=ep,
                         blockers=[] if (cap_id or why == PENDING_CAPACITY) else [f"No usable capacity: {why}."],
                         notes=["The workspace is created already assigned to the capacity, so no separate assignment is needed."])
        have = st.workspace.properties.get("capacityId")
        if have and cap_id and have.lower() == cap_id.lower():
            return _step(req, Action.reuse, Api.fabric, endpoint=ep, existing=st.workspace, notes=["Already on the chosen capacity."])
        if have:
            if not (ctx.spec.capacity_id or ctx.spec.capacity_name or ctx.spec.create_capacity):
                return _step(req, Action.reuse, Api.fabric, endpoint=ep, existing=st.workspace, notes=["Already on a capacity; none was requested."])
            return _step(req, Action.conflict, Api.fabric, endpoint=ep, existing=st.workspace, blockers=[
                f"The workspace is already assigned to capacity {have}, not the one chosen. It is never moved automatically: moving "
                "a workspace with Fabric items between capacities is restricted and cancels running jobs."])
        if cap_id is None and why != PENDING_CAPACITY:
            return _step(req, Action.blocked, Api.fabric, endpoint=ep, existing=st.workspace, blockers=[f"No usable capacity: {why}."])
        roles = st.workspace.properties.get("caller_roles") or []
        blockers = [] if "Admin" in roles or not roles else ["Assigning a capacity needs the workspace Admin role (and capacity contributor/admin); yours: " + ", ".join(roles) + "."]
        return _step(req, Action.blocked if blockers else Action.configure, Api.fabric, endpoint=ep, request={"capacityId": cap_id or PENDING_CAPACITY},
                     existing=st.workspace, blockers=blockers)

    def find(self, ctx, step):
        r = ctx.fab.request("GET", f"workspaces/{_need_ws(ctx)}")
        if r.status != 200:
            raise ctx.fab.error(r, "Reading the workspace")
        return str(r.body.get("capacityId")) if (r.body or {}).get("capacityId") else None

    def apply(self, ctx, step):
        wid = _need_ws(ctx)
        cid = step.request.get("capacityId")
        if cid == PENDING_CAPACITY:
            cid = resolve_created_capacity(ctx)
        if not cid:  # the workspace was created with its capacity
            return Outcome(self.find(ctx, step), "assigned when the workspace was created", created=False)
        have = self.find(ctx, step)
        if have:
            return Outcome(have, "the workspace already has a capacity; not changed", created=False) if have.lower() == cid.lower() else _raise(
                f"The workspace is already on capacity {have}; it is not moved.")
        r = _write(ctx, "POST", f"workspaces/{wid}/assignToCapacity", {"capacityId": cid}, "Assigning the capacity")
        if r.status not in (200, 202):
            raise ctx.fab.error(r, "Assigning the capacity")
        for _ in range(60):  # the 202 has no operation address: poll the workspace
            ctx.sleep(5)
            g = ctx.fab.request("GET", f"workspaces/{wid}")
            if g.status != 200:
                raise ctx.fab.error(g, "Reading the workspace")
            progress = str((g.body or {}).get("capacityAssignmentProgress") or "")
            if (g.body or {}).get("capacityId") and progress.lower() in ("", "completed"):
                return Outcome(str(g.body["capacityId"]), "assigned")
            if progress.lower() == "failed":
                raise ProvisionError("Fabric reported the capacity assignment as Failed.")
        raise ProvisionError("The capacity assignment did not finish in time; check the workspace settings before retrying.", ambiguous=True)

    def verify(self, ctx, step):
        try:
            have = self.find(ctx, step)
        except ProvisionError as exc:
            return VerifiedResource(step_id=step.id, status=StepStatus.failed, detail=str(exc))
        if not have:
            return VerifiedResource(step_id=step.id, status=StepStatus.mismatch, detail="the workspace has no capacity")
        want = step.request.get("capacityId")
        if want and want != PENDING_CAPACITY and want.lower() != have.lower():
            return VerifiedResource(step_id=step.id, status=StepStatus.mismatch, detail=f"the workspace is on a different capacity ({have})")
        return VerifiedResource(step_id=step.id, status=StepStatus.verified, resource_id=have, detail="the workspace's capacity was read back")


def _raise(msg: str):
    raise ProvisionError(msg)


class IdentityHandler(Handler):
    type = T.workspace_identity

    def plan(self, ctx, req):
        st = ctx.state
        ep = "POST /v1/workspaces/{id}/provisionIdentity"
        if "workspace" in st.unread:
            return _step(req, Action.unknown, Api.fabric, endpoint=ep, blockers=["The workspace could not be read."])
        if st.workspace is None:
            return _step(req, Action.create, Api.fabric, endpoint=ep, notes=["Provisioned after the workspace is created."])
        if st.workspace.properties.get("has_identity"):
            return _step(req, Action.reuse, Api.fabric, endpoint=ep, existing=st.workspace)
        roles = st.workspace.properties.get("caller_roles") or []
        if roles and "Admin" not in roles:
            return _step(req, Action.blocked, Api.fabric, endpoint=ep, existing=st.workspace,
                         blockers=["Provisioning a workspace identity needs the workspace Admin role; yours: " + ", ".join(roles) + "."])
        return _step(req, Action.configure, Api.fabric, endpoint=ep, existing=st.workspace)

    def find(self, ctx, step):
        r = ctx.fab.request("GET", f"workspaces/{_need_ws(ctx)}")
        if r.status != 200:
            raise ctx.fab.error(r, "Reading the workspace")
        ident = (r.body or {}).get("workspaceIdentity")
        return str(ident.get("servicePrincipalId") or ident.get("applicationId") or "identity") if isinstance(ident, dict) and ident else None

    def apply(self, ctx, step):
        wid = _need_ws(ctx)
        live = self.find(ctx, step)
        if live:
            return Outcome(live, "the workspace already has an identity; not changed", created=False)
        r = _write(ctx, "POST", f"workspaces/{wid}/provisionIdentity", None, "Provisioning the workspace identity")
        if r.status == 202:
            ctx.fab.wait_operation(r, "Provisioning the workspace identity")
        elif r.status != 200:
            raise ctx.fab.error(r, "Provisioning the workspace identity")
        return Outcome(self.find(ctx, step), "provisioned")


class RoleAssignmentHandler(Handler):
    type = T.role_assignment

    def plan(self, ctx, req):
        st, want = ctx.state, req.spec
        ep = "POST /v1/workspaces/{id}/roleAssignments"
        pid, role, ptype = want.get("principal_id"), want.get("role"), want.get("principal_type")
        blockers = []
        if role not in ROLE_RANK:
            blockers.append(f"Role '{role}' is not one of Admin, Member, Contributor, Viewer.")
        if ptype not in ("User", "Group", "ServicePrincipal", "ServicePrincipalProfile", "EntireTenant"):
            blockers.append(f"Principal type '{ptype}' is not supported.")
        body = {"principal": {"id": pid, "type": ptype}, "role": role}
        if blockers:
            return _step(req, Action.blocked, Api.fabric, endpoint=ep, request=body, blockers=blockers)
        why = ctx.children_readable("role_assignments")
        if why:
            return _step(req, Action.unknown, Api.fabric, endpoint=ep, request=body, blockers=[f"Role assignments could not be read ({why}); nothing is assumed."])
        ex = next((r for r in st.role_assignments if r.name.lower() == str(pid).lower()), None)
        if ex is not None:
            if str(ex.properties.get("role")) == role:
                return _step(req, Action.reuse, Api.fabric, endpoint=ep, request=body, existing=ex)
            return _step(req, Action.conflict, Api.fabric, endpoint=ep, request=body, existing=ex,
                         blockers=[f"{pid} already has the role {ex.properties.get('role')}, not {role}. Roles are not changed here."])
        roles = (st.workspace.properties.get("caller_roles") if st.workspace else None) or []
        if roles and max(ROLE_RANK[r] for r in roles) < ROLE_RANK["Member"]:
            return _step(req, Action.blocked, Api.fabric, endpoint=ep, request=body, blockers=["Adding a role assignment needs the Member role or higher; yours: " + ", ".join(roles) + "."])
        if roles and role == "Admin" and "Admin" not in roles:
            return _step(req, Action.blocked, Api.fabric, endpoint=ep, request=body, blockers=["A Member can grant Member or lower; granting Admin needs the Admin role."])
        return _step(req, Action.configure if st.workspace else Action.create, Api.fabric, endpoint=ep, request=body)

    def _list(self, ctx):
        return ctx.fab.paged(f"workspaces/{_need_ws(ctx)}/roleAssignments", "listing role assignments")

    def find(self, ctx, step):
        want = step.request["principal"]["id"]
        for r in self._list(ctx):
            if str((r.get("principal") or {}).get("id", "")).lower() == str(want).lower():
                return str(r.get("id") or want) if r.get("role") == step.request["role"] else _raise(
                    f"{want} already has the role {r.get('role')}; it is not changed.")
        return None

    def apply(self, ctx, step):
        wid = _need_ws(ctx)
        live = self.find(ctx, step)
        if live:
            return Outcome(live, "the assignment already exists; not added", created=False)
        r = _write(ctx, "POST", f"workspaces/{wid}/roleAssignments", step.request, "Adding the role assignment")
        if r.status not in (200, 201):
            raise ctx.fab.error(r, "Adding the role assignment")
        return Outcome(str((r.body or {}).get("id") or ""), "added")


# =====================================================================================================================
# Items: lakehouse, warehouse, Spark pool, environment
# =====================================================================================================================


class _ItemHandler(Handler):
    item_type = ""
    path = ""
    label = ""

    def _body(self, step: ProvisionStep) -> dict:
        return dict(step.request)

    def plan(self, ctx, req):
        ep = f"POST /v1/workspaces/{{id}}/{self.path}"
        if req.spec.get("unnamed"):
            return _step(req, Action.blocked, Api.fabric, endpoint=ep, blockers=["Name the lakehouse the workloads should use (--lakehouse NAME): a name is never invented."])
        body = self._request(ctx, req)
        extra = self._blockers(ctx, req)
        if extra:
            return _step(req, Action.blocked, Api.fabric, endpoint=ep, request=body, blockers=extra)
        why = ctx.children_readable("items")
        if why:
            return _step(req, Action.unknown, Api.fabric, endpoint=ep, request=body, blockers=[f"The workspace's items could not be read ({why}); nothing is assumed."])
        ex = _by_name(ctx.state.items.get(self.item_type, []), req.name)
        if ex is not None:
            return _step(req, Action.reuse, Api.fabric, endpoint=ep, request=body, existing=ex, notes=["Reused by name; its contents are not compared or changed."])
        other = next((i for t, lst in ctx.state.items.items() if t != self.item_type for i in lst if i.name.lower() == req.name.lower()), None)
        if other is not None:
            return _step(req, Action.conflict, Api.fabric, endpoint=ep, request=body, existing=other,
                         blockers=[f"An item named '{req.name}' already exists with a different type; names are unique per workspace."])
        return _step(req, Action.create, Api.fabric, endpoint=ep, request=body)

    def _request(self, ctx, req) -> dict:
        return {"displayName": req.name}

    def _blockers(self, ctx, req) -> list[str]:
        return []

    def find(self, ctx, step):
        for i in ctx.fab.paged(f"workspaces/{_need_ws(ctx)}/items?type={self.item_type}", f"listing {self.label}s"):
            if str(i.get("displayName", "")).lower() == step.name.lower():
                return str(i["id"])
        return None

    def apply(self, ctx, step):
        wid = _need_ws(ctx)
        live = self.find(ctx, step)
        if live:
            return Outcome(live, "already exists (found immediately before creating); not created", created=False)
        r = _write(ctx, "POST", f"workspaces/{wid}/{self.path}", self._body(step), f"Creating the {self.label}")
        if r.status == 409 or (r.status == 400 and r.error_code() == "ItemDisplayNameAlreadyInUse"):
            now = self.find(ctx, step)  # created by someone else between the check and the write: reuse it
            if now:
                return Outcome(now, f"the name was taken just before the write; the existing {self.label} was reused, nothing was overwritten", created=False)
        if r.status == 202:
            ctx.fab.wait_operation(r, f"Creating the {self.label}")
        elif r.status not in (200, 201):
            raise ctx.fab.error(r, f"Creating the {self.label}")
        rid_ = str((r.body or {}).get("id") or "") if isinstance(r.body, dict) else ""
        return Outcome(rid_ or self.find(ctx, step), "created")


class LakehouseHandler(_ItemHandler):
    type, item_type, path, label = T.lakehouse, "Lakehouse", "lakehouses", "lakehouse"

    def _request(self, ctx, req):
        body = {"displayName": req.name}
        if req.spec.get("schemas"):
            body["creationPayload"] = {"enableSchemas": True}  # only 'true' is accepted (preview)
        return body


class WarehouseHandler(_ItemHandler):
    type, item_type, path, label = T.warehouse, "Warehouse", "warehouses", "warehouse"


class PoolHandler(Handler):
    type = T.spark_pool

    def plan(self, ctx, req):
        ep = "POST /v1/workspaces/{id}/spark/pools"
        body = req.spec.get("request_body")
        problems = compute_deploy.validate_pool_body(body)
        peak = req.spec.get("peak_spark_vcores")
        est = {"peak_spark_vcores": peak, "capacity_units_at_peak": (peak / 2) if peak else None}
        if problems:
            return _step(req, Action.blocked, Api.fabric, endpoint=ep, request=body or {}, estimate=est, blockers=["The pool request would be rejected: " + "; ".join(problems)])
        why = ctx.children_readable("pools")
        if why:
            return _step(req, Action.unknown, Api.fabric, endpoint=ep, request=body, estimate=est, blockers=[f"Existing pools could not be read ({why}); nothing is assumed."])
        existing = {p.name.lower(): {**p.properties, "id": p.id, "name": p.name} for p in ctx.state.pools}
        found, how = compute_deploy.find_reusable(existing, body)
        if found is not None:
            return _step(req, Action.reuse, Api.fabric, endpoint=ep, request=body, estimate=est,
                         existing=ExistingResource(id=found.get("id"), name=str(found.get("name")), properties={}), notes=[how] if how else [])
        same = existing.get(str(body.get("name", "")).lower())
        if same is not None:
            return _step(req, Action.conflict, Api.fabric, endpoint=ep, request=body, estimate=est,
                         blockers=[f"A pool named '{body.get('name')}' exists with different settings; it is not modified."])
        return _step(req, Action.create, Api.fabric, endpoint=ep, request=body, estimate=est)

    def find(self, ctx, step):
        for p in ctx.fab.paged(f"workspaces/{_need_ws(ctx)}/spark/pools", "listing Spark pools"):
            if str(p.get("name", "")).lower() == str(step.request.get("name", "")).lower():
                return str(p.get("id"))
        return None

    def apply(self, ctx, step):
        wid = _need_ws(ctx)
        live = self.find(ctx, step)
        if live:
            return Outcome(live, "a pool with this name already exists (found immediately before creating); not created", created=False)
        problems = compute_deploy.validate_pool_body(step.request)
        if problems:
            raise ProvisionError("The pool request breaks a documented constraint: " + "; ".join(problems))
        r = _write(ctx, "POST", f"workspaces/{wid}/spark/pools", step.request, "Creating the Spark pool")
        if r.status not in (200, 201):
            raise ctx.fab.error(r, "Creating the Spark pool")
        return Outcome(str((r.body or {}).get("id") or "") or self.find(ctx, step), "created")


class EnvironmentHandler(_ItemHandler):
    type, item_type, path, label = T.environment, "Environment", "environments", "environment"

    def _request(self, ctx, req):
        return {"displayName": req.name, "runtime_version": req.spec.get("runtime_version"), "pool": req.spec.get("pool_name")}

    def _blockers(self, ctx, req):
        if not req.spec.get("pool_proposal"):
            return ["An environment is created with its Spark pool; this one has no proposed pool (the compute is not sized)."]
        return []

    def apply(self, ctx, step):
        wid = _need_ws(ctx)
        live = self.find(ctx, step)
        if live:
            return Outcome(live, "already exists (found immediately before creating); not created", created=False)
        env = FabricEnvironmentProposal.model_validate(step.spec["environment"])
        pool = FabricPoolProposal.model_validate(step.spec["pool_proposal"])
        pool_id = ctx.created.get(rq.rid(T.spark_pool, pool.name))  # set for created AND reused pools
        if not pool_id:
            raise ProvisionError(f"The Spark pool '{pool.name}' this environment uses has no id (it was not created or found).")
        definition = compute_deploy.environment_definition(env, pool, pool_id)
        r = _write(ctx, "POST", f"workspaces/{wid}/environments",
                   {"displayName": step.name, "description": "Created by adf2fabric", "definition": definition}, "Creating the environment")
        if r.status == 202:
            ctx.fab.wait_operation(r, "Creating the environment")
        elif r.status not in (200, 201):
            raise ctx.fab.error(r, "Creating the environment")
        return Outcome((str((r.body or {}).get("id") or "") if isinstance(r.body, dict) else "") or self.find(ctx, step),
                       "created (not published: publishing runs a Spark job and is a separate step)")


# =====================================================================================================================
# Delegated: the write belongs to another approval-gated command
# =====================================================================================================================


class _DelegatedHandler(Handler):
    command = ""
    item_type = ""

    def plan(self, ctx, req):
        ep = f"delegated to `{self.command}`"
        why = ctx.children_readable("items") if self.item_type else ctx.state.unread.get("connections")
        if why:
            return _step(req, Action.unknown, Api.delegated, endpoint=ep, delegated_to=self.command, blockers=[f"Existing state could not be read ({why})."])
        items = ctx.state.items.get(self.item_type, []) if self.item_type else ctx.state.connections
        ex = _by_name(items, req.name)
        if ex is not None:
            return _step(req, Action.reuse, Api.delegated, endpoint=ep, existing=ex, delegated_to=self.command)
        notes = [f"Created by `{self.command}`, not by provisioning."]
        if req.spec.get("approved") is False:
            notes.append("Its workload is not approved yet (workloads-approve).")
        return _step(req, Action.delegate, Api.delegated, endpoint=ep, delegated_to=self.command, notes=notes)

    def find(self, ctx, step):
        if self.item_type:
            for i in ctx.fab.paged(f"workspaces/{_need_ws(ctx)}/items?type={self.item_type}", "listing items"):
                if str(i.get("displayName", "")).lower() == step.name.lower():
                    return str(i["id"])
            return None
        for c in ctx.fab.paged("connections", "listing connections"):
            if str(c.get("displayName", "")).lower() == step.name.lower():
                return str(c["id"])
        return None

    def apply(self, ctx, step):  # pragma: no cover - never reached: delegated steps are not applied here
        raise ProvisionError(f"This step is delegated to `{self.command}`.")


class ConnectionHandler(_DelegatedHandler):
    type, command, item_type = T.connection, "connections apply (credentials are needed)", ""


class NotebookHandler(_DelegatedHandler):
    type, command, item_type = T.notebook, "workloads-deploy", "Notebook"


class SparkJobHandler(_DelegatedHandler):
    type, command, item_type = T.spark_job_definition, "workloads-deploy", "SparkJobDefinition"


HANDLERS: dict[T, Handler] = {h.type: h for h in (AzureCapacityHandler(), WorkspaceHandler(), CapacityAssignmentHandler(), IdentityHandler(),
                                                   RoleAssignmentHandler(), LakehouseHandler(), WarehouseHandler(), PoolHandler(),
                                                   EnvironmentHandler(), ConnectionHandler(), NotebookHandler(), SparkJobHandler())}
