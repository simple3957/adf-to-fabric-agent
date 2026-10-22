"""Build the provisioning plan: pre-flight checks, one step per requirement, dependency order, blockers, estimates, hash.

Planning reads (state discovery, plus read-only checks against Azure Resource Manager); it never writes. The only POST it
sends is ARM's ``checkNameAvailability``, which Microsoft documents as a query that creates nothing.

A check that cannot be settled without creating something (can this account create workspaces?) is ``unknown`` and says so:
it is neither passed nor failed, and the refusal, if there is one, is reported when the step is applied.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Optional

from adf2fabric.agent.provisioning import requirements as rq
from adf2fabric.agent.provisioning.handlers import HANDLERS, PENDING_CAPACITY, Ctx, resolve_capacity
from adf2fabric.agent.provisioning.state import read_state
from adf2fabric.agent.provisioning.transport import ArmGateway, FabricGateway, ProvisionError, token_tenant
from adf2fabric.project.provisioning import (Action, Check, CheckStatus as CS, PlanSummary, ProvisioningPlan, ProvisionStep, ResourceType as T,
                                             TargetSpec, TargetState)

_RANK = [T.azure_capacity, T.workspace, T.capacity_assignment, T.workspace_identity, T.role_assignment, T.spark_pool, T.environment,
         T.lakehouse, T.warehouse, T.connection, T.notebook, T.spark_job_definition, T.shortcut]
ARM_API = "2023-11-01"


def plan_hash(steps: list[ProvisionStep]) -> str:
    canon = [[s.id, s.type.value, s.name, s.action.value, s.request] for s in steps]
    return hashlib.sha256(json.dumps(canon, sort_keys=True, default=str).encode()).hexdigest()


def order(steps: list[ProvisionStep]) -> list[ProvisionStep]:
    """Dependencies first (Kahn), ties broken by resource type then name so the order is stable. A cycle is impossible by construction."""
    by = {s.id: s for s in steps}
    done: list[ProvisionStep] = []
    placed: set[str] = set()
    pending = sorted(steps, key=lambda s: (_RANK.index(s.type) if s.type in _RANK else 99, s.name.lower()))
    while pending:
        nxt = next((s for s in pending if all(d in placed or d not in by for d in s.depends_on)), None)
        if nxt is None:  # defensive: place the rest as they are rather than loop forever
            nxt = pending[0]
        pending.remove(nxt)
        placed.add(nxt.id)
        done.append(nxt)
    return done


def _propagate(steps: list[ProvisionStep]) -> None:
    by = {s.id: s for s in steps}
    for s in steps:  # already in dependency order
        for d in s.depends_on:
            dep = by.get(d)
            if dep is None:
                continue
            if dep.action in (Action.blocked, Action.conflict):
                s.blockers.append(f"Depends on {dep.type.value} '{dep.name}', which is {dep.action.value}.")
                if s.action not in (Action.blocked, Action.conflict):
                    s.action, s.requires_approval = Action.blocked, False
            elif dep.action is Action.unknown and s.action not in (Action.blocked, Action.conflict, Action.unknown):
                s.blockers.append(f"Depends on {dep.type.value} '{dep.name}', whose state could not be read.")
                s.action, s.requires_approval = Action.unknown, False


def _summarize(steps: list[ProvisionStep]) -> PlanSummary:
    s = PlanSummary(total=len(steps))
    for st in steps:
        s.by_action[st.action.value] = s.by_action.get(st.action.value, 0) + 1
        s.by_type[st.type.value] = s.by_type.get(st.type.value, 0) + 1
    s.to_create, s.to_configure = s.by_action.get("create", 0), s.by_action.get("configure", 0)
    s.reused, s.delegated = s.by_action.get("reuse", 0), s.by_action.get("delegate", 0)
    s.blocked, s.conflicts, s.unknown = s.by_action.get("blocked", 0), s.by_action.get("conflict", 0), s.by_action.get("unknown", 0)
    s.by_action, s.by_type = dict(sorted(s.by_action.items())), dict(sorted(s.by_type.items()))
    return s


# =====================================================================================================================
# pre-flight checks (read-only)
# =====================================================================================================================


def _chk(name: str, status: CS, detail: str = "", proved: str = "", not_proved: str = "", action: str = "", blocking: bool = False) -> Check:
    return Check(name=name, status=status, detail=detail, proved=proved, not_proved=not_proved, action=action, blocking=blocking and status is CS.failed)


def preflight(ctx: Ctx, spec: TargetSpec, estimate: dict[str, Any], steps_needed: set[T]) -> list[Check]:
    out: list[Check] = []
    st = ctx.state
    if "capacities" in st.unread:
        out.append(_chk("fabric_access", CS.failed, st.unread["capacities"], action="Sign in to Fabric with an account that can use the Fabric APIs.", blocking=True))
    else:
        out.append(_chk("fabric_access", CS.passed, f"{len(st.capacities)} capacity(ies) visible", proved="the token is accepted by the Fabric API and can read capacities",
                        not_proved="permission to create anything"))
    try:
        tid = token_tenant(ctx.fab._token())
    except Exception:  # noqa: BLE001
        tid = None
    if spec.tenant_id:
        if tid is None:
            out.append(_chk("tenant", CS.unknown, "the token's tenant could not be read", not_proved="that the token is for the expected tenant"))
        elif tid.lower() != spec.tenant_id.lower():
            out.append(_chk("tenant", CS.failed, f"the Fabric token is for tenant {tid}, not the expected {spec.tenant_id}",
                            action="Sign in to the intended tenant (az login --tenant ...).", blocking=True))
        else:
            out.append(_chk("tenant", CS.passed, f"the Fabric token is for tenant {tid}", proved="the identity used belongs to the expected tenant"))
    else:
        out.append(_chk("tenant", CS.unknown, f"no expected tenant was given (--tenant-id); the Fabric token is for {tid or 'an unreadable tenant'}",
                        not_proved="that this is the tenant the migration should land in", action="Pass --tenant-id to make this a hard check."))
    cap_id, region, why = resolve_capacity(ctx)
    needs_cap = bool(steps_needed & {T.workspace, T.capacity_assignment})
    ws_cap = st.workspace.properties.get("capacityId") if st.workspace is not None else None
    if cap_id is None and ws_cap and why.startswith("no capacity was chosen"):
        cap_id, region, why = ws_cap, str(st.workspace.properties.get("capacityRegion") or ""), ""  # none requested: the workspace keeps its own
    if st.workspace is not None and ws_cap:
        needs_cap = False
    if cap_id:
        cap = next((c for c in st.capacities if (c.id or "") == cap_id), None)
        out.append(_chk("capacity", CS.passed, f"{cap.name if cap else cap_id}: {cap.properties.get('sku') if cap else ''} in {region or 'unknown region'}, Active",
                        proved="the capacity is visible to this account and Active", not_proved="that you may assign workspaces to it (checked when assigning)"))
        have = rq.sku_units(str(cap.properties.get("sku")) if cap else None)
        peak = estimate.get("largest_pool_capacity_units")
        if peak and have is not None and have < peak:
            out.append(_chk("capacity_size", CS.failed, f"the capacity has {have} capacity units; the largest planned pool needs {peak:g} at peak",
                            action=f"Use at least {estimate.get('minimum_sku_for_largest_pool')} or shrink the pool (compute-plan).", blocking=False))
        elif peak and have is not None:
            out.append(_chk("capacity_size", CS.passed, f"{have} capacity units cover the largest planned pool ({peak:g} at peak)",
                            not_proved="concurrent load from other items on the capacity"))
        elif peak:
            out.append(_chk("capacity_size", CS.unknown, "the capacity's size is not an F SKU number (trial or other), so it cannot be compared"))
    elif why == PENDING_CAPACITY:
        out.append(_chk("capacity", CS.unknown, "the capacity will be created by this plan; its Fabric id is known only afterwards"))
    elif needs_cap:
        out.append(_chk("capacity", CS.failed, why, action="Choose a capacity you administer or contribute to, or pass --create-capacity.", blocking=True))
    if T.workspace in steps_needed and st.workspace is None and "workspace" not in st.unread:
        out.append(_chk("workspace_creation", CS.unknown, "whether this account may create workspaces cannot be checked without creating one",
                        proved="workspaces can be listed", not_proved="the tenant setting 'Create workspaces' for this account",
                        action="If creation is refused, a Fabric admin must allow it (tenant setting) or an admin must create the workspace."))
    if st.workspace is not None:
        roles = st.workspace.properties.get("caller_roles") or []
        if roles:
            out.append(_chk("workspace_role", CS.passed, "your role(s): " + ", ".join(roles), proved="role read via the workspace list filter (no write)"))
        elif "role:Admin" in st.unread:
            out.append(_chk("workspace_role", CS.unknown, "your workspace role could not be read"))
        else:
            out.append(_chk("workspace_role", CS.failed, "you hold no role in this workspace", action="Ask a workspace Admin to add you as Contributor or higher.", blocking=True))
    return out


def arm_checks(arm: Optional[ArmGateway], spec: TargetSpec, ctx: Ctx) -> list[Check]:
    if not spec.create_capacity:
        return []
    out: list[Check] = []
    sub, loc = spec.azure_subscription_id, spec.azure_region
    if arm is None:
        return [_chk("azure_access", CS.failed, "no Azure Resource Manager credential was supplied", action="Sign in to Azure for the subscription that will own the capacity.", blocking=True)]
    if not sub:
        return [_chk("azure_subscription", CS.failed, "no subscription id was given", blocking=True)]

    def get(path: str, what: str):
        try:
            return arm.request("GET", path)
        except ProvisionError as exc:
            out.append(_chk(what, CS.unknown, str(exc)))
            return None

    r = get(f"subscriptions/{sub}?api-version=2022-12-01", "azure_subscription")
    if r is not None:
        if r.status == 200:
            tenant = (r.body or {}).get("tenantId")
            out.append(_chk("azure_subscription", CS.passed, f"subscription {(r.body or {}).get('displayName') or sub} ({(r.body or {}).get('state')})",
                            proved="the subscription is readable with this credential" + (f"; its tenant is {tenant}" if tenant else "")))
            if spec.tenant_id and tenant and str(tenant).lower() != spec.tenant_id.lower():
                out.append(_chk("azure_tenant", CS.failed, f"the subscription belongs to tenant {tenant}, not {spec.tenant_id}", blocking=True,
                                action="A Fabric capacity must be created in the tenant the workspace lives in."))
            if str((r.body or {}).get("state", "Enabled")).lower() != "enabled":
                out.append(_chk("azure_subscription_state", CS.failed, f"the subscription is {(r.body or {}).get('state')}", blocking=True))
        else:
            out.append(_chk("azure_subscription", CS.failed, f"reading the subscription: HTTP {r.status}" + (" (no access)" if r.status in (401, 403, 404) else ""),
                            action="Use an account with at least Reader on the subscription.", blocking=True))
    r = get(f"subscriptions/{sub}/providers/Microsoft.Fabric?api-version=2021-04-01", "resource_provider")
    if r is not None:
        state = str((r.body or {}).get("registrationState", "")) if r.status == 200 else ""
        if r.status == 200 and state.lower() == "registered":
            out.append(_chk("resource_provider", CS.passed, "Microsoft.Fabric is registered", proved="the resource provider is registered on the subscription"))
        elif r.status == 200:
            out.append(_chk("resource_provider", CS.failed, f"Microsoft.Fabric is {state or 'not registered'}", blocking=True,
                            action="Register it: az provider register --namespace Microsoft.Fabric (needs Contributor on the subscription)."))
        else:
            out.append(_chk("resource_provider", CS.unknown, f"HTTP {r.status} reading the provider registration"))
    r = get(f"subscriptions/{sub}/providers/Microsoft.Fabric/skus?api-version={ARM_API}", "sku_availability")
    if r is not None and r.status == 200:
        ctx.sku_regions = {str(x.get("name", "")).upper(): set(x.get("locations") or []) for x in (r.body or {}).get("value", []) if isinstance(x, dict)}
        out.append(_chk("sku_availability", CS.passed, f"{len(ctx.sku_regions)} SKU(s) listed for this subscription", proved="SKU/region offers were read from Azure"))
    elif r is not None:
        out.append(_chk("sku_availability", CS.unknown, f"HTTP {r.status} listing SKUs; the region/SKU pairing is not checked"))
    if loc:
        r = get(f"subscriptions/{sub}/providers/Microsoft.Fabric/locations/{loc}/usages?api-version={ARM_API}", "quota")
        out.append(_quota_check(r, rq.sku_units(spec.azure_sku)))
        if spec.azure_capacity_name:
            try:
                n = arm.request("POST", f"subscriptions/{sub}/providers/Microsoft.Fabric/locations/{loc}/checkNameAvailability?api-version={ARM_API}",
                                {"name": spec.azure_capacity_name, "type": "Microsoft.Fabric/capacities"}, ambiguous_retry=False)
                avail = (n.body or {}).get("nameAvailable") if n.status == 200 else None
                existing = bool(ctx.state.azure_capacities)
                if avail is True or existing:
                    out.append(_chk("capacity_name", CS.passed, "the name is available" if avail else "the capacity already exists in this plan's resource group",
                                    proved="Azure's name-availability query (creates nothing)"))
                elif avail is False:
                    out.append(_chk("capacity_name", CS.failed, f"the capacity name is not available: {(n.body or {}).get('reason')}", blocking=True, action="Choose another name."))
                else:
                    out.append(_chk("capacity_name", CS.unknown, f"HTTP {n.status} checking the name"))
            except ProvisionError as exc:
                out.append(_chk("capacity_name", CS.unknown, str(exc)))
    return out


def _quota_check(r, need: Optional[int]) -> Check:
    if r is None or r.status != 200:
        return _chk("quota", CS.unknown, "the Fabric capacity quota could not be read" + (f" (HTTP {r.status})" if r is not None else ""),
                    not_proved="that the subscription has capacity units left in this region", action="Check Fabric quotas in the Azure portal (Quotas).")
    rows = [x for x in (r.body or {}).get("value", []) if isinstance(x, dict)]
    row = next((x for x in rows if "quota" in json.dumps(x.get("name", "")).lower()), rows[0] if rows else None)
    try:
        cur, lim = float((row or {}).get("currentValue")), float((row or {}).get("limit"))
    except (TypeError, ValueError):
        return _chk("quota", CS.unknown, "the quota response was not in a recognised shape", not_proved="remaining quota")
    left = lim - cur
    if need is not None and left < need:
        return _chk("quota", CS.failed, f"{left:g} capacity units left in the region; {need} needed", blocking=True,
                    action="Request a quota increase (Quota Request Operator role) or pick another region.")
    return _chk("quota", CS.passed, f"{left:g} of {lim:g} capacity units left in the region", proved="regional quota was read from Azure")


# =====================================================================================================================
# the plan
# =====================================================================================================================


def make_plan(store, project_id: str, spec: TargetSpec, fab: FabricGateway, arm: Optional[ArmGateway] = None, now: Optional[datetime] = None,
              sleep=None) -> ProvisioningPlan:
    return make_plan_and_state(store, project_id, spec, fab, arm, now, sleep)[0]


def make_plan_and_state(store, project_id: str, spec: TargetSpec, fab: FabricGateway, arm: Optional[ArmGateway] = None,
                        now: Optional[datetime] = None, sleep=None) -> tuple[ProvisioningPlan, TargetState]:
    now = now or datetime.now(timezone.utc)
    inputs = rq.load_inputs(store, project_id)
    reqs, notes = rq.build_requirements(spec, inputs)
    state = read_state(fab, arm, spec, now)
    ctx = Ctx(spec=spec, state=state, fab=fab, arm=arm, sleep=sleep)
    est = rq.estimates(inputs)
    checks = preflight(ctx, spec, est, {r.type for r in reqs})
    checks += arm_checks(arm, spec, ctx)  # fills ctx.sku_regions, which the capacity step's plan then uses
    steps = [HANDLERS[r.type].plan(ctx, r) for r in reqs]
    steps = order(steps)
    _propagate(steps)
    blockers = [f"{c.name}: {c.detail}" for c in checks if c.blocking]
    for s in steps:
        if s.action is Action.blocked:
            blockers += [f"{s.type.value} '{s.name}': {b}" for b in s.blockers[:2]]
    if not est:
        notes.append("No Spark compute was found in the compute plan, so no capacity size is estimated.")
    if not any(s.type in (T.spark_pool, T.environment) for s in steps) and inputs.compute is None:
        notes.append("No compute plan exists for this project (run convert); pools and environments are not part of this plan.")
    for u, why in state.unread.items():
        if not u.startswith("role:"):
            notes.append(f"Could not read '{u}': {why}. Steps that depend on it are 'unknown', never assumed absent.")
    plan = ProvisioningPlan(project_id=project_id, created_at=now, target=spec, state_read_at=state.read_at, checks=checks, requirements=reqs, steps=steps,
                            blockers=list(dict.fromkeys(blockers)), summary=_summarize(steps), estimates=est, notes=notes)
    plan.plan_hash = plan_hash(steps)
    return plan, state
