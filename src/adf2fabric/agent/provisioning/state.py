"""Read what exists in the target. Read-only: this module only issues GET requests (a test enforces it).

A read that fails is recorded in ``TargetState.unread`` with its reason and is NEVER treated as "absent": planning an action
on a resource whose state could not be read would risk a duplicate or an overwrite, so the planner turns it into ``unknown``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from adf2fabric.agent.provisioning.transport import ArmGateway, FabricGateway, ProvisionError
from adf2fabric.project.provisioning import Existence, ExistingResource, TargetSpec, TargetState

ARM_CAPACITY_API = "2023-11-01"
ITEM_TYPES = ("Lakehouse", "Warehouse", "Environment", "Notebook", "SparkJobDefinition")
ROLES = ("Admin", "Member", "Contributor", "Viewer")


def _res(d: dict, now: datetime, name_key: str = "displayName", **props: Any) -> ExistingResource:
    keep = {k: v for k, v in d.items() if k in ("type", "capacityId", "capacityRegion", "sku", "region", "state", "description", "role",
                                                  "workspaceId", "connectivityType", "nodeFamily", "nodeSize", "autoScale",
                                                  "dynamicExecutorAllocation", "capacityAssignmentProgress") and isinstance(v, (str, int, bool, dict))}
    keep.update(props)
    return ExistingResource(id=str(d.get("id")) if d.get("id") else None, name=str(d.get(name_key) or d.get("name") or ""), properties=keep, read_at=now)


def arm_capacity_path(spec: TargetSpec) -> Optional[str]:
    if not (spec.azure_subscription_id and spec.azure_resource_group and spec.azure_capacity_name):
        return None
    return (f"subscriptions/{spec.azure_subscription_id}/resourceGroups/{spec.azure_resource_group}/providers/Microsoft.Fabric/"
            f"capacities/{spec.azure_capacity_name}?api-version={ARM_CAPACITY_API}")


def find_workspace(fab: FabricGateway, spec: TargetSpec) -> tuple[Optional[dict], Optional[str]]:
    """(the workspace record, an unread reason). Not found is (None, None)."""
    try:
        if spec.workspace_id:
            r = fab.request("GET", f"workspaces/{spec.workspace_id}")
            if r.status == 200 and isinstance(r.body, dict):
                return r.body, None
            if r.status == 404:
                return None, None
            return None, f"reading workspace {spec.workspace_id}: HTTP {r.status}"
        if spec.workspace_name:
            for w in fab.paged("workspaces", "listing workspaces"):
                if str(w.get("displayName", "")).lower() == spec.workspace_name.lower() and w.get("type") != "Personal":
                    full = fab.request("GET", f"workspaces/{w['id']}")
                    return (full.body if full.status == 200 and isinstance(full.body, dict) else w), None
            return None, None
    except ProvisionError as exc:
        return None, str(exc)
    return None, None


def read_state(fab: FabricGateway, arm: Optional[ArmGateway], spec: TargetSpec, now: Optional[datetime] = None) -> TargetState:
    now = now or datetime.now(timezone.utc)
    st = TargetState(read_at=now)

    def soft(key: str, fn):
        try:
            return fn()
        except ProvisionError as exc:
            st.unread[key] = str(exc)
            return None

    caps = soft("capacities", lambda: fab.paged("capacities", "listing capacities"))
    st.capacities = [_res(c, now) for c in caps or []]

    ws, why = find_workspace(fab, spec)
    if why:
        st.unread["workspace"] = why
    if ws is not None:
        roles: list[str] = []
        for role in ROLES:  # proves the caller's role without writing anything
            listed = soft(f"role:{role}", lambda role=role: fab.paged(f"workspaces?roles={role}", "listing workspaces by role"))
            if listed and any(w.get("id") == ws.get("id") for w in listed):
                roles.append(role)
        st.workspace = _res(ws, now, caller_roles=roles, has_identity=bool(ws.get("workspaceIdentity")),
                            capacity_progress=str(ws.get("capacityAssignmentProgress") or ""))
        wid = str(ws["id"])
        items = soft("items", lambda: fab.paged(f"workspaces/{wid}/items", "listing workspace items"))
        for t in ITEM_TYPES:
            st.items[t] = [_res(i, now) for i in items or [] if i.get("type") == t]
        pools = soft("pools", lambda: fab.paged(f"workspaces/{wid}/spark/pools", "listing Spark pools"))
        st.pools = [ExistingResource(id=str(p.get("id") or ""), name=str(p.get("name", "")), properties={k: v for k, v in p.items() if k != "id"}, read_at=now)
                    for p in pools or []]
        ras = soft("role_assignments", lambda: fab.paged(f"workspaces/{wid}/roleAssignments", "listing role assignments"))
        st.role_assignments = [ExistingResource(id=str(r.get("id") or ""), name=str((r.get("principal") or {}).get("id", "")),
                                                properties={"role": r.get("role"), "type": (r.get("principal") or {}).get("type")}, read_at=now)
                               for r in ras or []]
    conns = soft("connections", lambda: fab.paged("connections", "listing connections"))
    st.connections = [_res(c, now) for c in conns or []]

    path = arm_capacity_path(spec)
    if path is not None:
        if arm is None:
            st.unread["azure_capacity"] = "no Azure Resource Manager credential was supplied"
        else:
            try:
                r = arm.request("GET", path)
                if r.status == 200 and isinstance(r.body, dict):
                    props = r.body.get("properties") or {}
                    st.azure_capacities.append(ExistingResource(
                        id=str(r.body.get("id") or ""), name=str(r.body.get("name") or spec.azure_capacity_name), read_at=now,
                        properties={"sku": (r.body.get("sku") or {}).get("name"), "location": r.body.get("location"),
                                    "provisioningState": props.get("provisioningState"), "state": props.get("state")}))
                elif r.status != 404:
                    st.unread["azure_capacity"] = f"reading the Azure capacity: HTTP {r.status}" + (" (permission denied)" if r.status == 403 else "")
            except ProvisionError as exc:
                st.unread["azure_capacity"] = str(exc)
    return st
