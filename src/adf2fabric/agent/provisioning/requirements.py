"""What the migration actually needs in the target, derived from the plans that already exist.

Inputs: the compute plan (pools and environments, with peak vCores), the workload manifest (notebooks / Spark job definitions
that were generated, and what they require), the dependency inventory (linked services that need a Fabric connection) and the
user's ``TargetSpec`` (where it lands). Only resources the migration needs become requirements: a project without Spark
compute gets no pool, one without generated workloads gets no notebook.

Nothing here talks to a service. A requirement says WHAT and WHY, never whether it exists.
"""

from __future__ import annotations

from typing import Any, Optional

from adf2fabric.connections.service import _name as connection_name
from adf2fabric.project.compute import ComputePlan
from adf2fabric.project.dependencies import DependencyInventory, MigrationCategory
from adf2fabric.project.provisioning import Requirement, ResourceType as T, TargetSpec
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore
from adf2fabric.project.workloads import WorkloadManifest

FABRIC_SKU_CU = (2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048)  # F2 .. F2048: the SKU's number is its capacity units
UNNAMED_LAKEHOUSE = "(name not chosen)"


def rid(type_: T, name: str) -> str:
    return f"{type_.value}:{name.strip().lower()}"


def sku_units(sku: Optional[str]) -> Optional[int]:
    """Capacity units of an F SKU name (F64 -> 64); None for anything else (trial, P SKUs)."""
    s = (sku or "").upper()
    return int(s[1:]) if s.startswith("F") and s[1:].isdigit() else None


def recommended_sku(units: float) -> str:
    return next((f"F{u}" for u in FABRIC_SKU_CU if u >= units), f"F{FABRIC_SKU_CU[-1]}")


def load_compute_plan(store: ProjectStore, pid: str) -> Optional[ComputePlan]:
    path = store.conversion_dir(pid) / "compute-plan.json"
    try:
        if path.is_file():
            return ComputePlan.model_validate_json(path.read_text(encoding="utf-8"))
        return store.load_conversion(pid).compute_plan
    except (ProjectNotFoundError, ValueError):
        return None


class Inputs:
    def __init__(self, compute: Optional[ComputePlan], workloads: Optional[WorkloadManifest], inventory: Optional[DependencyInventory]):
        self.compute, self.workloads, self.inventory = compute, workloads, inventory


def load_inputs(store: ProjectStore, pid: str) -> Inputs:
    wl = store.load_workload_manifest(pid) if store.has_workload_manifest(pid) else None
    inv = store.load_dependency_inventory(pid) if store.has_dependency_inventory(pid) else None
    return Inputs(load_compute_plan(store, pid), wl, inv)


def estimates(inputs: Inputs) -> dict[str, Any]:
    c = inputs.compute
    if c is None or not c.pools:
        return {}
    proposed = [p for p in c.pools if p.status == "proposed"]
    peak = max((p.peak_spark_vcores or 0 for p in proposed), default=0)
    total = sum(p.peak_spark_vcores or 0 for p in proposed)
    out: dict[str, Any] = {"pools": len(proposed), "pools_needing_input": sum(1 for p in c.pools if p.status != "proposed"),
                           "largest_pool_spark_vcores": peak, "largest_pool_capacity_units": peak / 2,
                           "sum_of_pool_peaks_capacity_units": total / 2,
                           "basis": "1 capacity unit = 2 Spark vCores (Microsoft's Spark compute documentation); pools rarely peak together, "
                                    "so the largest pool is the least capacity that can run any one workload at peak."}
    if peak:
        out["minimum_sku_for_largest_pool"] = recommended_sku(peak / 2)
    return out


def build_requirements(spec: TargetSpec, inputs: Inputs) -> tuple[list[Requirement], list[str]]:
    """(requirements in no particular order, notes). Dependencies between them are explicit in ``depends_on``."""
    reqs: dict[str, Requirement] = {}
    notes: list[str] = []

    def add(type_: T, name: str, reason: str, needed_by=(), spec_=None, depends=()) -> Requirement:
        key = rid(type_, name)
        r = reqs.get(key)
        if r is None:
            r = reqs[key] = Requirement(id=key, type=type_, name=name, reason=reason, spec=dict(spec_ or {}), depends_on=list(depends))
        for n in needed_by:
            if n not in r.needed_by:
                r.needed_by.append(n)
        return r

    ws_name = spec.workspace_name or spec.workspace_id or "(workspace not chosen)"
    cap = None
    if spec.create_capacity:
        cap = add(T.azure_capacity, spec.azure_capacity_name or "(capacity name not chosen)",
                  "A new Fabric capacity was requested for the target workspace.",
                  spec_={"subscription": spec.azure_subscription_id, "resource_group": spec.azure_resource_group, "location": spec.azure_region,
                         "sku": spec.azure_sku, "admins": list(spec.capacity_admins)})
    ws = add(T.workspace, ws_name, "Holds every migrated item.",
             spec_={"id": spec.workspace_id, "capacity_id": spec.capacity_id, "capacity_name": spec.capacity_name},
             depends=[cap.id] if cap else [])
    assign = add(T.capacity_assignment, ws_name, "A workspace needs a Fabric capacity to hold Fabric items.",
                 spec_={"capacity_id": spec.capacity_id, "capacity_name": spec.capacity_name or spec.azure_capacity_name},
                 depends=[ws.id] + ([cap.id] if cap else []))
    base = [ws.id, assign.id]
    if spec.provision_workspace_identity:
        add(T.workspace_identity, ws_name, "Requested: a workspace identity lets the workspace authenticate to data sources.", depends=base)
    for ra in spec.role_assignments:
        add(T.role_assignment, f"{ra.get('principal_id')}:{ra.get('role')}", f"Requested: {ra.get('role')} for {ra.get('principal_type')} {ra.get('principal_id')}.",
            spec_=dict(ra), depends=[ws.id])

    c = inputs.compute
    pool_ids: dict[str, str] = {}
    if c is not None:
        env_pool = {e.name: e.pool for e in c.environments}
        for p in c.pools:
            if p.status != "proposed" or not p.request_body:
                notes.append(f"Spark pool '{p.name}' is not provisioned: its status is '{p.status}' (the source compute is not sized or has no Fabric equivalent).")
                continue
            used_by = sorted({t.pipeline + "/" + t.activity for t in c.targets if t.spark_pool == p.name})
            r = add(T.spark_pool, p.name, "Replaces source Spark compute: " + ", ".join(p.replaces) + ".", used_by,
                    {"request_body": p.request_body, "peak_spark_vcores": p.peak_spark_vcores}, depends=base)
            pool_ids[p.name] = r.id
        for e in c.environments:
            pool = env_pool.get(e.name)
            pp = next((p for p in c.pools if p.name == pool), None)
            used_by = sorted({t.pipeline + "/" + t.activity for t in c.targets if t.environment == e.name})
            add(T.environment, e.name, "Runtime, Spark properties and libraries for the migrated compute.", used_by,
                {"runtime_version": e.runtime_version, "pool_name": pool, "libraries": list(e.libraries),
                 "environment": e.model_dump(mode="json"), "pool_proposal": pp.model_dump(mode="json") if pp is not None else None},
                depends=base + ([pool_ids[pool]] if pool in pool_ids else []))

    lh_needed_by: list[str] = []
    wm = inputs.workloads
    if wm is not None:
        for w in wm.workloads:
            if spec.pipelines and not any(u.split("/", 1)[0].lower() in {p.lower() for p in spec.pipelines} for u in w.used_by):
                continue
            if any(r.kind == "lakehouse" for r in w.required_resources):
                lh_needed_by.append(w.source_item)
            if w.artifact.kind == "fabric_notebook" and w.artifact.display_name:
                add(T.notebook, w.artifact.display_name, "A generated Fabric notebook for a migrated workload.", [w.source_item],
                    {"approved": w.approval.status == "approved"}, depends=base)
            elif w.artifact.kind == "fabric_spark_job_definition" and w.artifact.display_name:
                add(T.spark_job_definition, w.artifact.display_name, "A generated Spark job definition for a migrated workload.", [w.source_item],
                    {"approved": w.approval.status == "approved"}, depends=base)
            if any(r.kind == "shortcut" for r in w.required_resources):
                notes.append(f"'{w.source_item}' needs OneLake shortcut(s) in place of mounts / DBFS; shortcuts need a lakehouse and a connection (create-shortcut).")
    names = list(spec.lakehouse_names)
    if not names and lh_needed_by:
        add(T.lakehouse, UNNAMED_LAKEHOUSE, "Migrated workloads read metastore tables or need a default lakehouse.", lh_needed_by,
            {"unnamed": True}, depends=base)
    for n in names:
        add(T.lakehouse, n, "Requested lakehouse" + (" (needed by migrated workloads)" if lh_needed_by else "") + ".", lh_needed_by,
            {"schemas": spec.lakehouse_schemas_enabled}, depends=base)
    for n in spec.warehouse_names:
        add(T.warehouse, n, "Requested warehouse.", depends=base)

    inv = inputs.inventory
    if inv is not None:
        for d in inv.dependencies:
            if d.type.value == "linked_service" and d.migration.category is MigrationCategory.needs_connection:
                add(T.connection, connection_name(d.name), f"A Fabric connection is needed for linked service '{d.name}'.", [d.name],
                    {"linked_service": d.name}, depends=[ws.id])
    return list(reqs.values()), notes
