"""Create the Fabric Spark pools and Environments a compute plan proposes - only the ones the user names.

Like pipeline deployment, this is a write stage kept apart from conversion, and conservative:

* Existing pools are reused before anything is created: a pool with identical settings is reused whatever it is called, and
  with ``reuse_compatible`` one with the same node size whose node range covers the proposal's is reused too.
* Dry run unless ``apply``. A dry run only lists the workspace's existing pools and environments.
* ``apply`` creates exactly what was selected. Nothing is created that was not named, nothing is ever modified
  or deleted: a pool or environment that already exists is left alone (reported ``exists``, or ``conflict`` when a
  same-named pool has different settings).
* A pool is refused when its request body breaks a documented constraint (the plan file may be edited by hand).
* An environment needs its pool: it is refused unless the pool was selected too or already exists, and a
  proposal that still ``needs_input`` is never created.
* Publishing an environment (which runs as a Spark job and uses capacity) is a separate opt-in.

Endpoints (Microsoft Fabric REST; verified 2026-10-14, see ``docs/compute-plan.md``):
  GET  /v1/workspaces/{ws}/spark/pools                          List Workspace Custom Pools (continuationToken)
  POST /v1/workspaces/{ws}/spark/pools                          Create Workspace Custom Pool (201; workspace Admin)
  GET  /v1/workspaces/{ws}/environments                         List Environments (continuationToken)
  POST /v1/workspaces/{ws}/environments                         Create Environment (201 or 202 LRO; definition parts)
  POST /v1/workspaces/{ws}/environments/{id}/staging/publish?beta=false   Publish Environment (200 or 202 LRO)
"""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from typing import Any, Optional

import yaml

from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, DeployError, FabricClient, _GUID, _error_text
from adf2fabric.project.compute import ComputePlan, FabricEnvironmentProposal, FabricPoolProposal

NODE_SIZES = {"Small": 4, "Medium": 8, "Large": 16, "XLarge": 32, "XXLarge": 64}
# Sparkcompute.yml allows driver/executor memory 28g, 56g, 112g, 224g, 400g for cores 4, 8, 16, 32, 64. The pairing is by
# position in those two documented lists (only 4 cores / 28g appears in Microsoft's example).
CORES_TO_MEMORY = {4: "28g", 8: "56g", 16: "112g", 32: "224g", 64: "400g"}
_POOL_NAME = re.compile(r"^[A-Za-z0-9_\- ]{1,64}$")
_PLACEHOLDER_ID = "00000000-0000-0000-0000-000000000000"


@dataclass
class ComputeResult:
    kind: str  # pool | environment | publish
    name: str
    action: str  # create | exists | conflict | not_selected | skipped | refused | failed | publish
    item_id: Optional[str] = None
    detail: str = ""


# --- validation and request building ---------------------------------------------------------------------------------


def validate_pool_body(body: Optional[dict]) -> list[str]:
    """Reasons a pool request body would be rejected, from the documented Create Workspace Custom Pool schema."""
    if not isinstance(body, dict):
        return ["it has no request body (the compute is not sized)"]
    problems: list[str] = []
    name = body.get("name")
    if not isinstance(name, str) or not _POOL_NAME.match(name):
        problems.append("name must be 1-64 characters: letters, numbers, dashes, underscores and spaces")
    elif name.lower() == "starter pool":
        problems.append("'Starter Pool' is a reserved name")
    if body.get("nodeFamily") != "MemoryOptimized":
        problems.append("nodeFamily must be MemoryOptimized")
    if body.get("nodeSize") not in NODE_SIZES:
        problems.append("nodeSize must be one of " + ", ".join(NODE_SIZES))
    for key, lo, hi in (("autoScale", "minNodeCount", "maxNodeCount"),
                        ("dynamicExecutorAllocation", "minExecutors", "maxExecutors")):
        block = body.get(key)
        if not isinstance(block, dict) or not isinstance(block.get("enabled"), bool):
            problems.append(f"{key}.enabled is required (true or false)")
            continue
        a, b = block.get(lo), block.get(hi)
        if not all(isinstance(v, int) and not isinstance(v, bool) and v >= 1 for v in (a, b)):
            problems.append(f"{key}.{lo} and {key}.{hi} must be integers of at least 1")
        elif a > b:
            problems.append(f"{key}.{lo} is greater than {key}.{hi}")
    return problems


def _executors(pool_: dict) -> Any:
    """Dynamic executor allocation as compared. Fabric returns only ``{"enabled": false}`` for a disabled one (live,
    2026-10-27), dropping the min/max it was created with: when off, the limits do not apply and are not compared."""
    d = pool_.get("dynamicExecutorAllocation")
    return {"enabled": False} if isinstance(d, dict) and d.get("enabled") is False else d


def pool_matches(existing: dict, body: dict) -> bool:
    """True when an existing pool already has the settings of the proposed body."""
    return (all(existing.get(k) == body.get(k) for k in ("nodeFamily", "nodeSize", "autoScale"))
            and _executors(existing) == _executors(body))


def find_reusable(existing: dict[str, dict], body: dict) -> tuple[Optional[dict], str]:
    """An existing pool that can stand in for the proposal: (pool, 'identical' | 'compatible'), else (None, '').

    Identical = the same four settings (any name). Compatible = the same family and node size, autoscale on, a node range
    that covers the proposal's, and dynamic executor allocation on; it may scale further than planned.
    """
    # The workspace's Starter Pool is Microsoft-managed and reserved; an environment points at a custom pool, so it is
    # never a stand-in (a null pool id already means "starter pool" in an environment).
    pools = sorted((p for p in existing.values() if p.get("name", "").lower() != "starter pool"),
                   key=lambda p: p.get("name", "").lower())
    for found in pools:
        if pool_matches(found, body):
            return found, "identical"
    want = body.get("autoScale") or {}
    for found in pools:
        have = found.get("autoScale") or {}
        if (found.get("nodeFamily") == body.get("nodeFamily") and found.get("nodeSize") == body.get("nodeSize")
                and have.get("enabled") and (found.get("dynamicExecutorAllocation") or {}).get("enabled")
                and isinstance(have.get("minNodeCount"), int) and isinstance(have.get("maxNodeCount"), int)
                and have["minNodeCount"] <= want.get("minNodeCount", 0) and have["maxNodeCount"] >= want.get("maxNodeCount", 0)):
            return found, "compatible"
    return None, ""


def _node_range(pool_: dict) -> str:
    a = pool_.get("autoScale") or {}
    return f"{a.get('minNodeCount')}-{a.get('maxNodeCount')} nodes"


def environment_definition(env: FabricEnvironmentProposal, pool: FabricPoolProposal, pool_id: str) -> dict:
    """The environment's definition parts (Sparkcompute.yml, and environment.yml for PyPI libraries)."""
    body = pool.request_body or {}
    cores = NODE_SIZES[body["nodeSize"]]
    dyn = body["dynamicExecutorAllocation"]
    compute: dict[str, Any] = {
        "enable_native_execution_engine": False,
        "instance_pool_id": pool_id,
        "driver_cores": cores, "driver_memory": CORES_TO_MEMORY[cores],
        "executor_cores": cores, "executor_memory": CORES_TO_MEMORY[cores],
        "dynamic_executor_allocation": {"enabled": dyn["enabled"], "min_executors": dyn["minExecutors"],
                                        "max_executors": dyn["maxExecutors"]},
    }
    if env.spark_properties:
        compute["spark_conf"] = dict(env.spark_properties)
    compute["runtime_version"] = str(env.runtime_version)  # quoted in the YAML: 1.3 / 2.0 are strings, not floats
    parts = [_part("Setting/Sparkcompute.yml", compute)]
    pip = [lib.split(":", 1)[1] for lib in env.libraries if lib.startswith("pypi:")]
    if pip:
        parts.append(_part("Libraries/PublicLibraries/environment.yml", {"dependencies": [{"pip": pip}]}))
    return {"parts": parts}


def _part(path: str, doc: dict) -> dict:
    text = yaml.safe_dump(doc, sort_keys=False, default_flow_style=False)
    return {"path": path, "payload": base64.b64encode(text.encode("utf-8")).decode("ascii"), "payloadType": "InlineBase64"}


# --- Fabric calls ----------------------------------------------------------------------------------------------------


def _paged(client: FabricClient, url: str, what: str) -> list[dict]:
    out: list[dict] = []
    token: Optional[str] = None
    while True:
        resp = client._call("GET", url + (f"?continuationToken={token}" if token else ""))
        if resp.status_code != 200:
            raise DeployError(f"Listing {what} failed (HTTP {resp.status_code}): {_error_text(resp)}")
        data = resp.json()
        out += data.get("value", [])
        token = data.get("continuationToken")
        if not token:
            return out


def list_pools(client: FabricClient, workspace: str) -> dict[str, dict]:
    return {p["name"].lower(): p for p in _paged(client, f"{FABRIC_API}/workspaces/{workspace}/spark/pools", "Spark pools")}


def list_environments(client: FabricClient, workspace: str) -> dict[str, str]:
    return {e["displayName"].lower(): e["id"]
            for e in _paged(client, f"{FABRIC_API}/workspaces/{workspace}/environments", "environments")}


def create_pool(client: FabricClient, workspace: str, body: dict) -> str:
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/spark/pools", body)
    if resp.status_code != 201:
        raise DeployError(f"Creating pool '{body['name']}' failed (HTTP {resp.status_code}): {_error_text(resp)}")
    pool_id = (resp.json() or {}).get("id") if resp.content else None
    if not pool_id:
        raise DeployError(f"Pool '{body['name']}' was created but Fabric returned no id; its environment cannot be linked.")
    return pool_id


def create_environment(client: FabricClient, workspace: str, name: str, description: str, definition: dict) -> str:
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/environments",
                        {"displayName": name, "description": description[:256], "definition": definition})
    body = client._finish(resp, f"Creating environment '{name}'") or {}
    env_id = body.get("id") or list_environments(client, workspace).get(name.lower())
    if not env_id:
        raise DeployError(f"Environment '{name}' was created but Fabric did not list it.")
    return env_id


def publish_environment(client: FabricClient, workspace: str, env_id: str, name: str) -> None:
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/environments/{env_id}/staging/publish?beta=false")
    client._finish(resp, f"Publishing environment '{name}'")


# --- the deploy ------------------------------------------------------------------------------------------------------


def deploy_compute(client: FabricClient, workspace: str, plan: ComputePlan, *, apply: bool,
                   pools: Optional[set[str]] = None, environments: Optional[set[str]] = None,
                   publish: bool = False, reuse_compatible: bool = False) -> list[ComputeResult]:
    """Plan (apply=False) or create (apply=True) the selected pools and environments. ``None`` selects everything.

    Names are matched case-insensitively. Per-item failures do not stop the rest.
    """
    if not _GUID.match(workspace or ""):
        raise DeployError("The workspace id must be a GUID (Fabric workspace settings > About).")
    sel_pools = {n.lower() for n in pools} if pools is not None else None
    sel_envs = {n.lower() for n in environments} if environments is not None else None
    existing_pools = list_pools(client, workspace)
    existing_envs = list_environments(client, workspace)
    pool_ids: dict[str, str] = {k: v["id"] for k, v in existing_pools.items()}  # name(lower) -> id
    by_name = {p.name.lower(): p for p in plan.pools}
    results: list[ComputeResult] = []
    unavailable: dict[str, str] = {}  # pool name(lower) -> why an environment cannot use it

    for pool in plan.pools:
        low = pool.name.lower()
        if pool.status != "proposed":
            results.append(ComputeResult("pool", pool.name, "skipped", detail={
                "needs_input": "its size is not known yet; supply it (see the plan) and convert again",
                "not_possible": "Fabric has no equivalent for this compute"}.get(pool.status, pool.status)))
            continue
        if sel_pools is not None and low not in sel_pools:
            results.append(ComputeResult("pool", pool.name, "not_selected", detail="not selected; nothing done"))
            continue
        problems = validate_pool_body(pool.request_body)
        if problems:
            unavailable[low] = "its request body is invalid"
            results.append(ComputeResult("pool", pool.name, "refused", detail="not created: " + "; ".join(problems)))
            continue
        body = pool.request_body or {}
        found = existing_pools.get(body["name"].lower())
        if found is not None and pool_matches(found, body):
            pool_ids[low] = found["id"]
            results.append(ComputeResult("pool", pool.name, "exists", found["id"],
                                         "a pool with this name and the same settings already exists; left unchanged"))
            continue
        other, kind = find_reusable(existing_pools, body)
        hint = ""
        if other is not None and (kind == "identical" or reuse_compatible):
            pool_ids[low] = other["id"]
            want = body["autoScale"]
            why = ("identical settings" if kind == "identical"
                   else f"same node size, and its {_node_range(other)} cover the proposed "
                        f"{want['minNodeCount']}-{want['maxNodeCount']}; it may scale further than planned")
            results.append(ComputeResult("pool", pool.name, "reuse", other["id"],
                                         f"reusing the existing pool '{other['name']}' ({why}); nothing created"))
            continue
        if other is not None:
            hint = (f" A compatible pool '{other['name']}' exists (same node size, {_node_range(other)}); "
                    "add --reuse to use it instead.")
        if found is not None:
            unavailable[low] = "a pool with that name exists with different settings"
            results.append(ComputeResult("pool", pool.name, "conflict", found["id"],
                                         "a pool with this name exists with different settings; not modified. "
                                         "Rename the proposal or change the existing pool yourself." + hint))
            continue
        if not apply:
            pool_ids[low] = _PLACEHOLDER_ID
            results.append(ComputeResult("pool", pool.name, "create", detail="dry run: would create it" + hint))
            continue
        try:
            pool_ids[low] = create_pool(client, workspace, body)
            results.append(ComputeResult("pool", pool.name, "create", pool_ids[low], "created" + hint))
        except DeployError as exc:
            unavailable[low] = "its creation failed"
            results.append(ComputeResult("pool", pool.name, "failed", detail=str(exc)))

    created_envs: list[tuple[str, str]] = []
    for env in plan.environments:
        if sel_envs is not None and env.name.lower() not in sel_envs:
            results.append(ComputeResult("environment", env.name, "not_selected", detail="not selected; nothing done"))
            continue
        plow = (env.pool or "").lower()
        pool = by_name.get(plow)
        if not env.pool or pool is None or pool.status != "proposed":
            results.append(ComputeResult("environment", env.name, "skipped",
                                         detail="it has no sized pool yet (see the plan's needs-input items)"))
            continue
        if plow in unavailable:
            results.append(ComputeResult("environment", env.name, "refused",
                                         detail=f"its pool '{env.pool}' is unavailable: {unavailable[plow]}"))
            continue
        if plow not in pool_ids:
            results.append(ComputeResult("environment", env.name, "refused",
                                         detail=f"its pool '{env.pool}' was not selected and is not in the workspace; "
                                                "select the pool too"))
            continue
        if env.name.lower() in existing_envs:
            results.append(ComputeResult("environment", env.name, "exists", existing_envs[env.name.lower()],
                                         "an environment with this name already exists; left unchanged"))
            continue
        if not apply:
            results.append(ComputeResult("environment", env.name, "create",
                                         detail="dry run: would create it" + (" and publish it" if publish else "")))
            continue
        try:
            definition = environment_definition(env, pool, pool_ids[plow])
            env_id = create_environment(client, workspace, env.name,
                                        f"Proposed by adf2fabric for {', '.join(env.replaces)}", definition)
            existing_envs[env.name.lower()] = env_id
            created_envs.append((env.name, env_id))
            results.append(ComputeResult("environment", env.name, "create", env_id,
                                         "created; publish it (--publish, or in the portal) to make its settings effective"))
        except DeployError as exc:
            results.append(ComputeResult("environment", env.name, "failed", detail=str(exc)))

    if publish and apply:
        for name, env_id in created_envs:  # one publish at a time: Fabric allows a single ongoing publish
            try:
                publish_environment(client, workspace, env_id, name)
                results.append(ComputeResult("publish", name, "publish", env_id, "published"))
            except DeployError as exc:
                results.append(ComputeResult("publish", name, "failed", env_id, str(exc)))
    return results


def record_results(plan: ComputePlan, results: list[ComputeResult]) -> ComputePlan:
    """A copy of the plan with the Fabric ids and each activity's provisioning status filled in from an applied run."""
    out = plan.model_copy(deep=True)
    status_of: dict[str, str] = {}
    for r in results:
        if r.item_id is None or r.item_id == _PLACEHOLDER_ID:
            continue
        if r.kind == "pool" and r.action in ("create", "exists", "reuse"):
            for p in out.pools:
                if p.name == r.name:
                    p.fabric_id = r.item_id
                    status_of[p.name] = "created" if r.action == "create" else "exists"
        elif r.kind == "environment" and r.action in ("create", "exists"):
            for e in out.environments:
                if e.name == r.name:
                    e.fabric_id = r.item_id
    for target in out.targets:
        if target.spark_pool in status_of:
            target.provisioning_status = status_of[target.spark_pool]
    return out
