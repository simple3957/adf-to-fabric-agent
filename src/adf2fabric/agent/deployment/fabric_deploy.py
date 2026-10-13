"""Push converted pipelines into a Fabric workspace through the Fabric REST API.

This is the only stage that writes to Fabric, so it is deliberately separate from conversion
(whose boundary is "never calls Azure or Fabric") and conservative:

* ``plan`` is read-only (it lists the workspace's data pipelines) and is the default.
* ``apply`` creates a pipeline or, when one with the same display name exists, overwrites its
  definition. It never deletes anything.
* A pipeline is refused while its JSON still holds unbound placeholders (``<connection:...>``)
  or failed structural validation, unless the caller explicitly allows unbound ones.
* Child pipelines are built first. An ``InvokePipeline`` holds ``<item:pipeline:Child>`` and
  ``<workspace-id>`` placeholders; the deploy orders pipelines so every child exists before its
  parent, then substitutes the child's real Fabric item id and the target workspace id. A parent
  whose child is neither selected nor already in the workspace (or whose child failed) is refused.
* The token comes from the signed-in ``az login`` session (azure-identity); it is never logged
  or stored.

Endpoints (Microsoft Fabric REST, DataPipeline + long-running operations):
  GET  /v1/workspaces/{ws}/dataPipelines                       (paged by continuationToken)
  POST /v1/workspaces/{ws}/dataPipelines                       (displayName + definition part)
  POST /v1/workspaces/{ws}/dataPipelines/{id}/updateDefinition
  GET  <Location>  (202 -> poll until status Succeeded|Failed)
"""

from __future__ import annotations

import base64
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_PLACEHOLDER = re.compile(r"<(connection|gateway|item|workspace-id|database)[:>][^\"]*>")
_CHILD = re.compile(r"<item:pipeline:([^>\"]+)>")
_GUID = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")
_MAX_POLLS = 60


class DeployError(Exception):
    """A Fabric call failed. The message is user-facing and carries no token."""


@dataclass
class DeployItem:
    name: str  # Fabric display name
    content: dict[str, Any]  # pipeline-content.json
    origin: str = ""  # where it came from (for reports)
    structural_valid: bool = True


@dataclass
class DeployResult:
    name: str
    action: str  # create | update | refused | failed
    item_id: Optional[str] = None
    detail: str = ""
    problems: list[str] = field(default_factory=list)
    children: list[str] = field(default_factory=list)  # child pipelines this one invokes


def unbound_placeholders(content: dict) -> list[str]:
    return sorted({m.group(0) for m in _PLACEHOLDER.finditer(json.dumps(content))})


def preflight(item: DeployItem, allow_unbound: bool = False) -> list[str]:
    """Reasons this pipeline must not be pushed (empty = ok)."""
    problems: list[str] = []
    if not item.structural_valid:
        problems.append("failed structural validation during conversion")
    if not (item.content.get("properties") or {}).get("activities"):
        problems.append("has no activities")
    tokens = unbound_placeholders(item.content)
    if tokens and not allow_unbound:
        problems.append("unbound placeholders still in the JSON: " + ", ".join(tokens))
    return problems


def child_refs(content: dict) -> list[str]:
    """Names of the pipelines this one invokes (from its ``<item:pipeline:...>`` placeholders), in order."""
    seen: list[str] = []
    for m in _CHILD.finditer(json.dumps(content)):
        if m.group(1) not in seen:
            seen.append(m.group(1))
    return seen


def resolve_tokens(content: dict, workspace: str, ids: dict[str, str]) -> dict:
    """A copy with ``<workspace-id>`` and ``<item:pipeline:X>`` replaced where the id is known.

    Placeholders for anything else (connections, other items) are left untouched so that
    ``preflight`` still refuses them.
    """
    def sub(node: Any) -> Any:
        if isinstance(node, str):
            node = node.replace("<workspace-id>", workspace)
            return _CHILD.sub(lambda m: ids.get(m.group(1).lower(), m.group(0)), node)
        if isinstance(node, list):
            return [sub(x) for x in node]
        if isinstance(node, dict):
            return {k: sub(v) for k, v in node.items()}
        return node

    return sub(content)


def order_children_first(items: list[DeployItem]) -> tuple[list[DeployItem], dict[str, str]]:
    """Dependency order (children before parents, otherwise input order) + {name: reason} for cycles."""
    by_name = {i.name.lower(): i for i in items}
    deps = {i.name.lower(): [c.lower() for c in child_refs(i.content) if c.lower() in by_name] for i in items}
    done: list[DeployItem] = []
    placed: set[str] = set()
    remaining = [i.name.lower() for i in items]
    while remaining:
        ready = [n for n in remaining if all(d in placed or d == n for d in deps[n])]
        ready = [n for n in ready if n not in deps[n]]  # a pipeline that invokes itself is a cycle
        if not ready:
            left = set(remaining)

            def reach(start: str) -> set[str]:
                seen: set[str] = set()
                todo = [d for d in deps[start] if d in left]
                while todo:
                    x = todo.pop()
                    if x not in seen:
                        seen.add(x)
                        todo += [d for d in deps[x] if d in left]
                return seen

            return done, {by_name[n].name: ("part of a pipeline-invocation cycle" if n in reach(n)
                                            else "invokes a pipeline that is part of a cycle") for n in remaining}
        for n in ready:
            done.append(by_name[n])
            placed.add(n)
            remaining.remove(n)
    return done, {}


def _definition(content: dict) -> dict:
    raw = json.dumps(content, ensure_ascii=False).encode("utf-8")
    return {"parts": [{"path": "pipeline-content.json",
                       "payload": base64.b64encode(raw).decode("ascii"), "payloadType": "InlineBase64"}]}


def _error_text(resp: Any) -> str:
    try:
        body = resp.json()
        return f"{body.get('errorCode', 'error')}: {body.get('message', '')}".strip()
    except Exception:  # noqa: BLE001
        return (getattr(resp, "text", "") or "")[:200]


class FabricClient:
    def __init__(self, token: str, session: Any = None, sleep: Callable[[float], None] = time.sleep):
        if session is None:
            try:
                import requests
            except ImportError as exc:
                raise DeployError("The 'requests' package is not installed. Install the 'connectivity' extra.") from exc
            session = requests.Session()
        self._s, self._sleep = session, sleep
        self._h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _call(self, method: str, url: str, body: Optional[dict] = None) -> Any:
        if not url.startswith(FABRIC_API + "/"):  # the bearer token only ever goes to Fabric
            raise DeployError("Refusing to send credentials to a non-Fabric URL.")
        for _ in range(6):
            resp = self._s.request(method, url, headers=self._h, json=body, timeout=60)
            if resp.status_code == 429:
                self._sleep(float(resp.headers.get("Retry-After", 5)))
                continue
            return resp
        raise DeployError("Fabric kept rate-limiting the request (HTTP 429).")

    def list_pipelines(self, workspace: str) -> dict[str, str]:
        found: dict[str, str] = {}
        url = f"{FABRIC_API}/workspaces/{workspace}/dataPipelines"
        token: Optional[str] = None
        while True:
            resp = self._call("GET", url + (f"?continuationToken={token}" if token else ""))
            if resp.status_code != 200:
                raise DeployError(f"Listing data pipelines failed (HTTP {resp.status_code}): {_error_text(resp)}")
            data = resp.json()
            for it in data.get("value", []):
                found[it["displayName"].lower()] = it["id"]
            token = data.get("continuationToken")
            if not token:
                return found

    def _finish(self, resp: Any, what: str) -> Optional[dict]:
        """200/201 -> body; 202 -> poll the operation until it finishes."""
        if resp.status_code in (200, 201):
            return resp.json() if resp.content else {}
        if resp.status_code != 202:
            raise DeployError(f"{what} failed (HTTP {resp.status_code}): {_error_text(resp)}")
        op = resp.headers.get("Location") or f"{FABRIC_API}/operations/{resp.headers.get('x-ms-operation-id', '')}"
        wait = float(resp.headers.get("Retry-After", 5))
        for _ in range(_MAX_POLLS):
            self._sleep(wait)
            state = self._call("GET", op)
            body = state.json() if state.content else {}
            status = str(body.get("status", "")).lower()
            if state.status_code in (200, 201) and status == "succeeded":
                return {"operation": op}
            if status == "failed":
                err = body.get("error") or {}
                raise DeployError(f"{what} failed: {err.get('errorCode', '')} {err.get('message', '')}".strip())
            wait = float(state.headers.get("Retry-After", wait))
        raise DeployError(f"{what} did not finish in time; check the workspace before retrying.")

    def create(self, workspace: str, name: str, content: dict) -> None:
        resp = self._call("POST", f"{FABRIC_API}/workspaces/{workspace}/dataPipelines",
                          {"displayName": name, "definition": _definition(content)})
        self._finish(resp, f"Creating '{name}'")

    def update(self, workspace: str, item_id: str, name: str, content: dict) -> None:
        resp = self._call("POST", f"{FABRIC_API}/workspaces/{workspace}/dataPipelines/{item_id}/updateDefinition",
                          {"definition": _definition(content)})
        self._finish(resp, f"Updating '{name}'")


def deploy(client: FabricClient, workspace: str, items: list[DeployItem], *, apply: bool,
           allow_unbound: bool = False) -> list[DeployResult]:
    """Plan (apply=False) or perform (apply=True) the deployment, children first.

    Per-item failures do not stop the rest, but a pipeline whose child failed or is unavailable is refused.
    """
    if not _GUID.match(workspace or ""):
        raise DeployError("The workspace id must be a GUID (Fabric workspace settings > About).")
    existing = client.list_pipelines(workspace)  # name(lower) -> id
    ids = dict(existing)
    ordered, cyclic = order_children_first(items)
    selected = {i.name.lower() for i in items}
    unavailable: set[str] = {n.lower() for n in cyclic}  # children that will not exist after this run
    results: list[DeployResult] = [
        DeployResult(n, "refused", problems=[r], detail="not pushed: " + r) for n, r in cyclic.items()]
    planned: set[str] = set()  # dry run: children that would be created by this run
    for item in ordered:
        children = child_refs(item.content)
        problems: list[str] = []
        for c in children:
            low = c.lower()
            if low in unavailable:
                problems.append(f"child pipeline '{c}' was not deployed")
            elif low not in ids and low not in planned:
                problems.append(f"child pipeline '{c}' is neither selected for deployment nor present in the workspace"
                                + ("" if low not in selected else " (it was refused)"))
        # A child planned but not yet created has no id in a dry run; any GUID stands in for the preflight.
        stand_in = {c.lower(): ids.get(c.lower(), "00000000-0000-0000-0000-000000000000") for c in children}
        resolved = DeployItem(item.name, resolve_tokens(item.content, workspace, stand_in), item.origin,
                               item.structural_valid)
        problems += preflight(resolved, allow_unbound)
        if problems:
            unavailable.add(item.name.lower())
            results.append(DeployResult(item.name, "refused", problems=problems, children=children,
                                        detail="not pushed: " + "; ".join(problems)))
            continue
        item_id = existing.get(item.name.lower())
        action = "update" if item_id else "create"
        if not apply:
            planned.add(item.name.lower())
            results.append(DeployResult(item.name, action, item_id, "dry run: nothing was changed", children=children))
            continue
        try:
            if item_id:
                client.update(workspace, item_id, item.name, resolved.content)
            else:
                client.create(workspace, item.name, resolved.content)
                item_id = client.list_pipelines(workspace).get(item.name.lower())
                if not item_id:
                    raise DeployError(f"'{item.name}' was created but Fabric did not list it; "
                                      "its parents cannot be linked to it.")
            ids[item.name.lower()] = item_id
            results.append(DeployResult(item.name, action, item_id, "done", children=children))
        except DeployError as exc:
            unavailable.add(item.name.lower())
            results.append(DeployResult(item.name, "failed", item_id, str(exc), children=children))
    return results


# --- schedules (generated from ADF triggers) ----------------------------------------------------------------
# GET/POST /v1/workspaces/{ws}/items/{id}/jobs/Pipeline/schedules  ("List Item Schedules" / "Create Item Schedule";
# verified 2026-10-13). An item may hold at most 20 schedules. Schedules are created DISABLED unless the caller opts in,
# nothing is ever deleted or modified, and a schedule identical to an existing one (ignoring `enabled`) is skipped so a
# re-run never duplicates it.

SCHEDULE_JOB_TYPE = "Pipeline"
MAX_SCHEDULES_PER_ITEM = 20


@dataclass
class ScheduleItem:
    pipeline: str  # Fabric display name of the pipeline
    trigger: str  # the ADF trigger it came from
    body: dict[str, Any]  # {"enabled": bool, "configuration": {...}} as generated


@dataclass
class ScheduleResult:
    pipeline: str
    trigger: str
    action: str  # create | exists | skipped | failed
    schedule_id: Optional[str] = None
    detail: str = ""


def list_schedules(client: FabricClient, workspace: str, item_id: str) -> list[dict]:
    url = f"{FABRIC_API}/workspaces/{workspace}/items/{item_id}/jobs/{SCHEDULE_JOB_TYPE}/schedules"
    found: list[dict] = []
    token: Optional[str] = None
    while True:
        resp = client._call("GET", url + (f"?continuationToken={token}" if token else ""))
        if resp.status_code != 200:
            raise DeployError(f"Listing schedules failed (HTTP {resp.status_code}): {_error_text(resp)}")
        data = resp.json()
        found += data.get("value", [])
        token = data.get("continuationToken")
        if not token:
            return found


def create_schedule(client: FabricClient, workspace: str, item_id: str, body: dict) -> Optional[str]:
    url = f"{FABRIC_API}/workspaces/{workspace}/items/{item_id}/jobs/{SCHEDULE_JOB_TYPE}/schedules"
    resp = client._call("POST", url, body)
    if resp.status_code != 201:
        raise DeployError(f"Creating the schedule failed (HTTP {resp.status_code}): {_error_text(resp)}")
    return (resp.json() or {}).get("id") if resp.content else None


def deploy_schedules(client: FabricClient, workspace: str, items: list[ScheduleItem], pipeline_ids: dict[str, str],
                     *, apply: bool, enable: bool = False) -> list[ScheduleResult]:
    """Plan or create the schedules. ``pipeline_ids`` maps a pipeline's display name to its Fabric item id; a pipeline
    without an id (not deployed yet, or a dry run of a new pipeline) is planned but not checked against Fabric."""
    results: list[ScheduleResult] = []
    cache: dict[str, list[dict]] = {}
    created: dict[str, int] = {}
    for it in items:
        item_id = pipeline_ids.get(it.pipeline)
        body = {"enabled": bool(it.body.get("enabled")) and enable, "configuration": it.body["configuration"]}
        if not item_id:
            results.append(ScheduleResult(it.pipeline, it.trigger, "skipped" if apply else "create",
                                          detail="the pipeline does not exist in the workspace yet" if apply
                                          else "dry run: the pipeline is not deployed yet, so it is not checked"))
            continue
        try:
            existing = cache.setdefault(item_id, list_schedules(client, workspace, item_id))
            same = next((e for e in existing if e.get("configuration") == it.body["configuration"]), None)
            if same:
                results.append(ScheduleResult(it.pipeline, it.trigger, "exists", same.get("id"),
                                              "an identical schedule already exists; left unchanged"))
                continue
            if len(existing) + created.get(item_id, 0) >= MAX_SCHEDULES_PER_ITEM:
                results.append(ScheduleResult(it.pipeline, it.trigger, "failed", detail=(
                    f"the pipeline already has {MAX_SCHEDULES_PER_ITEM} schedules (the Fabric limit)")))
                continue
            state = "enabled" if body["enabled"] else "disabled"
            if not apply:
                results.append(ScheduleResult(it.pipeline, it.trigger, "create", detail=f"dry run: would create it {state}"))
                continue
            sid = create_schedule(client, workspace, item_id, body)
            created[item_id] = created.get(item_id, 0) + 1
            results.append(ScheduleResult(it.pipeline, it.trigger, "create", sid, f"created {state}"))
        except DeployError as exc:
            results.append(ScheduleResult(it.pipeline, it.trigger, "failed", detail=str(exc)))
    return results


def list_pipelines_by_name(client: FabricClient, workspace: str) -> dict[str, str]:
    """Display name (as Fabric stores it) -> item id, for every data pipeline in the workspace."""
    found: dict[str, str] = {}
    url = f"{FABRIC_API}/workspaces/{workspace}/dataPipelines"
    token: Optional[str] = None
    while True:
        resp = client._call("GET", url + (f"?continuationToken={token}" if token else ""))
        if resp.status_code != 200:
            raise DeployError(f"Listing data pipelines failed (HTTP {resp.status_code}): {_error_text(resp)}")
        data = resp.json()
        for it in data.get("value", []):
            found[it["displayName"]] = it["id"]
        token = data.get("continuationToken")
        if not token:
            return found
