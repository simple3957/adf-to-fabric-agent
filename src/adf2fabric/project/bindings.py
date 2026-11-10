"""The ids a person must supply by hand after conversion: the Fabric notebook, Spark job, dataflow or connection an activity needs.

The converter leaves a placeholder (``<item:notebook:NB_X>``, ``<connection:invoke-pipeline>``) where it needs a real id that only exists once
the item is created in Fabric. This module lists those open items from the conversion record and writes the answer to
``conversion/resolutions.json`` (``ItemToId``), the file ``convert`` already reads. Two kinds are deliberately not here:

* a connection that belongs to an ADF linked service is set on the **Connect** step (``LinkedServiceToConnectionId``);
* a child pipeline's id and the workspace id are filled in by ``deploy`` itself.

Ids are kept per Fabric workspace. A notebook, Spark job, dataflow, lakehouse or child pipeline exists in one workspace only, so
``use_workspace`` sets the current workspace's ids aside (``conversion/bindings-by-workspace/<workspace>.json``) before another
workspace's are used, and brings them back when that workspace is chosen again. Connections are not tied to a workspace and stay.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from adf2fabric.project.locks import lock_for, read_text_retry, write_atomic
from adf2fabric.project.store import ProjectStore
from adf2fabric.ids import GUID as _GUID


# item kind in a binding key -> (what to call it, the Fabric item type to list candidates from)
ITEM_KINDS: dict[str, tuple[str, str]] = {
    "notebook": ("Fabric notebook", "Notebook"),
    "sparkjob": ("Fabric Spark job definition", "SparkJobDefinition"),
    "dataflow": ("Fabric dataflow", "Dataflow"),
    "lakehouse": ("Fabric lakehouse", "Lakehouse"),  # the Convert step's lakehouse option leaves <item:lakehouse:NAME> until the item's id is known
}
FABRIC_PIPELINE_CONNECTION = "FabricDataPipelines"  # the connection type behind an Invoke pipeline activity
WORKSPACE_KINDS = set(ITEM_KINDS) | {"pipeline"}  # binding kinds whose id belongs to one workspace (a child pipeline's too)


def _scoped(e: dict) -> bool:
    """An entry that belongs to one workspace: its workspace id, or the id of an item in it."""
    return e.get("type") == "WorkspaceId" or (e.get("type") == "ItemToId" and str(e.get("key", "")).split(":", 1)[0].lower() in WORKSPACE_KINDS)


def _kept_path(store: ProjectStore, pid: str, workspace: str):
    return store.conversion_dir(pid) / "bindings-by-workspace" / f"{workspace}.json"


def _kept(store: ProjectStore, pid: str, workspace: str) -> list[dict]:
    """The ids saved earlier for ``workspace`` while another was in use."""
    try:
        data = json.loads(read_text_retry(_kept_path(store, pid, workspace)))
    except (FileNotFoundError, ValueError):
        return []
    return [e for e in data if isinstance(e, dict) and _scoped(e)] if isinstance(data, list) else []


def use_workspace(store: ProjectStore, pid: str, workspace: Optional[str]) -> bool:
    """Make ``workspace``'s ids the ones Convert reads. The ids of the workspace in use are set aside for it, and the ids kept for
    ``workspace`` (if it was used before) come back; connections stay. True when something changed. Never drops an id."""
    from adf2fabric.connections.catalog import normalize_guid

    ws = normalize_guid(workspace or "")
    if not _GUID.match(ws):
        return False
    path = store.conversion_dir(pid) / "resolutions.json"
    if not path.parent.exists():
        return False
    with lock_for(path):
        entries = _resolution_entries(store, pid)
        bound = next((normalize_guid(str(e.get("value") or "")) for e in entries if e.get("type") == "WorkspaceId"), None)
        if bound == ws:
            return False
        scoped = [e for e in entries if _scoped(e)]
        if bound and scoped:
            kept = _kept_path(store, pid, bound)
            kept.parent.mkdir(parents=True, exist_ok=True)
            write_atomic(kept, json.dumps(scoped, indent=2) + "\n")
        back = [e for e in _kept(store, pid, ws) if e.get("type") != "WorkspaceId"]
        mine = back if bound else [e for e in scoped if e.get("type") != "WorkspaceId"]  # no workspace recorded yet: they become this one's
        out = [e for e in entries if not _scoped(e)] + mine + [{"type": "WorkspaceId", "key": "*", "value": ws}]
        write_atomic(path, json.dumps(out, indent=2) + "\n")
    return True


class BindingError(ValueError):
    """The binding was refused; the message says why and is safe to show."""


def _resolution_entries(store: ProjectStore, pid: str) -> list[dict]:
    path = store.conversion_dir(pid) / "resolutions.json"
    try:
        data = json.loads(read_text_retry(path))
    except (FileNotFoundError, ValueError):
        return []
    return [e for e in data if isinstance(e, dict)] if isinstance(data, list) else []


def _linked_service_names(store: ProjectStore, pid: str) -> set[str]:
    try:
        return {ls.name.lower() for ls in store.load_adf_semantic_discovery(pid).linked_services}
    except Exception:  # noqa: BLE001 - without discovery every connection is treated as one of the Connect step's
        return set()


def open_items(store: ProjectStore, pid: str, workspace: Optional[str] = None) -> list[dict[str, Any]]:
    """Every id a person has to supply for this conversion, bound or not (so a saved answer stays visible and can be changed).

    ``key`` is the resolutions key (``notebook:NB_X`` or ``connection:invoke-pipeline``); ``item_type`` is what Fabric calls it.
    With ``workspace`` other than the one the ids in use were saved for, a workspace's item shows that workspace's own saved id (or
    none) and is not bound: the last Convert used another workspace's."""
    from adf2fabric.connections.catalog import normalize_guid

    if not store.has_conversion(pid):
        return []
    conv = store.load_conversion(pid)
    raw = _resolution_entries(store, pid)
    ws, bound_ws = normalize_guid(workspace or ""), bound_workspace(store, pid)
    other = bool(ws and bound_ws and ws != bound_ws)
    if other:
        raw = [e for e in raw if not _scoped(e)] + _kept(store, pid, ws)
    entries = {(str(e.get("type")), str(e.get("key", "")).lower()): str(e.get("value", "")) for e in raw}
    ls_names = _linked_service_names(store, pid)
    out: list[dict[str, Any]] = []
    for q in conv.workspace_items:
        kind, _, name = q.name.partition(":")
        if kind not in ITEM_KINDS or not name:
            continue  # a child pipeline and the workspace are filled in at deploy
        label, item_type = ITEM_KINDS[kind]
        out.append({"key": q.name, "kind": kind, "name": name, "label": label, "item_type": item_type, "detail": q.detail,
                    "needed_by": list(q.needed_by), "bound": q.bound and not other, "value": entries.get(("ItemToId", q.name.lower())) or None})
    for q in conv.connections:
        if q.name.lower() in ls_names:
            continue  # belongs to a linked service: the Connect step
        key = f"connection:{q.name}"
        out.append({"key": key, "kind": "connection", "name": q.name, "label": "Fabric connection", "item_type": "Connection", "detail": q.detail,
                    "needed_by": list(q.needed_by), "bound": q.bound, "value": entries.get(("ItemToId", key.lower())) or None})
    return sorted(out, key=lambda i: (i["bound"], i["kind"], i["name"].lower()))


def set_binding(store: ProjectStore, pid: str, key: str, value: str, workspace: Optional[str] = None) -> dict[str, Any]:
    """Record ``key`` -> ``value`` in the resolutions file. Only a key this conversion asks for is accepted, and only a GUID. With
    ``workspace`` the ids in use are that workspace's first (``use_workspace``), so an id is saved for the workspace it was chosen in."""
    from adf2fabric.connections.catalog import normalize_guid

    value = normalize_guid(value)
    if not _GUID.match(value):
        raise BindingError("That is not a Fabric item id: it should look like 12345678-1234-1234-1234-123456789abc.")
    known = {i["key"].lower(): i["key"] for i in open_items(store, pid)}
    if key.lower() not in known:
        raise BindingError("This conversion does not need that item: it is not in the list of open bindings.")
    if key.split(":", 1)[0].lower() in WORKSPACE_KINDS:
        use_workspace(store, pid, workspace)
    key = known[key.lower()]
    path = store.conversion_dir(pid) / "resolutions.json"
    with lock_for(path):
        entries = _resolution_entries(store, pid)
        for e in entries:
            if e.get("type") == "ItemToId" and str(e.get("key", "")).lower() == key.lower():
                changed = e.get("value") != value
                e["value"] = value
                break
        else:
            entries.append({"type": "ItemToId", "key": key, "value": value})
            changed = True
        if changed:
            write_atomic(path, json.dumps(entries, indent=2) + "\n")
    return {"key": key, "changed": changed}


def bound_workspace(store: ProjectStore, pid: str) -> Optional[str]:
    """The Fabric workspace the saved bindings were made for (the resolutions file's ``WorkspaceId *``), lower case, or None when none was saved."""
    from adf2fabric.connections.catalog import normalize_guid

    for e in _resolution_entries(store, pid):
        if e.get("type") == "WorkspaceId":
            value = normalize_guid(str(e.get("value") or ""))
            return value or None
    return None


def mismatch_text(bound: Optional[str], workspace_id: Optional[str]) -> Optional[str]:
    """Words for "these bindings belong to another workspace", or None. Only the end of each id is shown: an ordinary view carries no resource ids."""
    from adf2fabric.connections.catalog import normalize_guid

    ws = normalize_guid(workspace_id or "")
    if not bound or not ws or bound == ws:
        return None
    return (f"The last Convert used the ids saved for another Fabric workspace (ending ...{bound[-6:]}); the one chosen now ends ...{ws[-6:]}. "
            "Ids are kept per workspace and never carried over: create or bind the notebooks in this workspace, then Convert again "
            "(the other workspace's ids are kept for it).")


def workspace_mismatch(store: ProjectStore, pid: str, workspace_id: Optional[str]) -> Optional[str]:
    return mismatch_text(bound_workspace(store, pid), workspace_id)


def candidates(fabric: Any, workspace_id: Optional[str], items: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, str]]], str]:
    """For each open item key, the Fabric items it could be (``id``, ``name``), read live; and why nothing was read, if so.

    A read that fails gives no candidates and a reason, never a guess."""
    if fabric is None:
        return {}, "Fabric is not connected for this session, so existing items can't be listed. You can still paste an id."
    out: dict[str, list[dict[str, str]]] = {}
    try:
        by_type: dict[str, list[dict[str, str]]] = {}
        for t in {i["item_type"] for i in items if i["item_type"] != "Connection"}:
            if not workspace_id:
                return {}, "Choose a Fabric workspace on the Connections page to list its notebooks, jobs, dataflows and lakehouses. You can still paste an id."
            by_type[t] = [{"id": str(x.get("id", "")), "name": str(x.get("displayName", ""))} for x in fabric.items(workspace_id, t)]
        conns = fabric.connections() if any(i["item_type"] == "Connection" for i in items) else []
        for i in items:
            if i["item_type"] == "Connection":
                want = FABRIC_PIPELINE_CONNECTION if i["name"] == "invoke-pipeline" else None
                out[i["key"]] = [{"id": str(c.get("id", "")), "name": f"{c.get('displayName', '')} ({(c.get('connectionDetails') or {}).get('type', '?')})"}
                                 for c in conns if want is None or (c.get("connectionDetails") or {}).get("type") == want]
            else:
                out[i["key"]] = by_type.get(i["item_type"], [])
    except Exception as exc:  # noqa: BLE001
        from adf2fabric.connections.service import reason

        return {}, f"Fabric's items could not be read ({reason(exc)}). You can still paste an id."
    return out, ""
