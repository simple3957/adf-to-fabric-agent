"""The ids a person must supply by hand after conversion: the Fabric notebook, Spark job, dataflow or connection an activity needs.

The converter leaves a placeholder (``<item:notebook:NB_X>``, ``<connection:invoke-pipeline>``) where it needs a real id that only exists once
the item is created in Fabric. This module lists those open items from the conversion record and writes the answer to
``conversion/resolutions.json`` (``ItemToId``), the file ``convert`` already reads. Two kinds are deliberately not here:

* a connection that belongs to an ADF linked service is set on the **Connect** step (``LinkedServiceToConnectionId``);
* a child pipeline's id and the workspace id are filled in by ``deploy`` itself.
"""
from __future__ import annotations

import json
import re
from typing import Any, Optional

from adf2fabric.project.locks import lock_for, write_atomic
from adf2fabric.project.store import ProjectStore

_GUID = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")

# item kind in a binding key -> (what to call it, the Fabric item type to list candidates from)
ITEM_KINDS: dict[str, tuple[str, str]] = {
    "notebook": ("Fabric notebook", "Notebook"),
    "sparkjob": ("Fabric Spark job definition", "SparkJobDefinition"),
    "dataflow": ("Fabric dataflow", "Dataflow"),
}
FABRIC_PIPELINE_CONNECTION = "FabricDataPipelines"  # the connection type behind an Invoke pipeline activity


class BindingError(ValueError):
    """The binding was refused; the message says why and is safe to show."""


def _resolution_entries(store: ProjectStore, pid: str) -> list[dict]:
    path = store.conversion_dir(pid) / "resolutions.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    return [e for e in data if isinstance(e, dict)] if isinstance(data, list) else []


def _linked_service_names(store: ProjectStore, pid: str) -> set[str]:
    try:
        return {ls.name.lower() for ls in store.load_adf_semantic_discovery(pid).linked_services}
    except Exception:  # noqa: BLE001 - without discovery every connection is treated as one of the Connect step's
        return set()


def open_items(store: ProjectStore, pid: str) -> list[dict[str, Any]]:
    """Every id a person has to supply for this conversion, bound or not (so a saved answer stays visible and can be changed).

    ``key`` is the resolutions key (``notebook:NB_X`` or ``connection:invoke-pipeline``); ``item_type`` is what Fabric calls it."""
    if not store.has_conversion(pid):
        return []
    conv = store.load_conversion(pid)
    entries = {(str(e.get("type")), str(e.get("key", "")).lower()): str(e.get("value", "")) for e in _resolution_entries(store, pid)}
    ls_names = _linked_service_names(store, pid)
    out: list[dict[str, Any]] = []
    for q in conv.workspace_items:
        kind, _, name = q.name.partition(":")
        if kind not in ITEM_KINDS or not name:
            continue  # a child pipeline and the workspace are filled in at deploy
        label, item_type = ITEM_KINDS[kind]
        out.append({"key": q.name, "kind": kind, "name": name, "label": label, "item_type": item_type, "detail": q.detail,
                    "needed_by": list(q.needed_by), "bound": q.bound, "value": entries.get(("ItemToId", q.name.lower())) or None})
    for q in conv.connections:
        if q.name.lower() in ls_names:
            continue  # belongs to a linked service: the Connect step
        key = f"connection:{q.name}"
        out.append({"key": key, "kind": "connection", "name": q.name, "label": "Fabric connection", "item_type": "Connection", "detail": q.detail,
                    "needed_by": list(q.needed_by), "bound": q.bound, "value": entries.get(("ItemToId", key.lower())) or None})
    return sorted(out, key=lambda i: (i["bound"], i["kind"], i["name"].lower()))


def set_binding(store: ProjectStore, pid: str, key: str, value: str) -> dict[str, Any]:
    """Record ``key`` -> ``value`` in the resolutions file. Only a key this conversion asks for is accepted, and only a GUID."""
    value = (value or "").strip()
    if not _GUID.match(value):
        raise BindingError("That is not a Fabric item id: it should look like 12345678-1234-1234-1234-123456789abc.")
    known = {i["key"].lower(): i["key"] for i in open_items(store, pid)}
    if key.lower() not in known:
        raise BindingError("This conversion does not need that item: it is not in the list of open bindings.")
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
                return {}, "Choose a Fabric workspace on the Connections page to list its notebooks, jobs and dataflows. You can still paste an id."
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
