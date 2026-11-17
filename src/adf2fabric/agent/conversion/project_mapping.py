"""The project's activity mapping: ``projects/<p>/conversion/activity-mapping.json``.

One row per real activity of the project (every pipeline, nested activities included): its id, pipeline, name and ADF type, the
capability it belongs to in the master compatibility catalog (``adf2fabric/catalog/compatibility.json``), every Fabric option the
catalog allows for that type, and, where a person may decide per activity, the switch and its ``chosen`` value:

* ``storage``  (Copy, Lookup, GetMetadata, Delete): ``connection`` or ``lakehouse``
* ``spark``    (Databricks Notebook, Databricks Job): ``databricks`` or ``fabric``
* ``dataflow`` (Execute Data Flow): ``notebook`` or ``dataflow_gen2``

``chosen`` null means "follow the run's normal decision" (the brief, the Convert options, what is approved and bound). A person edits
``chosen``; Convert writes the file again before it converts (new activities added, choices kept), refuses a value the catalog does
not allow, and follows the rest. After conversion every activity's Fabric output is checked against the options the catalog lists
for its type (``check``).
"""

from __future__ import annotations

import json
from typing import Any, Optional

from adf2fabric.agent.discovery.adf_semantic.inventory import walk
from adf2fabric.catalog import by_adf_type, capability_for, master
from adf2fabric.project.locks import lock_for, read_text_retry, write_atomic
from adf2fabric.project.store import ProjectStore

FILE = "activity-mapping.json"


class MappingError(ValueError):
    """The project's activity mapping asks for something the master catalog does not allow."""


def _switch_kind(adf_type: str) -> Optional[str]:
    for kind, sw in master()["switches"].items():
        if any(t.lower() == adf_type.lower() for t in sw["applies_to"]):
            return kind
    return None


def _path(store: ProjectStore, pid: str):
    return store.conversion_dir(pid) / FILE


def load(store: ProjectStore, pid: str) -> Optional[dict[str, Any]]:
    p = _path(store, pid)
    if not p.exists():
        return None
    try:
        return json.loads(read_text_retry(p))
    except ValueError as exc:
        raise MappingError(f"{FILE} is not valid JSON ({exc}); fix it or delete it to regenerate.") from None


def build(store: ProjectStore, pid: str) -> dict[str, Any]:
    """Write the mapping from the project's discovery, keeping the choices already made. Returns it."""
    d = store.load_adf_semantic_discovery(pid)
    old = {r["id"].lower(): r for r in ((load(store, pid) or {}).get("activities") or []) if isinstance(r, dict) and r.get("id")}
    rows: list[dict[str, Any]] = []
    for p in sorted(d.pipelines, key=lambda x: x.name.lower()):
        for a in walk(p.activities):
            aid = a.activity_id or f"{p.name}/{a.name}"
            hit = capability_for(a.type)
            entry = by_adf_type(a.type) if hit else None
            kind = _switch_kind(a.type or "")
            row: dict[str, Any] = {
                "id": aid, "pipeline": p.name, "name": a.name, "adf_type": a.type, "capability": hit[0] if hit else None,
                "microsoft_comparison": entry.get("microsoft_comparison") if entry else None,
                "options": [{"option": i, "fabric": o["fabric"] or ["Fail (placeholder)"], "status": o["status"], "when": o["when"],
                             **({"switch": o["switch"]["value"]} if o.get("switch") else {})} for i, o in enumerate(entry["options"])] if entry else [],
                "workarounds": [w["what"] for w in entry["workarounds"]] if entry else [],
            }
            if not hit:
                row["note"] = "No capability in the master catalog: the converter blocks it (never dropped)."
            if kind:
                allowed = list(master()["switches"][kind]["values"])
                prev = (old.get(aid.lower()) or {}).get("chosen")
                row.update({"switch": kind, "allowed": allowed, "chosen": prev})  # kept as written: an invalid value is refused, never dropped
            rows.append(row)
    data = {
        "about": "Every activity of this project with what the master compatibility catalog allows it to become in Fabric. Set 'chosen' "
                 "where a switch is offered (null = follow the run's normal decision); Convert follows it and refuses anything else.",
        "project": pid, "source_commit_sha": d.source_commit_sha, "catalog_version": master()["version"],
        "switches": {k: {"label": v["label"], "values": v["values"]} for k, v in master()["switches"].items()},
        "activities": rows,
    }
    p = _path(store, pid)
    p.parent.mkdir(parents=True, exist_ok=True)
    with lock_for(p):
        write_atomic(p, json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    return data


def choices(data: dict[str, Any]) -> dict[str, tuple[str, str]]:
    """activity id (lower case) -> (switch kind, chosen value), for the rows a person set. Raises MappingError for a value the catalog
    does not allow, or a choice on an activity that offers no switch."""
    out: dict[str, tuple[str, str]] = {}
    bad: list[str] = []
    for r in data.get("activities") or []:
        if not isinstance(r, dict) or r.get("chosen") in (None, ""):
            continue
        kind = _switch_kind(r.get("adf_type") or "")
        allowed = list(master()["switches"][kind]["values"]) if kind else []
        if r["chosen"] not in allowed:
            bad.append(f"{r.get('id')}: '{r['chosen']}' is not allowed" + (f" (allowed: {', '.join(allowed)})" if allowed else " (this activity offers no choice)"))
            continue
        out[str(r["id"]).lower()] = (kind, r["chosen"])
    if bad:
        raise MappingError("The project's activity mapping asks for options the master catalog does not allow: " + "; ".join(bad[:6]))
    return out


def check(data: dict[str, Any], conv: Any) -> list[str]:
    """Each converted activity's Fabric types must be among the options the catalog lists for its ADF type (a blocked activity's Fail
    placeholder included). Returns the mismatches (an empty list when the converter and the catalog agree)."""
    allowed: dict[str, set[str]] = {}
    for r in data.get("activities") or []:
        allowed[str(r["id"]).lower()] = {t for o in r.get("options") or [] for t in o["fabric"]} | {"Fail", "Fail (placeholder)"}
    out = []
    for p in conv.pipelines:
        for a in p.activities:
            want = allowed.get(a.source_activity_id.lower())
            if want is None:
                continue
            got = {c.fabric_type for c in a.components}
            extra = sorted(got - want)
            if extra:
                out.append(f"{a.source_activity_id}: produced {', '.join(extra)}, which the master catalog does not list for {a.source_type}")
    return out
