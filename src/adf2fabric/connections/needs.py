"""Which linked services still need a Fabric connection after the tool's own workarounds (2026-11-17, the user's rule: "for wherever we
are using the fabric notebooks or the next workaround is present it should only show that way and not asking the user to create things
manually").

A linked service needs a connection only when a converted Fabric activity still points at it (a Copy, Lookup, Get Metadata ... reading
its store). One whose activities became something the tool creates (a Fabric notebook, a Spark job definition, a lakehouse read
through a OneLake shortcut) or that nothing can run in Fabric needs none, and Connect, Plan and validation say so instead of asking.

The answer comes from the conversion record when it is current (what Deploy will use), else from the converter run in memory
(``ConversionAgent.forecast``: nothing written), so Connect knows it before Convert has run. Each answer says which of the two it came
from.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from adf2fabric.project.store import ProjectStore

# what a converted activity's components are, in words a person reads
_BECAME = {"TridentNotebook": "a Fabric notebook", "FabricSparkJobDefinition": "a Fabric Spark job definition",
           "RefreshDataflow": "a Dataflow Gen2", "InvokePipeline": "an Invoke pipeline activity", "Fail": "nothing (it cannot run in Fabric)"}

# a stand-in that waits for what the tool generates (Review > Compute: generate, approve, then Deploy creates it); not a dead end
_NOTEBOOK_SOON = "a Fabric notebook (the tool generates it; Deploy creates it)"
_SJD_SOON = "a Fabric Spark job definition (the tool generates it; Deploy creates it)"
_PENDING = {"databricks_notebook_pending": _NOTEBOOK_SOON, "databricks_job_notebooks_pending": _NOTEBOOK_SOON,
            "databricks_python_pending": _NOTEBOOK_SOON, "dataflow_notebook_pending": _NOTEBOOK_SOON, "usql_notebook_pending": _NOTEBOOK_SOON, "ssis_notebook_pending": _NOTEBOOK_SOON,
            "databricks_jar_pending": _SJD_SOON, "hdinsight_spark_job_pending": _SJD_SOON}


PENDING_CODES = frozenset(_PENDING)


def is_waiting(activity) -> bool:
    """A blocked activity that only waits for an item the tool generates and Deploy creates (a notebook, a Spark job definition): not a dead end.
    Until 2026-11-19 it was counted and worded as 'blocked', which read as 'the tool cannot migrate this'."""
    return activity.status.value == "blocked" and any(getattr(w, "code", None) in PENDING_CODES for w in (activity.warnings or []))


@dataclass
class Need:
    linked_service: str
    needed: bool
    source: str  # conversion | forecast
    needed_by: list[str] = field(default_factory=list)  # activity names that still use the connection
    reason: str = ""  # why no connection is needed (empty when one is)


def _walk(acts):
    """Activities, nested ones included (a discovery activity holds its children; a conversion record lists them flat, children by id)."""
    for a in acts or []:
        yield a
        yield from _walk([c for c in (getattr(a, "activities", None) or []) if not isinstance(c, str)])


def needs(store: ProjectStore, pid: str) -> Optional[dict[str, Need]]:
    """linked service (lower) -> Need; None when it cannot be told (no discovery, or the converter could not run)."""
    if not store.has_adf_semantic_discovery(pid):
        return None
    d = store.load_adf_semantic_discovery(pid)
    records, source = None, "conversion"
    if store.has_conversion(pid):
        conv = store.load_conversion(pid)
        if conv.source_commit_sha == d.source_commit_sha and conv.pipeline_count == len(d.pipelines):
            records = conv.pipelines
    if records is None:
        from adf2fabric.agent.conversion.agent import ConversionAgent

        try:
            records, source = ConversionAgent(store).forecast(pid), "forecast"
        except Exception:  # noqa: BLE001 - a forecast that fails leaves every linked service asked for, as before
            return None
        if not records and d.pipelines:
            return None
    from adf2fabric.connections.resolution import usage

    used: dict[str, list[str]] = {}
    for p in records:
        for q in p.prerequisites:
            if q.kind == "connection":
                used.setdefault(q.name.split("?", 1)[0].lower(), []).extend(q.needed_by)
    by_id = {a.source_activity_id: a for p in records for a in _walk(p.activities)}
    by_name = {(p.source_pipeline.lower(), a.source_name.lower()): a for p in records for a in _walk(p.activities)}
    act_ids = {(p.name.lower(), a.name.lower()): a.activity_id for p in d.pipelines for a in _walk(p.activities)}
    out: dict[str, Need] = {}
    for ls in d.linked_services:
        key = ls.name.lower()
        if key in used:
            out[key] = Need(ls.name, True, source, sorted(set(used[key])))
            continue
        _, acts, _ = usage(d, ls.name)
        became: list[str] = []
        for u in acts:
            rec = by_id.get(act_ids.get((u["pipeline"].lower(), u["activity"].lower()))) or by_name.get((u["pipeline"].lower(), u["activity"].lower()))
            comps = [getattr(c, "fabric_type", None) or str(c) for c in (rec.components if rec else [])]
            pending = [_PENDING[w.code] for w in (rec.warnings if rec else []) if getattr(w, "code", None) in _PENDING]
            words = pending or [_BECAME.get(c) for c in comps if c in _BECAME] or ["an activity that reads it through the lakehouse"]
            became.append(f"{u['activity']} ({u['type']}) became {', '.join(sorted(set(words)))}")
        if not acts:
            why = "no activity uses it."
        else:
            why = "; ".join(sorted(set(became))) + "."
            if any(_BECAME["Fail"] not in b for b in became):
                why += " The tool creates what replaces it."
        out[key] = Need(ls.name, False, source, [], f"No Fabric connection is needed: {why}")
    return out
