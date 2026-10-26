"""Account for every ADF dataset after conversion: inlined into which activities, repointed to the Lakehouse, or carried nowhere.

Fabric has no dataset item, so a dataset that no converted activity uses has nothing to be inlined into. That used to leave it out of
every report; now it is listed with the reason.
"""

from __future__ import annotations

from adf2fabric.agent.discovery.adf_semantic.inventory import walk
from adf2fabric.project.conversion import DatasetLedgerEntry, PipelineConversion
from adf2fabric.project.discovery import AdfSemanticDiscovery


def build(d: AdfSemanticDiscovery, pipelines: list[PipelineConversion]) -> list[DatasetLedgerEntry]:
    inlined: dict[str, list[str]] = {}
    repointed: set[str] = set()
    for p in pipelines:
        for a in p.activities:
            for m in a.property_mappings:
                src = str(m.source or "")
                if m.rule == "dataset_inlined" and src.startswith("dataset:"):
                    inlined.setdefault(src[8:].lower(), []).append(f"{p.source_pipeline}/{a.source_name}")
                elif m.rule == "storage_repointed" and src.startswith("dataset:"):
                    repointed.add(src[8:].split(".", 1)[0].lower())
    converted = {p.source_pipeline.lower() for p in pipelines}
    held_back: dict[str, set[str]] = {}
    for sp in d.pipelines:
        if sp.name.lower() in converted:
            continue
        for a in walk(sp.activities):
            for n in a.dataset_refs:
                held_back.setdefault(n.lower(), set()).add(sp.name)
    in_flows: dict[str, set[str]] = {}
    for f in d.dataflows:
        for n in f.dataset_refs:
            in_flows.setdefault(n.lower(), set()).add(f.name)
    out = []
    for ds in sorted(d.datasets, key=lambda x: x.name.lower()):
        low = ds.name.lower()
        used = sorted(set(inlined.get(low, [])))
        if used:
            out.append(DatasetLedgerEntry(
                name=ds.name, type=ds.type, used_by=used,
                treatment="repointed_to_lakehouse" if low in repointed else "inlined",
                note="Its location now points at the Lakehouse (OneLake shortcut)." if low in repointed else
                     "Its type, location, schema and connection are copied into each of these activities."))
        elif low in held_back:
            out.append(DatasetLedgerEntry(
                name=ds.name, type=ds.type, treatment="not_converted",
                note=f"Used by {', '.join(sorted(held_back[low]))}, which was not converted in this run."))
        elif low in in_flows:
            out.append(DatasetLedgerEntry(
                name=ds.name, type=ds.type, treatment="used_by_data_flow",
                note=f"Used by data flow {', '.join(sorted(in_flows[low]))}; data flows are not converted, so it is not carried."))
        else:
            out.append(DatasetLedgerEntry(
                name=ds.name, type=ds.type, treatment="not_used",
                note="No pipeline activity uses it, so there is nothing to inline it into and it is not carried. It stays in the source."))
    return out
