"""Recognition of common ingestion and orchestration patterns.

Recognition is read-only and evidence-based: a pattern is reported with the activities and facts that
make it one, and with the parts it normally has but this pipeline lacks (``missing``). Recognition never
creates anything — only the rules in ``rules_*`` may propose a missing part, and only with evidence.
"""

from __future__ import annotations

from adf2fabric.agent.reconstruction.context import PipelineCtx
from adf2fabric.agent.reconstruction.rules_patterns import _loaders, _sink_table
from adf2fabric.agent.reconstruction.rules_structure import _file_loop_item
from adf2fabric.project.discovery import AdfActivity
from adf2fabric.project.reconstruction import RecognizedPattern


def _t(a: AdfActivity) -> str:
    return (a.type or "").lower()


def recognize_patterns(pc: PipelineCtx) -> list[RecognizedPattern]:
    ev, p, out = pc.ev, pc.pipe, pc.out
    acts = ev.acts(p)
    open_codes = {(f.activity_id, f.code) for f in out.findings if f.pipeline == p.name}
    found: list[RecognizedPattern] = []

    def add(name, activities, evidence, missing=()):
        found.append(RecognizedPattern(
            name=name, pipeline=p.name, activity_ids=[a.activity_id for a in activities],
            evidence=evidence, complete=not missing, missing=list(missing)))

    # incremental load: watermark lookup -> filtered extraction -> write -> watermark update
    for c in acts:
        if "incremental_filter" in c.semantic_tags:
            lookups = [x for x in acts if "watermark_lookup" in x.semantic_tags]
            gap = (c.activity_id, "incremental_watermark_update_missing") in open_codes
            add("incremental_load", [*lookups, c],
                [f"{c.name} filters on the output of {', '.join(repr(x.name) for x in lookups) or 'a lookup'}"],
                ["watermark update"] if gap else [])

    # file ingestion: metadata -> (filter) -> loop -> copy
    for loop in [a for a in acts if _t(a) == "foreach"]:
        inner = [x for x in ev.descendants_of(p, loop) if _t(x) == "copy"]
        for c in inner:
            hit = _file_loop_item(pc, c)
            if hit and hit[0] is loop:
                gap = any((c.activity_id, code) in open_codes for code in ("dataset_parameter_not_supplied",))
                add("file_ingestion", [hit[1], loop, c],
                    [f"{loop.name} iterates the childItems of {hit[1].name}", f"{c.name} copies each item"],
                    ["binding of the current file to the Copy dataset"] if gap else [])
                break

    # parent pipeline orchestrating child pipelines
    calls = [a for a in acts if _t(a) == "executepipeline"]
    if calls:
        missing = [f"child pipeline '{a.configuration.get('pipeline')}'" for a in calls
                   if str(a.configuration.get("pipeline") or "").lower() not in ev.pipelines]
        add("parent_child_orchestration", calls, [f"{a.name} runs {a.configuration.get('pipeline')}" for a in calls], missing)

    # parameterized ingestion reused across tables
    param_copies = [a for a in acts if _t(a) == "copy" and any(
        r.kind == "pipeline_parameter" for e in a.expressions
        if e.role in ("source_selection", "destination_selection", "source_query") for r in e.references)]
    if param_copies:
        sibs = [q.name for q in ev.discovery.pipelines if q.name != p.name and len(ev.signature(p)) >= 2
                and ev.signature(q) == ev.signature(p)]
        add("parameterized_ingestion", param_copies,
            [f"{a.name} takes its source/destination from pipeline parameters" for a in param_copies]
            + ([f"same structure as: {', '.join(sorted(sibs))}"] if sibs else []))

    # staging -> curated
    for a in acts:
        st = _sink_table(pc, a)
        if st:
            loaded = bool(_loaders(pc, a, st[1], st[0]))
            gap = (a.activity_id, "staging_load_missing") in open_codes
            add("staging_to_curated", [a], [f"{a.name} writes {st[0]}.{st[1]}" + (" and a later step loads it" if loaded else "")],
                ["load into the curated layer"] if gap else [])

    # conditional execution on data availability / load status
    for a in acts:
        if _t(a) in ("ifcondition", "switch"):
            srcs = list({x.activity_id: x for x in acts for e in a.expressions for r in e.references
                         if r.kind == "activity_output" and (r.name or "").lower() == x.name.lower()
                         and _t(x) in ("lookup", "getmetadata")}.values())
            if srcs:
                add("conditional_execution", [*srcs, a],
                    [f"{a.name} branches on the output of {', '.join(repr(x.name) for x in srcs)}"])

    # retry loop
    for a in acts:
        if _t(a) == "until" and any(_t(x) == "wait" for x in ev.descendants_of(p, a)):
            add("retry_loop", [a], [f"{a.name} repeats its activities with a Wait between attempts"])

    # failure notification / error handling
    handlers = [x for x in acts if any({"Failed", "Completed"} & set(d.conditions) for d in x.depends_on)]
    if handlers:
        add("failure_handling", handlers,
            [f"{x.name} ({x.type}) runs on failure/completion of "
             f"{', '.join(d.activity for d in x.depends_on if {'Failed', 'Completed'} & set(d.conditions))}" for x in handlers])
    return found
