"""Semantic classification and plain-language description of parsed activities.

Everything is derived from the parsed record by fixed rules and fixed text templates:

* ``semantic_tags`` are assigned only from EVIDENCE in the activity's own properties,
  its expressions, and where its output is consumed — never from its name.
* Activity NAMES are only hints. :func:`_name_hints` checks each hint against the
  properties and raises a warning when they disagree (or cannot be verified).
* ``semantic_summary`` is generated from templates filled with extracted values.

:func:`finalize_pipeline` is idempotent: it can be re-run after cross-artifact
resolution adds dataset/linked-service context, and it then refreshes the tags,
summaries and confidence from the richer record.
"""

from __future__ import annotations

import re
from typing import Optional

from adf2fabric.agent.discovery.adf_semantic.activity_registry import get_spec
from adf2fabric.agent.discovery.adf_semantic.expressions import analyze_expression
from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import (
    CROSS_PIPELINE_PATTERNS,
    add_issue,
    walk,
)
from adf2fabric.project.discovery import (
    AdfActivity,
    AdfConfidence,
    AdfIO,
    AdfIssueSeverity,
    AdfParseIssue,
    AdfPattern,
    AdfPipeline,
)

# Warning codes this module owns (removed and recomputed on every finalize).
_OWNED_CODES = frozenset({
    "name_suggests_different_type", "name_contradicts_properties", "name_hint_unverified",
    "lookup_output_unused",
})
# Warning codes that mean the activity is only partly understood.
_PARTIAL_CODES = frozenset({
    "expression_unparsed", "unknown_expression_function", "dynamic_reference_name", "dynamic_mapping",
    "unresolved_dataset", "unresolved_linked_service", "unresolved_child_pipeline",
    "unresolved_activity_reference", "unknown_properties", "cyclic_dependency", "unresolved_dependency", "dependency_outside_scope",
    "unwrapped_expression_like_string",
})

_COMPARISON = re.compile(r"(>=|<=|<>|!=|>|<)")
_AGGREGATE = re.compile(r"\b(max|min)\s*\(", re.I)
_FAMILIES = [
    ("sql", ("sql", "oracle", "postgre", "mysql", "maria", "db2", "teradata", "snowflake", "synapse",
             "redshift", "netezza", "vertica")),
    ("rest", ("rest", "http", "odata", "web")),
    ("nosql", ("cosmos", "mongo", "dynamics", "salesforce")),
    ("file", ("blob", "datalake", "adls", "file", "sftp", "ftp", "s3", "gcs", "hdfs", "parquet",
              "delimitedtext", "json", "orc", "avro", "binary", "excel", "xml", "delta")),
]
_HINT_FAMILY = {
    "sql": "sql", "db": "sql", "database": "sql", "dwh": "sql", "synapse": "sql",
    "lake": "file", "adls": "file", "datalake": "file", "blob": "file", "storage": "file",
    "file": "file", "files": "file", "parquet": "file", "csv": "file", "json": "file", "orc": "file",
    "api": "rest", "rest": "rest", "http": "rest",
}
_EXECUTE = {"executepipeline", "executedataflow", "executewranglingdataflow", "executessispackage",
            "sqlserverstoredprocedure"}
_PREFIX_TYPES = {
    "copy": {"copy"}, "lookup": {"lookup"}, "foreach": {"foreach"}, "loop": {"foreach", "until"},
    "if": {"ifcondition"}, "switch": {"switch"}, "until": {"until"}, "set": {"setvariable"},
    "append": {"appendvariable"}, "exec": _EXECUTE, "execute": _EXECUTE,
    "sp": {"sqlserverstoredprocedure"}, "usp": {"sqlserverstoredprocedure"},
    "proc": {"sqlserverstoredprocedure"}, "procedure": {"sqlserverstoredprocedure"},
    "web": {"webactivity", "webhook"}, "wait": {"wait"}, "fail": {"fail"},
    "validate": {"validation"}, "validation": {"validation"}, "filter": {"filter"},
    "delete": {"delete"}, "script": {"script"},
    "notebook": {"databricksnotebook", "synapsenotebook"},
    "dataflow": {"executedataflow", "executewranglingdataflow"},
    "df": {"executedataflow", "executewranglingdataflow"},
}
_WATERMARK_TOKENS = {"watermark", "wm", "incremental", "cdc"}
_SQL_READ_ROLES = {"source_query", "source_selection", "filter_condition", "sql_parameter", "source_path"}


# --- evidence helpers ------------------------------------------------------------------------------------


def _family(io: Optional[AdfIO]) -> Optional[str]:
    if io is None:
        return None
    text = " ".join(x for x in (io.connector_type, io.store, io.format, io.dataset_type,
                                io.connection_type) if x).lower()
    for fam, needles in _FAMILIES:
        if any(n in text for n in needles):
            return fam
    return None


def _tokens(name: str) -> list[str]:
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", name)
    return [t.lower() for t in re.split(r"[^A-Za-z0-9]+", spaced) if t]


def _short(text: Optional[str], n: int = 90) -> str:
    t = " ".join((text or "").split())
    return t if len(t) <= n else t[: n - 1] + "…"


def _source_text(act: AdfActivity) -> str:
    """Lowercased query / procedure / table text the activity reads (property evidence)."""
    parts = []
    for io in act.inputs:
        parts.extend(str(io.location.get(k, "")) for k in ("query", "stored_procedure", "table"))
    return " ".join(parts).lower()


def _comparison_consumers(act: AdfActivity, by_id: dict[str, AdfActivity]) -> list[tuple[AdfActivity, str, str]]:
    """Consumers that compare this output inside a query/filter (incremental evidence)."""
    out = []
    for u in act.output_consumers:
        consumer = by_id.get(u.activity_id)
        if consumer is None or u.via != "activity_output":
            continue
        for e in consumer.expressions:
            if e.property_path == u.property_path and e.role in _SQL_READ_ROLES \
                    and _COMPARISON.search(e.literal_text):
                out.append((consumer, u.property_path, _short(e.literal_text, 60)))
    return out


# --- tags ----------------------------------------------------------------------------------------------------


def _tag(act: AdfActivity, by_id: dict[str, AdfActivity]) -> None:
    tags: set[str] = set()
    t = (act.type or "").lower()
    refs = [r for e in act.expressions for r in e.references]
    roles = {e.role for e in act.expressions}

    if t == "lookup":
        text = _source_text(act)
        evidence = bool(_AGGREGATE.search(text)) or "watermark" in text
        if evidence and act.output_consumers:
            tags.add("watermark_lookup")
        if _AGGREGATE.search(text):
            tags.add("aggregate_query")
        if any(by_id.get(u.activity_id) and (by_id[u.activity_id].type or "").lower() == "foreach"
               and u.property_path == "typeProperties.items" for u in act.output_consumers):
            tags.add("metadata_driven_iteration")
    elif t == "foreach":
        items = [e for e in act.expressions if e.role == "loop_collection"]
        if any(r.kind == "activity_output" for e in items for r in e.references):
            tags.add("metadata_driven_loop")
        elif any(r.kind == "pipeline_parameter" for e in items for r in e.references):
            tags.add("parameter_driven_loop")
        elif not items:
            tags.add("static_loop")
        if act.configuration.get("is_sequential"):
            tags.add("sequential")
    elif t == "copy":
        if "source_query" in roles or "source_selection" in roles or "source_path" in roles:
            if any(r.kind == "activity_output" for e in act.expressions
                   if e.role in ("source_query", "source_selection", "source_path") for r in e.references):
                tags.add("dynamic_source_from_activity")
        if roles & {"destination_selection", "destination_path", "sink_script"}:
            tags.add("dynamic_destination")
        src = _family(act.inputs[0]) if act.inputs else None
        dst = _family(act.outputs[0]) if act.outputs else None
        if src and dst:
            act.configuration["direction"] = f"{src} -> {dst}"
        else:
            act.configuration.pop("direction", None)
        if "enable_staging" in act.configuration and act.configuration.get("enable_staging") is True:
            tags.add("staged_copy")
    elif t == "setvariable":
        for e in act.expressions:
            if e.role != "variable_value":
                continue
            kinds = {r.kind for r in e.references}
            if "activity_output" in kinds:
                tags.add("captures_activity_output")
            if "datetime" in e.function_groups and "activity_output" not in kinds:
                tags.add("captures_timestamp")
            if "pipeline_parameter" in kinds:
                tags.add("derives_from_parameter")
        if not any(e.role == "variable_value" for e in act.expressions) and not act.configuration.get(
                "is_pipeline_return_value"):
            tags.add("sets_constant")
        if act.configuration.get("is_pipeline_return_value"):
            tags.add("sets_pipeline_return_value")
    elif t == "executepipeline":
        tags.add("invokes_child_pipeline")
        if any(r.kind == "loop_item" for r in refs):
            tags.add("per_item_child_execution")
    elif t in ("webactivity", "webhook", "azurefunctionactivity"):
        if act.configuration.get("url_is_dynamic"):
            tags.add("dynamic_url")
    elif t in ("ifcondition", "switch", "until", "filter"):
        if any(r.kind == "activity_output" for r in refs):
            tags.add("depends_on_activity_output")

    # Consumer-side evidence (set on the consuming activity from the producer's record).
    for producer in by_id.values():
        if producer is act:
            continue
        if any(c is act for c, _, _ in _comparison_consumers(producer, by_id)) and \
                (producer.type or "").lower() in ("lookup", "setvariable"):
            tags.add("incremental_filter")
    if any(r.kind == "loop_item" for r in refs):
        tags.add("uses_loop_item")
    if any(r.kind == "pipeline_parameter" for r in refs):
        tags.add("parameterized")
    act.semantic_tags = sorted(tags)


# --- patterns ------------------------------------------------------------------------------------------------------


def _detect_patterns(acts: list[AdfActivity], by_id: dict[str, AdfActivity]) -> list[AdfPattern]:
    out: list[AdfPattern] = []

    def pattern(name: str, description: str, members: list[AdfActivity], evidence: list[str]) -> None:
        if members:
            out.append(AdfPattern(name=name, description=description,
                                  activities=[m.activity_id for m in members], evidence=sorted(set(evidence))))

    loops = [a for a in acts if "metadata_driven_loop" in a.semantic_tags]
    members, ev = [], []
    for loop in loops:
        for e in loop.expressions:
            for r in e.references:
                if e.role == "loop_collection" and r.kind == "activity_output" and r.name:
                    src = next((a for a in acts if a.name.lower() == r.name.lower()), None)
                    if src:
                        members += [src, loop]
                        ev.append(f"{loop.name}.items reads output of {src.name} ({r.path or ''})".replace(" ()", ""))
    pattern("metadata_driven_foreach",
            "A retrieval activity supplies the collection a ForEach iterates over.",
            list({m.activity_id: m for m in members}.values()), ev)

    members, ev = [], []
    for a in acts:
        if "watermark_lookup" in a.semantic_tags:
            for c, path, text in _comparison_consumers(a, by_id):
                members += [a, c]
                ev.append(f"{a.name} output compared in {c.name}.{path}: {text}")
    pattern("incremental_load",
            "A looked-up value is compared inside a downstream query/filter (watermark pattern).",
            list({m.activity_id: m for m in members}.values()), ev)

    execs = [a for a in acts if (a.type or "").lower() == "executepipeline"]
    pattern("parent_child_orchestration", "The pipeline invokes other pipelines.", execs,
            [f"{a.name} -> {a.configuration.get('pipeline')}" for a in execs])

    branches = [a for a in acts if (a.type or "").lower() in ("ifcondition", "switch")]
    pattern("conditional_routing", "Execution is routed by If/Switch activities.", branches,
            [f"{a.name} ({a.type})" for a in branches])

    handlers = [a for a in acts if any(c in ("Failed", "Completed", "Skipped")
                                       for d in a.depends_on for c in d.conditions)]
    pattern("failure_handling", "Activities run on failure, completion or skip of others.", handlers,
            [f"{a.name} runs on {','.join(sorted({c for d in a.depends_on for c in d.conditions}))}"
             for a in handlers])

    polls = [a for a in acts if (a.type or "").lower() == "until"]
    pattern("polling_loop", "An Until activity repeats until a condition holds.", polls,
            [f"{a.name}: {_short(_value_text(a.configuration.get('expression')))}" for a in polls])
    return out


# --- name hints -------------------------------------------------------------------------------------------------------


def _name_hints(act: AdfActivity) -> None:
    toks = _tokens(act.name)
    if not toks:
        return
    t = (act.type or "").lower()
    first = "".join(toks[:2]) if toks[:2] == ["for", "each"] else toks[0]
    if first == "foreach" or toks[:2] == ["for", "each"]:
        expected = {"foreach"}
    elif toks[:2] == ["get", "metadata"]:
        expected = {"getmetadata"}
    else:
        expected = _PREFIX_TYPES.get(first)
    if expected and act.type and t not in expected:
        add_issue(act.warnings, AdfParseIssue(
            code="name_suggests_different_type", severity=AdfIssueSeverity.warning,
            message=f"The name suggests {'/'.join(sorted(expected))}, but the activity type is "
                    f"{act.type}. The type and properties are authoritative."))

    if t == "copy" and ("to" in toks):
        i = toks.index("to")
        start = toks.index("from") + 1 if "from" in toks[:i] else 1
        src_hint = next((_HINT_FAMILY[x] for x in toks[start:i] if x in _HINT_FAMILY), None)
        dst_hint = next((_HINT_FAMILY[x] for x in toks[i + 1:] if x in _HINT_FAMILY), None)
        src = _family(act.inputs[0]) if act.inputs else None
        dst = _family(act.outputs[0]) if act.outputs else None
        bad = []
        if src_hint and src and src_hint != src:
            bad.append(f"source looks like {src_hint} by name but is {src} by configuration")
        if dst_hint and dst and dst_hint != dst:
            bad.append(f"destination looks like {dst_hint} by name but is {dst} by configuration")
        if bad:
            add_issue(act.warnings, AdfParseIssue(
                code="name_contradicts_properties", severity=AdfIssueSeverity.warning,
                message="; ".join(bad).capitalize() + ". The configuration is authoritative."))

    if _WATERMARK_TOKENS & set(toks) and not ({"watermark_lookup", "incremental_filter"} & set(act.semantic_tags)):
        add_issue(act.warnings, AdfParseIssue(
            code="name_hint_unverified", severity=AdfIssueSeverity.info,
            message="The name suggests incremental/watermark handling, but no query or downstream "
                    "comparison in the properties supports it."))
    if t == "lookup" and "max" in toks and not _AGGREGATE.search(_source_text(act)):
        add_issue(act.warnings, AdfParseIssue(
            code="name_hint_unverified", severity=AdfIssueSeverity.info,
            message="The name suggests a MAX lookup, but no MAX/MIN aggregate was found in its query."))
    if t == "lookup" and not act.output_consumers and act.configuration.get("first_row_only") is not None:
        add_issue(act.warnings, AdfParseIssue(
            code="lookup_output_unused", severity=AdfIssueSeverity.info,
            message="No expression in the pipeline reads this Lookup's output."))


# --- confidence -----------------------------------------------------------------------------------------------------------


def _confidence(act: AdfActivity) -> AdfConfidence:
    if get_spec(act.type) is None:
        return AdfConfidence.unsupported
    if act.missing_properties or act.empty_properties:
        return AdfConfidence.low
    if any(w.code in _PARTIAL_CODES for w in act.warnings) or any(
            e.parse_status == "unparsed" for e in act.expressions):
        return AdfConfidence.medium
    return AdfConfidence.high


# --- summaries -----------------------------------------------------------------------------------------------------------------


def _value_text(v) -> str:
    if v is None:
        return "(not set)"
    if isinstance(v, dict) and "expression" in v:
        return f"expression `{_short(str(v['expression']))}`"
    return f"`{_short(str(v))}`" if isinstance(v, str) else str(v)


def _refs_phrase(raw: str) -> str:
    e = analyze_expression(raw, "")
    parts = []
    for r in e.references:
        suffix = f" ({r.path.lstrip('.')})" if r.path else ""
        parts.append({
            "pipeline_parameter": f"pipeline parameter '{r.name}'",
            "global_parameter": f"global parameter '{r.name}'",
            "variable": f"variable '{r.name}'",
            "activity_output": f"output of activity '{r.name}'{suffix}",
            "loop_item": f"the current loop item{suffix}",
            "pipeline_system": f"pipeline system value {r.name}",
            "dataset_parameter": f"dataset parameter '{r.name}'",
            "trigger": f"trigger value {r.name}",
        }.get(r.kind, f"{r.kind} {r.name or ''}".strip()))
    if "datetime" in e.function_groups:
        parts.append("date/time functions")
    if e.unknown_functions:
        parts.append(f"unrecognised function(s) {', '.join(e.unknown_functions)}")
    if e.parse_status == "unparsed":
        parts.append("(could not be parsed)")
    return ", ".join(dict.fromkeys(parts))


def _expr_desc(v) -> str:
    if isinstance(v, dict) and "expression" in v:
        phrase = _refs_phrase(str(v["expression"]))
        return f"`{_short(str(v['expression']))}`" + (f" (uses {phrase})" if phrase else "")
    return _value_text(v)


def _io_desc(io: AdfIO) -> str:
    if io.kind == "dataflow":
        return f"data flow '{io.name}'"
    bits = []
    if io.kind == "inline":
        head = f"inline {io.connector_type or 'source'}"
    else:
        head = f"dataset '{io.name}'"
        if io.resolved is False:
            bits.append("not found in the discovered artifacts")
    ds_type = io.dataset_type or io.connector_type
    if ds_type:
        bits.append(ds_type)
    if io.format and io.format != ds_type:
        bits.append(f"format {io.format}")
    if io.store and io.store not in (ds_type or ""):
        bits.append(f"store {io.store}")
    if io.linked_service:
        bits.append(f"linked service '{io.linked_service}'" + (f" [{io.connection_type}]" if io.connection_type else ""))
    loc = io.location
    if loc.get("table"):
        bits.append(f"table {loc['table']}")
    if loc.get("path"):
        bits.append(f"path {loc['path']}")
    if loc.get("relative_url"):
        bits.append(f"relative URL {loc['relative_url']}")
    if loc.get("query"):
        bits.append("query " + ("expression " if loc.get("query_is_expression") else "") + f"`{_short(str(loc['query']), 70)}`")
    if loc.get("stored_procedure"):
        bits.append(f"stored procedure {loc['stored_procedure']}")
    if io.parameters:
        bits.append("dataset arguments: " + ", ".join(sorted(io.parameters)))
    return head + (f" ({'; '.join(bits)})" if bits else "")


def _children(act: AdfActivity) -> str:
    kids = [f"'{c.name}' ({c.type})" for c in act.activities]
    return ", ".join(kids[:4]) + (f" and {len(kids) - 4} more" if len(kids) > 4 else "")


def _dynamic(act: AdfActivity) -> str:
    by_role: dict[str, list[str]] = {}
    for e in act.expressions:
        if e.role in ("source_query", "source_selection", "source_path", "destination_selection",
                      "destination_path", "sink_script") and e.is_dynamic:
            what = _refs_phrase(e.raw)
            by_role.setdefault(e.role, [])
            if what and what not in by_role[e.role]:
                by_role[e.role].append(what)
    notes = [f"{role.replace('_', ' ')} is dynamic" + (f" (uses {'; '.join(w)})" if w else "")
             for role, w in by_role.items()]
    return ("; ".join(notes) + ".") if notes else ""


def _consumers(act: AdfActivity, by_id: dict[str, AdfActivity]) -> str:
    grouped: dict[str, list[str]] = {}
    for u in act.output_consumers:
        c = by_id.get(u.activity_id)
        grouped.setdefault(c.name if c else u.activity_id, []).append(u.property_path)
    if not grouped:
        return ""
    uses = [f"'{n}' ({paths[0]}" + (f" +{len(paths) - 1} more" if len(paths) > 1 else "") + ")"
            for n, paths in grouped.items()]
    shown = ", ".join(uses[:3]) + (f" and {len(uses) - 3} more activities" if len(uses) > 3 else "")
    return f" Consumed by: {shown}."


def summarize(act: AdfActivity, by_id: dict[str, AdfActivity]) -> str:
    t = (act.type or "").lower()
    c = act.configuration
    spec = get_spec(act.type)
    q = f"'{act.name}'"
    tail = _consumers(act, by_id)
    if act.state and act.state.lower() == "inactive":
        tail += f" The activity is deactivated (marked {c.get('on_inactive_mark_as', 'Succeeded')} without running)."

    if spec is None:
        refs = sorted({f"{r.kind} '{r.name}'" for r in act.references})
        return (f"Unrecognised activity type '{act.type or '(missing)'}' ({q}); its "
                f"{len(act.raw_properties)} propert{'y' if len(act.raw_properties) == 1 else 'ies'} and "
                f"{len(act.expressions)} expression(s) are preserved but not interpreted."
                + (f" References: {', '.join(refs)}." if refs else "") + tail)

    if t == "copy":
        src = _io_desc(act.inputs[0]) if act.inputs else "an unspecified source"
        dst = _io_desc(act.outputs[0]) if act.outputs else "an unspecified sink"
        tr = c.get("translator") or {}
        mapping = (" Column mapping is dynamic." if tr.get("dynamic") else
                   f" {tr['mapping_count']} explicit column mapping(s)." if tr.get("mapping_count") else
                   " Automatic column mapping." if tr.get("auto_mapping") else "")
        stg = f" Staged through linked service '{c['staging']['linked_service']}'." if c.get("staging") else ""
        return f"Copies data from {src} to {dst}.{mapping}{stg} {_dynamic(act)}".strip() + tail
    if t == "lookup":
        src = _io_desc(act.inputs[0]) if act.inputs else "an unspecified source"
        rows = "the first row only" if c.get("first_row_only") else "all rows" if c.get("first_row_only") is False else "rows"
        extra = " It retrieves an aggregate (MAX/MIN) value." if "aggregate_query" in act.semantic_tags else ""
        unused = "" if act.output_consumers else " Its output is not referenced by any expression."
        return f"Reads {rows} from {src} into the pipeline.{extra} {_dynamic(act)}".strip() + (tail or unused)
    if t in ("getmetadata", "validation", "delete"):
        io = _io_desc(act.inputs[0]) if act.inputs else "an unspecified dataset"
        verb = {"getmetadata": "Reads metadata of", "validation": "Waits until it can validate",
                "delete": "Deletes files/folders at"}[t]
        extra = ""
        if t == "getmetadata" and c.get("field_list") is not None:
            fl = c["field_list"]
            extra = f" Fields: {', '.join(str(x) for x in fl) if isinstance(fl, list) else _value_text(fl)}."
        if t == "validation" and c.get("timeout"):
            extra = f" Timeout {c['timeout']}."
        return f"{verb} {io}.{extra}" + tail
    if t == "foreach":
        mode = "sequentially" if c.get("is_sequential") else f"in parallel (batch count {c.get('batch_count')})"
        return (f"Iterates over {_expr_desc(c.get('items'))}, running {len(act.activities)} nested "
                f"activit{'y' if len(act.activities) == 1 else 'ies'} {mode}: {_children(act) or 'none'}." + tail)
    if t == "ifcondition":
        return (f"Evaluates {_expr_desc(c.get('expression'))}; runs {c.get('true_branch_count', 0)} "
                f"activit(ies) when true and {c.get('false_branch_count', 0)} when false." + tail)
    if t == "switch":
        cases = ", ".join(str(v) for v in c.get("cases") or []) or "none"
        return (f"Routes on {_expr_desc(c.get('on'))} to case(s) [{cases}]"
                f"{' plus a default branch' if c.get('has_default') else ''}; "
                f"{len(act.activities)} nested activit{'y' if len(act.activities) == 1 else 'ies'} in total." + tail)
    if t == "until":
        tmo = f", timeout {c['timeout']}" if c.get("timeout") else ""
        return (f"Repeats {len(act.activities)} nested activit{'y' if len(act.activities) == 1 else 'ies'} "
                f"until {_expr_desc(c.get('expression'))}{tmo}." + tail)
    if t == "filter":
        return f"Filters {_expr_desc(c.get('items'))} keeping items where {_expr_desc(c.get('condition'))}." + tail
    if t == "wait":
        return f"Pauses the pipeline for {_expr_desc(c.get('wait_seconds'))} second(s)." + tail
    if t == "fail":
        return (f"Fails the pipeline with message {_expr_desc(c.get('message'))} and error code "
                f"{_expr_desc(c.get('error_code'))}.")
    if t == "setvariable":
        if c.get("is_pipeline_return_value"):
            return "Sets the pipeline return value." + tail
        why = {"captures_timestamp": " (a timestamp)", "captures_activity_output": " (an activity result)",
               "derives_from_parameter": " (derived from a parameter)", "sets_constant": " (a constant)"}
        tag = next((why[x] for x in act.semantic_tags if x in why), "")
        vt = f" ({c['variable_type']})" if c.get("variable_type") else ""
        return f"Sets variable '{c.get('variable_name')}'{vt} to {_expr_desc(c.get('value'))}{tag}." + tail
    if t == "appendvariable":
        return f"Appends {_expr_desc(c.get('value'))} to array variable '{c.get('variable_name')}'." + tail
    if t == "executepipeline":
        params = ", ".join(sorted((c.get("parameters") or {}))) or "no parameters"
        wait = ("and waits for it" if c.get("wait_on_completion") else
                "without waiting" if c.get("wait_on_completion") is False else "(wait behavior not stated)")
        loop = " once per loop item" if "per_item_child_execution" in act.semantic_tags else ""
        return f"Invokes pipeline '{c.get('pipeline')}'{loop} {wait}, passing {params}." + tail
    if t in ("webactivity", "webhook", "azurefunctionactivity"):
        what = c.get("function_name") or (c.get("host") or _value_text(c.get("url")))
        kind = {"webactivity": "Calls", "webhook": "Calls (with callback)", "azurefunctionactivity": "Invokes Azure Function"}[t]
        return f"{kind} {c.get('method') or ''} {what}{' (dynamic URL)' if c.get('url_is_dynamic') else ''}.".replace("  ", " ") + tail
    if t in ("sqlserverstoredprocedure", "sqlpoolstoredprocedure"):
        n = len(c.get("parameters") or {})
        pool = f" in SQL pool '{c['sql_pool']}'" if c.get("sql_pool") else ""
        return f"Executes stored procedure {_value_text(c.get('stored_procedure'))}{pool} with {n} parameter(s)." + tail
    if t == "databricksjob":
        jp = c.get("job_parameters")
        n = len(jp) if isinstance(jp, (dict, list)) else 0
        return f"Runs Databricks job {_value_text(c.get('job_id'))} with {n} job parameter(s)." + tail
    if t == "script":
        blocks = c.get("scripts") or []
        kinds = ", ".join(c.get("statement_kinds") or []) or "unknown statements"
        return f"Runs {len(blocks)} SQL script block(s) ({kinds})." + tail
    if t in ("executedataflow", "executewranglingdataflow"):
        return f"Runs data flow '{c.get('dataflow')}'." + tail
    # Remaining compute/external types: registry description + the key reference.
    refs = sorted({f"{r.kind} '{r.name}'" for r in act.references})
    return f"{spec.description}" + (f" References: {', '.join(refs)}." if refs else "") + tail


# --- entry point ----------------------------------------------------------------------------------------------------------------------


def finalize_pipeline(pipeline: AdfPipeline) -> None:
    """(Re)compute tags, patterns, name checks, confidence, summaries and counts."""
    acts = list(walk(pipeline.activities))
    by_id = {a.activity_id: a for a in acts}
    for a in acts:
        a.warnings = [w for w in a.warnings if w.code not in _OWNED_CODES]
    for a in acts:
        _tag(a, by_id)
    pipeline.patterns = [p for p in pipeline.patterns if p.name in CROSS_PIPELINE_PATTERNS] + \
        _detect_patterns(acts, by_id)
    for a in acts:
        _name_hints(a)
        a.confidence = _confidence(a)
        a.semantic_summary = summarize(a, by_id)

    counts: dict[str, int] = {}
    for a in acts:
        counts[a.type or "<missing>"] = counts.get(a.type or "<missing>", 0) + 1
    pipeline.activity_total = len(acts)
    pipeline.activity_type_counts = {k: counts[k] for k in sorted(counts)}
    pipeline.unknown_activity_types = sorted({a.type for a in acts if a.type and get_spec(a.type) is None})
