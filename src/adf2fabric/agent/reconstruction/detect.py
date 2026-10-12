"""Incompleteness detection that has no reconstruction rule of its own.

Every finding is classified as one of four different problems with different fixes:

    invalid_json              the file cannot be parsed (truncated / malformed / empty)
    missing_artifact          a referenced dataset, linked service or pipeline is not there
    incomplete_configuration  the artifact is there but properties / values are missing or unresolved
    unsupported_functionality it is complete, but Fabric has no equivalent

Detection is a pure function of the persisted semantic record, the repository inventory and
read-only access to the original files. Gaps a rule can bridge (copy endpoints, dependencies,
output contracts, arguments, renamed references, patterns) are detected in ``rules_*``; this module
reports the rest and states, for each, what information would resolve it.
"""

from __future__ import annotations

import json
from adf2fabric.agent.reconstruction.context import Collector, PipelineCtx
from adf2fabric.agent.reconstruction.index import ProjectEvidence
from adf2fabric.project.discovery import AdfActivity
from adf2fabric.project.reconstruction import Finding, IncompletenessClass as C, Severity

# What the Fabric conversion cannot do without a human supplying a value (the semantic record
# deliberately does not read secrets / connection strings / external items).
_FABRIC_CONFIG: dict[str, tuple[str, str, str]] = {
    "script": ("script_database",
               "The Fabric Script activity needs the database name; ADF takes it from the linked service, whose "
               "connection string is deliberately not read.", "the target database name"),
    "databricksnotebook": ("databricks_cluster",
                           "The Databricks cluster (existing id or new-cluster settings) lives in the ADF linked service.",
                           "the existing cluster id or the new-cluster settings"),
    "azuredataexplorercommand": ("kusto_database", "The Kusto database lives in the ADF linked service.",
                                 "the Kusto database name"),
    "synapsenotebook": ("notebook_item", "The notebook must exist as a Fabric notebook.",
                        "the migrated Fabric notebook"),
    "sparkjob": ("sparkjob_item", "The Spark job definition must exist in Fabric.", "the Fabric Spark job definition"),
    "executedataflow": ("dataflow_item", "The mapping data flow must be re-authored as a Dataflow Gen2.",
                        "the Dataflow Gen2 that replaces the data flow"),
    "executewranglingdataflow": ("dataflow_item", "The wrangling data flow must exist as a Dataflow Gen2.",
                                 "the Dataflow Gen2"),
}


def classify_json_failure(text: str) -> tuple[str, str]:
    """('empty_json_file' | 'truncated_json' | 'invalid_json', explanation) for a file that does not parse."""
    if not text.strip():
        return "empty_json_file", "the file is empty"
    try:
        json.loads(text)
    except json.JSONDecodeError as exc:
        stack: list[str] = []
        in_str = esc = False
        mismatch = False
        for ch in text:
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch in "{[":
                stack.append(ch)
            elif ch in "}]":
                if not stack or {"}": "{", "]": "["}[ch] != stack[-1]:
                    mismatch = True
                    break
                stack.pop()
        if not mismatch and (stack or in_str):
            return "truncated_json", (
                f"the file ends inside an open {'string' if in_str else 'structure'} "
                f"({len(stack)} unclosed bracket(s)); it looks cut off - {exc.msg} near character {exc.pos}")
        return "invalid_json", f"{exc.msg} at line {exc.lineno}, column {exc.colno}"
    return "invalid_json", "the file did not load"  # pragma: no cover - the inventory said it failed


# --- project / file level ----------------------------------------------------------------------------------


def detect_sources(ev: ProjectEvidence, out: Collector) -> list[Finding]:
    """File-level problems: unreadable JSON, dangling artifact references between artifacts."""
    found: list[Finding] = []
    for cand in ev.broken_files():
        text = None  # None = the file could not be read at all (an empty file is "", which IS readable)
        if ev.source_root is not None:
            try:
                text = (ev.source_root / cand.path).read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                text = None
        code, why = classify_json_failure(text) if text is not None else ("invalid_json", cand.note or "unreadable")
        f = out.finding(C.invalid_json, code, f"{cand.path}: {why}.", source_path=cand.path, severity=Severity.error,
                        detail=cand.note)
        found.append(f)
        out.blocker(
            [f], pipeline=None, activity=None, reason=f"{cand.artifact_kind.value} file is {code.replace('_', ' ')}.",
            information_required=[f"a complete, valid copy of {cand.path}"],
            conversion_impact="Everything that references this artifact is analysed and converted without it; "
                              "references to it are reported as unresolved.",
            minimum_information="re-export the file from ADF (or restore it from the repository history)",
            closest_safe_implementation="the artifact is skipped; dependent activities are blocked or flagged")
    for ds in ev.discovery.datasets:
        r = ds.linked_service_ref
        if r is not None and r.name.lower() not in ev.linked_services:
            found.append(out.finding(
                C.missing_artifact, "dataset_linked_service_missing",
                f"Dataset '{ds.name}' uses linked service '{r.name}', which is not among the discovered artifacts.",
                source_path=ds.path, property_path="properties.linkedServiceName", severity=Severity.error))
    for t in ev.discovery.triggers:
        for pl in t.pipeline_refs:
            if pl.lower() not in ev.pipelines:
                found.append(out.finding(
                    C.missing_artifact, "trigger_pipeline_missing",
                    f"Trigger '{t.name}' starts pipeline '{pl}', which is not among the discovered artifacts.",
                    source_path=t.path, severity=Severity.warning))
    return found


# --- per pipeline ------------------------------------------------------------------------------------------------


def _covered(pc: PipelineCtx, a: AdfActivity, path: str) -> bool:
    return any(f.activity_id == a.activity_id and f.property_path == path for f in pc.out.findings)


def _block(pc: PipelineCtx, f: Finding, a: AdfActivity, reason: str, info: list[str], minimum: str,
           closest: str = "") -> None:
    pc.out.blocker(
        [f], pipeline=pc.pipe.name, activity=a, reason=reason, information_required=info,
        conversion_impact=f"{a.type} '{a.name}' cannot be converted faithfully until this is supplied.",
        minimum_information=minimum,
        closest_safe_implementation=closest or "the activity is converted as authored and flagged for manual completion")


def detect_pipeline(pc: PipelineCtx) -> None:
    ev, out, p = pc.ev, pc.out, pc.pipe
    for i in p.issues:
        if i.severity.value == "error":
            f = out.finding(C.incomplete_configuration, f"pipeline_{i.code}", i.message, pipeline=p.name,
                            source_path=p.path, property_path=i.property_path, severity=Severity.error)
            out.blocker([f], pipeline=p.name, activity=None, reason=i.message, information_required=[i.message],
                        conversion_impact="The pipeline definition is structurally broken.",
                        minimum_information="a corrected pipeline definition",
                        closest_safe_implementation="activities that could be read are converted individually")
    for a in ev.acts(p):
        _config(pc, a)
        _params(pc, a)
        _expressions(pc, a)
        _bodies(pc, a)
        _fabric_config(pc, a)
        _unsupported(pc, a)


def _config(pc: PipelineCtx, a: AdfActivity) -> None:
    for path in a.missing_properties:
        if _covered(pc, a, path):
            continue
        f = pc.out.finding(C.incomplete_configuration, "missing_required_property",
                           f"{a.type} '{a.name}' has no {path}.", pipeline=pc.pipe.name, activity=a,
                           property_path=path, severity=Severity.error, source_path=pc.pipe.path)
        _block(pc, f, a, f"required property {path} is absent and nothing in the project determines it",
               [f"a value for {path}"], f"set {path} on activity '{a.name}'")
    for path in a.empty_properties:
        if _covered(pc, a, path):
            continue
        f = pc.out.finding(C.incomplete_configuration, "empty_required_property",
                           f"{a.type} '{a.name}': {path} is present but empty.", pipeline=pc.pipe.name, activity=a,
                           property_path=path, severity=Severity.error, source_path=pc.pipe.path)
        _block(pc, f, a, f"{path} is explicitly empty", [f"a non-empty value for {path}"],
               f"fill {path} on activity '{a.name}'")


def _params(pc: PipelineCtx, a: AdfActivity) -> None:
    for w in a.warnings:
        if w.code in ("undeclared_parameter", "undeclared_variable"):
            what = "parameter" if w.code == "undeclared_parameter" else "variable"
            f = pc.out.finding(C.incomplete_configuration, f"unresolved_{what}", w.message, pipeline=pc.pipe.name,
                               activity=a, property_path=w.property_path, severity=Severity.error,
                               source_path=pc.pipe.path)
            _block(pc, f, a, f"an expression reads a {what} the pipeline does not declare",
                   [f"the {what}'s declaration (name, type, default)"],
                   f"declare the {what} on pipeline '{pc.pipe.name}', or correct the expression",
                   "the expression is kept verbatim; the converted pipeline fails validation until it is declared")
    for e in a.expressions:
        for r in e.references:
            if r.kind == "global_parameter" and r.name and r.name.lower() not in pc.ev.global_parameters:
                f = pc.out.finding(
                    C.incomplete_configuration, "unresolved_global_parameter",
                    f"{a.type} '{a.name}' reads global parameter '{r.name}', which the factory does not define.",
                    pipeline=pc.pipe.name, activity=a, property_path=e.property_path, severity=Severity.error,
                    source_path=pc.pipe.path)
                _block(pc, f, a, "the global parameter is not defined in the discovered factory configuration",
                       [f"the definition and default of global parameter '{r.name}'"],
                       "add the factory configuration file, or the parameter value")


def _expressions(pc: PipelineCtx, a: AdfActivity) -> None:
    table = {
        "expression_unparsed": ("unresolved_expression", Severity.error, "the expression does not parse",
                                "a corrected expression"),
        "unknown_expression_function": ("unresolved_expression", Severity.warning,
                                        "the expression uses a function outside the documented set", "a documented equivalent"),
        "dynamic_reference_name": ("unresolved_expression", Severity.warning,
                                   "a computed activity/variable name cannot be resolved statically", "the fixed target name"),
        "unwrapped_expression_like_string": ("unresolved_expression", Severity.info,
                                             "a plain string looks like dynamic content but is not an Expression",
                                             "confirmation whether it is dynamic"),
    }
    for w in a.warnings:
        if w.code in table:
            code, sev, why, info = table[w.code]
            f = pc.out.finding(C.incomplete_configuration, code, f"{a.type} '{a.name}': {w.message}",
                               pipeline=pc.pipe.name, activity=a, property_path=w.property_path, severity=sev,
                               source_path=pc.pipe.path, detail=w.code)
            if sev is not Severity.info:
                _block(pc, f, a, why, [info], f"correct {w.property_path or 'the expression'} on '{a.name}'",
                       "the expression is preserved exactly as authored and flagged")


def _bodies(pc: PipelineCtx, a: AdfActivity) -> None:
    for i, blk in enumerate(a.configuration.get("scripts") or []):
        if not (blk.get("text") or "").strip() and not blk.get("is_dynamic"):
            path = f"typeProperties.scripts[{i}].text"
            f = pc.out.finding(C.incomplete_configuration, "empty_script_body",
                               f"Script block {i} of '{a.name}' has no SQL text.", pipeline=pc.pipe.name, activity=a,
                               property_path=path, severity=Severity.error, source_path=pc.pipe.path)
            _block(pc, f, a, "a script block with no statements; SQL is never invented without evidence",
                   [f"the SQL text of script block {i}"], f"supply the statements for block {i}")
    for io in [*a.inputs, *a.outputs]:
        if io.location.get("query") == "" and not io.location.get("query_is_expression"):
            f = pc.out.finding(C.incomplete_configuration, "empty_query",
                               f"{a.type} '{a.name}': the {io.role} query is empty.", pipeline=pc.pipe.name,
                               activity=a, property_path=f"typeProperties.{io.role}.query", severity=Severity.error,
                               source_path=pc.pipe.path)
            _block(pc, f, a, "an empty source query", ["the query text (or remove the property to read the whole table)"],
                   f"supply the {io.role} query")


def _fabric_config(pc: PipelineCtx, a: AdfActivity) -> None:
    key = (a.type or "").lower()
    if key in _FABRIC_CONFIG:
        code, msg, info = _FABRIC_CONFIG[key]
        f = pc.out.finding(C.incomplete_configuration, f"fabric_config_{code}",
                           f"{a.type} '{a.name}': {msg}", pipeline=pc.pipe.name, activity=a, severity=Severity.info,
                           source_path=pc.pipe.path, detail="needed to run in Fabric")
        pc.out.blocker([f], pipeline=pc.pipe.name, activity=a, reason=msg, information_required=[info],
                       conversion_impact="A bound scaffold is generated; it cannot run until this is supplied.",
                       minimum_information=info, closest_safe_implementation="the Fabric activity with a placeholder id")
    if key in ("webactivity", "webhook"):
        for e in a.expressions:
            if e.property_path == "typeProperties.url" and e.is_dynamic:
                f = pc.out.finding(C.incomplete_configuration, "fabric_config_dynamic_url",
                                   f"{a.type} '{a.name}': the URL is dynamic; Fabric needs a fixed base URL on the "
                                   "connection.", pipeline=pc.pipe.name, activity=a, property_path=e.property_path,
                                   severity=Severity.warning, source_path=pc.pipe.path)
                pc.out.blocker([f], pipeline=pc.pipe.name, activity=a, reason="a fully dynamic URL cannot be split",
                               information_required=["the fixed base URL of the endpoint"],
                               conversion_impact="The URL is kept as the relative URL and flagged.",
                               minimum_information="the base URL",
                               closest_safe_implementation="Web activity with the dynamic part as relativeUrl")


def _unsupported(pc: PipelineCtx, a: AdfActivity) -> None:
    from adf2fabric.agent.conversion import connectors  # lazy: avoids a package import cycle
    from adf2fabric.agent.conversion.registry import rule_for

    rule = rule_for(a.type)
    if rule is None or rule.strategy.value == "manual":
        reason = ("no conversion rule exists for this activity type" if rule is None
                  else "Fabric has no equivalent for this activity type")
        pc.out.finding(C.unsupported_functionality, "unsupported_activity_type",
                       f"{a.type} '{a.name}': {reason}.", pipeline=pc.pipe.name, activity=a, severity=Severity.warning,
                       source_path=pc.pipe.path)
    for io in [*a.inputs, *a.outputs]:
        if io.connection_type and connectors.verdict(io.connection_type)[0] == "unsupported":
            pc.out.finding(C.unsupported_functionality, "unsupported_connector",
                           f"{a.type} '{a.name}': connector {io.connection_type} has no Fabric counterpart "
                           f"({connectors.verdict(io.connection_type)[1]}).", pipeline=pc.pipe.name, activity=a,
                           severity=Severity.error, source_path=pc.pipe.path)
