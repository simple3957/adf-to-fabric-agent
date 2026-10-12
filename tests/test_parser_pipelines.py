"""Activity-level pipeline parser (``parser_pipelines``) — offline and deterministic.

Covers the expression engine, the activity registry, representative activity types,
nesting, dependency/control-flow analysis, reference checks, cross-artifact
resolution, unknown/malformed input, secret safety and determinism.
"""

from __future__ import annotations

import json

import pytest

from adf2fabric.agent.discovery.adf_semantic import parser, parser_pipelines as pp
from adf2fabric.agent.discovery.adf_semantic.activity_registry import ACTIVITY_REGISTRY, get_spec
from adf2fabric.agent.discovery.adf_semantic.expressions import analyze_expression
from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.project.discovery import AdfConfidence, AdfOperationKind, AdfSemanticCategory
from tests.test_adf_semantic_discovery import _make_project, _run

# --- builders ----------------------------------------------------------------------------------


def ref(kind: str, name: str, **params) -> dict:
    r = {"referenceName": name, "type": f"{kind}Reference"}
    if params:
        r["parameters"] = params
    return r


def expr(text: str) -> dict:
    return {"value": text, "type": "Expression"}


def act(name: str, type_: str, tp: dict | None = None, deps=None, **extra) -> dict:
    node = {"name": name, "type": type_, "dependsOn": deps or []}
    if tp is not None:
        node["typeProperties"] = tp
    node.update(extra)
    return node


def dep(name: str, *conds: str) -> dict:
    return {"activity": name, "dependencyConditions": list(conds) or ["Succeeded"]}


def pipe(*activities: dict, name="PL", parameters=None, variables=None, **props) -> dict:
    p = {"activities": list(activities), **props}
    if parameters:
        p["parameters"] = {k: {"type": "String"} for k in parameters}
    if variables:
        p["variables"] = {k: {"type": t} for k, t in variables.items()}
    return {"name": name, "properties": p}


def parse(*activities: dict, **kw):
    return pp.parse_pipeline(pipe(*activities, **kw), "pipeline/PL.json")


def one(*activities: dict, **kw):
    """Parse and return the activity named like the first one given."""
    p = parse(*activities, **kw)
    return next(a for a in walk(p.activities) if a.name == activities[0]["name"])


def codes(a) -> set[str]:
    return {w.code for w in a.warnings}


def sql_copy(name="Copy", query="SELECT 1", sink_type="ParquetSink", **tp):
    return act(name, "Copy", {
        "source": {"type": "AzureSqlSource", "sqlReaderQuery": query},
        "sink": {"type": sink_type}, **tp},
        inputs=[ref("Dataset", "DS_SQL")], outputs=[ref("Dataset", "DS_LAKE")])


# =============================================================================
# Expression engine
# =============================================================================


def test_expression_references_and_functions():
    e = analyze_expression(
        "@concat(pipeline().parameters.env, variables('v'), activity('A').output.firstRow.x, "
        "formatDateTime(utcnow(), 'yyyy'), pipeline().globalParameters.g, pipeline().RunId)", "p")
    kinds = {(r.kind, r.name) for r in e.references}
    assert ("pipeline_parameter", "env") in kinds and ("variable", "v") in kinds
    assert ("activity_output", "A") in kinds and ("global_parameter", "g") in kinds
    assert ("pipeline_system", "RunId") in kinds
    assert next(r for r in e.references if r.kind == "activity_output").path == ".output.firstRow.x"
    assert {"datetime", "string"} <= set(e.function_groups)
    assert e.unknown_functions == [] and e.parse_status == "parsed"


def test_expression_forms():
    assert analyze_expression("@pipeline().parameters.x", "p").form == "expression"
    interp = analyze_expression("SELECT * FROM t WHERE d > '@{activity('L').output.firstRow.w}'", "p")
    assert interp.form == "interpolated" and "WHERE d >" in interp.literal_text
    assert analyze_expression("@@not_dynamic", "p").form == "escaped_literal"
    lit = analyze_expression("plain text", "p")
    assert lit.form == "literal" and lit.is_dynamic is False


def test_expression_conditional_collection_and_optional_access():
    e = analyze_expression("@if(and(equals(length(item().rows), 0), not(empty(variables('v')))), 'a', 'b')", "p")
    assert {"logical", "collection"} <= set(e.function_groups)
    assert {r.kind for r in e.references} == {"loop_item", "variable"}
    e2 = analyze_expression("@activity('X')?.output?['a b'][0]", "p")
    assert e2.references[0].path == ".output['a b'][0]"


def test_expression_function_names_are_case_insensitive():
    e = analyze_expression("@CONCAT('a', UtcNow())", "p")
    assert e.unknown_functions == [] and "utcNow" in e.functions


def test_unknown_function_flagged_and_preserved():
    e = analyze_expression("@frobnicate(1)", "p")
    assert e.unknown_functions == ["frobnicate"] and e.raw == "@frobnicate(1)"


@pytest.mark.parametrize("raw", ["@concat(", "@{unclosed", "@bare", "@", "@concat('a'", "@a.b.", "@'x"])
def test_malformed_expression_is_unparsed_and_exact(raw):
    e = analyze_expression(raw, "p")
    assert e.parse_status == "unparsed" and e.error and e.raw == raw


def test_expressions_are_never_evaluated():
    e = analyze_expression("@add(1, 2)", "p")
    assert e.raw == "@add(1, 2)" and "3" not in e.model_dump_json().replace("1, 2", "")


# =============================================================================
# Registry coverage
# =============================================================================

PROMPT_TYPES = [
    "Copy", "ExecuteDataFlow", "ExecuteSSISPackage", "Script", "SqlServerStoredProcedure", "Lookup",
    "GetMetadata", "DatabricksNotebook", "DatabricksSparkJar", "DatabricksSparkPython", "HDInsightHive",
    "HDInsightPig", "HDInsightMapReduce", "HDInsightStreaming", "HDInsightSpark", "Custom",
    "AzureMLBatchExecution", "IfCondition", "Switch", "ForEach", "Until", "ExecutePipeline", "Wait",
    "Fail", "Validation", "Filter", "SetVariable", "AppendVariable", "WebActivity", "WebHook",
    "AzureFunctionActivity", "SynapseNotebook", "Delete",
]


def test_registry_covers_every_required_type():
    for t in PROMPT_TYPES:
        assert get_spec(t) is not None, t
    assert get_spec("copy") is get_spec("COPY")


def _minimal_value(key: str):
    return {"source": {"type": "S"}, "sink": {"type": "K"}, "dataset": ref("Dataset", "D"),
            "pipeline": ref("Pipeline", "P"), "dataflow": ref("DataFlow", "F"),
            "activities": [{"name": "child", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}],
            "scripts": [{"type": "Query", "text": "SELECT 1"}],
            "notebook": ref("Notebook", "N"), "sparkJob": ref("SparkJobDefinition", "J"),
            "packageLocation": {"type": "SSISDB"}}.get(key, "x")


@pytest.mark.parametrize("spec", sorted(ACTIVITY_REGISTRY.values(), key=lambda s: s.type), ids=lambda s: s.type)
def test_every_registry_type_yields_a_complete_semantic_record(spec):
    tp = {}
    for path in spec.required:
        if path.startswith("typeProperties."):
            tp[path.split(".", 1)[1]] = _minimal_value(path.split(".", 1)[1])
    node = act("A1", spec.type, tp, linkedServiceName=ref("LinkedService", "LS"))
    a = one(node)
    assert a.activity_id == "PL/A1" and a.name == "A1" and a.type == spec.type
    assert a.semantic_category == spec.category and a.operation in a.operations
    # Script's primary operation is derived from its statements (a pure SELECT is a read).
    assert spec.type == "Script" or a.operation == spec.operations[0]
    assert a.semantic_summary and a.source_location.pointer == "properties.activities[0]"
    assert a.missing_properties == [], a.missing_properties
    assert a.confidence in (AdfConfidence.high, AdfConfidence.medium)
    assert a.raw_properties is not None and "unknown_activity_type" not in codes(a)


# =============================================================================
# Representative activities
# =============================================================================


def test_copy_extracts_endpoints_query_mapping_and_direction():
    node = sql_copy(query="SELECT * FROM dbo.Customer", translator={
        "type": "TabularTranslator",
        "mappings": [{"source": {"name": "Id"}, "sink": {"name": "CustomerId", "type": "Int64"}}]})
    a = one(node)
    src, dst = a.inputs[0], a.outputs[0]
    assert (src.role, src.name, src.connector_type) == ("source", "DS_SQL", "AzureSqlSource")
    assert src.location["query"] == "SELECT * FROM dbo.Customer" and dst.connector_type == "ParquetSink"
    assert a.configuration["translator"]["mapping_count"] == 1
    assert a.configuration["direction"] == "sql -> file"
    assert (a.semantic_category, a.operation) == (AdfSemanticCategory.data_movement, AdfOperationKind.read)
    assert a.operations == [AdfOperationKind.read, AdfOperationKind.write]
    assert a.dataset_refs == ["DS_SQL", "DS_LAKE"] or sorted(a.dataset_refs) == ["DS_LAKE", "DS_SQL"]
    assert "SELECT * FROM dbo.Customer" in a.semantic_summary


def test_copy_name_is_a_hint_verified_against_configuration():
    good = one(sql_copy("Copy_Customer_From_SQL_To_Lake"))
    assert "name_contradicts_properties" not in codes(good)
    # Same name, but the real sink is SQL: the configuration wins and the clash is reported.
    node = act("Copy_Customer_From_SQL_To_Lake", "Copy", {
        "source": {"type": "AzureSqlSource"}, "sink": {"type": "AzureSqlSink"}},
        inputs=[ref("Dataset", "A")], outputs=[ref("Dataset", "B")])
    bad = one(node)
    assert "name_contradicts_properties" in codes(bad)
    assert bad.configuration["direction"] == "sql -> sql"
    assert "lake" not in bad.semantic_summary.lower()


def test_copy_with_missing_sink_is_low_confidence_not_dropped():
    a = one(act("C", "Copy", {"source": {"type": "S"}}))
    assert a.missing_properties == ["typeProperties.sink"] and a.confidence == AdfConfidence.low


def test_watermark_pattern_is_verified_from_query_and_consumer():
    lookup = act("Lookup_Max_Watermark", "Lookup", {
        "source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT MAX(ModifiedDate) AS wm FROM dbo.Customer"},
        "dataset": ref("Dataset", "DS_SQL"), "firstRowOnly": True})
    copy = act("Copy_Delta", "Copy", {
        "source": {"type": "AzureSqlSource", "sqlReaderQuery": expr(
            "SELECT * FROM dbo.Customer WHERE ModifiedDate > '@{activity('Lookup_Max_Watermark').output.firstRow.wm}'")},
        "sink": {"type": "ParquetSink"}}, deps=[dep("Lookup_Max_Watermark")],
        inputs=[ref("Dataset", "DS_SQL")], outputs=[ref("Dataset", "DS_LAKE")])
    p = parse(lookup, copy)
    by = {a.name: a for a in p.activities}
    assert "watermark_lookup" in by["Lookup_Max_Watermark"].semantic_tags
    assert "incremental_filter" in by["Copy_Delta"].semantic_tags
    assert [x.name for x in p.patterns] == ["incremental_load"]
    assert by["Lookup_Max_Watermark"].output_consumers[0].activity_id == "PL/Copy_Delta"
    assert by["Copy_Delta"].expressions[0].role == "source_query"
    assert "Consumed by" in by["Lookup_Max_Watermark"].semantic_summary


def test_name_claiming_watermark_without_evidence_is_flagged_not_believed():
    lookup = act("Lookup_Max_Watermark", "Lookup", {
        "source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT * FROM cfg.Settings"},
        "dataset": ref("Dataset", "D")})
    a = one(lookup)
    assert "watermark_lookup" not in a.semantic_tags
    assert {"name_hint_unverified", "lookup_output_unused"} <= codes(a)


def test_foreach_loop_scope_nesting_and_dependency_behavior():
    lookup = act("Lookup_Files", "GetMetadata", {"dataset": ref("Dataset", "D"), "fieldList": ["childItems"]})
    inner = act("Copy_One", "Copy", {"source": {"type": "S"}, "sink": {"type": "K"}},
                inputs=[ref("Dataset", "D2", fileName=expr("@item().name"))])
    loop = act("ForEach_Process_Files", "ForEach", {
        "items": expr("@activity('Lookup_Files').output.childItems"), "isSequential": False,
        "batchCount": 5, "activities": [inner]}, deps=[dep("Lookup_Files")])
    p = parse(lookup, loop)
    f = next(a for a in p.activities if a.name == "ForEach_Process_Files")
    assert f.operation == AdfOperationKind.loop and "metadata_driven_loop" in f.semantic_tags
    assert f.configuration["batch_count"] == 5 and f.configuration["is_sequential"] is False
    child = f.activities[0]
    assert child.activity_id == "PL/ForEach_Process_Files/Copy_One"
    assert child.parent_activity_id == f.activity_id and child.nesting_path == ["ForEach_Process_Files"]
    assert child.branch == "activities" and "uses_loop_item" in child.semantic_tags
    assert any(e.role == "source_selection" for e in child.expressions)
    assert "item()" not in "".join(w.code for w in child.warnings)
    assert p.activity_total == 3 and [x.name for x in p.patterns] == ["metadata_driven_foreach"]
    assert "Iterates over" in f.semantic_summary and "in parallel" in f.semantic_summary


def test_foreach_default_batch_count_is_marked_as_default():
    a = one(act("F", "ForEach", {"items": expr("@createArray(1,2)"), "activities": [
        act("W", "Wait", {"waitTimeInSeconds": 1})]}))
    assert a.configuration["batch_count"] == 20 and a.configuration["batch_count_is_default"] is True


def test_set_variable_is_classified_by_expression_and_use_not_name():
    ts = act("Set_Last_Load_Time", "SetVariable", {"variableName": "ts", "value": expr("@utcnow()")})
    const = act("Set_Last_Load_Time2", "SetVariable", {"variableName": "ts", "value": "2020-01-01"})
    sp = act("Log", "SqlServerStoredProcedure", {
        "storedProcedureName": "[dbo].[usp_log]",
        "storedProcedureParameters": {"Ts": {"value": expr("@variables('ts')"), "type": "String"}}},
        deps=[dep("Set_Last_Load_Time")], linkedServiceName=ref("LinkedService", "LS"))
    p = parse(ts, const, sp, variables={"ts": "String"})
    by = {a.name: a for a in p.activities}
    assert "captures_timestamp" in by["Set_Last_Load_Time"].semantic_tags
    assert "sets_constant" in by["Set_Last_Load_Time2"].semantic_tags  # same naming, different truth
    assert by["Set_Last_Load_Time"].variables_written == ["ts"]
    assert by["Log"].variables_read == ["ts"]
    use = by["Set_Last_Load_Time"].output_consumers[0]
    assert (use.activity_id, use.via, use.detail) == ("PL/Log", "variable", "ts")
    assert "timestamp" in by["Set_Last_Load_Time"].semantic_summary
    assert by["Log"].configuration["parameters"]["Ts"]["is_expression"] is True


def test_variable_checks():
    p = parse(act("S", "SetVariable", {"variableName": "nope", "value": "x"}),
              act("A", "AppendVariable", {"variableName": "s", "value": "x"}),
              variables={"s": "String"})
    by = {a.name: a for a in p.activities}
    assert "undeclared_variable" in codes(by["S"]) and "append_to_non_array" in codes(by["A"])


def test_if_switch_until_filter_wait_fail():
    ifc = act("If1", "IfCondition", {
        "expression": expr("@equals(pipeline().parameters.mode, 'full')"),
        "ifTrueActivities": [act("T", "Wait", {"waitTimeInSeconds": 1})],
        "ifFalseActivities": [act("F", "Fail", {"message": "no", "errorCode": "1"})]})
    sw = act("Sw", "Switch", {"on": expr("@pipeline().parameters.mode"), "cases": [
        {"value": "a", "activities": [act("CA", "Wait", {"waitTimeInSeconds": 1})]},
        {"value": "a", "activities": []}], "defaultActivities": [act("D", "Wait", {"waitTimeInSeconds": 2})]})
    un = act("Un", "Until", {"expression": expr("@equals(variables('done'), true)"), "timeout": "0.01:00:00",
                             "activities": [act("W", "Wait", {"waitTimeInSeconds": 5})]})
    fl = act("Fi", "Filter", {"items": expr("@createArray(1,2)"), "condition": expr("@greater(item(), 1)")})
    p = parse(ifc, sw, un, fl, parameters=["mode"], variables={"done": "Boolean"})
    by = {a.name: a for a in walk(p.activities)}
    assert by["T"].branch == "ifTrue" and by["F"].branch == "ifFalse"
    assert by["CA"].branch == "case:a" and by["D"].branch == "default"
    assert by["If1"].configuration["true_branch_count"] == 1 and by["If1"].operation == AdfOperationKind.branch
    assert by["If1"].expressions[0].role == "branch_condition"
    assert by["Sw"].configuration["cases"] == ["a", "a"] and "duplicate_switch_case" in codes(by["Sw"])
    assert by["Sw"].configuration["has_default"] is True
    assert by["Un"].expressions[0].role == "loop_condition" and by["Un"].configuration["timeout"] == "0.01:00:00"
    assert by["Fi"].expressions[1].role == "filter_condition" and "item_outside_loop" not in codes(by["Fi"])
    assert "Routes on" in by["Sw"].semantic_summary and "Repeats" in by["Un"].semantic_summary


def test_execute_pipeline_arguments_and_resolution():
    parent = pp.parse_pipeline(pipe(
        act("Run", "ExecutePipeline", {
            "pipeline": ref("Pipeline", "Child"), "waitOnCompletion": True,
            "parameters": {"a": expr("@pipeline().parameters.p"), "ghost": "x"}}), name="Parent",
        parameters=["p"]), "pipeline/Parent.json")
    child = pp.parse_pipeline(pipe(act("W", "Wait", {"waitTimeInSeconds": 1}), name="Child",
                                   parameters=["a", "required_b"]), "pipeline/Child.json")
    run = parent.activities[0]
    assert run.pipeline_refs == ["Child"] and run.configuration["pipeline"] == "Child"
    assert run.parameters == ["p"] and run.operation == AdfOperationKind.orchestrate
    pp.resolve_pipelines([parent, child], [], [])
    assert {"child_parameter_not_supplied", "child_parameter_undeclared"} <= codes(run)
    assert next(r for r in run.references if r.kind == "pipeline").resolved is True
    assert "waits for it" in run.semantic_summary


def test_unresolved_and_cyclic_pipeline_calls():
    a = pp.parse_pipeline(pipe(act("X", "ExecutePipeline", {"pipeline": ref("Pipeline", "B")}), name="A"), "a.json")
    b = pp.parse_pipeline(pipe(act("Y", "ExecutePipeline", {"pipeline": ref("Pipeline", "A")}), name="B"), "b.json")
    c = pp.parse_pipeline(pipe(act("Z", "ExecutePipeline", {"pipeline": ref("Pipeline", "Missing")}), name="C"), "c.json")
    pp.resolve_pipelines([a, b, c], [], [])
    assert "cyclic_pipeline_reference" in {i.code for i in a.issues} | set()
    assert "cyclic_pipeline_reference" in {i.code for i in b.issues}
    assert "cyclic_pipeline_reference" not in {i.code for i in c.issues}
    assert "unresolved_child_pipeline" in codes(c.activities[0])
    assert c.activities[0].confidence == AdfConfidence.medium


def test_web_activities_and_secret_masking():
    web = act("Call", "WebActivity", {
        "url": expr("@concat('https://api.example.com/', pipeline().parameters.p)"), "method": "post",
        "headers": {"Authorization": "Bearer abc123SECRET", "X": "1"},
        "body": {"a": 1},
        "authentication": {"type": "Basic", "username": "u", "password": {"type": "SecureString", "value": "SECRET_PW"}}})
    hook = act("Hook", "WebHook", {"url": "https://x.example.com/h", "method": "POST", "timeout": "00:10:00"})
    fn = act("Fn", "AzureFunctionActivity", {"functionName": "doit", "method": "GET"},
             linkedServiceName=ref("LinkedService", "LS_FN"))
    p = parse(web, hook, fn, parameters=["p"])
    by = {a.name: a for a in p.activities}
    c = by["Call"]
    assert c.configuration["method"] == "POST" and c.configuration["url_is_dynamic"] is True
    assert c.configuration["header_names"] == ["Authorization", "X"] and c.configuration["has_body"] is True
    assert c.configuration["authentication"]["type"] == "Basic"
    assert by["Hook"].configuration["host"] == "x.example.com"
    assert by["Fn"].configuration["function_name"] == "doit"
    dumped = p.model_dump_json()
    for secret in ("abc123SECRET", "SECRET_PW"):
        assert secret not in dumped
    assert "value_redacted" in codes(c)


def test_stored_procedure_and_script():
    sp = act("Sp", "SqlServerStoredProcedure", {
        "storedProcedureName": "[dbo].[usp_x]",
        "storedProcedureParameters": {"p1": {"value": "1", "type": "Int32"}}},
        linkedServiceName=ref("LinkedService", "LS"))
    script = act("Sc", "Script", {"scripts": [
        {"type": "NonQuery", "text": "TRUNCATE TABLE t;\nINSERT INTO t SELECT * FROM s"},
        {"type": "Query", "text": "select count(*) from t"}]}, linkedServiceName=ref("LinkedService", "LS"))
    p = parse(sp, script)
    by = {a.name: a for a in p.activities}
    assert by["Sp"].stored_procedures == ["[dbo].[usp_x]"]
    assert by["Sp"].configuration["parameters"]["p1"] == {"value": "1", "is_expression": False, "type": "Int32"}
    sc = by["Sc"]
    assert {"TRUNCATE", "INSERT", "SELECT"} <= set(sc.configuration["statement_kinds"])
    assert AdfOperationKind.write in sc.operations and AdfOperationKind.read in sc.operations
    assert "TRUNCATE TABLE t" in sc.configuration["scripts"][0]["text"]  # body preserved (SQL ';' is fine)


def test_dataflow_ssis_and_compute_activities():
    df = act("Df", "ExecuteDataFlow", {"dataflow": ref("DataFlow", "DF1", p1=expr("@pipeline().parameters.x")),
                                       "compute": {"coreCount": 8, "computeType": "General"}})
    ssis = act("Ss", "ExecuteSSISPackage", {"packageLocation": {"type": "SSISDB", "packagePath": "f/p/x.dtsx"},
                                            "loggingLevel": "Basic"})
    nb = act("Nb", "DatabricksNotebook", {"notebookPath": "/Shared/etl", "baseParameters": {"d": expr("@utcnow()")}},
             linkedServiceName=ref("LinkedService", "LS_DBX"))
    p = parse(df, ssis, nb, parameters=["x"])
    by = {a.name: a for a in p.activities}
    assert by["Df"].configuration["dataflow"] == "DF1" and by["Df"].configuration["compute"]["core_count"] == 8
    assert any(r.kind == "dataflow" and r.name == "DF1" for r in by["Df"].references)
    assert by["Df"].expressions[0].role == "dataflow_argument"
    assert by["Ss"].semantic_category == AdfSemanticCategory.compute and by["Ss"].configuration["logging_level"] == "Basic"
    assert by["Nb"].configuration["notebook_path"] == "/Shared/etl"
    assert by["Nb"].expressions[0].role == "notebook_argument"


# =============================================================================
# Unknown / unfamiliar constructs
# =============================================================================


def test_unknown_activity_type_is_reported_preserved_and_does_not_stop_the_rest():
    future = act("Fut", "FabricNotebookV9", {"workspace": "w", "inputs": {"a": expr("@pipeline().parameters.p")}},
                 deps=[dep("W")], linkedServiceName=ref("LinkedService", "LS"), newFeature={"x": 1})
    p = parse(act("W", "Wait", {"waitTimeInSeconds": 1}), future, act("After", "Wait", {"waitTimeInSeconds": 2}),
              parameters=["p"])
    f = next(a for a in p.activities if a.name == "Fut")
    assert f.confidence == AdfConfidence.unsupported and f.semantic_category == AdfSemanticCategory.unknown
    assert "unknown_activity_type" in codes(f) and p.unknown_activity_types == ["FabricNotebookV9"]
    assert f.unknown_properties["workspace"] == "w" and f.unknown_properties["activity.newFeature"] == {"x": 1}
    assert f.parameters == ["p"] and f.linked_service_refs == ["LS"] and [d.activity for d in f.depends_on] == ["W"]
    assert "Unrecognised activity type 'FabricNotebookV9'" in f.semantic_summary
    assert p.activity_total == 3 and len(p.activities) == 3  # nothing dropped


def test_unknown_typeproperties_on_known_type_are_preserved():
    a = one(act("W", "Wait", {"waitTimeInSeconds": 1, "brandNew": [1, 2]}))
    assert a.unknown_properties == {"brandNew": [1, 2]} and "unknown_properties" in codes(a)
    assert a.confidence == AdfConfidence.medium


def test_unwrapped_dynamic_looking_string_is_reported_as_ambiguous():
    a = one(act("S", "SetVariable", {"variableName": "v", "value": "@pipeline().parameters.p"}), variables={"v": "String"})
    assert "unwrapped_expression_like_string" in codes(a) and a.expressions == []


def test_inactive_activity_is_flagged():
    a = one(act("W", "Wait", {"waitTimeInSeconds": 1}, state="Inactive", onInactiveMarkAs="Skipped"))
    assert a.state == "Inactive" and a.configuration["on_inactive_mark_as"] == "Skipped"
    assert "deactivated" in a.semantic_summary


# =============================================================================
# Dependencies and control flow
# =============================================================================


def test_dependency_order_stages_and_conditions():
    p = parse(act("C", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("A"), dep("B", "failed", "COMPLETED")]),
              act("A", "Wait", {"waitTimeInSeconds": 1}),
              act("B", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("A")]))
    by = {a.name: a for a in p.activities}
    assert (by["A"].execution_order, by["B"].execution_order, by["C"].execution_order) == (0, 1, 2)
    assert (by["A"].stage, by["B"].stage, by["C"].stage) == (0, 1, 2)
    assert by["A"].dependents == ["C", "B"] or sorted(by["A"].dependents) == ["B", "C"]
    assert by["C"].depends_on[1].conditions == ["Completed", "Failed"]
    assert any(x.name == "failure_handling" for x in p.patterns)


def test_unknown_dependency_condition_and_missing_condition():
    a = one(act("X", "Wait", {"waitTimeInSeconds": 1},
                deps=[{"activity": "Y", "dependencyConditions": ["Weird"]}, {"activity": "Z"}]),
            act("Y", "Wait", {"waitTimeInSeconds": 1}), act("Z", "Wait", {"waitTimeInSeconds": 1}))
    assert a.depends_on[0].unknown_conditions == ["Weird"]
    assert {"unknown_dependency_condition", "dependency_without_condition"} <= codes(a)


def test_cycles_unresolved_self_and_cross_scope_dependencies():
    p = parse(act("A", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("B")]),
              act("B", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("A")]),
              act("S", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("S"), dep("Ghost")]),
              act("Box", "IfCondition", {"expression": expr("@true()"), "ifTrueActivities": [
                  act("In", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("A")])]}))
    by = {a.name: a for a in walk(p.activities)}
    assert "cyclic_dependency" in {i.code for i in p.issues}
    assert by["A"].execution_order is None and "cyclic_dependency" in codes(by["A"])
    assert {"self_dependency", "unresolved_dependency"} <= codes(by["S"])
    assert by["S"].depends_on[1].resolved is False
    assert "dependency_outside_scope" in codes(by["In"])  # 'A' exists, but not in this list


def test_duplicate_activity_names_reported():
    p = parse(act("Same", "Wait", {"waitTimeInSeconds": 1}), act("same", "Wait", {"waitTimeInSeconds": 1}))
    assert "duplicate_activity_name" in {i.code for i in p.issues}
    assert len({a.activity_id for a in p.activities}) == 2


def test_expression_reference_checks():
    p = parse(
        act("A", "SetVariable", {"variableName": "v", "value": expr("@activity('Ghost').output.x")}),
        act("B", "SetVariable", {"variableName": "v", "value": expr("@activity('C').output.x")}),
        act("C", "Wait", {"waitTimeInSeconds": expr("@pipeline().parameters.nope")}),
        act("D", "Wait", {"waitTimeInSeconds": expr("@item().n")}),
        variables={"v": "String"})
    by = {a.name: a for a in p.activities}
    assert "unresolved_activity_reference" in codes(by["A"])
    assert "activity_reference_not_upstream" in codes(by["B"])
    assert "undeclared_parameter" in codes(by["C"]) and "item_outside_loop" in codes(by["D"])
    assert by["C"].output_consumers[0].activity_id == "PL/B"


# =============================================================================
# Malformed input
# =============================================================================


@pytest.mark.parametrize("bad", [None, [], "text", 7])
def test_non_object_pipeline_does_not_raise(bad):
    p = pp.parse_pipeline(bad, "pipeline/Bad.json")
    assert p.name == "Bad" and p.issues[0].code == "invalid_pipeline_json"


def test_missing_properties_name_and_activities():
    p = pp.parse_pipeline({}, "pipeline/Anon.json")
    assert p.name == "Anon"
    assert {"missing_pipeline_name", "missing_properties", "activities_missing"} <= {i.code for i in p.issues}
    assert "activities_empty" in {i.code for i in pp.parse_pipeline(pipe(), "pipeline/PL.json").issues}


def test_missing_versus_explicitly_empty():
    missing = one(act("F", "ForEach", {"activities": []}))
    empty = one(act("F", "ForEach", {"items": "", "activities": []}))
    assert missing.missing_properties == ["typeProperties.items"] and missing.empty_properties == [
        "typeProperties.activities"]
    assert empty.missing_properties == [] and set(empty.empty_properties) == {
        "typeProperties.items", "typeProperties.activities"}


def test_malformed_activity_nodes_are_reported_and_siblings_survive():
    p = pp.parse_pipeline({"name": "PL", "properties": {"activities": [
        "not an object", 5, {"typeProperties": []}, {"name": "n"},
        act("Good", "Wait", {"waitTimeInSeconds": 1}, deps="oops"),
        act("Bad2", "Wait", {"waitTimeInSeconds": 1}, deps=[{"dependencyConditions": []}])]}}, "pipeline/PL.json")
    assert [i.code for i in p.issues].count("invalid_activity_node") == 2
    names = [a.name for a in p.activities]
    assert names == ["", "n", "Good", "Bad2"]
    by = {a.name: a for a in p.activities}
    assert "invalid_type_properties" in codes(p.activities[0]) and "missing_activity_name" in codes(p.activities[0])
    assert "missing_activity_type" in codes(by["n"])
    assert "invalid_depends_on" in codes(by["Good"]) and "invalid_dependency" in codes(by["Bad2"])


def test_activities_not_a_list_and_bad_nested_list():
    p = pp.parse_pipeline({"name": "PL", "properties": {"activities": {"a": 1}}}, "pipeline/PL.json")
    assert "invalid_activities" in {i.code for i in p.issues} and p.activities == []
    a = one(act("F", "ForEach", {"items": "x", "activities": "nope"}))
    assert "invalid_activities" in codes(a)


def test_invalid_json_file(tmp_path):
    (tmp_path / "pipeline").mkdir()
    (tmp_path / "pipeline" / "Broken.json").write_text("{ nope", encoding="utf-8")
    p = pp.parse_pipeline_file(tmp_path, "pipeline/Broken.json")
    assert p.issues[0].code == "invalid_json"


def test_policy_parsing():
    a = one(act("W", "Wait", {"waitTimeInSeconds": 1}, policy={
        "timeout": "0.12:00:00", "retry": 3, "retryIntervalInSeconds": 30, "secureInput": True,
        "secureOutput": False}))
    assert (a.policy.timeout, a.policy.retry, a.policy.retry_interval_in_seconds) == ("0.12:00:00", 3, 30)
    assert (a.policy.secure_input, a.policy.secure_output, a.policy.is_dynamic) == (True, False, False)
    dyn = one(act("W", "Wait", {"waitTimeInSeconds": 1}, policy={"timeout": expr("@pipeline().parameters.t")}),
              parameters=["t"])
    assert dyn.policy.is_dynamic and dyn.policy.timeout is None
    assert dyn.expressions[0].role == "execution_policy"


# =============================================================================
# Cross-artifact resolution
# =============================================================================


def _datasets():
    ds_sql = parser.parse_dataset({"name": "DS_SQL", "properties": {
        "type": "AzureSqlTable", "linkedServiceName": ref("LinkedService", "LS_SQL"),
        "schema": [{"name": "Id", "type": "int"}],
        "typeProperties": {"schema": "dbo", "table": "Customer"}}}, "dataset/DS_SQL.json")
    ds_lake = parser.parse_dataset({"name": "DS_LAKE", "properties": {
        "type": "Parquet", "linkedServiceName": ref("LinkedService", "LS_LAKE"),
        "parameters": {"folder": {"type": "String"}},
        "typeProperties": {"location": {"type": "AzureBlobFSLocation", "fileSystem": "raw",
                                        "folderPath": "customer"}}}}, "dataset/DS_LAKE.json")
    lss = [parser.parse_linked_service({"name": "LS_SQL", "properties": {"type": "AzureSqlDatabase", "typeProperties": {}}}, "ls1.json"),
           parser.parse_linked_service({"name": "LS_LAKE", "properties": {"type": "AzureBlobFS", "typeProperties": {"url": "https://x.dfs.core.windows.net"}}}, "ls2.json")]
    return [ds_sql, ds_lake], lss


def test_resolution_enriches_endpoints_and_summary():
    p = parse(sql_copy("Copy_Customer_From_SQL_To_Lake"))
    before = p.activities[0].semantic_summary
    datasets, lss = _datasets()
    pp.resolve_pipelines([p], datasets, lss)
    a = p.activities[0]
    src, dst = a.inputs[0], a.outputs[0]
    assert src.resolved and src.dataset_type == "AzureSqlTable" and src.connection_type == "AzureSqlDatabase"
    assert src.location["table"] == "dbo.Customer" and src.declared_columns == 1
    assert dst.format == "Parquet" and dst.linked_service == "LS_LAKE" and dst.location["path"] == "raw/customer"
    assert "dataset_parameter_not_supplied" in codes(a)  # DS_LAKE declares 'folder' without a default
    assert a.semantic_summary != before and "AzureSqlDatabase" in a.semantic_summary
    assert a.configuration["direction"] == "sql -> file" and "name_contradicts_properties" not in codes(a)


def test_unresolved_dataset_reported_and_lowers_confidence():
    p = parse(sql_copy())
    assert p.activities[0].confidence == AdfConfidence.high
    pp.resolve_pipelines([p], [], [])
    a = p.activities[0]
    assert "unresolved_dataset" in codes(a) and a.confidence == AdfConfidence.medium
    assert a.inputs[0].resolved is False


def test_resolution_is_idempotent():
    p = parse(sql_copy())
    datasets, lss = _datasets()
    pp.resolve_pipelines([p], datasets, lss)
    first = p.model_dump_json()
    pp.resolve_pipelines([p], datasets, lss)
    assert p.model_dump_json() == first


def test_recurring_pipeline_shape_detected_across_pipelines():
    mk = lambda n: pp.parse_pipeline(pipe(  # noqa: E731
        act("L", "Lookup", {"source": {"type": "S"}, "dataset": ref("Dataset", "D")}),
        act("C", "Copy", {"source": {"type": "S"}, "sink": {"type": "K"}}, deps=[dep("L")]), name=n),
        f"pipeline/{n}.json")
    a, b, c = mk("P1"), mk("P2"), pp.parse_pipeline(pipe(act("W", "Wait", {"waitTimeInSeconds": 1}), name="P3"), "p3.json")
    pp.resolve_pipelines([a, b, c], [], [])
    assert [x.name for x in a.patterns if x.name == "recurring_pipeline_shape"] == ["recurring_pipeline_shape"]
    assert "P1, P2" in a.patterns[-1].evidence[1] and not [x for x in c.patterns if x.name == "recurring_pipeline_shape"]


# =============================================================================
# Determinism, safety, integration
# =============================================================================


def test_parsing_is_deterministic_and_serializable():
    data = pipe(sql_copy(), act("W", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("Copy")]))
    one_, two = pp.parse_pipeline(data, "pipeline/PL.json"), pp.parse_pipeline(json.loads(json.dumps(data)), "pipeline/PL.json")
    assert one_.model_dump_json() == two.model_dump_json()
    json.loads(one_.model_dump_json())


def test_secrets_in_any_property_never_reach_the_record():
    node = act("X", "WebActivity", {
        "url": "https://h/?sig=SIGSECRET", "method": "GET",
        "body": {"connectionString": "Server=x;Password=PWSECRET", "accountKey": "KEYSECRET"},
        "authentication": {"type": "Basic", "password": {"type": "SecureString", "value": "SSSECRET"}}})
    s = act("S", "Script", {"scripts": [{"type": "NonQuery", "text": "EXEC x @pwd='PWDSECRET'; -- pwd=PWDSECRET2"}]},
            linkedServiceName=ref("LinkedService", "LS"))
    dumped = parse(node, s).model_dump_json()
    for secret in ("SIGSECRET", "PWSECRET", "KEYSECRET", "SSSECRET", "PWDSECRET"):
        assert secret not in dumped, secret


def test_original_expression_text_is_preserved_verbatim():
    raw = "@concat('a',   pipeline().parameters.p )"
    a = one(act("S", "SetVariable", {"variableName": "v", "value": expr(raw)}), parameters=["p"], variables={"v": "String"})
    assert a.expressions[0].raw == raw and a.raw_properties["value"] == expr(raw)


def test_legacy_structural_fields_unchanged():
    p = parse(act("F", "ForEach", {"items": "x", "activities": [
        act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "Child")})]}),
        sql_copy("C"), act("Sp", "SqlServerStoredProcedure", {"storedProcedureName": "[dbo].[p]"},
                           linkedServiceName=ref("LinkedService", "LS")))
    assert p.pipeline_dependencies == ["Child"] and p.activity_count == 3
    assert p.activities[0].activities[0].pipeline_refs == ["Child"]
    by = {a.name: a for a in p.activities}
    assert by["C"].source_type == "AzureSqlSource" and by["C"].sink_type == "ParquetSink"
    assert by["Sp"].stored_procedures == ["[dbo].[p]"] and by["Sp"].linked_service_refs == ["LS"]


def test_old_parser_entry_point_delegates():
    data = pipe(act("W", "Wait", {"waitTimeInSeconds": 1}))
    assert parser.parse_pipeline(data, "pipeline/PL.json") == pp.parse_pipeline(data, "pipeline/PL.json")


def test_discovery_populates_activity_level_records(tmp_path):
    files = {
        "pipeline/PL_Main.json": pipe(sql_copy("Copy_A"), name="PL_Main"),
        "dataset/DS_SQL.json": {"name": "DS_SQL", "properties": {"type": "AzureSqlTable",
            "linkedServiceName": ref("LinkedService", "LS_SQL"), "typeProperties": {"schema": "dbo", "table": "T"}}},
        "linkedService/LS_SQL.json": {"name": "LS_SQL", "properties": {"type": "AzureSqlDatabase", "typeProperties": {}}},
    }
    store, _pid = _make_project(tmp_path, files)
    result = _run(store)
    d = result.discovery
    assert result.success and d.activity_count == 1 and d.activity_type_counts == {"Copy": 1}
    a = d.pipelines[0].activities[0]
    assert a.inputs[0].resolved is True and a.inputs[0].connection_type == "AzureSqlDatabase"
    assert a.outputs[0].resolved is False and "unresolved_dataset" in codes(a)  # DS_LAKE absent
    assert d.parse_issue_count >= 1
    # persisted record round-trips with the new fields
    again = store.load_adf_semantic_discovery("test-proj")
    assert again.pipelines[0].activities[0].semantic_summary == a.semantic_summary
    assert again.pipelines[0].activities[0].confidence == AdfConfidence.medium


def test_real_style_pipeline_has_no_unknowns_or_dropped_activities():
    """A pipeline shaped like a real ADF export: nothing unrecognised, nothing lost."""
    p = parse(
        act("Lookup Cfg", "Lookup", {"source": {"type": "AzureSqlSource", "sqlReaderStoredProcedureName": "[cfg].[usp_get]"},
                                      "dataset": ref("Dataset", "DS_CFG"), "firstRowOnly": False}),
        act("For Each", "ForEach", {"items": expr("@activity('Lookup Cfg').output.value"), "isSequential": True,
                                    "activities": [act("Run Child", "ExecutePipeline", {
                                        "pipeline": ref("Pipeline", "PL_Child"), "waitOnCompletion": True,
                                        "parameters": {"code": expr("@item().Code")}})]},
            deps=[dep("Lookup Cfg")]))
    assert p.unknown_activity_types == [] and p.activity_total == 3 and p.issues == []
    assert all(a.confidence == AdfConfidence.high for a in walk(p.activities))


def test_wait_duration_reaches_configuration_and_summary():
    # Regression: Wait had no handler, so the duration never reached the summary ("(not set)").
    p = parse(act("W1", "Wait", {"waitTimeInSeconds": 7}),
              act("W2", "Wait", {"waitTimeInSeconds": expr("@pipeline().parameters.secs")}),
              parameters=["secs"])
    by = {a.name: a for a in walk(p.activities)}
    assert by["W1"].configuration["wait_seconds"] == 7
    assert "for 7 second(s)" in by["W1"].semantic_summary
    assert "pipeline().parameters.secs" in by["W2"].semantic_summary
