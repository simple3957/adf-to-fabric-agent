"""Activity-level conversion behavior, one family at a time (offline, deterministic)."""

from __future__ import annotations

import json

import pytest

from adf2fabric.project.conversion import ConversionStatus as S
from adf2fabric.project.conversion import ConversionStrategy as St
from adf2fabric.project.conversion import Impact
from tests.conversion_support import (
    DS_LAKE,
    DS_SQL,
    LAKE_LS,
    SQL_LS,
    act,
    activities,
    by_name,
    codes,
    convert,
    convert_one,
    copy_activity,
    dataset,
    dep,
    expr,
    find,
    linked_service,
    pipe,
    ref,
    trigger,
)

ESTATE = dict(datasets=[DS_SQL, DS_LAKE], linked_services=[SQL_LS, LAKE_LS])


def one(activity, *more, **kw):
    """Convert a pipeline of the given activities; return (record, content, the first activity's record)."""
    rec, content = convert_one(pipe(activity, *more, **{k: v for k, v in kw.items() if k in ("parameters", "variables")}),
                               **{k: v for k, v in kw.items() if k not in ("parameters", "variables")})
    return rec, content, by_name(rec)[activity["name"]]


# =============================================================================
# Copy
# =============================================================================


def test_copy_inlines_both_datasets_and_binds_connections():
    rec, content, a = one(copy_activity(), **ESTATE)
    c = find(content, "Copy")
    assert c["type"] == "Copy"
    src, snk = c["typeProperties"]["source"], c["typeProperties"]["sink"]
    assert src["type"] == "AzureSqlSource" and src["sqlReaderQuery"] == "SELECT * FROM dbo.Customer"
    assert src["datasetSettings"] == {
        "annotations": [], "type": "AzureSqlTable", "schema": [{"name": "Id", "type": "int"}],
        "typeProperties": {"schema": "dbo", "table": "Customer"},
        "externalReferences": {"connection": "<connection:LS_SQL>"}}
    assert snk["type"] == "ParquetSink" and snk["storeSettings"] == {"type": "AzureBlobFSWriteSettings"}
    assert snk["datasetSettings"]["externalReferences"]["connection"] == "<connection:LS_LAKE>"
    assert a.strategy is St.transformed and a.status is S.converted
    assert {c.operation for c in a.operation_coverage if c.covered_by} == {"read", "write"}
    assert {(p.kind, p.name) for p in a.prerequisites} == {("connection", "LS_SQL"), ("connection", "LS_LAKE")}


def test_copy_substitutes_dataset_parameters_with_the_activity_arguments():
    _, content, a = one(copy_activity(), **ESTATE)
    loc = find(content, "Copy")["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["location"]
    assert loc["folderPath"] == "customer"  # @dataset().folder -> the literal argument, no dataset() left
    assert "dataset()" not in json.dumps(content)
    assert any(m.rule == "dataset_inlined" for m in a.property_mappings)
    assert any("dataset:DS_LAKE" in e.source_path and e.status == "rewritten" for e in a.expression_conversions)


def test_copy_with_dynamic_arguments_keeps_them_dynamic():
    c = copy_activity()
    c["outputs"] = [ref("Dataset", "DS_LAKE", folder=expr("@concat('c/', pipeline().parameters.day)"))]
    _, content, a = one(c, parameters=["day"], **ESTATE)
    loc = find(content, "Copy")["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["location"]
    assert loc["folderPath"] == {"value": "@concat('c/', pipeline().parameters.day)", "type": "Expression"}
    assert a.status is S.converted


def test_copy_carries_documented_settings_and_translator():
    c = copy_activity(translator={"type": "TabularTranslator", "mappings": [
        {"source": {"name": "Id"}, "sink": {"name": "CustomerId"}}]},
        parallelCopies=4, dataIntegrationUnits=8, enableSkipIncompatibleRow=True, validateDataConsistency=True)
    _, content, a = one(c, **ESTATE)
    tp = find(content, "Copy")["typeProperties"]
    assert tp["parallelCopies"] == 4 and tp["dataIntegrationUnits"] == 8 and tp["validateDataConsistency"] is True
    assert tp["translator"]["mappings"][0]["sink"]["name"] == "CustomerId" and a.status is S.converted


def test_copy_staging_linked_service_becomes_an_external_reference():
    c = copy_activity(enableStaging=True, stagingSettings={
        "linkedServiceName": ref("LinkedService", "LS_LAKE"), "path": "stage", "enableCompression": True})
    _, content, a = one(c, **ESTATE)
    st = find(content, "Copy")["typeProperties"]["stagingSettings"]
    assert st == {"externalReferences": {"connection": "<connection:LS_LAKE>"}, "path": "stage",
                  "enableCompression": True}


def test_copy_logging_is_reported_not_silently_dropped():
    c = copy_activity(logSettings={"enableCopyActivityLog": True,
                                   "logLocationSettings": {"linkedServiceName": ref("LinkedService", "LS_LAKE")}})
    _, content, a = one(c, **ESTATE)
    assert "logSettings" not in find(content, "Copy")["typeProperties"]
    assert any(m.source == "typeProperties.logSettings" and not m.preserved for m in a.property_mappings)
    assert a.status is S.manual_required and "copy_logging_reconfigure" in codes(a)
    assert any("logSettings" in s for s in a.manual_steps)


def test_copy_diu_below_the_documented_minimum_is_flagged():
    _, _, a = one(copy_activity(dataIntegrationUnits=2), **ESTATE)
    assert "diu_below_fabric_minimum" in codes(a) and a.status is S.converted_with_differences


def test_copy_incremental_pattern_is_preserved_and_noted():
    lookup = act("Lookup_Max_Watermark", "Lookup", {
        "source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT MAX(ModifiedDate) AS wm FROM dbo.Customer"},
        "dataset": ref("Dataset", "DS_SQL")})
    c = copy_activity("Copy_Delta")
    c["typeProperties"]["source"]["sqlReaderQuery"] = expr(
        "SELECT * FROM dbo.Customer WHERE ModifiedDate > '@{activity('Lookup_Max_Watermark').output.firstRow.wm}'")
    c["dependsOn"] = [dep("Lookup_Max_Watermark")]
    rec, content = convert_one(pipe(lookup, c), **ESTATE)
    q = find(content, "Copy_Delta")["typeProperties"]["source"]["sqlReaderQuery"]
    assert q["value"].endswith("'@{activity('Lookup_Max_Watermark').output.firstRow.wm}'")  # unchanged
    r = by_name(rec)
    assert "incremental_pattern" in codes(r["Copy_Delta"])
    assert find(content, "Copy_Delta")["dependsOn"] == [
        {"activity": "Lookup_Max_Watermark", "dependencyConditions": ["Succeeded"]}]


def test_copy_output_consumers_get_a_verification_note():
    log = act("Log", "SqlServerStoredProcedure", {
        "storedProcedureName": "dbo.log",
        "storedProcedureParameters": {"rows": {"value": expr("@{activity('Copy').output.rowsCopied}"), "type": "Int32"}}},
        deps=[dep("Copy")], linkedServiceName=ref("LinkedService", "LS_SQL"))
    rec, _ = convert_one(pipe(copy_activity(), log), **ESTATE)
    assert "output_shape_unverified" in codes(by_name(rec)["Copy"])


def test_copy_with_an_unresolvable_dataset_is_blocked_with_a_failfast_placeholder():
    rec, content, a = one(copy_activity(), datasets=[DS_LAKE], linked_services=[SQL_LS, LAKE_LS])  # DS_SQL missing
    assert a.status is S.blocked and a.strategy is St.manual
    assert [c.fabric_type for c in a.components] == ["Fail"] and a.components[0].role == "placeholder"
    placeholder = find(content, "Copy")
    assert placeholder["type"] == "Fail" and "was not converted" in placeholder["typeProperties"]["message"]
    assert "dataset_not_found" in codes(a) and a.alternatives
    assert {c.operation: c.covered_by for c in a.operation_coverage} == {"read": [], "write": []}
    assert rec.coverage.blocked == ["PL/Copy"] and rec.status is S.blocked


def test_copy_with_a_retired_connector_is_blocked():
    gen1 = linked_service("LS_GEN1", "AzureDataLakeStore")
    ds = dataset("DS_GEN1", "AzureDataLakeStoreFile", "LS_GEN1", {"folderPath": "x"})
    c = copy_activity()
    c["inputs"] = [ref("Dataset", "DS_GEN1")]
    _, _, a = one(c, datasets=[ds, DS_LAKE], linked_services=[gen1, LAKE_LS])
    assert a.status is S.blocked and "connector_unsupported" in codes(a)


def test_copy_with_an_unlisted_connector_is_flagged_for_verification():
    odd = linked_service("LS_ODD", "SomeNewStore")
    ds = dataset("DS_ODD", "SomeNewTable", "LS_ODD", {})
    c = copy_activity()
    c["inputs"] = [ref("Dataset", "DS_ODD")]
    _, _, a = one(c, datasets=[ds, DS_LAKE], linked_services=[odd, LAKE_LS])
    assert "connector_unverified" in codes(a) and a.status is S.converted_with_differences


def test_self_hosted_runtime_becomes_a_gateway_prerequisite():
    sh = linked_service("LS_ONPREM", "SqlServer", connect_via="MyShir")
    ds = dataset("DS_ONPREM", "SqlServerTable", "LS_ONPREM", {"table": "t"})
    c = copy_activity()
    c["inputs"] = [ref("Dataset", "DS_ONPREM")]
    rec, _, a = one(c, datasets=[ds, DS_LAKE], linked_services=[sh, LAKE_LS])
    assert ("gateway", "MyShir") in {(p.kind, p.name) for p in rec.prerequisites}


# =============================================================================
# Lookup / GetMetadata / Delete / Validation
# =============================================================================


def test_lookup_keeps_source_and_inlines_the_dataset_at_type_properties_level():
    lk = act("Lookup1", "Lookup", {"source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT TOP 1 * FROM t"},
                                   "dataset": ref("Dataset", "DS_SQL"), "firstRowOnly": False})
    _, content, a = one(lk, **ESTATE)
    tp = find(content, "Lookup1")["typeProperties"]
    assert tp["source"] == {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT TOP 1 * FROM t"}
    assert tp["datasetSettings"]["type"] == "AzureSqlTable" and tp["firstRowOnly"] is False
    assert a.status is S.converted and {c.operation for c in a.operation_coverage if c.covered_by} == {"read"}


def test_lookup_default_first_row_is_not_invented():
    lk = act("L", "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS_SQL")})
    _, content, a = one(lk, **ESTATE)
    assert "firstRowOnly" not in find(content, "L")["typeProperties"]
    assert any(m.rule == "default_preserved" for m in a.property_mappings)


def test_downstream_reads_of_lookup_output_stay_valid():
    lk = act("L", "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS_SQL"),
                             "firstRowOnly": False})
    fe = act("Each", "ForEach", {"items": expr("@activity('L').output.value"), "activities": [
        act("W", "Wait", {"waitTimeInSeconds": expr("@item().secs")})]}, deps=[dep("L")])
    rec, content = convert_one(pipe(lk, fe), **ESTATE)
    assert find(content, "Each")["typeProperties"]["items"]["value"] == "@activity('L').output.value"
    assert rec.structural_valid and not [i for i in rec.validation.issues if i.severity.value == "error"]


def test_get_metadata_and_delete():
    gm = act("GM", "GetMetadata", {"dataset": ref("Dataset", "DS_SQL"), "fieldList": ["exists", "columnCount"]})
    dl = act("Del", "Delete", {"dataset": ref("Dataset", "DS_LAKE", folder="old"), "recursive": True,
                               "enableLogging": False}, deps=[dep("GM")])
    rec, content = convert_one(pipe(gm, dl), **ESTATE)
    assert find(content, "GM")["typeProperties"]["fieldList"] == ["exists", "columnCount"]
    d = find(content, "Del")["typeProperties"]
    assert d["recursive"] is True and d["datasetSettings"]["typeProperties"]["location"]["folderPath"] == "old"
    assert by_name(rec)["Del"].operation_coverage[0].operation == "delete"


def test_validation_becomes_an_until_loop_over_get_metadata_and_a_gated_wait():
    v = act("Wait_For_File", "Validation", {"dataset": ref("Dataset", "DS_LAKE", folder="in"), "timeout": "0.01:00:00",
                                            "sleep": 30})
    after = act("After", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("Wait_For_File")])
    rec, content = convert_one(pipe(v, after), **ESTATE)
    a = by_name(rec)["Wait_For_File"]
    assert a.strategy is St.composite and len(a.components) == 4
    assert {c.fabric_type for c in a.components} == {"Until", "GetMetadata", "IfCondition", "Wait"}
    until = find(content, "Wait_For_File")["typeProperties"]
    assert until["timeout"] == "0.01:00:00"
    assert until["expression"]["value"] == "@equals(activity('Wait_For_File_check').output.exists, true)"
    kids = {k["name"]: k for k in until["activities"]}
    assert kids["Wait_For_File_check"]["typeProperties"]["fieldList"] == ["exists"]
    gate = kids["Wait_For_File_wait_if_not_ready"]
    assert gate["typeProperties"]["ifTrueActivities"][0]["typeProperties"]["waitTimeInSeconds"] == 30
    assert gate["dependsOn"][0]["activity"] == "Wait_For_File_check"
    # dependents attach to the loop, which carries the original name
    assert find(content, "After")["dependsOn"] == [{"activity": "Wait_For_File", "dependencyConditions": ["Succeeded"]}]
    assert [o.operation for o in a.operation_coverage] == ["validate"] and a.alternatives
    assert rec.structural_valid


def test_validation_makes_adf_defaults_explicit():
    v = act("V", "Validation", {"dataset": ref("Dataset", "DS_SQL")})
    _, content, a = one(v, **ESTATE)
    until = find(content, "V")["typeProperties"]
    assert until["timeout"] == "0.12:00:00"  # ADF's 12h, not Fabric's 7-day Until default
    wait = until["activities"][1]["typeProperties"]["ifTrueActivities"][0]
    assert wait["typeProperties"]["waitTimeInSeconds"] == 10
    assert {m.rule for m in a.property_mappings} >= {"default_made_explicit"}


def test_validation_minimum_size_and_child_items_carry_documented_differences():
    v = act("V", "Validation", {"dataset": ref("Dataset", "DS_SQL"), "minimumSize": 1024, "childItems": True})
    _, content, a = one(v, **ESTATE)
    expression = find(content, "V")["typeProperties"]["expression"]["value"]
    assert "greaterOrEquals(activity('V_check').output.size, 1024)" in expression
    assert "greater(length(activity('V_check').output.childItems), 0)" in expression
    assert a.status is S.converted_with_differences and a.limitations


# =============================================================================
# Control flow
# =============================================================================


def test_foreach_preserves_items_mode_batch_and_nested_activities():
    inner = copy_activity("CopyOne")
    inner["outputs"] = [ref("Dataset", "DS_LAKE", folder=expr("@item().name"))]
    fe = act("Each", "ForEach", {"items": expr("@activity('L').output.value"), "isSequential": False, "batchCount": 5,
                                 "activities": [inner]})
    lk = act("L", "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS_SQL"),
                             "firstRowOnly": False})
    fe["dependsOn"] = [dep("L")]
    rec, content = convert_one(pipe(lk, fe), **ESTATE)
    tp = find(content, "Each")["typeProperties"]
    assert tp["isSequential"] is False and tp["batchCount"] == 5 and tp["items"]["value"] == "@activity('L').output.value"
    nested = tp["activities"][0]
    assert nested["name"] == "CopyOne"
    folder = nested["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["location"]["folderPath"]
    assert folder == {"value": "@item().name", "type": "Expression"}  # iteration-scoped reference preserved
    r = by_name(rec)
    assert r["Each"].children == ["PL/Each/CopyOne"] and r["CopyOne"].parent_activity_id == "PL/Each"
    assert r["Each"].components[0].children == ["CopyOne"] and rec.coverage.source_activities == 3
    assert rec.coverage.complete and rec.structural_valid


def test_if_condition_keeps_both_branches_and_the_expression():
    ifc = act("If1", "IfCondition", {
        "expression": expr("@greater(activity('L').output.firstRow.n, 0)"),
        "ifTrueActivities": [act("T", "Wait", {"waitTimeInSeconds": 1})],
        "ifFalseActivities": [act("F", "Fail", {"message": "none", "errorCode": "E1"})]})
    lk = act("L", "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS_SQL")})
    ifc["dependsOn"] = [dep("L")]
    rec, content = convert_one(pipe(lk, ifc), **ESTATE)
    tp = find(content, "If1")["typeProperties"]
    assert tp["expression"]["value"] == "@greater(activity('L').output.firstRow.n, 0)"
    assert [a["name"] for a in tp["ifTrueActivities"]] == ["T"] and [a["name"] for a in tp["ifFalseActivities"]] == ["F"]
    assert by_name(rec)["If1"].status is S.converted and by_name(rec)["If1"].strategy is St.direct


def test_switch_preserves_cases_order_and_default():
    sw = act("Sw", "Switch", {"on": expr("@pipeline().parameters.mode"), "cases": [
        {"value": "a", "activities": [act("CA", "Wait", {"waitTimeInSeconds": 1})]},
        {"value": "b", "activities": [act("CB", "Wait", {"waitTimeInSeconds": 2})]}],
        "defaultActivities": [act("D", "Wait", {"waitTimeInSeconds": 3})]})
    _, content, a = one(sw, parameters=["mode"])
    tp = find(content, "Sw")["typeProperties"]
    assert [c["value"] for c in tp["cases"]] == ["a", "b"] and tp["cases"][1]["activities"][0]["name"] == "CB"
    assert tp["defaultActivities"][0]["name"] == "D" and a.status is S.converted


def test_switch_without_default_generates_none():
    sw = act("Sw", "Switch", {"on": "x", "cases": [{"value": "a", "activities": [act("CA", "Wait", {"waitTimeInSeconds": 1})]}]})
    _, content, a = one(sw)
    assert "defaultActivities" not in find(content, "Sw")["typeProperties"]
    assert any(m.rule == "switch_no_default" for m in a.property_mappings)


def test_until_filter_wait_fail():
    un = act("Un", "Until", {"expression": expr("@equals(variables('done'), true)"), "timeout": "0.00:30:00",
                             "activities": [act("W", "Wait", {"waitTimeInSeconds": 5})]})
    fl = act("Fi", "Filter", {"items": expr("@createArray(1,2,3)"), "condition": expr("@greater(item(), 1)")})
    fa = act("Fa", "Fail", {"message": expr("@concat('bad ', pipeline().RunId)"), "errorCode": "42"}, deps=[dep("Un", "Failed")])
    rec, content = convert_one(pipe(un, fl, fa, variables={"done": "Boolean"}))
    assert find(content, "Un")["typeProperties"]["timeout"] == "0.00:30:00"
    assert find(content, "Fi")["typeProperties"]["condition"]["value"] == "@greater(item(), 1)"
    assert find(content, "Fa")["typeProperties"]["errorCode"] == "42"
    assert find(content, "Fa")["dependsOn"] == [{"activity": "Un", "dependencyConditions": ["Failed"]}]
    assert rec.structural_valid and rec.status is S.converted


def test_until_without_timeout_notes_the_differing_defaults():
    un = act("Un", "Until", {"expression": expr("@equals(1, 1)"), "activities": [act("W", "Wait", {"waitTimeInSeconds": 1})]})
    _, _, a = one(un)
    assert "until_default_timeout" in codes(a) and a.status is S.converted_with_differences


# =============================================================================
# Variables, parameters, execute pipeline
# =============================================================================


def test_variables_are_declared_and_keep_their_defaults():
    sv = act("Set", "SetVariable", {"variableName": "ts", "value": expr("@utcnow()")})
    ap = act("App", "AppendVariable", {"variableName": "log", "value": "x"}, deps=[dep("Set")])
    p = pipe(sv, ap, variables={"ts": "String", "log": "Array"})
    p["properties"]["variables"]["log"]["defaultValue"] = ["seed"]
    rec, content = convert_one(p)
    assert content["properties"]["variables"] == {"ts": {"type": "String"}, "log": {"type": "Array", "defaultValue": ["seed"]}}
    assert find(content, "Set")["typeProperties"] == {"variableName": "ts", "value": {"value": "@utcnow()", "type": "Expression"}}
    assert rec.structural_valid and rec.status is S.converted


def test_set_variable_policy_keeps_only_secure_flags():
    sv = act("Set", "SetVariable", {"variableName": "v", "value": "x"},
             policy={"secureOutput": True, "timeout": "0.01:00:00", "retry": 2})
    _, content, a = one(sv, variables={"v": "String"})
    assert find(content, "Set")["policy"] == {"secureOutput": True}
    assert sum(1 for m in a.property_mappings if not m.preserved) == 2 and a.status is S.converted_with_differences


def test_pipeline_parameters_and_defaults_are_carried():
    p = pipe(act("W", "Wait", {"waitTimeInSeconds": expr("@pipeline().parameters.n")}), parameters=["n"])
    p["properties"]["parameters"]["n"] = {"type": "Int", "defaultValue": 5}
    _, content = convert_one(p)
    assert content["properties"]["parameters"] == {"n": {"type": "Int", "defaultValue": 5}}


def test_execute_pipeline_becomes_invoke_pipeline_with_bound_references():
    child = pipe(act("W", "Wait", {"waitTimeInSeconds": 1}), name="Child", parameters=["p"])
    parent = pipe(act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "Child"),
                                                  "parameters": {"p": expr("@pipeline().parameters.src")},
                                                  "waitOnCompletion": True}), name="Parent", parameters=["src"])
    out = convert([parent, child])
    rec, content = out["Parent"]
    inv = find(content, "Run")
    assert inv["type"] == "InvokePipeline"
    assert inv["typeProperties"] == {
        "pipelineId": "<item:pipeline:Child>", "workspaceId": "<workspace-id>", "waitOnCompletion": True,
        "parameters": {"p": {"value": "@pipeline().parameters.src", "type": "Expression"}}}
    assert inv["externalReferences"] == {"connection": "<connection:invoke-pipeline>"}
    a = by_name(rec)["Run"]
    assert a.strategy is St.transformed and a.status is S.converted
    assert {(p.kind, p.name) for p in a.prerequisites} >= {("workspace_item", "pipeline:Child"),
                                                             ("workspace_item", "workspace")}


def test_execute_pipeline_wait_default_is_written_explicitly_and_unknown_child_needs_attention():
    run = act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "Ghost")})
    _, content, a = one(run)
    assert find(content, "Run")["typeProperties"]["waitOnCompletion"] is False
    assert a.status is S.manual_required and "child_pipeline_unknown" in codes(a)


def test_global_parameters_become_parameters_and_flow_down_to_children():
    child = pipe(act("W", "Wait", {"waitTimeInSeconds": expr("@int(pipeline().globalParameters.delay)")}), name="Child")
    parent = pipe(act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "Child")}), name="Parent")
    out = convert([parent, child], global_params={"delay": {"type": "Int", "defaultValue": 7}})
    c_rec, c_content = out["Child"]
    p_rec, p_content = out["Parent"]
    assert c_content["properties"]["parameters"] == {"gp_delay": {"type": "Int", "defaultValue": 7}}
    assert find(c_content, "W")["typeProperties"]["waitTimeInSeconds"]["value"] == "@int(pipeline().parameters.gp_delay)"
    assert p_content["properties"]["parameters"] == {"gp_delay": {"type": "Int", "defaultValue": 7}}
    assert find(p_content, "Run")["typeProperties"]["parameters"]["gp_delay"] == {
        "value": "@pipeline().parameters.gp_delay", "type": "Expression"}
    assert by_name(c_rec)["W"].status is S.converted_with_differences and "gp_delay" in c_rec.parameters_added
    assert c_rec.structural_valid and p_rec.structural_valid


# =============================================================================
# SQL / Web / Function
# =============================================================================


def test_stored_procedure_keeps_name_and_typed_parameters():
    sp = act("Sp", "SqlServerStoredProcedure", {
        "storedProcedureName": "[dbo].[usp_log]",
        "storedProcedureParameters": {"RunId": {"value": expr("@pipeline().RunId"), "type": "String"}}},
        linkedServiceName=ref("LinkedService", "LS_SQL"))
    _, content, a = one(sp, **ESTATE)
    c = find(content, "Sp")
    assert c["type"] == "SqlServerStoredProcedure" and c["externalReferences"] == {"connection": "<connection:LS_SQL>"}
    assert c["typeProperties"]["storedProcedureParameters"]["RunId"] == {
        "value": {"value": "@pipeline().RunId", "type": "Expression"}, "type": "String"}
    assert a.status is S.converted and a.strategy is St.transformed


def test_stored_procedure_on_an_unsupported_store_is_blocked_with_alternatives():
    sp = act("Sp", "SqlServerStoredProcedure", {"storedProcedureName": "p"}, linkedServiceName=ref("LinkedService", "LS_PG"))
    _, _, a = one(sp, linked_services=[linked_service("LS_PG", "PostgreSql")])
    assert a.status is S.blocked and "stored_procedure_store_unsupported" in codes(a)
    assert any("notebook" in x.lower() for x in a.alternatives)


def test_script_carries_blocks_and_requires_the_database_to_be_set():
    sc = act("Sc", "Script", {"scripts": [{"type": "NonQuery", "text": "TRUNCATE TABLE t; INSERT INTO t SELECT 1"},
                                          {"type": "Query", "text": expr("@concat('SELECT ', pipeline().parameters.n)")}]},
             linkedServiceName=ref("LinkedService", "LS_SQL"))
    rec, content, a = one(sc, parameters=["n"], **ESTATE)
    tp = find(content, "Sc")["typeProperties"]
    assert tp["scripts"][0]["text"] == "TRUNCATE TABLE t; INSERT INTO t SELECT 1"  # SQL text untouched
    assert tp["database"] == "<database:LS_SQL>"
    assert any(p.kind == "configuration" for p in a.prerequisites)
    assert {c.operation for c in a.operation_coverage if c.covered_by} >= {"transform"} and rec.structural_valid


def test_script_on_oracle_is_supported_but_on_mysql_is_blocked():
    sc = act("Sc", "Script", {"scripts": [{"type": "Query", "text": "select 1 from dual"}]},
             linkedServiceName=ref("LinkedService", "LS_X"))
    _, _, ok = one(sc, linked_services=[linked_service("LS_X", "Oracle")])
    _, _, bad = one(sc, linked_services=[linked_service("LS_X", "MySql")])
    assert ok.status is not S.blocked and bad.status is S.blocked


def test_web_activity_splits_the_url_into_connection_and_relative_url():
    w = act("Call", "WebActivity", {"url": "https://api.example.com/v1/items?x=1", "method": "post",
                                    "headers": {"Content-Type": "application/json"}, "body": {"a": 1},
                                    "authentication": {"type": "Basic", "username": "u",
                                                       "password": {"type": "SecureString", "value": "PW123"}}})
    _, content, a = one(w)
    c = find(content, "Call")
    assert c["type"] == "WebActivity" and c["typeProperties"]["relativeUrl"] == "/v1/items?x=1"
    assert c["typeProperties"]["method"] == "POST" and c["typeProperties"]["body"] == {"a": 1}
    assert c["externalReferences"] == {"connection": "<connection:web:api.example.com>"}
    assert "authentication" not in c["typeProperties"] and "PW123" not in json.dumps(content)
    assert any(m.source == "typeProperties.authentication" and not m.preserved for m in a.property_mappings)
    assert any(p.kind == "connection" and "api.example.com" in p.detail for p in a.prerequisites)


def test_web_activity_with_a_dynamic_url_needs_manual_work():
    w = act("Call", "WebActivity", {"url": expr("@concat('https://h/', pipeline().parameters.p)"), "method": "GET"})
    _, content, a = one(w, parameters=["p"])
    assert a.status is S.manual_required and "web_dynamic_url" in codes(a)
    assert find(content, "Call")["typeProperties"]["relativeUrl"]["value"].startswith("@concat(")


def test_webhook_and_azure_function():
    wh = act("Hook", "WebHook", {"url": "https://h.example.com/cb", "method": "POST", "timeout": "00:10:00",
                                 "reportStatusOnCallBack": True})
    fn = act("Fn", "AzureFunctionActivity", {"functionName": "doit", "method": "get"},
             linkedServiceName=ref("LinkedService", "LS_FN"))
    rec, content = convert_one(pipe(wh, fn), linked_services=[linked_service("LS_FN", "AzureFunction")])
    h = find(content, "Hook")
    assert h["type"] == "WebHook" and h["typeProperties"]["relativeUrl"] == "/cb" and h["typeProperties"]["timeout"] == "00:10:00"
    f = find(content, "Fn")
    assert f["type"] == "AzureFunction" and f["typeProperties"] == {"method": "GET", "functionName": "doit"}
    assert f["externalReferences"] == {"connection": "<connection:LS_FN>"}
    assert by_name(rec)["Fn"].runtime_disposition == "reuse_externally"


# =============================================================================
# Compute / transformation
# =============================================================================


def test_databricks_notebook_is_reused_externally_with_a_cluster_step():
    nb = act("Nb", "DatabricksNotebook", {"notebookPath": "/Shared/etl", "baseParameters": {"d": expr("@utcnow()")}},
             linkedServiceName=ref("LinkedService", "LS_DBX"))
    _, content, a = one(nb, linked_services=[linked_service("LS_DBX", "AzureDatabricks")])
    c = find(content, "Nb")
    assert c["type"] == "DatabricksNotebook" and c["typeProperties"]["notebookPath"] == "/Shared/etl"
    assert c["externalReferences"] == {"connection": "<connection:LS_DBX>"}
    assert a.runtime_disposition == "reuse_externally" and a.status is S.manual_required
    assert any("existingClusterId" in s for s in a.manual_steps)


def test_databricks_jar_is_blocked_with_an_alternative():
    jar = act("Jar", "DatabricksSparkJar", {"mainClassName": "x.Main"}, linkedServiceName=ref("LinkedService", "LS_DBX"))
    _, _, a = one(jar, linked_services=[linked_service("LS_DBX", "AzureDatabricks")])
    assert a.status is S.blocked and a.alternatives and a.runtime_disposition == "reuse_externally"


def test_hdinsight_hive_converts_but_mapreduce_is_blocked():
    hive = act("Hv", "HDInsightHive", {"scriptPath": "scripts/a.hql", "scriptLinkedService": ref("LinkedService", "LS_ST"),
                                       "getDebugInfo": "Failure"}, linkedServiceName=ref("LinkedService", "LS_HDI"))
    mr = act("Mr", "HDInsightMapReduce", {"className": "C", "jarFilePath": "j.jar"}, linkedServiceName=ref("LinkedService", "LS_HDI"))
    lss = [linked_service("LS_HDI", "HDInsight"), linked_service("LS_ST", "AzureBlobStorage")]
    rec, content = convert_one(pipe(hive, mr), linked_services=lss)
    h = find(content, "Hv")
    assert h["type"] == "AzureHDInsight" and h["typeProperties"]["hdiActivityType"] == "Hive"
    assert h["typeProperties"]["scriptSettings"] == {"scriptPath": "scripts/a.hql",
                                                     "externalReferences": {"connection": "<connection:LS_ST>"}}
    assert by_name(rec)["Mr"].status is S.blocked


def test_custom_batch_activity_maps_connections_and_drops_reference_objects():
    cu = act("Cu", "Custom", {"command": "python run.py", "resourceLinkedService": ref("LinkedService", "LS_ST"),
                              "folderPath": "app", "referenceObjects": {"linkedServices": [], "datasets": []}},
             linkedServiceName=ref("LinkedService", "LS_BATCH"))
    lss = [linked_service("LS_BATCH", "AzureBatch"), linked_service("LS_ST", "AzureBlobStorage")]
    _, content, a = one(cu, linked_services=lss)
    c = find(content, "Cu")
    assert c["type"] == "Custom" and c["typeProperties"]["externalReferences"] == {"connection": "<connection:LS_BATCH>"}
    assert c["staging"] == {"externalReferences": {"connection": "<connection:LS_ST>"}}
    assert any(m.source == "typeProperties.referenceObjects" and not m.preserved for m in a.property_mappings)


def test_azure_ml_pipeline_converts_and_deprecated_ml_is_blocked():
    ml = act("Ml", "AzureMLExecutePipeline", {"mlPipelineId": "abc", "experimentName": "e"},
             linkedServiceName=ref("LinkedService", "LS_ML"))
    old = act("Old", "AzureMLBatchExecution", {}, linkedServiceName=ref("LinkedService", "LS_ML"))
    rec, content = convert_one(pipe(ml, old), linked_services=[linked_service("LS_ML", "AzureMLService")])
    assert find(content, "Ml")["type"] == "AzureMLExecutePipeline" and "policy" in find(content, "Ml")
    assert by_name(rec)["Old"].status is S.blocked


def test_synapse_notebook_is_a_scaffold_that_needs_the_notebook_migrated():
    nb = act("Syn", "SynapseNotebook", {"notebook": ref("Notebook", "Clean"), "sparkPool": ref("BigDataPool", "pool"),
                                        "parameters": {"d": {"value": "x", "type": "string"}}})
    _, content, a = one(nb)
    c = find(content, "Syn")
    assert c["type"] == "TridentNotebook" and c["typeProperties"]["notebookId"] == "<item:notebook:Clean>"
    assert c["typeProperties"]["parameters"] == {"d": {"value": "x", "type": "string"}}
    assert a.strategy is St.alternative and a.status is S.manual_required and a.runtime_disposition == "replace"
    assert any(m.source == "typeProperties.sparkPool" and not m.preserved for m in a.property_mappings)


def test_mapping_data_flow_becomes_a_dataflow_scaffold_with_alternatives():
    df = act("Df", "ExecuteDataFlow", {"dataflow": {"referenceName": "MDF1", "type": "DataFlowReference",
                                                    "parameters": {"p": expr("@pipeline().parameters.x")}},
                                       "compute": {"coreCount": 8, "computeType": "General"}})
    _, content, a = one(df, parameters=["x"])
    c = find(content, "Df")
    assert c["type"] == "RefreshDataFlow" and c["typeProperties"]["dataflowId"] == "<item:dataflow:MDF1>"
    assert c["typeProperties"]["parameters"]["p"]["value"] == "@pipeline().parameters.x"
    assert a.strategy is St.alternative and a.status is S.manual_required
    assert any("Dataflow Gen2" in x for x in a.alternatives + a.manual_steps)


def test_ssis_and_usql_are_blocked_with_documented_alternatives():
    ss = act("Ss", "ExecuteSSISPackage", {"packageLocation": {"type": "SSISDB"}})
    us = act("Us", "DataLakeAnalyticsU-SQL", {"scriptPath": "a.usql", "scriptLinkedService": ref("LinkedService", "S")},
             linkedServiceName=ref("LinkedService", "ADLA"))
    rec, content = convert_one(pipe(ss, us))
    r = by_name(rec)
    assert r["Ss"].status is S.blocked and r["Ss"].runtime_disposition == "redesign"
    assert any("Invoke pipeline" in x for x in r["Ss"].alternatives)
    assert r["Us"].status is S.blocked and any("notebook" in x.lower() for x in r["Us"].alternatives)
    assert find(content, "Ss")["type"] == "Fail" and find(content, "Us")["type"] == "Fail"


def test_unknown_activity_type_is_blocked_not_dropped():
    new = act("Fut", "FabricWidgetV9", {"a": 1})
    rec, content, a = one(new)
    assert a.status is S.blocked and "no_conversion_rule" in codes(a)
    assert find(content, "Fut")["type"] == "Fail" and rec.coverage.blocked == ["PL/Fut"] and rec.coverage.complete


# =============================================================================
# Dependencies
# =============================================================================


def test_every_dependency_condition_is_preserved():
    a, b = act("A", "Wait", {"waitTimeInSeconds": 1}), act("B", "Wait", {"waitTimeInSeconds": 1})
    c = act("C", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("A", "Failed", "Skipped"), dep("B", "Completed")])
    rec, content = convert_one(pipe(a, b, c))
    assert find(content, "C")["dependsOn"] == [
        {"activity": "A", "dependencyConditions": ["Failed", "Skipped"]},
        {"activity": "B", "dependencyConditions": ["Completed"]}]
    assert [d.conditions for d in by_name(rec)["C"].dependency_mappings] == [["Failed", "Skipped"], ["Completed"]]


def test_dependents_of_a_composite_attach_to_its_exit_and_non_success_conditions_are_flagged():
    v = act("V", "Validation", {"dataset": ref("Dataset", "DS_SQL")})
    on_fail = act("Alert", "Fail", {"message": "late", "errorCode": "1"}, deps=[dep("V", "Failed")])
    rec, content = convert_one(pipe(v, on_fail), **ESTATE)
    assert find(content, "Alert")["dependsOn"] == [{"activity": "V", "dependencyConditions": ["Failed"]}]
    assert "composite_dependency_condition" in codes(by_name(rec)["Alert"])
    assert by_name(rec)["Alert"].status is S.converted_with_differences


def test_a_failure_handler_of_a_blocked_activity_is_warned_about():
    bad = act("Bad", "ExecuteSSISPackage", {"packageLocation": {"type": "SSISDB"}})
    handler = act("OnFail", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("Bad", "Failed")])
    ok = act("OnOk", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("Bad")])
    rec, _ = convert_one(pipe(bad, handler, ok))
    r = by_name(rec)
    assert "depends_on_blocked_activity" in codes(r["OnFail"]) and r["OnFail"].status is S.converted_with_differences
    assert "depends_on_blocked_activity" not in codes(r["OnOk"])  # skipped safely when the placeholder fails


def test_inactive_state_is_preserved_with_fabrics_spelling():
    w = act("W", "Wait", {"waitTimeInSeconds": 1}, state="Inactive", onInactiveMarkAs="Skipped")
    _, content, _ = one(w)
    c = find(content, "W")
    assert c["state"] == "InActive" and c["onInactiveMarkAs"] == "Skipped"


def test_policy_is_carried_and_out_of_range_values_are_flagged():
    w = act("W", "Wait", {"waitTimeInSeconds": 1}, policy={"timeout": "0.00:10:00", "retry": 3, "retryIntervalInSeconds": 10})
    rec, content, a = one(w)
    assert find(content, "W")["policy"] == {"timeout": "0.00:10:00", "retry": 3, "retryIntervalInSeconds": 10}
    assert "retry_interval_out_of_range" in codes(a)
    assert not rec.structural_valid and a.status is S.manual_required  # the schema rejects 10s


# =============================================================================
# Honesty: status, validity, readiness
# =============================================================================


def test_structural_validity_and_runtime_readiness_are_independent():
    rec, _ = convert_one(pipe(copy_activity()), **ESTATE)
    assert rec.structural_valid and not rec.runtime_ready
    assert all(not q.bound for q in rec.prerequisites)


def test_resolutions_bind_real_ids_and_make_the_pipeline_runtime_ready():
    guid_a, guid_b = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
    rec, content = convert_one(pipe(copy_activity()), **ESTATE, resolutions=[
        {"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": guid_a},
        {"type": "LinkedServiceToConnectionId", "key": "LS_LAKE", "value": guid_b}])
    c = find(content, "Copy")["typeProperties"]
    assert c["source"]["datasetSettings"]["externalReferences"]["connection"] == guid_a
    assert c["sink"]["datasetSettings"]["externalReferences"]["connection"] == guid_b
    assert all(q.bound for q in rec.prerequisites) and rec.runtime_ready and rec.structural_valid


def test_unsupported_expression_prevents_a_converted_status():
    w = act("W", "Wait", {"waitTimeInSeconds": expr("@int(decimal('1'))")})
    rec, _, a = one(w)
    assert a.status is S.manual_required and not a.structural_valid
    assert a.warnings and any(i.impact is Impact.manual for i in a.warnings)
    assert rec.status is S.manual_required and not rec.structural_valid


def test_masked_secrets_are_never_emitted_and_demand_re_entry():
    w = act("Call", "WebActivity", {"url": "https://h.example.com/x", "method": "POST",
                                    "headers": {"Authorization": "Bearer abc123SECRET"}})
    _, content, a = one(w)
    assert "abc123SECRET" not in json.dumps(content)
    assert "secret_masked" in codes(a) and a.status is S.manual_required


def test_every_source_activity_has_exactly_one_record_even_when_nested_and_blocked():
    inner = act("Inner", "ExecuteSSISPackage", {"packageLocation": {"type": "SSISDB"}})
    fe = act("Each", "ForEach", {"items": expr("@createArray(1)"), "activities": [inner, act("Ok", "Wait", {"waitTimeInSeconds": 1})]})
    rec, content = convert_one(pipe(fe, act("Tail", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("Each")])))
    ids = [a.source_activity_id for a in rec.activities]
    assert ids == ["PL/Each", "PL/Each/Inner", "PL/Each/Ok", "PL/Tail"] and len(set(ids)) == 4
    assert rec.coverage.complete and rec.coverage.blocked == ["PL/Each/Inner"] and rec.coverage.source_activities == 4
    assert find(content, "Inner")["type"] == "Fail"
    assert rec.status is S.blocked


def test_activity_limit_is_enforced_structurally():
    many = [act(f"W{i}", "Wait", {"waitTimeInSeconds": 1}) for i in range(121)]
    rec, _ = convert_one(pipe(*many))
    assert not rec.structural_valid and any(i.code == "activity_limit_exceeded" for i in rec.validation.issues)


def test_conversion_is_deterministic():
    p = pipe(copy_activity(), act("S", "SetVariable", {"variableName": "v", "value": expr("@utcnow()")}), variables={"v": "String"})
    a = convert_one(p, **ESTATE)
    b = convert_one(p, **ESTATE)
    assert a[0].model_dump_json() == b[0].model_dump_json() and a[1] == b[1]


def test_triggers_are_reported_for_manual_recreation():
    rec, _ = convert_one(pipe(act("W", "Wait", {"waitTimeInSeconds": 1}), name="PL"),
                         triggers=[trigger("Daily", "PL"), trigger("Win", "PL", "TumblingWindowTrigger")])
    kinds = {t.name: t for t in rec.triggers}
    assert "schedule" in kinds["Daily"].action.lower() and "interval schedule" in kinds["Win"].action
    assert "every 1 Day" in kinds["Daily"].summary


@pytest.mark.parametrize("activity", [
    copy_activity(), act("L", "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS_SQL")}),
    act("S", "SqlServerStoredProcedure", {"storedProcedureName": "p"}, linkedServiceName=ref("LinkedService", "LS_SQL")),
])
def test_generated_json_only_uses_documented_properties(activity):
    rec, _ = convert_one(pipe(activity), **ESTATE)
    undocumented = [i for i in rec.validation.issues if i.code.startswith("undocumented")]
    assert undocumented == [], undocumented
