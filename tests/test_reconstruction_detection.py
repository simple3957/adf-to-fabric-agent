"""Incompleteness detection at activity level, and the four kinds of 'not usable' kept apart."""

from __future__ import annotations

import json

from adf2fabric.project.reconstruction import Completeness, IncompletenessClass as C, Severity
from tests.reconstruction_support import (
    DS_LAKE, DS_SQL, ESTATE, SQL_LS, act, blockers, copy_activity, dataset, dep, expr, findings, linked_service,
    pipeline_of, pl, proposals, reconstruct, ref, trigger, with_estate,
)

WAIT = {"waitTimeInSeconds": 1}


# --- invalid JSON: truncated vs malformed vs empty ----------------------------------------------------------------


def test_truncated_malformed_and_empty_files_are_told_apart(tmp_path):
    good = json.dumps(pl(act("W", "Wait", WAIT), name="PL_Good"))
    files = with_estate(
        pl(act("W", "Wait", WAIT), name="PL_Good"),
        **{"pipeline/PL_Cut.json": good[: len(good) // 2],
           "pipeline/PL_Bad.json": '{"name": "PL_Bad", "properties": {"activities": [],}}',
           "pipeline/PL_Empty.json": "",
           })
    _, rec = reconstruct(tmp_path, files)
    by = {f.source_path: f for f in rec.source_findings}
    assert by["pipeline/PL_Cut.json"].code == "truncated_json" and "cut off" in by["pipeline/PL_Cut.json"].message
    assert by["pipeline/PL_Bad.json"].code == "invalid_json"
    assert by["pipeline/PL_Empty.json"].code == "empty_json_file"
    assert {f.incompleteness for f in by.values()} == {C.invalid_json}
    # an unreadable pipeline is reported as unusable, never silently absent
    unusable = {p.pipeline for p in rec.pipelines if p.completeness is Completeness.unusable}
    assert unusable == {"PL_Cut", "PL_Bad", "PL_Empty"}
    assert pipeline_of(rec, "PL_Good").completeness is Completeness.complete
    assert len([b for b in blockers(rec) if b.pipeline is None]) == 3


def test_a_referenced_dataset_whose_file_is_broken_is_invalid_json_not_missing(tmp_path):
    files = with_estate(pl(copy_activity("Copy")), **{"dataset/DS_SQL.json": '{"name": "DS_SQL", "properties": {'})
    _, rec = reconstruct(tmp_path, files)
    fs = findings(rec, "referenced_artifact_unreadable")
    assert fs and fs[0].incompleteness is C.invalid_json and "could not be parsed" in fs[0].message
    assert not findings(rec, "missing_dataset")  # the artifact exists - it is unreadable, a different problem


# --- missing artifacts ---------------------------------------------------------------------------------------------------


def test_missing_dataset_child_pipeline_linked_service_and_trigger_target(tmp_path):
    sp = act("Sp", "SqlServerStoredProcedure", {"storedProcedureName": "p"}, linkedServiceName=ref("LinkedService", "LS_GONE"))
    run = act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Ghost")})
    cp = copy_activity("Copy")
    cp["inputs"] = [ref("Dataset", "DS_MISSING")]
    files = with_estate(pl(cp, sp, run, name="PL_A"),
                        **{"trigger/T.json": trigger("T", "PL_Nowhere"),
                           "dataset/DS_Orphan.json": dataset("DS_Orphan", "AzureSqlTable", "LS_Absent", {"table": "t"})})
    _, rec = reconstruct(tmp_path, files)
    codes = {f.code: f for f in findings(rec)}
    for code in ("missing_dataset", "missing_pipeline", "missing_linkedservice", "trigger_pipeline_missing",
                 "dataset_linked_service_missing"):
        assert code in codes and codes[code].incompleteness is C.missing_artifact, code
    assert codes["missing_dataset"].activity_id == "PL_A/Copy"
    # nothing can be invented: every one of the activity-level gaps ends in a blocker that names what is needed
    names = " ".join(i for b in blockers(rec) for i in b.information_required)
    assert "DS_MISSING" in names and "PL_Ghost" in names and "LS_GONE" in names


# --- incomplete configuration -------------------------------------------------------------------------------------------


def test_missing_and_empty_required_properties_are_found_per_activity(tmp_path):
    fe = act("Each", "ForEach", {"activities": []})  # no items; empty activities
    wt = act("W", "Wait", {})
    rec = reconstruct(tmp_path, with_estate(pl(fe, wt)))[1]
    fs = {(f.activity_id, f.code, f.property_path) for f in findings(rec)}
    assert ("PL/Each", "missing_required_property", "typeProperties.items") in fs
    assert ("PL/Each", "empty_required_property", "typeProperties.activities") in fs
    assert ("PL/W", "missing_required_property", "typeProperties.waitTimeInSeconds") in fs
    assert all(f.incompleteness is C.incomplete_configuration for f in findings(rec) if f.activity_id)
    b = next(b for b in blockers(rec) if b.activity_id == "PL/W")
    assert "waitTimeInSeconds" in b.information_required[0] and b.minimum_information and b.closest_safe_implementation


def test_empty_script_and_query_are_blockers_and_no_sql_is_invented(tmp_path):
    sc = act("Sc", "Script", {"scripts": [{"type": "NonQuery", "text": ""}]}, linkedServiceName=ref("LinkedService", "LS_SQL"))
    lk = act("Lk", "Lookup", {"source": {"type": "AzureSqlSource", "sqlReaderQuery": ""}, "dataset": ref("Dataset", "DS_SQL")})
    _, rec = reconstruct(tmp_path, with_estate(pl(sc, lk)))
    assert {f.code for f in findings(rec)} >= {"empty_script_body", "empty_query"}
    assert proposals(rec) == []  # evidence never justifies fabricating SQL
    assert any("SQL" in " ".join(b.information_required) or "query" in " ".join(b.information_required) for b in blockers(rec))


def test_unresolved_parameters_variables_and_global_parameters(tmp_path):
    w = act("W", "Wait", {"waitTimeInSeconds": expr("@add(pipeline().parameters.nope, 1)")})
    v = act("V", "SetVariable", {"variableName": "ghost", "value": expr("@variables('missing')")}, deps=[dep("W")])
    g = act("G", "Wait", {"waitTimeInSeconds": expr("@pipeline().globalParameters.env")}, deps=[dep("V")])
    _, rec = reconstruct(tmp_path, with_estate(pl(w, v, g)))
    codes = {f.code for f in findings(rec)}
    assert {"unresolved_parameter", "unresolved_variable", "unresolved_global_parameter"} <= codes
    assert all(f.incompleteness is C.incomplete_configuration for f in findings(rec))


def test_unresolved_dynamic_expressions_are_reported_and_preserved(tmp_path):
    w = act("W", "Wait", {"waitTimeInSeconds": expr("@concat(")})
    x = act("X", "Wait", {"waitTimeInSeconds": expr("@frobnicate(1)")})
    rec = reconstruct(tmp_path, with_estate(pl(w, x)))[1]
    fs = findings(rec, "unresolved_expression")
    assert {f.activity_id for f in fs} == {"PL/W", "PL/X"} and all(f.property_path for f in fs)
    assert proposals(rec) == []  # an expression is never "repaired" by guessing


def test_missing_dependency_target_is_a_finding_and_blocker(tmp_path):
    a = act("A", "Wait", WAIT, deps=[dep("Ghost")])
    _, rec = reconstruct(tmp_path, with_estate(pl(a)))
    assert findings(rec, "dependency_undefined")[0].activity_id == "PL/A"
    assert any("Ghost" in " ".join(b.information_required) for b in blockers(rec))


def test_fabric_execution_configuration_gaps_are_reported(tmp_path):
    sc = act("Sc", "Script", {"scripts": [{"type": "Query", "text": "select 1"}]}, linkedServiceName=ref("LinkedService", "LS_SQL"))
    nb = act("Nb", "DatabricksNotebook", {"notebookPath": "/x"}, linkedServiceName=ref("LinkedService", "LS_DBX"))
    web = act("Web", "WebActivity", {"url": expr("@concat('https://h/', pipeline().RunId)"), "method": "GET"})
    _, rec = reconstruct(tmp_path, with_estate(pl(sc, nb, web), **{"linkedService/LS_DBX.json": linked_service("LS_DBX", "AzureDatabricks")}))
    codes = {f.code for f in findings(rec)}
    assert {"fabric_config_script_database", "fabric_config_databricks_cluster", "fabric_config_dynamic_url"} <= codes
    p = pipeline_of(rec, "PL")
    assert p.executable_as_is is False  # a pipeline needing configuration is never reported as runnable


# --- unsupported functionality is a different class ----------------------------------------------------------------------


def test_unsupported_functionality_is_not_confused_with_incompleteness(tmp_path):
    ss = act("Ss", "ExecuteSSISPackage", {"packageLocation": {"type": "SSISDB"}})
    gen1 = linked_service("LS_G1", "AzureDataLakeStore")
    ds = dataset("DS_G1", "AzureDataLakeStoreFile", "LS_G1", {"folderPath": "x"})
    cp = copy_activity("Copy")
    cp["inputs"] = [ref("Dataset", "DS_G1")]
    _, rec = reconstruct(tmp_path, with_estate(pl(ss, cp), **{"linkedService/LS_G1.json": gen1, "dataset/DS_G1.json": ds}))
    uns = [f for f in findings(rec) if f.incompleteness is C.unsupported_functionality]
    assert {f.code for f in uns} == {"unsupported_activity_type", "unsupported_connector"}
    p = pipeline_of(rec, "PL")
    assert p.executable_as_is is False
    # unsupported items do not count as gaps to reconstruct
    assert not any(f.id in p.unresolved_finding_ids and f.incompleteness is not C.unsupported_functionality
                   for f in p.findings if f.code in ("unsupported_activity_type", "unsupported_connector"))


def test_all_four_classes_are_counted_separately(tmp_path):
    files = with_estate(pl(act("W", "Wait", {}), act("Ss", "ExecuteSSISPackage", {"packageLocation": {}}),
                           _missing_ds_copy()),
                        **{"pipeline/Broken.json": "{"})
    _, rec = reconstruct(tmp_path, files)
    assert set(rec.finding_counts) >= {"invalid_json", "missing_artifact", "incomplete_configuration",
                                       "unsupported_functionality"}


def _missing_ds_copy():
    c = copy_activity("Copy")
    c["inputs"] = [ref("Dataset", "DS_NOPE")]
    return c


def test_a_clean_pipeline_is_complete_and_has_no_findings(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(pl(copy_activity("Copy"))))
    p = pipeline_of(rec, "PL")
    assert p.completeness is Completeness.complete and p.executable_as_is and not p.findings and not p.proposals
    assert rec.status.value == "complete"


def test_findings_have_stable_ids_and_severities(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(pl(act("W", "Wait", {}))))
    ids = [f.id for f in findings(rec)]
    assert ids == sorted(ids) and ids[0] == "F-0001"
    assert all(f.severity in set(Severity) for f in findings(rec))
