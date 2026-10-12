"""Recognised ingestion/orchestration patterns: incremental load, staging-to-curated, error handling, recognition."""

from __future__ import annotations

from adf2fabric.project.reconstruction import Completeness
from tests.reconstruction_support import (
    DS_LAKE, DS_SQL, HIGH, LOW, MEDIUM, act, blockers, completed, copy_activity, dataset, dep, expr, findings,
    pipeline_of, pl, proposals, reconstruct, ref, with_estate,
)

WAIT = {"waitTimeInSeconds": 1}
CTRL = dataset("DS_CTRL", "AzureSqlTable", "LS_SQL", {"schema": "dbo", "table": "Watermark"},
               columns=[{"name": "WatermarkValue", "type": "datetime"}])
OLD_Q = "SELECT WatermarkValue FROM dbo.Watermark WHERE TableName = 'Customer'"
NEW_Q = "SELECT MAX(ModifiedDate) AS NewWatermark FROM dbo.Customer"


def inc_pipeline(old_q=OLD_Q, new_q=NEW_Q, *, extra=(), name="PL_Inc", with_new=True):
    old = act("Lookup_Old_Watermark", "Lookup", {"source": {"type": "AzureSqlSource", "sqlReaderQuery": old_q},
                                                 "dataset": ref("Dataset", "DS_CTRL")})
    new = act("Lookup_New_Watermark", "Lookup", {"source": {"type": "AzureSqlSource", "sqlReaderQuery": new_q},
                                                 "dataset": ref("Dataset", "DS_SQL")})
    cp = copy_activity("Copy_Delta")
    q = "SELECT * FROM dbo.Customer WHERE ModifiedDate > '@{activity('Lookup_Old_Watermark').output.firstRow.WatermarkValue}'"
    if with_new:
        q += " AND ModifiedDate <= '@{activity('Lookup_New_Watermark').output.firstRow.NewWatermark}'"
    cp["typeProperties"]["source"]["sqlReaderQuery"] = expr(q)
    cp["dependsOn"] = [dep("Lookup_Old_Watermark")] + ([dep("Lookup_New_Watermark")] if with_new else [])
    acts = [old, *([new] if with_new else []), cp, *extra]
    return pl(*acts, name=name)


FILES = {"dataset/DS_CTRL.json": CTRL}


# =============================================================================
# incremental load: the watermark update
# =============================================================================


def test_the_missing_watermark_update_is_reconstructed_from_the_pipelines_own_lookups(tmp_path):
    store, rec = reconstruct(tmp_path, with_estate(inc_pipeline(), **FILES))
    [p] = proposals(rec, "watermark_update_from_lookups")
    assert p.confidence is MEDIUM and p.safe_to_generate and p.derived_sql and p.action == "add_activity"
    script = p.proposed
    assert script["type"] == "Script" and script["dependsOn"] == [{"activity": "Copy_Delta", "dependencyConditions": ["Succeeded"]}]
    text = script["typeProperties"]["scripts"][0]["text"]["value"]
    assert text == ("UPDATE dbo.Watermark SET WatermarkValue = "
                    "'@{activity('Lookup_New_Watermark').output.firstRow.NewWatermark}' WHERE TableName = 'Customer'")
    assert script["linkedServiceName"]["referenceName"] == "LS_SQL"
    # every part of the statement is traceable to evidence
    quoted = " ".join(e.detail for e in p.evidence)
    assert OLD_Q in quoted and NEW_Q in quoted and "dbo.Watermark" in quoted
    assert any("quoted" in a for a in p.assumptions) and any("WHERE" in a or "rows matching" in a for a in p.assumptions)
    assert p.alternatives
    # the completion has the new activity after the Copy; the original has none
    names = [a["name"] for a in completed(store, "pipeline/PL_Inc.json")["properties"]["activities"]]
    assert names == ["Lookup_Old_Watermark", "Lookup_New_Watermark", "Copy_Delta", "Update Watermark - Copy_Delta"]
    assert len(__import__("json").loads((store.source_dir("test-proj") / "pipeline/PL_Inc.json").read_text())["properties"]["activities"]) == 3
    assert p.creates_activity_id == "PL_Inc/Update Watermark - Copy_Delta" and p.affects_activity_ids == [p.creates_activity_id]
    pat = [x for x in pipeline_of(rec, "PL_Inc").patterns if x.name == "incremental_load"][0]
    assert not pat.complete and pat.missing == ["watermark update"]


def test_a_numeric_watermark_column_is_not_quoted(tmp_path):
    ctrl = dataset("DS_CTRL", "AzureSqlTable", "LS_SQL", {"schema": "dbo", "table": "Watermark"},
                   columns=[{"name": "WatermarkValue", "type": "bigint"}])
    _, rec = reconstruct(tmp_path, with_estate(inc_pipeline(), **{"dataset/DS_CTRL.json": ctrl}))
    [p] = proposals(rec, "watermark_update_from_lookups")
    assert "SET WatermarkValue = @{activity('Lookup_New_Watermark').output.firstRow.NewWatermark} WHERE" in \
        p.proposed["typeProperties"]["scripts"][0]["text"]["value"]
    assert any("numeric" in a for a in p.assumptions)


def test_without_a_where_clause_the_assumption_is_stated(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(inc_pipeline(old_q="SELECT WatermarkValue FROM dbo.Watermark"), **FILES))
    [p] = proposals(rec, "watermark_update_from_lookups")
    assert p.proposed["typeProperties"]["scripts"][0]["text"]["value"].endswith("firstRow.NewWatermark}'")
    assert any("no WHERE" in a for a in p.assumptions) and p.confidence is MEDIUM


def test_a_complex_stored_watermark_query_is_not_turned_into_sql(tmp_path):
    q = "SELECT w.WatermarkValue FROM dbo.Watermark w JOIN dbo.Tables t ON t.Id = w.TableId WHERE t.Name = 'C'"
    store, rec = reconstruct(tmp_path, with_estate(inc_pipeline(old_q=q), **FILES))
    [p] = proposals(rec, "watermark_update_from_lookups")
    assert p.confidence is LOW and not p.safe_to_generate and p.proposed["scripts"][0]["text"].startswith("<UPDATE")
    assert any("control table, column and key" in i for i in p.information_required)
    assert not (store.reconstruction_dir("test-proj") / "completed").exists()  # nothing generated
    assert any("watermark write-back cannot be derived" in b.reason for b in blockers(rec))
    assert pipeline_of(rec, "PL_Inc").completeness is Completeness.blocked


def test_a_max_query_without_an_alias_has_no_readable_output_property(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(inc_pipeline(new_q="SELECT MAX(ModifiedDate) FROM dbo.Customer"), **FILES))
    [p] = proposals(rec, "watermark_update_from_lookups")
    assert not p.safe_to_generate and any("alias" in i for i in p.information_required)


def test_a_pipeline_that_only_reads_the_stored_watermark_cannot_derive_the_new_value(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(inc_pipeline(with_new=False), **FILES))
    [p] = proposals(rec, "watermark_update_from_lookups")
    assert not p.safe_to_generate and any("new watermark value comes from" in i for i in p.information_required)


def test_an_existing_watermark_update_means_no_gap(tmp_path):
    upd = act("Update", "SqlServerStoredProcedure", {"storedProcedureName": "dbo.usp_UpdateWatermark",
              "storedProcedureParameters": {"v": {"value": expr("@{activity('Lookup_New_Watermark').output.firstRow.NewWatermark}"),
                                                  "type": "String"}}},
              deps=[dep("Copy_Delta")], linkedServiceName=ref("LinkedService", "LS_SQL"))
    _, rec = reconstruct(tmp_path, with_estate(inc_pipeline(extra=[upd]), **FILES))
    assert not findings(rec, "incremental_watermark_update_missing") and not proposals(rec, "watermark_update_from_lookups")
    pat = [x for x in pipeline_of(rec, "PL_Inc").patterns if x.name == "incremental_load"][0]
    assert pat.complete


def test_a_script_that_names_the_control_table_also_counts_as_the_update(tmp_path):
    upd = act("Update", "Script", {"scripts": [{"type": "NonQuery", "text": "UPDATE dbo.Watermark SET WatermarkValue = GETDATE()"}]},
              deps=[dep("Copy_Delta")], linkedServiceName=ref("LinkedService", "LS_SQL"))
    _, rec = reconstruct(tmp_path, with_estate(inc_pipeline(extra=[upd]), **FILES))
    assert not findings(rec, "incremental_watermark_update_missing")


def test_an_update_inside_a_loop_is_placed_in_the_same_scope_with_a_nested_id(tmp_path):
    old = act("Lookup_Old_Watermark", "Lookup", {"source": {"type": "AzureSqlSource", "sqlReaderQuery": OLD_Q},
                                                 "dataset": ref("Dataset", "DS_CTRL")})
    new = act("Lookup_New_Watermark", "Lookup", {"source": {"type": "AzureSqlSource", "sqlReaderQuery": NEW_Q},
                                                 "dataset": ref("Dataset", "DS_SQL")})
    cp = copy_activity("Copy_Delta")
    cp["typeProperties"]["source"]["sqlReaderQuery"] = expr(
        "SELECT * FROM t WHERE m > '@{activity('Lookup_Old_Watermark').output.firstRow.WatermarkValue}' AND m <= "
        "'@{activity('Lookup_New_Watermark').output.firstRow.NewWatermark}'")
    cp["dependsOn"] = [dep("Lookup_Old_Watermark"), dep("Lookup_New_Watermark")]
    each = act("Each", "ForEach", {"items": expr("@createArray(1)"), "activities": [old, new, cp]})
    store, rec = reconstruct(tmp_path, with_estate(pl(each, name="PL_Loop"), **FILES))
    [p] = proposals(rec, "watermark_update_from_lookups")
    assert p.creates_activity_id == "PL_Loop/Each/Update Watermark - Copy_Delta"
    inner = completed(store, "pipeline/PL_Loop.json")["properties"]["activities"][0]["typeProperties"]["activities"]
    assert [a["name"] for a in inner][-1] == "Update Watermark - Copy_Delta"


# =============================================================================
# staging -> curated
# =============================================================================


def stg_dataset(table):
    return dataset(f"DS_STG_{table}", "AzureSqlTable", "LS_SQL", {"schema": "Staging", "table": table})


def stg_pipeline(name, table, *, load=True, proc=None):
    cp = act("Copy", "Copy", {"source": {"type": "ParquetSource"}, "sink": {"type": "AzureSqlSink"}},
             inputs=[ref("Dataset", "DS_LAKE", folder="x")], outputs=[ref("Dataset", f"DS_STG_{table}")])
    acts = [cp]
    if load:
        acts.append(act("Load", "SqlServerStoredProcedure", {"storedProcedureName": proc or f"[Staging].[usp_Load{table}]"},
                        deps=[dep("Copy")], linkedServiceName=ref("LinkedService", "LS_SQL")))
    return pl(*acts, name=name)


def stg_files(*pipelines, tables=("Alpha", "Beta", "Gamma")):
    return with_estate(*pipelines, **{f"dataset/DS_STG_{t}.json": stg_dataset(t) for t in tables})


def test_a_missing_staging_load_is_derived_from_two_consistent_peers(tmp_path):
    files = stg_files(stg_pipeline("PL_A", "Alpha"), stg_pipeline("PL_B", "Beta"), stg_pipeline("PL_C", "Gamma", load=False))
    store, rec = reconstruct(tmp_path, files)
    [p] = proposals(rec, "staging_load_from_peers")
    assert p.confidence is MEDIUM and p.safe_to_generate and p.pipeline == "PL_C"
    assert p.proposed["typeProperties"]["storedProcedureName"] == "[Staging].[usp_LoadGamma]"
    assert p.proposed["linkedServiceName"]["referenceName"] == "LS_SQL"
    assert [e.kind.value for e in p.evidence] == ["repeated_pattern", "repeated_pattern"]
    done = completed(store, "pipeline/PL_C.json")["properties"]["activities"]
    assert [a["name"] for a in done] == ["Copy", "Load Gamma from staging"]


def test_one_peer_is_not_enough_unless_the_procedure_is_confirmed_to_exist(tmp_path):
    files = stg_files(stg_pipeline("PL_A", "Alpha"), stg_pipeline("PL_C", "Gamma", load=False))
    _, weak = reconstruct(tmp_path / "a", files)
    [p] = proposals(weak, "staging_load_from_peers")
    assert p.confidence is LOW and not p.safe_to_generate
    _, strong = reconstruct(tmp_path / "b", files, sql_procedures=[("Staging", "usp_LoadGamma")])
    [q] = proposals(strong, "staging_load_from_peers")
    assert q.confidence is MEDIUM and q.safe_to_generate
    assert {e.kind.value for e in q.evidence} == {"repeated_pattern", "project_artifact"}


def test_peers_whose_procedures_take_parameters_leave_that_information_required(tmp_path):
    pa = stg_pipeline("PL_A", "Alpha")
    pa["properties"]["activities"][1]["typeProperties"]["storedProcedureParameters"] = {"Batch": {"value": "x", "type": "String"}}
    files = stg_files(pa, stg_pipeline("PL_C", "Gamma", load=False), stg_pipeline("PL_B", "Beta"))
    pb = files["pipeline/PL_B.json"]
    pb["properties"]["activities"][1]["typeProperties"]["storedProcedureParameters"] = {"Batch": {"value": "y", "type": "String"}}
    _, rec = reconstruct(tmp_path, files)
    [p] = proposals(rec, "staging_load_from_peers")
    assert not p.safe_to_generate and "procedure parameters" in p.information_required[0]


def test_a_load_performed_by_the_calling_pipeline_is_not_a_gap(tmp_path):
    child = stg_pipeline("PL_Child", "Gamma", load=False)
    parent = pl(act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Child"), "waitOnCompletion": True}),
                act("Load", "SqlServerStoredProcedure", {"storedProcedureName": "[Staging].[usp_LoadGamma]"}, deps=[dep("Run")],
                    linkedServiceName=ref("LinkedService", "LS_SQL")), name="PL_Parent")
    _, rec = reconstruct(tmp_path, stg_files(child, parent))
    assert not findings(rec, "staging_load_missing")
    assert [x for x in pipeline_of(rec, "PL_Child").patterns if x.name == "staging_to_curated"][0].complete


def test_a_load_after_a_loop_that_calls_the_child_is_not_a_gap(tmp_path):
    """Regression (found on the real sample estate): the call sits inside a ForEach, the load follows the loop."""
    child = stg_pipeline("PL_Child", "Gamma", load=False)
    loop = act("Each", "ForEach", {"items": expr("@createArray(1, 2)"), "activities": [
        act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Child"), "waitOnCompletion": True})]})
    load = act("Load", "SqlServerStoredProcedure", {"storedProcedureName": "[Staging].[usp_LoadGamma]"}, deps=[dep("Each")],
               linkedServiceName=ref("LinkedService", "LS_SQL"))
    _, rec = reconstruct(tmp_path, stg_files(child, pl(loop, load, name="PL_Parent")))
    assert not findings(rec, "staging_load_missing")
    # ...but a load that runs BEFORE the loop does not count
    early = act("Load", "SqlServerStoredProcedure", {"storedProcedureName": "[Staging].[usp_LoadGamma]"},
                linkedServiceName=ref("LinkedService", "LS_SQL"))
    loop2 = act("Each", "ForEach", {"items": expr("@createArray(1)"), "activities": [
        act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Child")})]}, deps=[dep("Load")])
    _, rec2 = reconstruct(tmp_path / "b", stg_files(child, pl(early, loop2, name="PL_Parent")))
    assert findings(rec2, "staging_load_missing")


def test_with_no_peers_the_gap_is_reported_with_the_information_needed(tmp_path):
    _, rec = reconstruct(tmp_path, stg_files(stg_pipeline("PL_C", "Gamma", load=False)))
    assert findings(rec, "staging_load_missing") and not proposals(rec, "staging_load_from_peers")
    assert any("stored procedure or script that loads Staging.Gamma" in " ".join(b.information_required) for b in blockers(rec))


# =============================================================================
# error-handling convention
# =============================================================================


def guarded(name, with_handler=True):
    cp = copy_activity("Copy")
    acts = [cp]
    if with_handler:
        acts.append(act("Log Error", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Log"),
                                                         "parameters": {"msg": "failed"}}, deps=[dep("Copy", "Failed")]))
    return pl(*acts, name=name)


def test_a_convention_gap_is_reported_as_a_template_and_never_generated(tmp_path):
    log = pl(act("W", "Wait", WAIT), name="PL_Log", parameters=["msg"])
    files = with_estate(guarded("PL_A"), guarded("PL_B"), guarded("PL_C", with_handler=False), log)
    store, rec = reconstruct(tmp_path, files)
    [p] = proposals(rec, "error_handling_convention")
    assert p.pipeline == "PL_C" and not p.safe_to_generate and "convention" in p.not_generated_reason
    assert p.proposed["child_pipeline"] == "PL_Log" and p.proposed["dependency"] == "Failed"
    assert p.confidence is MEDIUM and not p.patches
    assert not (store.reconstruction_dir("test-proj") / "completed" / "pipeline" / "PL_C.json").exists()


def test_one_peer_or_inconsistent_peers_do_not_make_a_convention(tmp_path):
    log = pl(act("W", "Wait", WAIT), name="PL_Log", parameters=["msg"])
    _, rec = reconstruct(tmp_path, with_estate(guarded("PL_A"), guarded("PL_C", with_handler=False), log))
    assert not proposals(rec, "error_handling_convention")


# =============================================================================
# recognition
# =============================================================================


def names_of(rec, pipeline):
    return {p.name: p for p in pipeline_of(rec, pipeline).patterns}


def test_orchestration_and_conditional_and_retry_and_failure_patterns_are_recognized(tmp_path):
    lk = act("Check", "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS_SQL")})
    cond = act("Ready", "IfCondition", {"expression": expr("@greater(activity('Check').output.firstRow.n, 0)"),
                                        "ifTrueActivities": [act("Go", "Wait", WAIT)]}, deps=[dep("Check")])
    poll = act("Poll", "Until", {"expression": expr("@equals(1, 1)"), "activities": [act("Nap", "Wait", WAIT)]})
    run = act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Ghost")})
    alert = act("Alert", "Fail", {"message": "x", "errorCode": "1"}, deps=[dep("Run", "Failed")])
    _, rec = reconstruct(tmp_path, with_estate(pl(lk, cond, poll, run, alert)))
    pats = names_of(rec, "PL")
    assert {"conditional_execution", "retry_loop", "parent_child_orchestration", "failure_handling"} <= set(pats)
    assert pats["parent_child_orchestration"].missing == ["child pipeline 'PL_Ghost'"] and not pats["parent_child_orchestration"].complete
    assert pats["failure_handling"].activity_ids == ["PL/Alert"] and pats["retry_loop"].complete


def test_parameterized_ingestion_reused_across_tables_is_recognized_with_its_siblings(tmp_path):
    def reuse(name):
        c = copy_activity("Copy")
        c["outputs"] = [ref("Dataset", "DS_LAKE", folder=expr("@pipeline().parameters.table"))]
        return pl(c, act("Done", "Wait", WAIT, deps=[dep("Copy")]), name=name, parameters=["table"])
    _, rec = reconstruct(tmp_path, with_estate(reuse("PL_Orders"), reuse("PL_Customers")))
    pat = names_of(rec, "PL_Orders")["parameterized_ingestion"]
    assert any("PL_Customers" in e for e in pat.evidence) and pat.complete


def test_a_complete_incremental_pipeline_is_recognized_as_complete(tmp_path):
    upd = act("Update", "SqlServerStoredProcedure", {"storedProcedureName": "dbo.usp_UpdateWatermark"}, deps=[dep("Copy_Delta")],
              linkedServiceName=ref("LinkedService", "LS_SQL"))
    _, rec = reconstruct(tmp_path, with_estate(inc_pipeline(extra=[upd]), **FILES))
    assert names_of(rec, "PL_Inc")["incremental_load"].complete


def test_recognition_never_creates_anything(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(pl(act("Poll", "Until", {"expression": expr("@equals(1, 1)"),
                                                                      "activities": [act("Nap", "Wait", WAIT)]}))))
    assert names_of(rec, "PL")["retry_loop"] and not proposals(rec)
