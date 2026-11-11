"""Independent migration validation: static, API, deployment and runtime stages, with mutation tests on the generated files.

The engine must catch defects the converter does not report. The mutation tests prove it: each one edits a GENERATED file (or the
source) after a clean conversion and asserts the right check turns FAIL / BLOCKED. The real API, deployment and runtime stages are
checked against the real workspace in ``tests/test_live.py``.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from adf2fabric.agent.validation import diffing, engine, views
from adf2fabric.agent.validation.report import render
from adf2fabric.project.validation import ComponentStatus as S, Result as R, Stage
from tests.conversion_support import DS_LAKE, DS_SQL, LAKE_LS, SQL_LS, act, copy_activity, dep, expr, pipe, ref, trigger
from tests.test_adf_semantic_discovery import _make_project

CONN = "11111111-1111-1111-1111-111111111111"
WS = "53d67e72-8124-4ca3-ae55-c4b266935faa"
CHILD = "22222222-2222-2222-2222-222222222222"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

NESTED = pipe(
    act("Loop", "ForEach", {"items": expr("@activity('Look').output.value"), "isSequential": True, "activities": [
        act("Inner_Wait", "Wait", {"waitTimeInSeconds": 1}),
        act("Branch", "IfCondition", {"expression": expr("@equals(pipeline().parameters.mode, 'x')"),
                                      "ifTrueActivities": [act("Say", "SetVariable", {"variableName": "v", "value": expr("@pipeline().globalParameters.env")})],
                                      "ifFalseActivities": [act("Other", "Wait", {"waitTimeInSeconds": 2})]},
            deps=[dep("Inner_Wait")])]}, deps=[dep("Look")]),
    act("Look", "Lookup", {"source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT 1"}, "dataset": ref("Dataset", "DS_SQL"), "firstRowOnly": False}),
    name="PL_Nested", parameters=["mode"], variables={"v": "String"})

FILES = {
    "pipeline/PL_Main.json": pipe(
        copy_activity("Copy_Customer", ),
        act("Run_Child", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Child"), "waitOnCompletion": True}, deps=[dep("Copy_Customer")]), name="PL_Main"),
    "pipeline/PL_Child.json": pipe(act("Pause", "Wait", {"waitTimeInSeconds": 3}), name="PL_Child"),
    "pipeline/PL_Nested.json": NESTED,
    "dataset/DS_SQL.json": DS_SQL, "dataset/DS_LAKE.json": DS_LAKE,
    "linkedService/LS_SQL.json": SQL_LS, "linkedService/LS_LAKE.json": LAKE_LS,
    "trigger/TR_Daily.json": trigger("TR_Daily", "PL_Main"),
    "factory/f.json": {"name": "f", "properties": {"globalParameters": {"env": {"type": "String", "value": "dev"}}}},
}
RESOLUTIONS = [{"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": CONN}, {"type": "LinkedServiceToConnectionId", "key": "LS_LAKE", "value": CONN},
               {"type": "ItemToId", "key": "pipeline:PL_Child", "value": CHILD}, {"type": "WorkspaceId", "key": "*", "value": WS}, {"type": "ItemToId", "key": "connection:invoke-pipeline", "value": CONN}]


@pytest.fixture(scope="module")
def baseline(tmp_path_factory):
    """A project converted with every connection bound: the baseline every mutation starts from."""
    tmp = tmp_path_factory.mktemp("val")
    store, pid = _make_project(tmp, FILES)
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    res = tmp / "res.json"
    res.write_text(json.dumps(RESOLUTIONS), encoding="utf-8")
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj", resolutions_path=str(res)))
    assert r.success, r.error
    return store, pid


@pytest.fixture()
def project(baseline, tmp_path):
    """A private copy of the baseline's project directory so a mutation never leaks into another test."""
    store, pid = baseline
    from adf2fabric.project.store import ProjectStore

    dest = tmp_path / "projects"
    shutil.copytree(store.projects_root, dest)
    return ProjectStore(dest), pid


def run(store, pid, stages=("static",), **kw):
    return engine.validate(store, pid, stages=set(stages), now=NOW, **kw)


def check(report, id_, component=None):
    rows = [c for c in report.checks if c.id == id_ and (component is None or c.component == component)]
    assert rows, f"no check {id_} {component} in {[c.id for c in report.checks]}"
    return rows


def results(report, id_, component=None):
    return {c.result for c in check(report, id_, component)}


def edit_target(store, pid, pipeline, fn):
    f = store.conversion_dir(pid) / "pipelines" / f"{pipeline}.DataPipeline" / "pipeline-content.json"
    data = json.loads(f.read_text(encoding="utf-8"))
    fn(data)
    f.write_text(json.dumps(data), encoding="utf-8")


def acts(data):
    return list(views.walk_activities(data["properties"]["activities"]))


# =====================================================================================================================
# the baseline: a clean conversion validates statically, and says what it did not check
# =====================================================================================================================


def test_a_clean_conversion_passes_static_validation(project):
    store, pid = project
    r = run(store, pid)
    failing = [(c.id, c.component, c.summary) for c in r.checks if c.result in (R.FAIL, R.BLOCKED) and c.required]
    assert failing == [], failing
    assert {s.stage: s.status for s in r.stages}[Stage.static] is R.PASS


def test_a_clean_static_pass_is_never_a_migration_success(project):
    store, pid = project
    r = run(store, pid)
    assert r.overall == "NOT_VALIDATED" and r.full_success_claimed is False
    assert any("runtime" in x.lower() or "Stages not requested" in x for x in r.overall_reasons)
    assert {s.stage: s.status for s in r.stages}[Stage.runtime] is R.NOT_RUN and {s.stage: s.status for s in r.stages}[Stage.api] is R.NOT_RUN


def test_every_category_has_an_applicable_check(project):
    store, pid = project
    cats = {c.category.value for c in run(store, pid, stages=("static", "api", "deployment", "runtime")).checks}
    assert cats == {"source_inventory", "reference_integrity", "target_schema", "expression_preservation", "activity_graph", "connections_secrets",
                    "compute", "execution_semantics", "deployment", "runtime"}


def test_inventory_comparison_uses_stable_identities_and_statuses(project):
    store, pid = project
    comps = {c.id: c for c in run(store, pid).components}
    assert comps["pipeline:PL_Nested/activity:Say"].status is S.matched
    assert comps["pipeline:PL_Nested"].status in (S.matched, S.transformed)
    assert comps["dataset:DS_SQL"].status is S.transformed and "inlined" in comps["dataset:DS_SQL"].note
    assert comps["linked_service:LS_SQL"].status is S.transformed
    assert comps["trigger:TR_Daily"].status is S.transformed
    assert comps["global_parameter:env"].status is S.matched
    assert comps["pipeline:PL_Main/activity:Run_Child"].status is S.transformed  # ExecutePipeline -> InvokePipeline, a documented conversion
    assert comps["pipeline:PL_Main/activity:Run_Child"].diff == ["~ type: ExecutePipeline -> InvokePipeline"]


def test_nested_activities_are_covered(project):
    store, pid = project
    r = run(store, pid)
    inv = check(r, "activity_graph.inventory", "pipeline:PL_Nested")[0]
    assert inv.result is R.PASS and "6 activities" in inv.summary  # Look, Loop and the four nested ones: nested activities are counted
    assert check(r, "activity_graph.dependencies", "pipeline:PL_Nested")[0].result is R.PASS


# =====================================================================================================================
# mutations of the generated files: the engine must notice what the converter did not report
# =====================================================================================================================


def test_a_dropped_activity_fails(project):
    store, pid = project
    edit_target(store, pid, "PL_Nested", lambda d: d["properties"]["activities"][0]["typeProperties"]["activities"].pop(0))
    r = run(store, pid)
    c = check(r, "activity_graph.inventory", "pipeline:PL_Nested")[0]
    assert c.result is R.FAIL and any("missing: Inner_Wait" in e for e in c.evidence)
    assert {x.id: x for x in r.components}["pipeline:PL_Nested/activity:Inner_Wait"].status is S.missing
    assert r.overall == "FAILED" and not r.full_success_claimed


def test_an_extra_activity_is_reported(project):
    store, pid = project
    edit_target(store, pid, "PL_Child", lambda d: d["properties"]["activities"].append(
        {"name": "Sneaky", "type": "Wait", "dependsOn": [], "typeProperties": {"waitTimeInSeconds": 1}}))
    r = run(store, pid)
    assert results(r, "activity_graph.inventory", "pipeline:PL_Child") == {R.FAIL}
    assert {x.id: x for x in r.components}["pipeline:PL_Child/activity:Sneaky"].status is S.extra


def test_a_changed_dependency_fails(project):
    store, pid = project
    edit_target(store, pid, "PL_Main", lambda d: [a.update(dependsOn=[]) for a in d["properties"]["activities"] if a["name"] == "Run_Child"])
    c = check(run(store, pid), "activity_graph.dependencies", "pipeline:PL_Main")[0]
    assert c.result is R.FAIL and any("dependency lost: copy_customer -> run_child" in e.lower() for e in c.evidence)


def test_a_changed_dependency_condition_fails(project):
    store, pid = project
    edit_target(store, pid, "PL_Main", lambda d: [a["dependsOn"][0].update(dependencyConditions=["Failed"]) for a in d["properties"]["activities"] if a["name"] == "Run_Child"])
    assert results(run(store, pid), "activity_graph.dependencies", "pipeline:PL_Main") == {R.FAIL}


def test_a_dropped_expression_fails_and_a_rewritten_one_is_listed(project):
    store, pid = project
    edit_target(store, pid, "PL_Nested", lambda d: d["properties"]["activities"][0]["typeProperties"].pop("items"))
    c = check(run(store, pid), "expression_preservation.expressions", "pipeline:PL_Nested")[0]
    assert c.result is R.FAIL and "Loop" in c.evidence[0]


def test_global_parameters_normalise_to_pipeline_parameters(project):
    store, pid = project
    c = check(run(store, pid), "expression_preservation.expressions", "pipeline:PL_Nested")[0]
    assert c.result is R.PASS  # @pipeline().globalParameters.env == @pipeline().parameters.gp_env


def test_a_dropped_parameter_or_variable_fails(project):
    store, pid = project

    def drop(d):
        d["properties"]["parameters"].pop("mode")
        d["properties"]["variables"].pop("v")

    edit_target(store, pid, "PL_Nested", drop)
    c = check(run(store, pid), "expression_preservation.parameters_variables", "pipeline:PL_Nested")[0]
    assert c.result is R.FAIL and len(c.evidence) == 2


def test_an_undeclared_reference_in_the_target_fails(project):
    store, pid = project
    edit_target(store, pid, "PL_Nested", lambda d: acts(d) and d["properties"]["activities"][0]["typeProperties"].update(items={"value": "@variables('ghost')", "type": "Expression"}))
    c = check(run(store, pid), "reference_integrity.target_internal", "pipeline:PL_Nested")[0]
    assert c.result is R.FAIL and any("ghost" in e for e in c.evidence)


def test_a_policy_or_loop_setting_change_fails(project):
    store, pid = project

    def tweak(d):
        for a in d["properties"]["activities"]:
            if a["type"] == "ForEach":
                a["typeProperties"]["isSequential"] = False
            if a["type"] == "Lookup":
                a.setdefault("policy", {})["retry"] = 7

    edit_target(store, pid, "PL_Nested", tweak)
    c = check(run(store, pid), "execution_semantics.static", "pipeline:PL_Nested")[0]
    assert c.result is R.FAIL and any("isSequential" in e for e in c.evidence)


def test_a_dropped_dataset_settings_fails(project):
    store, pid = project
    edit_target(store, pid, "PL_Nested", lambda d: [a["typeProperties"].pop("datasetSettings", None) for a in d["properties"]["activities"] if a["type"] == "Lookup"])
    r = run(store, pid)
    assert results(r, "connections_secrets.dataset", None) >= {R.FAIL} or results(r, "execution_semantics.static", "pipeline:PL_Nested") == {R.FAIL}


def test_a_schema_violation_fails(project):
    store, pid = project
    edit_target(store, pid, "PL_Child", lambda d: d["properties"]["activities"][0].update(type="NotAFabricType"))
    r = run(store, pid)
    assert results(r, "target_schema.pipeline", "pipeline:PL_Child") == {R.FAIL}


def test_an_unreadable_generated_file_fails(project):
    store, pid = project
    (store.conversion_dir(pid) / "pipelines" / "PL_Child.DataPipeline" / "pipeline-content.json").write_text("{ broken", encoding="utf-8")
    assert results(run(store, pid), "target_schema.pipeline", "pipeline:PL_Child") == {R.FAIL}


def test_a_missing_generated_pipeline_fails_and_is_listed_as_missing(project):
    store, pid = project
    shutil.rmtree(store.conversion_dir(pid) / "pipelines" / "PL_Child.DataPipeline")
    r = run(store, pid)
    assert results(r, "target_schema.generated") == {R.FAIL}
    assert {x.id: x for x in r.components}["pipeline:PL_Child"].status is S.missing and r.overall == "FAILED"


def test_a_stale_generated_pipeline_with_no_source_is_extra(project):
    store, pid = project
    (store.source_dir(pid) / "pipeline" / "PL_Child.json").unlink()
    r = run(store, pid)
    assert {x.id: x for x in r.components}["pipeline:PL_Child"].status is S.extra


def test_a_plaintext_secret_in_a_generated_file_fails_without_printing_it(project):
    store, pid = project
    secret = "SUPER-SECRET-VALUE-0123456789"
    edit_target(store, pid, "PL_Main", lambda d: d["properties"]["activities"][0]["typeProperties"].update(password=secret, note="AccountKey=" + secret))
    r = run(store, pid)
    c = check(r, "connections_secrets.plaintext_secrets")[0]
    assert c.result is R.FAIL and c.evidence and r.overall == "FAILED"
    blob = r.model_dump_json() + render(r)
    assert secret not in blob


def test_an_unbound_connection_blocks_instead_of_passing(baseline, tmp_path):
    store, pid = baseline
    from adf2fabric.project.store import ProjectStore

    dest = tmp_path / "projects"
    shutil.copytree(store.projects_root, dest)
    s2 = ProjectStore(dest)
    ConversionAgent(s2).run(ConversionRequest(project_name=pid))  # no resolutions: every connection and item is a token
    r = run(s2, pid)
    assert results(r, "reference_integrity.target_unbound") == {R.BLOCKED}
    assert R.BLOCKED in results(r, "connections_secrets.linked_service")
    assert r.overall == "BLOCKED" and not r.full_success_claimed
    assert any(c.remediation for c in r.checks if c.result is R.BLOCKED)


def test_a_trigger_without_a_schedule_or_enabled_schedule_fails(project):
    store, pid = project
    f = next((store.conversion_dir(pid) / "schedules").glob("*.json"))
    d = json.loads(f.read_text(encoding="utf-8"))
    d["schedules"][0]["enabled"] = False  # the source trigger was Started: a disabled flag contradicts it
    f.write_text(json.dumps(d), encoding="utf-8")
    assert results(run(store, pid), "target_schema.schedule") == {R.FAIL}
    f.unlink()
    r = run(store, pid)
    assert results(r, "execution_semantics.trigger", "trigger:TR_Daily") == {R.FAIL}
    assert {x.id: x for x in r.components}["trigger:TR_Daily"].status is S.missing


def test_a_non_schedule_trigger_is_blocked_not_silently_passed(project):
    store, pid = project
    t = json.loads((store.source_dir(pid) / "trigger" / "TR_Daily.json").read_text(encoding="utf-8"))
    t["properties"]["type"] = "BlobEventsTrigger"
    (store.source_dir(pid) / "trigger" / "TR_Daily.json").write_text(json.dumps(t), encoding="utf-8")
    r = run(store, pid)
    assert results(r, "execution_semantics.trigger", "trigger:TR_Daily") == {R.BLOCKED}


# =====================================================================================================================
# source inventory
# =====================================================================================================================


def test_unreadable_source_json_fails_and_is_unverified(project):
    store, pid = project
    (store.source_dir(pid) / "dataset" / "DS_SQL.json").write_text("{ nope", encoding="utf-8")
    r = run(store, pid)
    assert results(r, "source_inventory.readable") == {R.FAIL}
    assert {x.id: x for x in r.components}["dataset:DS_SQL"].status is S.unverified


def test_a_missing_referenced_definition_in_the_source_fails(project):
    store, pid = project
    (store.source_dir(pid) / "dataset" / "DS_LAKE.json").unlink()
    c = check(run(store, pid), "reference_integrity.source")[0]
    assert c.result is R.FAIL and any("DS_LAKE" in e for e in c.evidence)


def _write_manifest(store, pid, complete=True, failures=(), drop=(), add=()):
    import yaml
    from adf2fabric.project.source import RetrievalFailure, SourceArtifact, SourceManifest, SourceType

    root = views.scan_source(store.source_dir(pid))
    arts = [SourceArtifact(kind=f.kind, name=f.name, path=f.path, sha256="0" * 64) for f in root.files if f.path not in drop]
    arts += [SourceArtifact(kind="dataset", name=Path(p).stem, path=p, sha256="0" * 64) for p in add]
    m = SourceManifest(project_id=pid, source_type=SourceType.github, source_identifier="x", version="abc", content_sha256="0" * 64, retrieved_at=NOW,
                       artifacts=arts, failures=[RetrievalFailure(**f) for f in failures], complete=complete)
    (store.project_dir(pid) / "source-manifest.yaml").write_text(yaml.safe_dump(json.loads(m.model_dump_json())), encoding="utf-8")


def test_a_consistent_manifest_passes_and_a_mismatch_fails(project):
    store, pid = project
    _write_manifest(store, pid)
    assert results(run(store, pid), "source_inventory.manifest") == {R.PASS}
    _write_manifest(store, pid, drop=("dataset/DS_SQL.json",), add=("dataset/Ghost.json",))
    c = check(run(store, pid), "source_inventory.manifest")[0]
    assert c.result is R.FAIL and any("unlisted: dataset/DS_SQL.json" in e for e in c.evidence) and any("absent: dataset/Ghost.json" in e for e in c.evidence)


def test_an_incomplete_source_blocks_rather_than_fails(project):
    store, pid = project
    _write_manifest(store, pid, complete=False, failures=[{"kind": "dataset", "name": "DS_X", "status_code": 403, "error": "denied", "action": "grant"}])
    (store.source_dir(pid) / "dataset" / "DS_LAKE.json").unlink()
    r = run(store, pid)
    assert results(r, "source_inventory.manifest") == {R.BLOCKED} and results(r, "reference_integrity.source") == {R.BLOCKED}


def test_no_conversion_yet_is_blocked_not_failed(tmp_path):
    store, pid = _make_project(tmp_path, FILES)
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    r = run(store, pid)
    assert results(r, "target_schema.generated") == {R.BLOCKED} and r.overall == "BLOCKED"
    assert all(x.status in (S.unverified, S.not_applicable) for x in r.components)


def test_discovery_counts_are_cross_checked_independently(project):
    store, pid = project
    r = run(store, pid)
    assert results(r, "source_inventory.discovery") == {R.PASS}


# =====================================================================================================================
# diffs and redaction
# =====================================================================================================================


def test_diffs_are_normalised_and_redacted():
    a = {"b": 1, "a": {"password": "hunter2-very-secret", "n": 1}, "tok": "x"}
    b = {"a": {"n": 2, "password": "another-very-secret"}, "b": 1, "tok": "x"}
    lines = diffing.diff(a, b)
    assert lines == ["~ a.n: 1 -> 2", "~ a.password: [redacted] -> [redacted]"]
    assert "hunter2" not in "".join(lines)
    assert diffing.diff({"x": 1, "y": 2}, {"y": 2, "x": 1}) == []  # key order is not a difference


def test_redaction_catches_credential_shapes_under_innocent_keys():
    assert diffing.redact_value("note", "Server=x;AccountKey=abc123abc123abc123;") == "Server=x;[redacted];"
    assert diffing.redact_value("auth", "Bearer abc.def.ghi") == "[redacted]"
    assert diffing.redact_value("secretName", "my-secret") == "my-secret"  # a reference name, not a value
    assert diffing.redact_value("ref", "@pipeline().parameters.password") == "@pipeline().parameters.password"


def test_secret_detection_ignores_references_and_expressions():
    assert diffing.has_plaintext_secret({"password": {"type": "AzureKeyVaultSecret", "secretName": "kv-pw"}}) == []
    assert diffing.has_plaintext_secret({"password": "@pipeline().parameters.pw"}) == []
    assert diffing.has_plaintext_secret({"connection": "11111111-1111-1111-1111-111111111111"}) == []
    assert diffing.has_plaintext_secret({"accountKey": "abcdefabcdef"}) == ["accountKey"]


def test_an_adf_encrypted_credential_is_not_a_plaintext_secret_but_stays_redacted():
    blob = "ew0KICAiVmVyc2lvbiI6ICIyMDE3LTExLTMwIiwNCiAgIlByb3RlY3Rpb25Nb2RlIjogIktleSIsDQo="  # how ADF writes it into Git
    assert diffing.has_plaintext_secret({"typeProperties": {"url": "https://a.dfs.core.windows.net", "encryptedCredential": blob}}) == []
    assert diffing.has_plaintext_secret({"typeProperties": {"encryptedCredential": blob, "accountKey": "abcdefabcdef"}}) == ["typeProperties.accountKey"]
    assert blob not in diffing.redact_value("encryptedCredential", blob)  # never shown in a diff or report


def test_api_and_deployment_are_not_run_without_an_authorised_workspace(project):
    store, pid = project
    r = run(store, pid, stages=("api", "deployment"))
    assert {c.result for c in r.checks if c.stage in (Stage.api, Stage.deployment)} == {R.NOT_RUN}
    assert r.overall == "NOT_VALIDATED"


# =====================================================================================================================
# runtime: only recorded real runs count
# =====================================================================================================================


def record(store, pid, pipeline, **entry):
    from adf2fabric.agent.deployment import verify as v

    path = v.record_path(store.project_dir(pid))
    rec = v.load_record(path)
    rec[pipeline] = {"pipeline": pipeline, "verdict": "matches", "fabric_status": "Completed", "job_id": "job-1", "baseline": "adf", "matched": 3,
                     "ran_at": "2999-01-01T00:00:00+00:00", "differences": [], "detail": "", **entry}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rec), encoding="utf-8")


def rt(store, pid):
    return run(store, pid, stages=("static", "runtime"))


def test_runtime_is_not_run_without_a_record_or_without_asking(project):
    store, pid = project
    assert results(run(store, pid, stages=("static",)), "runtime.execution") == {R.NOT_RUN}
    r = rt(store, pid)
    assert results(r, "runtime.execution") == {R.NOT_RUN} and r.overall == "NOT_VALIDATED"


def test_a_matching_recorded_run_passes_that_pipeline_only(project):
    store, pid = project
    record(store, pid, "PL_Main")
    r = rt(store, pid)
    assert results(r, "runtime.execution", "pipeline:PL_Main") == {R.PASS}
    assert results(r, "runtime.execution", "pipeline:PL_Child") == {R.NOT_RUN}
    assert r.overall == "NOT_VALIDATED"  # the others never ran


def test_a_run_in_another_workspace_says_nothing_about_the_one_validated(project):
    store, pid = project
    record(store, pid, "PL_Main", workspace="99999999-0000-0000-0000-000000000000")
    r = engine.validate(store, pid, stages={"static", "runtime"}, workspace=WS, now=NOW)
    c = check(r, "runtime.execution", "pipeline:PL_Main")[0]
    assert c.result is R.NOT_RUN and "another Fabric workspace" in c.summary
    record(store, pid, "PL_Main", workspace=WS.upper())  # the same workspace, however it is written
    r = engine.validate(store, pid, stages={"static", "runtime"}, workspace=WS, now=NOW)
    assert results(r, "runtime.execution", "pipeline:PL_Main") == {R.PASS}


def test_a_differing_or_failed_run_fails(project):
    store, pid = project
    record(store, pid, "PL_Main", verdict="differs", differences=[{"activity": "Copy_Customer", "what": "rowsCopied", "fabric": 1, "baseline": 2}])
    record(store, pid, "PL_Child", fabric_status="Failed", verdict="matches", detail="boom")
    r = rt(store, pid)
    assert results(r, "runtime.execution", "pipeline:PL_Main") == {R.FAIL} and results(r, "runtime.execution", "pipeline:PL_Child") == {R.FAIL}
    assert r.overall == "FAILED"


def test_a_partial_comparison_is_blocked_not_passed(project):
    store, pid = project
    record(store, pid, "PL_Main", verdict="partial", detail="not every activity could be compared")
    assert results(rt(store, pid), "runtime.execution", "pipeline:PL_Main") == {R.BLOCKED}


def test_a_run_older_than_the_generated_file_does_not_vouch_for_it(project):
    store, pid = project
    record(store, pid, "PL_Main", ran_at="2000-01-01T00:00:00+00:00")
    c = check(rt(store, pid), "runtime.execution", "pipeline:PL_Main")[0]
    assert c.result is R.BLOCKED and "older than" in c.summary


def test_a_run_pinned_to_the_file_still_counts_when_the_same_content_is_saved_again(project):
    """An answer changed and changed back converts to the same bytes again: the save time moves, the content does not."""
    from adf2fabric.agent.deployment import verify as v

    store, pid = project
    art = v.artifact_file(store.project_dir(pid), "PL_Main")
    record(store, pid, "PL_Main", ran_at="2000-01-01T00:00:00+00:00", artifact_sha=v.file_sha(art))
    assert results(rt(store, pid), "runtime.execution", "pipeline:PL_Main") == {R.PASS}
    edit_target(store, pid, "PL_Main", lambda d: d["properties"].update(description="changed"))
    c = check(rt(store, pid), "runtime.execution", "pipeline:PL_Main")[0]
    assert c.result is R.BLOCKED and "older than" in c.summary


# =====================================================================================================================
# the overall decision and the report
# =====================================================================================================================


def test_overall_is_never_better_than_the_worst_required_result():
    from adf2fabric.project.validation import Category as K, CheckResult

    def ck(result, stage=Stage.static, required=True):
        return CheckResult(id="x.y", category=K.target_schema, stage=stage, result=result, summary="s", required=required)

    allstages = {"static", "api", "deployment", "runtime"}
    ok = [ck(R.PASS), ck(R.PASS, Stage.runtime)]
    assert engine.overall(ok, [], allstages)[0] == "MIGRATION_VERIFIED"
    assert engine.overall(ok + [ck(R.NOT_RUN)], [], allstages)[0] == "NOT_VALIDATED"
    assert engine.overall(ok + [ck(R.BLOCKED)], [], allstages)[0] == "BLOCKED"
    assert engine.overall(ok + [ck(R.FAIL)], [], allstages)[0] == "FAILED"
    assert engine.overall(ok + [ck(R.FAIL, required=False)], [], allstages)[0] == "MIGRATION_VERIFIED"  # informational checks never decide
    assert engine.overall([ck(R.PASS)], [], {"static"})[0] == "NOT_VALIDATED"  # no runtime evidence
    assert engine.overall([], [], allstages)[0] == "NOT_VALIDATED"  # nothing validated is not success


def test_a_missing_or_unverified_component_blocks_a_success_claim():
    from adf2fabric.project.validation import Category as K, CheckResult, ComponentComparison

    ok = [CheckResult(id="a.b", category=K.runtime, stage=Stage.runtime, result=R.PASS, summary="s")]
    comp = lambda st: [ComponentComparison(id="pipeline:P", kind="pipeline", status=st)]  # noqa: E731
    allstages = {"static", "api", "deployment", "runtime"}
    assert engine.overall(ok, comp(S.missing), allstages)[0] == "FAILED"
    assert engine.overall(ok, comp(S.unverified), allstages)[0] == "NOT_VALIDATED"
    assert engine.overall(ok, comp(S.altered), allstages)[0] == "NOT_VALIDATED"
    assert engine.overall(ok, comp(S.transformed), allstages)[0] == "MIGRATION_VERIFIED"


def test_the_report_is_honest_and_complete(project):
    store, pid = project
    r = run(store, pid)
    text = render(r)
    assert "NOT a claim of success" in text and "Overall: NOT_VALIDATED" in text
    for section in ("## Stages", "## Inventory comparison", "## Checks", "### static", "### runtime"):
        assert section in text
    assert "MIGRATION VERIFIED" not in text


# =====================================================================================================================
# a dataset repointed to the lakehouse (convert --lakehouse) carries the lakehouse as its connection, not an externalReferences.connection
# =====================================================================================================================


def _repoint_sink(artifact):
    def edit(data):
        copy = next(a for a in acts(data) if a.name == "Copy_Customer").raw
        ds = copy["typeProperties"]["sink"]["datasetSettings"]
        ds.pop("externalReferences", None)
        ds["linkedService"] = {"name": "lh_main", "properties": {"annotations": [], "type": "Lakehouse",
                                                                  "typeProperties": {"workspaceId": WS, "artifactId": artifact, "rootFolder": "Files"}}}
    return edit


def test_a_dataset_repointed_to_a_bound_lakehouse_is_not_a_missing_connection(project):
    store, pid = project
    edit_target(store, pid, "PL_Main", _repoint_sink("33333333-3333-3333-3333-333333333333"))
    r = run(store, pid)
    assert results(r, "connections_secrets.linked_service", "linked_service:LS_LAKE") == {R.PASS}  # it used to FAIL: 'no connection reference'
    assert not [c for c in r.checks if c.id == "connections_secrets.linked_service" and c.result is R.FAIL]


def test_a_dataset_repointed_to_an_unbound_lakehouse_blocks_rather_than_fails(project):
    store, pid = project
    edit_target(store, pid, "PL_Main", _repoint_sink("<item:lakehouse:lh_main>"))
    r = run(store, pid)
    assert results(r, "connections_secrets.linked_service", "linked_service:LS_LAKE") == {R.BLOCKED}
    assert results(r, "reference_integrity.target_unbound") == {R.BLOCKED}


def test_an_activity_whose_every_dataset_is_repointed_says_it_reads_through_the_lakehouse(project):
    store, pid = project

    def both(data):
        copy = next(a for a in acts(data) if a.name == "Copy_Customer").raw
        for side in ("source", "sink"):
            ds = copy["typeProperties"][side]["datasetSettings"]
            ds.pop("externalReferences", None)
            ds["linkedService"] = {"name": "lh_main", "properties": {"type": "Lakehouse", "typeProperties": {"workspaceId": WS, "artifactId": "33333333-3333-3333-3333-333333333333"}}}
    edit_target(store, pid, "PL_Main", both)
    r = run(store, pid)
    rows = check(r, "connections_secrets.linked_service", "linked_service:LS_LAKE")
    assert {c.result for c in rows} == {R.PASS} and "lakehouse shortcut" in " ".join(c.summary for c in rows)


@pytest.fixture(scope="module")
def dynamic_folder(tmp_path_factory):
    """A Copy whose sink folder is a pipeline parameter passed to the dataset: the case whose expression convert --lakehouse rewrites."""
    tmp = tmp_path_factory.mktemp("val_dyn")
    files = {**FILES, "pipeline/PL_Dyn.json": pipe(copy_activity("Copy_Dyn", outputs=[ref("Dataset", "DS_LAKE", folder=expr("@pipeline().parameters.dst"))]),
                                                    name="PL_Dyn", parameters=["dst"])}
    store, pid = _make_project(tmp, files)
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    res = tmp / "res.json"
    res.write_text(json.dumps(RESOLUTIONS), encoding="utf-8")
    assert ConversionAgent(store).run(ConversionRequest(project_name="test-proj", resolutions_path=str(res))).success
    return store, pid


def _sink_folder(location_type, folder):
    def edit(data):
        copy = next(a for a in acts(data) if a.name == "Copy_Dyn").raw
        copy["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["location"] = {"type": location_type, "folderPath": expr(folder),
                                                                                           "fileName": "data.parquet"}
    return edit


@pytest.mark.parametrize("location, folder, status", [
    ("LakehouseLocation", "@concat('raw/', pipeline().parameters.dst)", S.transformed),  # the shortcut in front: the repoint's own rewrite
    ("LakehouseLocation", "@concat('raw/', toUpper(pipeline().parameters.dst))", S.altered),  # the expression itself changed
    ("AzureBlobFSLocation", "@concat('raw/', pipeline().parameters.dst)", S.altered),  # not a Lakehouse folder: no repoint explains it
])
def test_a_lakehouse_folder_with_the_shortcut_in_front_is_a_transformation_not_an_alteration(dynamic_folder, tmp_path, location, folder, status):
    from adf2fabric.project.store import ProjectStore

    base, pid = dynamic_folder
    shutil.copytree(base.projects_root, tmp_path / "projects")
    store = ProjectStore(tmp_path / "projects")
    assert {x.id: x for x in run(store, pid).components}["pipeline:PL_Dyn"].status is S.matched  # before the edit the expression is carried verbatim
    edit_target(store, pid, "PL_Dyn", _sink_folder(location, folder))
    r = run(store, pid)
    assert {x.id: x for x in r.components}["pipeline:PL_Dyn/activity:Copy_Dyn"].status is status
    assert results(r, "expression_preservation.expressions", "pipeline:PL_Dyn") == {R.PASS}


def test_a_run_with_no_baseline_passes_only_when_its_output_matched_the_source(project):
    from adf2fabric.agent.deployment import output_check as oc
    from adf2fabric.agent.deployment import verify as v

    store, pid = project
    sha = v.file_sha(v.artifact_file(store.project_dir(pid), "PL_Main"))
    record(store, pid, "PL_Main", verdict="no_baseline", baseline="", ran_at="2026-01-01T00:00:00+00:00", artifact_sha=sha)
    assert results(rt(store, pid), "runtime.execution", "pipeline:PL_Main") == {R.BLOCKED}
    chk = {"name": "out", "pipeline": "PL_Main", "verdict": oc.COUNTS_MATCH, "detail": "content hash differs", "checked_at": "2026-02-01T00:00:00+00:00"}
    oc.save(store.project_dir(pid), chk)
    c = check(rt(store, pid), "runtime.execution", "pipeline:PL_Main")[0]
    assert c.result is R.BLOCKED and any("content hash differs" in e for e in c.evidence)  # a changed value does not vouch
    oc.save(store.project_dir(pid), {**chk, "verdict": oc.MATCHES})
    c = check(rt(store, pid), "runtime.execution", "pipeline:PL_Main")[0]
    assert c.result is R.PASS and "no earlier run" in c.summary and "Only the checked outputs" in c.summary
    oc.save(store.project_dir(pid), {**chk, "verdict": oc.MATCHES, "checked_at": "2025-01-01T00:00:00+00:00"})  # made before the run
    assert results(rt(store, pid), "runtime.execution", "pipeline:PL_Main") == {R.BLOCKED}


def _child_id(value):
    def fn(d):
        a = next(x for x in acts(d) if x.name == "Run_Child")
        a.raw["typeProperties"]["pipelineId"] = value
    return fn


def test_a_child_pipeline_of_this_conversion_is_filled_in_at_deploy_not_unbound(project):
    # found live (2026-11-10): validation BLOCKED a project whose deployed parent already called the real child, because the generated
    # file keeps the child's placeholder for deploy to fill in
    store, pid = project
    edit_target(store, pid, "PL_Main", _child_id("<item:pipeline:PL_Child>"))
    r = run(store, pid)
    assert results(r, "reference_integrity.target_unbound") == {R.PASS}
    assert results(r, "reference_integrity.child_pipelines") == {R.PASS}


def test_a_child_pipeline_that_was_not_generated_still_blocks(project):
    store, pid = project
    edit_target(store, pid, "PL_Main", _child_id("<item:pipeline:PL_Elsewhere>"))
    assert results(run(store, pid), "reference_integrity.target_unbound") == {R.BLOCKED}


def test_the_validation_view_shows_only_the_stages_chosen_with_every_check_they_ran(project):
    # each stage's checks (passing ones too) are listed; the overall verdict is unchanged
    from adf2fabric.api.views import validation_summary

    store, pid = project
    store.save_validation_report(pid, run(store, pid, stages=("static",)))
    v = validation_summary(store, pid)
    assert v["stages_selected"] == ["static"] and list(v["checks_by_stage"]) == ["static"]
    rows = v["checks_by_stage"]["static"]
    assert any(c["result"] == "PASS" for c in rows) and all("stage" not in c for c in rows)
    assert [c["result"] for c in rows] == sorted((c["result"] for c in rows), key=["FAIL", "BLOCKED", "NOT_RUN", "PASS"].index)
    assert v["overall"] == "NOT_VALIDATED"  # api, deployment and runtime were not chosen: the engine's verdict stays honest


def test_the_validate_step_records_static_and_runtime_and_says_where_the_deployment_check_is(project):
    # 2026-11-11: Validate no longer reads Fabric. Without a read-back from the Deploy step the record says so in plain words (not
    # "stages not requested", not a red "not deployed"), and leaves the api and deployment stages out instead of failing them
    from adf2fabric.api.views import validation_summary

    store, pid = project
    rep = engine.record(store, pid, workspace=WS, now=NOW)
    assert rep.scope["stages_requested"] == "runtime,static"
    assert not [c for c in rep.checks if c.stage.value in ("api", "deployment")]
    assert any("read back on the Deploy step" in r for r in rep.overall_reasons)
    assert not any(r.startswith("Stages not requested") for r in rep.overall_reasons)
    assert rep.overall != "MIGRATION_VERIFIED"
    v = validation_summary(store, pid, workspace=WS)
    assert v["stages_selected"] == ["static", "runtime"] and v["readback"]["current"] is False and v["readback"]["items"] == []


def test_a_read_back_counts_only_while_it_still_describes_the_workspace(project):
    from adf2fabric.agent.deployment import record as deploy_record

    store, pid = project
    at = datetime(2030, 1, 1, tzinfo=timezone.utc)
    scope = {"readback_at": at.isoformat(), "readback_workspace": WS, "readback_conversion": engine.conversion_fingerprint(store, pid)}
    assert engine.readback_current(store, pid, scope, WS) is None
    assert "another workspace" in engine.readback_current(store, pid, scope, "99999999-9999-9999-9999-999999999999")
    deploy_record.note_apply(store.project_dir(pid), WS, [], now=at.timestamp() + 60)  # a deploy after the read-back
    assert "deploy ran after" in engine.readback_current(store, pid, scope, WS)
    edit_target(store, pid, "PL_Child", lambda d: d.setdefault("properties", {}).setdefault("description", "changed"))
    assert "converted again" in engine.readback_current(store, pid, scope, WS)
