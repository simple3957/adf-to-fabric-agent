"""Fabric content validation, the Conversion agent end to end, and the CLI (offline)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from adf2fabric import cli as cli_mod
from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.conversion.context import Resolutions
from adf2fabric.agent.conversion.validation import validate_content
from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from adf2fabric.project.conversion import ConversionOutcome, ConversionStatus, ProjectConversion
from tests.conversion_support import DS_LAKE, DS_SQL, LAKE_LS, SQL_LS, act, copy_activity, dep, expr, pipe, ref
from tests.test_adf_semantic_discovery import _make_project

GUID = "11111111-1111-1111-1111-111111111111"


# =============================================================================
# Validation of generated Fabric content
# =============================================================================


def wait(name, **extra):
    return {"name": name, "type": "Wait", "dependsOn": [], "typeProperties": {"waitTimeInSeconds": 1}, **extra}


def content(*acts, **props):
    return {"properties": {"activities": list(acts), **props}}


def codes_of(v, severity="error"):
    return {i.code for _, i in v.issues if i.severity.value == severity}


def test_a_minimal_valid_pipeline_validates():
    v = validate_content(content(wait("A"), wait("B", dependsOn=[{"activity": "A", "dependencyConditions": ["Succeeded"]}])))
    assert v.issues == []


@pytest.mark.parametrize("bad,code", [
    ({"name": "X", "type": "NotAFabricType", "dependsOn": [], "typeProperties": {}}, "unknown_fabric_type"),
    ({"name": "X", "type": "Wait", "dependsOn": [], "typeProperties": {}}, "missing_required_property"),
    ({"name": "X", "type": "WebActivity", "dependsOn": [],
      "typeProperties": {"relativeUrl": "/", "method": "GET"}}, "missing_envelope_reference"),
    ({"name": "X", "type": "WebActivity", "dependsOn": [], "externalReferences": {"connection": GUID},
      "typeProperties": {"relativeUrl": "/", "method": "TRACE"}}, "invalid_http_method"),
    ({**wait("X"), "state": "Disabled"}, "invalid_state"),
    ({**wait("X"), "onInactiveMarkAs": "Maybe"}, "invalid_on_inactive_mark_as"),
    ({**wait("X"), "policy": {"retryIntervalInSeconds": 5}}, "retry_interval_out_of_range"),
    ({**wait("X"), "policy": {"retryIntervalInSeconds": 90000}}, "retry_interval_out_of_range"),
    ({**wait("X"), "policy": {"bogus": 1}}, "undocumented_policy_key"),
    ({**wait("X"), "dependsOn": [{"activity": "X", "dependencyConditions": ["Succeeded"]}]}, "self_dependency"),
    ({**wait("X"), "dependsOn": [{"activity": "Nope", "dependencyConditions": ["Succeeded"]}]}, "dependency_not_in_scope"),
    ({**wait("X"), "dependsOn": [{"activity": "X2", "dependencyConditions": ["Sometimes"]}]}, "invalid_dependency_conditions"),
    ({"name": "X", "type": "SetVariable", "dependsOn": [], "typeProperties": {"variableName": "ghost", "value": 1}},
     "undeclared_variable"),
    ({"name": "X", "type": "Switch", "dependsOn": [], "typeProperties": {"on": "a", "cases": [{"value": 1, "activities": []}]}},
     "invalid_switch_case"),
])
def test_structural_errors_are_detected(bad, code):
    v = validate_content(content(bad, wait("X2")))
    assert code in codes_of(v)


def test_dependency_cycles_and_duplicate_names_are_detected():
    a = wait("A", dependsOn=[{"activity": "B", "dependencyConditions": ["Succeeded"]}])
    b = wait("B", dependsOn=[{"activity": "A", "dependencyConditions": ["Succeeded"]}])
    assert "dependency_cycle" in codes_of(validate_content(content(a, b)))
    assert "duplicate_activity_name" in codes_of(validate_content(content(wait("A"), wait("a"))))


def test_dependencies_cannot_cross_container_boundaries():
    inner = wait("In", dependsOn=[{"activity": "Out", "dependencyConditions": ["Succeeded"]}])
    box = {"name": "Box", "type": "ForEach", "dependsOn": [], "typeProperties": {
        "items": {"value": "@createArray(1)", "type": "Expression"}, "activities": [inner]}}
    assert "dependency_not_in_scope" in codes_of(validate_content(content(wait("Out"), box)))


def test_activity_limit_counts_nested_activities():
    inner = [wait(f"I{i}") for i in range(60)]
    boxes = [{"name": f"B{j}", "type": "ForEach", "dependsOn": [], "typeProperties": {
        "items": {"value": "@createArray(1)", "type": "Expression"}, "activities": [dict(a, name=f"{a['name']}_{j}") for a in inner]}}
        for j in range(2)]
    assert "activity_limit_exceeded" in codes_of(validate_content(content(*boxes)))  # 2 + 120 > 120


def test_expressions_are_checked_for_syntax_functions_and_references():
    def w(value):
        return {"name": "W", "type": "Wait", "dependsOn": [], "typeProperties": {
            "waitTimeInSeconds": {"value": value, "type": "Expression"}}}

    base = dict(parameters={"p": {"type": "Int"}}, variables={"v": {"type": "String"}})
    ok = validate_content(content(w("@add(pipeline().parameters.p, length(variables('v')))"), **base))
    assert ok.issues == []
    cases = {
        "@concat(": "expression_syntax", "@decimal('1')": "unsupported_function",
        "@pipeline().globalParameters.g": "unsupported_reference", "@dataset().x": "unsupported_function",
        "@pipeline().TriggerType": "unsupported_system_variable", "@pipeline().parameters.nope": "undeclared_parameter",
        "@variables('nope')": "undeclared_variable", "@activity('Ghost').output.x": "unknown_activity_reference",
    }
    for raw, code in cases.items():
        assert code in codes_of(validate_content(content(w(raw), **base))), raw


def test_unbound_references_are_runtime_prerequisites_not_structural_errors():
    c = {"name": "L", "type": "Lookup", "dependsOn": [], "typeProperties": {
        "source": {"type": "AzureSqlSource"},
        "datasetSettings": {"type": "AzureSqlTable", "externalReferences": {"connection": "<connection:LS_SQL>"}}}}
    v = validate_content(content(c))
    assert v.errors == [] and v.unbound == {"L": {"<connection:LS_SQL>"}}
    c["typeProperties"]["datasetSettings"]["externalReferences"]["connection"] = GUID
    assert validate_content(content(c)).unbound == {}


def test_undocumented_properties_are_warnings_not_silent():
    w = {"name": "W", "type": "Wait", "dependsOn": [], "typeProperties": {"waitTimeInSeconds": 1, "extra": 1}, "weird": 1}
    v = validate_content(content(w))
    assert codes_of(v, "warning") >= {"undocumented_property", "undocumented_envelope_key"}


# =============================================================================
# Resolutions
# =============================================================================


def test_resolutions_accept_documented_and_extended_types_and_reject_bad_guids(tmp_path):
    f = tmp_path / "r.json"
    f.write_text(json.dumps([
        {"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": GUID},
        {"type": "ItemToId", "key": "pipeline:Child", "value": GUID},
        {"type": "WorkspaceId", "key": "*", "value": GUID},
        {"type": "LinkedServiceToConnectionId", "key": "LS_BAD", "value": "not-a-guid"},
        {"type": "Mystery", "key": "k", "value": GUID},
        {"key": "missing-type"},
    ]), encoding="utf-8")
    r = Resolutions.load(f)
    assert r.connections == {"ls_sql": GUID} and r.items == {"pipeline:child": GUID} and r.workspace == GUID
    assert len(r.problems) == 3 and any("not a GUID" in p for p in r.problems)
    assert Resolutions.load(tmp_path / "missing.json").problems


# =============================================================================
# The agent, end to end
# =============================================================================

FILES = {
    "pipeline/PL_Main.json": pipe(
        copy_activity("Copy_Customer"),
        act("Log", "SqlServerStoredProcedure", {"storedProcedureName": "dbo.log"}, deps=[dep("Copy_Customer")],
            linkedServiceName=ref("LinkedService", "LS_SQL")),
        act("Run_Child", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Child"), "waitOnCompletion": True},
            deps=[dep("Log")]), name="PL_Main"),
    "pipeline/PL_Child.json": pipe(act("Pause", "Wait", {"waitTimeInSeconds": 3}), name="PL_Child"),
    "dataset/DS_SQL.json": DS_SQL, "dataset/DS_LAKE.json": DS_LAKE,
    "linkedService/LS_SQL.json": SQL_LS, "linkedService/LS_LAKE.json": LAKE_LS,
}


def fingerprint(root: Path) -> dict:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file() and ".git" not in p.parts}


@pytest.fixture
def project(tmp_path):
    store, pid = _make_project(tmp_path, FILES)
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    return store, pid


def run(store, **kw):
    return ConversionAgent(store).run(ConversionRequest(project_name="test-proj", **kw))


def test_agent_converts_writes_artifacts_and_leaves_the_source_untouched(project):
    store, pid = project
    before = fingerprint(store.source_dir(pid))
    discovery_before = {p.name: p.read_bytes() for p in store.discovery_dir(pid).iterdir()}

    result = run(store)
    assert result.success, result.error
    conv = result.conversion
    assert conv.status is ConversionOutcome.converted and conv.pipeline_count == 2 and conv.activity_count == 4

    base = store.conversion_dir(pid)
    for name in ("PL_Main", "PL_Child"):
        content = json.loads((base / "pipelines" / f"{name}.DataPipeline" / "pipeline-content.json").read_text("utf-8"))
        assert list(content) == ["properties"] and content["properties"]["activities"]
    assert (base / "conversion-report.md").read_text("utf-8").startswith("# ADF -> Fabric conversion report")
    template = json.loads((base / "resolutions.template.json").read_text("utf-8"))
    assert {"type": "WorkspaceId", "key": "*", "value": "<Fabric workspace GUID>"} in template
    assert any(e["type"] == "LinkedServiceToConnectionId" and e["key"] == "LS_SQL" for e in template)

    assert fingerprint(store.source_dir(pid)) == before  # the original ADF source is unchanged
    assert {p.name: p.read_bytes() for p in store.discovery_dir(pid).iterdir()} == discovery_before
    assert not (base / "source").exists()


def test_agent_record_round_trips_and_is_secret_free(project):
    store, pid = project
    result = run(store)
    loaded = store.load_conversion(pid)
    assert isinstance(loaded, ProjectConversion) and loaded.model_dump() == result.conversion.model_dump()
    text = store.conversion_path(pid).read_text("utf-8")
    assert yaml.safe_load(text)["schema_version"] == "adf-fabric-conversion/1"
    assert "password" not in text.lower() and "secret" not in text.lower().replace("secret_masked", "")


def test_agent_statuses_reflect_reality_not_json_generation(project):
    store, pid = project
    conv = run(store).conversion
    main = next(p for p in conv.pipelines if p.source_pipeline == "PL_Main")
    assert main.structural_valid and not main.runtime_ready  # unbound connections / items
    assert main.status is ConversionStatus.converted
    assert conv.status_counts["converted"] == 4
    assert {q.name for q in conv.connections} >= {"LS_SQL", "LS_LAKE", "invoke-pipeline"}
    assert {q.name for q in conv.workspace_items} >= {"workspace", "pipeline:PL_Child"}
    assert any("Not documented by Microsoft" in n for n in conv.schema_notes)


def test_agent_with_resolutions_becomes_runtime_ready(project, tmp_path):
    store, pid = project
    g = lambda n: f"{n:08d}-0000-0000-0000-000000000000"  # noqa: E731
    f = tmp_path / "res.json"
    f.write_text(json.dumps([
        {"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": g(1)},
        {"type": "LinkedServiceToConnectionId", "key": "LS_LAKE", "value": g(2)},
        {"type": "ItemToId", "key": "connection:invoke-pipeline", "value": g(3)},
        {"type": "ItemToId", "key": "pipeline:PL_Child", "value": g(4)},
        {"type": "WorkspaceId", "key": "*", "value": g(5)}]), encoding="utf-8")
    conv = run(store, resolutions_path=str(f)).conversion
    main = next(p for p in conv.pipelines if p.source_pipeline == "PL_Main")
    assert main.runtime_ready and main.structural_valid and all(q.bound for q in main.prerequisites)
    text = (store.conversion_dir(pid) / "pipelines" / "PL_Main.DataPipeline" / "pipeline-content.json").read_text("utf-8")
    assert g(4) in text and "<" not in text.replace("<=", "")


def test_agent_can_convert_a_selection(project):
    store, pid = project
    conv = run(store, pipelines=["pl_child"]).conversion
    assert [p.source_pipeline for p in conv.pipelines] == ["PL_Child"]
    assert not (store.conversion_dir(pid) / "pipelines" / "PL_Main.DataPipeline").exists()


def test_agent_reports_a_helpful_error_for_an_unknown_pipeline(project):
    store, _ = project
    r = run(store, pipelines=["Nope"])
    assert not r.success and "Nope" in r.error and "PL_Main" in r.error


def test_agent_is_idempotent_and_keeps_timestamps_stable_when_nothing_changed(project):
    store, pid = project
    first = run(store).conversion
    second = run(store).conversion
    assert second.created_at == first.created_at and second.updated_at == first.updated_at
    assert second.model_dump(exclude={"created_at", "updated_at"}) == first.model_dump(exclude={"created_at", "updated_at"})


def test_agent_requires_semantic_discovery(tmp_path):
    store, _ = _make_project(tmp_path, FILES)
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    assert not r.success and "Semantic Discovery has not run" in r.error


def test_agent_rejects_a_stale_semantic_record(project):
    store, pid = project
    d = store.load_adf_semantic_discovery(pid)
    d.source_commit_sha = "0" * 40
    store.save_adf_semantic_discovery(pid, d)
    r = run(store)
    assert not r.success and "stale" in r.error


def test_agent_reports_a_missing_project(tmp_path):
    from adf2fabric.project.store import ProjectStore

    r = ConversionAgent(ProjectStore(tmp_path / "p")).run(ConversionRequest(project_name="ghost"))
    assert not r.success and "ghost" in r.error


def test_agent_survives_a_partially_blocked_estate(tmp_path):
    files = dict(FILES)
    files["pipeline/PL_Child.json"] = pipe(act("Pkg", "ExecuteSSISPackage", {"packageLocation": {"type": "SSISDB"}}),
                                           name="PL_Child")
    store, pid = _make_project(tmp_path, files)
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    r = run(store)
    assert r.success and r.conversion.status is ConversionOutcome.partial
    assert r.conversion.status_counts["blocked"] == 1
    report = (store.conversion_dir(pid) / "conversion-report.md").read_text("utf-8")
    assert "BLOCKED" in report and "Alternative:" in report and "Invoke pipeline" in report


# =============================================================================
# CLI
# =============================================================================


def test_cli_convert_end_to_end(project, monkeypatch):
    store, pid = project
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    out = CliRunner().invoke(cli_mod.cli, ["convert", "--project", "test-proj"])
    assert out.exit_code == 0, out.output
    assert "ADF -> FABRIC CONVERSION" in out.output and "PL_Main: converted (valid; runtime ready: no)" in out.output
    assert "binding(s) still to supply" in out.output and "resolutions template" in out.output
    assert store.has_conversion(pid)


def test_cli_convert_fails_cleanly_without_discovery(tmp_path, monkeypatch):
    store, _ = _make_project(tmp_path, FILES)
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    out = CliRunner().invoke(cli_mod.cli, ["convert", "--project", "test-proj"])
    assert out.exit_code == 1 and "Semantic Discovery has not run" in out.output


def test_existing_stages_are_untouched_by_the_new_stage(project):
    store, pid = project
    sem_before = store.adf_semantic_discovery_path(pid).read_bytes()
    repo_before = store.repository_discovery_path(pid).read_bytes()
    run(store)
    assert store.adf_semantic_discovery_path(pid).read_bytes() == sem_before
    assert store.repository_discovery_path(pid).read_bytes() == repo_before
    assert DiscoveryAgent(store).discover_repository(DiscoveryRequest(project_name="test-proj")).success


def test_connector_coverage_says_which_linked_services_the_tool_can_create():
    from types import SimpleNamespace as NS

    from adf2fabric.agent.conversion.connectors import coverage

    got = {c.linked_service: (c.level, c.adf_type) for c in coverage([
        NS(name="Lake", type="AzureBlobFS"), NS(name="Sql", type="AzureSqlDW"), NS(name="Dbx", type="AzureDatabricks"),
        NS(name="Pg", type="PostgreSql"), NS(name="Gen1", type="AzureDataLakeStore"), NS(name="Odd", type="SomethingNew"),
        NS(name="None", type=None)])}
    assert got == {"Lake": ("created", "AzureBlobFS"), "Sql": ("created", "AzureSqlDW"), "Dbx": ("created", "AzureDatabricks"),
                   "Pg": ("manual", "PostgreSql"), "Gen1": ("unsupported", "AzureDataLakeStore"),
                   "Odd": ("unlisted", "SomethingNew"), "None": ("unlisted", None)}


def test_the_report_has_a_connector_coverage_section_with_counts(tmp_path):
    from adf2fabric.agent.conversion.report import render_report

    store, pid = _make_project(tmp_path, FILES)
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    assert r.success and r.conversion.connector_coverage
    text = render_report(r.conversion)
    assert "## Connector coverage" in text and "can be created in Fabric by this tool" in text
    assert text.index("## Connector coverage") < text.index("## Prerequisites (all pipelines)")
    assert all(x.level in ("created", "manual", "unsupported", "unlisted") for x in r.conversion.connector_coverage)


def test_a_path_too_long_for_windows_is_explained_not_a_raw_file_error(tmp_path, monkeypatch):
    from adf2fabric.project.store import ProjectPathError, ProjectStore

    store = ProjectStore(tmp_path / ("d" * 120) / ("e" * 120))
    real = type(tmp_path).write_text

    def boom(self, *a, **k):
        err = OSError(2, "No such file or directory")
        err.winerror = 3
        raise err

    monkeypatch.setattr(type(tmp_path), "write_text", boom)
    monkeypatch.setattr(type(tmp_path), "mkdir", lambda self, *a, **k: None)
    with pytest.raises(ProjectPathError) as exc:
        store.write_conversion_file("p", "pipelines/PL_X.DataPipeline/pipeline-content.json", "{}")
    assert "260" in str(exc.value) and "projects_root" in str(exc.value) and str(tmp_path) not in str(exc.value)
    monkeypatch.setattr(type(tmp_path), "write_text", real)


def test_an_unchanged_generated_file_is_not_rewritten_so_its_timestamp_still_means_something(tmp_path):
    import os

    from adf2fabric.project.store import ProjectStore

    store = ProjectStore(tmp_path)
    f = store.write_conversion_file("p", "pipelines/PL_X.DataPipeline/pipeline-content.json", '{"a": 1}\n')
    os.utime(f, (1_000_000_000, 1_000_000_000))
    store.write_conversion_file("p", "pipelines/PL_X.DataPipeline/pipeline-content.json", '{"a": 1}\n')
    assert f.stat().st_mtime == 1_000_000_000
    store.write_conversion_file("p", "pipelines/PL_X.DataPipeline/pipeline-content.json", '{"a": 2}\n')
    assert f.stat().st_mtime > 1_000_000_000 and '"a": 2' in f.read_text(encoding="utf-8")
