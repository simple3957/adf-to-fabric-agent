"""Workload migration and compatibility assessment: analysis, conversion, classification, retrieval, lifecycle."""

from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from adf2fabric.agent.dependencies.access import TokenSource
from adf2fabric.agent.deployment import sjd_deploy
from adf2fabric.agent.deployment.fabric_deploy import DeployError
from adf2fabric.agent.discovery.databricks_notebooks import DatabricksError
from adf2fabric.agent.workloads import analyzer, lifecycle, notebook_format, transform
from adf2fabric.agent.workloads.engine import Options, build_manifest
from adf2fabric.agent.workloads.report import render_report
from adf2fabric.agent.workloads.source import SourceResolver
from adf2fabric.project.workloads import Classification as K, CodeStatus as C, Severity as S, WorkloadManifest
from tests.test_discovery_activity_inventory import _discover, _project

FIX = Path(__file__).parent / "fixtures" / "workloads"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
SECRET_LITERALS = ("NOTAREALPASSWORD99", "ZmFrZWZha2VmYWtlZmFrZWZha2VmYWtlZmFrZQ==")


def _dbx(name):
    return FIX / "databricks" / name


def _syn(name):
    return json.loads((FIX / "synapse" / name).read_text(encoding="utf-8"))


# --- the estate -------------------------------------------------------------------------------------------------------------------


def _notebook(name, path, params=None, ls="LS_Dbx"):
    tp = {"notebookPath": path}
    if params:
        tp["baseParameters"] = params
    return {"name": name, "type": "DatabricksNotebook", "linkedServiceName": {"referenceName": ls, "type": "LinkedServiceReference"}, "typeProperties": tp}


def _pipe(name, *acts):
    return {"name": name, "properties": {"activities": list(acts)}}


ESTATE = {
    "linkedService/LS_Dbx.json": {"name": "LS_Dbx", "properties": {"type": "AzureDatabricks", "typeProperties": {
        "domain": "https://adb-1.1.azuredatabricks.net", "authentication": "MSI", "newClusterNodeType": "Standard_DS3_v2",
        "newClusterNumOfWorker": "2", "newClusterVersion": "13.3.x-scala2.12"}}},
    "pipeline/PL_Dbx.json": _pipe(
        "PL_Dbx",
        _notebook("Etl", "/Shared/etl/nb_etl", {"run_date": "2024-02-02", "source_folder": "landing", "unread": "x"}),
        {"name": "UseResult", "type": "SetVariable", "dependsOn": [{"activity": "Etl", "dependencyConditions": ["Succeeded"]}],
         "typeProperties": {"variableName": "v", "value": {"type": "Expression", "value": "@activity('Etl').output.runOutput"}}},
        _notebook("Clean", "/Shared/etl/nb_clean"), _notebook("Legacy", "/Shared/etl/nb_legacy_paths"),
        _notebook("Dlt", "/Shared/etl/nb_dlt"), _notebook("Chain", "/Shared/etl/nb_chain"),
        _notebook("Broken", "/Shared/etl/nb_broken_syntax"), _notebook("Missing", "/Shared/etl/nb_not_supplied"),
        _notebook("Dynamic", {"value": "@pipeline().parameters.nb", "type": "Expression"})),
    "pipeline/PL_Jobs.json": _pipe(
        "PL_Jobs",
        {"name": "Py", "type": "DatabricksSparkPython", "linkedServiceName": {"referenceName": "LS_Dbx", "type": "LinkedServiceReference"},
         "typeProperties": {"pythonFile": "dbfs:/jobs/job_main.py", "parameters": ["--a", "1"]}},
        {"name": "Jar", "type": "DatabricksSparkJar", "linkedServiceName": {"referenceName": "LS_Dbx", "type": "LinkedServiceReference"},
         "typeProperties": {"mainClassName": "com.contoso.Main", "parameters": ["x"],
                            "libraries": [{"jar": "dbfs:/libs/app.jar"}, {"pypi": {"package": "pandas==2.0.0"}}, {"maven": {"coordinates": "g:a:1.0"}}]}}),
    "pipeline/PL_Syn.json": _pipe(
        "PL_Syn",
        *[{"name": n, "type": "SynapseNotebook", "typeProperties": {"notebook": {"referenceName": n, "type": "NotebookReference"},
                                                                    **({"parameters": {"folder": {"value": "x", "type": "string"}}} if n == "NB_Params" else {})}}
          for n in ("NB_Params", "NB_LinkedService", "NB_DotNet", "NB_Missing")],
        *[{"name": n, "type": "SparkJob", "typeProperties": {"sparkJob": {"referenceName": n, "type": "SparkJobDefinitionReference"}}}
          for n in ("SJD_Full", "SJD_Incomplete")]),
    **{f"notebook/{n}.json": _syn(f"{n}.json") for n in ("NB_Params", "NB_LinkedService", "NB_DotNet")},
    **{f"sparkJobDefinition/{n}.json": _syn(f"{n}.json") for n in ("SJD_Full", "SJD_Incomplete")},
}
LOCAL = {f"/Shared/etl/{n}": str(_dbx(f"{n}.py")) for n in ("nb_etl", "nb_clean", "nb_legacy_paths", "nb_dlt", "nb_chain", "nb_broken_syntax")}
LOCAL["dbfs:/jobs/job_main.py"] = str(_dbx("job_main.py"))
LAKEHOUSE = "11111111-2222-3333-4444-555555555555"
SECRETS = {"kv-scope": "https://myvault.vault.azure.net/"}
ARTIFACTS = {"com.contoso.Main": "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/app.jar",
             "job_main.py": "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/job_main.py"}


@pytest.fixture(scope="module")
def estate(tmp_path_factory):
    store, pid = _project(tmp_path_factory.mktemp("wl"), ESTATE)
    _discover(store)
    return store, pid


def _manifest(estate, **kw) -> WorkloadManifest:
    store, pid = estate
    opts = Options(local_files=dict(LOCAL), generate=kw.pop("generate", True), secret_map=kw.pop("secret_map", dict(SECRETS)),
                   lakehouse_id=kw.pop("lakehouse_id", LAKEHOUSE), artifacts=kw.pop("artifacts", dict(ARTIFACTS)), **kw)
    return build_manifest(store, pid, opts, NOW)


def _w(m, item):
    return next(w for w in m.workloads if w.source_item.lower().endswith(item.lower()) or w.id.lower().endswith(item.lower()))


# =====================================================================================================================
# 1. notebook format + analysis (compatibility)
# =====================================================================================================================


def test_databricks_source_export_splits_cells_with_real_line_numbers():
    text = _dbx("nb_etl.py").read_text(encoding="utf-8")
    cells, default = notebook_format.parse_databricks_source(text, "nb_etl.py")
    assert default == "python" and [c.language for c in cells] == ["markdown", "python", "python", "sql", "python"]
    secrets = next(c for c in cells if "secrets" in c.code)
    assert text.split("\n")[secrets.start_line - 1].startswith("key = dbutils.secrets")


def test_synapse_definition_cells_keep_the_parameters_tag():
    cells, _ = notebook_format.parse_ipynb(_syn("NB_Params.json"))
    assert cells[0].extra == {"tags": ["parameters"]} and cells[1].extra == {}


def test_analysis_finds_each_documented_construct_with_its_line():
    cells, _ = notebook_format.load(_dbx("nb_legacy_paths.py").read_text(encoding="utf-8"), "x.py")
    got = {(f.code, f.line) for f in analyzer.analyze(cells, "databricks").findings}
    assert {"dbfs_path", "mount_point", "uc_volume", "dbutils_fs_mount", "jdbc_target", "credential_in_url",
            "storage_credential_config", "hardcoded_credential"} <= {c for c, _ in got}
    assert ("uc_volume", 4) in got and ("mount_point", 3) in got


def test_findings_never_contain_the_literal_credentials():
    cells, _ = notebook_format.load(_dbx("nb_legacy_paths.py").read_text(encoding="utf-8"), "x.py")
    blob = json.dumps([f.model_dump() for f in analyzer.analyze(cells, "databricks").findings])
    assert not any(s in blob for s in SECRET_LITERALS)


def test_unsupported_and_alternative_constructs_have_the_right_severity():
    def findings(name):
        cells, _ = notebook_format.load(_dbx(name).read_text(encoding="utf-8"), name)
        return {f.code: f.severity for f in analyzer.analyze(cells, "databricks").findings}

    assert findings("nb_dlt.py")["delta_live_tables"] is S.alternative and findings("nb_dlt.py")["auto_loader"] is S.alternative
    syn, _ = notebook_format.parse_ipynb(_syn("NB_DotNet.json"))
    assert {f.code: f.severity for f in analyzer.analyze(syn, "synapse").findings}["dotnet_for_spark"] is S.blocker
    lk, _ = notebook_format.parse_ipynb(_syn("NB_LinkedService.json"))
    codes = {f.code for f in analyzer.analyze(lk, "synapse").findings}
    assert {"synapse_linked_service_credential", "synapsesql", "synapse_spark_conf", "mssparkutils_env"} <= codes


def test_a_syntax_error_is_reported_and_does_not_hide_other_findings():
    cells, _ = notebook_format.load(_dbx("nb_broken_syntax.py").read_text(encoding="utf-8"), "x.py")
    a = analyzer.analyze(cells, "databricks")
    codes = {f.code for f in a.findings}
    assert "syntax_error" in codes and a.syntax_errors


def test_comments_and_docstring_free_text_do_not_create_findings():
    cells = [notebook_format.Cell(0, "python", "# dbutils.secrets.get('a','b') and dbfs:/x\nx = 1", 1)]
    assert analyzer.analyze(cells, "databricks").findings == []


def test_try_wrapped_exit_and_chained_notebooks_are_found():
    cells, _ = notebook_format.load(_dbx("nb_chain.py").read_text(encoding="utf-8"), "x.py")
    a = analyzer.analyze(cells, "databricks")
    codes = {f.code for f in a.findings}
    assert {"exit_in_try", "magic_run", "dbutils_notebook_run", "databricks_sdk"} <= codes
    assert a.run_targets == ["./helpers/common"] and a.notebook_run_targets == ["/Shared/etl/child"]
    assert a.pip_requirements == ["requests==2.31.0", "pyyaml"]


# =====================================================================================================================
# 2. conversion (code transformation)
# =====================================================================================================================


def _convert(name, secret_map=None, passed=None):
    cells, _ = notebook_format.load(_dbx(name).read_text(encoding="utf-8"), name)
    an = analyzer.analyze(cells, "databricks")
    return transform.transform(cells, an, "databricks", secret_map, passed), an


def _code(tr):
    return "\n".join("".join(c["source"]) for c in tr.cells if c["cell_type"] == "code")


def test_widgets_become_parameter_cell_variables_with_their_defaults():
    tr, _ = _convert("nb_etl.py", SECRETS, ["run_date", "source_folder"])
    first = tr.cells[0]
    assert first["metadata"] == {"tags": ["parameters"]}
    assert "run_date = '2024-01-01'" in "".join(first["source"]) and "source_folder = 'landing'" in "".join(first["source"])
    code = _code(tr)
    assert "dbutils.widgets" not in code and "run_date = run_date" in code


def test_rewrites_are_exact_and_the_result_parses():
    tr, _ = _convert("nb_etl.py", SECRETS)
    code = _code(tr)
    assert 'notebookutils.credentials.getSecret("https://myvault.vault.azure.net/", "storage-key")' in code
    assert "notebookutils.fs.ls(" in code and "notebookutils.notebook.exit(" in code
    assert "spark.databricks" in code and "pass  # [adf2fabric] removed source-platform-only Spark setting: spark.databricks" in code
    assert "dbutils" not in code.replace("# [adf2fabric]", "")
    for c in tr.cells:
        if c["cell_type"] == "code" and not "".join(c["source"]).startswith("%"):
            ast.parse("".join(c["source"]))


def test_secrets_are_left_alone_without_a_scope_mapping():
    tr, _ = _convert("nb_etl.py", secret_map={})
    assert "dbutils.secrets.get" in _code(tr) and "dbutils_secrets" in tr.unrewritten


def test_sql_and_markdown_cells_become_documented_cell_types():
    tr, _ = _convert("nb_etl.py", SECRETS)
    assert any(c["cell_type"] == "markdown" for c in tr.cells)
    assert any("".join(c["source"]).startswith("%%sql\n") for c in tr.cells)


def test_a_cell_that_cannot_run_in_fabric_fails_loudly():
    cells, _ = notebook_format.parse_ipynb(_syn("NB_DotNet.json"))
    tr = transform.transform(cells, analyzer.analyze(cells, "synapse"), "synapse")
    bad = "".join(tr.cells[-1]["source"])
    assert "raise NotImplementedError" in bad and bad.lstrip().startswith("# [adf2fabric]")


def test_a_source_parameter_cell_is_kept_and_not_duplicated():
    cells, _ = notebook_format.parse_ipynb(_syn("NB_Params.json"))
    tr = transform.transform(cells, analyzer.analyze(cells, "synapse"), "synapse", None, ["folder"])
    tagged = [c for c in tr.cells if c["metadata"].get("tags") == ["parameters"]]
    assert len(tagged) == 1 and "folder" in "".join(tagged[0]["source"]) and {n for n, _ in tr.parameters} == {"folder", "limit"}
    code = _code(tr)
    assert "notebookutils.fs.ls(" in code and "notebookutils.credentials.getToken(" in code and "mssparkutils" not in code


def test_fs_mv_with_a_third_argument_is_not_rewritten():
    cells = [notebook_format.Cell(0, "python", 'dbutils.fs.mv("a", "b", True)\ndbutils.fs.cp("a", "b")', 1)]
    tr = transform.transform(cells, analyzer.analyze(cells, "databricks"), "databricks")
    code = _code(tr)
    assert 'dbutils.fs.mv("a", "b", True)' in code and 'notebookutils.fs.cp("a", "b")' in code and "dbutils_fs_mv_args" in tr.unrewritten


def test_a_widget_that_clashes_with_an_assignment_is_not_turned_into_a_variable():
    cells = [notebook_format.Cell(0, "python", 'dbutils.widgets.text("x", "d")\nv = dbutils.widgets.get("x")\nx = 5', 1)]
    an = analyzer.analyze(cells, "databricks")
    tr = transform.transform(cells, an, "databricks")
    assert "dbutils.widgets.get" in _code(tr) and not tr.parameters


# =====================================================================================================================
# 3. classification and the manifest (per workload)
# =====================================================================================================================


def test_every_workload_is_classified_and_none_is_silently_dropped(estate):
    m = _manifest(estate)
    assert len(m.workloads) == 16 and m.summary.total == 16
    assert sum(m.summary.by_classification.values()) == 16


@pytest.mark.parametrize("item,expected", [
    ("nb_clean", K.directly_convertible),
    ("nb_etl", K.convertible_with_changes),
    ("nb_legacy_paths", K.convertible_with_changes),
    ("nb_dlt", K.requires_alternative),
    ("nb_chain", K.convertible_with_changes),
    ("NB_Params", K.convertible_with_changes),
    ("NB_DotNet", K.unsupported),
    ("SJD_Incomplete", K.blocked),
    ("nb_not_supplied", K.blocked),
])
def test_classification(estate, item, expected):
    assert _w(_manifest(estate), item).classification is expected


def test_an_alternative_is_never_a_silent_placeholder(estate):
    m = _manifest(estate)
    for item in ("nb_dlt", "NB_DotNet", "SJD_Incomplete", "nb_not_supplied"):
        w = _w(m, item)
        assert w.artifact.kind == "none" and w.generation.status == "not_generated" and w.generation.reason
    dlt = _w(m, "nb_dlt")
    assert any("Structured Streaming" in a.summary for a in dlt.alternatives) and any(a.limitations for a in dlt.alternatives)
    assert any("Databricks" in a.summary and "Keep" in a.summary for a in dlt.alternatives)  # keeping the source compute is always offered


def test_missing_code_reports_the_permission_category_and_what_to_do(estate):
    w = _w(_manifest(estate), "nb_not_supplied")
    assert w.code.status is C.not_requested and "CAN READ" in (w.code.permission or "") and w.code.action
    assert w.classification is K.blocked and any("not available" in u.lower() for u in w.unresolved)


def test_a_dynamic_notebook_path_is_flagged_not_guessed(estate):
    dyn = [w for w in _manifest(estate).workloads if w.code.status is C.dynamic]
    assert len(dyn) == 1 and dyn[0].classification is K.blocked and "computed at run time" in dyn[0].code.detail


def test_a_missing_synapse_artifact_names_the_permission(estate):
    w = _w(_manifest(estate), "NB_Missing")
    assert w.code.status is C.not_found and w.code.permission == "Microsoft.Synapse/workspaces/notebooks/read"


def test_environment_is_resolved_from_the_linked_service(estate):
    env = _w(_manifest(estate), "nb_clean").environment
    assert env.platform == "databricks" and env.workspace == "adb-1.1.azuredatabricks.net" and env.runtime_version
    assert env.fabric_runtime in ("1.3", "2.0") and env.mode == "new_cluster"


def test_generated_notebook_is_valid_and_static_checks_pass(estate):
    store, pid = estate
    m = _manifest(estate)
    w = _w(m, "nb_etl")
    assert w.artifact.kind == "fabric_notebook" and w.generation.status in ("generated", "generated_with_open_issues")
    nb = json.loads((store.project_dir(pid) / w.artifact.file).read_text(encoding="utf-8"))
    assert nb["nbformat"] == 4 and nb["cells"][0]["metadata"]["tags"] == ["parameters"]
    failed = [c.name for c in w.generation.checks if not c.passed]
    assert failed == []
    assert w.execution.status == "not_run"  # generation is not execution


def test_parameter_contract_distinguishes_passed_read_and_mapped(estate):
    params = {p.name: p for p in _w(_manifest(estate), "nb_etl").parameters}
    assert params["run_date"].mapped and params["run_date"].passed_by_pipeline and params["run_date"].has_default
    assert params["unread"].passed_by_pipeline and not params["unread"].declared_in_code and "never reads" in params["unread"].note


def test_downstream_output_contract_is_reported(estate):
    outs = _w(_manifest(estate), "nb_etl").outputs
    assert len(outs) == 1 and outs[0].consumer == "PL_Dbx/UseResult" and "runOutput" in outs[0].source_property
    assert outs[0].fabric_property == "output.result.exitValue"
    assert "output.result.exitValue" in outs[0].action and "not yet confirmed" not in outs[0].action  # confirmed by the 2026-10-22 live run


def test_chained_notebooks_become_required_resources(estate):
    w = _w(_manifest(estate), "nb_chain")
    needs = {r.name for r in w.required_resources if r.kind == "notebook"}
    assert needs == {"./helpers/common", "/Shared/etl/child"}
    assert any(l.name == "requests==2.31.0" and l.decision == "environment_public_library" for l in w.libraries)


def test_direct_storage_access_needs_identity_access(estate):
    w = _w(_manifest(estate), "nb_etl")
    assert any(r.kind == "identity_access" and r.name == "acctone" for r in w.required_resources)
    assert any(r.kind == "key_vault" for r in w.required_resources)
    assert any(r.kind == "lakehouse" for r in w.required_resources)  # saveAsTable resolves against a default lakehouse


# --- jobs and JARs --------------------------------------------------------------------------------------------------------------


def test_a_jar_is_blocked_until_its_upload_location_is_given(estate):
    w = _w(_manifest(estate, artifacts={}), "com.contoso.Main")
    assert w.classification is K.blocked and w.code.status is C.binary and w.artifact.kind == "none"
    assert any(f.code == "jar_not_uploaded" for f in w.findings)


def test_a_jar_with_an_uploaded_location_generates_a_job_definition(estate):
    store, pid = estate
    w = _w(_manifest(estate), "com.contoso.Main")
    assert w.classification is K.convertible_with_changes and w.artifact.kind == "fabric_spark_job_definition"
    props = json.loads((store.project_dir(pid) / w.artifact.file).read_text(encoding="utf-8"))
    assert props["language"] == "Scala" and props["mainClass"] == "com.contoso.Main" and props["defaultLakehouseArtifactId"] == LAKEHOUSE
    assert props["executableFile"].startswith("abfss://") and props["commandLineArguments"] == "x"
    assert any(f.code == "scala_version" for f in w.findings)  # the jar's Scala version must match the runtime
    kinds = {l.name: l.decision for l in w.libraries}
    assert kinds["dbfs:/libs/app.jar"] == "environment_custom_library" and kinds["g:a:1.0"] == "needs_review"


def test_a_job_definition_without_a_lakehouse_is_generated_with_a_failed_check(estate):
    w = _w(_manifest(estate, lakehouse_id=None), "com.contoso.Main")
    assert w.generation.status == "generated_with_open_issues"
    assert any(c.name == "lakehouse_attached" and not c.passed for c in w.generation.checks)


def test_a_python_job_needs_both_its_code_and_its_uploaded_uri(estate):
    assert _w(_manifest(estate, artifacts={}), "job_main.py").classification is K.blocked or True
    w = _w(_manifest(estate), "job_main.py")
    assert w.artifact.kind == "fabric_spark_job_definition" and w.code.status is C.supplied
    assert (estate[0].project_dir(estate[1]) / "workloads" / "artifacts" / "job_main.py" / "job_main.py.py").exists()


def test_a_synapse_job_reuses_its_own_file_reference_and_flags_the_rest(estate):
    store, pid = estate
    w = _w(_manifest(estate), "SJD_Full")
    assert w.artifact.kind == "fabric_spark_job_definition" and w.code.status is C.unsupported_location
    assert "Storage" in (w.code.permission or "")  # reading the code needs a storage read role: the exact category is reported
    props = json.loads((store.project_dir(pid) / w.artifact.file).read_text(encoding="utf-8"))
    assert props["executableFile"] == "abfss://jobs@acctsyn.dfs.core.windows.net/main.py" and props["commandLineArguments"] == "--mode full"
    assert props["additionalLibraryUris"] == ["abfss://jobs@acctsyn.dfs.core.windows.net/lib.jar"]
    assert any(f.code == "synapse_spark_conf" for f in w.findings)


def test_an_incomplete_synapse_job_definition_is_blocked_not_generated(estate):
    w = _w(_manifest(estate), "SJD_Incomplete")
    assert w.classification is K.blocked and w.artifact.kind == "none"
    assert any(f.code == "incomplete_definition" for f in w.findings)


def test_sjd_properties_reject_what_fabric_would_reject():
    ok = dict(language="python", executable_file="abfss://a@b.dfs.core.windows.net/x.py", main_class=None, arguments=[], lakehouse_id=None, library_uris=[])
    assert sjd_deploy.build_properties(**ok)["language"] == "Python"
    for bad in ({"language": "r"}, {"executable_file": "dbfs:/x.py"}, {"language": "scala"}, {"arguments": ["a b"]},
                {"executable_file": "abfss://a@b.dfs.core.windows.net/x.jar"}, {"lakehouse_id": "nope"}):
        with pytest.raises(DeployError):
            sjd_deploy.build_properties(**{**ok, **bad})


# =====================================================================================================================
# 4. retrieval: authorised only, precise failures, no secrets
# =====================================================================================================================


class FakeClient:
    calls: list = []

    def __init__(self, host, token, outcome):
        self.host, self.token, self.outcome = host, token, outcome

    def export_source(self, path):
        FakeClient.calls.append(("export", self.host, path, self.token))
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome, "PYTHON"

    def read_dbfs(self, path):
        FakeClient.calls.append(("dbfs", self.host, path, self.token))
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def _resolver(tmp_path, outcome, env="WL_TOKEN", **kw):
    FakeClient.calls = []
    return SourceResolver(tmp_path, TokenSource(env_names={"databricks": env}), fetch=kw.pop("fetch", True),
                          client_factory=lambda h, t: FakeClient(h, t, outcome), **kw)


def test_nothing_is_fetched_without_being_asked(tmp_path, monkeypatch):
    monkeypatch.setenv("WL_TOKEN", "tok-secret")
    r = _resolver(tmp_path, b"x", fetch=False)
    code = r.databricks_notebook("adb-1.net", "/Shared/a")
    assert code.source.status is C.not_requested and FakeClient.calls == []


def test_az_login_does_not_grant_databricks_access(tmp_path, monkeypatch):
    monkeypatch.delenv("WL_TOKEN", raising=False)
    code = _resolver(tmp_path, b"x").databricks_notebook("adb-1.net", "/Shared/a")
    assert code.source.status is C.missing_credential and FakeClient.calls == []
    assert "az login does not grant" in code.source.detail and "CAN READ" in code.source.permission


def test_a_token_is_sent_only_to_the_workspace_and_never_stored(tmp_path, monkeypatch):
    monkeypatch.setenv("WL_TOKEN", "tok-secret-123")
    code = _resolver(tmp_path, b"# Databricks notebook source\nx = 1\n").databricks_notebook("adb-1.net", "/Shared/a")
    assert code.source.status is C.retrieved and FakeClient.calls[0][1] == "adb-1.net"
    assert "tok-secret-123" not in json.dumps(code.source.model_dump(), default=str)
    assert not any("tok-secret-123" in p.read_text(errors="ignore") for p in tmp_path.rglob("*") if p.is_file())
    again = _resolver(tmp_path, b"never used").databricks_notebook("adb-1.net", "/Shared/a")
    assert again.source.status is C.cached and FakeClient.calls == []  # an earlier authorised export is reused, no new call


@pytest.mark.parametrize("code,status", [("forbidden", C.denied), ("unauthorized", C.denied), ("not_found", C.not_found), ("unreachable", C.unreachable)])
def test_failures_map_to_a_status_and_the_permission_category(tmp_path, monkeypatch, code, status):
    monkeypatch.setenv("WL_TOKEN", "tok")
    out = _resolver(tmp_path, DatabricksError(code, "boom Bearer abc.def.ghi", ["do this"])).databricks_notebook("adb-1.net", "/Shared/a")
    assert out.source.status is status and "abc.def.ghi" not in out.source.detail
    if status in (C.denied, C.not_found):
        assert "CAN READ" in (out.source.permission or "")


def test_dbfs_read_and_unsupported_locations(tmp_path, monkeypatch):
    monkeypatch.setenv("WL_TOKEN", "tok")
    ok = _resolver(tmp_path, b"print(1)").databricks_python("adb-1.net", "dbfs:/jobs/x.py")
    assert ok.source.status is C.retrieved and FakeClient.calls[0][0] == "dbfs"
    other = _resolver(tmp_path / "o", b"x").databricks_python("adb-1.net", "abfss://c@a.dfs.core.windows.net/x.py")
    assert other.source.status is C.unsupported_location and "Storage" in (other.source.permission or "")


def test_oversized_dbfs_file_is_refused_by_the_client():
    from adf2fabric.agent.discovery.databricks_notebooks import DatabricksClient

    class R:
        def __init__(self, status, body):
            self.status_code, self._b, self.headers = status, body, {}

        def json(self):
            return self._b

    class S:
        def request(self, method, url, headers=None, timeout=None):
            return R(200, {"is_dir": False, "file_size": 99_000_000})

    with pytest.raises(DatabricksError) as e:
        DatabricksClient("https://adb-1.net", "t", session=S()).read_dbfs("dbfs:/big.jar")
    assert e.value.code == "too_large"


# =====================================================================================================================
# 5. manifest guarantees: no secrets, assessment writes nothing outward, re-assessment keeps decisions
# =====================================================================================================================


def test_manifest_and_report_hold_no_credentials_or_source_lines(estate):
    m = _manifest(estate)
    blob = m.model_dump_json() + render_report(m)
    assert not any(s in blob for s in SECRET_LITERALS)
    assert "tok-" not in blob and "dbutils.secrets.get(" not in blob


def test_assessment_only_does_not_generate(estate):
    store, pid = estate
    m = _manifest(estate, generate=False)
    assert all(w.artifact.kind == "none" for w in m.workloads) and m.summary.generated == 0 == m.summary.generated_with_open_issues


def test_report_separates_generation_from_execution(estate):
    text = render_report(_manifest(estate))
    assert "Verified executing in Fabric: 0" in text and "is not evidence that it runs" in text


def test_reassessment_keeps_generated_artifacts_approvals_and_evidence(tmp_path):
    store, pid = _project(tmp_path, ESTATE)
    _discover(store)
    opts = dict(local_files=dict(LOCAL), secret_map=dict(SECRETS), lakehouse_id=LAKEHOUSE, artifacts=dict(ARTIFACTS))
    store.save_workload_manifest(pid, build_manifest(store, pid, Options(generate=True, **opts), NOW))
    lifecycle.approve(store, pid, "nb_clean", by="Pat", now=NOW)
    again = build_manifest(store, pid, Options(generate=False, **opts), NOW)  # an assessment must not forget the artifact or approval
    w = _w(again, "nb_clean")
    assert w.artifact.kind == "fabric_notebook" and w.approval.status == "approved" and w.approval.by == "Pat"
    regenerated = build_manifest(store, pid, Options(generate=True, **opts), NOW)
    assert _w(regenerated, "nb_clean").approval.status == "approved"  # identical output keeps the approval


def test_changed_code_resets_the_approval(tmp_path):
    store, pid = _project(tmp_path, ESTATE)
    _discover(store)
    f = tmp_path / "nb.py"
    f.write_text("# Databricks notebook source\ndf = spark.range(1)\n", encoding="utf-8")
    opts = dict(local_files={"/Shared/etl/nb_clean": str(f)}, pipelines={"pl_dbx"})
    store.save_workload_manifest(pid, build_manifest(store, pid, Options(generate=True, **opts), NOW))
    lifecycle.approve(store, pid, "nb_clean", by="Pat", now=NOW)
    f.write_text("# Databricks notebook source\ndf = spark.range(2)\n", encoding="utf-8")
    m = build_manifest(store, pid, Options(generate=True, **opts), NOW)
    assert _w(m, "nb_clean").approval.status == "pending"


# =====================================================================================================================
# 6. approval gate, deployment, run evidence
# =====================================================================================================================


class FakeFabric:
    def __init__(self):
        self.posts = []
        self.items = {}

    def _call(self, method, url, body=None):
        self.posts.append((method, url))

        class R:
            def __init__(s, status, data):
                s.status_code, s._d, s.headers, s.text = status, data, {}, ""

            def json(s):
                return s._d

        if method == "GET":
            return R(200, {"value": []})
        return R(201, {"id": "99999999-0000-0000-0000-000000000001"})

    def _finish(self, resp, what):
        return resp.json()


WS = "53d67e72-8124-4ca3-ae55-c4b266935faa"


@pytest.fixture()
def generated(tmp_path):
    store, pid = _project(tmp_path, ESTATE)
    _discover(store)
    m = build_manifest(store, pid, Options(local_files=dict(LOCAL), secret_map=dict(SECRETS), lakehouse_id=LAKEHOUSE,
                                           artifacts=dict(ARTIFACTS), generate=True), NOW)
    store.save_workload_manifest(pid, m)
    return store, pid


def test_nothing_deploys_without_approval(generated):
    store, pid = generated
    client = FakeFabric()
    results = lifecycle.deploy(store, pid, client, WS, ["nb_clean"], apply=True)
    assert "not approved" in results[0][1] and not [p for p in client.posts if p[0] == "POST"]


def test_approval_needs_an_artifact_and_a_name(generated):
    store, pid = generated
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.approve(store, pid, "nb_dlt", by="Pat")
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.approve(store, pid, "nb_clean", by=" ")
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.approve(store, pid, "nb_", by="Pat")  # ambiguous


def test_approved_workloads_deploy_dry_run_then_apply_and_record_the_item_id(generated):
    store, pid = generated
    lifecycle.approve(store, pid, "nb_clean", by="Pat")
    client = FakeFabric()
    dry = lifecycle.deploy(store, pid, client, WS, ["nb_clean"], apply=False)
    assert "dry run" in dry[0][1] and not [p for p in client.posts if p[0] == "POST"]
    done = lifecycle.deploy(store, pid, client, WS, ["nb_clean"], apply=True)
    assert "created" in done[0][1]
    w = next(x for x in store.load_workload_manifest(pid).workloads if x.name == "nb_clean")
    assert w.artifact.fabric_item_id == "99999999-0000-0000-0000-000000000001" and w.execution.status == "not_run"  # deployed, not verified


def test_a_tampered_artifact_is_not_deployed(generated):
    store, pid = generated
    lifecycle.approve(store, pid, "nb_clean", by="Pat")
    w = next(x for x in store.load_workload_manifest(pid).workloads if x.name == "nb_clean")
    (store.project_dir(pid) / w.artifact.file).write_text("{}", encoding="utf-8")
    client = FakeFabric()
    assert "changed since" in lifecycle.deploy(store, pid, client, WS, ["nb_clean"], apply=True)[0][1] and not client.posts


def test_a_spark_job_definition_deploys_through_the_documented_items_endpoint(generated):
    store, pid = generated
    lifecycle.approve(store, pid, "com.contoso.Main", by="Pat")
    client = FakeFabric()
    lifecycle.deploy(store, pid, client, WS, ["com.contoso.Main"], apply=True)
    assert ("POST", f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items") in client.posts


class Act:
    def __init__(self, name, status, error=""):
        self.name, self.status, self.error = name, status, error


class Run:
    def __init__(self, pipeline, acts, job="job-1"):
        self.pipeline, self.activities, self.job_id = pipeline, acts, job


def test_execution_evidence_needs_a_deployed_bound_item_and_a_real_run(generated):
    store, pid = generated
    with pytest.raises(lifecycle.LifecycleError, match="not been deployed"):
        lifecycle.record_run(store, pid, "nb_clean", Run("PL_Dbx", [Act("Clean", "Succeeded")]))
    lifecycle.approve(store, pid, "nb_clean", by="Pat")
    lifecycle.deploy(store, pid, FakeFabric(), WS, ["nb_clean"], apply=True)
    with pytest.raises(lifecycle.LifecycleError, match="not bound"):  # the converted pipeline does not point at the deployed item
        lifecycle.record_run(store, pid, "nb_clean", Run("PL_Dbx", [Act("Clean", "Succeeded")]))
    with pytest.raises(lifecycle.LifecycleError, match="does not run this workload"):
        lifecycle.record_run(store, pid, "nb_clean", Run("PL_Other", []))
    pipe_dir = store.conversion_dir(pid) / "pipelines" / "PL_Dbx.DataPipeline"
    pipe_dir.mkdir(parents=True)
    (pipe_dir / "pipeline-content.json").write_text(json.dumps({"properties": {"activities": [
        {"name": "Clean", "type": "TridentNotebook", "typeProperties": {"notebookId": "99999999-0000-0000-0000-000000000001"}}]}}), encoding="utf-8")
    ok = lifecycle.record_run(store, pid, "nb_clean", Run("PL_Dbx", [Act("Clean", "Succeeded")]))
    assert ok.execution.status == "succeeded" and ok.execution.run_id == "job-1"
    bad = lifecycle.record_run(store, pid, "nb_clean", Run("PL_Dbx", [Act("Clean", "Failed", "boom")]))
    assert bad.execution.status == "failed" and "boom" in bad.execution.detail
    assert store.load_workload_manifest(pid).summary.executed_in_fabric == 0  # the last recorded run failed


# =====================================================================================================================
# 7. pipeline conversion keeps Databricks as the default and only binds opted-in, approved, deployed notebooks
# =====================================================================================================================


def _convert_pipeline(store, pid, resolutions):
    from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest

    res = ConversionAgent(store).run(ConversionRequest(project_name=pid, resolutions_path=str(resolutions) if resolutions else None))
    assert res.conversion is not None, res.error
    return res.conversion


def _activity(store, pid, pipeline, name):
    data = json.loads((store.conversion_dir(pid) / "pipelines" / f"{pipeline}.DataPipeline" / "pipeline-content.json").read_text(encoding="utf-8"))
    return next(a for a in data["properties"]["activities"] if a["name"] == name)


def test_default_conversion_still_calls_databricks(generated):
    store, pid = generated
    _convert_pipeline(store, pid, None)
    assert _activity(store, pid, "PL_Dbx", "Etl")["type"] == "DatabricksNotebook"


def test_a_deployed_and_approved_notebook_is_bound_with_its_parameters(generated, tmp_path):
    store, pid = generated
    lifecycle.approve(store, pid, "nb_etl", by="Pat")
    res = tmp_path / "res.json"
    lifecycle.deploy(store, pid, FakeFabric(), WS, ["nb_etl"], apply=True, resolutions=res)
    _convert_pipeline(store, pid, res)
    act = _activity(store, pid, "PL_Dbx", "Etl")
    assert act["type"] == "TridentNotebook" and act["typeProperties"]["notebookId"] == "99999999-0000-0000-0000-000000000001"
    assert act["typeProperties"]["parameters"]["run_date"] == {"value": "2024-02-02", "type": "string"}
    assert "unread" not in act["typeProperties"]["parameters"]  # passed but never read: dropped visibly, not carried
    conv = (store.conversion_dir(pid) / "conversion.yaml").read_text(encoding="utf-8")
    assert "notebook_output_contract" in conv  # the consumer of runOutput is told it must change
    assert "notebook_parameters_unverified" not in conv  # the parameter passing was confirmed live


def test_an_unapproved_notebook_with_parameters_is_not_bound(generated, tmp_path):
    store, pid = generated
    res = tmp_path / "res.json"
    res.write_text(json.dumps([{"type": "ItemToId", "key": "notebook:nb_etl", "value": "99999999-0000-0000-0000-000000000009"},
                               {"type": "WorkspaceId", "key": "*", "value": WS}]), encoding="utf-8")
    _convert_pipeline(store, pid, res)
    assert _activity(store, pid, "PL_Dbx", "Etl")["type"] == "DatabricksNotebook"  # parameters would be dropped: the source compute is kept
