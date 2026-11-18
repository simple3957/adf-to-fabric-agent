"""An Execute SSIS package activity as a workload and as a converted Fabric activity (2026-11-18): the package is a file the person supplies (or an
ADLS Gen2 path the tool can read), translated by agent/workloads/ssis.py, approved, created by Deploy; the activity then runs that notebook.
A real discovery and the real converter and workload engine run here."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from adf2fabric.agent.workloads import lifecycle
from adf2fabric.agent.workloads.engine import Options, build_manifest
from adf2fabric.project.workloads import Classification as K, CodeStatus as C, WorkloadKind as W
from tests.conversion_support import by_name, codes, convert, find, pipe
from tests.test_discovery_activity_inventory import _discover, _project

FIXTURE = Path(__file__).parent / "fixtures" / "ssis" / "orders_etl.dtsx"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
PATH = "\\\\fileshare\\packages\\orders_etl.dtsx"


def _activity(**tp):
    return {"name": "RunPkg", "type": "ExecuteSSISPackage", "typeProperties": {
        "packageLocation": {"type": "File", "typeProperties": {"packagePath": PATH}},
        "runtime": {"referenceName": "SsisIr", "type": "IntegrationRuntimeReference"}, "loggingLevel": "Basic", **tp}}


def _estate(**tp):
    return {"pipeline/PL_Ssis.json": {"name": "PL_Ssis", "properties": {"activities": [_activity(**tp)]}}}


def _manifest(tmp_path, **opts):
    store, pid = _project(tmp_path, _estate(packageParameters={"Threshold": {"value": "250"}},
                                            propertyOverrides={"\\Package.Variables[User::Threshold].Value": {"value": 300},
                                                               "\\Package.MaxConcurrentExecutables": {"value": 2}}))
    _discover(store)
    return store, pid, build_manifest(store, pid, Options(generate=True, **opts), NOW)


def test_the_package_must_be_supplied_and_the_finding_says_how(tmp_path):
    _, _, m = _manifest(tmp_path)
    [w] = [x for x in m.workloads if x.kind is W.adf_ssis]
    assert w.classification is K.blocked and w.code.status is C.not_requested and w.name == "orders_etl"
    assert any(f.code == "ssis_package_unavailable" and "--source" in f.action for f in w.findings)
    assert w.artifact.kind == "none"  # nothing is generated for a package nobody has read: never a placeholder


def test_a_supplied_package_is_translated_generated_and_its_parameters_are_the_notebooks(tmp_path):
    _, _, m = _manifest(tmp_path, local_files={PATH: str(FIXTURE)})
    [w] = [x for x in m.workloads if x.kind is W.adf_ssis]
    assert w.code.status is C.supplied and w.code.language == "ssis" and w.classification in (K.directly_convertible, K.convertible_with_changes)
    assert w.generation.status in ("generated", "generated_with_open_issues") and w.artifact.kind == "fabric_notebook"
    assert sorted(p.name for p in w.parameters) == ["cm_Big_csv", "cm_Orders_csv", "cm_Small_csv", "ssis_root", "v_Threshold"]
    assert all(p.mapped for p in w.parameters)
    # what the activity passes: the package parameter Threshold is not declared (the package has a variable), the override of the variable is
    unknown = {f.construct for f in w.findings if f.code == "ssis_parameter_not_declared"}
    assert "Threshold" in unknown and "\\Package.MaxConcurrentExecutables" in unknown and "\\Package.Variables[User::Threshold].Value" not in unknown
    assert any(f.code == "ssis_review" and "ssis_root" in f.message for f in w.findings)


def test_the_package_cached_by_an_earlier_generate_is_used_again(tmp_path):
    store, pid, _ = _manifest(tmp_path, local_files={PATH: str(FIXTURE)})
    cached = store.project_dir(pid) / "workloads" / "code" / "ssis"
    assert not cached.exists()  # a supplied file is read where it is; only a fetched one is cached
    cached = cached / "fileshare" / "packages"
    cached.mkdir(parents=True)
    (cached / "orders_etl.dtsx").write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    m = build_manifest(store, pid, Options(generate=True), NOW)
    [w] = [x for x in m.workloads if x.kind is W.adf_ssis]
    assert w.code.status is C.cached and w.artifact.kind == "fabric_notebook"


def test_an_inline_package_is_supplied_as_a_file_because_discovery_does_not_keep_its_text(tmp_path):
    xml = FIXTURE.read_text(encoding="utf-8").split("?>", 1)[1]
    act = {"name": "Inline", "type": "ExecuteSSISPackage", "typeProperties": {"packageLocation": {"type": "InlinePackage", "typeProperties": {"packagePath": xml}}}}
    store, pid = _project(tmp_path, {"pipeline/PL_I.json": {"name": "PL_I", "properties": {"activities": [act]}}})
    _discover(store)
    [w] = [x for x in build_manifest(store, pid, Options(generate=True), NOW).workloads if x.kind is W.adf_ssis]
    assert w.source_item == "inline:PL_I/Inline" and w.classification is K.blocked
    assert any("--source 'PL_I/Inline'=<file.dtsx>" in f.action for f in w.findings if f.code == "ssis_package_unavailable")
    f = tmp_path / "inline.dtsx"
    f.write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    [w] = [x for x in build_manifest(store, pid, Options(generate=True, local_files={"PL_I/Inline": str(f)}), NOW).workloads if x.kind is W.adf_ssis]
    assert w.code.status is C.supplied and w.artifact.kind == "fabric_notebook"


def test_a_package_with_a_task_the_rules_do_not_map_keeps_a_notebook_that_raises_there(tmp_path):
    xml = FIXTURE.read_text(encoding="utf-8").replace("</DTS:Executables>\n</DTS:Executable>",
                                                      '<DTS:Executable DTS:refId="Package\\Notify" DTS:ExecutableType="Microsoft.SendMailTask" DTS:ObjectName="Notify"/></DTS:Executables>\n</DTS:Executable>')
    f = tmp_path / "p.dtsx"
    f.write_text(xml, encoding="utf-8")
    _, _, m = _manifest(tmp_path, local_files={PATH: str(f)})
    [w] = [x for x in m.workloads if x.kind is W.adf_ssis]
    assert w.artifact.kind == "fabric_notebook"
    assert any(x.code == "ssis_untranslated" and x.construct == r"Package\Notify" and x.action.startswith("Draft it with a model") for x in w.findings)
    assert any("Package\\Notify" in u for u in w.unresolved)  # open: the notebook is generated, with open issues, until a person decides


# ---- the converted activity ----------------------------------------------------------------------------------------------------------------
def _convert(**kw):
    act = _activity(packageParameters={"Threshold": {"value": "250"}}, propertyOverrides={"\\Package.Variables[User::Threshold].Value": {"value": 300}},
                    packageConnectionManagers={"Orders.csv": {"ConnectionString": {"value": "x.csv"}, "passWord": {"value": {"type": "SecureString", "value": "p"}}}})
    return convert([pipe(act, name="PL")], **kw)["PL"]


def test_until_the_notebook_is_in_the_workspace_the_activity_names_it():
    rec, content = _convert()
    a = by_name(rec)["RunPkg"]
    assert find(content, "RunPkg")["type"] == "Fail" and "ssis_notebook_pending" in {i.code for i in a.warnings} and a.runtime_disposition == "replace"
    assert "notebook:orders_etl" in {q.name for q in rec.prerequisites if q.kind == "workspace_item"}


def test_a_computed_package_path_stays_a_redesign():
    act = {"name": "RunPkg", "type": "ExecuteSSISPackage", "typeProperties": {"packageLocation": {"type": "File", "typeProperties": {"packagePath": {"value": "@pipeline().parameters.p", "type": "Expression"}}}}}
    rec, _ = convert([pipe(act, name="PL", parameters=["p"])])["PL"]
    assert by_name(rec)["RunPkg"].runtime_disposition == "redesign" and "ssis_unsupported" in {i.code for i in by_name(rec)["RunPkg"].warnings}


def test_once_the_notebook_is_deployed_the_activity_runs_it_with_the_parameters_it_takes(tmp_path):
    store, pid, m = _manifest(tmp_path, local_files={PATH: str(FIXTURE)})
    store.save_workload_manifest(pid, m)
    lifecycle.approve(store, pid, "orders_etl", by="Pat", now=NOW)
    wm = store.load_workload_manifest(pid)
    w = next(x for x in wm.workloads if x.kind is W.adf_ssis)
    lifecycle._record_item(w, "26d52d4c-0000-0000-0000-000000000001", "99999999-0000-0000-0000-000000000009")
    store.save_workload_manifest(pid, wm)
    res = tmp_path / "resolutions.json"
    res.write_text(json.dumps([{"type": "ItemToId", "key": "notebook:orders_etl", "value": "99999999-0000-0000-0000-000000000009"}]), encoding="utf-8")
    # the converter reads the manifest through the project context; the same entry is what a Convert run passes it
    from adf2fabric.agent.conversion.agent import build_project_context
    from adf2fabric.agent.conversion.context import Resolutions
    from adf2fabric.agent.conversion.engine import ConversionEngine
    d = store.load_adf_semantic_discovery(pid)
    ctx = build_project_context(d, Resolutions.load(res))
    ctx.workloads = {x.source_item.lower(): x for x in wm.workloads if x.approval.status == "approved" and x.artifact.fabric_item_id}
    rec, content = ConversionEngine(ctx).convert_pipeline(d.pipelines[0])
    act = find(content, "RunPkg")
    assert act["type"] == "TridentNotebook" and act["typeProperties"]["notebookId"] == "99999999-0000-0000-0000-000000000009"
    # the variable override is passed; the package parameter and the engine setting the notebook does not take are dropped, never invented
    assert act["typeProperties"]["parameters"] == {"v_Threshold": {"value": 300, "type": "string"}}
    dropped = [m_ for m_ in by_name(rec)["RunPkg"].property_mappings if not m_.preserved]
    assert any("typeProperties.packageParameters.Threshold" in m_.source for m_ in dropped)
    assert any("typeProperties.propertyOverrides" in m_.source and "MaxConcurrentExecutables" in m_.source for m_ in dropped)
    assert any("typeProperties.loggingLevel" in m_.source for m_ in dropped) and any("typeProperties.runtime" in m_.source for m_ in dropped)
    assert by_name(rec)["RunPkg"].status.value in ("converted", "converted_with_differences") and "ssis_notebook_pending" not in codes(by_name(rec)["RunPkg"])
