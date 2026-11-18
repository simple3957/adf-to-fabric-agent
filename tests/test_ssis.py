"""SSIS packages for Fabric's Invoke SSIS package (preview): agent/workloads/ssis.py, the ssis_package workload, and the converter's placeholder.

The package is Microsoft's own sample (sql-server-samples, WideWorldImporters DailyETLMain.dtsx, MIT; tests/fixtures/ssis/NOTICE.md), read
as it is. Variants (encrypted, a third-party task) are made from it by changing one attribute, so the parser still reads a real package.
The ADF activity JSON follows the ADF REST spec (ExecuteSSISPackageActivityTypeProperties, SSISPackageLocation: SSISDB | File |
InlinePackage | PackageStore)."""

from __future__ import annotations

import base64
import gzip
import io
import json
import zipfile
import zlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from adf2fabric.agent.workloads import ssis
from adf2fabric.agent.workloads.engine import Options, build_manifest
from adf2fabric.agent.workloads.report import render_report
from adf2fabric.project.workloads import Classification as K, CodeStatus as C, Severity as S
from tests.test_discovery_activity_inventory import _discover, _project

FIX = Path(__file__).parent / "fixtures" / "ssis"
XML = gzip.decompress((FIX / "DailyETLMain.dtsx.gz").read_bytes())
PROJECT = {n: (FIX / n).read_bytes() for n in ("Project.params", "WWI_Source_DB.conmgr", "WWI_DW_Destination_DB.conmgr")}
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
AZURE_SQL = "Data Source=wwi.database.windows.net;Initial Catalog=WWI;Provider=MSOLEDBSQL;User ID=etl;"


def _ispac() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("DailyETLMain.dtsx", XML)
        for k, v in PROJECT.items():
            z.writestr(k, v)
    return buf.getvalue()


# =====================================================================================================================
# 1. where the package is
# =====================================================================================================================


@pytest.mark.parametrize("loc, kind, file, ident", [
    ({"type": "SSISDB", "packagePath": "Finance/Daily ETL/DailyETLMain.dtsx"}, "SSISDB", "DailyETLMain.dtsx", "ssisdb:Finance/Daily ETL/DailyETLMain.dtsx"),
    ({"type": "File", "packagePath": "\\\\acct.file.core.windows.net\\pkgs\\Load.dtsx"}, "File", "Load.dtsx", "file:\\\\acct.file.core.windows.net\\pkgs\\Load.dtsx"),
    ({"type": "File", "packagePath": "\\\\acct.file.core.windows.net\\pkgs\\Daily ETL.ispac", "typeProperties": {"packageName": "DailyETLMain.dtsx"}},
     "File", "DailyETLMain.dtsx", "file:\\\\acct.file.core.windows.net\\pkgs\\Daily ETL.ispac#DailyETLMain.dtsx"),
    ({"type": "PackageStore", "packagePath": "Store1\\Finance\\DailyETLMain"}, "PackageStore", "DailyETLMain.dtsx", "packagestore:Store1\\Finance\\DailyETLMain"),
    ({"type": "InlinePackage", "typeProperties": {"packageName": "Embedded.dtsx", "packageContent": "<x/>"}}, "InlinePackage", "Embedded.dtsx", "inline:PL/Act"),
])
def test_each_package_location_is_read(loc, kind, file, ident):
    ref = ssis.package_ref({"packageLocation": loc}, "PL", "Act")
    assert (ref.location_type, ref.package_file, ref.identity, ref.dynamic) == (kind, file, ident, False)


def test_an_expression_path_is_dynamic_and_never_guessed():
    ref = ssis.package_ref({"packageLocation": {"type": "SSISDB", "packagePath": {"value": "@pipeline().parameters.p", "type": "Expression"}}}, "PL", "Act")
    assert ref.dynamic and ref.path is None
    assert ssis.package_ref({"packageLocation": {"type": "InlinePackage", "typeProperties": {"packageContent": {"value": "@x", "type": "Expression"}}}},
                            "PL", "Act").dynamic


# =====================================================================================================================
# 2. reading it: every plausible embedded encoding, a project, and nothing unsafe
# =====================================================================================================================


@pytest.mark.parametrize("encode, how", [
    (lambda x: x.decode("utf-8"), "xml"),
    (lambda x: base64.b64encode(x).decode(), "base64+xml"),
    (lambda x: base64.b64encode(gzip.compress(x)).decode(), "base64+gzip"),
    (lambda x: base64.b64encode(zlib.compress(x)).decode(), "base64+zlib"),
    (lambda x: base64.b64encode(zlib.compress(x)[2:-4]).decode(), "base64+deflate"),
])
def test_embedded_content_in_any_plausible_encoding(encode, how):
    xml, got = ssis.decode_content(encode(XML))
    assert got == how and xml == XML


def test_unreadable_or_unsafe_content_is_refused_with_a_reason():
    with pytest.raises(ssis.PackageError, match="neither XML nor base64"):
        ssis.decode_content("not a package!")
    with pytest.raises(ssis.PackageError, match="not XML, gzip"):
        ssis.decode_content(base64.b64encode(b"\x00\x01binary").decode())
    with pytest.raises(ssis.PackageError, match="DTD or entities"):
        ssis.analyze_package(b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa">]><x>&a;</x>')
    with pytest.raises(ssis.PackageError, match="not an SSIS package"):
        ssis.analyze_package(b"<root/>")


def test_a_project_gives_its_package_and_its_connection_managers():
    pkg, files = ssis.from_ispac(_ispac(), "DailyETLMain.dtsx")
    assert pkg == XML and sorted(files) == sorted(PROJECT)
    assert ssis.from_ispac(_ispac(), "Other.dtsx")[0] is None


# =====================================================================================================================
# 3. what is in Microsoft's package, and what Fabric cannot run
# =====================================================================================================================


def test_the_sample_package_is_described():
    a, strings = ssis.analyze_package(XML, PROJECT)
    assert a.name == "DailyETLMain" and a.protection == "DontSaveSensitive" and a.format_version == 8
    assert a.executables["Microsoft.ExecuteSQLTask"] == 53 and a.executables["Microsoft.Pipeline"] == 13
    assert a.components == {"Microsoft.OLEDBSource": 13, "Microsoft.OLEDBDestination": 13}
    assert {(c.name, c.scope) for c in a.connections} == {("WWI_Source_DB", "project"), ("WWI_DW_Destination_DB", "project")}
    assert len(a.project_connection_ids) == 2


def test_local_windows_auth_connections_are_flagged_and_an_adf_override_clears_one():
    a, strings = ssis.analyze_package(XML, PROJECT)
    found = {(f.code, f.construct) for f in ssis.package_findings(a, strings, {}, True)}
    assert found == {("ssis_private_source", "WWI_Source_DB"), ("ssis_windows_auth", "WWI_Source_DB"),
                     ("ssis_private_source", "WWI_DW_Destination_DB"), ("ssis_windows_auth", "WWI_DW_Destination_DB")}
    tp = {"projectConnectionManagers": {"WWI_Source_DB": {"ConnectionString": {"value": AZURE_SQL}}},
          "propertyOverrides": {"\\Package.Connections[WWI_DW_Destination_DB].Properties[ConnectionString]": {"value": "@pipeline().parameters.cs"}}}
    a, strings = ssis.analyze_package(XML, PROJECT)
    found = {(f.code, f.construct) for f in ssis.package_findings(a, strings, tp, True)}
    assert found == {("ssis_connection_runtime", "WWI_DW_Destination_DB")}  # the source now points at Azure SQL; the destination is set at run time


def test_without_the_project_its_connections_are_named_as_unchecked():
    a, strings = ssis.analyze_package(XML)
    assert {f.code for f in ssis.package_findings(a, strings, {}, False)} == {"ssis_project_connections_unknown"}


def test_encryption_and_third_party_tasks():
    enc = XML.replace(b'DTS:ProtectionLevel="0"', b'DTS:ProtectionLevel="4"', 1)
    a, s = ssis.analyze_package(enc)
    fs = ssis.package_findings(a, s, {}, False)
    assert [(f.code, f.severity) for f in fs] == [("ssis_encrypted", S.blocker)] and "ADF does not support" in fs[0].message
    third = XML.replace(b'DTS:ExecutableType="Microsoft.ExpressionTask"', b'DTS:ExecutableType="Contoso.Ssis.SftpTask, Contoso.Ssis, Version=1.0.0.0"', 1)
    a, s = ssis.analyze_package(third, PROJECT)
    f = next(x for x in ssis.package_findings(a, s, {}, True) if x.code == "ssis_third_party_task")
    assert f.severity is S.alternative and f.construct == "Contoso.Ssis.SftpTask"  # the version is not part of the name


def test_activity_and_runtime_settings_fabric_does_not_have():
    runtime = {"properties": {"type": "Managed", "typeProperties": {
        "computeProperties": {"location": "westeurope", "vNetProperties": {"subnetId": "/subscriptions/x/subnets/s"}},
        "ssisProperties": {"edition": "Enterprise", "dataProxyProperties": {"connectVia": {"referenceName": "SHIR"}},
                           "expressCustomSetupProperties": [{"type": "ComponentSetup"}]}}}}
    tp = {"runtime": "x86", "environmentPath": "Finance/Prod", "executionCredential": {"domain": "D", "userName": "u", "password": {"type": "SecureString", "value": "**********"}},
          "packageLocation": {"type": "File", "packagePath": "\\\\a.file.core.windows.net\\s\\p.dtsx", "typeProperties": {"configurationPath": "\\\\a.file.core.windows.net\\s\\p.dtsConfig"}},
          "packageParameters": {"pw": {"value": {"type": "AzureKeyVaultSecret", "secretName": "pw", "store": {"referenceName": "KV"}}}}}
    codes = {f.code: f.severity for f in ssis.activity_findings(tp, ssis.package_ref(tp, "PL", "A"), runtime)}
    assert codes == {"ssis_32_bit": S.review, "ssis_environment": S.change, "ssis_windows_execution_credential": S.change,
                     "ssis_configuration_file": S.review, "ssis_key_vault_value": S.review, "ssis_ir_onprem_proxy": S.alternative,
                     "ssis_ir_vnet": S.review, "ssis_ir_custom_setup": S.review, "ssis_ir_enterprise": S.review}


# =====================================================================================================================
# 4. the workload manifest and the converter's placeholder, on a real project
# =====================================================================================================================


def _ssis(name, location, ir="IR_SSIS", **tp):
    return {"name": name, "type": "ExecuteSSISPackage", "typeProperties": {"packageLocation": location,
                                                                        "connectVia": {"referenceName": ir, "type": "IntegrationRuntimeReference"}, **tp}}


def _ir(name, **ssis_props):
    return {"name": name, "properties": {"type": "Managed", "typeProperties": {"computeProperties": {"location": "westeurope"},
                                                                               "ssisProperties": {"edition": "Standard", **ssis_props}}}}


OVERRIDE_BOTH = {"projectConnectionManagers": {cm: {"ConnectionString": {"value": AZURE_SQL}} for cm in ("WWI_Source_DB", "WWI_DW_Destination_DB")}}
ESTATE = {
    "integrationRuntime/IR_SSIS.json": _ir("IR_SSIS"),
    "integrationRuntime/IR_SSIS_Private.json": _ir("IR_SSIS_Private", dataProxyProperties={"connectVia": {"referenceName": "SHIR"}}),
    "pipeline/PL_Ssis.json": {"name": "PL_Ssis", "properties": {"activities": [
        _ssis("Embedded", {"type": "InlinePackage", "typeProperties": {
            "packageName": "DailyETLMain.dtsx", "packageContent": base64.b64encode(gzip.compress(XML)).decode(),
            "childPackages": [{"packagePath": "Child.dtsx", "packageName": "Child.dtsx", "packageContent": XML.decode("utf-8")}]}}),
        _ssis("Project", {"type": "File", "packagePath": "\\\\acct.file.core.windows.net\\pkgs\\Daily ETL.ispac",
                          "typeProperties": {"packageName": "DailyETLMain.dtsx"}}, **OVERRIDE_BOTH),
        _ssis("Catalog", {"type": "SSISDB", "packagePath": "Finance/Daily ETL/Other.dtsx"}, ir="IR_SSIS_Private", environmentPath="Finance/Prod"),
        _ssis("Encrypted", {"type": "InlinePackage", "typeProperties": {
            "packageName": "Locked.dtsx", "packageContent": XML.replace(b'DTS:ProtectionLevel="0"', b'DTS:ProtectionLevel="3"', 1).decode("utf-8")}}),
        _ssis("Dynamic", {"type": "SSISDB", "packagePath": {"value": "@pipeline().parameters.pkg", "type": "Expression"}}),
    ]}},
}


@pytest.fixture(scope="module")
def estate(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("ssis")
    store, pid = _project(tmp, ESTATE)
    _discover(store)
    ispac = tmp / "Daily ETL.ispac"
    ispac.write_bytes(_ispac())
    m = build_manifest(store, pid, Options(generate=True, local_files={"\\\\acct.file.core.windows.net\\pkgs\\Daily ETL.ispac": str(ispac)}), NOW)
    store.save_workload_manifest(pid, m)
    return store, pid, m


def _w(m, used_by):
    return next(w for w in m.workloads if f"PL_Ssis/{used_by}" in w.used_by)


def test_an_embedded_package_is_read_written_with_its_child_and_flagged(estate):
    store, pid, m = estate
    w = _w(m, "Embedded")
    assert w.kind.value == "ssis_package" and w.code.status is C.in_project and "base64+gzip" in w.code.detail
    # an embedded package carries no project files: its project connections are named for review, not guessed
    assert w.classification is K.directly_convertible and {f.code for f in w.findings} == {"ssis_project_connections_unknown"}
    assert w.environment.platform == "azure_ssis" and w.environment.source_ref == "IR_SSIS" and not w.environment.gaps
    main = store.project_dir(pid) / w.artifact.file
    assert main.name == "DailyETLMain.dtsx" and main.read_bytes() == XML and (main.parent / "Child.dtsx").read_bytes() == XML
    assert w.artifact.kind == "ssis_package" and w.artifact.configuration["onelake_folder"] == f"Files/ssis/{main.parent.parent.name}"
    assert w.generation.status == "generated_with_open_issues"
    assert not next(c for c in w.generation.checks if c.name == "fabric_activity_generated").passed  # the activity JSON is not documented
    assert {r.name for r in w.required_resources if r.kind == "file_upload"} == {"DailyETLMain.dtsx", "Child.dtsx"}


def test_a_supplied_project_with_both_connections_overridden_is_directly_convertible(estate):
    _, _, m = estate
    w = _w(m, "Project")
    assert w.code.status is C.supplied and w.classification is K.directly_convertible, w.findings
    # the same package name as the embedded one: each gets its own folder, so neither overwrites the other
    assert w.artifact.configuration["onelake_folder"] != _w(m, "Embedded").artifact.configuration["onelake_folder"]


def test_packages_the_tool_cannot_read_say_how_to_supply_them(estate):
    _, _, m = estate
    cat = _w(m, "Catalog")
    assert cat.code.status is C.not_requested and cat.classification is K.blocked and ".ispac" in cat.code.action and "--source" in cat.code.action
    assert {"ssis_environment", "ssis_ir_onprem_proxy"} <= {f.code for f in cat.findings}
    assert cat.artifact.kind == "none" and cat.generation.status == "not_generated"
    enc = _w(m, "Encrypted")
    assert enc.classification is K.blocked and enc.artifact.kind == "none" and "Re-save" in " ".join(enc.unresolved)
    assert _w(m, "Dynamic").code.status is C.dynamic


def test_no_connection_string_is_kept_and_the_report_reads_as_ssis(estate):
    store, pid, m = estate
    text = json.dumps(store.load_workload_manifest(pid).model_dump(mode="json"))
    assert "Integrated Security" not in text and "User ID=etl" not in text and "wwi.database.windows.net;" not in text
    report = render_report(m)
    assert "Azure-SSIS runtime 'IR_SSIS'" in report and "Invoke SSIS package" in report and "Spark unknown" not in report.split("PL_Ssis/Embedded")[1][:400]


def test_the_converter_states_the_readiness_and_still_generates_no_fabric_activity(estate):
    from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest

    store, pid, _ = estate
    res = ConversionAgent(store).run(ConversionRequest(project_name=pid))
    assert res.conversion is not None, res.error
    acts = {a.source_name: a for p in res.conversion.pipelines for a in p.activities}
    for name in ("Embedded", "Project", "Catalog", "Encrypted", "Dynamic"):
        assert "ssis_invoke_pending" in {i.code for i in acts[name].warnings}, name
    msg = next(i.message for i in acts["Project"].warnings if i.code == "ssis_invoke_pending")
    assert "directly convertible" in msg and "Invoke SSIS package" in msg
    assert acts["Project"].runtime_disposition == "replace" and acts["Catalog"].runtime_disposition == "redesign"
    content = json.loads((store.conversion_dir(pid) / "pipelines" / "PL_Ssis.DataPipeline" / "pipeline-content.json").read_text(encoding="utf-8"))
    assert {a["type"] for a in content["properties"]["activities"]} == {"Fail"}  # placeholders only: never a guessed ExecuteSSISPackage
