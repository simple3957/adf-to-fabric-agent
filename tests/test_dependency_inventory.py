"""Dependency inventory: resolved, unresolved, circular, inaccessible and partially available dependencies."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from adf2fabric.agent.dependencies import DependencyDiscoveryError, build_inventory, discover_dependencies
from adf2fabric.agent.dependencies import access
from adf2fabric.agent.dependencies.builder import access_key, classify_authentication
from adf2fabric.project.dependencies import AccessStatus, DependencyInventory, DependencyType as T, Resolution as R
from adf2fabric.project.discovery import AdfLinkedService
from adf2fabric.project.source import RetrievalFailure, SourceFinding, SourceManifest, SourceType
from tests.test_discovery_activity_inventory import _discover, _fixture_files, _project

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
SECRET = "SUPERSECRETVALUE123"


def _ls(name, type_, **tp):
    return {"name": name, "properties": {"type": type_, "typeProperties": tp}}


def _ds(name, ls, type_="AzureSqlTable", **tp):
    return {"name": name, "properties": {"type": type_, "linkedServiceName": {"referenceName": ls, "type": "LinkedServiceReference"},
                                         "typeProperties": tp}}


def _call(name, pipeline):
    return {"name": name, "type": "ExecutePipeline",
            "typeProperties": {"pipeline": {"referenceName": pipeline, "type": "PipelineReference"}}}


def _pipe(name, *acts, **props):
    return {"name": name, "properties": {"activities": list(acts), **props}}


def _estate(tmp_path, files):
    store, _ = _project(tmp_path, files)
    return store, _discover(store)


def _manifest(source_type=SourceType.github, failures=(), findings=(), complete=True):
    return SourceManifest(project_id="test-proj", source_type=source_type, source_identifier="x", version="abc123",
                          content_sha256="0" * 64, retrieved_at=NOW, failures=list(failures), findings=list(findings),
                          complete=complete)


def _dep(inv, type_, name):
    return next(d for d in inv.dependencies if d.type is type_ and d.name.lower() == name.lower())


BASE = {
    "linkedService/LS_Sql.json": _ls("LS_Sql", "AzureSqlDatabase", connectionString="Server=tcp:srv.database.windows.net;Database=d"),
    "dataset/DS_T.json": _ds("DS_T", "LS_Sql", schema="dbo", table="t"),
    "pipeline/Child.json": _pipe("Child", {"name": "W", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}),
    "pipeline/Parent.json": _pipe("Parent", _call("Run", "Child"), parameters={"p": {"type": "String"}}),
}


# --- resolved ---------------------------------------------------------------------------------------------------------------------


def test_resolved_references_carry_type_source_consumers_and_implication(tmp_path):
    _, d = _estate(tmp_path, BASE)
    inv = build_inventory(d, _manifest(), now=NOW)
    child = _dep(inv, T.pipeline, "Child")
    assert child.resolution is R.resolved and child.defined_in == "pipeline/Child.json"
    ref = next(r for r in child.references if r.consumer_kind == "activity")
    assert ref.consumer == "Parent/Run" and ref.role == "executePipeline" and ref.file == "pipeline/Parent.json" and ref.pointer
    ls = _dep(inv, T.linked_service, "LS_Sql")
    assert ls.consumers == ["dataset:DS_T"]
    assert ls.migration.category.value == "needs_connection"
    assert ls.authentication.mode in ("unknown", "password", "encrypted") and ls.authentication.needs_user_input
    assert not inv.findings and inv.summary.blocking == 0


def test_implicit_auto_resolve_runtime_is_not_a_missing_dependency(tmp_path):
    files = dict(BASE)
    files["linkedService/LS_Sql.json"] = {**BASE["linkedService/LS_Sql.json"], "properties": {
        **BASE["linkedService/LS_Sql.json"]["properties"],
        "connectVia": {"referenceName": "AutoResolveIntegrationRuntime", "type": "IntegrationRuntimeReference"}}}
    _, d = _estate(tmp_path, files)
    inv = build_inventory(d, _manifest(), now=NOW)
    assert _dep(inv, T.integration_runtime, "AutoResolveIntegrationRuntime").resolution is R.resolved
    assert not [f for f in inv.findings if f.code == "missing_file"]


# --- unresolved / missing file / expressions ---------------------------------------------------------------------------------------


def test_missing_definitions_are_unresolved_with_the_expected_file(tmp_path):
    files = {"pipeline/P.json": _pipe("P", _call("Run", "Ghost"), {
        "name": "Look", "type": "Lookup", "typeProperties": {
            "source": {"type": "AzureSqlSource"},
            "dataset": {"referenceName": "DS_Ghost", "type": "DatasetReference"}}})}
    _, d = _estate(tmp_path, files)
    inv = build_inventory(d, _manifest(), now=NOW)
    ghost = _dep(inv, T.pipeline, "Ghost")
    assert ghost.resolution is R.unresolved and ghost.migration.blocks and ghost.migration.category.value == "missing"
    assert _dep(inv, T.dataset, "DS_Ghost").resolution is R.unresolved
    codes = {(f.code, f.subject) for f in inv.findings}
    assert ("missing_file", "Ghost") in codes and ("missing_file", "DS_Ghost") in codes
    po = {o.pipeline: o for o in inv.pipeline_order}
    assert not po["P"].ready and po["P"].blocked_by == ["dataset:ds_ghost", "pipeline:ghost"]


def test_undeclared_parameters_variables_globals_and_outputs_are_broken_references(tmp_path):
    act = {"name": "S", "type": "SetVariable", "typeProperties": {
        "variableName": "nope", "value": {"type": "Expression", "value": "@concat(pipeline().parameters.missing, "
                                          "pipeline().globalParameters.gone, activity('NoSuch').output.x, variables('v2'))"}}}
    _, d = _estate(tmp_path, {"pipeline/P.json": _pipe("P", act, variables={"v2": {"type": "String"}})})
    inv = build_inventory(d, _manifest(), now=NOW)
    bad = {(x.type.value, x.name) for x in inv.dependencies if x.resolution is R.unresolved}
    assert ("pipeline_parameter", "P.missing") in bad and ("global_parameter", "gone") in bad
    assert ("activity_output", "P/NoSuch") in bad and ("variable", "P.nope") in bad
    assert ("variable", "P.v2") not in bad
    assert all(f.code == "broken_reference" for f in inv.findings if f.severity.value == "error")


def test_computed_targets_are_dynamic_not_missing(tmp_path):
    nb = {"name": "N", "type": "DatabricksNotebook", "linkedServiceName": {"referenceName": "LS_Dbx", "type": "LinkedServiceReference"},
          "typeProperties": {"notebookPath": {"value": "@pipeline().parameters.nb", "type": "Expression"}}}
    files = {"linkedService/LS_Dbx.json": _ls("LS_Dbx", "AzureDatabricks", domain="https://adb-1.azuredatabricks.net"),
             "pipeline/P.json": _pipe("P", nb, parameters={"nb": {"type": "String"}})}
    _, d = _estate(tmp_path, files)
    inv = build_inventory(d, _manifest(), now=NOW)
    nbs = [x for x in inv.dependencies if x.type is T.databricks_notebook]
    assert len(nbs) == 1 and nbs[0].resolution is R.dynamic and nbs[0].migration.category.value == "manual"
    assert any(f.code == "unresolved_expression" for f in inv.findings)


# --- circular ---------------------------------------------------------------------------------------------------------------------


def test_circular_pipeline_calls_are_reported_and_ordering_still_terminates(tmp_path):
    files = {"pipeline/A.json": _pipe("A", _call("c", "B")), "pipeline/B.json": _pipe("B", _call("c", "C")),
             "pipeline/C.json": _pipe("C", _call("c", "A")), "pipeline/Self.json": _pipe("Self", _call("c", "Self")),
             "pipeline/Leaf.json": _pipe("Leaf", {"name": "W", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}),
             "pipeline/Top.json": _pipe("Top", _call("c", "A"), _call("d", "Leaf"))}
    _, d = _estate(tmp_path, files)
    inv = build_inventory(d, _manifest(), now=NOW)
    assert inv.cycles == [["pipeline:a", "pipeline:b", "pipeline:c"], ["pipeline:self"]]
    assert _dep(inv, T.pipeline, "A").resolution is R.circular and _dep(inv, T.pipeline, "Leaf").resolution is R.resolved
    po = {o.pipeline: o for o in inv.pipeline_order}
    assert po["A"].in_cycle and po["Self"].in_cycle and not po["Leaf"].in_cycle
    assert po["A"].wave == po["B"].wave == po["C"].wave and po["Leaf"].wave < po["Top"].wave
    assert po["A"].wave < po["Top"].wave and not po["Top"].ready and po["Leaf"].ready
    assert {f.code for f in inv.findings} >= {"circular_dependency"}


def test_flowlet_cycles_are_found(tmp_path):
    keep = ("dataflow/DF_Loop", "dataset", "linkedService")
    _, d = _estate(tmp_path, {k: v for k, v in _fixture_files().items() if k.startswith(keep)})
    inv = build_inventory(d, _manifest(), now=NOW)
    assert inv.cycles == [["data_flow:df_loop_a", "data_flow:df_loop_b"]]
    assert _dep(inv, T.data_flow, "DF_Loop_A").resolution is R.circular


# --- inaccessible / partially available -----------------------------------------------------------------------------------------


def test_retrieval_failures_make_references_unavailable_not_missing_and_name_the_permission(tmp_path):
    files = {"pipeline/P.json": _pipe("P", {"name": "L", "type": "Lookup", "typeProperties": {
        "source": {"type": "AzureSqlSource"}, "dataset": {"referenceName": "DS_X", "type": "DatasetReference"}}})}
    _, d = _estate(tmp_path, files)
    m = _manifest(SourceType.azure_adf, failures=[RetrievalFailure(
        kind="dataset", status_code=403, error_code="AuthorizationFailed", error="denied", action="Grant Reader.")], complete=False)
    inv = build_inventory(d, m, now=NOW)
    x = _dep(inv, T.dataset, "DS_X")
    assert x.resolution is R.unavailable and x.migration.category.value == "missing"
    gap = inv.source_gaps[0]
    assert gap.reason == "denied" and gap.permission == "Microsoft.DataFactory/factories/datasets/read" and not inv.source.complete
    assert any(f.code == "source_unavailable" for f in inv.findings) and not any(f.code == "missing_file" for f in inv.findings)


def test_unparseable_artifact_file_makes_a_reference_to_it_unavailable(tmp_path):
    files = dict(BASE)
    files["dataset/DS_T.json"] = "{ not json"
    _, d = _estate(tmp_path, files)
    inv = build_inventory(d, _manifest(findings=[SourceFinding(severity="error", code="invalid_json", message="bad", artifact="dataset/DS_T.json")]), now=NOW)
    assert any(g.reason == "unprocessed" for g in inv.source_gaps)


def test_partial_source_still_orders_what_it_has(tmp_path):
    _, d = _estate(tmp_path, BASE)
    m = _manifest(SourceType.azure_adf, failures=[RetrievalFailure(kind="trigger", status_code=403, error="denied", action="x")], complete=False)
    inv = build_inventory(d, m, now=NOW)
    assert [o.pipeline for o in inv.pipeline_order] == ["Child", "Parent"] and not inv.source.complete


# --- access checks: separate, least privilege, no secrets ------------------------------------------------------------------------


@pytest.fixture
def ext(tmp_path):
    kv = _ls("LS_KV", "AzureKeyVault", baseUrl="https://myvault.vault.azure.net/")
    sql = {"name": "LS_Sql2", "properties": {"type": "AzureSqlDatabase", "typeProperties": {
        "connectionString": {"type": "AzureKeyVaultSecret", "store": {"referenceName": "LS_KV", "type": "LinkedServiceReference"}, "secretName": "sqlcs"},
        "password": {"type": "SecureString", "value": SECRET}}}}
    dbx = _ls("LS_Dbx", "AzureDatabricks", domain="https://adb-1.azuredatabricks.net")
    nb = {"name": "N", "type": "DatabricksNotebook", "linkedServiceName": {"referenceName": "LS_Dbx", "type": "LinkedServiceReference"},
          "typeProperties": {"notebookPath": "/Shared/etl/clean"}}
    files = {"linkedService/LS_KV.json": kv, "linkedService/LS_Sql2.json": sql, "linkedService/LS_Dbx.json": dbx,
             "pipeline/P.json": _pipe("P", nb)}
    _, d = _estate(tmp_path, files)
    return build_inventory(d, _manifest(), now=NOW), d


class Recorder:
    def __init__(self, status=200, body=None, fail=None):
        self.calls, self.status, self.body, self.fail = [], status, body, fail

    def __call__(self, method, url, headers):
        self.calls.append((method, url, dict(headers)))
        if self.fail:
            raise self.fail
        return self.status, self.body


def _tokens(**kw):
    return access.TokenSource(env_names={k: v for k, v in kw.items()}, allow_az=False)


def test_default_run_contacts_nothing_and_says_so(ext):
    inv, _ = ext
    checks = access.run_checks(inv.dependencies, [], _tokens(), Recorder(fail=AssertionError("no network")),
                               lambda h, p: (_ for _ in ()).throw(AssertionError("no network")), NOW)
    assert checks == {}
    assert {x.access.status for x in inv.dependencies if x.access} == {AccessStatus.not_checked}


def test_missing_token_is_reported_per_provider_and_az_login_is_not_assumed(ext, monkeypatch):
    inv, d = ext
    monkeypatch.delenv("ADF2FABRIC_DATABRICKS_TOKEN", raising=False)
    rec = Recorder()
    checks = access.run_checks(inv.dependencies, ["databricks", "key_vault"], access.TokenSource(), rec, None, NOW)
    assert rec.calls == []  # no token, so no request is sent
    out = build_inventory(d, _manifest(), checks, ["databricks", "key_vault"], NOW)
    nb = next(x for x in out.dependencies if x.type is T.databricks_notebook)
    assert nb.access.status is AccessStatus.missing_credential and "ADF2FABRIC_DATABRICKS_TOKEN" in nb.access.detail
    assert nb.migration.category.value == "needs_access"
    assert any(f.code == "inaccessible_external" for f in out.findings)


def test_databricks_probe_reads_metadata_only_and_names_the_permission(ext, monkeypatch):
    inv, d = ext
    monkeypatch.setenv("DBX_T", "tok-123")
    rec = Recorder(status=403)
    checks = access.run_checks(inv.dependencies, ["databricks"], _tokens(databricks="DBX_T"), rec, None, NOW)
    assert [(m, u.split("?")[0]) for m, u, _ in rec.calls] == [("GET", "https://adb-1.azuredatabricks.net/api/2.0/workspace/get-status")]
    assert "export" not in rec.calls[0][1]
    out = build_inventory(d, _manifest(), checks, ["databricks"], NOW)
    nb = next(x for x in out.dependencies if x.type is T.databricks_notebook)
    assert nb.access.status is AccessStatus.denied and "CAN READ" in nb.access.missing and "/Shared/etl/clean" in nb.access.missing
    assert "tok-123" not in out.model_dump_json()


def test_key_vault_probe_lists_versions_and_never_requests_the_value(ext, monkeypatch):
    inv, d = ext
    monkeypatch.setenv("KV_T", "kv-tok")
    rec = Recorder(status=200)
    checks = access.run_checks(inv.dependencies, ["key_vault"], _tokens(key_vault="KV_T"), rec, None, NOW)
    url = rec.calls[0][1]
    assert url.startswith("https://myvault.vault.azure.net/secrets/sqlcs/versions") and "/versions" in url
    assert not url.split("?")[0].endswith("sqlcs")  # never the bare secret URL, which returns the value
    out = build_inventory(d, _manifest(), checks, ["key_vault"], NOW)
    s = next(x for x in out.dependencies if x.type is T.key_vault_secret)
    assert s.access.status is AccessStatus.accessible and s.access.permission


@pytest.mark.parametrize("status,expected", [(401, AccessStatus.unauthenticated), (403, AccessStatus.denied),
                                             (404, AccessStatus.not_found), (500, AccessStatus.error)])
def test_http_statuses_map_to_distinct_outcomes(ext, monkeypatch, status, expected):
    inv, _ = ext
    monkeypatch.setenv("KV_T", "kv-tok")
    checks = access.run_checks(inv.dependencies, ["key_vault"], _tokens(key_vault="KV_T"), Recorder(status=status), None, NOW)
    assert [c.status for c in checks.values()] == [expected]


def test_unreachable_and_missing_package_are_distinguished(ext, monkeypatch):
    inv, _ = ext
    monkeypatch.setenv("KV_T", "kv-tok")
    c1 = access.run_checks(inv.dependencies, ["key_vault"], _tokens(key_vault="KV_T"), Recorder(fail=OSError("x")), None, NOW)
    c2 = access.run_checks(inv.dependencies, ["key_vault"], _tokens(key_vault="KV_T"), Recorder(fail=ModuleNotFoundError("requests")), None, NOW)
    assert [c.status for c in c1.values()] == [AccessStatus.unreachable]
    assert [c.status for c in c2.values()] == [AccessStatus.missing_dependency]


def test_one_provider_failing_does_not_hide_or_imply_another(ext, monkeypatch):
    inv, d = ext
    monkeypatch.setenv("KV_T", "kv-tok")
    monkeypatch.delenv("ADF2FABRIC_DATABRICKS_TOKEN", raising=False)
    checks = access.run_checks(inv.dependencies, ["key_vault", "databricks"], _tokens(key_vault="KV_T"), Recorder(status=200), None, NOW)
    by = {c.provider: c.status for c in checks.values()}
    assert by == {"key_vault": AccessStatus.accessible, "databricks": AccessStatus.missing_credential}


def test_network_probe_sends_nothing_and_classifies_dns_vs_refused():
    import socket

    ok = access.check_network("srv.database.windows.net", 1433, lambda h, p: None, NOW)
    dns = access.check_network("srv.database.windows.net", 1433, lambda h, p: (_ for _ in ()).throw(socket.gaierror()), NOW)
    refused = access.check_network("srv.database.windows.net", 1433, lambda h, p: (_ for _ in ()).throw(ConnectionRefusedError()), NOW)
    bad = access.check_network("not a host!", 1433, lambda h, p: None, NOW)
    assert ok.status is AccessStatus.accessible and "no login" in ok.detail
    assert dns.status is AccessStatus.unreachable and "DNS" in dns.missing and "private DNS" in dns.detail
    assert refused.status is AccessStatus.unreachable and "port" in refused.missing
    assert bad.status is AccessStatus.not_configured


def test_probes_are_deduplicated_per_target(ext, monkeypatch):
    inv, _ = ext
    monkeypatch.setenv("DBX_T", "t")
    rec = Recorder()
    access.run_checks(inv.dependencies, ["databricks", "key_vault", "network"], _tokens(databricks="DBX_T", key_vault="DBX_T"), rec,
                      lambda h, p: None, NOW)
    urls = [u for _, u, _ in rec.calls]
    assert len(urls) == len(set(urls))


# --- no secrets, no data ---------------------------------------------------------------------------------------------------------


def test_secret_values_never_reach_the_record_or_the_report(ext, tmp_path):
    inv, d = ext
    from adf2fabric.agent.dependencies.report import render_report

    blob = inv.model_dump_json() + render_report(inv)
    assert SECRET not in blob
    ls = _dep(inv, T.linked_service, "LS_Sql2")
    assert ls.authentication.key_vault_secrets == ["LS_KV/sqlcs"] and "password" in ls.authentication.secret_fields


def test_redact_removes_tokens_and_signatures():
    text = "Bearer abc.def.ghi sig=AAAA token=BBBB eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9xxxxxxxxxx"
    out = access.redact(text)
    assert all(s not in out for s in ("abc.def.ghi", "AAAA", "BBBB", "eyJhbGci"))


def test_classify_authentication_modes():
    def mk(**kw):
        return AdfLinkedService(name="x", path="linkedService/x.json", **kw)

    assert classify_authentication(mk(authentication_type="ManagedIdentity")).mode == "managed_identity"
    assert classify_authentication(mk(connection_field_names=["accountKey"])).mode == "account_key"
    enc = classify_authentication(mk(connection_field_names=["encryptedCredential"]))
    assert enc.needs_user_input and any("encryptedCredential" in n for n in enc.notes)


# --- adapters share one engine; persistence; ordering ----------------------------------------------------------------------------


@pytest.mark.parametrize("source_type", list(SourceType))
def test_the_inventory_is_identical_whichever_adapter_produced_the_source(tmp_path, source_type):
    _, d = _estate(tmp_path, BASE)
    base = build_inventory(d, _manifest(SourceType.github), now=NOW)
    other = build_inventory(d, _manifest(source_type), now=NOW)
    a, b = base.model_dump(mode="json"), other.model_dump(mode="json")
    assert a.pop("source")["source_type"] == "github" and b.pop("source")["source_type"] == source_type.value
    assert a == b


def test_builder_is_deterministic_and_idempotent(tmp_path):
    _, d = _estate(tmp_path, BASE)
    one = build_inventory(d, _manifest(), now=NOW)
    two = build_inventory(d, _manifest(), now=NOW, previous=one)
    assert one.model_dump_json() == two.model_dump_json()


def test_discover_saves_record_and_report_and_requires_semantic_discovery(tmp_path):
    store, pid = _project(tmp_path, BASE)
    with pytest.raises(DependencyDiscoveryError):
        discover_dependencies(store, pid)
    _discover(store)
    with pytest.raises(DependencyDiscoveryError):
        discover_dependencies(store, pid, ["nope"])
    inv = discover_dependencies(store, pid, now=NOW)
    assert isinstance(store.load_dependency_inventory(pid), DependencyInventory)
    assert store.dependency_report_path(pid).read_text(encoding="utf-8").startswith("# Dependency report")
    again = discover_dependencies(store, pid)
    assert again.created_at == inv.created_at


def test_waves_put_children_before_callers_and_triggers_last(tmp_path):
    files = dict(BASE)
    files["trigger/T.json"] = {"name": "T", "properties": {"type": "ScheduleTrigger", "pipelines": [
        {"pipelineReference": {"referenceName": "Parent", "type": "PipelineReference"}}],
        "typeProperties": {"recurrence": {"frequency": "Day", "interval": 1}}}}
    _, d = _estate(tmp_path, files)
    inv = build_inventory(d, _manifest(), now=NOW)
    names = [w.name for w in inv.waves]
    assert names.index("Connections") < names.index("Pipelines (level 0)") < names.index("Pipelines (level 1)") < names.index("Triggers")
    po = {o.pipeline: o for o in inv.pipeline_order}
    assert po["Child"].position < po["Parent"].position and po["Parent"].calls == ["Child"] and po["Child"].called_by == ["Parent"]


def test_blocking_dependency_propagates_to_callers_in_order(tmp_path):
    files = {"pipeline/Leaf.json": _pipe("Leaf", _call("x", "Missing")), "pipeline/Top.json": _pipe("Top", _call("c", "Leaf"))}
    _, d = _estate(tmp_path, files)
    po = {o.pipeline: o for o in build_inventory(d, _manifest(), now=NOW).pipeline_order}
    assert not po["Leaf"].ready and not po["Top"].ready and po["Top"].blocked_by == []
