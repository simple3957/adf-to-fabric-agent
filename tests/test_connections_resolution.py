"""Linked-service cards, integration runtime analysis, the connector catalog, credential testing and applying connections."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest, parser
from adf2fabric.connections import catalog
from adf2fabric.connections.fabric import FabricApiError
from adf2fabric.connections.integration_runtime import analyze, apply_network_hint, classify_host
from adf2fabric.connections.keyvault import KeyVaultError
from adf2fabric.connections.models import CheckStatus, ConnectionState
from adf2fabric.connections.resolution import ConnectionChoice, build_cards, existing_key
from adf2fabric.connections.service import ConnectionService
from adf2fabric.connections.testing import ConnectorTester
from tests.test_adf_semantic_discovery import _make_project

PW = "S3cretPassw0rd!"
KEY = "k3y+base64/Value=="
SAS = "sv=2022-11-02&sig=Zm9vYmFy"
CLIENT_SECRET = "cl13nt~secret.value"
GUID1, GUID2 = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"


def ref(kind, name, **params):
    r = {"referenceName": name, "type": f"{kind}Reference"}
    if params:
        r["parameters"] = params
    return r


def via(name):
    return {"referenceName": name, "type": "IntegrationRuntimeReference"}


def ls(name, type_, tp=None, ir=None, parameters=None):
    props = {"type": type_, "typeProperties": tp or {}}
    if ir:
        props["connectVia"] = via(ir)
    if parameters:
        props["parameters"] = parameters
    return f"linkedService/{name}.json", {"name": name, "properties": props}


def ds(name, type_, ls_name, tp=None, **ls_args):
    return f"dataset/{name}.json", {"name": name, "properties": {
        "type": type_, "linkedServiceName": ref("LinkedService", ls_name, **ls_args), "typeProperties": tp or {}}}


def copy(name, src, dst):
    return {"name": name, "type": "Copy", "dependsOn": [], "typeProperties": {
        "source": {"type": "AzureSqlSource"}, "sink": {"type": "ParquetSink"}},
        "inputs": [ref("Dataset", src)], "outputs": [ref("Dataset", dst)]}


def estate() -> dict:
    files = dict([
        ls("LS_SQL", "AzureSqlDatabase", {"server": "srv1.database.windows.net", "database": "db1", "userName": "sqluser",
                                          "password": {"type": "SecureString", "value": "**********"}}, ir="IR_AZ"),
        ls("LS_LAKE", "AzureBlobFS", {"url": "https://lake1.dfs.core.windows.net"}, ir="IR_VNET"),
        ls("LS_ONPREM", "SqlServer", {"server": "sqlbox01", "database": "erp"}, ir="IR_SHIR"),
        ls("LS_PARAM", "AzureSqlDatabase", {"connectionString": "Server=srv2.database.windows.net;Database=@{linkedService().DBName}"},
           parameters={"DBName": {"type": "String"}}),
        ls("LS_REST", "RestService", {"url": "https://api.example.com/v1"}),
        ls("LS_COSMOS", "CosmosDb", {"accountEndpoint": "https://c.documents.azure.com"}),
        ds("DS_SQL", "AzureSqlTable", "LS_SQL"),
        ds("DS_RAW", "DelimitedText", "LS_LAKE", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "raw"}}),
        ds("DS_CUR", "Parquet", "LS_LAKE", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "curated"}}),
        ds("DS_ERP", "SqlServerTable", "LS_ONPREM"),
        ds("DS_PA", "AzureSqlTable", "LS_PARAM", DBName="dbA"),
        ds("DS_PB", "AzureSqlTable", "LS_PARAM", DBName="dbB"),
        ds("DS_PX", "AzureSqlTable", "LS_PARAM", DBName={"value": "@pipeline().parameters.db", "type": "Expression"}),
    ])
    files["integrationRuntime/IR_AZ.json"] = {"name": "IR_AZ", "properties": {"type": "Managed", "typeProperties": {
        "computeProperties": {"location": "AutoResolve"}}}}
    files["integrationRuntime/IR_VNET.json"] = {"name": "IR_VNET", "properties": {"type": "Managed", "managedVirtualNetwork": {
        "referenceName": "default", "type": "ManagedVirtualNetworkReference"}, "typeProperties": {}}}
    files["integrationRuntime/IR_SHIR.json"] = {"name": "IR_SHIR", "properties": {"type": "SelfHosted", "typeProperties": {}}}
    files["pipeline/PL_Main.json"] = {"name": "PL_Main", "properties": {"activities": [
        copy("CopySql", "DS_SQL", "DS_RAW"), copy("CopyErp", "DS_ERP", "DS_CUR"), copy("CopyA", "DS_PA", "DS_RAW"),
        copy("CopyB", "DS_PB", "DS_CUR"), copy("CopyX", "DS_PX", "DS_CUR")]}}
    return files


@pytest.fixture
def project(tmp_path):
    store, pid = _make_project(tmp_path, estate())
    d = AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj")).discovery
    return store, pid, d


# --- parser additions -----------------------------------------------------------------------------------------


def test_linked_service_connection_metadata_is_explicit_only_and_never_from_connection_strings(project):
    _, _, d = project
    by = {x.name: x for x in d.linked_services}
    assert (by["LS_SQL"].connection.server, by["LS_SQL"].connection.database, by["LS_SQL"].connection.user_name) == (
        "srv1.database.windows.net", "db1", "sqluser")
    assert by["LS_PARAM"].connection.server is None and by["LS_PARAM"].connection.database is None  # a connection string: not read
    assert by["LS_PARAM"].connection.parameterized_fields == ["connectionString"]
    assert [p.name for p in by["LS_PARAM"].parameters] == ["DBName"] and not by["LS_PARAM"].parameters[0].has_default
    assert "PW" not in d.model_dump_json() and "**********" not in str(by["LS_SQL"].connection)


def test_datasets_record_the_arguments_they_pass_to_their_linked_service(project):
    _, _, d = project
    by = {x.name: x for x in d.datasets}
    assert by["DS_PA"].linked_service_arguments == {"DBName": "dbA"}
    assert isinstance(by["DS_PX"].linked_service_arguments["DBName"], dict)  # an expression is kept as authored


def test_integration_runtimes_are_recorded_with_their_kind_and_network(project):
    _, _, d = project
    by = {r.name: r for r in d.integration_runtimes}
    assert (by["IR_AZ"].kind, by["IR_AZ"].compute_location) == ("azure", "AutoResolve")
    assert (by["IR_VNET"].kind, by["IR_VNET"].managed_virtual_network) == ("managed_vnet", "default")
    assert (by["IR_SHIR"].kind, by["IR_SHIR"].type) == ("self_hosted", "SelfHosted")
    ssis = parser.parse_integration_runtime({"name": "S", "properties": {"type": "Managed", "typeProperties": {"ssisProperties": {}}}}, "p")
    assert ssis.is_ssis and ssis.kind is None
    shared = parser.parse_integration_runtime({"name": "L", "properties": {"type": "SelfHosted", "typeProperties": {
        "linkedInfo": {"resourceId": "/subscriptions/s/.../integrationRuntimes/x", "authorizationType": "RBAC"}}}}, "p")
    assert shared.shared_from.endswith("/integrationRuntimes/x")


# --- host classification + runtime analysis ---------------------------------------------------------------------


@pytest.mark.parametrize("host, exposure", [
    ("10.1.2.3", "private"), ("192.168.0.9", "private"), ("127.0.0.1", "private"), ("8.8.8.8", "public"),
    ("sqlbox01", "private"), ("db.corp", "private"), ("x.privatelink.database.windows.net", "private"),
    ("srv.database.windows.net", "public"), ("lake1.dfs.core.windows.net", "public"), ("api.example.com", "unknown"), (None, "unknown")])
def test_host_classification_is_evidence_based(host, exposure):
    got, evidence = classify_host(host)
    assert got == exposure and evidence


def test_arm_network_hints_refine_the_exposure():
    base = classify_host("srv.database.windows.net")
    assert apply_network_hint(*base, {"public_network_access": "Disabled", "private_endpoints": 2})[0] == "private"
    ex, ev = apply_network_hint(*base, {"public_network_access": "Enabled", "default_action": "Deny"})
    assert ex == "public_restricted" and any("Deny" in e for e in ev)
    assert apply_network_hint(*base, {"public_network_access": "Enabled", "default_action": "Allow"})[0] == "public"
    assert apply_network_hint(*base, None) == base


def analysis(project, name, gateways=None, hint=None):
    _, _, d = project
    runtimes = {r.name.lower(): r for r in d.integration_runtimes}
    return analyze(next(x for x in d.linked_services if x.name == name), runtimes, gateways, hint)


def test_azure_runtime_with_a_public_endpoint_needs_only_a_cloud_connection(project):
    a = analysis(project, "LS_SQL")
    assert (a.runtime_kind, a.exposure, a.fabric_path, a.api_creatable) == ("azure", "public", "cloud_connection", True)
    assert a.host == "srv1.database.windows.net" and not a.missing_info


def test_managed_vnet_maps_to_a_virtual_network_gateway_and_lists_candidates(project):
    gws = [{"id": "g1", "type": "VirtualNetwork", "displayName": "vnet-gw"}, {"id": "g2", "type": "OnPremises", "displayName": "op"}]
    a = analysis(project, "LS_LAKE", gws)
    assert (a.runtime_kind, a.fabric_path, a.api_creatable) == ("managed_vnet", "virtual_network_gateway", False)
    assert a.gateway_candidates == [{"id": "g1", "name": "vnet-gw", "type": "VirtualNetwork"}]
    assert "managed private endpoints" in " ".join(a.requirements + [a.recommendation]) and "default" in " ".join(a.evidence)
    assert any("No virtual network data gateway" in m for m in analysis(project, "LS_LAKE", []).missing_info)


def test_self_hosted_is_never_reused_directly_and_points_to_an_on_premises_gateway(project):
    a = analysis(project, "LS_ONPREM", [{"id": "g2", "type": "OnPremises", "displayName": "op"}])
    assert (a.runtime_kind, a.exposure, a.fabric_path, a.api_creatable) == ("self_hosted", "private", "on_premises_gateway", False)
    assert "cannot be reused" in a.recommendation and a.gateway_candidates[0]["id"] == "g2"
    assert any("cannot be exported" in r for r in a.requirements)
    assert any("No on-premises data gateway" in m for m in analysis(project, "LS_ONPREM", []).missing_info)


def test_a_missing_runtime_definition_and_other_kinds_are_flagged_not_guessed(project):
    _, _, d = project
    lsx = d.linked_services[0].model_copy(update={"integration_runtime": "IR_GONE"})
    a = analyze(lsx, {}, None, None)
    assert (a.runtime_kind, a.resolved, a.fabric_path) == ("unresolved", False, "manual") and "IR_GONE" in a.missing_info[0]
    ssis = parser.parse_integration_runtime({"name": "S", "properties": {"type": "Managed", "typeProperties": {"ssisProperties": {}}}}, "p")
    assert analyze(lsx.model_copy(update={"integration_runtime": "S"}), {"s": ssis}).fabric_path == "unsupported"
    weird = parser.parse_integration_runtime({"name": "W", "properties": {"type": "Other"}}, "p")
    assert analyze(lsx.model_copy(update={"integration_runtime": "W"}), {"w": weird}).fabric_path == "manual"


def test_a_private_endpoint_behind_an_azure_runtime_is_a_conflict_to_review(project):
    a = analysis(project, "LS_SQL", hint={"public_network_access": "Disabled", "private_endpoints": 1})
    assert a.exposure == "private" and a.fabric_path == "virtual_network_gateway" and a.warnings
    restricted = analysis(project, "LS_SQL", hint={"public_network_access": "Enabled", "default_action": "Deny"})
    assert restricted.fabric_path == "cloud_connection" and restricted.requirements


def test_no_connectvia_means_the_implicit_azure_runtime(project):
    a = analysis(project, "LS_REST")
    assert a.runtime_name is None and a.runtime_kind == "azure" and a.exposure == "unknown" and a.missing_info


# --- catalog ----------------------------------------------------------------------------------------------------


def test_methods_are_filtered_by_what_the_target_tenant_accepts():
    spec = catalog.connector_for("AzureSqlDW")
    live = ["Basic", "OAuth2", "ServicePrincipal", "WorkspaceIdentity"]
    avail = {m.method.id: m for m in catalog.methods_for(spec, live)}
    assert all(m.available for m in avail.values())
    limited = {m.method.id: m for m in catalog.methods_for(spec, ["Basic"])}
    assert limited["sql_login"].available and not limited["workspace_identity"].available
    assert "does not accept WorkspaceIdentity" in limited["workspace_identity"].reason
    assert limited["existing_connection"].available  # never hidden
    assert [m.method.id for m in catalog.methods_for(None, None)] == ["existing_connection"]


def test_every_connector_declares_its_methods_fields_and_fabric_parameters():
    ids = {c.id for c in catalog.CONNECTORS}
    assert {"sql", "adls_gen2", "blob", "rest", "databricks"} <= ids
    for c in catalog.CONNECTORS:
        assert c.adf_types and c.fabric_type and c.endpoint and any(m.id == "existing_connection" for m in c.methods)
        for m in c.methods:
            assert m.id and m.label and (m.fabric_credential or m.id == "existing_connection")
            assert all(f.kind in ("text", "secret", "guid", "url", "vault_url", "secret_name") for f in m.fields)
            assert (m.key_vault) == any(f.kind == "vault_url" for f in m.fields)
    sql = {m.id: m for m in catalog.connector_for("sqlserver").methods}
    assert {"workspace_identity", "sql_login", "sql_login_kv", "service_principal", "service_principal_kv"} <= set(sql)
    stor = {m.id for m in catalog.connector_for("azureblobfs").methods}
    assert {"workspace_identity", "account_key", "account_key_kv", "sas_token", "sas_token_kv", "service_principal",
            "service_principal_kv"} <= stor
    assert not catalog.method_by_id(catalog.connector_for("sqlserver"), "entra_oauth").api_creatable


def test_validation_reports_every_problem_without_echoing_secrets():
    spec, m = catalog.connector_for("azuresqldatabase"), None
    m = catalog.method_by_id(spec, "service_principal_kv")
    probs = catalog.validate_choice(spec, m, {}, {"tenant_id": "nope", "client_id": GUID1, "vault_url": "https://evil.com",
                                                  "secret_name": "a/b"}, set())
    text = " ".join(probs)
    assert "Server" in text and "GUID" in text and "Key Vault address" in text and "secret name" in text
    ok_probs = catalog.validate_choice(spec, catalog.method_by_id(spec, "sql_login"), {"server": "s"}, {"username": "u"}, {"password"})
    assert ok_probs == []
    assert catalog.validate_choice(spec, catalog.method_by_id(spec, "sql_login"), {"server": "s"}, {"username": "u"}, set())


@pytest.mark.parametrize("adf, method, values, secrets, cred", [
    ("azuresqldatabase", "sql_login", {"username": "u"}, {"password": PW}, {"credentialType": "Basic", "username": "u", "password": PW}),
    ("azuresqldatabase", "service_principal", {"tenant_id": GUID1, "client_id": GUID2}, {"client_secret": CLIENT_SECRET},
     {"credentialType": "ServicePrincipal", "tenantId": GUID1, "servicePrincipalClientId": GUID2, "servicePrincipalSecret": CLIENT_SECRET}),
    ("azureblobfs", "account_key", {}, {"account_key": KEY}, {"credentialType": "Key", "key": KEY}),
    ("azureblobfs", "sas_token", {}, {"sas_token": "?" + SAS}, {"credentialType": "SharedAccessSignature", "token": SAS}),
    ("azureblobfs", "workspace_identity", {}, {}, {"credentialType": "WorkspaceIdentity"}),
    ("azureblobstorage", "anonymous", {}, {}, {"credentialType": "Anonymous"}),
    ("azuredatabricks", "access_token", {}, {"token": "dapi123"}, {"credentialType": "Key", "key": "dapi123"}),
])
def test_fabric_credential_shapes(adf, method, values, secrets, cred):
    spec = catalog.connector_for(adf)
    body = catalog.build_connection_body(spec, catalog.method_by_id(spec, method),
                                         {"server": "s", "database": "d", "account": "a", "container": "c", "domain": "blob.core.windows.net",
                                          "url": "https://adb.azuredatabricks.net"}, values, secrets, "name")
    assert body["credentialDetails"]["credentials"] == cred and body["connectivityType"] == "ShareableCloud"
    assert body["credentialDetails"]["skipTestConnection"] is False  # Fabric validates the credentials when it creates it


def test_connection_details_follow_fabric_parameter_names():
    sql = catalog.connection_details(catalog.connector_for("azuresqldw"), {"server": "s.sql.azuresynapse.net", "database": "d"})
    assert sql == {"type": "SQL", "creationMethod": "Sql", "parameters": [
        {"dataType": "Text", "name": "server", "value": "s.sql.azuresynapse.net"}, {"dataType": "Text", "name": "database", "value": "d"}]}
    adls = catalog.connection_details(catalog.connector_for("azureblobfs"), {"account": "lake1", "container": "raw"})
    assert adls["type"] == "AzureDataLakeStorage" and adls["parameters"][0]["value"] == "lake1.dfs.core.windows.net"
    assert adls["parameters"][1] == {"dataType": "Text", "name": "path", "value": "raw"}
    blob = catalog.connection_details(catalog.connector_for("azureblobstorage"), {"account": "a", "domain": "blob.core.windows.net"})
    assert [p["name"] for p in blob["parameters"]] == ["account", "domain"]
    with pytest.raises(catalog.ChoiceError, match="cannot be created through the Fabric API"):
        spec = catalog.connector_for("azuresqldw")
        catalog.build_connection_body(spec, catalog.method_by_id(spec, "entra_oauth"), {}, {}, {}, "n")


# --- cards ------------------------------------------------------------------------------------------------------


def card_map(project, **kw):
    _, _, d = project
    return {c.name: c for c in build_cards(d, **kw)}


def test_cards_show_usage_endpoint_values_and_where_each_came_from(project):
    c = card_map(project)
    sql = c["LS_SQL"]
    assert sql.connector == "sql" and sql.used_by_datasets == ["DS_SQL"] and [a["activity"] for a in sql.used_by_activities] == ["CopySql"]
    assert {f.name: (f.value, f.source) for f in sql.endpoint_fields} == {"server": ("srv1.database.windows.net", "source"),
                                                                           "database": ("db1", "source")}
    assert sql.user_name == "sqluser" and sql.state is ConnectionState.resource_discovered
    lake = {f.name: (f.value, f.source, f.candidates) for f in c["LS_LAKE"].endpoint_fields}
    assert lake["account"] == ("lake1", "derived", []) and lake["container"] == (None, "candidate", ["curated", "raw"])
    assert [o["id"] for o in sql.auth_options][:2] == ["existing_connection", "workspace_identity"]


def test_a_parameterized_linked_service_is_never_guessed_and_offers_candidates(project):
    p = card_map(project)["LS_PARAM"]
    assert p.parameterized and p.parameter_candidates == {"DBName": ["dbA", "dbB"]}  # the expression argument is not a candidate
    assert all(f.value is None for f in p.endpoint_fields) and any("Server" in m for m in p.missing)
    assert any("Several values are used for DBName" in w and "one Fabric connection per value" in w for w in p.warnings)
    assert any("connection string" in s for s in p.next_steps)


def test_a_linked_service_with_no_connector_is_manual_with_the_one_supported_route(project):
    c = card_map(project)["LS_COSMOS"]
    assert not c.supported and c.state is ConnectionState.manual_action_required and c.connector is None
    assert [o["id"] for o in c.auth_options] == ["existing_connection"] and "existing Fabric connection" in c.next_steps[0]


def conn(id_, name, type_, path, cred="Basic"):
    return {"id": id_, "displayName": name, "connectionDetails": {"type": type_, "path": path}, "credentialDetails": {"credentialType": cred}}


def test_existing_connections_match_exactly_or_partially_and_credentials_are_compared(project):
    conns = [conn("c1", "SQL exact", "SQL", "srv1.database.windows.net;db1", "Basic"),
             conn("c2", "SQL other db", "SQL", "srv1.database.windows.net;other", "Basic"),
             conn("c3", "Unrelated", "SQL", "else.database.windows.net;x"),
             conn("c4", "Lake", "AzureDataLakeStorage", "https://lake1.dfs.core.windows.net/raw", "Key")]
    c = card_map(project, connections=conns)
    assert [(m.id, m.match) for m in c["LS_SQL"].existing_matches] == [("c1", "exact"), ("c2", "partial")]
    assert "different database" in c["LS_SQL"].existing_matches[1].note
    assert [m.match for m in c["LS_LAKE"].existing_matches] == ["partial"]  # raw container exists, container still unchosen
    chosen = {"ls_sql": ConnectionChoice(linked_service="LS_SQL", method="service_principal")}
    c2 = card_map(project, connections=conns, choices=chosen)
    m = c2["LS_SQL"].existing_matches[0]
    assert m.reusable_with_method is False and "ServicePrincipal" in m.note
    assert existing_key(conns[3]) == ("AzureDataLakeStorage", "lake1.dfs.core.windows.net/raw")


def test_state_follows_the_choice_and_never_claims_tested_without_a_test(project):
    held = {"LS_SQL": []}
    base = ConnectionChoice(linked_service="LS_SQL", method="sql_login", values={"username": "sqluser"})
    st = lambda **kw: card_map(project, choices={"ls_sql": base}, **kw)["LS_SQL"]  # noqa: E731
    assert st().state is ConnectionState.validation_failed and any("Password" in m for m in st().missing)
    assert st(secrets_held={"LS_SQL": ["password"]}).state is ConnectionState.connection_configured
    assert st(secrets_held={"LS_SQL": ["password"]}, states={"LS_SQL": ConnectionState.connection_tested}).state is ConnectionState.connection_tested
    assert st(secrets_held={"LS_SQL": ["password"]}, states={"LS_SQL": ConnectionState.validation_failed}).state is ConnectionState.validation_failed
    assert held


def test_a_gateway_dependent_linked_service_needs_a_manual_existing_connection(project):
    choice = {"ls_onprem": ConnectionChoice(linked_service="LS_ONPREM", method="sql_login", values={"username": "u"})}
    c = card_map(project, choices=choice, secrets_held={"LS_ONPREM": ["password"]})["LS_ONPREM"]
    assert c.state is ConnectionState.manual_action_required and any("gateway" in s for s in c.next_steps)
    reuse = {"ls_onprem": ConnectionChoice(linked_service="LS_ONPREM", method="existing_connection", values={"connection_id": GUID1})}
    assert card_map(project, choices=reuse)["LS_ONPREM"].state is ConnectionState.connection_configured


def test_unavailable_methods_are_marked_not_offered(project):
    c = card_map(project, supported_types={"SQL": ["Basic"]})["LS_SQL"]
    opts = {o["id"]: o for o in c.auth_options}
    assert opts["sql_login"]["available"] and not opts["workspace_identity"]["available"] and opts["workspace_identity"]["reason"]


# --- testing ----------------------------------------------------------------------------------------------------


class Probe:
    def __init__(self, ok=True):
        self.ok, self.calls = ok, []

    def __call__(self, host, port, timeout):
        self.calls.append((host, port))
        return (True, "") if self.ok else (False, "the connection timed out")


def make_tester(**kw):
    kw.setdefault("probe", Probe())
    kw.setdefault("driver", lambda: "ODBC Driver 18 for SQL Server")
    kw.setdefault("sql_connect", lambda cs, t: None)
    return ConnectorTester(**kw)


def sql_spec():
    return catalog.connector_for("azuresqldb" if False else "azuresqldatabase")


def run(t, spec_type, method, endpoint, values=None, secrets=None):
    spec = catalog.connector_for(spec_type)
    return t.test(spec, catalog.method_by_id(spec, method), endpoint, values or {}, secrets or {})


def test_sql_login_is_tested_by_authenticating_and_says_no_query_was_run():
    seen = {}
    t = make_tester(sql_connect=lambda cs, to: seen.setdefault("cs", cs))
    rep = run(t, "azuresqldatabase", "sql_login", {"server": "s.database.windows.net", "database": "d"}, {"username": "u"}, {"password": "p}ss"})
    assert rep.state is ConnectionState.connection_tested and rep.ok
    assert "authenticated to the server and database 'd'" in " ".join(rep.verified) and "no query was run" in " ".join(rep.not_verified)
    assert "PWD={p}}ss}" in seen["cs"] and "Encrypt=yes" in seen["cs"] and "TrustServerCertificate=no" in seen["cs"]
    assert any("reachable from this machine" in v for v in rep.verified) and any("not from Fabric" in n for n in rep.not_verified)


def test_service_principal_sql_uses_azure_ad_authentication():
    seen = {}
    run(make_tester(sql_connect=lambda cs, t: seen.setdefault("cs", cs)), "azuresqldatabase", "service_principal", {"server": "s", "database": "d"},
        {"tenant_id": GUID1, "client_id": GUID2}, {"client_secret": CLIENT_SECRET})
    assert "Authentication=ActiveDirectoryServicePrincipal" in seen["cs"] and f"UID={GUID2}" in seen["cs"]


@pytest.mark.parametrize("method, why", [("workspace_identity", "Fabric's own identity"), ("entra_oauth", "Fabric's own identity")])
def test_identities_that_cannot_be_exercised_here_are_configured_not_tested(method, why):
    rep = run(make_tester(), "azuresqldatabase", method, {"server": "s"})
    assert rep.state is ConnectionState.connection_configured and not rep.ok and why in " ".join(rep.not_verified)


def test_existing_connection_is_never_reported_as_tested():
    rep = run(make_tester(), "azuresqldatabase", "existing_connection", {}, {"connection_id": GUID1})
    assert rep.state is ConnectionState.connection_configured and "cannot be tested from here" in " ".join(rep.not_verified)


@pytest.mark.parametrize("raised, words", [
    (RuntimeError("[28000] Login failed for user 'u'. (18456)"), "rejected the login"),
    (RuntimeError("timeout expired: Could not open a connection to SQL Server"), "could not be reached"),
])
def test_sql_failures_are_actionable_and_never_echo_the_password(raised, words):
    def boom(cs, t):
        raise type(raised)(str(raised) + f" PWD={PW}")

    rep = run(make_tester(sql_connect=boom), "azuresqldatabase", "sql_login", {"server": "s"}, {"username": "u"}, {"password": PW})
    assert rep.state is ConnectionState.validation_failed and words in rep.error and PW not in rep.model_dump_json() and rep.actions


def test_a_missing_odbc_driver_is_a_stated_limitation_not_a_failure():
    rep = run(make_tester(driver=lambda: None), "azuresqldatabase", "sql_login", {"server": "s"}, {"username": "u"}, {"password": "p"})
    assert rep.state is ConnectionState.connection_configured and "no SQL Server ODBC driver" in " ".join(rep.not_verified)


class Fs:
    def __init__(self, fail=None):
        self.fail = fail

    def get_file_system_properties(self):
        if self.fail:
            raise self.fail

    def get_paths(self, max_results=None):
        return iter([])


def datalake(fs, seen):
    def factory(url, cred):
        seen.update(url=url, cred=cred)
        return type("S", (), {"get_file_system_client": lambda self, c: fs})()
    return factory


@pytest.mark.parametrize("method, secrets, expect_cred", [("account_key", {"account_key": KEY}, KEY), ("sas_token", {"sas_token": "?" + SAS}, SAS)])
def test_storage_account_key_and_sas_are_tested_by_listing_the_file_system(method, secrets, expect_cred):
    seen = {}
    rep = run(make_tester(datalake_factory=datalake(Fs(), seen)), "azureblobfs", method, {"account": "lake1", "container": "raw"}, {}, secrets)
    assert rep.ok and seen["url"] == "https://lake1.dfs.core.windows.net" and seen["cred"] == expect_cred
    assert "can read file system 'raw'" in " ".join(rep.verified) and "write permission was not tested" in " ".join(rep.not_verified)


@pytest.mark.parametrize("raised, words", [
    (RuntimeError("AuthorizationPermissionMismatch: This request is not authorized to perform this operation using this permission."), "not authorized"),
    (RuntimeError("ContainerNotFound / FilesystemNotFound 404"), "not found"),
    (RuntimeError("AuthenticationFailed: Server failed to authenticate the request"), "rejected"),
])
def test_storage_failures_map_to_clear_messages_without_leaking_the_key(raised, words):
    rep = run(make_tester(datalake_factory=datalake(Fs(type(raised)(str(raised) + KEY)), {})), "azureblobfs", "account_key",
              {"account": "lake1", "container": "raw"}, {}, {"account_key": KEY})
    assert rep.state is ConnectionState.validation_failed and words in rep.error and KEY not in rep.model_dump_json()


def test_unreachable_endpoint_is_reported_but_does_not_by_itself_fail_the_credential_test():
    rep = run(make_tester(probe=Probe(False)), "azuresqldatabase", "sql_login", {"server": "s"}, {"username": "u"}, {"password": "p"})
    assert rep.state is ConnectionState.connection_tested  # the credential did authenticate (from a machine that could reach it)
    assert any("not reachable from this machine" in n and "does not prove Fabric cannot reach" in n for n in rep.not_verified)


def test_key_vault_methods_read_the_secret_use_it_and_report_failures():
    got = {}
    t = make_tester(secret_reader=lambda vault, name: got.setdefault("v", (vault, name)) and PW, sql_connect=lambda cs, to: got.setdefault("cs", cs))
    rep = run(t, "azuresqldatabase", "sql_login_kv", {"server": "s"}, {"username": "u", "vault_url": "v1.vault.azure.net", "secret_name": "sql-pw"})
    assert rep.ok and got["v"] == ("https://v1.vault.azure.net", "sql-pw") and f"PWD={{{PW}}}" in got["cs"]
    assert "the Key Vault secret can be read" in " ".join(rep.verified) and PW not in rep.model_dump_json()

    def denied(vault, name):
        raise KeyVaultError("You are not allowed to read secrets in x.", ["Grant 'Key Vault Secrets User'."])

    bad_rep = run(make_tester(secret_reader=denied), "azuresqldatabase", "sql_login_kv", {"server": "s"},
                  {"username": "u", "vault_url": "v1.vault.azure.net", "secret_name": "sql-pw"})
    assert bad_rep.state is ConnectionState.validation_failed and bad_rep.actions == ["Grant 'Key Vault Secrets User'."]
    assert run(make_tester(), "azuresqldatabase", "sql_login_kv", {"server": "s"}, {"username": "u", "vault_url": "v1.vault.azure.net",
                                                                           "secret_name": "x"}).state is ConnectionState.validation_failed


def test_incomplete_configuration_fails_before_any_network_call():
    probe = Probe()
    rep = run(make_tester(probe=probe), "azuresqldatabase", "sql_login", {"server": "s"}, {}, {})
    assert rep.state is ConnectionState.validation_failed and not probe.calls
    assert rep.checks[0].status is CheckStatus.failed and any("Password" in a for a in rep.actions)


def test_connectors_without_a_credential_test_say_so():
    rep = run(make_tester(), "restservice", "basic", {"base_url": "https://api.example.com"}, {"username": "u"}, {"password": "p"})
    assert rep.state is ConnectionState.connection_configured and "not exercised by this tool" in " ".join(rep.not_verified)


# --- the service: choices, secrets, tests, apply, report ---------------------------------------------------------


class FakeFabric:
    def __init__(self, existing=None, fail=None, types=None, gateways=None):
        self._existing, self.fail, self.created = existing or [], fail, []
        self._types = types or {"SQL": ["Basic", "ServicePrincipal", "WorkspaceIdentity", "OAuth2"],
                                "AzureDataLakeStorage": ["Key", "SharedAccessSignature", "ServicePrincipal", "WorkspaceIdentity", "OAuth2"]}
        self._gateways = gateways or []

    def supported_connection_types(self):
        return self._types

    def connections(self):
        return self._existing + [{"id": c["id"], "displayName": c["body"]["displayName"], "connectionDetails": {
            "type": c["body"]["connectionDetails"]["type"], "path": ""}} for c in self.created]

    def gateways(self):
        return self._gateways

    def create_connection(self, body):
        if self.fail:
            raise self.fail
        cid = f"00000000-0000-0000-0000-{len(self.created) + 1:012d}"
        self.created.append({"id": cid, "body": body})
        return {"id": cid}


def svc(project, **kw):
    store, pid, _ = project
    return ConnectionService(store, **kw), pid


def sql_choice(**kw):
    return ConnectionChoice(linked_service="LS_SQL", method="sql_login", values={"username": "sqluser"}, **kw)


def test_choices_are_validated_and_persisted_without_secrets(project):
    s, pid = svc(project)
    s.set_choice(pid, sql_choice())
    on_disk = yaml.safe_load((s._dir(pid) / "choices.yaml").read_text(encoding="utf-8"))
    assert on_disk["LS_SQL"]["method"] == "sql_login" and "password" not in json.dumps(on_disk)
    with pytest.raises(catalog.ChoiceError, match="not in this project"):
        s.set_choice(pid, ConnectionChoice(linked_service="Nope", method="sql_login"))
    with pytest.raises(catalog.ChoiceError, match="not an authentication method"):
        s.set_choice(pid, ConnectionChoice(linked_service="LS_SQL", method="telepathy"))
    with pytest.raises(catalog.ChoiceError, match="is a secret"):
        s.set_choice(pid, ConnectionChoice(linked_service="LS_SQL", method="sql_login", values={"password": PW}))
    with pytest.raises(catalog.ChoiceError):
        s.set_choice(pid, ConnectionChoice(linked_service="LS_COSMOS", method="sql_login"))
    s.set_choice(pid, ConnectionChoice(linked_service="LS_COSMOS", method="existing_connection", values={"connection_id": GUID1}))


def test_secrets_are_held_in_memory_only_and_only_for_secret_fields(project):
    s, pid = svc(project)
    s.put_secret("sess", pid, "LS_SQL", "password", PW)
    assert s.vault.get("sess", "linked_service", "LS_SQL", "password") == PW and PW not in repr(s.vault)
    with pytest.raises(catalog.ChoiceError, match="not a secret field"):
        s.put_secret("sess", pid, "LS_SQL", "username", "x")
    with pytest.raises(catalog.ChoiceError):
        s.put_secret("sess", pid, "LS_COSMOS", "password", "x")
    s.set_choice(pid, sql_choice())
    card = {c.name: c for c in s.cards(pid, "sess")}["LS_SQL"]
    assert card.secrets_held == ["password"] and PW not in card.model_dump_json()  # names only, never values
    assert s.clear_secrets("sess") == 1 and not s.vault.has("sess", "linked_service", "LS_SQL", "password")
    assert PW not in "".join(p.read_text(errors="ignore") for p in Path(s.store.project_dir(pid)).rglob("*") if p.is_file())


def test_testing_updates_the_card_state_and_requires_a_choice(project):
    s, pid = svc(project, tester=make_tester())
    assert s.test(pid, "sess", "LS_SQL").state is ConnectionState.not_configured
    s.set_choice(pid, sql_choice())
    s.put_secret("sess", pid, "LS_SQL", "password", PW)
    rep = s.test(pid, "sess", "LS_SQL")
    assert rep.ok and {c.name: c for c in s.cards(pid, "sess")}["LS_SQL"].state is ConnectionState.connection_tested
    s.put_secret("sess", pid, "LS_SQL", "password", PW + "x")  # changing a secret invalidates the earlier test
    assert {c.name: c for c in s.cards(pid, "sess")}["LS_SQL"].state is ConnectionState.connection_configured
    with pytest.raises(catalog.ChoiceError):
        s.test(pid, "sess", "Nope")


def test_apply_creates_connections_binds_them_and_merges_resolutions(project):
    s, pid = svc(project)
    fab = FakeFabric()
    conv = s.store.conversion_dir(pid)
    conv.mkdir(parents=True, exist_ok=True)
    (conv / "resolutions.json").write_text(json.dumps([{"type": "ItemToId", "key": "pipeline:X", "value": GUID1},
                                                       {"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": GUID2}]))
    s.set_choice(pid, sql_choice(display_name="Sql prod"))
    s.put_secret("sess", pid, "LS_SQL", "password", PW)
    r = s.apply(pid, "sess", fab, workspace_id="ws")
    item = next(i for i in r.items if i.linked_service == "LS_SQL")
    assert (item.action, item.bound, item.fabric_validated, item.state) == ("created", True, True, ConnectionState.connection_tested)
    body = fab.created[0]["body"]
    assert body["displayName"] == "Sql prod" and body["credentialDetails"]["credentials"]["password"] == PW
    assert body["credentialDetails"]["skipTestConnection"] is False
    res = json.loads((conv / "resolutions.json").read_text())
    assert {"type": "ItemToId", "key": "pipeline:X", "value": GUID1} in res  # existing entries are preserved
    assert [e["value"] for e in res if e["key"] == "LS_SQL"] == [item.connection_ids[0]]  # the stale binding was replaced
    assert PW not in r.model_dump_json() and PW not in (s._dir(pid) / "apply.yaml").read_text()
    others = {i.linked_service: i for i in r.items if i.linked_service != "LS_SQL"}
    assert all(i.action in ("skipped", "manual") for i in others.values()) and others["LS_COSMOS"].action == "manual"


def test_apply_reuses_an_existing_connection_and_auto_matches_only_exact_endpoints(project):
    s, pid = svc(project)
    existing = [conn(GUID1, "Existing SQL", "SQL", "srv1.database.windows.net;db1"), conn(GUID2, "Partial", "SQL", "srv2.database.windows.net;zz")]
    fab = FakeFabric(existing=existing)
    s.set_choice(pid, ConnectionChoice(linked_service="LS_SQL", method="existing_connection", values={"connection_id": GUID1}))
    s.set_choice(pid, ConnectionChoice(linked_service="LS_REST", method="existing_connection", values={"connection_id": "9" * 8 + "-0000-0000-0000-" + "0" * 12}))
    r = s.apply(pid, "sess", fab)
    by = {i.linked_service: i for i in r.items}
    assert by["LS_SQL"].action == "reused" and by["LS_SQL"].connection_ids == [GUID1] and by["LS_SQL"].bound and not by["LS_SQL"].fabric_validated
    assert "validated when it was created in Fabric, not by this tool" in by["LS_SQL"].detail
    assert by["LS_REST"].action == "failed" and "not found in Fabric" in by["LS_REST"].detail
    (s._dir(pid) / "choices.yaml").unlink()  # forget the explicit choices: only automatic matching remains
    auto = s.apply(pid, "sess", FakeFabric(existing=existing[:1]), auto_match=False)
    assert next(i for i in auto.items if i.linked_service == "LS_SQL").action == "skipped"
    matched = s.apply(pid, "sess", FakeFabric(existing=existing[:1]), auto_match=True)
    m = next(i for i in matched.items if i.linked_service == "LS_SQL")
    assert (m.action, m.origin, m.bound) == ("reused", "auto_matched", True) and "matched automatically" in m.detail
    assert next(i for i in matched.items if i.linked_service == "LS_PARAM").action != "reused"  # no exact match, so never auto-bound


def test_apply_will_not_create_what_needs_a_gateway_or_a_ui_signin(project):
    s, pid = svc(project)
    fab = FakeFabric(gateways=[{"id": "g", "type": "OnPremises", "displayName": "gw"}])
    s.set_choice(pid, ConnectionChoice(linked_service="LS_ONPREM", method="sql_login", values={"username": "u"}))
    s.put_secret("sess", pid, "LS_ONPREM", "password", "p")
    s.set_choice(pid, ConnectionChoice(linked_service="LS_REST", method="entra_oauth"))
    r = s.apply(pid, "sess", fab)
    by = {i.linked_service: i for i in r.items}
    assert by["LS_ONPREM"].action == "manual" and "on-premises data gateway" in by["LS_ONPREM"].detail and by["LS_ONPREM"].actions
    assert by["LS_REST"].action == "manual" and "Create the connection in Fabric" in by["LS_REST"].detail
    assert fab.created == []


def test_apply_dry_run_changes_nothing_and_failures_are_redacted_and_actionable(project):
    s, pid = svc(project)
    s.set_choice(pid, sql_choice())
    s.put_secret("sess", pid, "LS_SQL", "password", PW)
    fab = FakeFabric()
    dry = s.apply(pid, "sess", fab, dry_run=True)
    assert next(i for i in dry.items if i.linked_service == "LS_SQL").action == "would_create" and not fab.created
    assert not (s.store.conversion_dir(pid) / "resolutions.json").exists() and not (s._dir(pid) / "apply.yaml").exists()
    bad = s.apply(pid, "sess", FakeFabric(fail=FabricApiError(f"Fabric returned HTTP 400. DuplicateConnectionName password={PW}", 400)))
    it = next(i for i in bad.items if i.linked_service == "LS_SQL")
    assert it.action == "failed" and PW not in bad.model_dump_json() and any("Rename" in a for a in it.actions)
    assert not it.bound


def test_apply_resolves_key_vault_secrets_through_the_reader_and_reports_a_denied_vault(project):
    calls = []
    s, pid = svc(project, secret_reader=lambda vault, name: calls.append((vault, name)) or PW)
    s.set_choice(pid, ConnectionChoice(linked_service="LS_SQL", method="sql_login_kv",
                                       values={"username": "u", "vault_url": "v1.vault.azure.net", "secret_name": "sql-pw"}))
    fab = FakeFabric()
    r = s.apply(pid, "sess", fab)
    assert calls == [("https://v1.vault.azure.net", "sql-pw")] and fab.created[0]["body"]["credentialDetails"]["credentials"]["password"] == PW
    assert PW not in r.model_dump_json()

    def denied(v, n):
        raise KeyVaultError("You are not allowed to read secrets in x.", ["Grant 'Key Vault Secrets User'."])

    s2 = ConnectionService(s.store, secret_reader=denied)
    bad = next(i for i in s2.apply(pid, "sess", FakeFabric()).items if i.linked_service == "LS_SQL")
    assert bad.action == "failed" and "not allowed" in bad.detail and bad.actions


def test_a_parameterized_linked_service_with_several_values_creates_one_connection_each_and_binds_none(project):
    s, pid = svc(project)
    s.set_choice(pid, ConnectionChoice(linked_service="LS_PARAM", method="sql_login", values={"username": "u"},
                                       endpoint={"server": "srv2.database.windows.net", "database": "dbA"},
                                       variants=[{"database": "dbB"}]))
    s.put_secret("sess", pid, "LS_PARAM", "password", PW)
    fab = FakeFabric()
    it = next(i for i in s.apply(pid, "sess", fab).items if i.linked_service == "LS_PARAM")
    assert it.action == "created" and len(it.connection_ids) == 2 and not it.bound and "none was bound automatically" in it.detail
    dbs = [c["body"]["connectionDetails"]["parameters"][1]["value"] for c in fab.created]
    assert dbs == ["dbA", "dbB"] and fab.created[1]["body"]["displayName"].endswith("-2")
    assert not (s.store.conversion_dir(pid) / "resolutions.json").exists()  # nothing single to bind


def test_the_connection_report_distinguishes_who_decided_and_leaks_nothing(project):
    s, pid = svc(project)
    s.set_choice(pid, sql_choice())
    s.put_secret("sess", pid, "LS_SQL", "password", PW)
    s.apply(pid, "sess", FakeFabric(existing=[conn(GUID1, "Lake", "AzureDataLakeStorage", "https://lake1.dfs.core.windows.net/raw", "Key")]))
    out = s.write_report(pid, "sess")
    md = (s.store.conversion_dir(pid) / out["markdown"]).read_text(encoding="utf-8")
    rows = {r["linked_service"]: r for r in json.loads((s.store.conversion_dir(pid) / out["json"]).read_text(encoding="utf-8"))}
    assert rows["LS_SQL"]["resolution"] == "user-configured" and rows["LS_COSMOS"]["resolution"] == "manual action required"
    assert rows["LS_ONPREM"]["runtime"]["fabric_path"] == "on_premises_gateway" and rows["LS_PARAM"]["resolution"] == "unresolved"
    assert "No secret appears in this report" in md and "## Open items" in md and "gateway" in md
    assert PW not in md and PW not in json.dumps(rows)


def test_workspace_identity_provisioning_reports_success_and_actionable_failure(project):
    s, _ = svc(project)

    class F:
        def provision_identity(self, ws):
            return {"applicationId": GUID1, "servicePrincipalId": GUID2}

    ok_ = s.provision_identity(F(), "ws")
    assert ok_["provisioned"] and ok_["application_id"] == GUID1 and "Grant this identity" in ok_["next"]

    class G:
        def provision_identity(self, ws):
            raise FabricApiError("Permission denied while provisioning.", 403, actions=["Ask an admin."])

    bad = s.provision_identity(G(), "ws")
    assert not bad["provisioned"] and "Admin role" in " ".join(bad["actions"])


def test_service_keeps_an_injected_empty_vault(tmp_path):
    """An empty SecretVault is falsy (it defines __len__); the service must still use the one it was given."""
    from adf2fabric.connections.secret_vault import SecretVault
    from adf2fabric.connections.service import ConnectionService
    from adf2fabric.project.store import ProjectStore

    vault = SecretVault()
    assert ConnectionService(ProjectStore(tmp_path), vault).vault is vault


def test_a_linked_service_can_be_skipped_and_nothing_is_created_or_bound_for_it(project):
    s, pid = svc(project)
    fab = FakeFabric()
    for name in ("LS_SQL", "LS_COSMOS"):  # a supported type and one with no connector
        s.set_choice(pid, ConnectionChoice(linked_service=name, method="skip", values={"x": "y"}))
    on_disk = yaml.safe_load((s._dir(pid) / "choices.yaml").read_text(encoding="utf-8"))
    assert on_disk["LS_SQL"]["method"] == "skip" and on_disk["LS_SQL"]["values"] == {}
    cards = {c.name: c for c in s.cards(pid, "sess")}
    assert cards["LS_SQL"].state is ConnectionState.skipped and cards["LS_COSMOS"].state is ConnectionState.skipped
    r = s.apply(pid, "sess", fab, workspace_id="ws")
    items = {i.linked_service: i for i in r.items}
    assert items["LS_SQL"].action == "skipped" and items["LS_SQL"].state is ConnectionState.skipped
    assert not items["LS_SQL"].bound and not fab.created


def test_skipping_can_be_undone_by_choosing_a_method(project):
    s, pid = svc(project)
    s.set_choice(pid, ConnectionChoice(linked_service="LS_SQL", method="skip"))
    s.set_choice(pid, sql_choice())
    s.put_secret("sess", pid, "LS_SQL", "password", PW)
    assert {c.name: c for c in s.cards(pid, "sess")}["LS_SQL"].state is ConnectionState.connection_configured
