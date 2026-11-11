"""Linked-service cards, integration runtime analysis, the connector catalog, credential testing and applying connections."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest, parser
from adf2fabric.connections import catalog
from adf2fabric.connections.integration_runtime import analyze, apply_network_hint, classify_host
from adf2fabric.connections.models import ConnectionState
from adf2fabric.connections.resolution import ConnectionChoice, build_cards, existing_key
from adf2fabric.connections.service import ConnectionService
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


def run(t, spec_type, method, endpoint, values=None, secrets=None):
    spec = catalog.connector_for(spec_type)
    return t.test(spec, catalog.method_by_id(spec, method), endpoint, values or {}, secrets or {})


def datalake(fs, seen):
    def factory(url, cred):
        seen.update(url=url, cred=cred)
        return type("S", (), {"get_file_system_client": lambda self, c: fs})()
    return factory


# --- the service: choices, secrets, tests, apply, report ---------------------------------------------------------


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


def test_service_keeps_an_injected_empty_vault(tmp_path):
    """An empty SecretVault is falsy (it defines __len__); the service must still use the one it was given."""
    from adf2fabric.connections.secret_vault import SecretVault
    from adf2fabric.connections.service import ConnectionService
    from adf2fabric.project.store import ProjectStore

    vault = SecretVault()
    assert ConnectionService(ProjectStore(tmp_path), vault).vault is vault


def test_skipping_a_linked_service_removes_its_earlier_binding(project):
    # found live (2026-11-10): the card said "no connection will be bound" while convert still bound the earlier connection
    import json

    s, pid = svc(project)
    res = s.store.conversion_dir(pid) / "resolutions.json"
    res.parent.mkdir(parents=True, exist_ok=True)
    res.write_text(json.dumps([{"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": "old"},
                               {"type": "LinkedServiceToConnectionId", "key": "LS_SQL?db=x", "value": "old2"},
                               {"type": "LinkedServiceToConnectionId", "key": "LS_SQL2", "value": "keep"},
                               {"type": "WorkspaceId", "key": "*", "value": "ws"}]), encoding="utf-8")
    s.set_choice(pid, ConnectionChoice(linked_service="LS_SQL", method="skip"))
    assert json.loads(res.read_text(encoding="utf-8")) == [{"type": "LinkedServiceToConnectionId", "key": "LS_SQL2", "value": "keep"},
                                                           {"type": "WorkspaceId", "key": "*", "value": "ws"}]


def test_skipping_can_be_undone_by_choosing_a_method(project):
    s, pid = svc(project)
    s.set_choice(pid, ConnectionChoice(linked_service="LS_SQL", method="skip"))
    s.set_choice(pid, sql_choice())
    s.put_secret("sess", pid, "LS_SQL", "password", PW)
    assert {c.name: c for c in s.cards(pid, "sess")}["LS_SQL"].state is ConnectionState.connection_configured


# --- why the "existing connection" list is empty -----------------------------------------------------------------------------


def test_a_damaged_resolutions_file_is_kept_aside_not_overwritten_with_nothing(project):
    """The merge used to treat unreadable JSON as empty and write only the new binding, silently dropping every notebook and item binding."""
    s, pid = svc(project)
    path = s.store.conversion_dir(pid) / "resolutions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[ {\"type\": \"ItemToId\", ", encoding="utf-8")  # half a write
    s._merge_resolutions(pid, {"LS_SQL": GUID1})
    kept = list(path.parent.glob("resolutions.json.damaged-*"))
    assert len(kept) == 1 and "ItemToId" in kept[0].read_text(encoding="utf-8")  # evidence for a person
    assert [e["key"] for e in json.loads(path.read_text(encoding="utf-8"))] == ["LS_SQL"]


def test_a_url_connection_on_another_host_is_not_offered_as_a_partial_match():
    # found live (2026-11-10): every Databricks connection was offered for any Databricks linked service, because only "https:"
    # (the text before the first "/") was compared as the server
    from adf2fabric.connections import catalog
    from adf2fabric.connections.resolution import EndpointField, match_existing

    spec = next(s for s in catalog.CONNECTORS if s.id == "databricks")
    fields = [EndpointField(name="url", label="Workspace URL", value="https://adb-7405617008097962.2.azuredatabricks.net")]
    conns = [{"id": "old", "displayName": "old", "connectionDetails": {"type": "AzureDatabricksWorkspace", "path": "https://adb-7405618998257241.1.azuredatabricks.net"}},
             {"id": "same", "displayName": "same", "connectionDetails": {"type": "AzureDatabricksWorkspace", "path": "https://adb-7405617008097962.2.azuredatabricks.net/"}},
             {"id": "sub", "displayName": "sub", "connectionDetails": {"type": "AzureDatabricksWorkspace", "path": "https://adb-7405617008097962.2.azuredatabricks.net/x"}}]
    assert [(m.id, m.match) for m in match_existing(spec, fields, conns, None)] == [("same", "exact"), ("sub", "partial")]
