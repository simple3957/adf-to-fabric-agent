"""Demo mode: the same routes and the same real discovery / card / report logic, with canned providers and no network.

Demo swaps ONLY the providers (Azure CLI, Fabric, Git); projects, parsing, resolution and persistence are the real code running
on a bundled sample estate in a throwaway store, so what the UI shows is genuine output for sample data. Nothing here touches Azure,
Fabric, git remotes or the real project store, and no secret is ever read.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from adf2fabric.api.container import Container, Session, Settings
from adf2fabric.connections.fabric import FabricService
from adf2fabric.connections.models import (
    ArmResource,
    AzureAccount,
    Check,
    CheckStatus,
    ConnectionState,
    FabricWorkspace,
    ProviderStatus,
    Subscription,
    TestReport,
)
from adf2fabric.connections.providers._login import SUCCEEDED, LoginAttempt
from adf2fabric.connections.testing import ConnectorTester
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.store import ProjectStore
from adf2fabric.repository import git

DEMO_PROJECT = "demo-estate"
DEMO_ACCOUNT, DEMO_TENANT = "demo.user@contoso.example", "00000000-0000-0000-0000-00000000d3a0"
DEMO_SUB = "00000000-0000-0000-0000-0000000005b1"
DEMO_WS = "00000000-0000-0000-0000-00000000aa01"


def _ls(name, type_, tp=None, ir=None, params=None):
    props: dict[str, Any] = {"type": type_, "typeProperties": tp or {}}
    if ir:
        props["connectVia"] = {"referenceName": ir, "type": "IntegrationRuntimeReference"}
    if params:
        props["parameters"] = params
    return f"linkedService/{name}.json", {"name": name, "properties": props}


def _ds(name, type_, ls_name, tp=None, **args):
    ref = {"referenceName": ls_name, "type": "LinkedServiceReference", **({"parameters": args} if args else {})}
    return f"dataset/{name}.json", {"name": name, "properties": {"type": type_, "linkedServiceName": ref, "typeProperties": tp or {}}}


def _copy(name, src, dst):
    return {"name": name, "type": "Copy", "dependsOn": [], "typeProperties": {
        "source": {"type": "AzureSqlSource"}, "sink": {"type": "ParquetSink"}},
        "inputs": [{"referenceName": src, "type": "DatasetReference"}], "outputs": [{"referenceName": dst, "type": "DatasetReference"}]}


def demo_estate() -> dict[str, dict]:
    files = dict([
        _ls("LS_Sales_SQL", "AzureSqlDatabase", {"server": "contoso-sales.database.windows.net", "database": "sales", "userName": "etl_user"},
            ir="IR_Azure"),
        _ls("LS_Lake", "AzureBlobFS", {"url": "https://contosolake.dfs.core.windows.net"}, ir="IR_ManagedVnet"),
        _ls("LS_ERP_OnPrem", "SqlServer", {"server": "erpsql01", "database": "erp"}, ir="IR_SelfHosted"),
        _ls("LS_Tenant_DB", "AzureSqlDatabase", {"connectionString": "Server=contoso-t.database.windows.net;Database=@{linkedService().DBName}"},
            params={"DBName": {"type": "String"}}),
        _ls("LS_Partner_API", "RestService", {"url": "https://api.partner.example/v2"}),
        _ls("LS_Cosmos", "CosmosDb", {"accountEndpoint": "https://contoso.documents.azure.com"}),
        _ds("DS_Orders", "AzureSqlTable", "LS_Sales_SQL"),
        _ds("DS_Raw", "DelimitedText", "LS_Lake", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "raw"}}),
        _ds("DS_Curated", "Parquet", "LS_Lake", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "curated"}}),
        _ds("DS_ERP", "SqlServerTable", "LS_ERP_OnPrem"),
        _ds("DS_TenantA", "AzureSqlTable", "LS_Tenant_DB", DBName="tenant_a"),
        _ds("DS_TenantB", "AzureSqlTable", "LS_Tenant_DB", DBName="tenant_b"),
    ])
    files["integrationRuntime/IR_Azure.json"] = {"name": "IR_Azure", "properties": {"type": "Managed", "typeProperties": {
        "computeProperties": {"location": "AutoResolve"}}}}
    files["integrationRuntime/IR_ManagedVnet.json"] = {"name": "IR_ManagedVnet", "properties": {"type": "Managed", "managedVirtualNetwork": {
        "referenceName": "default", "type": "ManagedVirtualNetworkReference"}, "typeProperties": {}}}
    files["integrationRuntime/IR_SelfHosted.json"] = {"name": "IR_SelfHosted", "properties": {"type": "SelfHosted", "typeProperties": {}}}
    files["pipeline/PL_Daily_Load.json"] = {"name": "PL_Daily_Load", "properties": {"activities": [
        _copy("Copy_Orders", "DS_Orders", "DS_Raw"), _copy("Copy_ERP", "DS_ERP", "DS_Curated"),
        _copy("Copy_TenantA", "DS_TenantA", "DS_Raw"), _copy("Copy_TenantB", "DS_TenantB", "DS_Curated")]}}
    return files


def write_estate(root: Path, files: dict[str, dict]) -> None:
    for rel, body in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(body, indent=2), encoding="utf-8")


def seed_project(store: ProjectStore, pid: str = DEMO_PROJECT) -> None:
    """Create the demo project by writing the sample estate and running the REAL discovery and semantic stages."""
    if store.exists(pid):
        return
    src = store.source_dir(pid)
    write_estate(src, demo_estate())
    git.init_repo(src, "main")
    sha = git.commit_all(src, "demo estate")
    from datetime import datetime, timezone

    store.save(Project(id=pid, name="Demo estate", repository=RepositoryMetadata(
        provider="demo", repository_url="demo://sample-estate", branch="main", commit_sha=sha,
        fetched_at=datetime.now(timezone.utc), local_source_path=str(src))))
    DiscoveryAgent(store).discover_repository(DiscoveryRequest(project_name=pid))
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name=pid))


# ---- canned providers ----------------------------------------------------------------------------------------------


class _Attempt(LoginAttempt):
    pass


class _DemoLogins:
    def __init__(self):
        self._a = {}

    def get(self, attempt_id):
        return self._a.get(attempt_id)


class DemoAzure:
    id = "azure_cli"

    def __init__(self):
        self.logins = _DemoLogins()

    def status(self, refresh: bool = False) -> ProviderStatus:
        return ProviderStatus(provider=self.id, available=True, version="demo", authenticated=True, account=DEMO_ACCOUNT,
                              tenant_id=DEMO_TENANT, subscription_id=DEMO_SUB, state=ConnectionState.authenticated,
                              checks=[Check(name="demo", status=CheckStatus.passed, detail="canned data; no Azure call is made")])

    def start_login(self, tenant=None, device_code=False):
        a = LoginAttempt(id="demo-login", command="az", state=SUCCEEDED)
        a.lines.append("Demo mode: no sign-in is performed.")
        self.logins._a[a.id] = a
        return a

    def logout(self, username=None):
        return self.status()  # demo: nothing to sign out of

    def accounts(self, refresh=False):
        return [AzureAccount(username=DEMO_ACCOUNT, tenants=[DEMO_TENANT], is_default=True, subscription_count=1)]

    def subscriptions(self, account=None, refresh=False):
        return [Subscription(id=DEMO_SUB, name="Contoso Demo", tenant_id=DEMO_TENANT, state="Enabled", is_default=True, user=DEMO_ACCOUNT)]

    def tenants(self):
        return [{"id": DEMO_TENANT, "name": "Contoso (demo)"}]

    def discover(self, subscription):
        def r(name, rtype, rg="rg-data"):
            return ArmResource(id=f"/subscriptions/{subscription}/resourceGroups/{rg}/providers/{rtype}/{name}", name=name, type=rtype,
                               location="westeurope", resource_group=rg)

        return {"factories": [r("contoso-adf", "Microsoft.DataFactory/factories")],
                "synapse_workspaces": [r("contoso-synapse", "Microsoft.Synapse/workspaces")],
                "storage_accounts": [r("contosolake", "Microsoft.Storage/storageAccounts")],
                "sql_servers": [r("contoso-sales", "Microsoft.Sql/servers")], "key_vaults": [r("contoso-kv", "Microsoft.KeyVault/vaults")],
                "fabric_capacities": []}

    def validate_factory(self, subscription, resource_group, factory):
        return TestReport(target=f"Data Factory '{factory}'", state=ConnectionState.connection_tested,
                          verified=["(demo) the factory exists and can be read", "(demo) 1 pipeline(s) can be listed"],
                          not_verified=["demo data: nothing was contacted"])

    def validate_synapse_workspace(self, subscription, resource_group, workspace):
        return TestReport(target=f"Synapse workspace '{workspace}'", state=ConnectionState.connection_tested,
                          verified=["(demo) the workspace exists"], not_verified=["demo data: nothing was contacted"])

    def token(self, *a, **k):
        from adf2fabric.connections.providers.azure_cli import ProviderError

        raise ProviderError("Demo mode never requests a token.")


class DemoFabricService(FabricService):
    """Canned Fabric data behind the real ``FabricService`` interface (no HTTP)."""

    def __init__(self):
        self._made: list[dict] = []
        super().__init__(api=None)  # type: ignore[arg-type]

    def workspaces(self, include_personal: bool = False):
        return [FabricWorkspace(id=DEMO_WS, display_name="Demo Fabric workspace", type="Workspace", capacity_id="cap-demo")]

    def workspace(self, workspace_id):
        return {"id": workspace_id, "displayName": "Demo Fabric workspace", "type": "Workspace", "capacityId": "cap-demo"}

    def capacities(self):
        return [{"id": "cap-demo", "displayName": "Demo capacity", "sku": "F64", "state": "Active"}]

    def connections(self):
        return [{"id": "00000000-0000-0000-0000-0000000000c1", "displayName": "Sales SQL (existing)",
                 "connectionDetails": {"type": "SQL", "path": "contoso-sales.database.windows.net;sales"},
                 "credentialDetails": {"credentialType": "Basic"}}] + self._made

    def supported_connection_types(self):
        return {"SQL": ["Basic", "OAuth2", "ServicePrincipal", "WorkspaceIdentity"],
                "AzureDataLakeStorage": ["Key", "OAuth2", "SharedAccessSignature", "ServicePrincipal", "WorkspaceIdentity"],
                "AzureBlobs": ["Anonymous", "Key", "OAuth2", "SharedAccessSignature", "ServicePrincipal", "WorkspaceIdentity"],
                "RestService": ["OAuth2", "Basic", "Anonymous", "ServicePrincipal"]}

    def gateways(self):
        return [{"id": "gw-vnet", "type": "VirtualNetwork", "displayName": "Demo VNet gateway"},
                {"id": "gw-onprem", "type": "OnPremises", "displayName": "Demo on-premises gateway"}]

    def pipelines(self, workspace_id):
        return []

    def create_connection(self, body):
        cid = f"00000000-0000-0000-0000-{len(self._made) + 100:012d}"
        self._made.append({"id": cid, "displayName": body["displayName"], "connectionDetails": {
            "type": body["connectionDetails"]["type"], "path": ""}, "credentialDetails": {
            "credentialType": body["credentialDetails"]["credentials"]["credentialType"]}})
        return {"id": cid}

    def provision_identity(self, workspace_id):
        return {"applicationId": "00000000-0000-0000-0000-00000000a111", "servicePrincipalId": "00000000-0000-0000-0000-00000000a222"}


class DemoFabricCli:
    id = "fabric_cli"

    def __init__(self):
        self.logins = _DemoLogins()

    def status(self) -> ProviderStatus:
        return ProviderStatus(provider=self.id, available=True, version="demo", authenticated=True, account=DEMO_ACCOUNT,
                              tenant_id=DEMO_TENANT, state=ConnectionState.authenticated,
                              checks=[Check(name="demo", status=CheckStatus.passed, detail="canned data")])

    def start_login(self, method="interactive", **kw):
        a = LoginAttempt(id="demo-fab-login", command="fab", state=SUCCEEDED)
        a.lines.append("Demo mode: no sign-in is performed.")
        self.logins._a[a.id] = a
        return a

    def logout(self):
        return self.status()  # demo: nothing to sign out of

    def capabilities(self):
        return {"auth": True, "api": True}

    def service(self):
        return DemoFabricService()


class DemoGit:
    id = "git"

    def is_available(self):
        return True

    def test(self, url, revision, auth):
        return TestReport(target=url, state=ConnectionState.connection_tested, verified=["(demo) the repository is reachable"],
                          not_verified=["demo data: nothing was contacted"])

    def fetch(self, url, revision, dest, auth):
        write_estate(dest, demo_estate())
        git.init_repo(dest, "main")
        return git.commit_all(dest, "demo fetch")


def build_demo_container(settings: Optional[Settings] = None) -> Container:
    root = Path(tempfile.mkdtemp(prefix="a2f-demo-"))
    store = ProjectStore(root / "projects")
    seed_project(store)
    fab = DemoFabricService()
    return Container(settings, store=store, mode="demo", azure=DemoAzure(), fabric_cli=DemoFabricCli(),  # type: ignore[arg-type]
                     git=DemoGit(),  # type: ignore[arg-type]
                     tester=ConnectorTester(probe=lambda h, p, t: (True, ""), sql_connect=lambda cs, t: None,
                                            driver=lambda: "ODBC Driver 18 for SQL Server", secret_reader=lambda v, n: "demo-secret"),
                     vault_reader=lambda v, n: "demo-secret", fabric_factory=lambda s, p: fab)
