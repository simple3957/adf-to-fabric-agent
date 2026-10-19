"""What each connector supports: its Fabric connection type, the endpoint fields it needs, its authentication methods and the
fields each method asks for. The UI renders cards from this data, so adding a connector is a data change, not UI code.

Nothing here is assumed to be available just because it is listed: ``methods_for`` intersects the declared methods with the
credential types the TARGET Fabric tenant reports for that connection type (``GET /v1/connections/supportedConnectionTypes``),
and methods Fabric cannot create through its API (interactive OAuth sign-in, gateway connections) say so and point to the one
supported route: create the connection in Fabric and then use "existing connection".

Fabric credential shapes (Microsoft "Connections - Create Connection"): Basic{username,password}, Key{key},
SharedAccessSignature{token}, ServicePrincipal{tenantId,servicePrincipalClientId,servicePrincipalSecret}, WorkspaceIdentity{},
Anonymous{}. Fabric has no Key Vault credential type: a "...in Key Vault" method means the backend reads the secret with the
user's Azure sign-in and sends it once (see ``keyvault``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

GUID = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")


@dataclass(frozen=True)
class FieldSpec:
    name: str
    label: str
    kind: str = "text"  # text | secret | guid | url | vault_url | secret_name
    required: bool = True
    help: str = ""

    def public(self) -> dict:
        return {"name": self.name, "label": self.label, "kind": self.kind, "required": self.required, "help": self.help}


@dataclass(frozen=True)
class AuthMethod:
    id: str
    label: str
    fabric_credential: Optional[str]  # Fabric credentialType; None for "use an existing connection"
    fields: tuple[FieldSpec, ...] = ()
    key_vault: bool = False  # the secret named by vault_url/secret_name is read by the backend
    needs_workspace_identity: bool = False
    api_creatable: bool = True  # False: must be created in the Fabric UI, then used through "existing connection"
    notes: str = ""

    @property
    def secret_fields(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.fields if f.kind == "secret")

    def public(self) -> dict:
        return {"id": self.id, "label": self.label, "fabric_credential": self.fabric_credential,
                "fields": [f.public() for f in self.fields], "key_vault": self.key_vault,
                "needs_workspace_identity": self.needs_workspace_identity, "api_creatable": self.api_creatable,
                "notes": self.notes}


@dataclass(frozen=True)
class EndpointParam:
    """One value a Fabric connection needs to address the data store, and the Fabric parameter it becomes."""

    field: FieldSpec
    fabric_param: str


@dataclass(frozen=True)
class ConnectorSpec:
    id: str
    label: str
    adf_types: tuple[str, ...]  # lower case
    fabric_type: str  # the connection type name in Fabric
    creation_method: str
    endpoint: tuple[EndpointParam, ...]
    methods: tuple[AuthMethod, ...]
    tests: tuple[str, ...] = ()  # connector-specific validations this connector can run (see testing.py)
    network_ports: tuple[int, ...] = ()
    notes: str = ""


# ---- field + method building blocks -------------------------------------------------------------------------------

_TENANT = FieldSpec("tenant_id", "Tenant ID", "guid", help="The service principal's Microsoft Entra tenant.")
_CLIENT = FieldSpec("client_id", "Client (application) ID", "guid")
_VAULT = FieldSpec("vault_url", "Key Vault URL", "vault_url", help="https://<vault>.vault.azure.net")
_SECRET_NAME = FieldSpec("secret_name", "Secret name", "secret_name", help="The Key Vault secret that holds the value.")
_SP_SECRET = FieldSpec("client_secret", "Client secret", "secret")


def _existing() -> AuthMethod:
    return AuthMethod("existing_connection", "Use an existing Fabric connection", None,
                      (FieldSpec("connection_id", "Fabric connection ID", "guid"),),
                      notes="Reuse a connection already in Fabric (including OAuth, gateway and workspace-identity "
                            "connections created in the Fabric UI). Its credentials are not verified by this tool.")


def _entra_oauth() -> AuthMethod:
    return AuthMethod("entra_oauth", "Microsoft Entra sign-in (OAuth)", "OAuth2", api_creatable=False,
                      notes="Signs in as a user, which cannot be done through the API. Create the connection in Fabric "
                            "(Manage connections and gateways), then choose 'Use an existing Fabric connection'.")


def _workspace_identity() -> AuthMethod:
    return AuthMethod("workspace_identity", "Workspace identity", "WorkspaceIdentity", needs_workspace_identity=True,
                      notes="No secret. The destination workspace needs an identity (this tool can provision one) and that "
                            "identity needs a role on the data source; the role grant is not something this tool can do.")


def _sp(kv: bool) -> AuthMethod:
    if kv:
        return AuthMethod("service_principal_kv", "Service principal, secret in Key Vault", "ServicePrincipal",
                          (_TENANT, _CLIENT, _VAULT, _SECRET_NAME), key_vault=True)
    return AuthMethod("service_principal", "Service principal", "ServicePrincipal", (_TENANT, _CLIENT, _SP_SECRET))


def _sql_methods() -> tuple[AuthMethod, ...]:
    user = FieldSpec("username", "SQL login", "text")
    return (_existing(), _workspace_identity(),
            AuthMethod("sql_login", "SQL login", "Basic", (user, FieldSpec("password", "Password", "secret"))),
            AuthMethod("sql_login_kv", "SQL login, password in Key Vault", "Basic", (user, _VAULT, _SECRET_NAME), key_vault=True),
            _sp(False), _sp(True), _entra_oauth())


def _storage_methods(anonymous: bool = False) -> tuple[AuthMethod, ...]:
    out = [_existing(), _workspace_identity(),
           AuthMethod("account_key", "Account key", "Key", (FieldSpec("account_key", "Account key", "secret"),)),
           AuthMethod("account_key_kv", "Account key in Key Vault", "Key", (_VAULT, _SECRET_NAME), key_vault=True),
           AuthMethod("sas_token", "SAS token", "SharedAccessSignature", (FieldSpec("sas_token", "SAS token", "secret"),)),
           AuthMethod("sas_token_kv", "SAS token in Key Vault", "SharedAccessSignature", (_VAULT, _SECRET_NAME), key_vault=True),
           _sp(False), _sp(True), _entra_oauth()]
    if anonymous:
        out.append(AuthMethod("anonymous", "Anonymous (public container)", "Anonymous"))
    return tuple(out)


_SERVER = FieldSpec("server", "Server", "text", help="e.g. myserver.database.windows.net")
_DATABASE = FieldSpec("database", "Database", "text", required=False)
_ACCOUNT = FieldSpec("account", "Storage account", "text")
_CONTAINER = FieldSpec("container", "Container / file system", "text")
_URL = FieldSpec("url", "URL", "url")

CONNECTORS: tuple[ConnectorSpec, ...] = (
    ConnectorSpec(
        id="sql", label="SQL / Azure Synapse SQL", fabric_type="SQL", creation_method="Sql", network_ports=(1433,),
        adf_types=("azuresqldatabase", "azuresqlmi", "azuresqldw", "sqlserver", "azuresynapseanalytics"),
        endpoint=(EndpointParam(_SERVER, "server"), EndpointParam(_DATABASE, "database")),
        methods=_sql_methods(), tests=("tcp", "sql_login"),
        notes="Covers Azure SQL Database, Managed Instance, Synapse dedicated and serverless SQL pools and SQL Server."),
    ConnectorSpec(
        id="adls_gen2", label="Azure Data Lake Storage Gen2", fabric_type="AzureDataLakeStorage",
        creation_method="AzureDataLakeStorage", network_ports=(443,), adf_types=("azureblobfs",),
        endpoint=(EndpointParam(_ACCOUNT, "server"), EndpointParam(_CONTAINER, "path")),
        methods=_storage_methods(), tests=("tcp", "storage")),
    ConnectorSpec(
        id="blob", label="Azure Blob Storage", fabric_type="AzureBlobs", creation_method="AzureBlobs", network_ports=(443,),
        adf_types=("azureblobstorage",),
        endpoint=(EndpointParam(_ACCOUNT, "account"), EndpointParam(FieldSpec("domain", "Domain", "text", help="blob.core.windows.net"), "domain")),
        methods=_storage_methods(anonymous=True), tests=("tcp", "storage")),
    ConnectorSpec(
        id="rest", label="REST service", fabric_type="RestService", creation_method="RestService.Contents", network_ports=(443,),
        adf_types=("restservice",),
        endpoint=(EndpointParam(FieldSpec("base_url", "Base URL", "url"), "baseUrl"),),
        methods=(_existing(),
                 AuthMethod("anonymous", "Anonymous", "Anonymous"),
                 AuthMethod("basic", "Basic", "Basic", (FieldSpec("username", "User name", "text"),
                                                         FieldSpec("password", "Password", "secret"))),
                 AuthMethod("basic_kv", "Basic, password in Key Vault", "Basic",
                            (FieldSpec("username", "User name", "text"), _VAULT, _SECRET_NAME), key_vault=True),
                 _sp(False), _sp(True), _entra_oauth()),
        tests=("tcp",)),
    ConnectorSpec(
        id="databricks", label="Azure Databricks workspace", fabric_type="AzureDatabricksWorkspace",
        creation_method="AzureDatabricksWorkspace.Actions", network_ports=(443,), adf_types=("azuredatabricks",),
        endpoint=(EndpointParam(FieldSpec("url", "Workspace URL", "url", help="https://adb-....azuredatabricks.net"), "url"),),
        methods=(_existing(), _workspace_identity(),
                 AuthMethod("access_token", "Personal access token", "Key", (FieldSpec("token", "Token", "secret"),)),
                 AuthMethod("access_token_kv", "Personal access token in Key Vault", "Key", (_VAULT, _SECRET_NAME), key_vault=True),
                 _sp(False), _sp(True), _entra_oauth()),
        tests=("tcp",)),
)

_BY_ADF_TYPE = {t: c for c in CONNECTORS for t in c.adf_types}
UNSUPPORTED_NOTE = ("No connector declaration exists for this linked-service type, so no authentication options can be offered. "
                    "Create the Fabric connection in Fabric and use 'Use an existing Fabric connection'.")


def connector_for(adf_type: Optional[str]) -> Optional[ConnectorSpec]:
    return _BY_ADF_TYPE.get((adf_type or "").lower())


def generic_methods() -> tuple[AuthMethod, ...]:
    return (_existing(),)


@dataclass
class MethodAvailability:
    method: AuthMethod
    available: bool
    reason: str = ""


def methods_for(spec: Optional[ConnectorSpec], supported_credentials: Optional[list[str]]) -> list[MethodAvailability]:
    """The methods to show for a connector. With ``supported_credentials`` (what the target tenant accepts for this Fabric
    connection type) a method Fabric does not accept is marked unavailable with the reason, not silently hidden or offered."""
    if spec is None:
        return [MethodAvailability(m, True) for m in generic_methods()]
    out = []
    for m in spec.methods:
        if m.fabric_credential is None or supported_credentials is None or m.fabric_credential in supported_credentials:
            out.append(MethodAvailability(m, True))
        else:
            out.append(MethodAvailability(m, False, f"Fabric does not accept {m.fabric_credential} credentials for "
                                                    f"{spec.fabric_type} connections in this tenant."))
    return out


# ---- validation + Fabric request building --------------------------------------------------------------------------------


SKIP_METHOD = "skip"  # not a Fabric sign-in method: the user decided this linked service needs no connection


class ChoiceError(Exception):
    """The choice cannot be used; ``problems`` lists every field to fix (never a secret value)."""

    def __init__(self, problems: list[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


def method_by_id(spec: ConnectorSpec, method_id: str) -> AuthMethod:
    for m in spec.methods:
        if m.id == method_id:
            return m
    raise ChoiceError([f"'{method_id}' is not an authentication method of {spec.label}."])


def validate_choice(spec: ConnectorSpec, method: AuthMethod, endpoint: dict[str, str], values: dict[str, str],
                    secrets_held: set[str]) -> list[str]:
    """Problems with a choice (empty = complete). ``secrets_held`` names secret fields already in the vault."""
    from adf2fabric.connections.keyvault import KeyVaultError, normalize_vault_url, validate_secret_name

    problems: list[str] = []
    if method.id != "existing_connection":
        for ep in spec.endpoint:
            if ep.field.required and not (endpoint.get(ep.field.name) or "").strip():
                problems.append(f"Endpoint field '{ep.field.label}' is required.")
    for f in method.fields:
        v = (values.get(f.name) or "").strip()
        if f.kind == "secret":
            if f.name not in secrets_held:
                problems.append(f"'{f.label}' has not been supplied.")
        elif not v and f.required:
            problems.append(f"'{f.label}' is required.")
        elif v and f.kind == "guid" and not GUID.match(v):
            problems.append(f"'{f.label}' must be a GUID.")
        elif v and f.kind == "vault_url":
            try:
                normalize_vault_url(v)
            except KeyVaultError as exc:
                problems.append(str(exc))
        elif v and f.kind == "secret_name":
            try:
                validate_secret_name(v)
            except KeyVaultError as exc:
                problems.append(str(exc))
        elif v and f.kind == "url" and urlparse(v).scheme != "https":
            problems.append(f"'{f.label}' must be an https URL.")
    return problems


def connection_details(spec: ConnectorSpec, endpoint: dict[str, str]) -> dict:
    """The ``connectionDetails`` object of a Fabric create-connection request."""
    params = []
    for ep in spec.endpoint:
        raw = (endpoint.get(ep.field.name) or "").strip()
        if not raw:
            continue
        if spec.id == "adls_gen2" and ep.fabric_param == "server" and "." not in raw:
            raw = f"{raw}.dfs.core.windows.net"  # an account name becomes its Data Lake endpoint host
        params.append({"dataType": "Text", "name": ep.fabric_param, "value": raw})
    return {"type": spec.fabric_type, "creationMethod": spec.creation_method, "parameters": params}


def credentials_object(method: AuthMethod, values: dict[str, str], secrets: dict[str, str]) -> dict:
    """The Fabric ``credentials`` object. ``secrets`` already holds any Key Vault-resolved value under the field's name."""
    c = method.fabric_credential
    if c == "WorkspaceIdentity" or c == "Anonymous":
        return {"credentialType": c}
    if c == "Basic":
        return {"credentialType": "Basic", "username": values["username"], "password": secrets["password"]}
    if c == "Key":
        return {"credentialType": "Key", "key": secrets.get("account_key") or secrets["token"]}
    if c == "SharedAccessSignature":
        return {"credentialType": "SharedAccessSignature", "token": secrets["sas_token"].lstrip("?")}
    if c == "ServicePrincipal":
        return {"credentialType": "ServicePrincipal", "tenantId": values["tenant_id"],
                "servicePrincipalClientId": values["client_id"], "servicePrincipalSecret": secrets["client_secret"]}
    raise ChoiceError([f"{method.label} cannot be created through the Fabric API."])


def build_connection_body(spec: ConnectorSpec, method: AuthMethod, endpoint: dict[str, str], values: dict[str, str],
                          secrets: dict[str, str], display_name: str, privacy: str = "Organizational") -> dict:
    """A complete create-connection request. The Fabric service tests the credentials when it creates the connection."""
    if not method.api_creatable or method.fabric_credential is None:
        raise ChoiceError([f"{method.label} cannot be created through the Fabric API; create it in Fabric and reuse it."])
    return {"connectivityType": "ShareableCloud", "displayName": display_name, "privacyLevel": privacy,
            "connectionDetails": connection_details(spec, endpoint),
            "credentialDetails": {"singleSignOnType": "None", "connectionEncryption": "NotEncrypted",
                                  "skipTestConnection": False, "credentials": credentials_object(method, values, secrets)}}


def endpoint_key(spec: ConnectorSpec, endpoint: dict[str, str]) -> str:
    """A normalised address used to match an existing Fabric connection (lower case; trailing dots/slashes removed)."""
    parts = []
    for ep in spec.endpoint:
        v = (endpoint.get(ep.field.name) or "").strip().lower().rstrip("/.")
        if spec.id == "adls_gen2" and ep.fabric_param == "server" and v and "." not in v:
            v = f"{v}.dfs.core.windows.net"
        parts.append(v)
    return ";".join(parts)
