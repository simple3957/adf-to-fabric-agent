"""Tests for the five real connectors, fully offline.

Heavy SDKs (pyodbc, azure-identity, azure-storage-file-datalake, requests) are
not required: each connector imports them lazily, so when they are absent the
connector returns a deterministic ``dependency_unavailable`` outcome, and where
a SDK is present its client is monkeypatched. The tests also assert the narrow
contract — connectors validate connectivity and never enumerate.
"""

from __future__ import annotations

import importlib.util
import inspect

import pytest

from adf2fabric.connectivity import azure_data_factory as adf_mod
from adf2fabric.connectivity import sql_server as sql_mod
from adf2fabric.connectivity.adls_gen2 import AdlsGen2Connector
from adf2fabric.connectivity.azure_data_factory import AdfConnector
from adf2fabric.connectivity.fabric import FabricConnector
from adf2fabric.connectivity.registry import get_connector, registered_domains
from adf2fabric.connectivity.repository import RepositoryConnector
from adf2fabric.connectivity.sql_server import SqlServerConnector
from adf2fabric.project.models import (
    AccessDomain,
    AdfAccess,
    AdlsGen2Access,
    AzureCredentialRef,
    ConnectivityStatus,
    FabricAccess,
    Project,
    RepositoryAccess,
    RepositoryMetadata,
    SqlCredentialRef,
    SqlServerAccess,
    SqlTlsMode,
    SqlTlsPolicy,
)

def _installed(name: str) -> bool:
    # find_spec raises ModuleNotFoundError if a parent package (e.g. 'azure') is
    # absent; treat that as "not installed".
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False


HAS_PYODBC = _installed("pyodbc")
HAS_AZURE_ID = _installed("azure.identity")
HAS_ADLS = _installed("azure.storage.filedatalake")
HAS_REQUESTS = _installed("requests")


def _project():
    return Project(
        id="demo",
        name="Demo",
        repository=RepositoryMetadata(
            provider="github",
            repository_url="https://github.com/acme/demo.git",
            branch="main",
            commit_sha="a" * 40,
            local_source_path="/tmp/demo/source",
        ),
    )


def _azure_cred():
    return AzureCredentialRef(
        auth="client_secret",
        tenant_id_env="AZ_TENANT_ID",
        client_id_env="AZ_CLIENT_ID",
        client_secret_env="AZ_CLIENT_SECRET",
    )


# --- registry ---------------------------------------------------------------

def test_registry_has_five_current_domains():
    assert registered_domains() == ["adf", "adls_gen2", "fabric", "repository", "sql_server"]


def test_get_connector_returns_instance():
    assert get_connector(AccessDomain.sql_server.value).domain == "sql_server"


def test_get_connector_unknown_raises():
    with pytest.raises(KeyError):
        get_connector("teradata")


# --- repository -------------------------------------------------------------

def test_repository_missing_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    access = RepositoryAccess(required=True, token_env="GITHUB_TOKEN")
    result = RepositoryConnector().validate(access, _project())
    assert result.status == ConnectivityStatus.missing_credential


def test_repository_validated(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_faketoken")
    # Stand in for the real remote check so no network is used.
    from adf2fabric.repository.client import RepositoryClient

    monkeypatch.setattr(RepositoryClient, "validate", lambda self, url, branch: None)
    access = RepositoryAccess(required=True, token_env="GITHUB_TOKEN")
    result = RepositoryConnector().validate(access, _project())
    assert result.status == ConnectivityStatus.validated
    assert result.reachable and result.authenticated


def test_repository_failure_redacts_token(monkeypatch):
    token = "ghp_supersecrettoken"
    monkeypatch.setenv("GITHUB_TOKEN", token)
    from adf2fabric.repository.client import RepositoryAccessError, RepositoryClient

    def boom(self, url, branch):
        raise RepositoryAccessError(f"denied for {token}")

    monkeypatch.setattr(RepositoryClient, "validate", boom)
    access = RepositoryAccess(required=True, token_env="GITHUB_TOKEN")
    result = RepositoryConnector().validate(access, _project())
    assert result.status == ConnectivityStatus.failed
    assert token not in result.detail


# --- sql server -------------------------------------------------------------
#
# The connect path is exercised through the ``_import_pyodbc`` seam with a fake
# driver, so these tests need no real pyodbc, no server, and no real secret.

class _FakeCursor:
    def __init__(self, log):
        self._log = log

    def execute(self, sql):
        self._log.append(("execute", sql))

    def fetchone(self):
        self._log.append(("fetchone",))
        return (1,)


class _FakeConn:
    def __init__(self, log):
        self._log = log

    def cursor(self):
        self._log.append(("cursor",))
        return _FakeCursor(self._log)

    def close(self):
        self._log.append(("close",))


class _FakeError(Exception):
    pass


class _FakePyodbc:
    def __init__(self, log, error=None, capture=None):
        self._log = log
        self._error = error
        self._capture = capture
        self.Error = _FakeError

    def connect(self, conn_str, timeout=None):
        if self._capture is not None:
            self._capture["conn_str"] = conn_str
            self._capture["timeout"] = timeout
        self._log.append(("connect", timeout))
        if self._error is not None:
            raise self._error
        return _FakeConn(self._log)


def _sql_access(**over):
    base = dict(
        required=True,
        server="srv",
        database="db",
        credential=SqlCredentialRef(auth="sql_login", username="svc", password_env="SQL_PASSWORD"),
    )
    base.update(over)
    return SqlServerAccess(**base)


def _inject(monkeypatch, fake):
    monkeypatch.setattr(sql_mod, "_import_pyodbc", lambda: fake)


def test_sql_missing_config():
    access = SqlServerAccess(required=True, credential=SqlCredentialRef(auth="sql_login"))
    result = SqlServerConnector().validate(access, _project())
    assert result.status == ConnectivityStatus.missing_config


def test_sql_missing_credential_env(monkeypatch):
    # Deterministic without pyodbc: the credential reference is resolved before
    # the driver is imported.
    monkeypatch.delenv("SQL_PASSWORD", raising=False)
    result = SqlServerConnector().validate(_sql_access(), _project())
    assert result.status == ConnectivityStatus.missing_credential


def test_sql_dependency_unavailable(monkeypatch):
    monkeypatch.setenv("SQL_PASSWORD", "pw")

    def _raise():
        raise ImportError("No module named 'pyodbc'")

    monkeypatch.setattr(sql_mod, "_import_pyodbc", _raise)
    result = SqlServerConnector().validate(_sql_access(), _project())
    assert result.status == ConnectivityStatus.dependency_unavailable


def test_sql_validated_runs_select_1_and_closes(monkeypatch):
    monkeypatch.setenv("SQL_PASSWORD", "pw-secret-value")
    log: list = []
    _inject(monkeypatch, _FakePyodbc(log))
    result = SqlServerConnector().validate(_sql_access(), _project())

    assert result.status == ConnectivityStatus.validated
    assert result.reachable and result.authenticated
    # Exactly one query, and it is SELECT 1.
    executed = [entry[1] for entry in log if entry[0] == "execute"]
    assert executed == ["SELECT 1"]
    # Connection is closed.
    assert ("close",) in log
    # No credential value leaks into the outcome.
    assert "pw-secret-value" not in result.detail
    assert "pw-secret-value" not in result.model_dump_json()


def test_sql_authentication_failure_normalized(monkeypatch):
    monkeypatch.setenv("SQL_PASSWORD", "pw-secret-value")
    # Fake error message deliberately contains the password to prove redaction.
    err = _FakeError("28000", "[28000] Login failed for user 'svc'. PWD=pw-secret-value")
    _inject(monkeypatch, _FakePyodbc([], error=err))
    result = SqlServerConnector().validate(_sql_access(), _project())

    assert result.status == ConnectivityStatus.failed
    assert result.reachable is True and result.authenticated is False
    assert "authentication" in result.detail.lower()
    assert "pw-secret-value" not in result.detail


def test_sql_network_failure_normalized(monkeypatch):
    monkeypatch.setenv("SQL_PASSWORD", "pw")
    err = _FakeError("08001", "[08001] Cannot open server; login timeout expired")
    _inject(monkeypatch, _FakePyodbc([], error=err))
    result = SqlServerConnector().validate(_sql_access(), _project())

    assert result.status == ConnectivityStatus.failed
    assert result.reachable is False
    assert "network" in result.detail.lower()


def test_sql_permission_failure_normalized(monkeypatch):
    monkeypatch.setenv("SQL_PASSWORD", "pw")
    err = _FakeError("42000", "The server principal is not able to access; permission denied")
    _inject(monkeypatch, _FakePyodbc([], error=err))
    result = SqlServerConnector().validate(_sql_access(), _project())

    assert result.status == ConnectivityStatus.failed
    assert result.reachable is True and result.authenticated is True
    assert "permission" in result.detail.lower()


def _conn_str_for(monkeypatch, access):
    monkeypatch.setenv("SQL_PASSWORD", "pw")
    cap: dict = {}
    _inject(monkeypatch, _FakePyodbc([], capture=cap))
    SqlServerConnector().validate(access, _project())
    return cap["conn_str"]


def test_sql_tls_strict_maps_to_encrypt_yes_no_trust(monkeypatch):
    cs = _conn_str_for(monkeypatch, _sql_access(tls=SqlTlsPolicy(mode=SqlTlsMode.strict)))
    assert "Encrypt=yes" in cs
    assert "TrustServerCertificate=no" in cs
    assert "TrustServerCertificate=yes" not in cs


def test_sql_tls_trust_server_certificate_maps_to_encrypt_yes_trust_yes(monkeypatch):
    cs = _conn_str_for(
        monkeypatch, _sql_access(tls=SqlTlsPolicy(mode=SqlTlsMode.trust_server_certificate))
    )
    assert "Encrypt=yes" in cs
    assert "TrustServerCertificate=yes" in cs


def test_sql_tls_disabled_maps_to_encrypt_no(monkeypatch):
    cs = _conn_str_for(monkeypatch, _sql_access(tls=SqlTlsPolicy(mode=SqlTlsMode.disabled)))
    assert "Encrypt=no" in cs
    assert "TrustServerCertificate=" not in cs


def test_sql_missing_tls_emits_no_encryption_option(monkeypatch):
    # No tls policy -> the connector emits nothing and never silently selects a
    # weaker mode (no TrustServerCertificate=yes, no Encrypt=no).
    cs = _conn_str_for(monkeypatch, _sql_access())  # tls omitted
    assert "Encrypt=" not in cs
    assert "TrustServerCertificate=" not in cs
    assert "SERVER=srv" in cs and "DATABASE=db" in cs


def test_sql_tls_not_inferred_from_sqlexpress_hostname(monkeypatch):
    # A SQL Express / local-looking host must NOT cause trust to be inferred.
    cs = _conn_str_for(monkeypatch, _sql_access(server=r"PTDELL0190\SQLEXPRESS"))  # tls omitted
    assert "TrustServerCertificate=yes" not in cs
    assert "Encrypt=no" not in cs


def test_sql_integrated_connection_string_has_no_credentials(monkeypatch):
    cs = _conn_str_for(
        monkeypatch,
        _sql_access(credential=SqlCredentialRef(auth="integrated")),
    )
    assert "Trusted_Connection=yes" in cs
    assert "UID=" not in cs and "PWD=" not in cs


def test_sql_cert_failure_no_fallback_and_prompts_for_policy(monkeypatch):
    monkeypatch.setenv("SQL_PASSWORD", "pw")
    log: list = []
    err = _FakeError(
        "08001",
        "[08001] SSL Provider: The certificate chain was issued by an authority "
        "that is not trusted.",
    )
    _inject(monkeypatch, _FakePyodbc(log, error=err))
    # No tls policy supplied.
    result = SqlServerConnector().validate(_sql_access(), _project())

    assert result.status == ConnectivityStatus.failed
    # Exactly one connect attempt — no automatic retry with a weaker TLS mode.
    assert sum(1 for e in log if e[0] == "connect") == 1
    # Safe remediation points at the explicit policy, not a specific weaker mode.
    assert "tls.mode" in result.detail
    assert "certificate" in result.detail.lower()


def test_sql_cert_failure_with_policy_has_no_remediation_nag(monkeypatch):
    monkeypatch.setenv("SQL_PASSWORD", "pw")
    err = _FakeError("08001", "SSL Provider: certificate chain not trusted")
    _inject(monkeypatch, _FakePyodbc([], error=err))
    result = SqlServerConnector().validate(
        _sql_access(tls=SqlTlsPolicy(mode=SqlTlsMode.strict)), _project()
    )
    assert result.status == ConnectivityStatus.failed
    # A policy was explicitly set, so no "set sql_server.tls.mode" nag.
    assert "No explicit TLS policy" not in result.detail


def test_sql_connector_does_not_enumerate_or_mutate():
    """Static guard: SQL connector only runs SELECT 1 — no discovery, no writes."""
    low = inspect.getsource(sql_mod).lower()
    for forbidden in (
        "information_schema",
        "sys.",
        "select *",
        "sp_help",
        "show tables",
        "fetchall",
        "insert ",
        "update ",
        "delete ",
        "drop ",
        "alter ",
        "create table",
    ):
        assert forbidden not in low
    # The one and only executed statement is SELECT 1.
    assert 'execute("select 1")' in low
    assert low.count(".execute(") == 1


# --- adf --------------------------------------------------------------------
#
# Fully offline via seams: `adf_mod._import_requests` (HTTP transport) and
# `adf_mod.build_credential` (authentication). No azure-identity/requests needed.

FAKE_TOKEN = "FAKE.BEARER.TOKEN.VALUE"


class _FakeResp:
    def __init__(self, status_code):
        self.status_code = status_code


class _FakeRequests:
    def __init__(self, status=200, raise_on_get=None):
        self.status = status
        self.raise_on_get = raise_on_get
        self.calls = []  # (method, url, had_auth_header)

    def get(self, url, headers=None, timeout=None):
        self.calls.append(("get", url, bool(headers and "Authorization" in headers)))
        if self.raise_on_get is not None:
            raise self.raise_on_get
        return _FakeResp(self.status)

    def _forbidden(self, *a, **k):  # pragma: no cover - must never be called
        raise AssertionError("ADF connector must issue no write request")

    post = put = patch = delete = _forbidden


class _FakeToken:
    def __init__(self, token):
        self.token = token


class _FakeCred:
    def __init__(self, scope_log, raise_exc=None, token=FAKE_TOKEN):
        self.scope_log = scope_log
        self.raise_exc = raise_exc
        self._token = token

    def get_token(self, scope):
        self.scope_log.append(scope)
        if self.raise_exc is not None:
            raise self.raise_exc
        return _FakeToken(self._token)


def _azure_cli_cred():
    return AzureCredentialRef(auth="azure_cli")


def _adf_access(**over):
    base = dict(
        required=True,
        mandatory=True,
        subscription_id="sub-123",
        resource_group="rg-data",
        factory_name="adf-prod",
        credential=_azure_cli_cred(),
    )
    base.update(over)
    return AdfAccess(**base)


def _run_adf(monkeypatch, *, status=200, raise_on_get=None, cred=None, secret="", scope_log=None):
    fake_requests = _FakeRequests(status=status, raise_on_get=raise_on_get)
    if cred is None:
        cred = _FakeCred(scope_log if scope_log is not None else [])
    monkeypatch.setattr(adf_mod, "_import_requests", lambda: fake_requests)
    monkeypatch.setattr(adf_mod, "build_credential", lambda c: (cred, secret))
    result = AdfConnector().validate(_adf_access(), _project())
    return result, fake_requests


@pytest.mark.parametrize("missing", ["subscription_id", "resource_group", "factory_name"])
def test_adf_missing_each_identifier(missing):
    result = AdfConnector().validate(_adf_access(**{missing: None}), _project())
    assert result.status == ConnectivityStatus.missing_config
    assert missing in result.detail


def test_adf_missing_credential(monkeypatch):
    # Real build_credential(None) -> missing_config (before any SDK import);
    # provide a fake requests so we reach the credential step deterministically.
    monkeypatch.setattr(adf_mod, "_import_requests", lambda: _FakeRequests())
    result = AdfConnector().validate(_adf_access(credential=None), _project())
    assert result.status == ConnectivityStatus.missing_config


def test_adf_requests_unavailable(monkeypatch):
    def _raise():
        raise ImportError("No module named 'requests'")

    monkeypatch.setattr(adf_mod, "_import_requests", _raise)
    result = AdfConnector().validate(_adf_access(), _project())
    assert result.status == ConnectivityStatus.dependency_unavailable


def test_adf_azure_sdk_unavailable(monkeypatch):
    from adf2fabric.connectivity._azure import CredentialUnavailable

    def _raise(cred):
        raise CredentialUnavailable(
            ConnectivityStatus.dependency_unavailable, "azure-identity is not installed."
        )

    monkeypatch.setattr(adf_mod, "_import_requests", lambda: _FakeRequests())
    monkeypatch.setattr(adf_mod, "build_credential", _raise)
    result = AdfConnector().validate(_adf_access(), _project())
    assert result.status == ConnectivityStatus.dependency_unavailable


def test_adf_validated_single_arm_get_and_scope(monkeypatch):
    scope_log: list = []
    result, fake = _run_adf(monkeypatch, status=200, scope_log=scope_log)

    assert result.status == ConnectivityStatus.validated
    assert result.reachable and result.authenticated
    # Amendment 1: exactly the ARM scope, nothing else.
    assert scope_log == ["https://management.azure.com/.default"]
    # Amendment 4: exactly one request, a GET, to the factory ARM path.
    assert len(fake.calls) == 1
    method, url, had_auth = fake.calls[0]
    assert method == "get" and had_auth is True
    assert "/providers/Microsoft.DataFactory/factories/adf-prod" in url
    assert "api-version=2018-06-01" in url
    for child in ("/pipelines", "/datasets", "/linkedservices", "/triggers",
                  "/integrationruntimes", "/dataflows"):
        assert child not in url
    # Amendment 3: the bearer token never appears in the outcome.
    assert FAKE_TOKEN not in result.detail
    assert FAKE_TOKEN not in result.model_dump_json()


def test_adf_auth_401(monkeypatch):
    result, _ = _run_adf(monkeypatch, status=401)
    assert result.status == ConnectivityStatus.failed
    assert result.reachable is True and result.authenticated is False
    assert "authentication" in result.detail.lower()


def test_adf_permission_403(monkeypatch):
    result, _ = _run_adf(monkeypatch, status=403)
    assert result.status == ConnectivityStatus.failed
    assert result.reachable is True and result.authenticated is True
    assert "permission" in result.detail.lower()


def test_adf_not_found_404(monkeypatch):
    result, _ = _run_adf(monkeypatch, status=404)
    assert result.status == ConnectivityStatus.failed
    assert "not found" in result.detail.lower()


def test_adf_network_failure_normalized(monkeypatch):
    boom = OSError("connection refused")
    result, fake = _run_adf(monkeypatch, raise_on_get=boom)
    assert result.status == ConnectivityStatus.failed
    assert result.reachable is False
    assert "network" in result.detail.lower()
    assert FAKE_TOKEN not in result.detail


def test_adf_token_acquisition_failure(monkeypatch):
    cred = _FakeCred([], raise_exc=RuntimeError("az login required"))
    result, fake = _run_adf(monkeypatch, cred=cred)
    assert result.status == ConnectivityStatus.failed
    assert "authentication" in result.detail.lower()
    # No HTTP request is made if a token cannot be acquired.
    assert fake.calls == []


def test_adf_connector_is_readiness_only():
    """Static guard: ADF connector makes only a factory GET — no discovery/writes."""
    src = adf_mod  # module
    import inspect as _inspect

    low = _inspect.getsource(src).lower()
    for forbidden in (
        "/pipelines",
        "/datasets",
        "/linkedservices",
        "/triggers",
        "/integrationruntimes",
        "/dataflows",
        ".post(",
        ".put(",
        ".patch(",
        ".delete(",
    ):
        assert forbidden not in low
    assert "requests.get" in low or ".get(" in low
    assert "/providers/microsoft.datafactory/factories/" in low


# --- azure credential helper (azure_cli) ------------------------------------

def _install_fake_azure_identity(monkeypatch):
    import sys
    import types

    identity = types.ModuleType("azure.identity")

    class _AzureCliCredential:
        pass

    class _ClientSecretCredential:
        def __init__(self, *a, **k):
            pass

    class _DefaultAzureCredential:
        pass

    identity.AzureCliCredential = _AzureCliCredential
    identity.ClientSecretCredential = _ClientSecretCredential
    identity.DefaultAzureCredential = _DefaultAzureCredential
    monkeypatch.setitem(sys.modules, "azure", types.ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    return _AzureCliCredential


def test_azure_cli_maps_to_azure_cli_credential(monkeypatch):
    fake_cls = _install_fake_azure_identity(monkeypatch)
    from adf2fabric.connectivity._azure import build_credential

    cred, secret = build_credential(AzureCredentialRef(auth="azure_cli"))
    assert isinstance(cred, fake_cls)
    assert secret == ""


def test_azure_unknown_auth_is_missing_config(monkeypatch):
    _install_fake_azure_identity(monkeypatch)
    from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential

    with pytest.raises(CredentialUnavailable) as exc:
        build_credential(AzureCredentialRef(auth="banana"))
    assert exc.value.status == ConnectivityStatus.missing_config


# --- adls gen2 --------------------------------------------------------------

def test_adls_missing_config():
    access = AdlsGen2Access(required=True, credential=_azure_cred())
    result = AdlsGen2Connector().validate(access, _project())
    assert result.status == ConnectivityStatus.missing_config


@pytest.mark.skipif(HAS_ADLS, reason="azure-storage-file-datalake installed")
def test_adls_dependency_unavailable():
    access = AdlsGen2Access(required=True, storage_account="acct", credential=_azure_cred())
    result = AdlsGen2Connector().validate(access, _project())
    assert result.status == ConnectivityStatus.dependency_unavailable


def test_adls_does_not_enumerate():
    """Static guard: the ADLS connector must not list paths/files/directories."""
    src = inspect.getsource(AdlsGen2Connector)
    for forbidden in ("list_paths", "get_paths", "list_file_systems", "download", "read"):
        assert forbidden not in src


# --- fabric -----------------------------------------------------------------

def test_fabric_missing_config():
    access = FabricAccess(required=True, credential=_azure_cred())
    result = FabricConnector().validate(access, _project())
    assert result.status == ConnectivityStatus.missing_config


def test_fabric_validated_via_monkeypatched_client(monkeypatch):
    if not (HAS_AZURE_ID and HAS_REQUESTS):
        pytest.skip("azure-identity/requests not installed")

    import adf2fabric.connectivity.fabric as fabmod

    class _Cred:
        def get_token(self, scope):
            class T:
                token = "x"
            return T()

    monkeypatch.setattr(fabmod, "build_credential", lambda cred: (_Cred(), ""))
    import requests

    class _Resp:
        status_code = 200

    monkeypatch.setattr(requests, "get", lambda *a, **k: _Resp())
    access = FabricAccess(required=True, workspace_id="ws-1", credential=_azure_cred())
    result = FabricConnector().validate(access, _project())
    assert result.status == ConnectivityStatus.validated


# --- boundary: connectors never create Fabric artifacts ---------------------

def test_fabric_connector_is_read_only():
    src = inspect.getsource(FabricConnector)
    # Only a GET workspace reachability check; no artifact creation/deploy.
    for forbidden in (".post(", ".put(", ".patch(", ".delete(", "create", "deploy"):
        assert forbidden not in src.lower()
