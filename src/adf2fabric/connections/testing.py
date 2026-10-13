"""Testing a connector + authentication choice, honestly.

A test is split into the steps that make up "it works", and the report says exactly which ran:

  1. configuration  - every required field is present and well-formed (no network)
  2. key vault      - the Key Vault secret named by the choice can be read with the user's sign-in (value dropped)
  3. reachability   - a TCP connection from THIS machine to the endpoint (not from Fabric: it proves nothing about Fabric's path)
  4. credential     - the credential actually authenticates to the data store (SQL login / storage SDK), where that can be
                      done without running a query or changing anything

``connection_tested`` is reported only when step 4 passed. Methods that cannot be exercised from here (workspace identity, an
existing Fabric connection, interactive OAuth) end in ``connection_configured`` with the limitation stated; a failing step ends in
``validation_failed`` with the reason and what to do. Secrets are held in local variables for the call and never returned.
"""

from __future__ import annotations

import socket
from typing import Any, Callable, Optional

from adf2fabric.connections import catalog
from adf2fabric.connections.keyvault import KeyVaultError, normalize_vault_url
from adf2fabric.connections.models import Check, CheckStatus, ConnectionState, TestReport
from adf2fabric.connections.redaction import redact

Probe = Callable[[str, int, float], tuple[bool, str]]
SecretReader = Callable[[str, str], str]  # (vault_url, secret_name) -> value; raises KeyVaultError
SqlConnect = Callable[[str, float], None]  # raises on failure; opens and closes a connection, runs no query
ClientFactory = Callable[[str, Any], Any]


def tcp_probe(host: str, port: int, timeout: float = 5.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, ""
    except socket.gaierror:
        return False, "the host name could not be resolved from this machine"
    except (TimeoutError, socket.timeout):
        return False, "the connection timed out"
    except OSError as exc:
        return False, f"the connection was refused or blocked ({type(exc).__name__})"


def _odbc_escape(value: str) -> str:
    return "{" + value.replace("}", "}}") + "}"


def pyodbc_connect(connection_string: str, timeout: float) -> None:
    import pyodbc

    conn = pyodbc.connect(connection_string, timeout=int(timeout), autocommit=True)  # authentication only; no statement is run
    conn.close()


def pick_driver() -> Optional[str]:
    try:
        import pyodbc
    except ImportError:
        return None
    available = set(pyodbc.drivers())
    for name in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if name in available:
            return name
    return None


class ConnectorTester:
    def __init__(self, *, probe: Probe = tcp_probe, sql_connect: SqlConnect = pyodbc_connect,
                 driver: Callable[[], Optional[str]] = pick_driver, secret_reader: Optional[SecretReader] = None,
                 datalake_factory: Optional[ClientFactory] = None, blob_factory: Optional[ClientFactory] = None):
        self._probe, self._sql_connect, self._driver = probe, sql_connect, driver
        self._read_secret = secret_reader
        self._datalake, self._blob = datalake_factory, blob_factory

    # ---- entry point ----------------------------------------------------------------------------------------------
    def test(self, spec: catalog.ConnectorSpec, method: catalog.AuthMethod, endpoint: dict[str, str], values: dict[str, str],
             secrets: dict[str, str]) -> TestReport:
        rep = TestReport(target=f"{spec.label} ({method.label})", state=ConnectionState.validation_failed)
        problems = catalog.validate_choice(spec, method, endpoint, values, {k for k, v in secrets.items() if v})
        if problems:
            rep.error = problems[0]
            rep.checks.append(Check(name="configuration", status=CheckStatus.failed, detail="; ".join(problems)))
            rep.actions = problems
            return rep
        rep.checks.append(Check(name="configuration", status=CheckStatus.passed))
        rep.verified.append("the configuration is complete and well-formed")

        if method.id == "existing_connection":
            rep.state = ConnectionState.connection_configured
            rep.not_verified.append("an existing Fabric connection's credentials cannot be tested from here; Fabric validated "
                                    "them when it was created")
            self._reach(rep, spec, endpoint)
            return rep

        resolved = dict(secrets)
        if method.key_vault:
            err = self._resolve_vault(rep, method, values, resolved)
            if err:
                return rep
        self._reach(rep, spec, endpoint)

        credential_checked = self._credential(rep, spec, method, endpoint, values, resolved)
        if rep.error:
            rep.state = ConnectionState.validation_failed
        elif credential_checked:
            rep.state = ConnectionState.connection_tested
        else:
            rep.state = ConnectionState.connection_configured
        return rep

    # ---- steps ----------------------------------------------------------------------------------------------------
    def _resolve_vault(self, rep: TestReport, method: catalog.AuthMethod, values: dict[str, str], resolved: dict[str, str]) -> bool:
        vault, name = values.get("vault_url", ""), values.get("secret_name", "")
        if self._read_secret is None:
            rep.not_verified.append("Key Vault access was not checked (no reader configured)")
            rep.checks.append(Check(name="key vault", status=CheckStatus.unverified))
            rep.error = "Key Vault access is not configured on this server."
            return True
        try:
            value = self._read_secret(normalize_vault_url(vault), name)
        except KeyVaultError as exc:
            rep.error, rep.actions = str(exc), exc.actions
            rep.checks.append(Check(name="key vault", status=CheckStatus.failed, detail=str(exc)))
            return True
        # the secret lands in the field this method's Fabric credential expects, under the name catalog.credentials_object reads
        target = {"Basic": "password", "Key": "account_key" if "account_key" in method.id else "token",
                  "SharedAccessSignature": "sas_token", "ServicePrincipal": "client_secret"}[method.fabric_credential or "Basic"]
        resolved[target] = value
        rep.verified.append("the Key Vault secret can be read with your sign-in (its value was not kept)")
        rep.checks.append(Check(name="key vault", status=CheckStatus.passed))
        return False

    def _reach(self, rep: TestReport, spec: catalog.ConnectorSpec, endpoint: dict[str, str]) -> None:
        host = _host(spec, endpoint)
        if not host or not spec.network_ports:
            rep.checks.append(Check(name="reachability", status=CheckStatus.skipped, detail="no host to probe"))
            return
        ok, why = self._probe(host, spec.network_ports[0], 5.0)
        if ok:
            rep.verified.append(f"{host}:{spec.network_ports[0]} is reachable from this machine")
            rep.checks.append(Check(name="reachability", status=CheckStatus.passed, detail=host))
        else:
            rep.not_verified.append(f"{host} was not reachable from this machine ({why}); this does not prove Fabric cannot reach "
                                    "it, but a private endpoint or firewall may need a gateway")
            rep.checks.append(Check(name="reachability", status=CheckStatus.failed, detail=why))
        rep.not_verified.append("reachability is from THIS machine, not from Fabric's network")

    def _credential(self, rep: TestReport, spec: catalog.ConnectorSpec, method: catalog.AuthMethod, endpoint: dict[str, str],
                    values: dict[str, str], secrets: dict[str, str]) -> bool:
        """True when a credential was actually exercised (and passed); sets ``rep.error`` on failure."""
        cred = method.fabric_credential
        if cred in ("WorkspaceIdentity", "OAuth2"):
            rep.not_verified.append("a workspace identity or interactive sign-in is Fabric's own identity and cannot be exercised "
                                    "from here; Fabric validates it when the connection is created")
            rep.checks.append(Check(name="credential", status=CheckStatus.unverified))
            return False
        if "credential" not in spec.tests and "sql_login" not in spec.tests and "storage" not in spec.tests:
            rep.not_verified.append(f"{spec.label} credentials are not exercised by this tool; Fabric validates them when the "
                                    "connection is created")
            rep.checks.append(Check(name="credential", status=CheckStatus.unverified))
            return False
        try:
            if spec.id == "sql":
                return self._sql(rep, method, endpoint, values, secrets)
            if spec.id in ("adls_gen2", "blob") and "storage" in spec.tests:
                return self._storage(rep, spec, method, endpoint, values, secrets)
        except Exception as exc:  # noqa: BLE001 - mapped to a safe message; the raw text may echo a secret
            rep.error = _safe(exc, *secrets.values())
            rep.actions = _actions(rep.error)
            rep.checks.append(Check(name="credential", status=CheckStatus.failed, detail=rep.error))
            return False
        rep.not_verified.append(f"{spec.label} credentials are not exercised by this tool")
        rep.checks.append(Check(name="credential", status=CheckStatus.unverified))
        return False

    def _sql(self, rep, method, endpoint, values, secrets) -> bool:
        driver = self._driver()
        if driver is None:
            rep.not_verified.append("no SQL Server ODBC driver (17 or 18) is installed, so the login could not be tested")
            rep.checks.append(Check(name="credential", status=CheckStatus.unverified, detail="no ODBC driver"))
            return False
        parts = [f"DRIVER={{{driver}}}", f"SERVER={endpoint['server']}", "Encrypt=yes", "TrustServerCertificate=no"]
        if endpoint.get("database"):
            parts.append(f"DATABASE={endpoint['database']}")
        if method.fabric_credential == "Basic":
            parts += [f"UID={_odbc_escape(values['username'])}", f"PWD={_odbc_escape(secrets['password'])}"]
        else:  # service principal
            parts += ["Authentication=ActiveDirectoryServicePrincipal", f"UID={values['client_id']}",
                      f"PWD={_odbc_escape(secrets['client_secret'])}"]
        self._sql_connect(";".join(parts), 15.0)
        rep.verified.append("the login authenticated to the server" + (f" and database '{endpoint['database']}'" if endpoint.get("database") else ""))
        rep.not_verified.append("authentication only: no query was run, so object-level permissions were not checked")
        rep.checks.append(Check(name="credential", status=CheckStatus.passed))
        return True

    def _storage(self, rep, spec, method, endpoint, values, secrets) -> bool:
        account = endpoint["account"].split(".")[0]
        container = endpoint.get("container") or ""
        cred: Any
        c = method.fabric_credential
        if c == "Key":
            cred = secrets["account_key"] if "account_key" in secrets else secrets["token"]
        elif c == "SharedAccessSignature":
            cred = secrets["sas_token"].lstrip("?")
        elif c == "ServicePrincipal":
            from azure.identity import ClientSecretCredential

            cred = ClientSecretCredential(values["tenant_id"], values["client_id"], secrets["client_secret"])
        else:  # Anonymous
            cred = None
        if spec.id == "adls_gen2":
            factory = self._datalake or _datalake_client
            fs = factory(f"https://{account}.dfs.core.windows.net", cred).get_file_system_client(container)
            fs.get_file_system_properties()
            next(iter(fs.get_paths(max_results=1)), None)
            rep.verified.append(f"the credential can read file system '{container}' and list its paths")
        else:
            factory = self._blob or _blob_client
            svc = factory(f"https://{account}.{endpoint.get('domain') or 'blob.core.windows.net'}", cred)
            if container:
                svc.get_container_client(container).get_container_properties()
                rep.verified.append(f"the credential can read container '{container}'")
            else:
                svc.get_account_information()
                rep.verified.append("the credential can read the account's information")
        rep.not_verified.append("read/list access only: write permission was not tested (it would change data)")
        rep.checks.append(Check(name="credential", status=CheckStatus.passed))
        return True


def _datalake_client(url: str, cred: Any):
    from azure.storage.filedatalake import DataLakeServiceClient

    return DataLakeServiceClient(url, credential=cred)


def _blob_client(url: str, cred: Any):
    from azure.storage.blob import BlobServiceClient

    return BlobServiceClient(url, credential=cred)


def _host(spec: catalog.ConnectorSpec, endpoint: dict[str, str]) -> Optional[str]:
    from urllib.parse import urlparse

    if spec.id == "sql":
        return (endpoint.get("server") or "").split(",")[0].strip() or None
    if spec.id == "adls_gen2":
        acct = (endpoint.get("account") or "").strip()
        return (acct if "." in acct else f"{acct}.dfs.core.windows.net") if acct else None
    if spec.id == "blob":
        acct = (endpoint.get("account") or "").strip()
        return f"{acct}.{endpoint.get('domain') or 'blob.core.windows.net'}" if acct else None
    raw = endpoint.get("base_url") or endpoint.get("url") or ""
    return urlparse(raw).hostname if raw else None


def _safe(exc: Exception, *secrets: str) -> str:
    text = redact(f"{type(exc).__name__}: {exc}", *secrets).replace("\n", " ")
    low = text.lower()
    if "login failed" in low or "authentication failed" in low or "28000" in low or "18456" in low:
        return "The server rejected the login (wrong user name, password, or the login has no access to this database)."
    if "authorizationpermissionmismatch" in low or "authorizationfailure" in low or "403" in low or "this request is not authorized" in low:
        return "The credential authenticated but is not authorized for this resource (a role such as Storage Blob Data Reader is needed)."
    if "containernotfound" in low or "filesystemnotfound" in low or "resourcenotfound" in low or "404" in low:
        return "The container or file system was not found in this account."
    if "authenticationfailed" in low or "invalid_client" in low or "aadsts7000215" in low or "invalid client secret" in low:
        return "The service principal or key was rejected (check the secret, its expiry, and the tenant and client IDs)."
    if "timeout" in low or "could not open a connection" in low or "tcp provider" in low or "name or service not known" in low:
        return "The server could not be reached from this machine (check the address, firewall and network path)."
    return text[:240]


def _actions(message: str) -> list[str]:
    m = message.lower()
    if "rejected the login" in m:
        return ["Check the user name and password, and that the login is mapped to a user in this database."]
    if "not authorized" in m:
        return ["Grant the identity a data-plane role (for example Storage Blob Data Reader) on the account or container."]
    if "not found" in m:
        return ["Check the account and container names."]
    if "rejected" in m:
        return ["Check the secret value and its expiry."]
    if "could not be reached" in m:
        return ["Check the address, and whether the endpoint is private and needs a gateway."]
    return []
