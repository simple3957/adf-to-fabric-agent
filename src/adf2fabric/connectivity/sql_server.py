"""SQL Server connectivity check.

Consultation readiness only: establish that the configured SQL Server database
is reachable and the supplied credentials are sufficient, by opening a real
connection, running a single deterministic ``SELECT 1``, and closing it.

    configure -> resolve credential reference -> connect -> SELECT 1
              -> normalize result -> close -> return outcome

It does NOT perform Discovery: no enumeration of databases/schemas/tables/views/
columns/procedures/indexes/constraints, no row reads, no data profiling, no
catalog queries, and no user-supplied SQL. It never modifies the database.

``pyodbc`` is optional and imported lazily (``connectivity`` extra); if it is
unavailable the connector returns a deterministic ``dependency_unavailable``
outcome instead of raising. No credential value is ever placed in the returned
outcome or its ``detail``.

The connector never assumes host, port, authentication mode, or TLS settings —
it uses only what the configuration supplies (``driver`` is a local client detail
with a documented, overridable default). TLS is an EXPLICIT policy
(``tls.mode``): the connector translates the chosen mode to ODBC options and
never infers a mode, never weakens TLS silently, and never retries with a weaker
mode after a failure.
"""

from __future__ import annotations

import os
from typing import Any

from adf2fabric.connectivity.base import outcome
from adf2fabric.project.models import (
    AccessDomain,
    ConnectivityOutcome,
    ConnectivityStatus,
    Project,
    SqlServerAccess,
    SqlTlsMode,
)
from adf2fabric.repository import authentication

# Local ODBC driver default. This is a client-machine concern (not server
# topology); override it in the questionnaire with ``driver:`` when needed.
DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_LOGIN_TIMEOUT = 10

# SQLSTATE classes used to normalize expected failures. Lower-cased for lookup.
_AUTH_SQLSTATES = {"28000", "18456"}
_NETWORK_SQLSTATES = {"08001", "08s01", "08004", "08006", "hyt00", "hyt01"}


def _import_pyodbc() -> Any:
    """Lazy import seam. Tests monkeypatch this to inject a fake driver."""
    import pyodbc

    return pyodbc


# Deterministic translation of an explicit TLS mode to ODBC options. This is
# the ONLY place a TLS mode becomes connection options — no inference, no
# environment-based choice, no fallback.
_TLS_OPTIONS: dict[SqlTlsMode, tuple[str, ...]] = {
    SqlTlsMode.strict: ("Encrypt=yes", "TrustServerCertificate=no"),
    SqlTlsMode.trust_server_certificate: ("Encrypt=yes", "TrustServerCertificate=yes"),
    SqlTlsMode.disabled: ("Encrypt=no",),
}


def _tls_options(mode: SqlTlsMode) -> tuple[str, ...]:
    return _TLS_OPTIONS[mode]


def _fail(detail: str, *, reachable: bool = False, authenticated: bool = False) -> ConnectivityOutcome:
    return outcome(
        AccessDomain.sql_server,
        ConnectivityStatus.failed,
        reachable=reachable,
        authenticated=authenticated,
        detail=detail,
    )


def _classify(exc: Exception, password: str, *, tls_configured: bool) -> ConnectivityOutcome:
    """Normalize an expected pyodbc failure into a secret-free outcome.

    Distinguishes authentication / encryption-trust / network / permission /
    generic connectivity via the outcome's ``reachable``/``authenticated`` flags
    and a safe message. The readiness engine remains the policy authority — this
    only classifies. It never recommends a *specific* weaker TLS mode.
    """
    sqlstate = ""
    if exc.args and isinstance(exc.args[0], str):
        sqlstate = exc.args[0].strip().lower()
    raw = authentication.redact(str(exc), password)
    low = raw.lower()

    if sqlstate in _AUTH_SQLSTATES or "login failed" in low:
        # Server was reached; credentials were rejected.
        return _fail(f"Authentication failed. {raw}", reachable=True, authenticated=False)
    if "certificate" in low or "ssl provider" in low or "ssl security error" in low:
        # TLS negotiation failed (e.g. untrusted self-signed cert). The endpoint
        # was contacted but the encrypted channel could not be established.
        detail = f"Encryption/certificate-trust failure. {raw}"
        if not tls_configured:
            detail += (
                " No explicit TLS policy was configured; set sql_server.tls.mode "
                "(strict | trust_server_certificate | disabled) to state the "
                "intended encryption behavior."
            )
        return _fail(detail, reachable=True, authenticated=False)
    if sqlstate in _NETWORK_SQLSTATES or "timeout" in low or "unable to" in low \
            or "cannot open server" in low or "does not exist or is not accessible" in low:
        return _fail(f"Network/connectivity failure. {raw}", reachable=False)
    if "permission" in low or "denied" in low:
        # Authenticated, but not authorized for the requested resource.
        return _fail(f"Permission failure. {raw}", reachable=True, authenticated=True)
    return _fail(f"Connectivity failure. {raw}")


class SqlServerConnector:
    domain = AccessDomain.sql_server.value

    def validate(self, access: SqlServerAccess, project: Project) -> ConnectivityOutcome:
        # 1. Required configuration (no assumptions; a missing value blocks).
        if not access.server or not access.database:
            return outcome(
                AccessDomain.sql_server,
                ConnectivityStatus.missing_config,
                detail="Both 'server' and 'database' are required.",
            )
        cred = access.credential
        if cred is None:
            return outcome(
                AccessDomain.sql_server,
                ConnectivityStatus.missing_config,
                detail="A credential reference is required.",
            )

        # 2. Resolve the credential reference (before importing the driver, so a
        #    missing config/credential is deterministic even without pyodbc).
        auth = (cred.auth or "").strip().lower()
        password = ""
        if auth == "sql_login":
            if not cred.username or not cred.password_env:
                return outcome(
                    AccessDomain.sql_server,
                    ConnectivityStatus.missing_config,
                    detail="sql_login auth requires 'username' and 'password_env'.",
                )
            password = os.environ.get(cred.password_env) or ""
            if not password:
                return outcome(
                    AccessDomain.sql_server,
                    ConnectivityStatus.missing_credential,
                    detail=f"Environment variable {cred.password_env} is not set.",
                )
        elif auth == "integrated":
            password = ""
        else:
            return outcome(
                AccessDomain.sql_server,
                ConnectivityStatus.missing_config,
                detail=f"Unknown SQL auth '{cred.auth}'. Use 'sql_login' or 'integrated'.",
            )

        # 3. Optional dependency.
        try:
            pyodbc = _import_pyodbc()
        except ImportError:
            return outcome(
                AccessDomain.sql_server,
                ConnectivityStatus.dependency_unavailable,
                detail="pyodbc is not installed. Install the 'connectivity' extra.",
            )

        # 4. Connect, SELECT 1, close. The connection string is built only from
        #    explicit configuration and is never surfaced.
        conn_str = self._connection_string(access, cred.username, password)
        login_timeout = access.connect_timeout or DEFAULT_LOGIN_TIMEOUT
        try:
            conn = pyodbc.connect(conn_str, timeout=login_timeout)
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001 - pyodbc.Error and friends
            # A single attempt only — never retry with a weaker TLS mode.
            return _classify(exc, password, tls_configured=access.tls is not None)

        return outcome(
            AccessDomain.sql_server,
            ConnectivityStatus.validated,
            reachable=True,
            authenticated=True,
            detail=f"Connected to database '{access.database}'; SELECT 1 succeeded.",
        )

    @staticmethod
    def _connection_string(access: SqlServerAccess, username: str | None, password: str) -> str:
        driver = access.driver or DEFAULT_DRIVER
        server = access.server
        if access.port:
            server = f"{server},{access.port}"
        parts = [f"DRIVER={{{driver}}}", f"SERVER={server}", f"DATABASE={access.database}"]
        # TLS is applied only from an EXPLICIT policy — never assumed, never
        # inferred, never weakened silently. When ``tls`` is absent, no
        # encryption option is emitted and the ODBC driver's defaults apply.
        if access.tls is not None:
            parts.extend(_tls_options(access.tls.mode))
        if access.connect_timeout is not None:
            parts.append(f"Connection Timeout={access.connect_timeout}")
        auth = (access.credential.auth or "").strip().lower()
        if auth == "integrated":
            parts.append("Trusted_Connection=yes")
        else:  # sql_login
            parts.append(f"UID={username}")
            parts.append(f"PWD={password}")
        return ";".join(parts)
