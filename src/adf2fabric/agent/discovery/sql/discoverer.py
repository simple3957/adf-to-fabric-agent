"""The SQL Server Discoverer — the SQL domain of the Discovery phase (Phase 1).

Responsibility (and hard boundary):

    load project -> resolve explicit SQL target -> require explicit TLS mode
    -> resolve credential reference -> reuse SqlServerConnector._connection_string
    -> open ONE connection -> run exactly the four approved catalog queries
    -> normalize outcome -> persist discovery/sql.yaml -> STOP

It records the STRUCTURAL estate only — schemas, tables, views, stored
procedures — from read-only catalog metadata. It performs NO ``SELECT 1``, NO
business-data reads, NO row counts, NO column/key/relationship inspection, and
NO writes/DDL/EXEC. The password is read only from the named environment
variable at run time and is never logged, persisted, or placed in ``detail``.

The DSN is built by the authoritative connectivity builder
(``SqlServerConnector._connection_string``) so TLS and auth translation are
never duplicated. ``connectivity/sql_server.py`` is reused, never modified.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from adf2fabric.agent.discovery.sql import catalog
from adf2fabric.agent.discovery.sql.models import (
    SqlDiscoveryRequest,
    SqlDiscoveryResult,
)
from adf2fabric.connectivity.sql_server import (
    DEFAULT_LOGIN_TIMEOUT,
    _AUTH_SQLSTATES,
    _NETWORK_SQLSTATES,
    SqlServerConnector,
)
from adf2fabric.project.discovery import (
    SqlDiscovery,
    SqlDiscoveryStatus,
    SqlObject,
)
from adf2fabric.project.models import SqlServerAccess
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify
from adf2fabric.repository import authentication

_PROVENANCE = {
    "server": "explicit_cli_input",
    "database": "explicit_cli_input",
    "authentication": "explicit_cli_input",
    "status": "sql_catalog_metadata",
    "schemas": "sys.schemas",
    "tables": "sys.tables",
    "views": "sys.views",
    "procedures": "sys.procedures",
}


class SqlDiscoverer:
    """SQL Server domain discoverer, invoked by the phase-level ``DiscoveryAgent``."""

    def __init__(
        self,
        store: ProjectStore,
        connection_factory: Callable[[str, int], Any] = catalog.connect,
    ):
        self.store = store
        # Injection point for tests; production opens a real pyodbc connection.
        self.connection_factory = connection_factory

    def run(self, request: SqlDiscoveryRequest) -> SqlDiscoveryResult:
        try:
            project_id = slugify(request.project_name)
            if not self.store.exists(project_id):
                return SqlDiscoveryResult(
                    success=False,
                    message="SQL Discovery failed.",
                    error=(
                        f"No project '{project_id}' found. Run `adf2fabric intake` "
                        "first to establish the project."
                    ),
                )

            access = request.access
            server = (access.server or "").strip()
            database = (access.database or "").strip()

            # 1. Required target (no assumptions; a missing value blocks).
            if not server or not database:
                return self._persist_result(
                    project_id, server, database, "",
                    SqlDiscoveryStatus.missing_config,
                    detail="Both 'server' and 'database' are required.",
                )

            # 2. TLS is an EXPLICIT policy. A missing mode is never inferred or
            #    allowed to fall through to driver defaults.
            if access.tls is None:
                return self._persist_result(
                    project_id, server, database, "",
                    SqlDiscoveryStatus.missing_config,
                    detail=(
                        "An explicit TLS mode is required "
                        "(strict | trust_server_certificate | disabled)."
                    ),
                )

            # 3. Resolve the credential reference. Only the auth MODE is recorded;
            #    the password value (sql_login) is read from the named env var and
            #    never stored. Integrated auth needs no secret.
            cred = access.credential
            if cred is None:
                return self._persist_result(
                    project_id, server, database, "",
                    SqlDiscoveryStatus.missing_config,
                    detail="A credential reference is required.",
                )
            auth = (cred.auth or "").strip().lower()
            username = cred.username
            password = ""
            if auth == "sql_login":
                if not cred.username or not cred.password_env:
                    return self._persist_result(
                        project_id, server, database, auth,
                        SqlDiscoveryStatus.missing_config,
                        detail="sql_login auth requires 'username' and 'password_env'.",
                    )
                import os

                password = os.environ.get(cred.password_env) or ""
                if not password:
                    return self._persist_result(
                        project_id, server, database, auth,
                        SqlDiscoveryStatus.missing_credential,
                        detail=f"Environment variable {cred.password_env} is not set.",
                    )
            elif auth == "integrated":
                password = ""
            else:
                return self._persist_result(
                    project_id, server, database, auth,
                    SqlDiscoveryStatus.missing_config,
                    detail=(
                        f"Unknown SQL auth '{cred.auth}'. "
                        "Use 'sql_login' or 'integrated'."
                    ),
                )

            # 4. Reuse the authoritative DSN builder (TLS + auth translation live
            #    there — never duplicated here). Open ONE connection, run exactly
            #    the four approved catalog queries on it, then close it.
            conn_str = SqlServerConnector._connection_string(access, username, password)
            login_timeout = access.connect_timeout or DEFAULT_LOGIN_TIMEOUT
            conn = None
            try:
                conn = self.connection_factory(conn_str, login_timeout)
                schemas, tables, views, procedures = catalog.run_metadata_queries(conn)
            except ImportError:
                return self._persist_result(
                    project_id, server, database, auth,
                    SqlDiscoveryStatus.dependency_unavailable,
                    detail="pyodbc is not installed. Install the 'connectivity' extra.",
                )
            except Exception as exc:  # noqa: BLE001 - normalized below, redacted
                status = _classify_error(exc)
                detail = authentication.redact(str(exc), password)
                return self._persist_result(
                    project_id, server, database, auth, status, detail=detail
                )
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001 - close is best-effort
                        pass
                password = ""  # drop the secret reference promptly

            return self._persist_result(
                project_id, server, database, auth,
                SqlDiscoveryStatus.discovered,
                detail="",
                schemas=schemas,
                tables=tables,
                views=views,
                procedures=procedures,
                metadata_queries=list(catalog.METADATA_QUERY_LABELS),
            )
        except ProjectNotFoundError as exc:
            return SqlDiscoveryResult(
                success=False, message="SQL Discovery failed.", error=str(exc)
            )

    # --- assembly + persistence --------------------------------------------

    def _persist_result(
        self,
        project_id: str,
        server: str,
        database: str,
        authentication_mode: str,
        status: SqlDiscoveryStatus,
        *,
        detail: str,
        schemas: list[str] | None = None,
        tables: list[SqlObject] | None = None,
        views: list[SqlObject] | None = None,
        procedures: list[SqlObject] | None = None,
        metadata_queries: list[str] | None = None,
    ) -> SqlDiscoveryResult:
        discovery = self._assemble(
            project_id, server, database, authentication_mode, status, detail,
            schemas or [], tables or [], views or [], procedures or [],
            metadata_queries or [],
        )
        self.store.save_sql_discovery(project_id, discovery)
        ok = status == SqlDiscoveryStatus.discovered
        if ok:
            msg = (
                f"SQL Discovery complete for '{project_id}': "
                f"{discovery.schema_count} schema(s), {discovery.table_count} table(s), "
                f"{discovery.view_count} view(s), {discovery.procedure_count} procedure(s)."
            )
            return SqlDiscoveryResult(success=True, discovery=discovery, message=msg)
        return SqlDiscoveryResult(
            success=False,
            discovery=discovery,
            message="SQL Discovery failed.",
            error=f"{status.value}: {detail}" if detail else status.value,
        )

    def _assemble(
        self, project_id, server, database, authentication_mode, status, detail,
        schemas, tables, views, procedures, metadata_queries,
    ) -> SqlDiscovery:
        now = datetime.now(timezone.utc)
        discovery = SqlDiscovery(
            project_id=project_id,
            server=server,
            database=database,
            authentication=authentication_mode,
            status=status,
            created_at=now,
            updated_at=now,
            schema_count=len(schemas),
            schemas=schemas,
            table_count=len(tables),
            tables=tables,
            view_count=len(views),
            views=views,
            procedure_count=len(procedures),
            procedures=procedures,
            metadata_queries=metadata_queries,
            detail=detail,
            provenance=dict(_PROVENANCE),
        )

        # Timestamp semantics: created_at is set once; updated_at bumps only when
        # the semantic content changes (timestamps excluded from comparison).
        if self.store.has_sql_discovery(project_id):
            try:
                existing = self.store.load_sql_discovery(project_id)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                discovery.created_at = existing.created_at
                if _semantic(existing) == _semantic(discovery):
                    discovery.updated_at = existing.updated_at
                else:
                    discovery.updated_at = now
        return discovery


def _classify_error(exc: Exception) -> SqlDiscoveryStatus:
    """Map an expected pyodbc failure to a distinct, deterministic discovery status.

    Keyed on the SQLSTATE (reusing the connectivity connector's constant sets) and
    a safe substring scan of the message. An auth/permission failure is NEVER
    collapsed into an empty ``discovered`` result. Anything not attributable to
    auth/network/permission is treated as a metadata-query failure.
    """
    sqlstate = ""
    if exc.args and isinstance(exc.args[0], str):
        sqlstate = exc.args[0].strip().lower()
    low = str(exc).lower()

    if sqlstate in _AUTH_SQLSTATES or "login failed" in low:
        return SqlDiscoveryStatus.authentication_failed
    if (
        sqlstate in _NETWORK_SQLSTATES
        or "timeout" in low
        or "cannot open server" in low
        or "does not exist or is not accessible" in low
        or "unable to" in low
        or "certificate" in low
        or "ssl provider" in low
        or "ssl security error" in low
    ):
        return SqlDiscoveryStatus.connectivity_failed
    if "permission" in low or "denied" in low:
        return SqlDiscoveryStatus.permission_denied
    return SqlDiscoveryStatus.metadata_query_failed


def _semantic(discovery: SqlDiscovery) -> dict:
    """Discovery content used to detect meaningful change (excludes timestamps)."""
    data = json.loads(discovery.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data
