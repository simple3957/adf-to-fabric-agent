"""The SQL Structural Discoverer — SQL Server domain, Phase 2 (structural metadata).

Responsibility (and hard boundary):

    load project -> resolve explicit SQL target -> require explicit TLS mode
    -> resolve credential reference -> reuse SqlServerConnector._connection_string
    -> open ONE connection -> run exactly the three approved structural queries
    -> assemble per-table structures + relationships -> persist
    discovery/sql-structural.yaml -> STOP

It records columns / data types / nullability / identity / primary keys / foreign
keys / relationships from read-only ``sys.*`` catalog metadata only. NO business
data, NO ``SELECT *``, NO row reads/sampling/aggregates/profiling, NO
writes/DDL/EXEC, NO ``SELECT 1``, NO stored-procedure execution or body inspection.

The DSN is built by the authoritative connectivity builder
(``SqlServerConnector._connection_string``) and the pyodbc connection seam is
reused from the Phase-1 SQL package — there is no second connection/DSN builder.
The Phase-1 ``sql.yaml`` contract is never mutated. The password is read only from
the named env var and never logged or persisted.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from adf2fabric.agent.discovery.sql import catalog as sql_catalog
from adf2fabric.agent.discovery.sql_structural import catalog
from adf2fabric.agent.discovery.sql_structural.models import (
    SqlStructuralDiscoveryRequest,
    SqlStructuralDiscoveryResult,
)
from adf2fabric.connectivity.sql_server import (
    DEFAULT_LOGIN_TIMEOUT,
    _AUTH_SQLSTATES,
    _NETWORK_SQLSTATES,
    SqlServerConnector,
)
from adf2fabric.project.discovery import (
    SqlColumn,
    SqlDiscoveryStatus,
    SqlForeignKey,
    SqlPrimaryKey,
    SqlRelationship,
    SqlStructuralDiscovery,
    SqlTableStructure,
)
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify
from adf2fabric.repository import authentication

_PROVENANCE = {
    "server": "explicit_cli_input",
    "database": "explicit_cli_input",
    "authentication": "explicit_cli_input",
    "status": "sql_catalog_metadata",
    "columns": "sys.columns",
    "primary_keys": "sys.indexes/sys.index_columns",
    "foreign_keys": "sys.foreign_keys/sys.foreign_key_columns",
}


class SqlStructuralDiscoverer:
    """SQL Server Phase-2 discoverer, invoked by the phase-level ``DiscoveryAgent``."""

    def __init__(
        self,
        store: ProjectStore,
        connection_factory: Callable[[str, int], Any] = sql_catalog.connect,
    ):
        self.store = store
        # Reuse the Phase-1 pyodbc connect seam; injectable for offline tests.
        self.connection_factory = connection_factory

    def run(self, request: SqlStructuralDiscoveryRequest) -> SqlStructuralDiscoveryResult:
        try:
            project_id = slugify(request.project_name)
            if not self.store.exists(project_id):
                return SqlStructuralDiscoveryResult(
                    success=False,
                    message="SQL structural discovery failed.",
                    error=(
                        f"No project '{project_id}' found. Run `adf2fabric intake` "
                        "first to establish the project."
                    ),
                )

            access = request.access
            server = (access.server or "").strip()
            database = (access.database or "").strip()

            if not server or not database:
                return self._persist_result(
                    project_id, server, database, "",
                    SqlDiscoveryStatus.missing_config,
                    detail="Both 'server' and 'database' are required.",
                )

            # TLS is an EXPLICIT policy — never inferred or defaulted.
            if access.tls is None:
                return self._persist_result(
                    project_id, server, database, "",
                    SqlDiscoveryStatus.missing_config,
                    detail=(
                        "An explicit TLS mode is required "
                        "(strict | trust_server_certificate | disabled)."
                    ),
                )

            # Resolve the credential reference (auth MODE recorded; secret never stored).
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
                        f"Unknown SQL auth '{cred.auth}'. Use 'sql_login' or 'integrated'."
                    ),
                )

            # Reuse the authoritative DSN builder; open ONE connection; run the three
            # structural queries on it; close it.
            conn_str = SqlServerConnector._connection_string(access, username, password)
            login_timeout = access.connect_timeout or DEFAULT_LOGIN_TIMEOUT
            conn = None
            try:
                conn = self.connection_factory(conn_str, login_timeout)
                columns, primary_keys, foreign_keys = catalog.run_structural_queries(conn)
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

            tables, relationships = _assemble_tables(columns, primary_keys, foreign_keys)
            return self._persist_result(
                project_id, server, database, auth,
                SqlDiscoveryStatus.discovered, detail="",
                tables=tables, relationships=relationships,
                metadata_queries=list(catalog.METADATA_QUERY_LABELS),
            )
        except ProjectNotFoundError as exc:
            return SqlStructuralDiscoveryResult(
                success=False, message="SQL structural discovery failed.", error=str(exc)
            )

    # --- assembly + persistence --------------------------------------------

    def _persist_result(
        self, project_id, server, database, authentication_mode, status, *, detail,
        tables=None, relationships=None, metadata_queries=None,
    ) -> SqlStructuralDiscoveryResult:
        discovery = self._assemble(
            project_id, server, database, authentication_mode, status, detail,
            tables or [], relationships or [], metadata_queries or [],
        )
        self.store.save_sql_structural_discovery(project_id, discovery)
        if status == SqlDiscoveryStatus.discovered:
            msg = (
                f"SQL structural discovery complete for '{project_id}': "
                f"{discovery.table_count} table(s), {discovery.column_count} column(s), "
                f"{discovery.primary_key_count} primary key(s), "
                f"{discovery.foreign_key_count} foreign key(s)."
            )
            return SqlStructuralDiscoveryResult(success=True, discovery=discovery, message=msg)
        return SqlStructuralDiscoveryResult(
            success=False, discovery=discovery,
            message="SQL structural discovery failed.",
            error=f"{status.value}: {detail}" if detail else status.value,
        )

    def _assemble(
        self, project_id, server, database, authentication_mode, status, detail,
        tables, relationships, metadata_queries,
    ) -> SqlStructuralDiscovery:
        now = datetime.now(timezone.utc)
        column_count = sum(len(t.columns) for t in tables)
        pk_count = sum(1 for t in tables if t.primary_key is not None)
        fk_count = sum(len(t.foreign_keys) for t in tables)
        discovery = SqlStructuralDiscovery(
            project_id=project_id,
            server=server,
            database=database,
            authentication=authentication_mode,
            status=status,
            created_at=now,
            updated_at=now,
            table_count=len(tables),
            column_count=column_count,
            primary_key_count=pk_count,
            foreign_key_count=fk_count,
            tables=tables,
            relationships=relationships,
            metadata_queries=metadata_queries,
            detail=detail,
            provenance=dict(_PROVENANCE),
        )
        if self.store.has_sql_structural_discovery(project_id):
            try:
                existing = self.store.load_sql_structural_discovery(project_id)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                discovery.created_at = existing.created_at
                if _semantic(existing) == _semantic(discovery):
                    discovery.updated_at = existing.updated_at
                else:
                    discovery.updated_at = now
        return discovery


def _assemble_tables(
    columns: list[SqlColumn],
    primary_keys: list[SqlPrimaryKey],
    foreign_keys: list[SqlForeignKey],
) -> tuple[list[SqlTableStructure], list[SqlRelationship]]:
    """Group columns + PK + FKs into per-table structures + a relationship graph."""
    pk_by_table = {(pk.schema, pk.table): pk for pk in primary_keys}
    fks_by_table: dict[tuple[str, str], list[SqlForeignKey]] = {}
    for fk in foreign_keys:
        fks_by_table.setdefault((fk.parent_schema, fk.parent_table), []).append(fk)

    tables: dict[tuple[str, str], SqlTableStructure] = {}
    for col in columns:
        key = (col.schema, col.table)
        struct = tables.get(key)
        if struct is None:
            struct = SqlTableStructure(schema=col.schema, table=col.table, columns=[])
            tables[key] = struct
        struct.columns.append(col)

    for key, struct in tables.items():
        struct.columns.sort(key=lambda c: c.ordinal)
        struct.primary_key = pk_by_table.get(key)
        struct.foreign_keys = sorted(
            fks_by_table.get(key, []), key=lambda f: f.name
        )

    table_list = sorted(tables.values(), key=lambda t: (t.schema, t.table))

    relationships = [
        SqlRelationship(
            parent_schema=fk.parent_schema, parent_table=fk.parent_table,
            referenced_schema=fk.referenced_schema, referenced_table=fk.referenced_table,
            foreign_key_name=fk.name, columns=list(fk.columns),
        )
        for fk in foreign_keys
    ]
    relationships.sort(
        key=lambda r: (r.parent_schema, r.parent_table, r.referenced_schema,
                       r.referenced_table, r.foreign_key_name)
    )
    return table_list, relationships


def _classify_error(exc: Exception) -> SqlDiscoveryStatus:
    """Map an expected pyodbc failure to a distinct, deterministic discovery status.

    Keyed on the SQLSTATE (reusing the connector's constant sets) + a safe message
    scan. Auth/permission failures are never collapsed into an empty result; an
    otherwise-unclassified failure is a metadata-query failure.
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


def _semantic(discovery: SqlStructuralDiscovery) -> dict:
    """Discovery content used to detect meaningful change (excludes timestamps)."""
    data = json.loads(discovery.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data
