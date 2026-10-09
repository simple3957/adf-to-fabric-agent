"""SQL Server catalog metadata queries (Phase 1 behavior).

The ONLY database operations performed here are four fixed, read-only catalog
queries against ``sys.schemas`` / ``sys.tables`` / ``sys.views`` /
``sys.procedures``. They run on a SINGLE connection: the caller opens one
connection, this module runs all four queries on it, then closes the cursor;
the caller closes the connection. Nothing else is executed — no ``SELECT 1``,
no row/data reads, no columns/keys, no writes, no DDL, no EXEC, no user SQL.

The ``pyodbc`` connection is built behind a seam so tests inject a fake and
prove exactly the four approved statements are executed.
"""

from __future__ import annotations

from typing import Any

from adf2fabric.project.discovery import SqlObject

# The four approved, read-only catalog queries. Deterministic ORDER BY; the
# results are additionally sorted in Python so ordering never depends on the
# driver/collation. These are the ONLY statements SQL Discovery Phase 1 runs.
SCHEMA_QUERY = "SELECT name FROM sys.schemas ORDER BY name"
TABLE_QUERY = (
    "SELECT s.name, t.name FROM sys.tables t "
    "JOIN sys.schemas s ON t.schema_id = s.schema_id "
    "ORDER BY s.name, t.name"
)
VIEW_QUERY = (
    "SELECT s.name, v.name FROM sys.views v "
    "JOIN sys.schemas s ON v.schema_id = s.schema_id "
    "ORDER BY s.name, v.name"
)
PROCEDURE_QUERY = (
    "SELECT s.name, p.name FROM sys.procedures p "
    "JOIN sys.schemas s ON p.schema_id = s.schema_id "
    "ORDER BY s.name, p.name"
)

# Purpose labels persisted as the record of metadata queries performed (never
# secret-bearing, never the raw SQL text of anything else).
METADATA_QUERY_LABELS = ["sys.schemas", "sys.tables", "sys.views", "sys.procedures"]


def connect(conn_str: str, timeout: int) -> Any:
    """Open a single pyodbc connection (lazy import seam).

    Raises ``ImportError`` if pyodbc is not installed; the caller maps that to a
    ``dependency_unavailable`` outcome. The connection string is built by the
    authoritative connectivity DSN builder and is never surfaced here.
    """
    import pyodbc

    return pyodbc.connect(conn_str, timeout=timeout)


def run_metadata_queries(
    conn: Any,
) -> tuple[list[str], list[SqlObject], list[SqlObject], list[SqlObject]]:
    """Run exactly the four approved catalog queries on ONE open connection.

    Returns ``(schemas, tables, views, procedures)`` with schemas sorted by name
    and every object list sorted by ``(schema, name)``. Opens a single cursor,
    executes the four statements in order, fetches, and closes the cursor. The
    caller owns the connection lifecycle and closes it.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(SCHEMA_QUERY)
        schemas = sorted(row[0] for row in cursor.fetchall())

        cursor.execute(TABLE_QUERY)
        tables = _objects(cursor.fetchall())

        cursor.execute(VIEW_QUERY)
        views = _objects(cursor.fetchall())

        cursor.execute(PROCEDURE_QUERY)
        procedures = _objects(cursor.fetchall())
    finally:
        cursor.close()

    return schemas, tables, views, procedures


def _objects(rows: Any) -> list[SqlObject]:
    """Build a deterministically sorted list of schema-qualified objects."""
    objects = [SqlObject(schema=row[0], name=row[1]) for row in rows]
    objects.sort(key=lambda o: (o.schema, o.name))
    return objects
