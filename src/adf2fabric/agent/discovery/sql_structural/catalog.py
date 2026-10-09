"""SQL Server structural catalog queries (Phase 2 behavior).

Exactly THREE fixed, read-only catalog queries — columns, primary keys, foreign
keys — run against ``sys.*`` metadata views on a SINGLE connection. They read no
business data: no ``SELECT *``, no row/data reads, no aggregates, no profiling,
no writes/DDL/EXEC, no ``SELECT 1``, and no stored-procedure execution or body
inspection. Only user tables (``sys.tables``) are inspected.

The pyodbc connection seam is reused from the Phase-1 SQL package
(:func:`adf2fabric.agent.discovery.sql.catalog.connect`) — there is no second
connection/DSN builder. Determinism: deterministic ``ORDER BY`` plus a Python
re-sort so results never depend on driver/collation.
"""

from __future__ import annotations

from typing import Any

from adf2fabric.project.discovery import (
    SqlColumn,
    SqlForeignKey,
    SqlForeignKeyColumn,
    SqlKeyColumn,
    SqlPrimaryKey,
)

# 1. Columns of every user table (type + length/precision/scale + flags).
COLUMN_QUERY = (
    "SELECT s.name, t.name, c.column_id, c.name, ty.name, "
    "c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, c.is_computed "
    "FROM sys.columns c "
    "JOIN sys.tables t ON c.object_id = t.object_id "
    "JOIN sys.schemas s ON t.schema_id = s.schema_id "
    "JOIN sys.types ty ON c.user_type_id = ty.user_type_id "
    "ORDER BY s.name, t.name, c.column_id"
)

# 2. Primary-key columns (ordered by key position).
PRIMARY_KEY_QUERY = (
    "SELECT s.name, t.name, i.name, c.name, ic.key_ordinal "
    "FROM sys.indexes i "
    "JOIN sys.tables t ON i.object_id = t.object_id "
    "JOIN sys.schemas s ON t.schema_id = s.schema_id "
    "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
    "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
    "WHERE i.is_primary_key = 1 "
    "ORDER BY s.name, t.name, ic.key_ordinal"
)

# 3. Foreign-key columns with the referenced schema/table/column (direction: parent -> referenced).
FOREIGN_KEY_QUERY = (
    "SELECT fk.name, ps.name, pt.name, pc.name, rs.name, rt.name, rc.name, "
    "fkc.constraint_column_id "
    "FROM sys.foreign_keys fk "
    "JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id "
    "JOIN sys.tables pt ON fkc.parent_object_id = pt.object_id "
    "JOIN sys.schemas ps ON pt.schema_id = ps.schema_id "
    "JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id "
    "AND fkc.parent_column_id = pc.column_id "
    "JOIN sys.tables rt ON fkc.referenced_object_id = rt.object_id "
    "JOIN sys.schemas rs ON rt.schema_id = rs.schema_id "
    "JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id "
    "AND fkc.referenced_column_id = rc.column_id "
    "ORDER BY fk.name, fkc.constraint_column_id"
)

METADATA_QUERY_LABELS = ["sys.columns", "sys.indexes(primary_keys)", "sys.foreign_keys"]


def _int(value: Any) -> int | None:
    return int(value) if value is not None else None


def _bool(value: Any) -> bool:
    return bool(value)


def run_structural_queries(
    conn: Any,
) -> tuple[list[SqlColumn], list[SqlPrimaryKey], list[SqlForeignKey]]:
    """Run the three approved structural queries on ONE open connection.

    Returns ``(columns, primary_keys, foreign_keys)``. Opens a single cursor,
    executes the three statements in order, fetches, and closes the cursor; the
    caller owns (and closes) the connection.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(COLUMN_QUERY)
        columns = [
            SqlColumn(
                schema=r[0], table=r[1], ordinal=int(r[2]), name=r[3], data_type=r[4],
                max_length=_int(r[5]), precision=_int(r[6]), scale=_int(r[7]),
                is_nullable=_bool(r[8]), is_identity=_bool(r[9]), is_computed=_bool(r[10]),
            )
            for r in cursor.fetchall()
        ]
        columns.sort(key=lambda c: (c.schema, c.table, c.ordinal))

        cursor.execute(PRIMARY_KEY_QUERY)
        primary_keys = _group_primary_keys(cursor.fetchall())

        cursor.execute(FOREIGN_KEY_QUERY)
        foreign_keys = _group_foreign_keys(cursor.fetchall())
    finally:
        cursor.close()

    return columns, primary_keys, foreign_keys


def _group_primary_keys(rows: Any) -> list[SqlPrimaryKey]:
    """Group PK column rows into one SqlPrimaryKey per (schema, table)."""
    by_table: dict[tuple[str, str], SqlPrimaryKey] = {}
    for schema, table, pk_name, column, key_ordinal in rows:
        key = (schema, table)
        pk = by_table.get(key)
        if pk is None:
            pk = SqlPrimaryKey(schema=schema, table=table, name=pk_name, columns=[])
            by_table[key] = pk
        pk.columns.append(SqlKeyColumn(column=column, ordinal=int(key_ordinal)))
    result = list(by_table.values())
    for pk in result:
        pk.columns.sort(key=lambda kc: kc.ordinal)
    result.sort(key=lambda p: (p.schema, p.table))
    return result


def _group_foreign_keys(rows: Any) -> list[SqlForeignKey]:
    """Group FK column rows into one SqlForeignKey per (parent schema, table, name)."""
    by_fk: dict[tuple[str, str, str], SqlForeignKey] = {}
    for name, ps, pt, pc, rs, rt, rc, ordinal in rows:
        key = (ps, pt, name)
        fk = by_fk.get(key)
        if fk is None:
            fk = SqlForeignKey(
                name=name, parent_schema=ps, parent_table=pt,
                referenced_schema=rs, referenced_table=rt, columns=[],
            )
            by_fk[key] = fk
        fk.columns.append(
            SqlForeignKeyColumn(parent_column=pc, referenced_column=rc, ordinal=int(ordinal))
        )
    result = list(by_fk.values())
    for fk in result:
        fk.columns.sort(key=lambda fc: fc.ordinal)
    result.sort(key=lambda f: (f.parent_schema, f.parent_table, f.name))
    return result
