"""Read-only inventory of an existing Fabric Warehouse over its SQL endpoint (Entra token, ODBC Driver 18).

Only SELECT statements run here: catalog views and COUNT_BIG(*). The Warehouse may be in another tenant (guest
access): the token is requested for the configured tenant. Nothing is created, altered or loaded.
"""

from __future__ import annotations

import struct
from typing import Any, Callable, Optional

from adf2fabric.agent.warehouse.models import Column, ObjectKind, TargetObject

SQL_RESOURCE = "https://database.windows.net/"
_KINDS = {"U": ObjectKind.table, "V": ObjectKind.view, "P": ObjectKind.procedure, "FN": ObjectKind.function,
          "IF": ObjectKind.function, "TF": ObjectKind.function}

OBJECTS_SQL = """SELECT s.name AS sch, o.name AS obj, RTRIM(o.type) AS typ
FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.is_ms_shipped = 0 AND o.type IN ('U','V','P','FN','IF','TF')"""
COLUMNS_SQL = """SELECT s.name AS sch, o.name AS obj, c.name AS col, ty.name AS typ, c.max_length, c.precision, c.scale,
       c.is_nullable
FROM sys.columns c JOIN sys.objects o ON o.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.types ty ON ty.user_type_id = c.user_type_id
WHERE o.is_ms_shipped = 0 AND o.type IN ('U','V') ORDER BY s.name, o.name, c.column_id"""


def format_type(typ: str, max_length: int, precision: int, scale: int) -> str:
    t = typ.lower()
    if t in ("varchar", "char", "varbinary", "binary"):
        return f"{t}({'max' if max_length == -1 else max_length})"
    if t in ("decimal", "numeric"):
        return f"decimal({precision},{scale})"
    if t in ("datetime2", "time"):
        return f"{t}({scale})"
    return t


def entra_connection(server: str, database: str, tenant: Optional[str], token_fn: Optional[Callable[..., str]] = None):
    import pyodbc

    if token_fn is None:
        from adf2fabric.connections.providers.azure_cli import AzureCliProvider

        token_fn = AzureCliProvider().token
    raw = token_fn(tenant=tenant, resource=SQL_RESOURCE).encode("utf-16-le")
    return pyodbc.connect(
        f"Driver={{ODBC Driver 18 for SQL Server}};Server={server},1433;Database={database};Encrypt=yes;TrustServerCertificate=no;",
        attrs_before={1256: struct.pack(f"<I{len(raw)}s", len(raw), raw)}, timeout=60)


def _rows(cur: Any, sql: str) -> list[dict[str, Any]]:
    cur.execute(sql)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def inventory(conn: Any, row_counts: bool = True) -> dict[str, TargetObject]:
    cur = conn.cursor()
    out: dict[str, TargetObject] = {}
    for r in _rows(cur, OBJECTS_SQL):
        o = TargetObject(schema_name=r["sch"], name=r["obj"], kind=_KINDS.get(r["typ"], ObjectKind.other))
        out[o.key] = o
    for r in _rows(cur, COLUMNS_SQL):
        o = out.get(f"{r['sch']}.{r['obj']}".lower())
        if o:
            o.columns.append(Column(name=r["col"], source_type=format_type(r["typ"], r["max_length"], r["precision"], r["scale"]),
                                    nullable=bool(r["is_nullable"])))
    if row_counts:
        for o in out.values():
            if o.kind == ObjectKind.table:
                n = o.schema_name.replace("]", "]]"), o.name.replace("]", "]]")
                o.row_count = _rows(cur, f"SELECT COUNT_BIG(*) AS n FROM [{n[0]}].[{n[1]}]")[0]["n"]
    return out
