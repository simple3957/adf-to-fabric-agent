"""SQL type and feature rules for a Microsoft Fabric Warehouse target.

Each rule names its evidence. ``observed`` rules were seen in a real Fabric Warehouse (transportationdedicated,
migrated from the same source, inspected read-only on 2026-10-15); ``documented`` rules come from Microsoft Learn
(https://learn.microsoft.com/fabric/data-warehouse/data-types and .../tsql-surface-area) and were not re-checked
in this run - re-check them before relying on them for a new source.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

DOCS_TYPES = "https://learn.microsoft.com/fabric/data-warehouse/data-types"
DOCS_TSQL = "https://learn.microsoft.com/fabric/data-warehouse/tsql-surface-area"


@dataclass(frozen=True)
class TypeResult:
    target: Optional[str]  # None = unsupported, needs a manual decision
    note: str = ""
    evidence: str = ""


_SAME = {"bit", "smallint", "int", "bigint", "real", "float", "date", "uniqueidentifier"}
_SIZED = {"char", "varchar", "binary", "varbinary"}


def map_type(source_type: str, identity: bool = False, default_varchar: int = 8000) -> TypeResult:
    t = source_type.strip().lower()
    m = re.match(r"^([a-z0-9_ ]+?)\s*(?:\((.*)\))?$", t)
    base, args = (m.group(1).strip(), (m.group(2) or "").replace(" ", "")) if m else (t, "")
    if identity:
        return TypeResult("bigint", "IDENTITY dropped: Fabric IDENTITY is bigint-only and cannot keep source values; "
                                    "values are loaded from the source.", "observed")
    if base in _SAME:
        return TypeResult(base)
    if base in ("decimal", "numeric"):
        return TypeResult(f"decimal({args or '18,0'})")
    if base in _SIZED:
        if not args:
            return TypeResult(f"{base}({default_varchar})", f"Length unknown (inferred type); {default_varchar} assumed - confirm.",
                              "inferred")
        return TypeResult(f"{base}({args})")
    if base == "tinyint":
        return TypeResult("smallint", "tinyint is not supported.", "observed")
    if base in ("datetime", "smalldatetime"):
        return TypeResult("datetime2(3)" if base == "datetime" else "datetime2(0)", f"{base} is not supported.", "observed")
    if base == "datetime2":
        p = int(args) if args.isdigit() else 6
        return TypeResult(f"datetime2({min(p, 6)})", "Precision capped at 6." if p > 6 else "", "documented")
    if base == "time":
        p = int(args) if args.isdigit() else 6
        return TypeResult(f"time({min(p, 6)})", "Precision capped at 6." if p > 6 else "", "documented")
    if base == "datetimeoffset":
        return TypeResult("datetime2(6)", "datetimeoffset is not supported: the offset is lost; store UTC.", "documented")
    if base in ("money", "smallmoney"):
        return TypeResult("decimal(19,4)" if base == "money" else "decimal(10,4)", f"{base} is not supported.", "documented")
    if base in ("nchar", "nvarchar"):
        n = args if args else "4000"
        size = "max" if n == "max" else str(min(int(n) * 2, 8000))
        return TypeResult(f"{base[1:]}({size})", f"{base} mapped to UTF-8 {base[1:]}; length doubled - check.", "documented")
    if base in ("text", "ntext"):
        return TypeResult("varchar(max)", f"{base} is not supported.", "documented")
    if base == "image":
        return TypeResult("varbinary(max)", "image is not supported.", "documented")
    return TypeResult(None, f"{base} has no Fabric Warehouse equivalent - manual decision.", "documented")


# Constructs in view/procedure/function bodies that need a manual rewrite or review on Fabric Warehouse.
TSQL_RULES: list[tuple[str, re.Pattern, str]] = [
    ("external", re.compile(r"\bOPENROWSET\b|\bEXTERNAL\s+TABLE\b", re.I), "PolyBase/OPENROWSET: no PolyBase in Fabric Warehouse."),
    ("ctas_with", re.compile(r"\bWITH\s*\(\s*(?:DISTRIBUTION|CLUSTERED|HEAP)", re.I), "Table options (DISTRIBUTION/CCI/HEAP) are not supported."),
    ("label", re.compile(r"\bOPTION\s*\(\s*LABEL\b", re.I), "OPTION (LABEL) is Synapse-only."),
    ("rename", re.compile(r"\bRENAME\s+OBJECT\b", re.I), "RENAME OBJECT: use sp_rename."),
    ("identity_insert", re.compile(r"\bSET\s+IDENTITY_INSERT\b", re.I), "IDENTITY_INSERT is not supported."),
    ("pdw", re.compile(r"\bsys\s*\.\s*(?:pdw_|dm_pdw_)\w+", re.I), "sys.pdw_* / dm_pdw_* views do not exist in Fabric."),
    ("cursor", re.compile(r"\bDECLARE\s+\w+\s+CURSOR\b", re.I), "Cursors are not supported."),
    ("trigger", re.compile(r"\bCREATE\s+TRIGGER\b", re.I), "Triggers are not supported."),
]


def tsql_findings(definition: str) -> list[str]:
    return [msg for _, pat, msg in TSQL_RULES if pat.search(definition)]
