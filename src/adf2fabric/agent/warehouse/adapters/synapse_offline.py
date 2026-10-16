"""Synapse dedicated SQL pool, offline: authored DDL (Synapse Git ``sqlscript/*.json``) + a Parquet export.

Used when the pool cannot be queried (paused, no credits). What it can and cannot know:
- tables, external tables, views, procedures and their columns/definitions: **verified** from the authored DDL;
- objects that only exist in the export (no DDL anywhere): columns **inferred** from the Parquet schema;
- row counts: from the export, labelled with its source (not a live count);
- object type of export-only objects, statistics, partitions, security, functions not in Git: **needs_live**.
Secrets in the scripts are masked before parsing; only their count is kept.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.warehouse import adapter
from adf2fabric.agent.warehouse.models import CatalogObject, Column, ObjectKind, Provenance, SourceCatalog
from adf2fabric.agent.warehouse.secrets import mask

_SERVERLESS_POOLS = {"built-in"}

_IDENT = r"(?:\[[^\]]+\]|\"[^\"]+\"|[A-Za-z_#@][\w$#@]*)"
_QNAME = rf"(?:{_IDENT}\s*\.\s*)?{_IDENT}"
_CREATE = re.compile(
    rf"\bCREATE\s+(?:OR\s+ALTER\s+)?(?P<kind>SCHEMA|EXTERNAL\s+TABLE|TABLE|VIEW|PROC(?:EDURE)?|FUNCTION|"
    rf"(?:UNIQUE\s+)?(?:CLUSTERED\s+|NONCLUSTERED\s+)?INDEX|MASTER\s+KEY|EXTERNAL\s+DATA\s+SOURCE|"
    rf"EXTERNAL\s+FILE\s+FORMAT|DATABASE\s+SCOPED\s+CREDENTIAL)\b",
    re.I,
)
_STOP = re.compile(r"^\s*(?:GO|-{6,}.*)\s*$", re.I | re.M)  # batch separator or a "------" divider line
_INSERT = re.compile(rf"\bINSERT\s+INTO\s+(?P<obj>{_QNAME})", re.I)
_COLUMN = re.compile(
    rf"^(?P<name>{_IDENT})\s+(?P<type>\[[^\]]+\]|[A-Za-z_]\w*)\s*(?P<args>\(\s*(?:max|\d+)(?:\s*,\s*\d+)?\s*\))?(?P<rest>.*)$",
    re.I | re.S,
)
_CONSTRAINT_START = re.compile(r"^(?:CONSTRAINT|PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK|INDEX)\b", re.I)


def _unquote(ident: str) -> str:
    ident = ident.strip()
    return ident[1:-1] if ident[:1] in "[\"" else ident


def split_qname(qname: str, default_schema: str = "dbo") -> tuple[str, str]:
    parts = [_unquote(p) for p in re.findall(_IDENT, qname)]
    return (parts[0], parts[1]) if len(parts) >= 2 else (default_schema, parts[0])


def _balanced(text: str, start: int) -> tuple[str, int]:
    """Text inside the parenthesis group opening at/after ``start`` and the index after it."""
    i = text.index("(", start)
    depth = 0
    for j in range(i, len(text)):
        if text[j] == "(":
            depth += 1
        elif text[j] == ")":
            depth -= 1
            if depth == 0:
                return text[i + 1 : j], j + 1
    return text[i + 1 :], len(text)


def _split_top(text: str) -> list[str]:
    out, depth, cur = [], 0, []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return [p.strip() for p in out if p.strip()]


def parse_columns(body: str) -> tuple[list[Column], list[str]]:
    cols: list[Column] = []
    constraints: list[str] = []
    for part in _split_top(body):
        flat = " ".join(part.split())
        if _CONSTRAINT_START.match(flat):
            constraints.append(flat)
            continue
        m = _COLUMN.match(flat)
        if not m:
            constraints.append(flat)
            continue
        base = _unquote(m.group("type").strip()).lower()
        args = (m.group("args") or "").replace(" ", "").lower()
        rest = m.group("rest").upper()
        nullable: Optional[bool] = False if "NOT NULL" in rest else (True if re.search(r"\bNULL\b", rest) else None)
        cols.append(Column(name=_unquote(m.group("name")), source_type=f"{base}{args}", nullable=nullable,
                           identity="IDENTITY" in rest))
    return cols, constraints


def parse_with_options(text: str) -> dict[str, Any]:
    props: dict[str, Any] = {}
    m = re.search(r"DISTRIBUTION\s*=\s*(HASH\s*\([^)]*\)|REPLICATE|ROUND_ROBIN)", text, re.I)
    if m:
        d = " ".join(m.group(1).split())
        props["distribution"] = re.sub(r"^(\w+)", lambda k: k.group(1).upper(), d)
    if re.search(r"CLUSTERED\s+COLUMNSTORE\s+INDEX", text, re.I):
        props["storage"] = "CLUSTERED COLUMNSTORE INDEX"
    elif m := re.search(r"CLUSTERED\s+INDEX\s*\(([^)]*)\)", text, re.I):
        props["storage"] = f"CLUSTERED INDEX ({' '.join(m.group(1).split())})"
    elif re.search(r"\bHEAP\b", text, re.I):
        props["storage"] = "HEAP"
    if re.search(r"\bPARTITION\s*\(", text, re.I):
        props["partitioned"] = True
    for key in ("LOCATION", "DATA_SOURCE", "FILE_FORMAT"):
        if m := re.search(rf"\b{key}\s*=\s*('[^']*'|\[[^\]]+\]|\w+)", text, re.I):
            props[key.lower()] = m.group(1).strip("'[]")
    return props


def _module_end(text: str, start: int) -> int:
    """End of a view/procedure/function statement: the BEGIN/END block, else the first ';' at depth 0."""
    tokens = re.finditer(r"\bBEGIN\b(?!\s+(?:TRAN|TRANSACTION|TRY|CATCH|DISTRIBUTED)\b)|\bEND\b(?!\s+(?:TRY|CATCH)\b)|;", text[start:], re.I)
    depth, seen_begin = 0, False
    for t in tokens:
        tok = t.group(0).upper()
        if tok == "BEGIN":
            depth += 1
            seen_begin = True
        elif tok == "END" and depth:
            depth -= 1
            if depth == 0 and seen_begin:
                end = start + t.end()
                return end + 1 if text[end : end + 1] == ";" else end
        elif tok == ";" and depth == 0 and not seen_begin:
            return start + t.end()
    return len(text)


def parse_script(text: str, source: str) -> list[CatalogObject]:
    """Parse (already masked) T-SQL into catalog objects. Index statements become ``index`` pseudo-objects
    that the adapter folds into their table."""
    objs: list[CatalogObject] = []
    matches = list(_CREATE.finditer(text))
    for n, m in enumerate(matches):
        kind = " ".join(m.group("kind").upper().split())
        nxt = matches[n + 1].start() if n + 1 < len(matches) else len(text)
        stop = _STOP.search(text, m.end(), nxt)
        stmt_end = stop.start() if stop else nxt
        after = text[m.end() : stmt_end]
        if kind == "SCHEMA":
            continue
        if kind in ("TABLE", "EXTERNAL TABLE"):
            name_m = re.match(rf"\s*({_QNAME})", after)
            if not name_m:
                continue
            schema, name = split_qname(name_m.group(1))
            body, pos = _balanced(after, name_m.end())
            cols, constraints = parse_columns(body)
            props = parse_with_options(after[pos:])
            if constraints:
                props["constraints"] = constraints
            objs.append(CatalogObject(
                schema_name=schema, name=name, kind=ObjectKind.external_table if "EXTERNAL" in kind else ObjectKind.table,
                columns=cols, properties=props, sources=[source], definition=text[m.start() : stmt_end].strip()))
        elif kind in ("VIEW", "PROC", "PROCEDURE", "FUNCTION"):
            name_m = re.match(rf"\s*({_QNAME})", after)
            if not name_m:
                continue
            schema, name = split_qname(name_m.group(1))
            end = min(_module_end(text, m.end()), stmt_end) if kind != "VIEW" else stmt_end
            k = {"VIEW": ObjectKind.view, "FUNCTION": ObjectKind.function}.get(kind, ObjectKind.procedure)
            objs.append(CatalogObject(schema_name=schema, name=name, kind=k, sources=[source],
                                      definition=text[m.start() : end].strip()))
        elif kind.endswith("INDEX"):
            im = re.match(rf"\s*({_IDENT})\s+ON\s+({_QNAME})\s*\(([^)]*)\)", after, re.I)
            if im:
                schema, table = split_qname(im.group(2))
                objs.append(CatalogObject(schema_name=schema, name=table, kind=ObjectKind.other, sources=[source],
                                          properties={"index": f"{kind} {_unquote(im.group(1))} ({' '.join(im.group(3).split())})"}))
        else:  # MASTER KEY, EXTERNAL DATA SOURCE / FILE FORMAT, DATABASE SCOPED CREDENTIAL
            name_m = re.match(rf"\s*({_IDENT})", after) if kind != "MASTER KEY" else None
            label = _unquote(name_m.group(1)) if name_m else kind
            objs.append(CatalogObject(schema_name="", name=label, kind=ObjectKind.other, sources=[source],
                                      properties={"statement": kind}, definition=text[m.start() : stmt_end].strip()))
    return objs


_ARROW_SQL = [
    (r"^int8$", "tinyint"), (r"^int16$", "smallint"), (r"^int32$", "int"), (r"^int64$", "bigint"),
    (r"^(?:large_)?string$", "varchar"), (r"^bool$", "bit"), (r"^double$", "float"), (r"^float$", "real"),
    (r"^date32", "date"), (r"^timestamp", "datetime2"), (r"^(?:large_)?binary$", "varbinary"),
]


def arrow_to_sql(arrow_type: str) -> str:
    if m := re.match(r"decimal\d*\((\d+),\s*(\d+)\)", arrow_type):
        return f"decimal({m.group(1)},{m.group(2)})"
    for pat, sql in _ARROW_SQL:
        if re.match(pat, arrow_type):
            return sql
    return f"unknown({arrow_type})"


class SynapseOfflineAdapter:
    source_type = "synapse_dedicated"
    mode = "offline"

    def __init__(self, options: dict[str, Any]):
        self.scripts_dir = Path(options["scripts_dir"]) if options.get("scripts_dir") else None
        self.parquet_dir = Path(options["parquet_dir"]) if options.get("parquet_dir") else None
        self.export_label = options.get("export_label", "parquet export")
        self.pools = {p.lower() for p in options.get("pools", [])}  # restrict to these Synapse pools (empty: all dedicated)

    def capabilities(self) -> dict[str, bool]:
        return {"columns": True, "definitions": True, "row_counts": bool(self.parquet_dir), "distribution": True,
                "dependencies_live": False, "statistics": False, "security": False}

    def _scripts(self) -> list[tuple[str, str, str]]:
        """(file name, pool, query) for dedicated-pool scripts."""
        out = []
        if not self.scripts_dir:
            return out
        for f in sorted(self.scripts_dir.glob("*.json")):
            props = json.loads(f.read_text(encoding="utf-8-sig")).get("properties", {})
            content = props.get("content", {})
            pool = (content.get("currentConnection") or {}).get("poolName") or ""
            if pool.lower() in _SERVERLESS_POOLS or (self.pools and pool.lower() not in self.pools):
                continue
            out.append((f.name, pool, content.get("query") or ""))
        return out

    def discover(self) -> SourceCatalog:
        cat = SourceCatalog(source_type=self.source_type, mode=self.mode, discovered_at=datetime.now(timezone.utc))
        by_key: dict[str, CatalogObject] = {}
        indexes: list[CatalogObject] = []
        seeds: dict[str, int] = {}
        pools: set[str] = set()
        for fname, pool, query in self._scripts():
            pools.add(pool)
            masked, n = mask(query)
            cat.secrets_masked += n
            if n:
                cat.notes.append(f"{fname}: {n} secret value(s) masked; rotate them at the source and keep them out of Git.")
            for o in parse_script(masked, f"git:sqlscript/{fname}"):
                if o.kind == ObjectKind.other and "index" in o.properties:
                    indexes.append(o)
                elif o.key in by_key:
                    by_key[o.key].notes.append(f"Also defined in {fname}; first definition kept.")
                else:
                    by_key[o.key] = o
            for im in _INSERT.finditer(masked):
                k = "{}.{}".format(*split_qname(im.group("obj"))).lower()
                seeds[k] = seeds.get(k, 0) + 1
        for ix in indexes:
            if t := by_key.get(ix.key):
                t.properties.setdefault("indexes", []).append(ix.properties["index"])
            else:
                cat.notes.append(f"Index on {ix.key} ({ix.properties['index']}) refers to a table with no DDL.")
        for k, count in seeds.items():
            if k in by_key:
                by_key[k].properties["seed_inserts"] = count
        if pools:
            cat.notes.append(f"DDL scripts target pool(s): {', '.join(sorted(pools))}.")
        self._add_parquet(by_key, cat)
        for o in by_key.values():
            if o.kind in (ObjectKind.table, ObjectKind.external_table) and o.row_count is None:
                o.notes.append("Row count needs a live source or an export.")
        cat.objects = sorted(by_key.values(), key=lambda o: (o.kind != ObjectKind.other, o.schema_name, o.name))
        return cat

    def _add_parquet(self, by_key: dict[str, CatalogObject], cat: SourceCatalog) -> None:
        if not self.parquet_dir:
            return
        import pyarrow.parquet as pq

        for f in sorted(self.parquet_dir.glob("*.parquet")):
            schema, _, name = f.stem.partition("_")
            if not name:
                cat.notes.append(f"Export file {f.name} does not follow <schema>_<object>.parquet; skipped.")
                continue
            meta = pq.read_metadata(f)
            arrow = pq.read_schema(f)
            key = f"{schema}.{name}".lower()
            src = f"export:{f.name}"
            o = by_key.get(key)
            if o is None:
                o = CatalogObject(
                    schema_name=schema, name=name, kind=ObjectKind.table, provenance=Provenance.inferred,
                    columns=[Column(name=fld.name, source_type=arrow_to_sql(str(fld.type)), nullable=fld.nullable,
                                    provenance=Provenance.inferred) for fld in arrow],
                    notes=["No DDL found: object type and column types inferred from the export; confirm with a live source."],
                )
                by_key[key] = o
            else:
                ddl_cols = {c.name.lower() for c in o.columns}
                exp_cols = {fld.name.lower() for fld in arrow}
                if o.columns and ddl_cols != exp_cols:
                    o.notes.append(f"Export columns differ from DDL: only in DDL {sorted(ddl_cols - exp_cols)}, "
                                   f"only in export {sorted(exp_cols - ddl_cols)}.")
            o.row_count = meta.num_rows
            o.row_count_source = self.export_label
            o.sources.append(src)


adapter.register("synapse_offline", SynapseOfflineAdapter)
