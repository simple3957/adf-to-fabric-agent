"""Manifest + dependency order + dry-run DDL. Never executes anything against a target."""

from __future__ import annotations

from typing import Optional

from adf2fabric.agent.warehouse.models import (
    AdfReference, CatalogObject, ManifestEntry, ObjectKind, Provenance, SourceCatalog, TargetObject,
)
from adf2fabric.agent.warehouse.typemap import map_type, tsql_findings

_ORDER = {ObjectKind.table: 0, ObjectKind.external_table: 1, ObjectKind.function: 2, ObjectKind.view: 3, ObjectKind.procedure: 4}


def table_ddl(o: CatalogObject, schema: str, name: str, default_varchar: int = 8000) -> tuple[str, list[str], list[str]]:
    """(DDL, transformations, warnings) for a table on Fabric Warehouse."""
    lines, xf, warn = [], [], []
    for c in o.columns:
        r = map_type(c.source_type, c.identity, default_varchar)
        if r.target is None:
            warn.append(f"{c.name}: {r.note}")
            continue
        if r.target != c.source_type or r.note:
            xf.append(f"{c.name}: {c.source_type}{' IDENTITY' if c.identity else ''} -> {r.target}"
                      + (f" ({r.note})" if r.note else ""))
        null = "" if c.nullable is None else (" NULL" if c.nullable else " NOT NULL")
        lines.append(f"    [{c.name}] {r.target}{null}")
    p = o.properties
    if p.get("distribution"):
        xf.append(f"DISTRIBUTION = {p['distribution']} dropped (Fabric manages distribution).")
    if p.get("storage"):
        xf.append(f"{p['storage']} dropped (Fabric storage is always columnar Delta).")
    for ix in p.get("indexes", []):
        xf.append(f"{ix} dropped (no user indexes in Fabric Warehouse).")
    if p.get("partitioned"):
        xf.append("Partitioning dropped.")
    for con in p.get("constraints", []):
        warn.append(f"Constraint not generated, add as NOT ENFORCED if needed: {con}")
    if o.provenance != Provenance.verified:
        warn.append("Structure inferred from an export, not from DDL - confirm before deploying.")
    ddl = f"CREATE TABLE [{schema}].[{name}]\n(\n" + ",\n".join(lines) + "\n);"
    return ddl, xf, warn


def build_manifest(
    catalog: SourceCatalog,
    usage: dict[str, str],
    refs: list[AdfReference],
    deps: dict[str, list[str]],
    target: Optional[dict[str, TargetObject]] = None,
    schema_map: Optional[dict[str, str]] = None,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
    offline_load_method: str = "copy_into_from_parquet_export",
    default_varchar: int = 8000,
    out_of_scope: Optional[dict[str, str]] = None,
) -> list[ManifestEntry]:
    schema_map = {k.lower(): v for k, v in (schema_map or {}).items()}
    inc = {k.lower() for k in include or []}
    exc = {k.lower() for k in exclude or []}
    by_obj: dict[str, list[str]] = {}
    for r in refs:
        if r.object_key:
            by_obj.setdefault(r.object_key, []).append(f"{r.asset_kind}:{r.asset}")
    out: list[ManifestEntry] = []
    for o in catalog.objects:
        if o.kind == ObjectKind.other:
            out.append(ManifestEntry(
                source_key=o.name.lower(), kind=o.kind, target_schema="", target_name=o.name, action="manual",
                adf_usage="n/a", warnings=[f"{o.properties.get('statement', 'Server-level object')}: not migrated; Fabric "
                                           "has no PolyBase/master key. Use OneLake shortcuts or a Fabric connection."]))
            continue
        tschema = schema_map.get(o.schema_name.lower(), o.schema_name)
        e = ManifestEntry(source_key=o.key, kind=o.kind, target_schema=tschema, target_name=o.name, action="create",
                          adf_usage=usage.get(o.key, "standalone"), adf_references=sorted(set(by_obj.get(o.key, []))),
                          depends_on=deps.get(o.key, []))
        if (inc and o.key not in inc) or o.key in exc:
            e.action = "skip"
            e.warnings.append("Excluded by configuration.")
        elif out_of_scope and o.key in out_of_scope:
            e.action = "skip"
            e.warnings.append(f"Out of scope: {out_of_scope[o.key]}.")
        elif o.kind == ObjectKind.table:
            e.ddl, e.transformations, e.warnings = table_ddl(o, tschema, o.name, default_varchar)
            e.load_method = offline_load_method if catalog.mode == "offline" else "pipeline_copy_from_source"
            if catalog.mode == "offline" and o.row_count is None:
                e.warnings.append("No export to load from.")
        elif o.kind == ObjectKind.external_table:
            e.action = "manual"
            e.transformations.append(f"External table over {o.properties.get('data_source', '?')}/{o.properties.get('location', '?')}: "
                                     "recreate as a regular table loaded from the files, or a OneLake shortcut + view.")
        else:
            e.transformations.append("Definition copied as is; compile in the target to confirm.")
            e.warnings.extend(tsql_findings(o.definition or ""))
        if e.adf_usage == "standalone" and e.action != "skip":
            e.warnings.append("Not used by any ADF pipeline directly.")
        tkey = f"{tschema}.{o.name}".lower()
        if target is not None and tkey in target and e.action == "create":
            e.action = "exists"
            e.warnings.append("Already in the target: not recreated or overwritten (conflict policy 'fail' needs approval to change).")
        out.append(e)
    return order(out)


def order(entries: list[ManifestEntry]) -> list[ManifestEntry]:
    """Kind first (tables before views before procedures), then dependencies before dependents."""
    keyed = {e.source_key: e for e in entries}
    done: list[ManifestEntry] = []
    seen: set[str] = set()

    def visit(e: ManifestEntry, stack: tuple[str, ...] = ()) -> None:
        if e.source_key in seen or e.source_key in stack:
            return
        for d in e.depends_on:
            if d in keyed:
                visit(keyed[d], stack + (e.source_key,))
        seen.add(e.source_key)
        done.append(e)

    for e in sorted(entries, key=lambda x: (x.kind != ObjectKind.other, _ORDER.get(x.kind, 9), x.source_key)):
        visit(e)
    return done
