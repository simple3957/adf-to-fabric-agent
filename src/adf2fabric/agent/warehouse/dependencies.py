"""Which warehouse objects does ADF use? Links the ADF semantic record to the source catalog.

Reference kinds found (all read from the stored ADF semantic record, nothing is called):
- ``dataset``: a SQL dataset's literal schema/table (static), expression (dynamic) or no table at all (broken);
- ``activity_query``: object names in a literal query; an expression query is dynamic;
- ``catalog_query``: a query over ``sys.objects``/``INFORMATION_SCHEMA`` filtered by type (e.g. U, V, ET) feeding a
  ForEach: at run time it covers every object of those types (dynamic, scope known);
- ``stored_procedure``: a stored-procedure activity's procedure;
- ``parameter_value``: object names in parameter defaults (mentioned; the pipeline may never query them).

Usage per object: ``direct`` (static dataset/query/procedure), ``dynamic`` (only via run-time resolution),
``indirect`` (used by a view/procedure that ADF uses), ``mentioned`` (name in a parameter only) or ``standalone``.
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterable, Optional

from adf2fabric.agent.warehouse.models import AdfReference, CatalogObject, ObjectKind, Resolution, SourceCatalog
from adf2fabric.project.discovery import AdfActivity, AdfSemanticDiscovery

SQL_LINKED_SERVICE_TYPES = {"azuresqldw", "azuresqldatabase", "sqlserver", "azuresqlmi", "warehouse", "datawarehouse"}
_SYS_TYPES = {"U": ObjectKind.table, "V": ObjectKind.view, "ET": ObjectKind.external_table, "P": ObjectKind.procedure,
              "FN": ObjectKind.function, "IF": ObjectKind.function, "TF": ObjectKind.function}


def _name_pattern(o: CatalogObject) -> re.Pattern:
    s, n = re.escape(o.schema_name), re.escape(o.name)
    return re.compile(rf"(?<![\w\]])(?:\[{s}\]|{s})\s*\.\s*(?:\[{n}\]|{n})(?![\w\[])", re.I)


def find_objects(text: str, catalog: SourceCatalog) -> list[str]:
    return [o.key for o in catalog.objects if o.schema_name and _name_pattern(o).search(text)]


def _is_expr(v: Any) -> bool:
    return isinstance(v, dict) and v.get("type") == "Expression" or (isinstance(v, str) and v.startswith("@"))


def _walk(acts: Iterable[AdfActivity]) -> Iterable[AdfActivity]:
    for a in acts:
        yield a
        yield from _walk(a.activities)


def _catalog_query_types(query: str) -> Optional[list[ObjectKind]]:
    """``sys.objects ... type IN ('U','V','ET')`` -> the kinds it enumerates (None if not a catalog query)."""
    if not re.search(r"\bsys\s*\.\s*(?:objects|tables|views|external_tables)\b|\bINFORMATION_SCHEMA\s*\.\s*TABLES\b", query, re.I):
        return None
    m = re.search(r"\btype\s+IN\s*\(([^)]*)\)", query, re.I) or re.search(r"\btype\s*=\s*('[^']*')", query, re.I)
    if m:
        codes = [c.strip().strip("'\"").upper() for c in m.group(1).split(",")]
        return [_SYS_TYPES[c] for c in codes if c in _SYS_TYPES]
    if re.search(r"sys\s*\.\s*views", query, re.I):
        return [ObjectKind.view]
    return [ObjectKind.table]


def analyze(adf: AdfSemanticDiscovery, catalog: SourceCatalog, source_database: Optional[str] = None) -> tuple[list[AdfReference], list[str]]:
    """Return (references, notes). ``source_database`` flags SQL linked services that point elsewhere."""
    refs: list[AdfReference] = []
    notes: list[str] = []
    sql_ls = {ls.name: ls for ls in adf.linked_services if (ls.type or "").lower() in SQL_LINKED_SERVICE_TYPES}
    for ls in sql_ls.values():
        db = ls.connection.database if ls.connection else None
        if db and source_database and db.lower() != source_database.lower():
            notes.append(f"Linked service {ls.name} points at database '{db}', not the source '{source_database}'.")
        elif not db:
            notes.append(f"Linked service {ls.name}: database not stated in the definition"
                         f"{' (parameterized: ' + ', '.join(ls.connection.parameterized_fields) + ')' if ls.connection and ls.connection.parameterized_fields else ''}; "
                         "assumed to be the source warehouse - confirm.")
    sql_ds = {d.name: d for d in adf.datasets if d.linked_service_ref and d.linked_service_ref.name in sql_ls}

    for d in sql_ds.values():
        tp = d.type_properties
        schema, table = tp.get("schema"), tp.get("table") or tp.get("tableName")
        ls = d.linked_service_ref.name
        if table is None and schema is None:
            refs.append(AdfReference(asset_kind="dataset", asset=d.name, linked_service=ls, resolution=Resolution.broken,
                                     note="SQL dataset names no table; any activity using it must supply a query."))
        elif _is_expr(schema) or _is_expr(table):
            refs.append(AdfReference(asset_kind="dataset", asset=d.name, linked_service=ls, resolution=Resolution.dynamic,
                                     note=f"Table resolved at run time from parameters {[p.name for p in d.parameters]}."))
        else:
            key = f"{schema or 'dbo'}.{table}".lower() if "." not in str(table) else str(table).lower()
            found = catalog.get(key)
            unused = [p.name for p in d.parameters]
            refs.append(AdfReference(
                object_key=key, asset_kind="dataset", asset=d.name, linked_service=ls,
                resolution=Resolution.static if found else Resolution.broken,
                note=("" if found else "Object not in the source catalog. ")
                + (f"Declares parameters {unused} that the table does not use." if unused else "")))

    for p in adf.pipelines:
        params = {x.name: x.default_raw for x in p.parameters if x.has_default}
        if params:
            for key in find_objects(json.dumps(params, default=str), catalog):
                refs.append(AdfReference(object_key=key, asset_kind="parameter_value", asset=p.name, pipeline=p.name,
                                         note="Named in a pipeline parameter default; check whether the pipeline queries it."))
        catalog_lookups: dict[str, list[ObjectKind]] = {}
        for a in _walk(p.activities):
            for io in a.inputs + a.outputs:
                if io.name not in sql_ds:
                    continue
                q = io.location.get("query")
                if isinstance(q, str) and q and not io.location.get("query_is_expression"):
                    kinds = _catalog_query_types(q)
                    if kinds is not None:
                        catalog_lookups[a.name] = kinds
                        continue
                    for key in find_objects(q, catalog):
                        refs.append(AdfReference(object_key=key, asset_kind="activity_query", asset=f"{p.name}/{a.name}",
                                                 pipeline=p.name, linked_service=sql_ds[io.name].linked_service_ref.name))
                elif q:
                    refs.append(AdfReference(asset_kind="activity_query", asset=f"{p.name}/{a.name}", pipeline=p.name,
                                             linked_service=sql_ds[io.name].linked_service_ref.name, resolution=Resolution.dynamic,
                                             note=f"Query built at run time: {' '.join(str(q).split())[:120]}"))
            for sp in a.stored_procedures:
                found = catalog.get(sp if "." in sp else f"dbo.{sp}")
                refs.append(AdfReference(object_key=found.key if found else sp.lower(), asset_kind="stored_procedure",
                                         asset=f"{p.name}/{a.name}", pipeline=p.name,
                                         resolution=Resolution.static if found else Resolution.broken,
                                         note="" if found else "Procedure not in the source catalog."))
        for a in _walk(p.activities):  # ForEach over a catalog Lookup: every object of those kinds, at run time
            items = json.dumps(a.raw_properties.get("items", ""), default=str)
            for lookup, kinds in catalog_lookups.items():
                if f"activity('{lookup}')" in items:
                    for o in catalog.objects:
                        if o.kind in kinds:
                            refs.append(AdfReference(object_key=o.key, asset_kind="catalog_query", asset=f"{p.name}/{a.name}",
                                                     pipeline=p.name, resolution=Resolution.dynamic,
                                                     note=f"Enumerated at run time by {lookup} (types: {', '.join(k.value for k in kinds)})."))
    return refs, notes


def object_dependencies(catalog: SourceCatalog) -> dict[str, list[str]]:
    """View/procedure/function -> catalog objects its definition names."""
    deps: dict[str, list[str]] = {}
    for o in catalog.objects:
        if o.kind in (ObjectKind.view, ObjectKind.procedure, ObjectKind.function) and o.definition:
            deps[o.key] = [k for k in find_objects(o.definition, catalog) if k != o.key]
    return deps


def classify(catalog: SourceCatalog, refs: list[AdfReference], deps: dict[str, list[str]]) -> dict[str, str]:
    usage = {o.key: "standalone" for o in catalog.objects if o.schema_name}
    rank = {"standalone": 0, "mentioned": 1, "indirect": 2, "dynamic": 3, "direct": 4}

    def bump(key: str, level: str) -> None:
        if key in usage and rank[level] > rank[usage[key]]:
            usage[key] = level

    for r in refs:
        if not r.object_key:
            continue
        if r.asset_kind == "parameter_value":
            bump(r.object_key, "mentioned")
        elif r.resolution == Resolution.dynamic:
            bump(r.object_key, "dynamic")
        elif r.resolution == Resolution.static:
            bump(r.object_key, "direct")
    changed = True
    while changed:  # objects used by a used view/procedure are indirect
        changed = False
        for parent, children in deps.items():
            if usage.get(parent) in ("direct", "dynamic", "indirect"):
                for c in children:
                    if rank[usage.get(c, "standalone")] < rank["indirect"]:
                        usage[c] = "indirect"
                        changed = True
    return usage


def select_scope(catalog: SourceCatalog, usage: dict[str, str], deps: dict[str, list[str]],
                 mode: str = "adf_related") -> tuple[set[str], dict[str, str]]:
    """(included object keys, {excluded key: reason}).

    ``adf_related``: objects ADF uses (statically or at run time), then the closure over definitions both ways:
    objects a selected view/procedure reads, and views/procedures that read a selected object.
    ``all``: every schema object. Server-level objects (master key, data sources) are never selected; they are manual.
    """
    keys = {o.key for o in catalog.objects if o.schema_name}
    if mode == "all":
        return keys, {}
    if mode != "adf_related":
        raise ValueError(f"Unknown scope mode '{mode}' (use adf_related or all).")
    reason: dict[str, str] = {k: f"used by ADF ({usage[k]})" for k in keys if usage.get(k) in ("direct", "dynamic")}
    selected = set(reason)
    changed = True
    while changed:
        changed = False
        for parent, children in deps.items():
            if parent in selected:
                for c in children:
                    if c in keys and c not in selected:
                        selected.add(c)
                        reason[c] = f"read by {parent}"
                        changed = True
            elif any(c in selected for c in children) and parent in keys:
                selected.add(parent)
                reason[parent] = f"reads {', '.join(c for c in children if c in selected)}"
                changed = True
    excluded = {k: ("only named in a parameter, never queried" if usage.get(k) == "mentioned" else "not used by ADF")
                for k in keys - selected}
    return selected, excluded
