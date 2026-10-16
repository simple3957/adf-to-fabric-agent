"""Point Fabric pipeline activities that read the old source warehouse at the migrated Fabric Warehouse.

An activity is repointed when its dataset settings use one of ``from_connections`` (Fabric connection ids, e.g. the
Synapse SQL connection the converted pipelines were bound to). Its source becomes ``DataWarehouseSource`` and its
dataset a ``DataWarehouseTable`` on the Warehouse, referenced by item id. Queries, schema/table and every other
setting stay as authored. Sinks that write to the old warehouse are reported, not changed.

The format was verified on 2026-10-16 by a Lookup on WH_transportationdedicated in a Fabric pipeline run.
"""

from __future__ import annotations

import copy
import re
from typing import Any, Iterable

SQL_SOURCE_TYPES = {"AzureSqlSource", "SqlDWSource", "SqlServerSource", "AzureSqlMISource", "SqlSource"}
SQL_DATASET_TYPES = {"AzureSqlDWTable", "AzureSqlTable", "SqlServerTable", "AzureSqlMITable"}
_CONTAINERS = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")


def warehouse_linked_service(name: str, warehouse_id: str, workspace_id: str, endpoint: str) -> dict:
    return {"name": name, "properties": {"annotations": [], "type": "DataWarehouse",
                                         "typeProperties": {"endpoint": endpoint, "artifactId": warehouse_id,
                                                            "workspaceId": workspace_id}}}


def _walk(activities: Iterable[dict], path: str = "") -> Iterable[tuple[str, dict]]:
    for a in activities or []:
        here = f"{path}/{a.get('name')}" if path else a.get("name", "?")
        yield here, a
        tp = a.get("typeProperties") or {}
        for key in _CONTAINERS:
            yield from _walk(tp.get(key) or [], here)
        for case in tp.get("cases") or []:
            yield from _walk(case.get("activities") or [], here)


# T-SQL reserved words that Fabric Warehouse rejects as bare column aliases (Synapse accepted e.g. ``AS RowCount``).
# Bracketing keeps the column name, so expressions reading the activity output still work.
RESERVED_ALIASES = ("ROWCOUNT", "IDENTITY", "PERCENT", "KEY", "TOP", "FILE", "PLAN", "USER", "SCHEMA", "VALUES")
_ALIAS = re.compile(r"\bAS\s+(" + "|".join(RESERVED_ALIASES) + r")\b(?!\])", re.I)


def fix_query(query: Any) -> tuple[Any, list[str]]:
    """Bracket reserved-word aliases in a literal query. Expression queries are left alone (and reported)."""
    if not isinstance(query, str):
        return query, []
    found = [m.group(1) for m in _ALIAS.finditer(query)]
    return (_ALIAS.sub(lambda m: f"AS [{m.group(1)}]", query), [f"alias {w} -> [{w}]" for w in found]) if found else (query, [])


def _reads_target(ds: Any, ls: dict) -> bool:
    tp = ((ds or {}).get("linkedService") or {}).get("properties", {}).get("typeProperties", {}) if isinstance(ds, dict) else {}
    return tp.get("artifactId") == ls["properties"]["typeProperties"]["artifactId"]


def _uses(ds: Any, conns: set[str]) -> bool:
    return isinstance(ds, dict) and ((ds.get("externalReferences") or {}).get("connection") or "").lower() in conns


def _convert_dataset(ds: dict, ls: dict) -> dict:
    out = {k: v for k, v in ds.items() if k != "externalReferences"}
    if out.get("type") in SQL_DATASET_TYPES:
        out["type"] = "DataWarehouseTable"
    out["linkedService"] = copy.deepcopy(ls)
    return out


def repoint(content: dict, from_connections: Iterable[str], ls: dict) -> tuple[dict, list[str]]:
    """(new pipeline content, list of changes). The input is not modified. Running it twice changes nothing."""
    conns = {c.lower() for c in from_connections}
    new = copy.deepcopy(content)
    changes: list[str] = []
    for where, a in _walk((new.get("properties") or {}).get("activities") or []):
        tp = a.get("typeProperties") or {}
        # Lookup / GetMetadata / Script-style: datasetSettings next to source
        if _uses(tp.get("datasetSettings"), conns):
            src = tp.get("source") or {}
            if src and src.get("type") not in SQL_SOURCE_TYPES:
                changes.append(f"{where}: skipped, source type {src.get('type')} is not a SQL source")
                continue
            tp["datasetSettings"] = _convert_dataset(tp["datasetSettings"], ls)
            if src:
                src["type"] = "DataWarehouseSource"
            changes.append(f"{where} ({a.get('type')}): reads {ls['name']}")
        # Copy: datasetSettings inside source / sink
        src = tp.get("source") or {}
        if _uses(src.get("datasetSettings"), conns):
            if src.get("type") in SQL_SOURCE_TYPES:
                src["datasetSettings"] = _convert_dataset(src["datasetSettings"], ls)
                src["type"] = "DataWarehouseSource"
                changes.append(f"{where} ({a.get('type')}): source reads {ls['name']}")
            else:
                changes.append(f"{where}: skipped, source type {src.get('type')} is not a SQL source")
        if _uses((tp.get("sink") or {}).get("datasetSettings"), conns):
            changes.append(f"{where}: sink writes to the old warehouse - NOT changed (review manually)")
        # Queries now running on the Warehouse (just repointed or repointed earlier): Fabric T-SQL fixes.
        src = tp.get("source") or {}
        if _reads_target(tp.get("datasetSettings"), ls) or _reads_target(src.get("datasetSettings"), ls):
            fixed, notes = fix_query(src.get("sqlReaderQuery"))
            if notes:
                src["sqlReaderQuery"] = fixed
                changes.append(f"{where}: query {', '.join(notes)}")
    return new, changes
