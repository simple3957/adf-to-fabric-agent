"""Pre-flight checks on the source estate: problems the conversion cannot fix and that only show when a run fails.

Compatibility findings (``compatibility.py``) are about components Microsoft retired. These are about the authored
content itself, found by reading the converted pipelines and the semantic record. Every check says what it found, why
that fails and what to change; none of them blocks the conversion.
"""

from __future__ import annotations

import re
from typing import Any, Iterator

from adf2fabric.project.conversion import PreflightFinding
from adf2fabric.project.discovery import AdfSemanticDiscovery

RESERVED_URL = "https://learn.microsoft.com/en-us/sql/t-sql/language-elements/reserved-keywords-transact-sql"

# Microsoft's "SQL Server and Azure Synapse Analytics reserved keywords" table (verified 2026-10-21), plus the one
# keyword that page lists as exclusive to Azure Synapse Analytics (LABEL).
RESERVED_SQL = frozenset("""
ADD ALL ALTER AND ANY AS ASC AUTHORIZATION BACKUP BEGIN BETWEEN BREAK BROWSE BULK BY CASCADE CASE CHECK CHECKPOINT
CLOSE CLUSTERED COALESCE COLLATE COLUMN COMMIT COMPUTE CONSTRAINT CONTAINS CONTAINSTABLE CONTINUE CONVERT CREATE CROSS
CURRENT CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURSOR DATABASE DBCC DEALLOCATE DECLARE DEFAULT DELETE
DENY DESC DISK DISTINCT DISTRIBUTED DOUBLE DROP DUMP ELSE END ERRLVL ESCAPE EXCEPT EXEC EXECUTE EXISTS EXIT EXTERNAL
FETCH FILE FILLFACTOR FOR FOREIGN FREETEXT FREETEXTTABLE FROM FULL FUNCTION GOTO GRANT GROUP HAVING HOLDLOCK IDENTITY
IDENTITY_INSERT IDENTITYCOL IF IN INDEX INNER INSERT INTERSECT INTO IS JOIN KEY KILL LEFT LIKE LINENO LOAD MERGE
NATIONAL NOCHECK NONCLUSTERED NOT NULL NULLIF OF OFF OFFSETS ON OPEN OPENDATASOURCE OPENQUERY OPENROWSET OPENXML OPTION
OR ORDER OUTER OVER PERCENT PIVOT PLAN PRECISION PRIMARY PRINT PROC PROCEDURE PUBLIC RAISERROR READ READTEXT
RECONFIGURE REFERENCES REPLICATION RESTORE RESTRICT RETURN REVERT REVOKE RIGHT ROLLBACK ROWCOUNT ROWGUIDCOL RULE SAVE
SCHEMA SECURITYAUDIT SELECT SEMANTICKEYPHRASETABLE SEMANTICSIMILARITYDETAILSTABLE SEMANTICSIMILARITYTABLE SESSION_USER
SET SETUSER SHUTDOWN SOME STATISTICS SYSTEM_USER TABLE TABLESAMPLE TEXTSIZE THEN TO TOP TRAN TRANSACTION TRIGGER
TRUNCATE TRY_CONVERT TSEQUAL UNION UNIQUE UNPIVOT UPDATE UPDATETEXT USE USER VALUES VARYING VIEW WAITFOR WHEN WHERE
WHILE WITH WRITETEXT LABEL
""".split())

_QUERY_KEYS = {"sqlReaderQuery", "query"}
# `AS word` where word is a bare identifier and what follows is where a column alias ends (a comma, a closing bracket, the
# end, or the next clause). An alias in [brackets] or "quotes" starts with a delimiter, so it never matches; neither do
# `FOR SYSTEM_TIME AS OF '...'` or `CAST(x AS DOUBLE PRECISION)`, where the word after AS is followed by more of the clause.
_ALIAS = re.compile(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)(?=\s*(?:,|\)|;|$|\b(?:FROM|INTO|WHERE|GROUP|ORDER|UNION|HAVING)\b))",
                    re.IGNORECASE)
# String literals and comments are blanked before matching: words in them are not SQL.
_NOT_SQL = re.compile(r"'(?:[^']|'')*'|--[^\n]*|/\*.*?\*/", re.DOTALL)


def _queries(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        for k, v in node.items():
            if k in _QUERY_KEYS and isinstance(v, str):
                yield v
            elif k in _QUERY_KEYS and isinstance(v, dict) and isinstance(v.get("value"), str):
                yield v["value"]  # an expression query: its literal SQL parts are still SQL
            else:
                yield from _queries(v)
    elif isinstance(node, list):
        for v in node:
            yield from _queries(v)


def _activities(node: Any) -> Iterator[dict]:
    if isinstance(node, dict):
        if "name" in node and "type" in node and "typeProperties" in node:
            yield node
        for v in node.values():
            yield from _activities(v)
    elif isinstance(node, list):
        for v in node:
            yield from _activities(v)


def _reserved_aliases(contents: dict[str, dict]) -> list[PreflightFinding]:
    out = []
    for pipeline, content in contents.items():
        hits: dict[str, list[str]] = {}  # activity -> reserved aliases
        for act in _activities(content):
            words = []
            for q in _queries(act.get("typeProperties") or {}):
                sql = _NOT_SQL.sub(lambda m: "''" if m.group(0).startswith("'") else " ", q)  # a literal stays a value
                words += [m.group(1).upper() for m in _ALIAS.finditer(sql) if m.group(1).upper() in RESERVED_SQL]
            if words:
                hits[act["name"]] = sorted(dict.fromkeys(words))
        if not hits:
            continue
        words = sorted({w for ws in hits.values() for w in ws})
        shown = ", ".join(f"'{n}'" for n in list(hits)[:4]) + (f" and {len(hits) - 4} more" if len(hits) > 4 else "")
        out.append(PreflightFinding(
            code="reserved_sql_alias", severity="error", subject=pipeline, pipelines=[pipeline],
            message=f"{len(hits)} query(ies) in '{pipeline}' use a reserved SQL keyword as an unquoted alias "
                    f"({', '.join(words)}): {shown}. They fail on SQL Server and Synapse with 'Incorrect syntax near the keyword'.",
            fix=f"Wrap the alias in square brackets (for example AS [{words[0].title()}]) or rename it, in the source "
                "pipeline.", source_url=RESERVED_URL))
    return out


def _parameters_without_value(d: AdfSemanticDiscovery) -> list[PreflightFinding]:
    from adf2fabric.connections.resolution import usage  # local: the connections package imports the conversion one

    out = []
    for ls in d.linked_services:
        if not ls.parameters:
            continue
        ds_names, acts, args = usage(d, ls.name)
        if not ds_names and not acts:
            continue
        missing = [p.name for p in ls.parameters if not (p.has_default and p.default_value) and not args.get(p.name)]
        if not missing:
            continue
        out.append(PreflightFinding(
            code="linked_service_parameter_without_value", severity="warning", subject=ls.name,  # breaks ADF runs, not Fabric ones
            pipelines=sorted({a["pipeline"] for a in acts}),
            message=f"Linked service '{ls.name}' needs a value for parameter(s) {', '.join(missing)}, but it has no default "
                    "and no dataset or activity passes one. A run in ADF fails with 'No value provided for Parameter'.",
            fix="Give the parameter a default value on the linked service, or pass it from the dataset. In Fabric the "
                "bound connection fixes the database, so the converted pipelines do not need it."))
    return out


def _wait_only(d: AdfSemanticDiscovery) -> list[PreflightFinding]:
    out = []
    for p in d.pipelines:
        if set(p.activity_type_counts or {}) == {"Wait"}:
            out.append(PreflightFinding(
                code="wait_only_pipeline", severity="info", subject=p.name, pipelines=[p.name],
                message=f"Pipeline '{p.name}' contains only Wait activities. It converts and runs, but it moves no data and "
                        "does not prove the migration.",
                fix="Treat it as a placeholder, or replace the Waits with the real activities before relying on it."))
    return out


def _missing_artifacts(d: AdfSemanticDiscovery) -> list[PreflightFinding]:
    """Linked services and datasets that pipelines name but the source does not contain (a partial ZIP or Git folder).

    Without a dataset the activity cannot be converted (blocked); without a linked service it converts as manual, because the
    connection cannot be described. Both are listed with who needs them and every way to get them.
    """
    from adf2fabric.project.discovery import AdfArtifactKind as K

    wanted = {K.linked_service: "linked service", K.dataset: "dataset"}
    missing: dict[K, list[str]] = {k: sorted({r.name for r in d.unresolved_references if r.kind == k}) for k in wanted}
    users: dict[tuple[K, str], set[str]] = {}
    for rel in d.relationships:
        if not rel.resolved and rel.target_kind in wanted:
            users.setdefault((rel.target_kind, rel.target_name), set()).add(rel.source_name)
    out = []
    for kind, names in missing.items():
        if not names:
            continue
        what = wanted[kind]
        shown = ", ".join(f"'{n}'" for n in names[:8]) + (f" and {len(names) - 8} more" if len(names) > 8 else "")
        needed_by = sorted({u for n in names for u in users.get((kind, n), ())})
        if kind == K.dataset:
            sev, effect = "error", "every Copy, Lookup or GetMetadata that uses one is blocked: Fabric inlines datasets and there is nothing to inline"
            fix = ("Add the dataset folder to the source: take the full Git collaboration branch (not a subfolder), a ZIP that includes "
                   "'dataset/', or re-run intake from the live factory (`adf2fabric intake --factory ...`).")
        else:
            sev, effect = "warning", "activities that use one convert as manual: the connector type, endpoint and authentication cannot be described"
            fix = ("Best: add them to the source: a ZIP or Git folder that includes 'linkedService/', the factory's ARMTemplateForFactory.json "
                   "export, or `adf2fabric intake --factory ... --artifact-kind linkedservice` against the live factory. Otherwise create each "
                   "Fabric connection by hand and bind it with 'connection:<name>' in --resolutions; the activities then convert with a "
                   "difference noting that the connector type was not checked. Stored procedure and Script activities stay blocked "
                   "either way: the store type decides whether Fabric supports them, so only the linked service itself resolves them.")
        out.append(PreflightFinding(
            code="missing_datasets" if kind == K.dataset else "missing_linked_services", severity=sev, subject=", ".join(names[:8]), pipelines=needed_by,
            message=f"{len(names)} {what}(s) are named by the pipelines but are not in the source: {shown}. Result: {effect}.", fix=fix))
    return out


def scan(d: AdfSemanticDiscovery, contents: dict[str, dict]) -> list[PreflightFinding]:
    """Findings for the converted ``contents`` (pipeline name -> pipeline-content JSON) and the semantic record."""
    return [*_missing_artifacts(d), *_reserved_aliases(contents), *_parameters_without_value(d), *_wait_only(d)]
