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
# `AS word` where word is a bare identifier: an alias in [brackets] or "quotes" starts with a delimiter, so it never matches.
_ALIAS = re.compile(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE)


def _queries(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        for k, v in node.items():
            if k in _QUERY_KEYS and isinstance(v, str):
                yield v
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
                words += [m.group(1).upper() for m in _ALIAS.finditer(q) if m.group(1).upper() in RESERVED_SQL]
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
            code="linked_service_parameter_without_value", severity="error", subject=ls.name,
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


def scan(d: AdfSemanticDiscovery, contents: dict[str, dict]) -> list[PreflightFinding]:
    """Findings for the converted ``contents`` (pipeline name -> pipeline-content JSON) and the semantic record."""
    return [*_reserved_aliases(contents), *_parameters_without_value(d), *_wait_only(d)]
