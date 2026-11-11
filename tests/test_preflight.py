"""Pre-flight checks: source problems that only show when a run fails."""

from __future__ import annotations

from adf2fabric.agent.conversion import preflight
from adf2fabric.agent.conversion.preflight import RESERVED_SQL


def _content(*queries):
    return {"properties": {"activities": [
        {"name": f"Q{i}", "type": "Lookup", "typeProperties": {"source": {"type": "AzureSqlSource", "sqlReaderQuery": q}}}
        for i, q in enumerate(queries)]}}


def test_the_microsoft_reserved_list_includes_the_word_that_failed_live():
    assert {"ROWCOUNT", "USER", "KEY", "LABEL"} <= RESERVED_SQL and "COUNTS" not in RESERVED_SQL


def test_a_reserved_word_used_as_an_alias_is_one_finding_per_pipeline():
    got = preflight._reserved_aliases({"PL_A": _content("SELECT COUNT_BIG(*) AS RowCount FROM t", "SELECT 1 AS [RowCount]",
                                                         "SELECT 2 as rowcount, 3 AS Total FROM u"),
                                       "PL_B": _content("SELECT 1 AS Total")})
    assert len(got) == 1 and got[0].pipelines == ["PL_A"] and got[0].severity == "error"
    assert "2 query" in got[0].message and "ROWCOUNT" in got[0].message and "[Rowcount]" in got[0].fix


def test_a_bracketed_or_ordinary_alias_is_not_flagged():
    assert not preflight._reserved_aliases({"PL": _content("SELECT 1 AS [Order]", "SELECT 1 AS Total, 2 AS \"Key\"")})


def test_temporal_queries_types_literals_and_comments_are_not_flagged():
    assert not preflight._reserved_aliases({"PL": _content(
        "SELECT * FROM t FOR SYSTEM_TIME AS OF '2024-01-01'",
        "SELECT CAST(x AS DOUBLE PRECISION) AS Total FROM t",
        "SELECT 'value as of today' AS Note FROM t -- returned as is",
        "SELECT 1 AS Total /* AS RowCount, */ FROM t")})


def test_an_alias_before_a_clause_or_bracket_is_flagged_and_expression_queries_are_read():
    got = preflight._reserved_aliases({"PL": {"properties": {"activities": [
        {"name": "A", "type": "Lookup", "typeProperties": {"source": {
            "sqlReaderQuery": {"value": "SELECT COUNT(*) AS RowCount FROM @{pipeline().parameters.t}", "type": "Expression"}}}},
        {"name": "B", "type": "Lookup", "typeProperties": {"source": {"sqlReaderQuery": "SELECT (SELECT 1 AS User)"}}}]}}})
    assert len(got) == 1 and "ROWCOUNT" in got[0].message and "USER" in got[0].message


def test_a_lookup_that_returns_all_rows_is_noted_for_its_5000_row_cap_and_the_others_are_not():
    def lookup(name, **tp):
        return {"name": name, "type": "Lookup", "typeProperties": tp}

    contents = {
        "Many": {"properties": {"activities": [lookup("All_Rows", firstRowOnly=False), lookup("Also_All", firstRowOnly=False),
                                               {"name": "If", "type": "IfCondition", "typeProperties": {"ifTrueActivities": [lookup("Nested", firstRowOnly=False)]}}]}},
        "First": {"properties": {"activities": [lookup("One_Row", firstRowOnly=True), lookup("Default")]}},
    }
    got = preflight._lookup_row_cap(contents)
    assert [f.pipelines for f in got] == [["Many"]]  # firstRowOnly true, or left at its default (true), has no cap to hit
    assert got[0].subject == "All_Rows, Also_All, Nested" and got[0].severity == "info" and got[0].code == "lookup_row_cap"
    assert "5,000" in got[0].message and "rowsRead" in got[0].fix
