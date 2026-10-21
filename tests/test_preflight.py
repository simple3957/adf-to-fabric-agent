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


class _P:
    def __init__(self, name, default=None, has_default=False):
        self.name, self.default_value, self.has_default = name, default, has_default


def test_wait_only_pipelines_are_noted_and_real_ones_are_not():
    class Pipe:
        def __init__(self, name, counts):
            self.name, self.activity_type_counts = name, counts

    class D:
        pipelines = [Pipe("W", {"Wait": 2}), Pipe("C", {"Copy": 1}), Pipe("M", {"Wait": 1, "Lookup": 1})]

    got = preflight._wait_only(D())
    assert [f.subject for f in got] == ["W"] and got[0].severity == "info"


def test_a_linked_service_parameter_with_no_value_is_flagged_as_an_adf_side_warning(monkeypatch):
    import adf2fabric.connections.resolution as R

    class LS:
        name, parameters = "LS_SQL", [_P("DBName"), _P("Port", "1433", True)]

    class D:
        linked_services = [LS()]

    monkeypatch.setattr(R, "usage", lambda d, n: (["DS"], [{"pipeline": "PL_X", "activity": "A", "type": "Lookup"}], {}))
    got = preflight._parameters_without_value(D())
    assert len(got) == 1 and got[0].severity == "warning" and "DBName" in got[0].message and "Port" not in got[0].message
    monkeypatch.setattr(R, "usage", lambda d, n: (["DS"], [], {"DBName": {"db1"}}))
    assert preflight._parameters_without_value(D()) == []  # a dataset passes it


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
