"""SQL Server Discovery Phase 2 (structural) tests — fully offline, deterministic.

A fake pyodbc connection/cursor is injected via the discoverer's
``connection_factory`` seam (the same seam Phase 1 uses), serving per-query rows
for exactly the three approved structural queries. No pyodbc, no server, no
secret. The fake logs every statement, so the tests prove Phase 2 runs exactly
the three read-only ``sys.*`` queries on ONE connection and touches no business
data and no write/DDL/EXEC.
"""

from __future__ import annotations

import inspect

import adf2fabric.agent.discovery.sql_structural.catalog as catalog_mod
import adf2fabric.agent.discovery.sql_structural.discoverer as discoverer_mod
from adf2fabric.agent.discovery.sql_structural import (
    SqlStructuralDiscoverer,
    SqlStructuralDiscoveryRequest,
    SqlStructuralDiscoveryResult,
)
from adf2fabric.agent.discovery.sql_structural.catalog import (
    COLUMN_QUERY,
    FOREIGN_KEY_QUERY,
    PRIMARY_KEY_QUERY,
)
from adf2fabric.project.discovery import SqlDiscoveryStatus, SqlStructuralDiscovery
from adf2fabric.project.models import (
    Project,
    RepositoryMetadata,
    SqlCredentialRef,
    SqlServerAccess,
    SqlTlsMode,
    SqlTlsPolicy,
)
from adf2fabric.project.store import ProjectStore

THREE_QUERIES = [COLUMN_QUERY, PRIMARY_KEY_QUERY, FOREIGN_KEY_QUERY]


# --- fakes ------------------------------------------------------------------


class FakeCursor:
    def __init__(self, rows_by_query, log, execute_error=None):
        self._rows = rows_by_query
        self._log = log
        self._execute_error = execute_error
        self._last = None
        self.closed = False

    def execute(self, sql):
        self._log.append(("execute", sql))
        if self._execute_error is not None:
            raise self._execute_error
        self._last = sql

    def fetchall(self):
        self._log.append(("fetchall",))
        return list(self._rows.get(self._last, []))

    def close(self):
        self.closed = True
        self._log.append(("cursor_close",))


class FakeConn:
    def __init__(self, cursor, log):
        self._cursor = cursor
        self._log = log
        self.closed = False

    def cursor(self):
        self._log.append(("cursor",))
        return self._cursor

    def close(self):
        self.closed = True
        self._log.append(("conn_close",))

    def __getattr__(self, name):
        raise AssertionError(f"forbidden SQL connection operation accessed: {name!r}")


# --- representative catalog data (supplied UNSORTED) ------------------------
# int columns: (max_length=4, precision=10, scale=0); nvarchar: max_length in bytes.
# Column tuple: schema, table, column_id, name, type, max_len, prec, scale, nullable, identity, computed

_COLUMNS = [
    ("dbo", "Team", 1, "Id", "int", 4, 10, 0, 0, 1, 0),
    ("dbo", "Team", 2, "Name", "nvarchar", -1, 0, 0, 1, 0, 0),
    ("dbo", "Player", 2, "Name", "nvarchar", 100, 0, 0, 0, 0, 0),  # unsorted ordinal
    ("dbo", "Player", 1, "Id", "int", 4, 10, 0, 0, 1, 0),
    ("dbo", "Match", 1, "Id", "int", 4, 10, 0, 0, 0, 0),
    ("dbo", "Match", 2, "HomeTeamId", "int", 4, 10, 0, 0, 0, 0),
    ("dbo", "Match", 3, "AwayTeamId", "int", 4, 10, 0, 0, 0, 0),
    ("dbo", "Score", 1, "MatchId", "int", 4, 10, 0, 0, 0, 0),
    ("dbo", "Score", 2, "PlayerId", "int", 4, 10, 0, 0, 0, 0),
    ("dbo", "Score", 3, "Runs", "int", 4, 10, 0, 1, 0, 0),
    ("dbo", "ScoreDetail", 1, "MatchId", "int", 4, 10, 0, 0, 0, 0),
    ("dbo", "ScoreDetail", 2, "PlayerId", "int", 4, 10, 0, 0, 0, 0),
    ("dbo", "ScoreDetail", 3, "Ball", "int", 4, 10, 0, 0, 0, 0),
    ("dbo", "Node", 1, "Id", "int", 4, 10, 0, 0, 0, 0),
    ("dbo", "Node", 2, "ParentId", "int", 4, 10, 0, 1, 0, 0),
    ("dbo", "NoKey", 1, "Val", "int", 4, 10, 0, 1, 0, 0),
]

_PRIMARY_KEYS = [
    ("dbo", "Team", "PK_Team", "Id", 1),
    ("dbo", "Player", "PK_Player", "Id", 1),
    ("dbo", "Match", "PK_Match", "Id", 1),
    ("dbo", "Score", "PK_Score", "PlayerId", 2),  # composite, unsorted
    ("dbo", "Score", "PK_Score", "MatchId", 1),
    ("dbo", "Node", "PK_Node", "Id", 1),
]

_FOREIGN_KEYS = [
    ("FK_Match_Home", "dbo", "Match", "HomeTeamId", "dbo", "Team", "Id", 1),
    ("FK_Match_Away", "dbo", "Match", "AwayTeamId", "dbo", "Team", "Id", 1),
    ("FK_ScoreDetail_Score", "dbo", "ScoreDetail", "PlayerId", "dbo", "Score", "PlayerId", 2),
    ("FK_ScoreDetail_Score", "dbo", "ScoreDetail", "MatchId", "dbo", "Score", "MatchId", 1),
    ("FK_Node_Parent", "dbo", "Node", "ParentId", "dbo", "Node", "Id", 1),  # self-ref
]

_ROWS = {COLUMN_QUERY: _COLUMNS, PRIMARY_KEY_QUERY: _PRIMARY_KEYS, FOREIGN_KEY_QUERY: _FOREIGN_KEYS}


# --- helpers ----------------------------------------------------------------


def _setup(tmp_path):
    store = ProjectStore(tmp_path / "projects")
    pid = "test-proj"
    store.save(
        Project(
            id=pid, name="Test Proj",
            repository=RepositoryMetadata(
                provider="github", repository_url="https://example.invalid/x.git",
                branch="main", commit_sha="0" * 40, local_source_path=str(store.source_dir(pid)),
            ),
        )
    )
    return store, pid


def _access(**over):
    base = dict(
        required=True, server=r"HOST\SQLEXPRESS", database="T20",
        tls=SqlTlsPolicy(mode=SqlTlsMode.trust_server_certificate),
        credential=SqlCredentialRef(auth="integrated"),
    )
    base.update(over)
    return SqlServerAccess(**base)


def _run(store, access=None, rows=None, connect_error=None, execute_error=None, capture=None):
    log = []

    def factory(conn_str, timeout):
        log.append(("connect", timeout))
        if capture is not None:
            capture["conn_str"] = conn_str
        if connect_error is not None:
            raise connect_error
        return FakeConn(FakeCursor(rows if rows is not None else _ROWS, log, execute_error), log)

    disc = SqlStructuralDiscoverer(store, connection_factory=factory)
    result = disc.run(
        SqlStructuralDiscoveryRequest(project_name="test-proj", access=access or _access())
    )
    return result, log


def _tables(discovery):
    return {t.table: t for t in discovery.tables}


# --- columns / types / flags ------------------------------------------------


def test_column_metadata_and_types(tmp_path):
    store, _ = _setup(tmp_path)
    d = _run(store)[0].discovery
    assert d.status == SqlDiscoveryStatus.discovered
    assert d.table_count == 7
    assert d.column_count == 16
    tables = _tables(d)
    team = tables["Team"]
    # ordered by ordinal
    assert [c.name for c in team.columns] == ["Id", "Name"]
    idc = team.columns[0]
    assert idc.data_type == "int" and idc.is_identity is True and idc.is_nullable is False
    name = team.columns[1]
    assert name.data_type == "nvarchar" and name.max_length == -1 and name.is_nullable is True
    # precision/scale carried; Player.Name length
    assert tables["Player"].columns[1].max_length == 100
    assert tables["Score"].columns[2].is_nullable is True  # Runs nullable


# --- primary keys (single + composite) --------------------------------------


def test_primary_keys_including_composite(tmp_path):
    store, _ = _setup(tmp_path)
    d = _run(store)[0].discovery
    assert d.primary_key_count == 5  # Team, Player, Match, Score, Node
    tables = _tables(d)
    assert [c.column for c in tables["Team"].primary_key.columns] == ["Id"]
    # composite PK, ordered by key_ordinal even though supplied unsorted
    assert [c.column for c in tables["Score"].primary_key.columns] == ["MatchId", "PlayerId"]
    # tables without a PK
    assert tables["NoKey"].primary_key is None
    assert tables["ScoreDetail"].primary_key is None


# --- foreign keys (single, composite, self-ref, multiple to same table) -----


def test_foreign_keys_and_relationships(tmp_path):
    store, _ = _setup(tmp_path)
    d = _run(store)[0].discovery
    assert d.foreign_key_count == 4
    tables = _tables(d)
    # Match has two FKs, both -> Team
    match_fks = {fk.name: fk for fk in tables["Match"].foreign_keys}
    assert set(match_fks) == {"FK_Match_Home", "FK_Match_Away"}
    assert all(fk.referenced_table == "Team" for fk in match_fks.values())
    # composite FK ordered
    sd_fk = tables["ScoreDetail"].foreign_keys[0]
    assert [(c.parent_column, c.referenced_column) for c in sd_fk.columns] == [
        ("MatchId", "MatchId"),
        ("PlayerId", "PlayerId"),
    ]
    # self-referencing FK
    node_fk = tables["Node"].foreign_keys[0]
    assert node_fk.referenced_table == "Node" and node_fk.parent_table == "Node"
    # relationship graph mirrors FKs with direction parent -> referenced
    rels = {(r.foreign_key_name, r.parent_table, r.referenced_table) for r in d.relationships}
    assert ("FK_Node_Parent", "Node", "Node") in rels
    assert ("FK_ScoreDetail_Score", "ScoreDetail", "Score") in rels
    assert len(d.relationships) == 4


# --- determinism ------------------------------------------------------------


def test_deterministic_ordering_and_timestamps(tmp_path):
    store, _ = _setup(tmp_path)
    first = _run(store)[0].discovery
    # tables sorted by (schema, table)
    assert [t.table for t in first.tables] == [
        "Match", "NoKey", "Node", "Player", "Score", "ScoreDetail", "Team"
    ]
    second = _run(store)[0].discovery
    assert discoverer_mod._semantic(first) == discoverer_mod._semantic(second)
    assert second.created_at == first.created_at
    assert second.updated_at == first.updated_at


def test_empty_database_is_success(tmp_path):
    store, _ = _setup(tmp_path)
    d = _run(store, rows={COLUMN_QUERY: [], PRIMARY_KEY_QUERY: [], FOREIGN_KEY_QUERY: []})[0].discovery
    assert d.status == SqlDiscoveryStatus.discovered
    assert (d.table_count, d.column_count, d.primary_key_count, d.foreign_key_count) == (0, 0, 0, 0)


# --- exactly three read-only queries on one connection ----------------------


def test_exactly_three_queries_one_connection(tmp_path):
    store, _ = _setup(tmp_path)
    _, log = _run(store)
    executed = [t[1] for t in log if t[0] == "execute"]
    assert executed == THREE_QUERIES  # exactly these three, in order
    assert [t for t in log if t[0] == "connect"] == [("connect", 10)]
    assert log.count(("cursor",)) == 1
    assert log.count(("conn_close",)) == 1


def test_read_only_no_business_data_in_queries():
    # Scan the actual SQL (prose-free) — the only SQL statements Phase 2 runs.
    sql = " ".join(THREE_QUERIES).lower()
    for forbidden in (
        "select *", "select 1", "information_schema", "count(", "sum(", "avg(",
        "insert ", "update ", "delete ", "merge ", "truncate ",
        "create ", "alter ", "drop ", "exec ", "execute(",
    ):
        assert forbidden not in sql, f"Phase 2 SQL must not contain {forbidden!r}"
    # every statement targets sys.* metadata only, no wildcard
    for q in THREE_QUERIES:
        low = q.lower()
        assert "from sys." in low and "*" not in q
        assert low.startswith("select ")


def test_catalog_defines_exactly_three_sql_statements():
    # The ONLY SQL string literals in the catalog module are the three approved queries.
    selects = [
        v for v in vars(catalog_mod).values()
        if isinstance(v, str) and v.lower().startswith("select ")
    ]
    assert sorted(selects) == sorted(THREE_QUERIES)


# --- failure/status mapping -------------------------------------------------


def _err(sqlstate, message):
    return Exception(sqlstate, message)


def test_metadata_query_failure(tmp_path):
    store, _ = _setup(tmp_path)
    d = _run(store, execute_error=_err("42S02", "Invalid object name"))[0].discovery
    assert d.status == SqlDiscoveryStatus.metadata_query_failed


def test_authentication_failure(tmp_path):
    store, _ = _setup(tmp_path)
    d = _run(store, connect_error=_err("28000", "Login failed for user"))[0].discovery
    assert d.status == SqlDiscoveryStatus.authentication_failed


def test_permission_failure(tmp_path):
    store, _ = _setup(tmp_path)
    d = _run(store, execute_error=_err("42000", "SELECT permission was denied"))[0].discovery
    assert d.status == SqlDiscoveryStatus.permission_denied


def test_connectivity_failure(tmp_path):
    store, _ = _setup(tmp_path)
    d = _run(store, connect_error=_err("08001", "Unable to open a connection"))[0].discovery
    assert d.status == SqlDiscoveryStatus.connectivity_failed


def test_dependency_unavailable(tmp_path):
    store, _ = _setup(tmp_path)

    def factory(conn_str, timeout):
        raise ImportError("no pyodbc")

    disc = SqlStructuralDiscoverer(store, connection_factory=factory)
    result = disc.run(SqlStructuralDiscoveryRequest(project_name="test-proj", access=_access()))
    assert result.discovery.status == SqlDiscoveryStatus.dependency_unavailable


def test_missing_config_and_tls(tmp_path):
    store, _ = _setup(tmp_path)
    assert _run(store, access=_access(server=None))[0].discovery.status == SqlDiscoveryStatus.missing_config
    assert _run(store, access=_access(tls=None))[0].discovery.status == SqlDiscoveryStatus.missing_config


# --- DSN reuse (no second connection builder) -------------------------------


def test_reuses_connection_string_builder(tmp_path):
    store, _ = _setup(tmp_path)
    capture: dict = {}
    _run(store, capture=capture)
    cs = capture["conn_str"]
    assert "Encrypt=yes" in cs and "TrustServerCertificate=yes" in cs
    assert "Trusted_Connection=yes" in cs
    src = inspect.getsource(discoverer_mod)
    assert "SqlServerConnector._connection_string" in src
    assert "DRIVER=" not in src  # DSN is never rebuilt here


# --- persistence + delegation + layering ------------------------------------


def test_persisted_round_trips(tmp_path):
    store, pid = _setup(tmp_path)
    _run(store)
    loaded = store.load_sql_structural_discovery(pid)
    assert loaded.schema_version == "sql-structural-discovery/1"
    assert loaded.table_count == 7
    assert _tables(loaded)["Score"].primary_key.columns[0].column == "MatchId"


def test_discovery_agent_delegates(tmp_path):
    from adf2fabric.agent.discovery import DiscoveryAgent

    store, _ = _setup(tmp_path)
    sentinel = SqlStructuralDiscoveryResult(success=True, message="delegated")

    class Fake:
        def __init__(self):
            self.calls = []

        def run(self, request):
            self.calls.append(request)
            return sentinel

    fake = Fake()
    agent = DiscoveryAgent(store, sql_structural_discoverer=fake)
    req = SqlStructuralDiscoveryRequest(project_name="test-proj", access=_access())
    assert agent.discover_sql_structural(req) is sentinel and fake.calls == [req]


def test_persisted_models_only_in_project_layer():
    import adf2fabric.agent.discovery.sql_structural.models as sm

    for name in ("SqlStructuralDiscovery", "SqlTableStructure", "SqlColumn"):
        assert name not in sm.__dict__ or (
            getattr(sm, name).__module__ == "adf2fabric.project.discovery"
        )
    assert SqlStructuralDiscovery.__module__ == "adf2fabric.project.discovery"


def test_project_layer_does_not_import_agent():
    import adf2fabric.project.discovery as disc
    import adf2fabric.project.store as store_mod

    for mod in (disc, store_mod):
        assert "adf2fabric.agent" not in inspect.getsource(mod)


def test_package_imports_cleanly():
    import importlib

    for name in (
        "adf2fabric.agent.discovery.sql_structural",
        "adf2fabric.agent.discovery.sql_structural.catalog",
        "adf2fabric.agent.discovery.sql_structural.discoverer",
        "adf2fabric.agent.discovery.sql_structural.models",
    ):
        assert importlib.import_module(name) is not None


def test_phase1_sql_yaml_untouched(tmp_path):
    store, pid = _setup(tmp_path)
    # A pre-existing Phase-1 record must be left byte-for-byte identical.
    store.discovery_dir(pid).mkdir(parents=True, exist_ok=True)
    p1 = store.sql_discovery_path(pid)
    p1.write_bytes(b"schema_version: sql-discovery/1\nphase1: untouched\n")
    before = p1.read_bytes()
    _run(store)
    assert p1.read_bytes() == before
    assert store.has_sql_structural_discovery(pid)  # wrote its own separate file
