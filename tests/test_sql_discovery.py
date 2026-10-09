"""SQL Server Discovery (Phase 1) tests — fully offline and deterministic.

A fake pyodbc connection/cursor is injected via the discoverer's
``connection_factory`` seam, so no pyodbc, no server, and no real secret are
needed. The fake logs every executed statement, so the tests prove that Phase 1
runs EXACTLY the four approved read-only catalog queries on a SINGLE connection —
never ``SELECT 1``, business data, row counts, columns/keys, writes, DDL, or EXEC.
Failure modes are simulated with plain exceptions whose SQLSTATE/message match
what the normalizer keys on.
"""

from __future__ import annotations

import inspect

import adf2fabric.agent.discovery.sql.catalog as catalog_mod
import adf2fabric.agent.discovery.sql.discoverer as discoverer_mod
from adf2fabric.agent.discovery.sql import (
    SqlDiscoverer,
    SqlDiscoveryRequest,
    SqlDiscoveryResult,
)
from adf2fabric.agent.discovery.sql.catalog import (
    PROCEDURE_QUERY,
    SCHEMA_QUERY,
    TABLE_QUERY,
    VIEW_QUERY,
)
from adf2fabric.project.discovery import (
    SqlDiscovery,
    SqlDiscoveryStatus,
    SqlObject,
)
from adf2fabric.project.models import (
    Project,
    RepositoryMetadata,
    SqlCredentialRef,
    SqlServerAccess,
    SqlTlsMode,
    SqlTlsPolicy,
)
from adf2fabric.project.store import ProjectStore

FOUR_QUERIES = [SCHEMA_QUERY, TABLE_QUERY, VIEW_QUERY, PROCEDURE_QUERY]


# --- fakes ------------------------------------------------------------------


class FakeCursor:
    """Returns per-query rows; any execute may be made to raise. Logs all SQL."""

    def __init__(self, results, log, execute_error=None):
        self._results = results
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
        return list(self._results.get(self._last, []))

    def close(self):
        self.closed = True
        self._log.append(("cursor_close",))


class FakeConnection:
    """Only ``cursor``/``close`` are legal; any other access is a boundary breach."""

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


# --- helpers ----------------------------------------------------------------


def _setup(tmp_path):
    store = ProjectStore(tmp_path / "projects")
    pid = "test-proj"
    store.save(
        Project(
            id=pid,
            name="Test Proj",
            repository=RepositoryMetadata(
                provider="github",
                repository_url="https://example.invalid/x.git",
                branch="main",
                commit_sha="0" * 40,
                local_source_path=str(store.source_dir(pid)),
            ),
        )
    )
    return store, pid


def _access(**over):
    base = dict(
        required=True,
        server=r"HOST\SQLEXPRESS",
        database="T20",
        tls=SqlTlsPolicy(mode=SqlTlsMode.trust_server_certificate),
        credential=SqlCredentialRef(auth="integrated"),
    )
    base.update(over)
    return SqlServerAccess(**base)


def _factory(results=None, log=None, connect_error=None, execute_error=None, capture=None):
    results = results or {}
    log = log if log is not None else []

    def factory(conn_str, timeout):
        log.append(("connect", timeout))
        if capture is not None:
            capture["conn_str"] = conn_str
            capture["timeout"] = timeout
        if connect_error is not None:
            raise connect_error
        cursor = FakeCursor(results, log, execute_error=execute_error)
        return FakeConnection(cursor, log)

    return factory, log


def _run(store, access=None, **factory_kw):
    factory, log = _factory(**factory_kw)
    disc = SqlDiscoverer(store, connection_factory=factory)
    result = disc.run(
        SqlDiscoveryRequest(project_name="test-proj", access=access or _access())
    )
    return result, log


# Representative estate, supplied UNSORTED to prove deterministic ordering.
_RESULTS = {
    SCHEMA_QUERY: [("sales",), ("dbo",)],
    TABLE_QUERY: [("sales", "Orders"), ("dbo", "Customers")],
    VIEW_QUERY: [("dbo", "vActive"), ("dbo", "vAll")],
    PROCEDURE_QUERY: [("dbo", "usp_Load")],
}


# --- 1-6. successful discovery, mapping, deterministic ordering -------------


def test_successful_discovery_maps_all_object_kinds(tmp_path):
    store, _ = _setup(tmp_path)
    result, _log = _run(store, results=_RESULTS)
    assert result.success
    d = result.discovery
    assert d.status == SqlDiscoveryStatus.discovered
    assert d.server == r"HOST\SQLEXPRESS"
    assert d.database == "T20"
    assert d.authentication == "integrated"
    # schemas sorted
    assert d.schema_count == 2
    assert d.schemas == ["dbo", "sales"]
    # tables sorted by (schema, name)
    assert d.table_count == 2
    assert [(o.schema, o.name) for o in d.tables] == [
        ("dbo", "Customers"),
        ("sales", "Orders"),
    ]
    # views sorted
    assert d.view_count == 2
    assert [(o.schema, o.name) for o in d.views] == [("dbo", "vActive"), ("dbo", "vAll")]
    # procedures
    assert d.procedure_count == 1
    assert [(o.schema, o.name) for o in d.procedures] == [("dbo", "usp_Load")]


# --- 7. empty database (success, not an error) ------------------------------


def test_empty_database_is_success(tmp_path):
    store, _ = _setup(tmp_path)
    result, _log = _run(store, results={})
    assert result.success
    d = result.discovery
    assert d.status == SqlDiscoveryStatus.discovered
    assert (d.schema_count, d.table_count, d.view_count, d.procedure_count) == (0, 0, 0, 0)
    assert d.schemas == [] and d.tables == [] and d.views == [] and d.procedures == []


# --- 13-14. exactly the four approved queries, in order, on ONE connection --


def test_exactly_four_metadata_queries_one_connection(tmp_path):
    store, _ = _setup(tmp_path)
    result, log = _run(store, results=_RESULTS)
    assert result.success
    executed = [t[1] for t in log if t[0] == "execute"]
    assert executed == FOUR_QUERIES  # exactly these four, in this order
    # A single connection: opened once, one cursor, closed once. No SELECT 1.
    assert [t for t in log if t[0] == "connect"] == [("connect", 10)]
    assert log.count(("cursor",)) == 1
    assert log.count(("conn_close",)) == 1
    assert all("select 1" != sql.lower() for sql in executed)
    assert d_meta(result) == ["sys.schemas", "sys.tables", "sys.views", "sys.procedures"]


def d_meta(result):
    return result.discovery.metadata_queries


# --- 8-12. failure modes are distinct and never collapsed to empty ----------


def _pyodbc_error(sqlstate, message):
    return Exception(sqlstate, message)


def test_authentication_failure(tmp_path):
    store, _ = _setup(tmp_path)
    result, _log = _run(
        store, connect_error=_pyodbc_error("28000", "Login failed for user")
    )
    assert result.success is False
    assert result.discovery.status == SqlDiscoveryStatus.authentication_failed
    assert result.discovery.schema_count == 0


def test_permission_failure_not_empty(tmp_path):
    store, _ = _setup(tmp_path)
    result, _log = _run(
        store, execute_error=_pyodbc_error("42000", "SELECT permission was denied")
    )
    assert result.success is False
    assert result.discovery.status == SqlDiscoveryStatus.permission_denied
    assert result.discovery.schemas == []  # NOT reported as "zero schemas"


def test_connectivity_failure(tmp_path):
    store, _ = _setup(tmp_path)
    result, _log = _run(
        store, connect_error=_pyodbc_error("08001", "Unable to open a connection")
    )
    assert result.success is False
    assert result.discovery.status == SqlDiscoveryStatus.connectivity_failed


def test_metadata_query_failure(tmp_path):
    store, _ = _setup(tmp_path)
    result, _log = _run(
        store, execute_error=_pyodbc_error("42S02", "Invalid object name 'sys.foo'")
    )
    assert result.success is False
    assert result.discovery.status == SqlDiscoveryStatus.metadata_query_failed


def test_dependency_unavailable(tmp_path):
    store, _ = _setup(tmp_path)

    def factory(conn_str, timeout):
        raise ImportError("no pyodbc")

    disc = SqlDiscoverer(store, connection_factory=factory)
    result = disc.run(SqlDiscoveryRequest(project_name="test-proj", access=_access()))
    assert result.success is False
    assert result.discovery.status == SqlDiscoveryStatus.dependency_unavailable


# --- config / credential guards ---------------------------------------------


def test_missing_server_or_database(tmp_path):
    store, _ = _setup(tmp_path)
    result, _log = _run(store, access=_access(server=None))
    assert result.success is False
    assert result.discovery.status == SqlDiscoveryStatus.missing_config


def test_missing_tls_mode_is_missing_config(tmp_path):
    store, _ = _setup(tmp_path)
    result, _log = _run(store, access=_access(tls=None))
    assert result.success is False
    assert result.discovery.status == SqlDiscoveryStatus.missing_config


def test_sql_login_missing_password_env_is_missing_credential(tmp_path, monkeypatch):
    store, _ = _setup(tmp_path)
    monkeypatch.delenv("SQL_PASSWORD", raising=False)
    access = _access(
        credential=SqlCredentialRef(
            auth="sql_login", username="svc", password_env="SQL_PASSWORD"
        )
    )
    result, _log = _run(store, access=access)
    assert result.success is False
    assert result.discovery.status == SqlDiscoveryStatus.missing_credential


def test_project_not_found(tmp_path):
    store = ProjectStore(tmp_path / "projects")
    disc = SqlDiscoverer(store, connection_factory=lambda cs, t: None)
    result = disc.run(SqlDiscoveryRequest(project_name="nope", access=_access()))
    assert result.success is False
    assert result.discovery is None


# --- DSN reuse + timeout ----------------------------------------------------


def test_reuses_connection_string_builder_with_tls(tmp_path):
    """The DSN must come from the connectivity builder — TLS options present."""
    store, _ = _setup(tmp_path)
    capture: dict = {}
    result, _log = _run(store, results=_RESULTS, capture=capture)
    assert result.success
    cs = capture["conn_str"]
    # trust_server_certificate -> Encrypt=yes;TrustServerCertificate=yes; integrated
    assert "Encrypt=yes" in cs
    assert "TrustServerCertificate=yes" in cs
    assert "Trusted_Connection=yes" in cs
    assert "PWD=" not in cs and "UID=" not in cs
    assert capture["timeout"] == 10


# --- 17-19. static guards: no data access, no writes, no DDL/DML/EXEC -------


_SQL_VERBS = (
    "select",
    "insert",
    "update",
    "delete",
    "merge",
    "create",
    "alter",
    "drop",
    "truncate",
    "exec",
)


def _sql_literals(mod):
    """Every SQL-statement string literal in a module (ignores prose/comments).

    A literal counts as SQL only if it begins with a SQL verb, so docstrings and
    comments that merely *mention* forbidden statements never trip the guard.
    """
    import ast

    # A SQL statement has its verb followed by whitespace (e.g. "SELECT ..."),
    # which excludes identifiers like "created_at"/"updated_at".
    prefixes = tuple(f"{v} " for v in _SQL_VERBS)
    tree = ast.parse(inspect.getsource(mod))
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            s = node.value.strip()
            if s.lower().startswith(prefixes):
                out.append(s)
    return out


def test_only_approved_sql_statements_in_source():
    """The ONLY SQL statements in the SQL discovery code are the four approved
    read-only catalog queries — no SELECT *, SELECT 1, data reads, DML, DDL, or EXEC."""
    literals = _sql_literals(catalog_mod) + _sql_literals(discoverer_mod)
    assert sorted(set(literals)) == sorted(FOUR_QUERIES)


def test_catalog_defines_exactly_four_queries():
    # Every SELECT in the catalog module is one of the four approved queries.
    selects = [
        v
        for k, v in vars(catalog_mod).items()
        if isinstance(v, str) and v.lower().startswith("select")
    ]
    assert sorted(selects) == sorted(FOUR_QUERIES)
    for q in FOUR_QUERIES:
        low = q.lower()
        assert "sys." in low and "order by" in low and "*" not in q


# --- 19. no secret persisted ------------------------------------------------


def test_no_secret_fields_or_values(tmp_path, monkeypatch):
    store, _ = _setup(tmp_path)
    monkeypatch.setenv("SQL_PASSWORD", "pw-super-secret")
    access = _access(
        credential=SqlCredentialRef(
            auth="sql_login", username="svc", password_env="SQL_PASSWORD"
        )
    )
    result, _log = _run(store, access=access, results=_RESULTS)
    assert result.success
    fields = set(SqlDiscovery.model_fields)
    assert fields.isdisjoint(
        {"password", "credential", "token", "connection_string", "dsn", "secret"}
    )
    serialized = result.discovery.model_dump_json()
    assert "pw-super-secret" not in serialized
    assert "PWD=" not in serialized and "password" not in serialized.lower()
    text = store.sql_discovery_path("test-proj").read_text(encoding="utf-8")
    assert "pw-super-secret" not in text and "PWD=" not in text


# --- 20. persisted round-trip -----------------------------------------------


def test_persisted_sql_round_trips(tmp_path):
    store, _ = _setup(tmp_path)
    _run(store, results=_RESULTS)
    loaded = store.load_sql_discovery("test-proj")
    assert loaded.server == r"HOST\SQLEXPRESS"
    assert loaded.database == "T20"
    assert loaded.schemas == ["dbo", "sales"]
    assert [(o.schema, o.name) for o in loaded.tables] == [
        ("dbo", "Customers"),
        ("sales", "Orders"),
    ]


def test_deterministic_repeatable_timestamps(tmp_path):
    store, _ = _setup(tmp_path)
    first = _run(store, results=_RESULTS)[0].discovery
    second = _run(store, results=_RESULTS)[0].discovery
    assert discoverer_mod._semantic(first) == discoverer_mod._semantic(second)
    assert second.created_at == first.created_at
    assert second.updated_at == first.updated_at


# --- 21. DiscoveryAgent delegates to SqlDiscoverer --------------------------


def test_discovery_agent_delegates_to_sql_discoverer(tmp_path):
    from adf2fabric.agent.discovery import DiscoveryAgent

    store, _ = _setup(tmp_path)
    sentinel = SqlDiscoveryResult(success=True, message="delegated")

    class FakeSql:
        def __init__(self):
            self.calls = []

        def run(self, request):
            self.calls.append(request)
            return sentinel

    fake = FakeSql()
    agent = DiscoveryAgent(store, sql_discoverer=fake)
    req = SqlDiscoveryRequest(project_name="test-proj", access=_access())
    out = agent.discover_sql(req)
    assert out is sentinel
    assert fake.calls == [req]


# --- 25 / boundary: persisted contracts live only in the project layer ------


def test_persisted_models_defined_only_in_project_layer():
    import adf2fabric.agent.discovery.sql.models as sql_models

    for model in (SqlDiscovery, SqlObject, SqlDiscoveryStatus):
        assert model.__module__ == "adf2fabric.project.discovery"
    for name in ("SqlDiscovery", "SqlObject", "SqlDiscoveryStatus"):
        assert name not in sql_models.__dict__ or (
            getattr(sql_models, name).__module__ == "adf2fabric.project.discovery"
        )


def test_project_layer_does_not_import_agent():
    import adf2fabric.project.discovery as disc
    import adf2fabric.project.store as store_mod

    for mod in (disc, store_mod):
        assert "adf2fabric.agent" not in inspect.getsource(mod)


# --- 22-23. connectivity connector reused, not modified ---------------------


def test_reuses_connectivity_sql_module_not_reimplemented():
    src = inspect.getsource(discoverer_mod)
    # DSN construction is delegated, never rebuilt here.
    assert "SqlServerConnector._connection_string" in src
    assert "DRIVER=" not in src and "Trusted_Connection" not in src


# --- 24. no circular imports (importing the package fully must succeed) ------


def test_package_imports_cleanly():
    import importlib

    for name in (
        "adf2fabric.agent.discovery.sql",
        "adf2fabric.agent.discovery.sql.catalog",
        "adf2fabric.agent.discovery.sql.discoverer",
        "adf2fabric.agent.discovery.sql.models",
        "adf2fabric.agent.discovery.agent",
    ):
        assert importlib.import_module(name) is not None
