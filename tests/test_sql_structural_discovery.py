"""SQL Server Discovery Phase 2 (structural) tests.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import inspect

import adf2fabric.agent.discovery.sql_structural.catalog as catalog_mod
from adf2fabric.agent.discovery.sql_structural import (
    SqlStructuralDiscoverer,
    SqlStructuralDiscoveryRequest,
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


# --- representative catalog data (supplied UNSORTED) ------------------------
# int columns: (max_length=4, precision=10, scale=0); nvarchar: max_length in bytes.
# Column tuple: schema, table, column_id, name, type, max_len, prec, scale, nullable, identity, computed


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


# --- columns / types / flags ------------------------------------------------


# --- primary keys (single + composite) --------------------------------------


# --- foreign keys (single, composite, self-ref, multiple to same table) -----


# --- determinism ------------------------------------------------------------


# --- exactly three read-only queries on one connection ----------------------


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


def test_dependency_unavailable(tmp_path):
    store, _ = _setup(tmp_path)

    def factory(conn_str, timeout):
        raise ImportError("no pyodbc")

    disc = SqlStructuralDiscoverer(store, connection_factory=factory)
    result = disc.run(SqlStructuralDiscoveryRequest(project_name="test-proj", access=_access()))
    assert result.discovery.status == SqlDiscoveryStatus.dependency_unavailable


# --- DSN reuse (no second connection builder) -------------------------------


# --- persistence + delegation + layering ------------------------------------


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


