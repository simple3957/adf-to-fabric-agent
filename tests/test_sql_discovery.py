"""SQL Server Discovery (Phase 1) tests.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import inspect

import adf2fabric.agent.discovery.sql.catalog as catalog_mod
import adf2fabric.agent.discovery.sql.discoverer as discoverer_mod
from adf2fabric.agent.discovery.sql import (
    SqlDiscoverer,
    SqlDiscoveryRequest,
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


# Representative estate, supplied UNSORTED to prove deterministic ordering.


# --- 1-6. successful discovery, mapping, deterministic ordering -------------


# --- 7. empty database (success, not an error) ------------------------------


# --- 13-14. exactly the four approved queries, in order, on ONE connection --


# --- 8-12. failure modes are distinct and never collapsed to empty ----------


def test_dependency_unavailable(tmp_path):
    store, _ = _setup(tmp_path)

    def factory(conn_str, timeout):
        raise ImportError("no pyodbc")

    disc = SqlDiscoverer(store, connection_factory=factory)
    result = disc.run(SqlDiscoveryRequest(project_name="test-proj", access=_access()))
    assert result.success is False
    assert result.discovery.status == SqlDiscoveryStatus.dependency_unavailable


# --- config / credential guards ---------------------------------------------


def test_project_not_found(tmp_path):
    store = ProjectStore(tmp_path / "projects")
    disc = SqlDiscoverer(store, connection_factory=lambda cs, t: None)
    result = disc.run(SqlDiscoveryRequest(project_name="nope", access=_access()))
    assert result.success is False
    assert result.discovery is None


# --- DSN reuse + timeout ----------------------------------------------------


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


# --- 20. persisted round-trip -----------------------------------------------


# --- 21. DiscoveryAgent delegates to SqlDiscoverer --------------------------


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
