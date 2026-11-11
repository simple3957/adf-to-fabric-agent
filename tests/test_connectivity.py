"""Tests for the five real connectors.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import inspect

import pytest

from adf2fabric.connectivity import azure_data_factory as adf_mod
from adf2fabric.connectivity import sql_server as sql_mod
from adf2fabric.connectivity.adls_gen2 import AdlsGen2Connector
from adf2fabric.connectivity.fabric import FabricConnector
from adf2fabric.connectivity.registry import get_connector, registered_domains
from adf2fabric.project.models import (
    AccessDomain,
)


# --- registry ---------------------------------------------------------------

def test_registry_has_five_current_domains():
    assert registered_domains() == ["adf", "adls_gen2", "fabric", "repository", "sql_server"]


def test_get_connector_returns_instance():
    assert get_connector(AccessDomain.sql_server.value).domain == "sql_server"


def test_get_connector_unknown_raises():
    with pytest.raises(KeyError):
        get_connector("teradata")


# --- repository -------------------------------------------------------------


# --- sql server -------------------------------------------------------------
#


def test_sql_connector_does_not_enumerate_or_mutate():
    """Static guard: SQL connector only runs SELECT 1 — no discovery, no writes."""
    low = inspect.getsource(sql_mod).lower()
    for forbidden in (
        "information_schema",
        "sys.",
        "select *",
        "sp_help",
        "show tables",
        "fetchall",
        "insert ",
        "update ",
        "delete ",
        "drop ",
        "alter ",
        "create table",
    ):
        assert forbidden not in low
    # The one and only executed statement is SELECT 1.
    assert 'execute("select 1")' in low
    assert low.count(".execute(") == 1


# --- adf --------------------------------------------------------------------
#
# Fully offline via seams: `adf_mod._import_requests` (HTTP transport) and
# `adf_mod.build_credential` (authentication). No azure-identity/requests needed.


def test_adf_connector_is_readiness_only():
    """Static guard: ADF connector makes only a factory GET — no discovery/writes."""
    src = adf_mod  # module
    import inspect as _inspect

    low = _inspect.getsource(src).lower()
    for forbidden in (
        "/pipelines",
        "/datasets",
        "/linkedservices",
        "/triggers",
        "/integrationruntimes",
        "/dataflows",
        ".post(",
        ".put(",
        ".patch(",
        ".delete(",
    ):
        assert forbidden not in low
    assert "requests.get" in low or ".get(" in low
    assert "/providers/microsoft.datafactory/factories/" in low


# --- azure credential helper (azure_cli) ------------------------------------


# --- adls gen2 --------------------------------------------------------------


def test_adls_does_not_enumerate():
    """Static guard: the ADLS connector must not list paths/files/directories."""
    src = inspect.getsource(AdlsGen2Connector)
    for forbidden in ("list_paths", "get_paths", "list_file_systems", "download", "read"):
        assert forbidden not in src


# --- fabric -----------------------------------------------------------------


# --- boundary: connectors never create Fabric artifacts ---------------------

def test_fabric_connector_is_read_only():
    src = inspect.getsource(FabricConnector)
    # Only a GET workspace reachability check; no artifact creation/deploy.
    for forbidden in (".post(", ".put(", ".patch(", ".delete(", "create", "deploy"):
        assert forbidden not in src.lower()
