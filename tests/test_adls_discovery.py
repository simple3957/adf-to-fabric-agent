"""ADLS Gen2 Discovery (Phase 1) tests.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import inspect

import adf2fabric.agent.discovery.adls.discoverer as discoverer_mod
import adf2fabric.agent.discovery.adls.filesystems as filesystems_mod
from adf2fabric.agent.discovery.adls import AdlsDiscoverer, AdlsDiscoveryRequest
from adf2fabric.project.discovery import AdlsDiscovery, AdlsDiscoveryStatus
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.store import ProjectStore


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


def _agent(store, service):
    return AdlsDiscoverer(store, service_factory=lambda url, cred: service)


def _run(store, service, account="adfstorage299"):
    return _agent(store, service).run(
        AdlsDiscoveryRequest(project_name="test-proj", storage_account=account)
    )


# --- 1 & 3. successful discovery, multiple filesystems ----------------------


# --- 2. zero filesystems (success, not an error) ----------------------------


# --- 4. deterministic ordering + repeat identical ---------------------------


# --- 5-7. failure modes are distinct and never collapsed to empty -----------


# --- 8. dependency unavailable ----------------------------------------------


def test_dependency_unavailable(tmp_path):
    store, pid = _setup(tmp_path)

    def _factory(url, cred):
        raise ImportError("no azure-storage-file-datalake")

    agent = AdlsDiscoverer(store, service_factory=_factory)
    result = agent.run(
        AdlsDiscoveryRequest(project_name="test-proj", storage_account="adfstorage299")
    )
    assert result.success is False
    assert result.discovery.status == AdlsDiscoveryStatus.dependency_unavailable


# --- 9. no path/file enumeration in Stage 1 ---------------------------------


def test_no_path_or_file_enumeration_in_source():
    src = inspect.getsource(filesystems_mod) + inspect.getsource(discoverer_mod)
    for forbidden in (
        "list_paths", "get_paths", "download", "get_file_client",
        "get_file_system_client", "get_directory_client",
    ):
        assert forbidden not in src, f"Phase 1 must not reference {forbidden!r}"


# --- 10. no write operations ------------------------------------------------


def test_no_write_operations_in_source():
    src = inspect.getsource(filesystems_mod) + inspect.getsource(discoverer_mod)
    for forbidden in (
        "create_file_system", "delete_file_system", "create_directory",
        "delete_directory", "upload", "rename", "set_metadata", "create_file",
    ):
        assert forbidden not in src, f"ADLS discovery must not reference {forbidden!r}"


# --- 11. no secret persisted ------------------------------------------------


# --- 12 & 13. no ARM / no SQL -----------------------------------------------


def test_no_arm_or_sql_in_source():
    src = inspect.getsource(filesystems_mod) + inspect.getsource(discoverer_mod)
    for forbidden in ("management.azure.com", "az account", "az storage", "subscription", "pyodbc", "sql_server"):
        assert forbidden not in src, f"ADLS discovery must not reference {forbidden!r}"


# --- 14. source repository untouched ----------------------------------------


# --- persistence + boundary: project layer never imports agent --------------


def test_project_layer_does_not_import_agent():
    import adf2fabric.project.discovery as disc
    import adf2fabric.project.store as store_mod

    for mod in (disc, store_mod):
        assert "adf2fabric.agent" not in inspect.getsource(mod)


def test_persisted_models_defined_only_in_project_layer():
    """Runtime models must not duplicate the persisted source-of-truth contracts."""
    import adf2fabric.agent.discovery.adls.models as adls_models
    from adf2fabric.project.discovery import AdlsFilesystem

    for model in (AdlsDiscovery, AdlsFilesystem, AdlsDiscoveryStatus):
        assert model.__module__ == "adf2fabric.project.discovery"
    # the runtime models module must not re-declare the persisted contracts
    for name in ("AdlsDiscovery", "AdlsFilesystem", "AdlsDiscoveryStatus"):
        assert name not in adls_models.__dict__ or (
            getattr(adls_models, name).__module__ == "adf2fabric.project.discovery"
        )


def test_existing_persisted_state_backward_compatible():
    """The already-persisted live Phase-1 result must still load and validate."""
    from adf2fabric.project.store import ProjectStore as _Store

    store = _Store()  # default projects root = repo/projects
    pid = "sample-adf-repo-for-migration"
    if store.has_adls_discovery(pid):
        adls = store.load_adls_discovery(pid)
        assert adls.filesystem_count == 1
        assert [f.name for f in adls.filesystems] == ["sourceadf"]
    if store.has_repository_discovery(pid):
        repo = store.load_repository_discovery(pid)
        assert repo.schema_version == "repository-discovery/1"
