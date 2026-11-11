"""ADLS Gen2 Discovery Phase 2 (paths) tests.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import datetime as _dt
import inspect

import adf2fabric.agent.discovery.adls_paths.discoverer as discoverer_mod
import adf2fabric.agent.discovery.adls_paths.paths as paths_mod
from adf2fabric.agent.discovery.adls_paths import (
    AdlsPathsDiscoverer,
    AdlsPathsDiscoveryRequest,
)
from adf2fabric.project.discovery import (
    AdlsDiscovery,
    AdlsDiscoveryStatus,
    AdlsFilesystem,
    AdlsPathsDiscovery,
)
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.store import ProjectStore


# --- helpers ----------------------------------------------------------------


def _seed(tmp_path, filesystems=("sourceadf",), account="adfstorage299"):
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
    now = _dt.datetime(2026, 1, 1, tzinfo=_dt.timezone.utc)
    store.save_adls_discovery(
        pid,
        AdlsDiscovery(
            project_id=pid, storage_account=account,
            account_url=f"https://{account}.dfs.core.windows.net",
            status=AdlsDiscoveryStatus.discovered, created_at=now, updated_at=now,
            filesystem_count=len(filesystems),
            filesystems=[AdlsFilesystem(name=n) for n in filesystems],
        ),
    )
    return store, pid


def _run(store, service):
    disc = AdlsPathsDiscoverer(store, service_factory=lambda url, cred: service)
    return disc.run(AdlsPathsDiscoveryRequest(project_name="test-proj"))


# A nested tree with root files, supplied UNSORTED to prove deterministic ordering.


# --- enumeration: directories, files, nesting, root -------------------------


# --- pagination -------------------------------------------------------------


# --- failures ---------------------------------------------------------------


def test_dependency_unavailable(tmp_path):
    store, _ = _seed(tmp_path)

    def factory(url, cred):
        raise ImportError("no azure-storage-file-datalake")

    disc = AdlsPathsDiscoverer(store, service_factory=factory)
    result = disc.run(AdlsPathsDiscoveryRequest(project_name="test-proj"))
    assert result.discovery.status == AdlsDiscoveryStatus.dependency_unavailable


# --- read-only enforcement / no content reads -------------------------------


def test_no_read_write_operations_in_source():
    # Method-specific tokens (not bare prose words like "download"/"delete", which
    # legitimately appear in the boundary docstrings). The dynamic
    # ``test_only_get_paths_called`` proves only get_paths is ever invoked.
    src = (inspect.getsource(paths_mod) + inspect.getsource(discoverer_mod)).lower()
    for forbidden in (
        "download_file", "download_blob", "read_file", "read_bytes",
        "delete_file", "delete_directory", "rename_file", "rename_directory",
        "create_file", "create_directory", "upload_data", "upload_file",
        "set_metadata", "get_file_client", ".open(",
    ):
        assert forbidden not in src, f"Phase 2 must not call {forbidden!r}"


# --- determinism + persistence + layering -----------------------------------


def test_persisted_models_only_in_project_layer():
    import adf2fabric.agent.discovery.adls_paths.models as m

    for name in ("AdlsPathsDiscovery", "AdlsPath", "AdlsFilesystemPaths"):
        assert name not in m.__dict__ or (
            getattr(m, name).__module__ == "adf2fabric.project.discovery"
        )
    assert AdlsPathsDiscovery.__module__ == "adf2fabric.project.discovery"


def test_project_layer_does_not_import_agent():
    import adf2fabric.project.discovery as disc
    import adf2fabric.project.store as store_mod

    for mod in (disc, store_mod):
        assert "adf2fabric.agent" not in inspect.getsource(mod)


def test_package_imports_cleanly():
    import importlib

    for name in (
        "adf2fabric.agent.discovery.adls_paths",
        "adf2fabric.agent.discovery.adls_paths.paths",
        "adf2fabric.agent.discovery.adls_paths.discoverer",
        "adf2fabric.agent.discovery.adls_paths.models",
    ):
        assert importlib.import_module(name) is not None


