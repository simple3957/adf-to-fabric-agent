"""ADLS Gen2 Discovery (Phase 1) tests — fully offline and deterministic.

A fake DataLakeServiceClient is injected via the agent's ``service_factory``
seam, so no Azure/network is needed. The fake exposes ONLY ``list_file_systems``
and raises on any other attribute access, proving Phase 1 never lists
paths/directories/files or performs writes. Failure modes are simulated with
plain fakes whose class names / ``status_code`` match what the normalizer keys on.
"""

from __future__ import annotations

import inspect

import adf2fabric.agent.discovery.adls.discoverer as discoverer_mod
import adf2fabric.agent.discovery.adls.filesystems as filesystems_mod
from adf2fabric.agent.discovery.adls import AdlsDiscoverer, AdlsDiscoveryRequest
from adf2fabric.project.discovery import AdlsDiscovery, AdlsDiscoveryStatus
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.store import ProjectStore


# --- fakes ------------------------------------------------------------------


class FakeFS:
    def __init__(self, name, last_modified=None):
        self.name = name
        self.last_modified = last_modified


class FakeService:
    """Only ``list_file_systems`` is legal; any other access is a boundary breach."""

    def __init__(self, filesystems=(), raise_exc=None):
        self._filesystems = list(filesystems)
        self._raise = raise_exc
        self.calls: list[str] = []

    def list_file_systems(self):
        self.calls.append("list_file_systems")
        if self._raise is not None:
            raise self._raise
        return list(self._filesystems)

    def __getattr__(self, name):
        raise AssertionError(f"forbidden ADLS operation accessed: {name!r}")


# Fakes whose __name__/status_code match the real azure exceptions the
# normalizer keys on (kept decoupled from azure imports).
class ClientAuthenticationError(Exception):
    pass


class ServiceRequestError(Exception):
    pass


class HttpResponseError(Exception):
    def __init__(self, status_code):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}")


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


def test_successful_discovery(tmp_path):
    svc = FakeService([FakeFS("raw"), FakeFS("curated"), FakeFS("staging")])
    store, pid = _setup(tmp_path)
    result = _run(store, svc)
    assert result.success
    d = result.discovery
    assert d.status == AdlsDiscoveryStatus.discovered
    assert d.filesystem_count == 3
    assert [f.name for f in d.filesystems] == ["curated", "raw", "staging"]
    assert d.account_url == "https://adfstorage299.dfs.core.windows.net"


# --- 2. zero filesystems (success, not an error) ----------------------------


def test_zero_filesystems(tmp_path):
    store, pid = _setup(tmp_path)
    result = _run(store, FakeService([]))
    assert result.success
    assert result.discovery.status == AdlsDiscoveryStatus.discovered
    assert result.discovery.filesystem_count == 0
    assert result.discovery.filesystems == []


# --- 4. deterministic ordering + repeat identical ---------------------------


def test_deterministic_sorted_and_repeatable(tmp_path):
    store, pid = _setup(tmp_path)
    first = _run(store, FakeService([FakeFS("z"), FakeFS("a"), FakeFS("m")])).discovery
    assert [f.name for f in first.filesystems] == ["a", "m", "z"]
    second = _run(store, FakeService([FakeFS("m"), FakeFS("z"), FakeFS("a")])).discovery
    assert discoverer_mod._semantic(first) == discoverer_mod._semantic(second)
    assert second.created_at == first.created_at
    assert second.updated_at == first.updated_at


def test_last_modified_metadata_captured(tmp_path):
    import datetime as _dt

    store, pid = _setup(tmp_path)
    ts = _dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=_dt.timezone.utc)
    d = _run(store, FakeService([FakeFS("raw", last_modified=ts)])).discovery
    assert d.filesystems[0].last_modified == ts.isoformat()


# --- 5-7. failure modes are distinct and never collapsed to empty -----------


def test_authentication_failure(tmp_path):
    store, pid = _setup(tmp_path)
    result = _run(store, FakeService(raise_exc=ClientAuthenticationError("denied")))
    assert result.success is False
    assert result.discovery.status == AdlsDiscoveryStatus.authentication_failed
    assert result.discovery.filesystem_count == 0


def test_permission_failure_not_empty(tmp_path):
    store, pid = _setup(tmp_path)
    result = _run(store, FakeService(raise_exc=HttpResponseError(403)))
    assert result.success is False
    assert result.discovery.status == AdlsDiscoveryStatus.permission_denied
    # a permission error must NOT be reported as "zero filesystems"
    assert result.discovery.filesystems == []


def test_connectivity_failure(tmp_path):
    store, pid = _setup(tmp_path)
    result = _run(store, FakeService(raise_exc=ServiceRequestError("no route")))
    assert result.success is False
    assert result.discovery.status == AdlsDiscoveryStatus.connectivity_failed


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


def test_only_list_file_systems_called(tmp_path):
    store, pid = _setup(tmp_path)
    svc = FakeService([FakeFS("raw")])
    result = _run(store, svc)
    assert result.success
    assert svc.calls == ["list_file_systems"]  # nothing else was touched


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


def test_no_secret_fields_or_values(tmp_path):
    store, pid = _setup(tmp_path)
    d = _run(store, FakeService([FakeFS("raw")])).discovery
    fields = set(AdlsDiscovery.model_fields)
    assert fields.isdisjoint(
        {"credential", "token", "secret", "password", "connection_string", "sas"}
    )
    serialized = d.model_dump_json()
    for marker in ("Bearer", "AccountKey=", "SharedAccessSignature", "password"):
        assert marker not in serialized
    # persisted YAML likewise clean
    text = store.adls_discovery_path(pid).read_text(encoding="utf-8")
    assert "Bearer" not in text and "AccountKey=" not in text


# --- 12 & 13. no ARM / no SQL -----------------------------------------------


def test_no_arm_or_sql_in_source():
    src = inspect.getsource(filesystems_mod) + inspect.getsource(discoverer_mod)
    for forbidden in ("management.azure.com", "az account", "az storage", "subscription", "pyodbc", "sql_server"):
        assert forbidden not in src, f"ADLS discovery must not reference {forbidden!r}"


# --- 14. source repository untouched ----------------------------------------


def test_source_repo_untouched(tmp_path):
    store, pid = _setup(tmp_path)
    source = store.source_dir(pid)
    source.mkdir(parents=True, exist_ok=True)
    marker = source / "keep.txt"
    marker.write_text("original", encoding="utf-8")
    _run(store, FakeService([FakeFS("raw")]))
    assert marker.read_text(encoding="utf-8") == "original"


# --- persistence + boundary: project layer never imports agent --------------


def test_persisted_adls_round_trips(tmp_path):
    store, pid = _setup(tmp_path)
    _run(store, FakeService([FakeFS("raw"), FakeFS("curated")]))
    loaded = store.load_adls_discovery(pid)
    assert loaded.storage_account == "adfstorage299"
    assert [f.name for f in loaded.filesystems] == ["curated", "raw"]


def test_project_layer_does_not_import_agent():
    import adf2fabric.project.discovery as disc
    import adf2fabric.project.store as store_mod

    for mod in (disc, store_mod):
        assert "adf2fabric.agent" not in inspect.getsource(mod)


def test_missing_storage_account(tmp_path):
    store, pid = _setup(tmp_path)
    result = _run(store, FakeService([FakeFS("raw")]), account="")
    assert result.success is False
    assert result.discovery.status == AdlsDiscoveryStatus.missing_config


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
