"""ADLS Gen2 Discovery Phase 2 (paths) tests — fully offline, deterministic.

A fake DataLakeServiceClient is injected via the discoverer's ``service_factory``
seam. The fake exposes ONLY ``get_file_system_client`` and ``get_paths`` and
raises on any other attribute access, proving Phase 2 lists paths but never reads
contents, downloads, writes, deletes, or renames. The Phase-1 ``AdlsDiscovery``
record is seeded via the store, since Phase 2 reuses it for account + filesystem
identity. No Azure, no network.
"""

from __future__ import annotations

import datetime as _dt
import inspect

import adf2fabric.agent.discovery.adls_paths.discoverer as discoverer_mod
import adf2fabric.agent.discovery.adls_paths.paths as paths_mod
from adf2fabric.agent.discovery.adls_paths import (
    AdlsPathsDiscoverer,
    AdlsPathsDiscoveryRequest,
    AdlsPathsDiscoveryResult,
)
from adf2fabric.project.discovery import (
    AdlsDiscovery,
    AdlsDiscoveryStatus,
    AdlsFilesystem,
    AdlsPathsDiscovery,
)
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.store import ProjectStore


# --- fakes ------------------------------------------------------------------


class FakeProp:
    def __init__(self, name, is_directory=False, content_length=None, last_modified=None):
        self.name = name
        self.is_directory = is_directory
        self.content_length = content_length
        self.last_modified = last_modified


class FakePaged:
    """Mimics the SDK ItemPaged: by_page() yields successive pages of items."""

    def __init__(self, items, page_size=1000):
        self._items = list(items)
        self._page_size = page_size

    def by_page(self):
        for i in range(0, len(self._items), self._page_size):
            yield list(self._items[i : i + self._page_size])


class FakeFsClient:
    """Only ``get_paths`` is legal; any other access is a boundary breach."""

    def __init__(self, items, log, raise_exc=None, page_size=1000):
        object.__setattr__(self, "_items", items)
        object.__setattr__(self, "_log", log)
        object.__setattr__(self, "_raise", raise_exc)
        object.__setattr__(self, "_page_size", page_size)

    def get_paths(self, recursive=False, **kwargs):
        self._log.append(("get_paths", recursive))
        if self._raise is not None:
            raise self._raise
        return FakePaged(self._items, self._page_size)

    def __getattr__(self, name):
        raise AssertionError(f"forbidden ADLS filesystem operation accessed: {name!r}")


class FakeService:
    def __init__(self, fs_map, page_size=1000):
        # fs_map: filesystem name -> list[FakeProp] OR an Exception to raise.
        object.__setattr__(self, "_fs_map", fs_map)
        object.__setattr__(self, "_page_size", page_size)
        object.__setattr__(self, "calls", [])

    def get_file_system_client(self, name):
        self.calls.append(("get_file_system_client", name))
        entry = self._fs_map.get(name, [])
        if isinstance(entry, Exception):
            return FakeFsClient([], self.calls, raise_exc=entry)
        return FakeFsClient(entry, self.calls, page_size=self._page_size)

    def __getattr__(self, name):
        raise AssertionError(f"forbidden ADLS service operation accessed: {name!r}")


class ClientAuthenticationError(Exception):
    pass


class ServiceRequestError(Exception):
    pass


class HttpResponseError(Exception):
    def __init__(self, status_code):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}")


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


_LM = _dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=_dt.timezone.utc)

# A nested tree with root files, supplied UNSORTED to prove deterministic ordering.
_TREE = [
    FakeProp("raw/FHRS", is_directory=True),
    FakeProp("rootfile.txt", content_length=10, last_modified=_LM),
    FakeProp("raw", is_directory=True),
    FakeProp("raw/FHRS/Authorities/auth_20200801.json", content_length=1234, last_modified=_LM),
    FakeProp("curated", is_directory=True),
    FakeProp("raw/FHRS/Authorities", is_directory=True),
    FakeProp("raw/FHRS/file2.json", content_length=50),
]


# --- enumeration: directories, files, nesting, root -------------------------


def test_directory_and_file_enumeration(tmp_path):
    store, _ = _seed(tmp_path)
    result = _run(store, FakeService({"sourceadf": _TREE}))
    assert result.success
    d = result.discovery
    assert d.status == AdlsDiscoveryStatus.discovered
    assert d.filesystem_count == 1
    fs = d.filesystems[0]
    assert fs.name == "sourceadf"
    assert fs.path_count == 7
    assert fs.directory_count == 4  # raw, raw/FHRS, raw/FHRS/Authorities, curated
    assert fs.file_count == 3
    assert fs.total_size_bytes == 1234 + 50 + 10
    # deterministic sort by path
    assert [p.path for p in fs.paths] == [
        "curated", "raw", "raw/FHRS", "raw/FHRS/Authorities",
        "raw/FHRS/Authorities/auth_20200801.json", "raw/FHRS/file2.json", "rootfile.txt",
    ]


def test_path_metadata_normalization(tmp_path):
    store, _ = _seed(tmp_path)
    d = _run(store, FakeService({"sourceadf": _TREE})).discovery
    by_path = {p.path: p for p in d.filesystems[0].paths}
    # nested file: directory + name + size + last_modified normalized to ISO
    nested = by_path["raw/FHRS/Authorities/auth_20200801.json"]
    assert nested.is_directory is False
    assert nested.directory == "raw/FHRS/Authorities"
    assert nested.name == "auth_20200801.json"
    assert nested.size_bytes == 1234
    assert nested.last_modified == _LM.isoformat()
    # root file: empty directory
    root = by_path["rootfile.txt"]
    assert root.directory == "" and root.name == "rootfile.txt"
    # directory: no size
    d_raw = by_path["raw"]
    assert d_raw.is_directory is True and d_raw.size_bytes is None


def test_empty_filesystem_is_success(tmp_path):
    store, _ = _seed(tmp_path)
    d = _run(store, FakeService({"sourceadf": []})).discovery
    assert d.status == AdlsDiscoveryStatus.discovered
    assert d.total_path_count == 0
    assert d.filesystems[0].path_count == 0


def test_multiple_filesystems(tmp_path):
    store, _ = _seed(tmp_path, filesystems=("raw", "curated"))
    service = FakeService({
        "raw": [FakeProp("a.json", content_length=5)],
        "curated": [FakeProp("dir", is_directory=True), FakeProp("dir/b.csv", content_length=7)],
    })
    d = _run(store, service).discovery
    assert d.filesystem_count == 2
    names = {fs.name: fs for fs in d.filesystems}
    assert names["raw"].file_count == 1
    assert names["curated"].directory_count == 1 and names["curated"].file_count == 1
    assert d.total_file_count == 2


# --- pagination -------------------------------------------------------------


def test_pagination_followed_fully(tmp_path):
    store, _ = _seed(tmp_path)
    many = [FakeProp(f"dir/file_{i:05d}.json", content_length=1) for i in range(2500)]
    service = FakeService({"sourceadf": many}, page_size=1000)  # -> 3 pages
    d = _run(store, service).discovery
    assert d.filesystems[0].file_count == 2500
    assert d.filesystems[0].complete is True and d.filesystems[0].truncated is False


# --- failures ---------------------------------------------------------------


def test_missing_phase1_is_missing_config(tmp_path):
    store = ProjectStore(tmp_path / "projects")
    pid = "test-proj"
    store.save(
        Project(
            id=pid, name="T",
            repository=RepositoryMetadata(
                provider="github", repository_url="https://x.invalid/x.git",
                branch="main", commit_sha="0" * 40, local_source_path=str(store.source_dir(pid)),
            ),
        )
    )
    result = _run(store, FakeService({}))
    assert result.success is False
    assert result.discovery.status == AdlsDiscoveryStatus.missing_config


def test_authentication_failure(tmp_path):
    store, _ = _seed(tmp_path)
    result = _run(store, FakeService({"sourceadf": ClientAuthenticationError("denied")}))
    assert result.success is False
    assert result.discovery.status == AdlsDiscoveryStatus.authentication_failed


def test_permission_failure(tmp_path):
    store, _ = _seed(tmp_path)
    result = _run(store, FakeService({"sourceadf": HttpResponseError(403)}))
    assert result.discovery.status == AdlsDiscoveryStatus.permission_denied


def test_connectivity_failure(tmp_path):
    store, _ = _seed(tmp_path)
    result = _run(store, FakeService({"sourceadf": ServiceRequestError("no route")}))
    assert result.discovery.status == AdlsDiscoveryStatus.connectivity_failed


def test_dependency_unavailable(tmp_path):
    store, _ = _seed(tmp_path)

    def factory(url, cred):
        raise ImportError("no azure-storage-file-datalake")

    disc = AdlsPathsDiscoverer(store, service_factory=factory)
    result = disc.run(AdlsPathsDiscoveryRequest(project_name="test-proj"))
    assert result.discovery.status == AdlsDiscoveryStatus.dependency_unavailable


# --- read-only enforcement / no content reads -------------------------------


def test_only_get_paths_called(tmp_path):
    store, _ = _seed(tmp_path)
    service = FakeService({"sourceadf": _TREE})
    _run(store, service)
    assert service.calls == [
        ("get_file_system_client", "sourceadf"),
        ("get_paths", True),
    ]


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


def test_no_secret_fields_or_values(tmp_path):
    store, pid = _seed(tmp_path)
    d = _run(store, FakeService({"sourceadf": _TREE})).discovery
    fields = set(AdlsPathsDiscovery.model_fields)
    assert fields.isdisjoint(
        {"credential", "token", "secret", "password", "connection_string", "sas", "account_key"}
    )
    serialized = d.model_dump_json()
    for marker in ("Bearer", "AccountKey=", "SharedAccessSignature", "sig=", "password"):
        assert marker not in serialized
    text = store.adls_paths_discovery_path(pid).read_text(encoding="utf-8")
    assert "Bearer" not in text and "AccountKey=" not in text


# --- determinism + persistence + layering -----------------------------------


def test_deterministic_repeatable_timestamps(tmp_path):
    store, _ = _seed(tmp_path)
    first = _run(store, FakeService({"sourceadf": _TREE})).discovery
    second = _run(store, FakeService({"sourceadf": list(reversed(_TREE))})).discovery
    assert discoverer_mod._semantic(first) == discoverer_mod._semantic(second)
    assert second.created_at == first.created_at
    assert second.updated_at == first.updated_at


def test_persisted_round_trips(tmp_path):
    store, pid = _seed(tmp_path)
    _run(store, FakeService({"sourceadf": _TREE}))
    loaded = store.load_adls_paths_discovery(pid)
    assert loaded.schema_version == "adls-paths-discovery/1"
    assert loaded.storage_account == "adfstorage299"
    assert loaded.filesystems[0].file_count == 3


def test_discovery_agent_delegates(tmp_path):
    from adf2fabric.agent.discovery import DiscoveryAgent

    store, _ = _seed(tmp_path)
    sentinel = AdlsPathsDiscoveryResult(success=True, message="delegated")

    class Fake:
        def __init__(self):
            self.calls = []

        def run(self, request):
            self.calls.append(request)
            return sentinel

    fake = Fake()
    agent = DiscoveryAgent(store, adls_paths_discoverer=fake)
    req = AdlsPathsDiscoveryRequest(project_name="test-proj")
    assert agent.discover_adls_paths(req) is sentinel and fake.calls == [req]


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


def test_phase1_adls_yaml_untouched(tmp_path):
    store, pid = _seed(tmp_path)
    before = store.adls_discovery_path(pid).read_bytes()
    _run(store, FakeService({"sourceadf": _TREE}))
    assert store.adls_discovery_path(pid).read_bytes() == before  # Phase-1 record intact
    assert store.has_adls_paths_discovery(pid)  # wrote its own separate file
