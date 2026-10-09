"""ADF Live Discovery (Phase 1) tests — fully offline and deterministic.

A fake ARM transport and a fake Azure credential are injected via the
discoverer's ``transport_factory`` + ``credential_builder`` seams, so no
requests, no azure-identity, no network, and no real token are needed. The fake
transport logs every GET and RAISES on any write verb, proving Phase 1 issues
exactly the six read-only child-collection GETs and never mutates the factory.
Each fake ARM item carries secret-looking ``properties``; the tests prove only
``name`` (+ kind) is ever persisted.
"""

from __future__ import annotations

import inspect

import adf2fabric.agent.discovery.adf.arm as arm_mod
import adf2fabric.agent.discovery.adf.discoverer as discoverer_mod
from adf2fabric.agent.discovery.adf import (
    AdfDiscoverer,
    AdfDiscoveryRequest,
    AdfDiscoveryResult,
)
from adf2fabric.agent.discovery.adf.arm import COLLECTION_LABELS
from adf2fabric.connectivity._azure import CredentialUnavailable
from adf2fabric.project.discovery import (
    AdfArtifact,
    AdfArtifactKind,
    AdfDiscovery,
    AdfDiscoveryStatus,
)
from adf2fabric.project.models import (
    AdfAccess,
    AzureCredentialRef,
    ConnectivityStatus,
    Project,
    RepositoryMetadata,
)
from adf2fabric.project.store import ProjectStore

SECRET_MARKER = "SUPERSECRET-CONNECTION-STRING"


# --- fakes ------------------------------------------------------------------


class FakeResp:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class FakeTransport:
    """GET-only ARM transport. Serves per-segment pages; write verbs are illegal."""

    def __init__(self, pages_by_segment=None, error_status=None, raise_on_get=None):
        # pages_by_segment: segment -> list of (names, has_next) pages.
        self.pages_by_segment = pages_by_segment or {}
        self.error_status = error_status
        self.raise_on_get = raise_on_get
        self.calls: list[str] = []
        self._links: dict[str, tuple[str, int]] = {}

    def get(self, url, headers=None, timeout=None):
        assert headers and "Authorization" in headers, "ARM GET must be authorized"
        self.calls.append(url)
        if self.raise_on_get is not None:
            raise self.raise_on_get
        if self.error_status is not None:
            return FakeResp(self.error_status)
        segment, page_idx = self._links.get(url, (self._segment_for(url), 0))
        pages = self.pages_by_segment.get(segment, [([], False)])
        if page_idx >= len(pages):
            return FakeResp(200, {"value": []})
        names, has_next = pages[page_idx]
        payload = {"value": [self._item(n) for n in names]}
        if has_next:
            nl = f"https://management.azure.com/__next__/{segment}/{page_idx + 1}"
            self._links[nl] = (segment, page_idx + 1)
            payload["nextLink"] = nl
        return FakeResp(200, payload)

    @staticmethod
    def _segment_for(url):
        return url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]

    @staticmethod
    def _item(name):
        # Full ARM shape, including secret-looking material that MUST be discarded.
        return {
            "id": f"/subscriptions/x/providers/Microsoft.DataFactory/.../{name}",
            "name": name,
            "type": "Microsoft.DataFactory/factories/pipelines",
            "properties": {
                "activities": [{"name": "Copy1"}],
                "typeProperties": {"connectionString": SECRET_MARKER},
            },
        }

    def _forbidden(self, *a, **k):  # pragma: no cover - must never be called
        raise AssertionError("ADF discovery must issue no write request")

    post = put = patch = delete = _forbidden


class FakeToken:
    def __init__(self, token):
        self.token = token


class FakeCredential:
    def __init__(self, token="FAKE.ARM.TOKEN.VALUE", raise_exc=None):
        self.token = token
        self.raise_exc = raise_exc
        self.scopes: list[str] = []

    def get_token(self, scope):
        self.scopes.append(scope)
        if self.raise_exc is not None:
            raise self.raise_exc
        return FakeToken(self.token)


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
        subscription_id="sub-123",
        resource_group="rg-data",
        factory_name="adf-prod",
        credential=AzureCredentialRef(auth="azure_cli"),
    )
    base.update(over)
    return AdfAccess(**base)


def _discoverer(store, transport, credential=None, credential_builder=None, secret=""):
    cred = credential if credential is not None else FakeCredential()
    builder = credential_builder or (lambda ref: (cred, secret))
    return AdfDiscoverer(
        store, transport_factory=lambda: transport, credential_builder=builder
    )


def _run(store, transport, access=None, **disc_kw):
    disc = _discoverer(store, transport, **disc_kw)
    return disc.run(
        AdfDiscoveryRequest(project_name="test-proj", access=access or _access())
    )


def _single(**by_segment):
    """Build single-page responses; each kwarg is segment=list_of_names."""
    return {seg: [(names, False)] for seg, names in by_segment.items()}


# A representative factory, supplied UNSORTED to prove deterministic ordering.
_FULL = _single(
    pipelines=["PL_Copy", "PL_Load", "PL_Aggregate"],
    datasets=["DS_Sink", "DS_Source"],
    linkedservices=["LS_Sql"],
    triggers=["TRG_Daily"],
    dataflows=[],
    integrationruntimes=["AutoResolveIntegrationRuntime"],
)


# --- success + mapping ------------------------------------------------------


def test_successful_discovery_maps_all_kinds(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(_FULL))
    assert result.success
    d = result.discovery
    assert d.status == AdfDiscoveryStatus.discovered
    assert d.subscription_id == "sub-123"
    assert d.resource_group == "rg-data"
    assert d.factory_name == "adf-prod"
    assert d.authentication == "azure_cli"
    assert d.pipeline_count == 3
    assert [a.name for a in d.pipelines] == ["PL_Aggregate", "PL_Copy", "PL_Load"]
    assert all(a.kind == AdfArtifactKind.pipeline for a in d.pipelines)
    assert [a.name for a in d.datasets] == ["DS_Sink", "DS_Source"]
    assert d.linked_service_count == 1 and d.linked_services[0].name == "LS_Sql"
    assert d.linked_services[0].kind == AdfArtifactKind.linked_service
    assert d.trigger_count == 1
    assert d.dataflow_count == 0 and d.dataflows == []
    assert d.integration_runtime_count == 1
    assert d.artifact_count == 3 + 2 + 1 + 1 + 0 + 1
    assert d.collections_queried == COLLECTION_LABELS


def test_empty_factory_is_success(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(_single()))
    assert result.success
    d = result.discovery
    assert d.status == AdfDiscoveryStatus.discovered
    assert d.artifact_count == 0
    assert (d.pipeline_count, d.dataset_count, d.linked_service_count) == (0, 0, 0)
    assert (d.trigger_count, d.dataflow_count, d.integration_runtime_count) == (0, 0, 0)


def test_deterministic_repeatable_timestamps(tmp_path):
    store, _ = _setup(tmp_path)
    first = _run(store, FakeTransport(_FULL)).discovery
    second = _run(store, FakeTransport(_FULL)).discovery
    assert discoverer_mod._semantic(first) == discoverer_mod._semantic(second)
    assert second.created_at == first.created_at
    assert second.updated_at == first.updated_at


# --- pagination -------------------------------------------------------------


def test_pagination_follows_next_link(tmp_path):
    store, _ = _setup(tmp_path)
    pages = _single()
    # pipelines paginated across two pages
    pages["pipelines"] = [(["PL_a"], True), (["PL_b"], False)]
    result = _run(store, FakeTransport(pages))
    assert result.success
    assert [a.name for a in result.discovery.pipelines] == ["PL_a", "PL_b"]


# --- exactly six GETs, in order, GET-only -----------------------------------


def test_exactly_six_collection_gets_in_order(tmp_path):
    store, _ = _setup(tmp_path)
    transport = FakeTransport(_FULL)
    result = _run(store, transport)
    assert result.success
    segments = [FakeTransport._segment_for(u) for u in transport.calls]
    assert segments == COLLECTION_LABELS  # exactly these six, this order, once each


def test_no_write_verbs_in_source():
    src = (
        inspect.getsource(arm_mod) + inspect.getsource(discoverer_mod)
    ).lower()
    for forbidden in (
        ".post(", ".put(", ".patch(", ".delete(",
        "createrun", "createpipelinerun", "?api-version=2018-06-01&action",
    ):
        assert forbidden not in src, f"ADF discovery must not reference {forbidden!r}"


# --- name + kind only; no secret persisted ----------------------------------


def test_only_name_and_kind_persisted_no_secrets(tmp_path):
    store, pid = _setup(tmp_path)
    result = _run(store, FakeTransport(_FULL))
    assert result.success
    # persisted contract carries no property/secret fields
    fields = set(AdfArtifact.model_fields)
    assert fields == {"name", "kind"}
    disc_fields = set(AdfDiscovery.model_fields)
    assert disc_fields.isdisjoint(
        {"properties", "type_properties", "connection", "token", "secret", "password"}
    )
    serialized = result.discovery.model_dump_json()
    for marker in (SECRET_MARKER, "connectionString", "typeProperties", "activities", "properties"):
        assert marker not in serialized
    text = store.adf_discovery_path(pid).read_text(encoding="utf-8")
    assert SECRET_MARKER not in text and "connectionString" not in text


# --- config / credential guards ---------------------------------------------


def test_missing_subscription(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(_FULL), access=_access(subscription_id=None))
    assert result.success is False
    assert result.discovery.status == AdfDiscoveryStatus.missing_config
    assert "subscription_id" in result.discovery.detail


def test_missing_resource_group(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(_FULL), access=_access(resource_group=None))
    assert result.discovery.status == AdfDiscoveryStatus.missing_config


def test_missing_factory(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(_FULL), access=_access(factory_name=None))
    assert result.discovery.status == AdfDiscoveryStatus.missing_config


def test_credential_unavailable_maps_status(tmp_path):
    store, _ = _setup(tmp_path)

    for cred_status, expected in (
        (ConnectivityStatus.dependency_unavailable, AdfDiscoveryStatus.dependency_unavailable),
        (ConnectivityStatus.missing_credential, AdfDiscoveryStatus.missing_credential),
        (ConnectivityStatus.missing_config, AdfDiscoveryStatus.missing_config),
    ):
        def _raise(ref, _s=cred_status):
            raise CredentialUnavailable(_s, "credential problem")

        result = _run(store, FakeTransport(_FULL), credential_builder=_raise)
        assert result.success is False
        assert result.discovery.status == expected


def test_token_acquisition_failure_is_authentication_failed(tmp_path):
    store, _ = _setup(tmp_path)
    cred = FakeCredential(raise_exc=RuntimeError("AADSTS token error"))
    result = _run(store, FakeTransport(_FULL), credential=cred)
    assert result.success is False
    assert result.discovery.status == AdfDiscoveryStatus.authentication_failed


# --- HTTP + network failures are distinct -----------------------------------


def test_http_401_authentication_failed(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(error_status=401))
    assert result.discovery.status == AdfDiscoveryStatus.authentication_failed


def test_http_403_permission_denied(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(error_status=403))
    assert result.discovery.status == AdfDiscoveryStatus.permission_denied


def test_http_404_factory_not_found(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(error_status=404))
    assert result.discovery.status == AdfDiscoveryStatus.factory_not_found


def test_network_exception_connectivity_failed(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(raise_on_get=ConnectionError("no route")))
    assert result.discovery.status == AdfDiscoveryStatus.connectivity_failed


def test_error_result_records_no_collections_queried(tmp_path):
    store, _ = _setup(tmp_path)
    result = _run(store, FakeTransport(error_status=403))
    assert result.discovery.collections_queried == []
    assert result.discovery.artifact_count == 0


# --- redaction: a secret in an error detail is masked -----------------------


def test_token_error_detail_redacts_secret(tmp_path):
    store, _ = _setup(tmp_path)
    secret = "client-secret-value-xyz"
    cred = FakeCredential(raise_exc=RuntimeError(f"failed using {secret}"))
    result = _run(store, FakeTransport(_FULL), credential=cred, secret=secret)
    assert secret not in result.discovery.detail
    assert "***" in result.discovery.detail


# --- persistence + round-trip -----------------------------------------------


def test_persisted_adf_round_trips(tmp_path):
    store, pid = _setup(tmp_path)
    _run(store, FakeTransport(_FULL))
    loaded = store.load_adf_discovery(pid)
    assert loaded.factory_name == "adf-prod"
    assert [a.name for a in loaded.pipelines] == ["PL_Aggregate", "PL_Copy", "PL_Load"]
    assert loaded.integration_runtimes[0].kind == AdfArtifactKind.integration_runtime


# --- delegation + layering + reuse ------------------------------------------


def test_discovery_agent_delegates_to_adf_discoverer(tmp_path):
    from adf2fabric.agent.discovery import DiscoveryAgent

    store, _ = _setup(tmp_path)
    sentinel = AdfDiscoveryResult(success=True, message="delegated")

    class FakeAdf:
        def __init__(self):
            self.calls = []

        def run(self, request):
            self.calls.append(request)
            return sentinel

    fake = FakeAdf()
    agent = DiscoveryAgent(store, adf_discoverer=fake)
    req = AdfDiscoveryRequest(project_name="test-proj", access=_access())
    out = agent.discover_adf(req)
    assert out is sentinel
    assert fake.calls == [req]


def test_persisted_models_defined_only_in_project_layer():
    import adf2fabric.agent.discovery.adf.models as adf_models

    for model in (AdfDiscovery, AdfArtifact, AdfArtifactKind, AdfDiscoveryStatus):
        assert model.__module__ == "adf2fabric.project.discovery"
    for name in ("AdfDiscovery", "AdfArtifact", "AdfDiscoveryStatus"):
        assert name not in adf_models.__dict__ or (
            getattr(adf_models, name).__module__ == "adf2fabric.project.discovery"
        )


def test_project_layer_does_not_import_agent():
    import adf2fabric.project.discovery as disc
    import adf2fabric.project.store as store_mod

    for mod in (disc, store_mod):
        assert "adf2fabric.agent" not in inspect.getsource(mod)


def test_reuses_connector_constants_not_reimplemented():
    from adf2fabric.connectivity import azure_data_factory as conn

    # ARM scope + api-version are the connector's, imported — not re-declared.
    assert arm_mod.ARM_SCOPE is conn.ARM_SCOPE
    assert arm_mod.API_VERSION is conn.API_VERSION
    assert 'ARM_SCOPE = "https' not in inspect.getsource(arm_mod)
    assert "https://management.azure.com/.default" not in inspect.getsource(discoverer_mod)


def test_package_imports_cleanly():
    import importlib

    for name in (
        "adf2fabric.agent.discovery.adf",
        "adf2fabric.agent.discovery.adf.arm",
        "adf2fabric.agent.discovery.adf.discoverer",
        "adf2fabric.agent.discovery.adf.models",
        "adf2fabric.agent.discovery.agent",
    ):
        assert importlib.import_module(name) is not None
