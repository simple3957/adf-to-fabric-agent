"""ADF Live Discovery (Phase 1) tests.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import inspect

import adf2fabric.agent.discovery.adf.arm as arm_mod
import adf2fabric.agent.discovery.adf.discoverer as discoverer_mod
from adf2fabric.project.discovery import (
    AdfArtifact,
    AdfArtifactKind,
    AdfDiscovery,
    AdfDiscoveryStatus,
)
from adf2fabric.project.models import (
    AdfAccess,
    AzureCredentialRef,
    Project,
    RepositoryMetadata,
)
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


# A representative factory, supplied UNSORTED to prove deterministic ordering.


# --- success + mapping ------------------------------------------------------


# --- pagination -------------------------------------------------------------


# --- exactly six GETs, in order, GET-only -----------------------------------


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


# --- config / credential guards ---------------------------------------------


# --- HTTP + network failures are distinct -----------------------------------


# --- redaction: a secret in an error detail is masked -----------------------


# --- persistence + round-trip -----------------------------------------------


# --- delegation + layering + reuse ------------------------------------------


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
