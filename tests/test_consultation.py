"""End-to-end tests for the Consultation Agent, fully offline.

Connectors are injected as fakes so no network/credentials are needed. The real
connectors are covered separately in ``test_connectivity.py``.
"""

from __future__ import annotations

import pytest
import yaml

from adf2fabric.agent.consultation import ConsultationAgent, ConsultationRequest
from adf2fabric.connectivity.base import outcome
from adf2fabric.project.models import (
    AccessDomain,
    ConnectivityStatus,
    Project,
    RepositoryMetadata,
)
from adf2fabric.project.store import ProjectStore


# --- fakes ------------------------------------------------------------------

class FakeConnector:
    """A connector that returns a fixed status for its domain."""

    def __init__(self, domain: AccessDomain, status: ConnectivityStatus):
        self.domain = domain.value
        self._domain = domain
        self._status = status
        self.calls = 0

    def validate(self, access, project):
        self.calls += 1
        validated = self._status == ConnectivityStatus.validated
        return outcome(
            self._domain,
            self._status,
            reachable=validated,
            authenticated=validated,
            detail=f"fake {self._status.value}",
        )


def all_validated_connectors():
    return {
        d.value: FakeConnector(d, ConnectivityStatus.validated) for d in AccessDomain
    }


# --- fixtures ---------------------------------------------------------------

@pytest.fixture
def store(tmp_path):
    return ProjectStore(tmp_path / "projects")


@pytest.fixture
def project(store):
    """Persist a project record (as Intake would) without any source/ clone."""
    proj = Project(
        id="demo",
        name="Demo",
        repository=RepositoryMetadata(
            provider="github",
            repository_url="https://github.com/acme/demo.git",
            branch="main",
            commit_sha="a" * 40,
            local_source_path=str(store.source_dir("demo")),
        ),
    )
    store.save(proj)
    return proj


def _access_block(required=True, mandatory=True):
    """A fully-specified access block for all five domains."""
    azure_cred = {
        "auth": "client_secret",
        "tenant_id_env": "AZ_TENANT_ID",
        "client_id_env": "AZ_CLIENT_ID",
        "client_secret_env": "AZ_CLIENT_SECRET",
    }
    return {
        "repository": {"required": required, "mandatory": mandatory, "token_env": "GITHUB_TOKEN"},
        "adf": {
            "required": required,
            "mandatory": mandatory,
            "subscription_id": "sub",
            "resource_group": "rg",
            "factory_name": "adf1",
            "credential": azure_cred,
        },
        "sql_server": {
            "required": required,
            "mandatory": mandatory,
            "server": "sql.example.net",
            "database": "WarehouseDB",
            "credential": {"auth": "sql_login", "username": "svc", "password_env": "SQL_PASSWORD"},
        },
        "adls_gen2": {
            "required": required,
            "mandatory": mandatory,
            "storage_account": "acmedatalake",
            "filesystem": "raw",
            "credential": azure_cred,
        },
        "fabric": {
            "required": required,
            "mandatory": mandatory,
            "tenant": "tenant",
            "workspace_id": "ws-123",
            "credential": azure_cred,
        },
    }


def _questionnaire(mode="like_for_like", access=None, additional=None, whole_repo=True, artifacts=None):
    q = {
        "migration": {
            "source_tool": "azure_data_factory",
            "source_warehouse": "sql_server",
            "target_platform": "microsoft_fabric",
            "mode": mode,
            "objective": "Migrate the ADF + SQL Server estate to Fabric as-is.",
        },
        "requested_scope": {"whole_repository": whole_repo, "artifacts": artifacts or []},
        "success_criteria": ["All in-scope artifacts run in Fabric with parity"],
        "constraints": ["Data residency: EU only"],
        "exclusions": [],
        "access": access if access is not None else _access_block(),
    }
    if additional is not None:
        q["additional_requirements"] = additional
    return q


def _write(tmp_path, data, name="consultation-input.yaml"):
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def _run(store, path, connectors=None, name="Demo"):
    agent = ConsultationAgent(store, connectors=connectors)
    return agent.run(ConsultationRequest(project_name=name, input_path=path))


# --- happy path -------------------------------------------------------------

def test_ready_when_all_validated(store, project, tmp_path):
    path = _write(tmp_path, _questionnaire())
    result = _run(store, path, connectors=all_validated_connectors())

    assert result.success is True
    c = result.consultation
    assert c.readiness.status.value == "ready"
    assert c.readiness.blockers == []
    assert c.capability_fit.currently_supported is True
    assert c.source_commit_sha == "a" * 40
    # All five domains present and validated.
    assert {r.domain for r in c.access} == set(AccessDomain)
    assert all(r.validated for r in c.access)
    # Persisted.
    assert store.has_consultation("demo")


def test_named_subset_scope_recorded_verbatim(store, project, tmp_path):
    q = _questionnaire(whole_repo=False, artifacts=[{"name": "Aple", "type": "pipeline"}])
    path = _write(tmp_path, q)
    result = _run(store, path, connectors=all_validated_connectors())

    assert result.success is True
    scope = result.consultation.requested_scope
    assert scope.whole_repository is False
    assert scope.artifacts[0].name == "Aple"
    assert scope.artifacts[0].type == "pipeline"


# --- capability boundary ----------------------------------------------------

def test_unsupported_mode_blocks_and_is_flagged(store, project, tmp_path):
    path = _write(tmp_path, _questionnaire(mode="redesign"))
    result = _run(store, path, connectors=all_validated_connectors())

    assert result.success is True
    c = result.consultation
    assert c.capability_fit.currently_supported is False
    assert c.capability_fit.mode_supported is False
    # Recorded, not reinterpreted as like_for_like.
    assert c.migration.mode.value == "redesign"
    # Core-mode-unsupported is a blocker.
    assert c.readiness.status.value == "blocked"
    assert any(b.requirement == "capability" for b in c.readiness.blockers)


def test_unsupported_additional_requirement_is_warning(store, project, tmp_path):
    q = _questionnaire(
        additional=[{"description": "Redesign orchestration in Fabric", "kind": "redesign"}]
    )
    path = _write(tmp_path, q)
    result = _run(store, path, connectors=all_validated_connectors())

    c = result.consultation
    assert c.capability_fit.currently_supported is False
    assert c.capability_fit.unsupported_requirements[0].kind == "redesign"
    # Additional requirement does not block the like-for-like engagement.
    assert c.readiness.status.value == "ready"
    assert any("redesign" in w.lower() for w in c.readiness.warnings)


# --- readiness / blockers ---------------------------------------------------

def test_mandatory_failure_blocks(store, project, tmp_path):
    connectors = all_validated_connectors()
    connectors[AccessDomain.adls_gen2.value] = FakeConnector(
        AccessDomain.adls_gen2, ConnectivityStatus.failed
    )
    path = _write(tmp_path, _questionnaire())
    result = _run(store, path, connectors=connectors)

    c = result.consultation
    assert c.readiness.status.value == "blocked"
    assert any(b.requirement == "adls_gen2" for b in c.readiness.blockers)


def test_optional_failure_does_not_block(store, project, tmp_path):
    access = _access_block()
    access["adls_gen2"]["mandatory"] = False  # optional
    connectors = all_validated_connectors()
    connectors[AccessDomain.adls_gen2.value] = FakeConnector(
        AccessDomain.adls_gen2, ConnectivityStatus.failed
    )
    path = _write(tmp_path, _questionnaire(access=access))
    result = _run(store, path, connectors=connectors)

    c = result.consultation
    assert c.readiness.status.value == "ready"
    assert any("adls_gen2" in w for w in c.readiness.warnings)


def test_not_required_domain_is_skipped(store, project, tmp_path):
    access = _access_block()
    access["adf"]["required"] = False
    connectors = all_validated_connectors()
    connectors[AccessDomain.adf.value] = FakeConnector(
        AccessDomain.adf, ConnectivityStatus.validated
    )
    path = _write(tmp_path, _questionnaire(access=access))
    result = _run(store, path, connectors=connectors)

    c = result.consultation
    adf_req = next(r for r in c.access if r.domain == AccessDomain.adf)
    assert adf_req.required is False
    assert adf_req.connectivity.status == ConnectivityStatus.not_required
    # The connector for a not-required domain is never called.
    assert connectors[AccessDomain.adf.value].calls == 0
    assert c.readiness.status.value == "ready"


def test_readiness_is_deterministic(store, project, tmp_path):
    path = _write(tmp_path, _questionnaire())
    a = _run(store, path, connectors=all_validated_connectors()).consultation
    b = _run(store, path, connectors=all_validated_connectors()).consultation
    assert a.readiness.status == b.readiness.status
    assert [x.domain for x in a.access] == [x.domain for x in b.access]


# --- questionnaire validation ----------------------------------------------

def test_missing_required_field_fails(store, project, tmp_path):
    q = _questionnaire()
    del q["success_criteria"]
    path = _write(tmp_path, q)
    result = _run(store, path, connectors=all_validated_connectors())
    assert result.success is False
    assert "success_criteria" in result.error
    assert not store.has_consultation("demo")


def test_unknown_key_rejected(store, project, tmp_path):
    q = _questionnaire()
    q["migration"]["surprise"] = "nope"
    path = _write(tmp_path, q)
    result = _run(store, path, connectors=all_validated_connectors())
    assert result.success is False
    assert not store.has_consultation("demo")


def test_invalid_platform_rejected(store, project, tmp_path):
    q = _questionnaire()
    q["migration"]["target_platform"] = "snowflake"
    path = _write(tmp_path, q)
    result = _run(store, path, connectors=all_validated_connectors())
    assert result.success is False


def test_missing_project_fails(store, tmp_path):
    path = _write(tmp_path, _questionnaire())
    result = _run(store, path, connectors=all_validated_connectors(), name="ghost")
    assert result.success is False
    assert "intake" in result.error.lower()


def test_missing_input_file_fails(store, project):
    result = _run(store, "does-not-exist.yaml", connectors=all_validated_connectors())
    assert result.success is False
    assert "not found" in result.error.lower()


# --- reproducibility / timestamp semantics ----------------------------------

def test_reproducible_no_op_rerun_preserves_timestamps(store, project, tmp_path):
    path = _write(tmp_path, _questionnaire())
    first = _run(store, path, connectors=all_validated_connectors()).consultation
    second = _run(store, path, connectors=all_validated_connectors()).consultation
    assert second.created_at == first.created_at
    assert second.updated_at == first.updated_at


def test_content_change_bumps_updated_at(store, project, tmp_path):
    path = _write(tmp_path, _questionnaire())
    first = _run(store, path, connectors=all_validated_connectors()).consultation

    changed = _questionnaire()
    changed["constraints"] = ["Different constraint"]
    path2 = _write(tmp_path, changed, name="changed.yaml")
    second = _run(store, path2, connectors=all_validated_connectors()).consultation

    assert second.created_at == first.created_at
    assert second.updated_at >= first.updated_at
    assert second.updated_at != first.updated_at


# --- security ---------------------------------------------------------------

def test_no_secret_values_persisted(store, project, tmp_path, monkeypatch):
    # Set real secret VALUES in the environment. Even though fakes don't read
    # them, the persisted record must contain none of them.
    monkeypatch.setenv("AZ_CLIENT_SECRET", "super-secret-value-xyz")
    monkeypatch.setenv("SQL_PASSWORD", "p@ssw0rd-do-not-store")
    path = _write(tmp_path, _questionnaire())
    result = _run(store, path, connectors=all_validated_connectors())

    text = store.consultation_path("demo").read_text(encoding="utf-8")
    for secret in ("super-secret-value-xyz", "p@ssw0rd-do-not-store"):
        assert secret not in text
        assert secret not in result.consultation.model_dump_json()

    # Credential *references* (env-var names) are recorded so Discovery knows
    # where to resolve them — that is metadata, not a secret.
    assert "AZ_CLIENT_SECRET" in text
    assert "password_env" in text


# --- boundary: no source/ access, no discovery ------------------------------

def test_runs_without_source_dir(store, project, tmp_path):
    # No source/ directory exists for this project; Consultation must not need it.
    assert not store.source_dir("demo").exists()
    path = _write(tmp_path, _questionnaire())
    result = _run(store, path, connectors=all_validated_connectors())
    assert result.success is True


def test_no_discovery_artifacts_and_no_source_reads(store, project, tmp_path):
    path = _write(tmp_path, _questionnaire())
    _run(store, path, connectors=all_validated_connectors())
    project_dir = store.project_dir("demo")
    entries = {p.name for p in project_dir.iterdir()}
    # Only intake + consultation records; no discovery inventory/artifacts.
    assert entries == {"project.yaml", "consultation.yaml"}

    data = yaml.safe_load(store.consultation_path("demo").read_text(encoding="utf-8"))
    # No source-estate inventory keys leak into the consultation record.
    for forbidden in ("inventory", "pipelines", "datasets", "tables", "schemas", "files"):
        assert forbidden not in data


def test_consultation_agent_does_not_import_source_readers():
    """Static guard: the consultation agent must not pull in git / source readers."""
    import inspect

    import adf2fabric.agent.consultation.agent as agent_mod

    src = inspect.getsource(agent_mod)
    assert "repository.git" not in src
    assert "source_dir" not in src
