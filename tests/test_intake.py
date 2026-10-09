"""End-to-end tests for the Intake Agent, fully offline.

A local-path provider stands in for GitHub so intake can clone from a real git
repo on disk without any network access. The GitHub-specific URL/token logic is
covered separately in ``test_repository.py``.
"""

from __future__ import annotations

import pytest
import yaml

import adf2fabric.repository.git as git_mod
from adf2fabric.agent.intake import IntakeAgent, IntakeRequest
from adf2fabric.agent.intake import agent as agent_mod
from adf2fabric.project.store import ProjectStore
from adf2fabric.repository.providers import RepositoryProvider
from tests.conftest import repo_head

TOKEN = "ghp_TESTTOKEN123"


class LocalPathProvider(RepositoryProvider):
    """Clones from a local filesystem path; needs no real authentication."""

    name = "local"

    def normalize_url(self, url: str) -> str:
        return url

    def authenticated_url(self, url: str, token: str) -> str:
        return url


@pytest.fixture(autouse=True)
def use_local_provider(monkeypatch):
    monkeypatch.setattr(agent_mod, "get_provider", lambda name: LocalPathProvider())


@pytest.fixture
def token_env(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", TOKEN)


@pytest.fixture
def store(tmp_path):
    return ProjectStore(tmp_path / "projects")


def _run(store, repo_path, name="demo", branch="main", refresh=False):
    agent = IntakeAgent(store)
    return agent.run(
        IntakeRequest(
            project_name=name,
            repository_url=str(repo_path),
            branch=branch,
            provider="local",
            refresh=refresh,
        )
    )


# --- happy path -------------------------------------------------------------

def test_intake_success_records_metadata(store, source_repo, token_env):
    result = _run(store, source_repo)

    assert result.success is True
    assert result.reused is False
    project = result.project
    assert project.id == "demo"

    meta = project.repository
    assert meta.provider == "local"
    assert meta.repository_url == str(source_repo)
    assert meta.branch == "main"
    assert meta.commit_sha == repo_head(source_repo)
    assert meta.fetched_at is not None

    # Canonical local source is projects/<id>/source and actually cloned.
    assert meta.local_source_path == str(store.source_dir("demo"))
    assert (store.source_dir("demo") / ".git").exists()
    assert (store.source_dir("demo") / "README.md").exists()

    # project.yaml persisted.
    assert store.exists("demo")


# --- reuse / refresh --------------------------------------------------------

def test_intake_reuses_existing_source(store, source_repo, token_env, monkeypatch):
    first = _run(store, source_repo)
    assert first.success and first.reused is False

    # Prove the second run neither clones nor needs a token.
    def fail_clone(*a, **k):  # pragma: no cover - must not be called
        raise AssertionError("clone should not be called on reuse")

    monkeypatch.setattr(git_mod, "clone", fail_clone)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    second = _run(store, source_repo)
    assert second.success is True
    assert second.reused is True
    assert second.project.repository.commit_sha == repo_head(source_repo)


def test_refresh_forces_refetch(store, source_repo, token_env, monkeypatch):
    assert _run(store, source_repo).success

    calls = {"n": 0}
    orig = git_mod.clone

    def counting_clone(*a, **k):
        calls["n"] += 1
        return orig(*a, **k)

    monkeypatch.setattr(git_mod, "clone", counting_clone)
    result = _run(store, source_repo, refresh=True)
    assert result.success is True
    assert result.reused is False
    assert calls["n"] == 1


def test_different_repo_not_reused(store, make_git_repo, tmp_path, token_env):
    repo_a = make_git_repo(tmp_path / "repoA", content="A")
    repo_b = make_git_repo(tmp_path / "repoB", content="B")

    ra = _run(store, repo_a, name="proj-a")
    rb = _run(store, repo_b, name="proj-b")

    # Independent source directories, each matching its own origin.
    assert ra.project.repository.commit_sha == repo_head(repo_a)
    assert rb.project.repository.commit_sha == repo_head(repo_b)
    assert store.source_dir("proj-a") != store.source_dir("proj-b")

    # Same project pointed at a different repo replaces (does not reuse).
    replaced = _run(store, repo_b, name="proj-a")
    assert replaced.success is True
    assert replaced.reused is False
    assert replaced.project.repository.commit_sha == repo_head(repo_b)


# --- failure paths ----------------------------------------------------------

def test_intake_missing_token_fails_cleanly(store, source_repo, monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    result = _run(store, source_repo)
    assert result.success is False
    assert result.project is None
    assert "token" in result.error.lower()


def test_intake_bad_branch_fails(store, source_repo, token_env):
    result = _run(store, source_repo, branch="nonexistent")
    assert result.success is False
    assert "nonexistent" in result.error
    assert not store.exists("demo")


# --- security ---------------------------------------------------------------

def test_token_absent_from_git_config(store, source_repo, token_env):
    _run(store, source_repo)
    config_text = (store.source_dir("demo") / ".git" / "config").read_text(encoding="utf-8")
    assert TOKEN not in config_text


def test_secrets_never_persisted(store, source_repo, token_env):
    result = _run(store, source_repo)
    # Not in the serialized project record.
    project_yaml = (store.project_dir("demo") / "project.yaml").read_text(encoding="utf-8")
    assert TOKEN not in project_yaml
    # Not in the result object.
    assert TOKEN not in result.model_dump_json()


# --- scope guard ------------------------------------------------------------

def test_no_discovery_artifacts_produced(store, source_repo, token_env):
    _run(store, source_repo)
    project_dir = store.project_dir("demo")
    # Only the project record and the source/ copy exist - no inventory,
    # no artifacts, no classification output.
    entries = {p.name for p in project_dir.iterdir()}
    assert entries == {"project.yaml", "source"}

    # And project.yaml records only intake-level fields.
    data = yaml.safe_load((project_dir / "project.yaml").read_text(encoding="utf-8"))
    assert set(data) == {"id", "name", "created_at", "repository"}
