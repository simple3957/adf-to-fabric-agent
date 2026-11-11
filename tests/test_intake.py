"""Tests for the Intake Agent.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import pytest

from adf2fabric.project.store import ProjectStore

TOKEN = "ghp_TESTTOKEN123"


@pytest.fixture
def token_env(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", TOKEN)


@pytest.fixture
def store(tmp_path):
    return ProjectStore(tmp_path / "projects")


# --- happy path -------------------------------------------------------------


# --- reuse / refresh --------------------------------------------------------


# --- failure paths ----------------------------------------------------------


def test_github_urls_carry_a_token_only_when_there_is_one():
    from adf2fabric.repository.providers import GitHubProvider

    p = GitHubProvider()
    assert p.authenticated_url("https://github.com/o/r", None) == "https://github.com/o/r.git"
    assert p.authenticated_url("https://github.com/o/r", "t") == "https://x-access-token:t@github.com/o/r.git"


# --- security ---------------------------------------------------------------


# --- scope guard ------------------------------------------------------------

