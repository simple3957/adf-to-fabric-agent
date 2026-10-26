"""Shared test helpers.

Tests are fully offline: they build real local git repositories in ``tmp_path``
and clone from them by filesystem path, so no network or GitHub access is needed.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _full_consultation_unless_a_test_says_otherwise(monkeypatch):
    """The product defaults to the simple consultation; the existing tests cover the full one (submit, a second approver). Tests of the simple
    mode set ADF2FABRIC_CONSULT=simple themselves."""
    monkeypatch.setenv("ADF2FABRIC_CONSULT", "full")


def _git(args: list[str], cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def make_repo(
    path: Path,
    *,
    branch: str = "main",
    filename: str = "README.md",
    content: str = "hello",
    extra_branches: tuple[str, ...] = (),
) -> Path:
    """Create a real git repo at ``path`` with one commit on ``branch``."""
    path.mkdir(parents=True, exist_ok=True)
    _git(["init", "-b", branch], path)
    _git(["config", "user.email", "test@example.com"], path)
    _git(["config", "user.name", "Test"], path)
    (path / filename).write_text(content, encoding="utf-8")
    _git(["add", "-A"], path)
    _git(["commit", "-m", "initial"], path)
    for extra in extra_branches:
        _git(["branch", extra], path)
    return path


def repo_head(path: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture
def make_git_repo():
    """Return the ``make_repo`` factory for tests that need custom repos."""
    return make_repo


@pytest.fixture
def source_repo(tmp_path):
    """A ready-made local source repository on branch ``main``."""
    return make_repo(tmp_path / "origin")


@pytest.fixture(autouse=True)
def _audit_to_tmp(tmp_path_factory, monkeypatch):
    """CLI audit events from tests never touch the real projects folder."""
    monkeypatch.setenv("ADF2FABRIC_AUDIT_DIR", str(tmp_path_factory.mktemp("audit")))
