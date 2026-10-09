"""Tests for the deterministic repository layer: git wrappers, provider URL
handling, token redaction, and provider selection. All offline."""

from __future__ import annotations

import pytest

from adf2fabric.repository import authentication, git
from adf2fabric.repository.providers import (
    GitHubProvider,
    InvalidRepositoryUrlError,
    UnsupportedProviderError,
    get_provider,
)
from tests.conftest import repo_head


# --- git wrappers -----------------------------------------------------------

def test_ls_remote_finds_branch(source_repo):
    assert git.ls_remote(str(source_repo), "main") is True


def test_ls_remote_missing_branch_false(source_repo):
    assert git.ls_remote(str(source_repo), "does-not-exist") is False


def test_clone_and_head_sha(source_repo, tmp_path):
    dest = tmp_path / "clone"
    git.clone(str(source_repo), dest, "main")
    sha = git.head_sha(dest)
    assert len(sha) == 40
    assert sha == repo_head(source_repo)
    assert git.current_branch(dest) == "main"


def test_scrub_remote_removes_token(source_repo, tmp_path):
    dest = tmp_path / "clone"
    git.clone(str(source_repo), dest, "main")

    # Simulate a credential-bearing remote, then scrub it.
    secret_url = "https://x-access-token:SUPERSECRET@github.com/o/r.git"
    clean_url = "https://github.com/o/r.git"
    git.set_remote_url(dest, secret_url)

    config_text = (dest / ".git" / "config").read_text(encoding="utf-8")
    assert "SUPERSECRET" in config_text  # precondition: token is present

    git.set_remote_url(dest, clean_url)
    config_text = (dest / ".git" / "config").read_text(encoding="utf-8")
    assert "SUPERSECRET" not in config_text
    assert git.get_remote_url(dest) == clean_url


def test_is_git_repo(source_repo, tmp_path):
    assert git.is_git_repo(source_repo) is True
    assert git.is_git_repo(tmp_path / "nope") is False


# --- provider ---------------------------------------------------------------

def test_github_provider_normalizes_urls():
    p = GitHubProvider()
    expected = "https://github.com/octocat/Hello-World.git"
    assert p.normalize_url("https://github.com/octocat/Hello-World") == expected
    assert p.normalize_url("github.com/octocat/Hello-World.git") == expected
    assert p.normalize_url("https://github.com/octocat/Hello-World/") == expected


def test_github_provider_authenticated_url_embeds_token():
    p = GitHubProvider()
    url = p.authenticated_url("https://github.com/octocat/Hello-World", "TOK123")
    assert url == "https://x-access-token:TOK123@github.com/octocat/Hello-World.git"


def test_github_provider_rejects_bad_url():
    with pytest.raises(InvalidRepositoryUrlError):
        GitHubProvider().normalize_url("https://example.com/not/github")


def test_get_provider_github():
    assert get_provider("github").name == "github"
    assert get_provider("GitHub").name == "github"  # case-insensitive


def test_get_provider_future_raises():
    for future in ("azure-devops", "gitlab", "bitbucket"):
        with pytest.raises(UnsupportedProviderError):
            get_provider(future)


def test_get_provider_unknown_raises():
    with pytest.raises(UnsupportedProviderError):
        get_provider("sourceforge")


# --- redaction --------------------------------------------------------------

def test_redact_hides_token():
    text = "fatal: could not read from https://x-access-token:abc123@github.com/o/r"
    assert "abc123" not in authentication.redact(text, "abc123")
    assert "***" in authentication.redact(text, "abc123")


def test_redact_ignores_empty_secret():
    assert authentication.redact("unchanged", "") == "unchanged"
