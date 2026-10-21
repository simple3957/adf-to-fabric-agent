"""Thin, deterministic wrappers around the ``git`` command-line tool.

No network library and no third-party git binding - just subprocess calls to
the installed ``git``. Each function accepts an already-authenticated URL when
one is needed; callers are responsible for redacting secrets from any error
they surface (see ``authentication.redact``).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


class GitError(Exception):
    """A git command failed. The message is caller-redacted before surfacing."""


class GitNotAvailableError(Exception):
    """The ``git`` executable was not found on PATH."""


def _git_path() -> str:
    git = shutil.which("git")
    if git is None:
        raise GitNotAvailableError(
            "The 'git' executable was not found on PATH. Install Git and ensure "
            "it is available to run the Intake Agent."
        )
    return git


def _run(args: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess:
    """Run a git command, returning the completed process.

    Raises :class:`GitError` on non-zero exit. The raw (possibly secret-bearing)
    stderr is attached so the caller can redact and re-raise.
    """
    # Never wait for a credential prompt (terminal or Git Credential Manager window): without a token a private
    # repository must fail fast with a clear message, not hang the intake.
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GCM_INTERACTIVE": "never"}
    proc = subprocess.run(
        [_git_path(), *args],
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.returncode != 0:
        raise GitError(proc.stderr.strip() or proc.stdout.strip() or "git command failed")
    return proc


def ls_remote(url: str, branch: str) -> bool:
    """Return True if ``branch`` exists on the remote at ``url``.

    Validates accessibility + authentication + branch existence in a single
    deterministic step without cloning. ``url`` may carry credentials.
    """
    proc = _run(["ls-remote", "--heads", url, branch])
    return bool(proc.stdout.strip())


def default_branch(url: str) -> str | None:
    """Return the branch the remote's ``HEAD`` points at (its default branch), or None if it cannot be told.

    ``url`` may carry credentials. A detached or empty remote has no symbolic ``HEAD``, so the answer is None.
    """
    proc = _run(["ls-remote", "--symref", url, "HEAD"])
    for line in proc.stdout.splitlines():
        if line.startswith("ref:"):
            ref = line[len("ref:"):].split("\t", 1)[0].strip()
            if ref.startswith("refs/heads/"):
                return ref[len("refs/heads/"):]
    return None


def clone(url: str, dest: Path, branch: str) -> None:
    """Clone ``branch`` from ``url`` into ``dest`` (single-branch, shallowish).

    ``url`` may carry credentials; scrub them afterwards with :func:`set_remote_url`.
    """
    _run(["clone", "--branch", branch, "--single-branch", url, str(dest)])


def head_sha(repo_dir: Path) -> str:
    """Return the full HEAD commit SHA of the repository at ``repo_dir``."""
    return _run(["-C", str(repo_dir), "rev-parse", "HEAD"]).stdout.strip()


def current_branch(repo_dir: Path) -> str:
    """Return the checked-out branch name of the repository at ``repo_dir``."""
    return _run(
        ["-C", str(repo_dir), "rev-parse", "--abbrev-ref", "HEAD"]
    ).stdout.strip()


def get_remote_url(repo_dir: Path, remote: str = "origin") -> str:
    """Return the configured URL of ``remote`` (token-free after scrubbing)."""
    return _run(["-C", str(repo_dir), "remote", "get-url", remote]).stdout.strip()


def set_remote_url(repo_dir: Path, url: str, remote: str = "origin") -> None:
    """Reset ``remote`` to ``url`` so no token remains in .git/config."""
    _run(["-C", str(repo_dir), "remote", "set-url", remote, url])


def is_git_repo(path: Path) -> bool:
    """True if ``path`` looks like the working tree of a git repository."""
    return (Path(path) / ".git").exists()


def init_repo(path: Path, branch: str) -> None:
    """Initialize a new repository at ``path`` on ``branch``."""
    Path(path).mkdir(parents=True, exist_ok=True)
    _run(["init", "-b", branch, str(path)])


def commit_all(repo_dir: Path, message: str) -> str:
    """Stage everything in ``repo_dir`` and commit it; return the commit SHA.

    Identity is passed per-command so the result never depends on (or modifies)
    the user's git configuration. Used to give a locally-built source (e.g. an
    Azure export) the same verifiable commit identity a cloned source has.
    """
    _run(["-C", str(repo_dir), "add", "-A"])
    _run(
        [
            "-C", str(repo_dir),
            "-c", "user.name=adf2fabric",
            "-c", "user.email=adf2fabric@localhost",
            "-c", "commit.gpgsign=false",
            "commit", "--allow-empty", "-m", message,
        ]
    )
    return head_sha(repo_dir)
