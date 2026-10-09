
"""The Intake Agent.

Responsibility (and hard boundary):

    accept request -> pick provider -> authenticate -> validate -> fetch/reuse
    -> record metadata -> STOP

INTAKE DOES NOT INSPECT THE CONTENTS OF THE SOURCE REPOSITORY. The fetched repo
is an opaque artifact. Understanding what is inside it is Discovery's job, a
later stage that consumes the canonical local source produced here.

The agent is idempotent with respect to an already-fetched source: if the
canonical local copy already points at the requested repo + branch it is reused
rather than re-cloned, so future stages never need to re-authenticate.
"""

from __future__ import annotations

import os
import shutil
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

from adf2fabric.agent.intake.models import IntakeRequest, IntakeResult
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.store import ProjectStore, slugify
from adf2fabric.repository import authentication, git
from adf2fabric.repository.client import RepositoryClient
from adf2fabric.repository.providers import get_provider


def _remove_tree(path: Path) -> None:
    """Recursively delete ``path``, clearing read-only bits.

    Git marks objects read-only, which trips shutil.rmtree on Windows; the
    handler restores write permission and retries the failed removal.
    """

    def on_error(func, p, _exc):
        os.chmod(p, stat.S_IWRITE)
        func(p)

    if sys.version_info >= (3, 12):
        shutil.rmtree(path, onexc=on_error)
    else:  # pragma: no cover - Python 3.11 fallback
        shutil.rmtree(path, onerror=lambda f, p, _e: on_error(f, p, None))


class IntakeAgent:
    def __init__(self, store: ProjectStore, token_env: str = authentication.DEFAULT_TOKEN_ENV):
        self.store = store
        self.token_env = token_env

    def run(self, request: IntakeRequest) -> IntakeResult:
        token: str = ""
        try:
            project_id = slugify(request.project_name)
            provider = get_provider(request.provider)
            clean_url = provider.normalize_url(request.repository_url)
            dest = self.store.source_dir(project_id)

            # --- Idempotent reuse: does a matching local source already exist? ---
            if git.is_git_repo(dest) and not request.refresh:
                if self._matches(dest, clean_url, request.branch):
                    metadata = RepositoryMetadata(
                        provider=provider.name,
                        repository_url=clean_url,
                        branch=request.branch,
                        commit_sha=git.head_sha(dest),
                        fetched_at=datetime.now(timezone.utc),
                        local_source_path=str(dest),
                    )
                    project = self._persist(project_id, request.project_name, metadata)
                    return IntakeResult(
                        success=True,
                        project=project,
                        reused=True,
                        message=(
                            f"Reused existing local source for '{project_id}' "
                            f"({request.branch} @ {metadata.commit_sha[:8]})."
                        ),
                    )
                # Different repo/branch already occupies this project's source dir.
                _remove_tree(dest)
            elif request.refresh and dest.exists():
                # Explicit refresh: replace whatever is there with a fresh clone.
                _remove_tree(dest)

            # --- Fresh fetch (requires authentication) ---
            token = authentication.resolve_token(self.token_env)
            client = RepositoryClient(provider, token)
            metadata = client.fetch(request.repository_url, request.branch, dest)
            project = self._persist(project_id, request.project_name, metadata)
            return IntakeResult(
                success=True,
                project=project,
                reused=False,
                message=(
                    f"Fetched '{clean_url}' ({request.branch} @ "
                    f"{metadata.commit_sha[:8]}) into {dest}."
                ),
            )
        except Exception as exc:  # noqa: BLE001 - surface a clean, redacted failure
            message = authentication.redact(str(exc), token) if token else str(exc)
            return IntakeResult(success=False, error=message, message="Intake failed.")

    def _matches(self, dest, clean_url: str, branch: str) -> bool:
        """True if the existing local source is the requested repo + branch."""
        try:
            return (
                git.get_remote_url(dest) == clean_url
                and git.current_branch(dest) == branch
            )
        except git.GitError:
            return False

    def _persist(self, project_id: str, name: str, metadata: RepositoryMetadata) -> Project:
        project = Project(id=project_id, name=name, repository=metadata)
        self.store.save(project)
        return project
