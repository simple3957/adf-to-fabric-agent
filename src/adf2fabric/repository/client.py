"""Fetch a source repository into a local directory, safely.

The client ties together a provider, a token, and the git wrappers to perform
the actual fetch. It guarantees two things:

1. Accessibility/branch are validated before any clone (fail fast).
2. The token is scrubbed from the clone's .git/config afterwards, so the local
   copy is usable by later stages without ever storing the credential.

The client treats the repository as an opaque artifact - it clones it and
records where it landed. It never inspects the contents.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from adf2fabric.project.models import RepositoryMetadata
from adf2fabric.repository import authentication, git
from adf2fabric.repository.providers import RepositoryProvider


class RepositoryAccessError(Exception):
    """The repository could not be validated or fetched (secret-redacted)."""


class RepositoryClient:
    def __init__(self, provider: RepositoryProvider, token: str):
        self.provider = provider
        self.token = token

    def _redact(self, text: str) -> str:
        return authentication.redact(text, self.token)

    def validate(self, repository_url: str, branch: str) -> None:
        """Confirm the repo is reachable and the branch exists, or raise."""
        clean_url = self.provider.normalize_url(repository_url)
        auth_url = self.provider.authenticated_url(clean_url, self.token)
        try:
            found = git.ls_remote(auth_url, branch)
        except git.GitError as exc:
            raise RepositoryAccessError(
                f"Could not access repository '{clean_url}': {self._redact(str(exc))}"
            ) from None
        if not found:
            raise RepositoryAccessError(
                f"Branch '{branch}' was not found in repository '{clean_url}'."
            )

    def fetch(self, repository_url: str, branch: str, dest: Path) -> RepositoryMetadata:
        """Validate, clone into ``dest``, scrub the token, and return metadata.

        ``dest`` must not already exist as a populated clone; the caller
        (IntakeAgent) decides on reuse vs. fresh clone before calling this.
        """
        clean_url = self.provider.normalize_url(repository_url)
        auth_url = self.provider.authenticated_url(clean_url, self.token)

        self.validate(repository_url, branch)

        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            git.clone(auth_url, dest, branch)
            # Immediately remove the credential from the local git config so the
            # token never lives on disk.
            git.set_remote_url(dest, clean_url)
            commit_sha = git.head_sha(dest)
        except git.GitError as exc:
            raise RepositoryAccessError(
                f"Failed to fetch repository '{clean_url}': {self._redact(str(exc))}"
            ) from None

        return RepositoryMetadata(
            provider=self.provider.name,
            repository_url=clean_url,
            branch=branch,
            commit_sha=commit_sha,
            fetched_at=datetime.now(timezone.utc),
            local_source_path=str(dest),
        )
