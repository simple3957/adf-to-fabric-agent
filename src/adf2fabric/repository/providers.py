"""Repository provider abstraction.

Only GitHub is implemented right now. The abstraction exists so future
providers can be added without redesigning Intake - it is deliberately NOT
over-built.

    RepositoryProvider
        |
        +-- GitHubProvider       <- IMPLEMENTED
        |
        +-- AzureDevOpsProvider  <- FUTURE (not implemented)
        +-- GitLabProvider       <- FUTURE (not implemented)
        +-- BitbucketProvider    <- FUTURE (not implemented)
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod


class UnsupportedProviderError(Exception):
    """Raised when a provider is requested that is not implemented yet."""


class InvalidRepositoryUrlError(Exception):
    """Raised when a repository URL is not valid for the selected provider."""


class RepositoryProvider(ABC):
    """A source-control provider Intake can authenticate with and fetch from."""

    name: str

    @abstractmethod
    def normalize_url(self, url: str) -> str:
        """Return a canonical, token-free HTTPS clone URL (ending in .git)."""

    @abstractmethod
    def authenticated_url(self, url: str, token: str) -> str:
        """Return a clone URL with the token embedded for the git operation."""


class GitHubProvider(RepositoryProvider):
    name = "github"

    # Accepts https://github.com/owner/repo(.git) and github.com/owner/repo forms.
    _PATTERN = re.compile(
        r"^(?:https?://)?(?:www\.)?github\.com/"
        r"(?P<owner>[^/\s]+)/(?P<repo>[^/\s]+?)(?:\.git)?/?$",
        re.IGNORECASE,
    )

    def parse(self, url: str) -> tuple[str, str]:
        match = self._PATTERN.match(url.strip())
        if not match:
            raise InvalidRepositoryUrlError(
                f"'{url}' is not a valid GitHub repository URL. "
                "Expected https://github.com/<owner>/<repo>."
            )
        return match.group("owner"), match.group("repo")

    def normalize_url(self, url: str) -> str:
        owner, repo = self.parse(url)
        return f"https://github.com/{owner}/{repo}.git"

    def authenticated_url(self, url: str, token: str) -> str:
        owner, repo = self.parse(url)
        # x-access-token is GitHub's documented username for token auth over HTTPS.
        return f"https://x-access-token:{token}@github.com/{owner}/{repo}.git"


# Providers that are part of the product vision but not implemented yet. Listing
# them here documents intent without creating dead code paths.
_FUTURE_PROVIDERS = {"azure-devops", "gitlab", "bitbucket"}

_IMPLEMENTED: dict[str, type[RepositoryProvider]] = {
    "github": GitHubProvider,
}


def get_provider(name: str) -> RepositoryProvider:
    """Return an instance of the requested provider.

    Raises :class:`UnsupportedProviderError` for known-but-future providers and
    for anything unrecognized.
    """
    key = name.strip().lower()
    if key in _IMPLEMENTED:
        return _IMPLEMENTED[key]()
    if key in _FUTURE_PROVIDERS:
        raise UnsupportedProviderError(
            f"Provider '{name}' is part of the future roadmap but is not "
            "implemented yet. Only 'github' is supported currently."
        )
    raise UnsupportedProviderError(
        f"Unknown provider '{name}'. Supported: {sorted(_IMPLEMENTED)}."
    )
