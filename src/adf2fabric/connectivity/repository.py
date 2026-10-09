"""Repository connectivity check.

Reuses the existing repository layer (provider + git ls-remote + token from the
environment) to confirm the source repository recorded by Intake is reachable
on the requested branch. It does NOT read repository contents.
"""

from __future__ import annotations

import os

from adf2fabric.connectivity.base import outcome
from adf2fabric.project.models import (
    AccessDomain,
    ConnectivityOutcome,
    ConnectivityStatus,
    Project,
    RepositoryAccess,
)
from adf2fabric.repository import authentication
from adf2fabric.repository.client import RepositoryAccessError, RepositoryClient
from adf2fabric.repository.providers import get_provider


class RepositoryConnector:
    domain = AccessDomain.repository.value

    def validate(self, access: RepositoryAccess, project: Project) -> ConnectivityOutcome:
        meta = project.repository
        token = os.environ.get(access.token_env)
        if not token:
            return outcome(
                AccessDomain.repository,
                ConnectivityStatus.missing_credential,
                detail=(
                    f"No repository token found. Set the {access.token_env} "
                    "environment variable."
                ),
            )

        try:
            provider = get_provider(meta.provider)
            client = RepositoryClient(provider, token)
            client.validate(meta.repository_url, meta.branch)
        except RepositoryAccessError as exc:
            return outcome(
                AccessDomain.repository,
                ConnectivityStatus.failed,
                detail=authentication.redact(str(exc), token),
            )
        except Exception as exc:  # noqa: BLE001 - surface a clean, redacted failure
            return outcome(
                AccessDomain.repository,
                ConnectivityStatus.failed,
                detail=authentication.redact(str(exc), token),
            )

        return outcome(
            AccessDomain.repository,
            ConnectivityStatus.validated,
            reachable=True,
            authenticated=True,
            detail=f"Repository reachable on branch '{meta.branch}'.",
        )
