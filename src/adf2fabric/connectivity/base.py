"""Connectivity abstraction for the Consultation readiness gate.

A ``Connector`` performs a *genuine but narrow* auth/reachability/permission
check for one access domain and returns a secret-free ``ConnectivityOutcome``.

Hard boundary — connectivity validation is NOT Discovery. A connector may only
answer "can we authenticate / reach / access the designated resource?". It must
never enumerate, read, inspect, profile, or otherwise understand the source
estate (pipelines, datasets, linked services, triggers, SQL schemas/tables,
ADLS containers/directories/files, Fabric artifacts). That belongs to Discovery
and later phases.

The contract types (``ConnectivityStatus``, ``ConnectivityOutcome``,
``AccessDomain``) live in :mod:`adf2fabric.project.models` so they can be
persisted without creating an import cycle; they are re-exported here for
convenience.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from adf2fabric.project.models import (
    AccessDomain,
    ConnectivityOutcome,
    ConnectivityStatus,
    Project,
)

__all__ = [
    "AccessDomain",
    "ConnectivityOutcome",
    "ConnectivityStatus",
    "Connector",
    "outcome",
]


@runtime_checkable
class Connector(Protocol):
    """Validates connectivity for a single access domain.

    ``access`` is the domain's typed access-config model (e.g. ``AdfAccess``).
    ``project`` is the loaded :class:`Project` (used by the repository
    connector; ignored by the others). Implementations must not raise for
    expected failures — they return a ``failed``/``missing_*`` outcome instead —
    and must never perform discovery.
    """

    domain: str

    def validate(self, access: Any, project: Project) -> ConnectivityOutcome: ...


def outcome(
    domain: AccessDomain,
    status: ConnectivityStatus,
    *,
    reachable: bool = False,
    authenticated: bool = False,
    detail: str = "",
    stamped: bool = True,
) -> ConnectivityOutcome:
    """Build a ``ConnectivityOutcome``, stamping ``validated_at`` by default."""
    return ConnectivityOutcome(
        domain=domain,
        status=status,
        reachable=reachable,
        authenticated=authenticated,
        detail=detail,
        validated_at=datetime.now(timezone.utc) if stamped else None,
    )
