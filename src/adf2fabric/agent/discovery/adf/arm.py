"""ADF factory artifact enumeration over the Azure Resource Manager REST API.

Phase 1 issues exactly one read-only child-collection GET per artifact kind
(pipelines / datasets / linkedservices / triggers / dataflows /
integrationruntimes), follows ``nextLink`` pagination to completion, and extracts
each artifact's ``name`` ONLY. ARM list responses return each artifact's full
``properties`` (and linked-service properties can carry secret material); this
module reads the response but keeps nothing beyond the name + kind. It performs
NO write (POST/PUT/PATCH/DELETE), no run/activation, and no non-GET request.

The HTTP transport is built behind a seam (``default_transport``) so tests inject
a fake and prove exactly the six GETs are made and no write verb is ever used.
The ARM scope + api-version are reused from the connectivity connector — never
re-declared here.
"""

from __future__ import annotations

from typing import Any

from adf2fabric import cloud
from adf2fabric.connectivity.azure_data_factory import API_VERSION, ARM_SCOPE
from adf2fabric.project.discovery import AdfArtifact, AdfArtifactKind

__all__ = [
    "ARM_SCOPE",
    "API_VERSION",
    "ARTIFACT_COLLECTIONS",
    "COLLECTION_LABELS",
    "ArmHttpError",
    "default_transport",
    "factory_base_url",
    "enumerate_artifacts",
]

# The fixed, ordered set of factory collections enumerated in Phase 1. Each entry
# is (artifact kind -> ARM URL segment). Deterministic order == report order.
ARTIFACT_COLLECTIONS: list[tuple[AdfArtifactKind, str]] = [
    (AdfArtifactKind.pipeline, "pipelines"),
    (AdfArtifactKind.dataset, "datasets"),
    (AdfArtifactKind.linked_service, "linkedservices"),
    (AdfArtifactKind.trigger, "triggers"),
    (AdfArtifactKind.dataflow, "dataflows"),
    (AdfArtifactKind.integration_runtime, "integrationruntimes"),
]

# ARM collection labels recorded as the queries performed (secret-free).
COLLECTION_LABELS: list[str] = [segment for _kind, segment in ARTIFACT_COLLECTIONS]

# Defensive cap on pagination follow-through (far above any real factory).
_MAX_PAGES = 10_000


class ArmHttpError(Exception):
    """A non-200 ARM response. Carries the status code for deterministic mapping."""

    def __init__(self, status_code: int, url: str):
        self.status_code = status_code
        # Store only the path portion; never echo query strings that could carry
        # anything sensitive. (Factory URLs carry no secret, but stay conservative.)
        self.url = url.split("?", 1)[0]
        super().__init__(f"ARM GET returned HTTP {status_code} for {self.url}")


def default_transport() -> Any:
    """Return the HTTP transport (lazy import seam).

    Raises ``ImportError`` if ``requests`` is not installed; the caller maps that
    to a ``dependency_unavailable`` outcome.
    """
    import requests

    return requests


def factory_base_url(subscription_id: str, resource_group: str, factory_name: str) -> str:
    """The ARM resource id (URL) of the factory. Child collections hang off this."""
    return (
        f"{cloud.ARM_ENDPOINT}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
    )


def enumerate_artifacts(
    transport: Any, base_url: str, token: str, *, timeout: int = 15
) -> dict[AdfArtifactKind, list[AdfArtifact]]:
    """Enumerate all six factory collections, returning name+kind artifacts only.

    One paginated GET per kind. Each artifact list is sorted by name. Raises
    :class:`ArmHttpError` on any non-200 response (the caller normalizes it);
    transport/network exceptions propagate unchanged.
    """
    headers = {"Authorization": f"Bearer {token}"}
    result: dict[AdfArtifactKind, list[AdfArtifact]] = {}
    for kind, segment in ARTIFACT_COLLECTIONS:
        url = f"{base_url}/{segment}?api-version={API_VERSION}"
        names = _collect_names(transport, url, headers, timeout)
        result[kind] = [AdfArtifact(name=n, kind=kind) for n in sorted(names)]
    return result


def _collect_names(transport: Any, url: str, headers: dict, timeout: int) -> list[str]:
    """GET one collection, following ``nextLink``; return artifact names only."""
    names: list[str] = []
    seen: set[str] = set()
    next_url: str | None = url
    pages = 0
    while next_url and pages < _MAX_PAGES:
        if next_url in seen:  # defensive: never loop on a repeated link
            break
        seen.add(next_url)
        pages += 1
        resp = transport.get(next_url, headers=headers, timeout=timeout)
        status = getattr(resp, "status_code", None)
        if status != 200:
            raise ArmHttpError(status, next_url)
        payload = resp.json() or {}
        for item in payload.get("value", []) or []:
            name = item.get("name")
            if name:
                names.append(name)  # name ONLY — properties are discarded
        next_url = payload.get("nextLink")
    return names
