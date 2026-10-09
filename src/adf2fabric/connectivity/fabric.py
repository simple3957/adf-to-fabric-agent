"""Microsoft Fabric connectivity check.

Acquires a Fabric API token via ``azure-identity`` and issues a single GET for
the configured workspace to confirm authentication + reachability + permission.
It does NOT create, generate, deploy, or transform any Fabric artifact — those
are future phases.
"""

from __future__ import annotations

from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential
from adf2fabric.connectivity.base import outcome
from adf2fabric.project.models import (
    AccessDomain,
    ConnectivityOutcome,
    ConnectivityStatus,
    FabricAccess,
    Project,
)
from adf2fabric.repository import authentication

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_API = "https://api.fabric.microsoft.com/v1"


class FabricConnector:
    domain = AccessDomain.fabric.value

    def validate(self, access: FabricAccess, project: Project) -> ConnectivityOutcome:
        if not access.workspace_id:
            return outcome(
                AccessDomain.fabric,
                ConnectivityStatus.missing_config,
                detail="'workspace_id' is required.",
            )

        try:
            import requests
        except ImportError:
            return outcome(
                AccessDomain.fabric,
                ConnectivityStatus.dependency_unavailable,
                detail="requests is not installed. Install the 'connectivity' extra.",
            )

        try:
            credential, secret = build_credential(access.credential)
        except CredentialUnavailable as exc:
            return outcome(AccessDomain.fabric, exc.status, detail=exc.detail)

        try:
            token = credential.get_token(FABRIC_SCOPE).token
            url = f"{FABRIC_API}/workspaces/{access.workspace_id}"
            resp = requests.get(
                url, headers={"Authorization": f"Bearer {token}"}, timeout=15
            )
        except Exception as exc:  # noqa: BLE001
            return outcome(
                AccessDomain.fabric,
                ConnectivityStatus.failed,
                detail=authentication.redact(str(exc), secret),
            )

        if resp.status_code == 200:
            return outcome(
                AccessDomain.fabric,
                ConnectivityStatus.validated,
                reachable=True,
                authenticated=True,
                detail=f"Workspace '{access.workspace_id}' reachable (HTTP 200).",
            )
        if resp.status_code in (401, 403):
            return outcome(
                AccessDomain.fabric,
                ConnectivityStatus.failed,
                reachable=True,
                authenticated=False,
                detail=f"Authenticated request denied for workspace (HTTP {resp.status_code}).",
            )
        return outcome(
            AccessDomain.fabric,
            ConnectivityStatus.failed,
            authenticated=True,
            detail=f"Workspace '{access.workspace_id}' not reachable (HTTP {resp.status_code}).",
        )
