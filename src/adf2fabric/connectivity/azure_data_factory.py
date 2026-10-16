"""Azure Data Factory connectivity check.

Acquires an ARM token via the shared Azure credential abstraction and issues a
single ARM GET for the explicitly configured factory to confirm authentication +
reachability + permission:

    credential.get_token("https://management.azure.com/.default")
        -> Authorization: Bearer <token> -> requests.get(<factory ARM url>)

It does NOT enumerate pipelines, datasets, linked services, triggers, integration
runtimes, or any other ADF artifact, and issues no child-resource request and no
write — that is Discovery's job. Exactly one ARM request is made.

``requests`` is only the HTTP transport; authentication is owned entirely by the
credential abstraction. The bearer token is never logged, returned, or placed in
an outcome ``detail``. ``requests`` is imported lazily (``connectivity`` extra);
if it is unavailable the connector returns ``dependency_unavailable``.
"""

from __future__ import annotations

from typing import Any

from adf2fabric import cloud
from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential
from adf2fabric.connectivity.base import outcome
from adf2fabric.project.models import (
    AccessDomain,
    AdfAccess,
    ConnectivityOutcome,
    ConnectivityStatus,
    Project,
)
from adf2fabric.repository import authentication

# The only token scope this connector requests, and the only request it makes.
ARM_SCOPE = cloud.ARM_SCOPE


def _scoped(cred, subscription_id):
    """``cred`` with the subscription set, so an azure_cli sign-in picks the account that owns the factory."""
    if cred is not None and subscription_id and not cred.subscription_id:
        return cred.model_copy(update={"subscription_id": subscription_id})
    return cred
API_VERSION = "2018-06-01"


def _import_requests() -> Any:
    """Lazy import seam. Tests monkeypatch this to inject a fake HTTP client."""
    import requests

    return requests


def _fail(detail: str, *, reachable: bool = False, authenticated: bool = False) -> ConnectivityOutcome:
    return outcome(
        AccessDomain.adf,
        ConnectivityStatus.failed,
        reachable=reachable,
        authenticated=authenticated,
        detail=detail,
    )


class AdfConnector:
    domain = AccessDomain.adf.value

    def validate(self, access: AdfAccess, project: Project) -> ConnectivityOutcome:
        # 1. Explicit configuration — never searched, enumerated, or inferred.
        missing = [
            name
            for name, val in (
                ("subscription_id", access.subscription_id),
                ("resource_group", access.resource_group),
                ("factory_name", access.factory_name),
            )
            if not val
        ]
        if missing:
            return outcome(
                AccessDomain.adf,
                ConnectivityStatus.missing_config,
                detail=f"Missing required config: {', '.join(missing)}.",
            )

        # 2. Transport dependency.
        try:
            requests = _import_requests()
        except ImportError:
            return outcome(
                AccessDomain.adf,
                ConnectivityStatus.dependency_unavailable,
                detail="requests is not installed. Install the 'connectivity' extra.",
            )

        # 3. Credential — authentication is owned entirely by the abstraction.
        try:
            credential, secret = build_credential(_scoped(access.credential, access.subscription_id))
        except CredentialUnavailable as exc:
            return outcome(AccessDomain.adf, exc.status, detail=exc.detail)

        # 4. Acquire an ARM-scoped token (this scope only).
        try:
            token = credential.get_token(ARM_SCOPE).token
        except Exception as exc:  # noqa: BLE001
            return _fail(
                f"Authentication failed: could not acquire an Azure token. "
                f"{authentication.redact(str(exc), secret)}"
            )

        # 5. Exactly one ARM request: the factory-level GET. No child resources.
        url = (
            f"{cloud.ARM_ENDPOINT}/subscriptions/{access.subscription_id}"
            f"/resourceGroups/{access.resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{access.factory_name}"
            f"?api-version={API_VERSION}"
        )
        try:
            resp = requests.get(
                url, headers={"Authorization": f"Bearer {token}"}, timeout=15
            )
        except Exception as exc:  # noqa: BLE001
            # The token must never reach the message; redact the secret too.
            return _fail(
                "Network/connectivity failure contacting Azure Resource Manager: "
                f"{authentication.redact(str(exc), secret, token)}"
            )
        finally:
            token = None  # drop the token reference promptly

        return self._normalize(resp.status_code, access.factory_name)

    @staticmethod
    def _normalize(status_code: int, factory_name: str) -> ConnectivityOutcome:
        if status_code == 200:
            return outcome(
                AccessDomain.adf,
                ConnectivityStatus.validated,
                reachable=True,
                authenticated=True,
                detail=f"Factory '{factory_name}' reachable (HTTP 200).",
            )
        if status_code == 401:
            return _fail(
                "Authentication failed for Azure Resource Manager (HTTP 401).",
                reachable=True,
                authenticated=False,
            )
        if status_code == 403:
            return _fail(
                f"Permission/authorization failure accessing factory "
                f"'{factory_name}' (HTTP 403).",
                reachable=True,
                authenticated=True,
            )
        if status_code == 404:
            return _fail(
                f"Factory '{factory_name}' not found (HTTP 404).",
                reachable=True,
                authenticated=True,
            )
        return _fail(
            f"Unexpected response contacting factory '{factory_name}' "
            f"(HTTP {status_code}).",
            reachable=True,
            authenticated=True,
        )
