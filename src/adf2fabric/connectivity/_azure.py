"""Shared Azure credential resolution for the ADF, ADLS Gen2, and Fabric
connectors.

Credentials are resolved from environment variables named in an
``AzureCredentialRef`` (never inline secrets). ``azure-identity`` is imported
lazily so the base package installs without the optional ``connectivity`` extra.
"""

from __future__ import annotations

import os
from typing import Any

from adf2fabric.project.models import AzureCredentialRef, ConnectivityStatus


class CredentialUnavailable(Exception):
    """A credential could not be built. Carries the matching outcome status.

    ``secret`` is the resolved secret (if any) so the caller can redact it from
    error text; it is never surfaced otherwise.
    """

    def __init__(self, status: ConnectivityStatus, detail: str, secret: str = ""):
        super().__init__(detail)
        self.status = status
        self.detail = detail
        self.secret = secret


def build_credential(cred: AzureCredentialRef | None) -> tuple[Any, str]:
    """Return ``(azure credential, secret)`` for ``cred``.

    ``secret`` is returned only so callers can redact it from error messages.
    Raises :class:`CredentialUnavailable` with a precise status when the SDK is
    absent or a referenced env var is unset.
    """
    if cred is None:
        raise CredentialUnavailable(
            ConnectivityStatus.missing_config,
            "No credential reference was provided.",
        )

    try:
        from azure.identity import (
            AzureCliCredential,
            ClientSecretCredential,
            DefaultAzureCredential,
        )
    except ImportError:
        raise CredentialUnavailable(
            ConnectivityStatus.dependency_unavailable,
            "azure-identity is not installed. Install the 'connectivity' extra.",
        ) from None

    auth = (cred.auth or "").strip().lower()

    if auth == "azure_cli":
        # Uses the identity of the current `az login` session. Authentication is
        # owned entirely by AzureCliCredential; nothing is read from env here.
        return AzureCliCredential(), ""

    if auth == "client_secret":
        missing = [
            name
            for name, env in (
                ("tenant_id_env", cred.tenant_id_env),
                ("client_id_env", cred.client_id_env),
                ("client_secret_env", cred.client_secret_env),
            )
            if not env
        ]
        if missing:
            raise CredentialUnavailable(
                ConnectivityStatus.missing_config,
                f"client_secret auth requires {', '.join(missing)}.",
            )
        tenant = os.environ.get(cred.tenant_id_env)
        client = os.environ.get(cred.client_id_env)
        secret = os.environ.get(cred.client_secret_env)
        unset = [
            env
            for env, val in (
                (cred.tenant_id_env, tenant),
                (cred.client_id_env, client),
                (cred.client_secret_env, secret),
            )
            if not val
        ]
        if unset:
            raise CredentialUnavailable(
                ConnectivityStatus.missing_credential,
                f"Environment variable(s) not set: {', '.join(unset)}.",
            )
        return ClientSecretCredential(tenant, client, secret), secret

    if auth == "azure_identity":
        # Ambient identity (managed identity / CLI / env). Nothing to resolve.
        return DefaultAzureCredential(), ""

    raise CredentialUnavailable(
        ConnectivityStatus.missing_config,
        f"Unknown Azure auth mechanism '{cred.auth}'. "
        "Use 'azure_cli', 'client_secret', or 'azure_identity'.",
    )
