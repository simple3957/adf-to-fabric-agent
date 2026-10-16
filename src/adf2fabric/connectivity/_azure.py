"""Shared Azure credential resolution for the ADF, ADLS Gen2, and Fabric
connectors.

Credentials are resolved from environment variables named in an
``AzureCredentialRef`` (never inline secrets). ``azure-identity`` is imported
lazily so the base package installs without the optional ``connectivity`` extra.

Supported mechanisms (``AUTH_MECHANISMS``):

* Microsoft Entra, usable for ARM (Data Factory), Fabric and ADLS Gen2:
  ``azure_cli`` (reuse ``az login``), ``client_secret``, ``client_certificate``,
  ``managed_identity``, ``interactive_browser``, ``device_code``, ``access_token`` (a pre-acquired
  bearer token), ``azure_identity`` (the SDK's default chain).
* Storage data plane only (ADLS Gen2): ``account_key``, ``sas_token``, ``connection_string``. These
  are returned as the plain string the storage SDK accepts as ``credential=``; asking for them
  where a token is required (ARM, Fabric) is refused with a clear message.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any

from adf2fabric.project.models import AzureCredentialRef, ConnectivityStatus

ENTRA_MECHANISMS = (
    "azure_cli", "client_secret", "client_certificate", "managed_identity", "interactive_browser",
    "device_code", "access_token", "azure_identity",
)
STORAGE_ONLY_MECHANISMS = ("account_key", "sas_token", "connection_string")
AUTH_MECHANISMS = ENTRA_MECHANISMS + STORAGE_ONLY_MECHANISMS


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


class _StaticTokenCredential:
    """A pre-acquired bearer token presented as an azure-core credential (the caller owns its audience/lifetime)."""

    def __init__(self, token: str):
        self._token = token

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        from azure.core.credentials import AccessToken

        return AccessToken(self._token, int(time.time()) + 3600)


def _env(name: str | None) -> str | None:
    return os.environ.get(name) if name else None


def _need(mechanism: str, **refs: str | None) -> dict[str, str]:
    """Resolve required env-var references; raise a precise error naming what is missing or unset."""
    unnamed = [k for k, v in refs.items() if not v]
    if unnamed:
        raise CredentialUnavailable(
            ConnectivityStatus.missing_config, f"{mechanism} auth requires {', '.join(unnamed)}.")
    values = {k: os.environ.get(v or "") for k, v in refs.items()}
    unset = [refs[k] for k, v in values.items() if not v]
    if unset:
        raise CredentialUnavailable(
            ConnectivityStatus.missing_credential, f"Environment variable(s) not set: {', '.join(unset)}.")
    return values  # type: ignore[return-value]


def _storage_secret(cred: AzureCredentialRef, mechanism: str) -> tuple[str, str]:
    """The account key / SAS token the storage SDK takes as ``credential=``, plus the secret to redact."""
    if mechanism == "account_key":
        v = _need(mechanism, account_key_env=cred.account_key_env)["account_key_env"]
        return v, v
    if mechanism == "sas_token":
        v = _need(mechanism, sas_token_env=cred.sas_token_env)["sas_token_env"].lstrip("?")
        return v, v
    conn = _need(mechanism, connection_string_env=cred.connection_string_env)["connection_string_env"]
    for field in ("AccountKey", "SharedAccessSignature"):
        m = re.search(rf"(?:^|;)\s*{field}=([^;]+)", conn, re.IGNORECASE)
        if m:
            v = m.group(1).strip().lstrip("?")
            return v, v
    raise CredentialUnavailable(
        ConnectivityStatus.missing_config,
        "The connection string holds neither AccountKey nor SharedAccessSignature.", conn)


def build_credential(cred: AzureCredentialRef | None, *, storage_ok: bool = False) -> tuple[Any, str]:
    """Return ``(credential, secret)`` for ``cred``.

    ``secret`` is returned only so callers can redact it from error messages. ``storage_ok`` is set
    by ADLS Gen2 call sites: only they may use ``account_key`` / ``sas_token`` / ``connection_string``.
    Raises :class:`CredentialUnavailable` with a precise status when the SDK is absent, a referenced
    env var is unset, or the mechanism is unknown or not valid for the target.
    """
    if cred is None:
        raise CredentialUnavailable(
            ConnectivityStatus.missing_config,
            "No credential reference was provided.",
        )

    auth = (cred.auth or "").strip().lower()
    if auth in STORAGE_ONLY_MECHANISMS:
        if not storage_ok:
            raise CredentialUnavailable(
                ConnectivityStatus.missing_config,
                f"'{auth}' authenticates storage (ADLS Gen2) only; Azure Resource Manager and Fabric need a "
                f"Microsoft Entra mechanism: {', '.join(ENTRA_MECHANISMS)}.")
        return _storage_secret(cred, auth)  # a plain string; no azure-identity needed
    if auth not in ENTRA_MECHANISMS:
        raise CredentialUnavailable(
            ConnectivityStatus.missing_config,
            f"Unknown Azure auth mechanism '{cred.auth}'. Use one of: {', '.join(AUTH_MECHANISMS)}.")

    if auth == "access_token":
        token = _need(auth, access_token_env=cred.access_token_env)["access_token_env"]
        return _StaticTokenCredential(token), token

    try:
        import azure.identity as ai
    except ImportError:
        raise CredentialUnavailable(
            ConnectivityStatus.dependency_unavailable,
            "azure-identity is not installed. Install the 'connectivity' extra.",
        ) from None

    if auth == "azure_cli":
        # Uses the identity of the current `az login` session. An optional tenant (when several tenants are
        # logged in) selects which one; the secret handling is owned entirely by the Azure CLI.
        tenant = _env(cred.tenant_id_env)
        kwargs = {k: v for k, v in (("tenant_id", tenant), ("subscription", (cred.subscription_id or "").strip() or None))
                  if v}
        return ai.AzureCliCredential(**kwargs), ""

    if auth == "client_secret":
        v = _need(auth, tenant_id_env=cred.tenant_id_env, client_id_env=cred.client_id_env,
                  client_secret_env=cred.client_secret_env)
        return (ai.ClientSecretCredential(v["tenant_id_env"], v["client_id_env"], v["client_secret_env"]),
                v["client_secret_env"])

    if auth == "client_certificate":
        v = _need(auth, tenant_id_env=cred.tenant_id_env, client_id_env=cred.client_id_env,
                  certificate_path_env=cred.certificate_path_env)
        password = _env(cred.certificate_password_env)
        return (ai.CertificateCredential(v["tenant_id_env"], v["client_id_env"],
                                         certificate_path=v["certificate_path_env"], password=password),
                password or "")

    if auth == "managed_identity":
        client_id = _env(cred.client_id_env)  # optional: only a user-assigned identity needs it
        if cred.client_id_env and not client_id:
            raise CredentialUnavailable(
                ConnectivityStatus.missing_credential, f"Environment variable(s) not set: {cred.client_id_env}.")
        return (ai.ManagedIdentityCredential(client_id=client_id) if client_id
                else ai.ManagedIdentityCredential()), ""

    if auth == "interactive_browser":
        tenant = _env(cred.tenant_id_env)
        return (ai.InteractiveBrowserCredential(tenant_id=tenant) if tenant
                else ai.InteractiveBrowserCredential()), ""

    if auth == "device_code":
        tenant = _env(cred.tenant_id_env)
        return (ai.DeviceCodeCredential(tenant_id=tenant) if tenant else ai.DeviceCodeCredential()), ""

    # azure_identity: the SDK's default chain (environment, workload/managed identity, CLI, ...).
    return ai.DefaultAzureCredential(), ""
