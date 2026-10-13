"""Reading a secret from Azure Key Vault on the user's behalf.

Fabric connections have no Key Vault credential type, so a "secret in Key Vault" choice means: the BACKEND reads the secret
with the user's own Azure sign-in, sends it once to Fabric (or uses it for a test), and drops it. The value is never logged,
returned, stored, or placed in an exception. Only the vault host is trusted: the URL must be an Azure Key Vault endpoint, so
a crafted vault address can never make the backend send a token to another server.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from adf2fabric.connections.redaction import redact

# Public Azure and the sovereign clouds' Key Vault DNS suffixes.
_VAULT_SUFFIXES = (".vault.azure.net", ".vault.azure.cn", ".vault.usgovcloudapi.net", ".vault.microsoftazure.de",
                   ".managedhsm.azure.net")
_NAME = re.compile(r"^[A-Za-z0-9-]{1,127}$")
KEYVAULT_SCOPE = "https://vault.azure.net/.default"


class KeyVaultError(Exception):
    """User-facing and secret-free; ``actions`` says what to do about it."""

    def __init__(self, message: str, actions: Optional[list[str]] = None):
        super().__init__(message)
        self.actions = actions or []


@dataclass
class SecretInfo:
    """What a successful read proves, without the value."""

    vault: str
    name: str
    enabled: Optional[bool] = None
    content_type: Optional[str] = None


def normalize_vault_url(value: str) -> str:
    """``https://<vault>.vault.azure.net`` (no path), or a KeyVaultError."""
    raw = (value or "").strip()
    if raw and "://" not in raw:
        raw = f"https://{raw}"
    u = urlparse(raw)
    host = (u.hostname or "").lower()
    if u.scheme != "https" or not host or not host.endswith(_VAULT_SUFFIXES):
        raise KeyVaultError("The Key Vault address must be an https Azure Key Vault endpoint such as "
                            "https://<vault-name>.vault.azure.net.",
                            ["Copy the 'Vault URI' from the vault's Overview page in the Azure portal."])
    return f"https://{host}"


def validate_secret_name(name: str) -> str:
    if not _NAME.match(name or ""):
        raise KeyVaultError("A Key Vault secret name has 1-127 letters, digits or dashes.")
    return name


def _client(vault_url: str, credential: Any):
    from azure.keyvault.secrets import SecretClient

    return SecretClient(vault_url=vault_url, credential=credential)


def read_secret(vault_url: str, secret_name: str, credential: Any,
                client_factory: Callable[[str, Any], Any] = _client) -> tuple[str, SecretInfo]:
    """``(value, info)``. The caller owns the value and must not log it."""
    url = normalize_vault_url(vault_url)
    name = validate_secret_name(secret_name)
    try:
        secret = client_factory(url, credential).get_secret(name)
    except ImportError:
        raise KeyVaultError("azure-keyvault-secrets is not installed.",
                            ["Install the 'api' extra: pip install -e \".[api]\"."]) from None
    except Exception as exc:  # noqa: BLE001 - mapped below; never echo the raw message (it may carry a token)
        raise _map(exc, url, name) from None
    value = getattr(secret, "value", None)
    if not value:
        raise KeyVaultError(f"The secret '{name}' in {url} exists but has no value.")
    props = getattr(secret, "properties", None)
    return value, SecretInfo(vault=url, name=name, enabled=getattr(props, "enabled", None),
                             content_type=getattr(props, "content_type", None))


def _map(exc: Exception, url: str, name: str) -> KeyVaultError:
    status = getattr(exc, "status_code", None)
    kind = type(exc).__name__
    text = redact(str(exc)).lower()
    if kind == "ClientAuthenticationError" or status == 401 or "expired" in text or "az login" in text:
        return KeyVaultError("Azure rejected the sign-in used for Key Vault.",
                             ["Run `az login` again with an account that can read the vault."])
    if status == 403 or "forbidden" in text:
        return KeyVaultError(f"You are not allowed to read secrets in {url}.",
                             ["Grant your account 'Key Vault Secrets User' (RBAC) or a 'Get' secret access policy, "
                              "and check the vault's firewall allows this machine."])
    if status == 404 or "notfound" in text.replace(" ", ""):
        return KeyVaultError(f"The secret '{name}' was not found in {url}.", ["Check the secret name and the vault."])
    if kind in ("ServiceRequestError", "ConnectionError", "ConnectTimeout") or "name or service not known" in text:
        return KeyVaultError(f"{url} could not be reached.", ["Check the vault address and your network or firewall."])
    return KeyVaultError(f"Reading the secret failed ({kind}).")
