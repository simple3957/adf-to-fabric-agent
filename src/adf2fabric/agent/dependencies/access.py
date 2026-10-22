"""Least-privilege access checks, one per provider, run only when the caller asks for them.

Why separate checks: being signed in to ``az`` proves nothing about Databricks, Key Vault, a SQL server, a storage
account or a private endpoint. Each provider has its own token audience, its own permission and its own network path, so
each gets its own probe, and a failure in one says nothing about another.

What every probe does and does not do:

* Databricks: ``workspace/get-status`` on the notebook path. Reads the object's metadata (type, language). It never calls
  ``export``, so no notebook source is retrieved.
* Key Vault: lists the versions of ONE named secret (``secrets/{name}/versions``). That returns attributes (enabled,
  dates), never the value, and needs only the ``list`` permission, not ``get``.
* Storage: reads the properties of the container (``HEAD ?resource=filesystem``). No path is listed and no blob is read.
* Network: a DNS lookup and a TCP connect to host:port. No bytes are sent.

A token is obtained per provider from an environment variable the caller NAMES, or (only when allowed) from ``az`` for that
provider's own audience. It lives in memory for one request, is sent only to the host being checked, and appears in no
result. Every detail string is passed through :func:`redact`.
"""

from __future__ import annotations

import os
import re
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional
from urllib.parse import quote

from adf2fabric.project.dependencies import AccessCheck, AccessStatus, Dependency

DATABRICKS_AUDIENCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
KEYVAULT_AUDIENCE = "https://vault.azure.net"
STORAGE_AUDIENCE = "https://storage.azure.com/"
PROVIDERS = ("databricks", "key_vault", "storage", "network")
DEFAULT_TOKEN_ENV = {"databricks": "ADF2FABRIC_DATABRICKS_TOKEN"}
_AUDIENCE = {"databricks": DATABRICKS_AUDIENCE, "key_vault": KEYVAULT_AUDIENCE, "storage": STORAGE_AUDIENCE}
_SECRETISH = re.compile(r"(?i)(bearer\s+[\w\-.~+/=]+|eyJ[\w\-.]{20,}|dapi[0-9a-f]{20,}|(sig|key|token|pwd|password)=[^&\s]+)")
_HOST = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9.\-]{0,251}[A-Za-z0-9])?$")


def redact(text: str) -> str:
    return _SECRETISH.sub("[redacted]", text or "")[:300]


# A transport is (method, url, headers) -> (status, parsed json body or None). It raises OSError when nothing answered.
Transport = Callable[[str, str, dict], "tuple[int, Any]"]
TokenFn = Callable[[str], Optional[str]]
Connector = Callable[[str, int], None]  # raises OSError if it cannot connect


def _requests_transport(method: str, url: str, headers: dict) -> tuple[int, Any]:
    try:
        import requests
    except ImportError as exc:
        raise ModuleNotFoundError("requests") from exc
    try:
        r = requests.request(method, url, headers=headers, timeout=20, allow_redirects=False)
    except requests.RequestException as exc:  # no URL (and so no secret) in what is re-raised
        raise OSError(type(exc).__name__) from None
    try:
        body = r.json() if r.content else None
    except ValueError:
        body = None
    return r.status_code, body


def _tcp(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=5):
        pass


@dataclass
class TokenSource:
    """Where a provider's token comes from. Environment variable NAMES only; ``az`` only when allowed."""

    env_names: dict[str, str] = field(default_factory=dict)  # provider -> variable name
    allow_az: bool = False
    az_account: Optional[str] = None
    _az: Any = None

    def __call__(self, provider: str) -> Optional[str]:
        name = self.env_names.get(provider) or DEFAULT_TOKEN_ENV.get(provider)
        if name and os.environ.get(name, "").strip():
            return os.environ[name].strip()
        if self.allow_az and provider in _AUDIENCE:
            try:
                if self._az is None:
                    from adf2fabric.connections.providers.azure_cli import AzureCliProvider

                    self._az = AzureCliProvider()
                return self._az.token(resource=_AUDIENCE[provider], account=self.az_account)
            except Exception:  # noqa: BLE001 - the reason is not worth a secret-bearing message
                return None
        return None


def _check(provider: str, target: str, status: AccessStatus, now: datetime, *, permission: Optional[str] = None,
           missing: Optional[str] = None, detail: str = "", probe: str = "") -> AccessCheck:
    return AccessCheck(provider=provider, target=target, status=status, permission=permission, missing=missing,
                       detail=redact(detail), probe=probe, checked_at=now)


def _no_token(provider: str, target: str, now: datetime, probe: str, tokens: TokenSource) -> AccessCheck:
    var = tokens.env_names.get(provider) or DEFAULT_TOKEN_ENV.get(provider)
    how = (f"set the environment variable {var}" if var else "name an environment variable that holds a token")
    return _check(provider, target, AccessStatus.missing_credential, now,
                  missing=f"a {provider.replace('_', ' ')} token (audience {_AUDIENCE.get(provider, 'n/a')})",
                  detail="No token was available for this provider. Being signed in to az does not grant it: " + how +
                         (", or allow the az token for this audience." if provider in _AUDIENCE else "."),
                  probe=probe)


def _http_status(provider: str, target: str, status: int, now: datetime, *, permission: str, missing: str, probe: str,
                 detail_ok: str = "") -> AccessCheck:
    if 200 <= status < 300:
        return _check(provider, target, AccessStatus.accessible, now, permission=permission, detail=detail_ok, probe=probe)
    if status == 401:
        return _check(provider, target, AccessStatus.unauthenticated, now, permission=permission,
                      missing="a valid token for this provider's audience", detail="HTTP 401: the token was rejected.", probe=probe)
    if status == 403:
        return _check(provider, target, AccessStatus.denied, now, permission=permission, missing=missing,
                      detail="HTTP 403: authenticated, but not permitted. This may also be a firewall or private-endpoint rule.", probe=probe)
    if status == 404:
        return _check(provider, target, AccessStatus.not_found, now, permission=permission,
                      detail="HTTP 404: the target does not exist (or this identity cannot see it).", probe=probe)
    return _check(provider, target, AccessStatus.error, now, permission=permission, detail=f"HTTP {status}.", probe=probe)


def _unreachable(provider: str, target: str, now: datetime, exc: BaseException, probe: str) -> AccessCheck:
    if isinstance(exc, ModuleNotFoundError):
        return _check(provider, target, AccessStatus.missing_dependency, now,
                      missing="the 'requests' package (install the connectivity extra)", probe=probe)
    return _check(provider, target, AccessStatus.unreachable, now,
                  missing="network path to the target (DNS, firewall or private endpoint)", detail=type(exc).__name__, probe=probe)


# --- one probe per provider ------------------------------------------------------------------------------------------


def check_databricks(host: str, path: str, tokens: TokenSource, transport: Transport, now: datetime) -> AccessCheck:
    probe = "GET workspace/get-status on the notebook path (metadata only; the notebook source is not exported)."
    perm = "Databricks workspace: CAN READ on the notebook (or its folder)"
    if not _HOST.match(host or ""):
        return _check("databricks", host or "?", AccessStatus.not_configured, now, missing="the workspace host", probe=probe)
    token = tokens("databricks")
    if not token:
        return _no_token("databricks", host, now, probe, tokens)
    url = f"https://{host}/api/2.0/workspace/get-status?path={quote(path, safe='')}"
    try:
        status, body = transport("GET", url, {"Authorization": f"Bearer {token}"})
    except (OSError, ModuleNotFoundError) as exc:
        return _unreachable("databricks", host, now, exc, probe)
    if status == 200 and isinstance(body, dict) and body.get("object_type") not in (None, "NOTEBOOK"):
        return _check("databricks", host, AccessStatus.error, now, permission=perm,
                      detail=f"The path is a {body.get('object_type')}, not a notebook.", probe=probe)
    return _http_status("databricks", host, status, now, permission=perm, missing=f"CAN READ on '{path}'", probe=probe)


def check_key_vault(vault_host: str, secret: str, tokens: TokenSource, transport: Transport, now: datetime) -> AccessCheck:
    probe = "GET secrets/{name}/versions (attributes only; the secret value is never requested)."
    perm = "Key Vault: secrets/list (access policy) or 'Key Vault Secrets User'-style data-plane list on the secret"
    if not _HOST.match(vault_host or ""):
        return _check("key_vault", vault_host or "?", AccessStatus.not_configured, now,
                      missing="the vault address (the Key Vault linked service has no baseUrl)", probe=probe)
    token = tokens("key_vault")
    if not token:
        return _no_token("key_vault", vault_host, now, probe, tokens)
    url = f"https://{vault_host}/secrets/{quote(secret, safe='')}/versions?api-version=7.4&maxresults=1"
    try:
        status, _ = transport("GET", url, {"Authorization": f"Bearer {token}"})
    except (OSError, ModuleNotFoundError) as exc:
        return _unreachable("key_vault", vault_host, now, exc, probe)
    return _http_status("key_vault", vault_host, status, now, permission=perm, missing="list permission on this secret", probe=probe)


def check_storage(account: str, container: str, tokens: TokenSource, transport: Transport, now: datetime) -> AccessCheck:
    target = f"{account}/{container}"
    probe = "HEAD ?resource=filesystem (container properties only; nothing is listed or read)."
    perm = "Storage: Storage Blob Data Reader (or any data-plane role) on the container"
    if not re.match(r"^[a-z0-9]{3,24}$", account or "") or not container:
        return _check("storage", target, AccessStatus.not_configured, now, missing="a literal storage account and container", probe=probe)
    token = tokens("storage")
    if not token:
        return _no_token("storage", target, now, probe, tokens)
    url = f"https://{account}.dfs.core.windows.net/{quote(container, safe='')}?resource=filesystem"
    try:
        status, _ = transport("HEAD", url, {"Authorization": f"Bearer {token}", "x-ms-version": "2023-11-03"})
    except (OSError, ModuleNotFoundError) as exc:
        return _unreachable("storage", target, now, exc, probe)
    return _http_status("storage", target, status, now, permission=perm, missing=f"a data-plane role on '{container}'", probe=probe)


def check_network(host: str, port: int, connector: Connector, now: datetime) -> AccessCheck:
    target = f"{host}:{port}"
    probe = "DNS lookup and TCP connect; no bytes are sent."
    if not _HOST.match(host or ""):
        return _check("network", target, AccessStatus.not_configured, now, missing="a literal host name", probe=probe)
    try:
        connector(host, port)
    except socket.gaierror:
        return _check("network", target, AccessStatus.unreachable, now, missing="DNS resolution of the host", detail="The name did not resolve (a private endpoint may need private DNS).", probe=probe)
    except OSError as exc:
        return _check("network", target, AccessStatus.unreachable, now, missing="a network path to the port",
                      detail=f"Could not connect ({type(exc).__name__}): a firewall or private endpoint may be blocking it.", probe=probe)
    return _check("network", target, AccessStatus.accessible, now, detail="The port accepted a connection (no login attempted).", probe=probe)


# --- running them ---------------------------------------------------------------------------------------------------------------


def run_checks(deps: Iterable[Dependency], providers: Iterable[str], tokens: Optional[TokenSource] = None,
               transport: Transport = _requests_transport, connector: Connector = _tcp,
               now: Optional[datetime] = None) -> dict[str, AccessCheck]:
    """Probe each dependency whose provider is in ``providers``. One probe per distinct target, whoever consumes it.

    A failure of one probe never stops the others. Keys are ``builder.access_key(dep)``.
    """
    from adf2fabric.agent.dependencies.builder import access_key

    wanted, tokens, now = set(providers), tokens or TokenSource(), now or datetime.now(timezone.utc)
    out: dict[str, AccessCheck] = {}
    for dep in deps:
        a = dep.attributes
        prov = a.get("access.provider")
        key = access_key(dep)
        if not prov or prov not in wanted or key is None or key in out:
            continue
        try:
            if prov == "databricks":
                out[key] = check_databricks(a["access.target"], a.get("access.path", ""), tokens, transport, now)
            elif prov == "key_vault":
                out[key] = check_key_vault(a["access.target"], a.get("access.secret", ""), tokens, transport, now)
            elif prov == "storage":
                acct, _, cont = a["access.target"].partition("/")
                out[key] = check_storage(acct, cont, tokens, transport, now)
            elif prov == "network":
                out[key] = check_network(a["access.target"], int(a.get("access.port", "443")), connector, now)
        except Exception as exc:  # noqa: BLE001 - a bug in one probe must not hide the rest
            out[key] = _check(prov, a.get("access.target", "?"), AccessStatus.error, now, detail=f"Check failed ({type(exc).__name__}).")
    return out
