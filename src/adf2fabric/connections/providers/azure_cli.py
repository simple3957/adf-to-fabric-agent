"""Azure CLI provider: availability, sign-in state, subscriptions, resource discovery and source validation.

It drives the same ``az login`` session the rest of the project uses (intake, deploy) and never asks for a password or keeps
a token: tokens are fetched per call (``az account get-access-token``), held only in local variables, and sent only to ARM
(the existing ``ArmFetcher`` refuses any other host). Subscription selection is a parameter on every call; the provider never
changes the CLI's global default subscription.

What "authenticated" means here is stated, not assumed: an account exists AND a management token can be acquired. It says
nothing about Fabric or about any particular resource; those have their own checks.
"""

from __future__ import annotations

import json
import re
import threading
import time
from typing import Any, Callable, Optional
from urllib.parse import quote

from adf2fabric.agent.discovery.adf.arm import API_VERSION as ADF_API_VERSION
from adf2fabric.agent.intake.azure_source import AdfRequestError, ArmFetcher, describe_failure
from adf2fabric.connections.models import (
    ArmResource,
    AzureAccount,
    Check,
    CheckStatus,
    ConnectionState,
    ProviderStatus,
    Subscription,
    TestReport,
)
from adf2fabric import cloud
from adf2fabric.connections.providers._login import LoginAttempt, LoginManager
from adf2fabric.connections.redaction import redact
from adf2fabric.connections.runner import CliResult, Runner, run_cli

ARM = cloud.ARM_ENDPOINT
ARM_RESOURCE = cloud.ARM_RESOURCE
INSTALL_HINT = "Install the Azure CLI (https://aka.ms/installazurecli), then run `az login`."

# Resource types the UI lists, with the label shown to the user.
DISCOVERABLE = {
    "factories": "Microsoft.DataFactory/factories",
    "synapse_workspaces": "Microsoft.Synapse/workspaces",
    "storage_accounts": "Microsoft.Storage/storageAccounts",
    "sql_servers": "Microsoft.Sql/servers",
    "key_vaults": "Microsoft.KeyVault/vaults",
    "fabric_capacities": "Microsoft.Fabric/capacities",
}

_EXPIRED = re.compile(r"(?i)AADSTS(700082|50173|70043|50078|50079)|refresh token has expired|interactive authentication is needed|"
                      r"token (?:has )?expired|session has expired|az login")


# What Azure's AADSTS codes mean for the person who has to act. Anything not listed falls back to Azure's own first line.
_AAD_HINTS = {
    "530035": ("Azure blocked this sign-in because of the directory's security defaults.",
               "Set up multi-factor authentication for this account (portal.azure.com > the account's security info), then "
               "sign in with it again."),
    "50076": ("Azure needs multi-factor verification for this account.", "Sign in again and complete the verification."),
    "50079": ("Azure needs multi-factor verification for this account.", "Sign in again and complete the verification."),
    "90072": ("This account is not a user in that directory.", "Sign in with an account that belongs to that directory."),
    "700082": ("The sign-in expired.", "Sign in again."),
    "50173": ("The sign-in expired.", "Sign in again."),
    "70043": ("The sign-in expired.", "Sign in again."),
}


# The Azure CLI refuses to work at all when its sign-in cache holds two entries for one username (Azure CLI issue 20168).
# It happens when a browser sign-in picks the wrong identity, for example a work account's address that also exists as a
# personal Microsoft account, and the same username then appears in a second directory.
_DUPLICATE = re.compile(r"Found multiple accounts with the same username")
DUPLICATE_MESSAGE = "The Azure CLI has two saved sign-ins for the same username, so it refuses to pick one."
DUPLICATE_ACTIONS = [
    "Run `az account clear` in a terminal (it signs every account out of the CLI; nothing else is lost).",
    "Sign in again, one account at a time. For the second account use a private browser window so the browser does not "
    "reuse the first account's session, and choose the right identity (work or school vs personal) when asked.",
]


TOKEN_MARGIN_SECONDS = 300  # a held token is replaced 5 minutes before it expires
_UNKNOWN_EXPIRY_SECONDS = 600  # an answer without an expiry is held this long (well within any token's life)


def _token_and_expiry(stdout: str, now: float) -> tuple[str, float]:
    """The token and its expiry (epoch seconds) from `az account get-access-token`. Accepts the JSON this provider asks
    for, and a bare token (older CLIs, test fakes), which is held for a short time only."""
    text = stdout.strip()
    try:
        data = json.loads(text)
    except ValueError:
        return text, now + TOKEN_MARGIN_SECONDS + _UNKNOWN_EXPIRY_SECONDS
    if not isinstance(data, dict) or not data.get("t"):
        raise ProviderError("`az account get-access-token` returned no token.", ["Sign in again (`az login`)."])
    try:
        expires = float(data.get("e"))
    except (TypeError, ValueError):
        expires = now + TOKEN_MARGIN_SECONDS + _UNKNOWN_EXPIRY_SECONDS
    return str(data["t"]), expires


def token_failure(text: str) -> tuple[str, list[str]]:
    """A message and next step for a failed `az account get-access-token`, from what Azure said (never a token)."""
    if _DUPLICATE.search(text or ""):
        return DUPLICATE_MESSAGE, list(DUPLICATE_ACTIONS)
    code = re.search(r"AADSTS(\d+)", text or "")
    if code and code.group(1) in _AAD_HINTS:
        what, action = _AAD_HINTS[code.group(1)]
        return f"Could not get an Azure token (AADSTS{code.group(1)}): {what}", [action]
    if _EXPIRED.search(text or ""):
        return "Could not get an Azure token. The session has expired.", ["Sign in again (`az login`)."]
    line = _first_line(text)
    return "Could not get an Azure token." + (f" Azure said: {line}" if line else ""), ["Sign in again (`az login`)."]


class ProviderError(Exception):
    """A failure with an actionable message (already redacted)."""

    def __init__(self, message: str, actions: Optional[list[str]] = None):
        super().__init__(message)
        self.actions = actions or []


class AzureCliProvider:
    id = "azure_cli"

    def __init__(self, runner: Runner = run_cli, login_manager: Optional[LoginManager] = None,
                 transport_factory: Optional[Callable[[], Any]] = None,
                 fetcher_factory: Optional[Callable[[str], ArmFetcher]] = None,
                 cache_seconds: float = 20.0, clock: Callable[[], float] = time.monotonic,
                 wall_clock: Callable[[], float] = time.time):
        self._run = runner
        self.logins = login_manager or LoginManager()
        self._transport_factory = transport_factory
        self._fetcher_factory = fetcher_factory
        # `az account list` and `az account show` each take seconds, and a page asks for them on every click. Their
        # answers change only when someone signs in or out, so they are kept briefly and dropped at once on a sign-in or
        # sign-out (or when a caller asks for `refresh`).
        self._ttl, self._clock = cache_seconds, clock
        self._rows_cache: Optional[tuple[float, list[dict]]] = None
        self._status_cache: Optional[tuple[float, ProviderStatus]] = None
        # A token costs a CLI call (about a second) and is valid for about an hour, so it is reused until 5 minutes
        # before it expires. It is kept in this process's memory only: never written, logged or returned to a client.
        self._now = wall_clock
        self._tokens: dict[tuple, tuple[float, str]] = {}
        self._token_lock = threading.Lock()

    def invalidate(self) -> None:
        self._rows_cache = self._status_cache = None
        with self._token_lock:
            self._tokens.clear()

    # ---- availability + sign-in state ---------------------------------------------------------------------
    def version(self) -> Optional[str]:
        r = self._run(["az", "version", "--output", "json"], timeout=40)
        if not r.found:
            return None
        try:
            return str(json.loads(r.stdout).get("azure-cli"))
        except (ValueError, AttributeError):
            return "unknown" if r.ok else None

    def status(self, refresh: bool = False) -> ProviderStatus:
        """The default account's state; kept for a few seconds unless `refresh`."""
        now = self._clock()
        if not refresh and self._status_cache and now - self._status_cache[0] < self._ttl:
            return self._status_cache[1].model_copy(deep=True)
        st = self._status()
        if st.available:  # "not installed" is not worth keeping: installing it is exactly what the user does next
            self._status_cache = (now, st)
        return st.model_copy(deep=True)

    def _status(self) -> ProviderStatus:
        """Installed? Signed in? Can a management token actually be acquired (so a stale session is caught)?"""
        st = ProviderStatus(provider=self.id)
        show = self._run(["az", "account", "show", "--output", "json"], timeout=40)
        if not show.found:
            st.state, st.error, st.actions = ConnectionState.not_configured, "The Azure CLI is not installed.", [INSTALL_HINT]
            st.checks.append(Check(name="az installed", status=CheckStatus.failed, detail="'az' was not found on PATH"))
            return st
        st.available = True
        st.checks.append(Check(name="az installed", status=CheckStatus.passed))
        if not show.ok and _DUPLICATE.search(show.text):
            st.state, st.error, st.actions = ConnectionState.manual_action_required, DUPLICATE_MESSAGE, list(DUPLICATE_ACTIONS)
            st.checks.append(Check(name="signed in", status=CheckStatus.failed, detail="two sign-ins for one username"))
            return st
        if not show.ok:
            st.state = ConnectionState.authentication_required
            st.error = "No Azure CLI sign-in was found." if not _EXPIRED.search(show.text) else "The Azure CLI session has expired."
            st.actions = ["Start the sign-in (runs `az login` on this machine)."]
            st.checks.append(Check(name="signed in", status=CheckStatus.failed, detail=_first_line(show.text)))
            return st
        try:
            acct = json.loads(show.stdout)
        except ValueError:
            st.state, st.error = ConnectionState.validation_failed, "`az account show` returned something unreadable."
            return st
        st.account = (acct.get("user") or {}).get("name")
        st.tenant_id, st.subscription_id = acct.get("tenantId"), acct.get("id")
        st.checks.append(Check(name="signed in", status=CheckStatus.passed, detail=st.account or ""))
        tok = self._run(["az", "account", "get-access-token", "--resource", ARM_RESOURCE, "--query", "expiresOn", "-o", "tsv"],
                        timeout=60)
        if not tok.ok and _DUPLICATE.search(tok.text):
            st.state, st.error, st.actions = ConnectionState.manual_action_required, DUPLICATE_MESSAGE, list(DUPLICATE_ACTIONS)
            st.checks.append(Check(name="management token", status=CheckStatus.failed, detail="two sign-ins for one username"))
            return st
        if not tok.ok:
            st.state = ConnectionState.authentication_required
            st.error = ("The sign-in has expired." if _EXPIRED.search(tok.text) else "A management token could not be acquired.")
            st.actions = ["Sign in again (`az login`)."]
            st.checks.append(Check(name="management token", status=CheckStatus.failed, detail=_first_line(tok.text)))
            return st
        st.checks.append(Check(name="management token", status=CheckStatus.passed, detail="acquired (not stored)"))
        st.authenticated, st.state = True, ConnectionState.authenticated
        st.version = self.version()
        return st

    # ---- sign-in --------------------------------------------------------------------------------------------
    def start_login(self, tenant: Optional[str] = None, device_code: bool = False) -> LoginAttempt:
        if self._run(["az", "--version"], timeout=40).found is False:
            raise ProviderError("The Azure CLI is not installed.", [INSTALL_HINT])
        self.invalidate()
        argv = ["az", "login", "--allow-no-subscriptions"]
        if tenant:
            argv += ["--tenant", tenant]
        if device_code:
            argv.append("--use-device-code")
        # The API runs in the background, where Windows' account-picker (WAM) can't come to the front and the sign-in
        # hangs unseen. Use the browser sign-in instead (still supports Microsoft Authenticator / MFA).
        return self.logins.start(argv, env_extra={"AZURE_CORE_ENABLE_BROKER_ON_WINDOWS": "false"})

    def logout(self, username: Optional[str] = None) -> ProviderStatus:
        """Sign ``username`` (default: the current account) out of the Azure CLI on this machine.

        Other signed-in accounts stay signed in; the CLI then falls back to one of them, which ``status()`` reports.
        """
        argv = ["az", "logout"] + (["--username", username] if username else [])
        r = self._run(argv, timeout=60)
        self.invalidate()
        if not r.found:
            raise ProviderError("The Azure CLI is not installed.", [INSTALL_HINT])
        if not r.ok and "no account" not in r.text.lower() and "please run 'az login'" not in r.text.lower():
            raise ProviderError(f"Sign-out failed: {redact(r.text)[:300]}")
        return self.status(refresh=True)

    # ---- tokens (internal only; never returned by any API) ---------------------------------------------------
    def token(self, subscription: Optional[str] = None, tenant: Optional[str] = None, resource: str = ARM_RESOURCE,
              account: Optional[str] = None) -> str:
        """A token for `resource`. `account` (a signed-in username) selects that person without changing the CLI's
        default account: it is resolved to one of the account's own subscription entries, which the CLI maps back to it."""
        if account and not subscription:
            subscription = self.subscription_for(account, tenant)
            if subscription is None:
                raise ProviderError(f"{account} is not signed in on this machine.", ["Sign in with that account first."])
        key = (resource, subscription or "", "" if subscription else (tenant or ""))
        with self._token_lock:
            held = self._tokens.get(key)
        if held and held[0] - TOKEN_MARGIN_SECONDS > self._now():
            return held[1]
        argv = ["az", "account", "get-access-token", "--resource", resource,
                "--query", "{t:accessToken,e:expires_on}", "-o", "json"]
        if subscription:
            argv += ["--subscription", subscription]
        elif tenant:
            argv += ["--tenant", tenant]
        r = self._run(argv, timeout=60, raw=True)  # the only call that keeps stdout unredacted: the token stays local
        if not r.ok or not r.stdout.strip():
            raise ProviderError(*token_failure(r.text))
        token, expires = _token_and_expiry(r.stdout, self._now())
        with self._token_lock:
            self._tokens[key] = (expires, token)
        return token

    # ---- subscriptions + discovery ---------------------------------------------------------------------------
    def _rows(self, refresh: bool = False) -> list[dict]:
        now = self._clock()
        if not refresh and self._rows_cache and now - self._rows_cache[0] < self._ttl:
            return self._rows_cache[1]
        r = self._run(["az", "account", "list", "--all", "--output", "json"], timeout=90)
        if not r.ok:
            raise ProviderError("Could not list subscriptions." + (" " + _first_line(r.text) if r.text else ""),
                                ["Sign in with `az login`, then retry."])
        try:
            rows = json.loads(r.stdout or "[]")
        except ValueError:
            raise ProviderError("`az account list` returned something unreadable.") from None
        self._rows_cache = (now, rows)
        return rows

    def subscriptions(self, account: Optional[str] = None, refresh: bool = False) -> list[Subscription]:
        """Subscriptions of every signed-in account, or of just `account`."""
        out = []
        for a in self._rows(refresh):
            user = (a.get("user") or {}).get("name")
            if account and (user or "").lower() != account.lower():
                continue
            out.append(Subscription(id=a.get("id", ""), name=a.get("name", ""), tenant_id=a.get("tenantId"),
                                    state=a.get("state"), is_default=bool(a.get("isDefault")), user=user))
        return out

    def accounts(self, refresh: bool = False) -> list[AzureAccount]:
        """Who is signed in on this machine: one entry per username, default first, then by name."""
        by_user: dict[str, AzureAccount] = {}
        for a in self._rows(refresh):
            user = (a.get("user") or {}).get("name")
            if not user:
                continue
            acc = by_user.setdefault(user.lower(), AzureAccount(username=user))
            tenant = a.get("tenantId")
            if tenant and tenant not in acc.tenants:
                acc.tenants.append(tenant)
            acc.is_default = acc.is_default or bool(a.get("isDefault"))
            if not str(a.get("name", "")).startswith("N/A(tenant level"):
                acc.subscription_count += 1
        return sorted(by_user.values(), key=lambda x: (not x.is_default, x.username.lower()))

    def subscription_for(self, account: str, tenant: Optional[str] = None) -> Optional[str]:
        """An entry of `account` the CLI can use to pick that person for a token. Prefers the given tenant, then a real
        subscription over the tenant-level entry a sign-in without subscriptions leaves behind."""
        mine = [a for a in self._rows() if ((a.get("user") or {}).get("name") or "").lower() == account.lower()]
        if tenant:
            in_tenant = [a for a in mine if a.get("tenantId") == tenant]
            mine = in_tenant or mine
        if not mine:
            return None
        mine.sort(key=lambda a: (a.get("state") != "Enabled", str(a.get("name", "")).startswith("N/A(tenant level")))
        return mine[0].get("id")

    def tenants(self) -> list[dict[str, str]]:
        r = self._run(["az", "account", "tenant", "list", "--output", "json"], timeout=60)
        if not r.ok:
            return []
        return [{"id": t.get("tenantId", ""), "name": t.get("displayName") or t.get("defaultDomain") or ""}
                for t in json.loads(r.stdout or "[]")]

    def _fetcher(self, subscription: str) -> ArmFetcher:
        token = self.token(subscription=subscription)
        if self._fetcher_factory:
            return self._fetcher_factory(token)
        if self._transport_factory:
            transport = self._transport_factory()
        else:
            import requests

            transport = requests.Session()
        return ArmFetcher(transport, token)

    def list_resources(self, subscription: str, kind: str) -> list[ArmResource]:
        """Resources of one discoverable kind in a subscription (generic ARM list, so no per-service api-version)."""
        rtype = DISCOVERABLE[kind]
        url = f"{ARM}/subscriptions/{subscription}/resources?api-version=2021-04-01&$filter={quote(f'resourceType eq {chr(39)}{rtype}{chr(39)}')}"
        try:
            items = self._fetcher(subscription).list_all(url)
        except AdfRequestError as exc:
            raise ProviderError(*_arm_failure(exc, f"the {kind.replace('_', ' ')} of subscription '{subscription}'",
                                              subscription)) from None
        out = []
        for i in items:
            rid = str(i.get("id", ""))
            m = re.search(r"/resourceGroups/([^/]+)/", rid, re.I)
            out.append(ArmResource(id=rid, name=str(i.get("name", "")), type=str(i.get("type", rtype)),
                                   location=i.get("location"), resource_group=m.group(1) if m else None))
        return sorted(out, key=lambda r: ((r.resource_group or "").lower(), r.name.lower()))

    def discover(self, subscription: str) -> dict[str, list[ArmResource]]:
        """Everything the UI lists for a subscription; a kind that cannot be read is reported, not fatal."""
        found: dict[str, list[ArmResource]] = {}
        for kind in DISCOVERABLE:
            try:
                found[kind] = self.list_resources(subscription, kind)
            except ProviderError as exc:
                found[kind] = []
                found.setdefault("_errors", []).append(  # type: ignore[arg-type]
                    ArmResource(id=kind, name=str(exc), type="error"))
        return found

    def get_resource(self, subscription: str, resource_id: str, api_version: str) -> dict:
        try:
            return self._fetcher(subscription).get(f"{ARM}{resource_id}?api-version={api_version}")
        except AdfRequestError as exc:
            raise ProviderError(*_arm_failure(exc, f"resource '{resource_id.rsplit('/', 1)[-1]}'", subscription)) from None

    # ---- source validation -----------------------------------------------------------------------------------
    def validate_factory(self, subscription: str, resource_group: str, factory: str) -> TestReport:
        """Reads the factory and counts its pipelines: proves the signed-in identity can read what migration needs."""
        rep = TestReport(target=f"Data Factory '{factory}'", state=ConnectionState.validation_failed)
        base = (f"{ARM}/subscriptions/{subscription}/resourceGroups/{resource_group}"
                f"/providers/Microsoft.DataFactory/factories/{factory}")
        try:
            f = self._fetcher(subscription)
        except ProviderError as exc:
            rep.state, rep.error, rep.actions = ConnectionState.authentication_required, str(exc), exc.actions
            return rep
        try:
            f.get(f"{base}?api-version={ADF_API_VERSION}")
            rep.verified.append("the factory exists and this identity can read it")
            rep.checks.append(Check(name="factory read", status=CheckStatus.passed))
            n = len(f.list_all(f"{base}/pipelines?api-version={ADF_API_VERSION}"))
            rep.verified.append(f"{n} pipeline(s) can be listed")
            rep.checks.append(Check(name="pipelines list", status=CheckStatus.passed, detail=str(n)))
        except AdfRequestError as exc:
            err, action = describe_failure(exc, what=f"factory '{factory}'", factory=factory, resource_group=resource_group,
                                           subscription=subscription)
            rep.error, rep.actions = err, [action]
            rep.checks.append(Check(name="factory read", status=CheckStatus.failed, detail=err))
            return rep
        rep.state = ConnectionState.connection_tested
        rep.not_verified.append("linked-service credentials and the data stores behind them are not tested here")
        return rep

    def validate_synapse_workspace(self, subscription: str, resource_group: str, workspace: str) -> TestReport:
        rep = TestReport(target=f"Synapse workspace '{workspace}'", state=ConnectionState.validation_failed)
        rid = f"/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.Synapse/workspaces/{workspace}"
        try:
            props = self.get_resource(subscription, rid, "2021-06-01")
        except ProviderError as exc:
            rep.error, rep.actions = str(exc), exc.actions
            return rep
        rep.verified.append("the workspace exists and this identity can read it")
        ep = ((props.get("properties") or {}).get("connectivityEndpoints") or {})
        if ep.get("dev"):
            rep.verified.append("its development endpoint is advertised")
        rep.state = ConnectionState.connection_tested
        rep.not_verified.append("Synapse artifacts (pipelines, notebooks) are read from a Git export or ZIP, not from the live workspace")
        return rep


def _first_line(text: str) -> str:
    for line in (text or "").splitlines():
        if line.strip():
            return redact(line.strip())[:300]
    return ""


def _arm_failure(exc: AdfRequestError, what: str, subscription: str) -> tuple[str, list[str]]:
    s = exc.status_code
    if s == 401:
        return f"Azure rejected the sign-in while reading {what}.", ["Sign in again (`az login`)."]
    if s == 403:
        return (f"Permission denied reading {what}.",
                [f"Grant this identity the Reader role on subscription '{subscription}' (or the resource group)."])
    if s == 404:
        return f"{what[0].upper() + what[1:]} was not found.", ["Check the subscription id and that this account can see it."]
    if s == 429:
        return f"Azure throttled the request for {what}.", ["Wait a minute and retry."]
    return f"Reading {what} failed ({s if s is not None else 'network'}).", ["Retry; check your network."]
