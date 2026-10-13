"""Fabric destination: one API surface, two transports.

* ``RestFabricApi``  - HTTPS to api.fabric.microsoft.com with a token the Azure CLI session supplies (held in a local variable
                       per call, never returned; the host is pinned, so a token can only ever go to Fabric).
* ``FabCliApi``      - the installed Fabric CLI's own authenticated ``fab api``; the CLI owns its token cache and this code
                       never reads it.

``FabricService`` is written once against the ``FabricApi`` protocol: workspaces, capacities, connections, supported connection
types, gateways, workspace identity. Azure CLI and Fabric CLI sign-in states are separate objects; nothing here assumes a
successful sign-in means a workspace operation is authorised, and ``validate_workspace`` says exactly what it proved.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol

from adf2fabric.connections.models import Check, CheckStatus, ConnectionState, FabricWorkspace, TestReport
from adf2fabric.connections.redaction import redact
from adf2fabric.connections.runner import Runner, run_cli

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE_RESOURCE = "https://api.fabric.microsoft.com"


@dataclass
class ApiResponse:
    status: int
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)


class FabricApiError(Exception):
    """A Fabric call failed. The message is redacted and carries what to do about it."""

    def __init__(self, message: str, status: Optional[int] = None, code: str = "", actions: Optional[list[str]] = None):
        super().__init__(message)
        self.status, self.code, self.actions = status, code, actions or []


class FabricApi(Protocol):
    def request(self, method: str, path: str, body: Optional[dict] = None) -> ApiResponse: ...


# ---- transports -----------------------------------------------------------------------------------------------


class RestFabricApi:
    """Token supplied per call by ``token_provider`` (for example ``AzureCliProvider.token(resource=...)``)."""

    def __init__(self, token_provider: Callable[[], str], session: Any = None, sleep: Callable[[float], None] = time.sleep):
        self._token = token_provider
        self._session = session
        self._sleep = sleep

    def request(self, method: str, path: str, body: Optional[dict] = None) -> ApiResponse:
        url = path if path.startswith("http") else f"{FABRIC_API}/{path.lstrip('/')}"
        if not url.startswith(FABRIC_API + "/"):
            raise FabricApiError("Refusing to send a credential to a host other than the Fabric API.")
        if self._session is None:
            import requests

            self._session = requests.Session()
        token = self._token()
        try:
            for _ in range(6):
                resp = self._session.request(method, url, headers={"Authorization": f"Bearer {token}",
                                             "Content-Type": "application/json"}, json=body, timeout=60)
                if resp.status_code == 429:
                    self._sleep(float(resp.headers.get("Retry-After", 5)))
                    continue
                try:
                    payload = resp.json() if resp.content else None
                except ValueError:
                    payload = None
                return ApiResponse(resp.status_code, payload, dict(resp.headers))
        except FabricApiError:
            raise
        except Exception as exc:  # noqa: BLE001 - network failure; never echo a message that could hold a token
            raise FabricApiError(f"Fabric could not be reached ({type(exc).__name__}).",
                                 actions=["Check your network and try again."]) from None
        raise FabricApiError("Fabric kept rate-limiting the request.", 429, actions=["Wait a minute and retry."])


def _first_json(text: str) -> Any:
    """The first JSON document in CLI output (the CLI prints a status line before it)."""
    for i, ch in enumerate(text):
        if ch in "{[":
            try:
                return json.JSONDecoder().raw_decode(text[i:])[0]
            except ValueError:
                continue
    return None


def unwrap_fab(obj: Any) -> Any:
    """The Fabric REST body inside ``fab`` JSON output. Tolerant: the CLI wraps results as
    ``{"result": {"data": [{...}]}}``; the body may be the item itself or under ``text``."""
    cur = obj
    if isinstance(cur, dict) and isinstance(cur.get("result"), dict):
        data = cur["result"].get("data")
        cur = data[0] if isinstance(data, list) and len(data) == 1 else data
    if isinstance(cur, dict) and "text" in cur:
        t = cur["text"]
        if isinstance(t, str):
            parsed = _first_json(t)
            return parsed if parsed is not None else t
        return t
    return cur


class FabCliApi:
    """``fab api <endpoint> -X <method>`` through the installed, authenticated Fabric CLI."""

    def __init__(self, runner: Runner = run_cli):
        self._run = runner

    def request(self, method: str, path: str, body: Optional[dict] = None) -> ApiResponse:
        argv = ["fab", "api", path.lstrip("/"), "-X", method.lower(), "--output_format", "json"]
        if body is not None:
            argv += ["-i", json.dumps(body)]
        r = self._run(argv, timeout=120, exe_env="ADF2FABRIC_FAB_PATH")
        if not r.found:
            raise FabricApiError("The Fabric CLI is not installed.",
                                 actions=["Install it (`pip install ms-fabric-cli`) and run `fab auth login`."])
        parsed = _first_json(r.stdout)
        if not r.ok and parsed is None:
            raise FabricApiError(f"The Fabric CLI could not complete the request: {r.text[:200]}",
                                 actions=["Run `fab auth status`; sign in with `fab auth login` if needed."])
        status = 200 if r.ok else 400
        if isinstance(parsed, dict) and str(parsed.get("status", "")).lower() == "failure":
            status = int((parsed.get("result") or {}).get("status_code") or 400) if isinstance(parsed.get("result"), dict) else 400
        return ApiResponse(status, unwrap_fab(parsed))


# ---- service ---------------------------------------------------------------------------------------------------


def _error(resp: ApiResponse, what: str) -> FabricApiError:
    code, msg = "", ""
    if isinstance(resp.body, dict):
        code, msg = str(resp.body.get("errorCode", "")), str(resp.body.get("message", ""))
    msg = redact(msg)[:300]
    s = resp.status
    if s == 401:
        return FabricApiError(f"Fabric rejected the sign-in while {what}.", s, code, ["Sign in again."])
    if s == 403:
        return FabricApiError(f"Permission denied while {what}. {msg}".strip(), s, code,
                              ["Ask a workspace admin for the Contributor role, or check the Fabric admin settings that "
                               "allow this account (or service principal) to use the Fabric APIs."])
    if s == 404:
        return FabricApiError(f"Not found while {what}. {msg}".strip(), s, code,
                              ["Check the id, and that this account can see the item."])
    return FabricApiError(f"Fabric returned HTTP {s} while {what}. {code} {msg}".strip(), s, code)


class FabricService:
    def __init__(self, api: FabricApi):
        self.api = api

    def _get(self, path: str, what: str) -> Any:
        resp = self.api.request("GET", path)
        if resp.status != 200:
            raise _error(resp, what)
        return resp.body

    def _paged(self, path: str, what: str) -> list[dict]:
        items: list[dict] = []
        token: Optional[str] = None
        while True:
            body = self._get(path + (("&" if "?" in path else "?") + f"continuationToken={token}" if token else ""), what)
            if isinstance(body, dict):
                items += [x for x in body.get("value", []) if isinstance(x, dict)]
                token = body.get("continuationToken")
            else:
                token = None
            if not token:
                return items

    def workspaces(self, include_personal: bool = False) -> list[FabricWorkspace]:
        out = []
        for w in self._paged("workspaces", "listing workspaces"):
            if w.get("type") == "Personal" and not include_personal:
                continue  # a personal workspace has no capacity and cannot hold pipelines
            out.append(FabricWorkspace(id=str(w.get("id", "")), display_name=str(w.get("displayName", "")), type=w.get("type"),
                                       capacity_id=w.get("capacityId"), capacity_region=w.get("capacityRegion"),
                                       description=w.get("description")))
        return sorted(out, key=lambda w: w.display_name.lower())

    def workspace(self, workspace_id: str) -> dict:
        return self._get(f"workspaces/{workspace_id}", f"reading workspace {workspace_id}") or {}

    def capacities(self) -> list[dict]:
        return self._paged("capacities", "listing capacities")

    def connections(self) -> list[dict]:
        return self._paged("connections", "listing connections")

    def supported_connection_types(self) -> dict[str, list[str]]:
        """connection type -> credential types Fabric accepts for it (live from the tenant, not assumed)."""
        out: dict[str, list[str]] = {}
        for t in self._paged("connections/supportedConnectionTypes", "listing supported connection types"):
            out[str(t.get("type"))] = list(t.get("supportedCredentialTypes") or [])
        return out

    def gateways(self) -> list[dict]:
        return self._paged("gateways", "listing gateways")

    def pipelines(self, workspace_id: str) -> list[dict]:
        return self._paged(f"workspaces/{workspace_id}/dataPipelines", "listing data pipelines")

    def create_connection(self, body: dict) -> dict:
        resp = self.api.request("POST", "connections", body)
        if resp.status not in (200, 201):
            raise _error(resp, "creating the connection")
        return resp.body if isinstance(resp.body, dict) else {}

    def provision_identity(self, workspace_id: str) -> dict:
        """Creates the workspace identity (admin role required); a 202 is polled by the caller via the operation id."""
        resp = self.api.request("POST", f"workspaces/{workspace_id}/provisionIdentity")
        if resp.status not in (200, 202):
            raise _error(resp, "provisioning the workspace identity")
        return resp.body if isinstance(resp.body, dict) else {"accepted": True}

    # ---- validation ----------------------------------------------------------------------------------------------
    def validate_workspace(self, workspace_id: str) -> TestReport:
        rep = TestReport(target=f"Fabric workspace {workspace_id}", state=ConnectionState.validation_failed)
        try:
            ws = self.workspace(workspace_id)
        except FabricApiError as exc:
            rep.error, rep.actions = str(exc), exc.actions
            rep.checks.append(Check(name="workspace read", status=CheckStatus.failed, detail=str(exc)))
            return rep
        rep.verified.append(f"workspace '{ws.get('displayName')}' can be read")
        rep.checks.append(Check(name="workspace read", status=CheckStatus.passed, detail=str(ws.get("displayName"))))
        if ws.get("type") == "Personal":
            rep.error = "A personal workspace has no capacity and cannot hold data pipelines."
            rep.actions = ["Choose (or create) a workspace on a Fabric capacity."]
            rep.checks.append(Check(name="capacity", status=CheckStatus.failed, detail="personal workspace"))
            return rep
        if ws.get("capacityId"):
            rep.verified.append("the workspace is assigned to a capacity")
            rep.checks.append(Check(name="capacity", status=CheckStatus.passed, detail=str(ws["capacityId"])))
        else:
            rep.error = "The workspace is not assigned to a Fabric capacity, so pipelines cannot be created in it."
            rep.actions = ["Assign a trial or paid Fabric capacity in the workspace settings."]
            rep.checks.append(Check(name="capacity", status=CheckStatus.failed))
            return rep
        try:
            n = len(self.pipelines(workspace_id))
            rep.verified.append(f"its data pipelines can be listed ({n} present)")
            rep.checks.append(Check(name="pipelines list", status=CheckStatus.passed, detail=str(n)))
        except FabricApiError as exc:
            rep.error, rep.actions = str(exc), exc.actions
            rep.checks.append(Check(name="pipelines list", status=CheckStatus.failed, detail=str(exc)))
            return rep
        rep.state = ConnectionState.connection_tested
        rep.not_verified += ["permission to CREATE items is not probed (it would create one); a Contributor or higher role is "
                             "required to deploy",
                             "workspace identity availability depends on tenant settings and is checked when you choose it"]
        return rep
