"""Thin HTTP client for the Migration Accelerator API (``python -m adf2fabric.api``).

The UI never talks to agents directly: every page calls one method here. No Streamlit import, so it can be tested on its
own. Secrets (Git token, connection passwords) are passed straight through to the API and never kept on this object.
"""

from __future__ import annotations

from urllib.parse import quote

import os
import time
from typing import Any, Callable, Iterable, Optional

import requests

DEFAULT_URL = "http://127.0.0.1:8765"
TERMINAL = ("succeeded", "failed")


class ApiError(Exception):
    """An error response from the API, unwrapped from its ``{error, actions, details}`` envelope."""

    def __init__(self, status: int, message: str, actions: Optional[list[str]] = None, details: Optional[list[Any]] = None, request_id: str = ""):
        super().__init__(message)
        self.status = status
        self.request_id = request_id  # quote it to an administrator: the server log has the detail, the browser never does
        self.message = message
        self.actions = actions or []
        self.details = details or []


class ApiClient:
    def __init__(self, session_id: str, base_url: Optional[str] = None, token: Optional[str] = None,
                 http: Any = None, timeout: float = 120):
        self.base_url = (base_url or os.environ.get("ADF2FABRIC_API_URL") or DEFAULT_URL).rstrip("/")
        self.session_id = session_id
        self._token = token if token is not None else os.environ.get("ADF2FABRIC_API_TOKEN")
        self._http = http or requests.Session()
        self.timeout = timeout

    # ---- transport -----------------------------------------------------------------------------------------------------
    def _request(self, method: str, path: str, *, json: Any = None, params: Optional[dict] = None,
                 data: Optional[bytes] = None, content_type: Optional[str] = None, idempotency_key: Optional[str] = None) -> Any:
        headers = {"X-Session-Id": self.session_id}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key  # a repeated submission of the same intent returns the first job instead of starting another
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        if content_type:
            headers["Content-Type"] = content_type
        try:
            resp = self._http.request(method, f"{self.base_url}{path}", json=json, params=params, data=data,
                                      headers=headers, timeout=self.timeout)
        except requests.ConnectionError:
            raise ApiError(0, f"Cannot reach the API at {self.base_url}.",
                           ["Start it with: python -m adf2fabric.api"]) from None
        if resp.status_code >= 400:
            try:
                body = resp.json()
            except ValueError:
                body = {}
            raise ApiError(resp.status_code, body.get("error") or f"HTTP {resp.status_code}",
                           body.get("actions"), body.get("details"), str(getattr(resp, "headers", {}).get("x-request-id", "") or ""))
        return resp.json() if resp.content else None

    def _get(self, path: str, **params: Any) -> Any:
        return self._request("GET", path, params={k: v for k, v in params.items() if v is not None} or None)

    def _post(self, path: str, body: Any = None, idempotency_key: Optional[str] = None) -> Any:
        return self._request("POST", path, json=body if body is not None else {}, idempotency_key=idempotency_key)

    def _put(self, path: str, body: Any) -> Any:
        return self._request("PUT", path, json=body)

    # ---- health + identity + mode --------------------------------------------------------------------------------------
    def health(self) -> dict:
        return self._get("/health")

    def me(self) -> dict:
        """Who the API thinks this is, with the permissions that identity holds. 401 when the API needs a token and none (or a wrong one) was sent."""
        return self._get("/me")

    def set_token(self, token: str) -> None:
        """Use this person's own API token for every later call. It lives on this object (one per browser session) and nowhere else."""
        self._token = token.strip() or None

    def clear_token(self) -> None:
        self._token = None

    def get_mode(self) -> str:
        return self._get("/mode")["mode"]

    def set_mode(self, mode: str) -> str:
        return self._post("/mode", {"mode": mode})["mode"]

    def session(self) -> dict:
        return self._get("/session")

    # ---- sign-in -------------------------------------------------------------------------------------------------------
    def auth_status(self, provider: str, refresh: bool = False) -> dict:
        return self._get(f"/auth/{provider}/status", refresh="true" if refresh else None)

    def accounts(self, refresh: bool = False) -> list[dict]:
        """Everyone signed in to the Azure CLI on the API machine. Source and destination each pick one."""
        return self._get("/auth/azure_cli/accounts", refresh="true" if refresh else None)["accounts"]

    def start_login(self, provider: str, *, tenant_id: Optional[str] = None, device_code: bool = False,
                    method: str = "interactive") -> dict:
        body: dict[str, Any] = {"tenant_id": tenant_id or None}
        if provider == "azure_cli":
            body["device_code"] = device_code
        else:
            body["method"] = method
        return self._post(f"/auth/{provider}/login", body)

    def logout(self, provider: str, account: Optional[str] = None) -> dict:
        """Sign out on the API machine; returns the new sign-in status."""
        return self._post(f"/auth/{provider}/logout", {"account": account} if provider == "azure_cli" else {})

    def login_state(self, provider: str, attempt_id: str) -> dict:
        return self._get(f"/auth/{provider}/login/{attempt_id}")

    def tenants(self) -> list[dict]:
        return self._get("/auth/azure_cli/tenants")["tenants"]

    # ---- source --------------------------------------------------------------------------------------------------------
    def subscriptions(self, account: Optional[str] = None, refresh: bool = False) -> list[dict]:
        return self._get("/azure/subscriptions", account=account, refresh="true" if refresh else None)["subscriptions"]

    def discover(self, subscription_id: str) -> dict:
        return self._get("/azure/discover", subscription_id=subscription_id)

    def set_source(self, source: dict) -> dict:
        return self._put("/session/source", source)["source"]

    def test_source(self, source: dict) -> dict:
        return self._post("/test/source", source)

    def upload_zip(self, data: bytes) -> dict:
        return self._request("PUT", "/uploads/zip", data=data, content_type="application/octet-stream")

    def import_project(self, project_name: str, source: dict, pipelines: Iterable[str] = ()) -> dict:
        return self._post("/projects", {"project_name": project_name, "source": source, "pipelines": list(pipelines)})

    # ---- destination ---------------------------------------------------------------------------------------------------
    def set_destination(self, provider: str = "azure_cli", workspace_id: Optional[str] = None,
                        tenant_id: Optional[str] = None, account: Optional[str] = None) -> dict:
        return self._put("/session/destination", {"provider": provider, "workspace_id": workspace_id,
                                                  "tenant_id": tenant_id, "account": account})["destination"]

    def workspaces(self, provider: Optional[str] = None, account: Optional[str] = None) -> list[dict]:
        return self._get("/fabric/workspaces", provider=provider, account=account)["workspaces"]

    def capacities(self, provider: Optional[str] = None) -> list[dict]:
        return self._get("/fabric/capacities", provider=provider)["capacities"]

    def test_destination(self, provider: str, workspace_id: str, tenant_id: Optional[str] = None,
                         account: Optional[str] = None) -> dict:
        return self._post("/test/destination", {"provider": provider, "workspace_id": workspace_id, "tenant_id": tenant_id,
                                                "account": account})

    # ---- projects ------------------------------------------------------------------------------------------------------
    def projects(self) -> list[dict]:
        return self._get("/projects")["projects"]

    def project(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}")

    def handover(self, pid: str) -> str:
        return self._get(f"/projects/{pid}/handover")["markdown"]

    def conversion(self, pid: str) -> dict:
        """The saved conversion's summary (the same shape the convert stage returns)."""
        return self._get(f"/projects/{pid}/conversion")

    def linked_services(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/linked-services")

    # ---- connections ---------------------------------------------------------------------------------------------------
    def connectors(self) -> list[dict]:
        return self._get("/connectors")["connectors"]

    def cards(self, pid: str) -> list[dict]:
        return self._get(f"/projects/{pid}/connections")["cards"]

    def set_choice(self, pid: str, ls: str, method: str, endpoint: Optional[dict] = None, values: Optional[dict] = None,
                   display_name: Optional[str] = None) -> dict:
        return self._put(f"/projects/{pid}/connections/{ls}/choice",
                         {"method": method, "endpoint": endpoint or {}, "values": values or {},
                          "display_name": display_name or None})

    def put_secret(self, pid: str, ls: str, field: str, value: str) -> list[str]:
        """Send one secret to the API's in-memory vault. Returns the field NAMES now held (never values)."""
        return self._put(f"/projects/{pid}/connections/{ls}/secrets", {"field": field, "value": value})["held"]

    def clear_secrets(self, pid: str, ls: Optional[str] = None) -> int:
        return self._request("DELETE", f"/projects/{pid}/connections/secrets", params={"ls": ls} if ls else None)["cleared"]

    def test_connection(self, pid: str, ls: str) -> dict:
        return self._post(f"/projects/{pid}/connections/{ls}/test")

    def apply_connections(self, pid: str, names: Iterable[str] = (), dry_run: bool = True, auto_match: bool = False,
                          workspace_id: Optional[str] = None, confirm: bool = False) -> dict:
        return self._post(f"/projects/{pid}/connections/apply",
                          {"names": list(names), "dry_run": dry_run, "auto_match": auto_match, "workspace_id": workspace_id, "confirm": confirm})

    def provision_workspace_identity(self, pid: str, workspace_id: Optional[str] = None, confirm: bool = False) -> dict:
        return self._post(f"/projects/{pid}/connections/workspace-identity", {"workspace_id": workspace_id, "confirm": confirm})

    def connections_report(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/connections/report")

    # ---- stages + jobs -------------------------------------------------------------------------------------------------
    def run_stage(self, pid: str, stage: str, *, pipelines: Iterable[str] = (), workspace_id: Optional[str] = None,
                  with_children: bool = False, allow_unbound: bool = False, confirm: bool = False, validate_stages: Iterable[str] = (),
                  run: bool = False, run_timeout: Optional[int] = None, lakehouse: Optional[str] = None, apply_reconstruction: Optional[bool] = None,
                  assisted: bool = False, idempotency_key: Optional[str] = None) -> dict:
        body = {"pipelines": list(pipelines), "workspace_id": workspace_id, "with_children": with_children,
                "allow_unbound": allow_unbound, "confirm": confirm, "validate_stages": list(validate_stages), "run": run, "assisted": assisted}
        for k, v in (("run_timeout", run_timeout), ("lakehouse", lakehouse), ("apply_reconstruction", apply_reconstruction)):
            if v is not None:  # left out = follow the approved brief
                body[k] = v
        return self._post(f"/projects/{pid}/stages/{stage}", body, idempotency_key)

    # ---- model-assisted drafts (optional) ------------------------------------------------------------------------------
    def assist(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/assist")

    def assist_detail(self, pid: str, name: str) -> dict:
        return self._get(f"/projects/{pid}/assist/{quote(name, safe='')}")

    def assist_prompt(self, pid: str, name: str) -> dict:
        return self._get(f"/projects/{pid}/assist/{quote(name, safe='')}/prompt")

    def assist_generate(self, pid: str, names: Iterable[str]) -> dict:
        return self._post(f"/projects/{pid}/assist/generate", {"names": list(names)})

    def assist_accept(self, pid: str, name: str, code: Optional[str] = None) -> dict:
        return self._post(f"/projects/{pid}/assist/{quote(name, safe='')}/accept", {"code": code} if code is not None else {})

    def assist_discard(self, pid: str, name: str, scope: str = "draft") -> dict:
        return self._post(f"/projects/{pid}/assist/{quote(name, safe='')}/discard", {"scope": scope})

    # ---- views built and redacted on the server ------------------------------------------------------------------------
    def inventory(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/inventory")

    def waves(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/waves")

    def compatibility(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/compatibility")

    def validation(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/validation")

    def report(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/report")

    def journey(self, pid: str) -> list[str]:
        return self._get(f"/projects/{pid}/journey")["confirmed"]

    def journey_confirm(self, pid: str, steps: Iterable[str]) -> list[str]:
        return self._put(f"/projects/{pid}/journey", {"steps": list(steps)})["confirmed"]

    def journey_reset(self, pid: str, steps: Iterable[str]) -> list[str]:
        return self._post(f"/projects/{pid}/journey/reset", {"steps": list(steps)})["confirmed"]

    # ---- consultation brief -------------------------------------------------------------------------------------------
    def brief(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/brief")

    def brief_answers(self, pid: str, answers: dict) -> dict:
        return self._put(f"/projects/{pid}/brief/answers", {"answers": answers})

    def brief_accept(self, pid: str) -> dict:
        return self._post(f"/projects/{pid}/brief/accept")

    def brief_submit(self, pid: str) -> dict:
        return self._post(f"/projects/{pid}/brief/submit")

    def brief_approve(self, pid: str, fingerprint: str) -> dict:
        return self._post(f"/projects/{pid}/brief/approve", {"fingerprint": fingerprint})

    def brief_import(self, pid: str, questionnaire: dict) -> dict:
        return self._post(f"/projects/{pid}/brief/import", {"questionnaire": questionnaire})

    def brief_note(self, pid: str, text: str) -> dict:
        return self._post(f"/projects/{pid}/brief/notes", {"text": text})

    def brief_markdown(self, pid: str) -> str:
        return self._get(f"/projects/{pid}/brief/markdown")["markdown"]

    def brief_questionnaire(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/brief/questionnaire")

    def provisioning(self, pid: str) -> dict:
        return self._get(f"/projects/{pid}/provisioning")

    def plan_provisioning(self, pid: str, *, lakehouses: Iterable[str] = (), warehouses: Iterable[str] = (), provision_identity: bool = False,
                          workspace_id: Optional[str] = None, idempotency_key: Optional[str] = None) -> dict:
        return self._post(f"/projects/{pid}/provisioning/plan", {"workspace_id": workspace_id, "lakehouses": list(lakehouses), "warehouses": list(warehouses),
                                                                  "provision_identity": provision_identity}, idempotency_key)

    def approve_provisioning(self, pid: str, plan_ref: str, steps: Optional[Iterable[str]] = None, note: str = "") -> dict:
        return self._post(f"/projects/{pid}/provisioning/approve", {"plan_ref": plan_ref, "steps": list(steps) if steps is not None else None, "note": note})

    def apply_provisioning(self, pid: str, plan_ref: str, *, apply: bool = False, confirm: bool = False, idempotency_key: Optional[str] = None) -> dict:
        return self._post(f"/projects/{pid}/provisioning/apply", {"plan_ref": plan_ref, "apply": apply, "confirm": confirm}, idempotency_key)

    def verify_provisioning(self, pid: str) -> dict:
        return self._post(f"/projects/{pid}/provisioning/verify")

    def jobs(self, project_id: Optional[str] = None) -> list[dict]:
        return self._get("/jobs", project_id=project_id)["jobs"]

    def job(self, job_id: str, since: int = 0) -> dict:
        return self._get(f"/jobs/{job_id}", since=since)

    def wait_job(self, job: dict, on_event: Optional[Callable[[dict], None]] = None, interval: float = 1.0,
                 timeout: float = 1800) -> dict:
        """Poll a job until it finishes, passing each new log event to ``on_event``. Returns the final job."""
        since, end = 0, time.time() + timeout
        while True:
            current = self.job(job["id"], since)
            for event in current.get("events", []):
                if on_event:
                    on_event(event)
            since = current.get("next_event", since)
            if current["state"] in TERMINAL:
                return current
            if time.time() > end:
                raise ApiError(0, f"The '{job.get('kind')}' job is still running after {int(timeout)} s.",
                               ["Check the Jobs page later."])
            time.sleep(interval)
