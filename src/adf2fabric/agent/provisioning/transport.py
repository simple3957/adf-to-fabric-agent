"""The two transports provisioning uses, kept separate because no single API provisions everything.

* ``FabricGateway``: Fabric REST (``cloud.FABRIC_API``). Workspaces, capacity assignment, identity, role assignments, items, pools.
* ``ArmGateway``: Azure Resource Manager (``cloud.ARM_ENDPOINT``). Only Microsoft.Fabric capacities (and read-only subscription checks).

Each is given a token PROVIDER (a callable), never a token string kept on the object, and each pins its host: a bearer token can
only be sent to its own service. Both retry what is safe to retry (429 with ``Retry-After``, 5xx, a dropped connection) with
bounded exponential backoff, and report everything else immediately. A retry of a CREATE is never blind: the executor
re-reads state first (see ``executor``), because a 5xx or a timeout can mean the resource was created.

Errors never carry a token, header or response body beyond a redacted, truncated message.
"""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from adf2fabric import cloud
from adf2fabric.connections.redaction import redact

FABRIC_API = cloud.FABRIC_API
ARM = cloud.ARM_ENDPOINT
_RETRIABLE = {500, 502, 503, 504}


class ProvisionError(Exception):
    """A call failed. ``ambiguous`` means the request may have taken effect (5xx, timeout, dropped connection)."""

    def __init__(self, message: str, status: Optional[int] = None, code: str = "", ambiguous: bool = False):
        super().__init__(message)
        self.status, self.code, self.ambiguous = status, code, ambiguous


@dataclass
class Response:
    status: int
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    def error_code(self) -> str:
        return str(self.body.get("errorCode") or (self.body.get("error") or {}).get("code") or "") if isinstance(self.body, dict) else ""

    def message(self) -> str:
        if not isinstance(self.body, dict):
            return ""
        err = self.body.get("error") if isinstance(self.body.get("error"), dict) else {}
        return redact(str(self.body.get("message") or err.get("message") or ""))[:300]


def _lower(headers: Any) -> dict[str, str]:
    return {str(k).lower(): str(v) for k, v in dict(headers or {}).items()}


class _Gateway:
    base = ""

    def __init__(self, token_provider: Callable[[], str], session: Any = None, sleep: Callable[[float], None] = time.sleep,
                 max_attempts: int = 4, backoff: float = 2.0):
        self._token, self._session, self._sleep = token_provider, session, sleep
        self.max_attempts, self.backoff = max_attempts, backoff
        self.calls: list[tuple[str, str]] = []  # (method, path) of every request sent, for the audit trail; no bodies

    def _url(self, path: str) -> str:
        url = path if path.startswith("http") else f"{self.base}/{path.lstrip('/')}"
        if not url.startswith(self.base + "/") and url != self.base:
            raise ProvisionError("Refusing to send a credential to a host other than this service.")
        return url

    def request(self, method: str, path: str, body: Optional[dict] = None, *, ambiguous_retry: bool = True) -> Response:
        """One request. ``ambiguous_retry=False`` (used for every write) retries throttling only: a 5xx or a dropped connection
        may mean the write took effect, so it is raised as ``ambiguous`` and the caller re-reads state before deciding."""
        url = self._url(path)
        if self._session is None:
            import requests

            self._session = requests.Session()
        attempts = self.max_attempts
        last: Optional[ProvisionError] = None
        for attempt in range(attempts):
            token = self._token()
            self.calls.append((method, url.split("?", 1)[0].replace(self.base, "")))
            try:
                resp = self._session.request(method, url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                                             json=body, timeout=60)
            except Exception as exc:  # noqa: BLE001 - no message that could hold a token
                last = ProvisionError(f"The service could not be reached ({type(exc).__name__}).", None, "", ambiguous=True)
                if not ambiguous_retry:
                    raise last from None
            else:
                headers = _lower(resp.headers)
                try:
                    payload = resp.json() if resp.content else None
                except ValueError:
                    payload = None
                out = Response(resp.status_code, payload, headers)
                if resp.status_code == 429:  # throttled: wait as told, then try again (this never takes effect server-side)
                    last = ProvisionError("The service is throttling requests (HTTP 429).", 429, out.error_code())
                    self._sleep(min(float(headers.get("retry-after", self.backoff * (attempt + 1))), 120))
                    continue
                if resp.status_code in _RETRIABLE:
                    last = ProvisionError(f"The service returned HTTP {resp.status_code}.", resp.status_code, out.error_code(), ambiguous=True)
                    if not ambiguous_retry:
                        raise last
                else:
                    return out
            if attempt + 1 < attempts:
                self._sleep(self.backoff * (2 ** attempt))
        assert last is not None
        raise last

    def paged(self, path: str, what: str, key: str = "value") -> list[dict]:
        out: list[dict] = []
        token: Optional[str] = None
        while True:
            sep = "&" if "?" in path else "?"
            resp = self.request("GET", path + (f"{sep}continuationToken={token}" if token else ""))
            if resp.status != 200:
                raise self.error(resp, what)
            body = resp.body if isinstance(resp.body, dict) else {}
            out += [x for x in body.get(key, []) if isinstance(x, dict)]
            token = body.get("continuationToken")
            if not token:
                return out

    @staticmethod
    def error(resp: Response, what: str) -> ProvisionError:
        s = resp.status
        msg = {401: "the sign-in was rejected", 403: "permission denied", 404: "not found"}.get(s, f"HTTP {s}")
        detail = f" {resp.error_code()}" if resp.error_code() else ""
        text = f" {resp.message()}" if resp.message() else ""
        return ProvisionError(f"{what}: {msg}.{detail}{text}".strip(), s, resp.error_code(), ambiguous=s in _RETRIABLE)


class FabricGateway(_Gateway):
    base = FABRIC_API

    def wait_operation(self, resp: Response, what: str, max_polls: int = 120) -> Optional[dict]:
        """Follow a 202 long-running operation to its end. The operation URL is built from the id, never taken from a header."""
        op_id = resp.headers.get("x-ms-operation-id") or resp.headers.get("location", "").split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
        if not op_id:
            raise ProvisionError(f"{what} was accepted but no operation id was returned.", 202, ambiguous=True)
        wait = float(resp.headers.get("retry-after", 5))
        for _ in range(max_polls):
            self._sleep(min(wait, 60))
            st = self.request("GET", f"operations/{op_id}")
            if st.status != 200:
                raise self.error(st, f"Reading the status of {what}")
            status = str((st.body or {}).get("status", "")).lower()
            if status == "succeeded":
                return st.body
            if status == "failed":
                err = (st.body or {}).get("error") or {}
                raise ProvisionError(f"{what} failed: {err.get('errorCode', '')} {redact(str(err.get('message', '')))[:200]}".strip(), None,
                                     str(err.get("errorCode", "")))
            wait = float(st.headers.get("retry-after", wait))
        raise ProvisionError(f"{what} did not finish in time; check the workspace before retrying.", None, "", ambiguous=True)


class ArmGateway(_Gateway):
    base = ARM


def token_tenant(token: str) -> Optional[str]:
    """The ``tid`` claim of a JWT, read without verifying it (to tell the user which tenant the token is for). Nothing else is kept."""
    try:
        part = token.split(".")[1]
        data = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
        tid = data.get("tid")
        return str(tid) if tid else None
    except Exception:  # noqa: BLE001
        return None
