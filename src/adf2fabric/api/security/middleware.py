"""Request-level protections that apply before any route runs.

* Host header allow-list (DNS-rebinding defence: a page on ``evil.example`` that resolves to 127.0.0.1 sends ``Host: evil.example``).
* Origin check on state-changing requests (CSRF defence for browsers; an ``Origin`` that is not allowed is refused).
* Content-Type: every state-changing request must declare ``application/json`` (the only body type the API takes) - a browser cannot
  send that cross-site without a CORS preflight, which the CORS policy denies. The one exception is the raw archive upload, a PUT
  (always preflighted) with its own size cap.
* Body size cap for JSON bodies, and a refusal of undeclared (chunked) JSON bodies.
* Security response headers, a request id on every response, and a generic per-client request limit.

Authentication uses an ``Authorization`` header (never a cookie), so a browser does not attach credentials on its own and classic
CSRF cannot ride them; the checks above also protect the local no-authentication mode, where no credential exists to protect it.
"""

from __future__ import annotations

import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from adf2fabric.api.security.audit import AuditLog, addr_hash
from adf2fabric.api.security.config import SecuritySettings
from adf2fabric.api.security.ratelimit import RateLimiter

UNSAFE = ("POST", "PUT", "PATCH", "DELETE")
UPLOAD_PATH = "/uploads/zip"
HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
    "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
    "Cross-Origin-Resource-Policy": "same-site",
}


def _deny(status: int, message: str, rid: str, headers: dict | None = None) -> JSONResponse:
    r = JSONResponse(status_code=status, content={"error": message, "actions": []}, headers={**HEADERS, "X-Request-Id": rid, **(headers or {})})
    return r


class SecurityMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, sec: SecuritySettings, limiter: RateLimiter, audit: AuditLog):
        super().__init__(app)
        self.sec, self.limiter, self.audit = sec, limiter, audit
        self.hosts = {h.lower() for h in sec.allowed_hosts}
        self.origins = {o.lower().rstrip("/") for o in sec.allowed_origins}

    def _host_ok(self, host: str) -> bool:
        host = host.lower()
        bare = host.rsplit(":", 1)[0] if (host.count(":") == 1 or host.startswith("[") and "]:" in host) else host
        return host in self.hosts or bare in self.hosts

    async def dispatch(self, request: Request, call_next):
        rid = uuid.uuid4().hex[:12]
        request.state.request_id = rid
        client = addr_hash(request.client.host if request.client else None)
        request.state.client = client
        if not self._host_ok(request.headers.get("host", "")):
            self.audit.event("request.rejected", "denied", request_id=rid, reason="host", client=client)
            return _deny(400, "This host name is not accepted.", rid)
        method = request.method.upper()
        if method in UNSAFE:
            origin = request.headers.get("origin")
            if origin is not None and origin.lower().rstrip("/") not in self.origins:
                self.audit.event("request.rejected", "denied", request_id=rid, reason="origin", client=client, path=request.url.path)
                return _deny(403, "This origin is not allowed to make changes.", rid)
            is_upload = method == "PUT" and request.url.path == UPLOAD_PATH
            ctype = request.headers.get("content-type", "").split(";")[0].strip().lower()
            if not is_upload and ctype != "application/json":
                return _deny(415, "Send application/json.", rid)
            if not is_upload:
                length = request.headers.get("content-length")
                if length is None and request.headers.get("transfer-encoding"):
                    return _deny(411, "A Content-Length is required.", rid)
                if length is not None and (not length.isdigit() or int(length) > self.sec.max_json_bytes):
                    return _deny(413, "The request body is too large.", rid)
        wait = self.limiter.check(f"ip:{client}", "global", self.sec.rate_per_min * 4)  # a loose ceiling per client; per-principal limits are stricter
        if wait is not None:
            return _deny(429, "Too many requests.", rid, {"Retry-After": str(int(wait))})
        response = await call_next(request)
        for k, v in HEADERS.items():
            response.headers.setdefault(k, v)
        response.headers["X-Request-Id"] = rid
        return response
