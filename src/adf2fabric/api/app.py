"""The FastAPI application. HTTP concerns only: every decision lives in the services it calls.

Security posture (details and limits in docs/security.md):

* Authentication: bearer tokens of named users (hash-only store), or the historical local mode (one implicit user, loopback only).
* Authorisation: every route declares the permission it needs and the server enforces it; projects, jobs, uploads and sessions
  belong to an organisation and are checked on every access (another organisation's project is "not found").
* A client may only point the server at subscriptions, tenants, accounts and workspaces its grants name; Git URLs are checked for
  SSRF; identifiers that become paths are validated.
* Request protections (Host / Origin / Content-Type / size / rate) run before routes (``security.middleware``); security headers on
  every response; validation errors never echo the submitted input; unexpected errors return a generic message with no path or trace.
* Sensitive actions are audited (who, what, outcome; never secrets or bodies). Secrets arrive as ``SecretStr``, go to the in-memory
  vault and are never returned.
"""

import hmac
import json
import re
import shutil
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from adf2fabric.agent.intake import IntakeAgent
from adf2fabric.agent.intake.git_source import GitIntakeRequest, GitSourceIntake
from adf2fabric.agent.intake.models import AzureIntakeRequest
from adf2fabric.agent.intake.zip_source import ZipIntakeRequest, ZipSourceIntake, inspect_zip
from adf2fabric.api import schemas as S
from adf2fabric.api import views
from adf2fabric.agent.consultation import brief_record
from adf2fabric.agent.deployment import record as deploy_record
from adf2fabric.api.container import Container, Session, Settings
from adf2fabric.api.jobs import JobConflict
from adf2fabric.api.security import AuditLog, Grants, LOCAL, Permission as P, Principal, ProjectAccess, SecuritySettings, UserError, UserStore
from adf2fabric.api.security import guards
from adf2fabric.api.security.audit import addr_hash
from adf2fabric.api.security.config import LOOPBACK_HOSTS
from adf2fabric.api.security.middleware import SecurityMiddleware
from adf2fabric.api.security.permissions import ROLES, permissions_for, valid_role
from adf2fabric.api.security.principals import WILDCARD, make_principal
from adf2fabric.api.security.ratelimit import RateLimiter
from adf2fabric.api.stages import STAGES, StageError, run_stage
from adf2fabric.connections import catalog
from adf2fabric.connections.fabric import FabricApiError
from adf2fabric.connections.providers.azure_cli import ProviderError
from adf2fabric.connections.providers.git_source import GitAuth, GitSourceError
from adf2fabric.connections.providers.zip_source import ZipRejected
from adf2fabric.connections.redaction import redact
from adf2fabric.connections.resolution import ConnectionChoice
from adf2fabric.connections.service import SCOPE
from adf2fabric.project.locks import lock_for, write_atomic
from adf2fabric.project.store import ProjectNotFoundError, slugify

SESSION_HEADER = "x-session-id"
_ID = guards.ID
_ZIP_MAGIC = (b"PK\x03\x04", b"PK\x05\x06")
_FIELD = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_LS = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.\-]{0,127}$")
MAX_PENDING_UPLOADS = 5

# the permission each migration stage needs (the stage route re-checks against this, on top of its baseline)
STAGE_PERMISSION = {"discovery": P.discovery_run, "semantic": P.discovery_run, "dependencies": P.discovery_run, "reconstruct": P.discovery_run,
                    "workloads": P.plans_generate, "convert": P.plans_generate, "compute": P.plans_generate, "validate": P.plans_generate,
                    "deploy_dry_run": P.plans_generate, "verify": P.plans_generate, "deploy_apply": P.migration_execute}
_IDEM = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
IDEMPOTENCY_TTL = 900.0


def create_app(settings: Optional[Settings] = None, live: Optional[Container] = None, demo: Optional[Container] = None,
               security: Optional[SecuritySettings] = None) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(title="ADF to Fabric Migration Accelerator API", version="0.1.0", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.containers = {"live": live or Container(settings), "demo": demo}
    app.state.modes = {}  # scoped session key -> "live" | "demo"
    app.state.idem_lock = threading.RLock()  # route handlers run on worker threads: check, submit and remember a key as one step
    app.state.idem = {}  # (scoped session key, Idempotency-Key) -> (time, response body): a repeated submission returns the first result
    root = app.state.containers["live"].store.projects_root
    sec = security or SecuritySettings(root=root)
    users, audit, limiter = UserStore(root), AuditLog(root), RateLimiter()
    app.state.security, app.state.users, app.state.audit, app.state.limiter = sec, users, audit, limiter
    path_roots = [root, Path(tempfile.gettempdir())]

    app.add_middleware(CORSMiddleware, allow_origins=sec.allowed_origins or settings.allowed_origins, allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
                       allow_headers=["Authorization", "Content-Type", "X-Session-Id", "Idempotency-Key"], allow_credentials=False)
    app.add_middleware(SecurityMiddleware, sec=sec, limiter=limiter, audit=audit)

    def scrubbed(message: str) -> str:
        return guards.scrub(redact(message), path_roots)

    def _err(status: int, message: str, actions: Optional[list[str]] = None, headers: Optional[dict] = None) -> JSONResponse:
        return JSONResponse(status_code=status, content={"error": scrubbed(message), "actions": [scrubbed(a) for a in (actions or [])]}, headers=headers)

    # ---- errors: never echo input, never leak internals -----------------------------------------------------------------
    @app.exception_handler(RequestValidationError)
    async def _validation(_r: Request, exc: RequestValidationError):
        problems = [{"field": ".".join(str(p) for p in e.get("loc", ())), "problem": e.get("msg", "invalid")} for e in exc.errors()]
        return JSONResponse(status_code=422, content={"error": "The request is not valid.", "details": problems, "actions": []})

    @app.exception_handler(HTTPException)
    async def _http(_r: Request, exc: HTTPException):
        return _err(exc.status_code, str(exc.detail), headers=getattr(exc, "headers", None))

    @app.exception_handler(guards.Rejected)
    async def _rejected(_r: Request, exc: guards.Rejected):
        return _err(422, str(exc))

    @app.exception_handler(StageError)
    async def _stage(_r: Request, exc: StageError):
        return _err(400, str(exc), exc.actions)

    @app.exception_handler(catalog.ChoiceError)
    async def _choice(_r: Request, exc: catalog.ChoiceError):
        return JSONResponse(status_code=422, content={"error": "The choice is not valid.",
                                                      "details": [scrubbed(str(p)) for p in getattr(exc, "problems", [str(exc)])], "actions": []})

    @app.exception_handler(ProviderError)
    async def _provider(_r: Request, exc: ProviderError):
        return _err(502, str(exc), getattr(exc, "actions", []))

    @app.exception_handler(GitSourceError)
    async def _git(_r: Request, exc: GitSourceError):
        return _err(400, str(exc), getattr(exc, "actions", []))

    @app.exception_handler(FabricApiError)
    async def _fabric(_r: Request, exc: FabricApiError):
        return _err(502, str(exc))

    @app.exception_handler(ProjectNotFoundError)
    async def _missing(_r: Request, exc: ProjectNotFoundError):
        return _err(409, "That project has not been through the required earlier stage.",
                    ["Run the 'discovery' then 'semantic' stages first."])

    @app.exception_handler(JobConflict)
    async def _conflict(_r: Request, exc: JobConflict):
        return _err(409, str(exc))

    @app.exception_handler(UserError)
    async def _user(_r: Request, exc: UserError):
        return _err(422, str(exc))

    @app.exception_handler(ZipRejected)
    async def _zip(_r: Request, exc: ZipRejected):
        return _err(422, f"The archive was rejected: {exc}", ["Export the source again as a plain ZIP of the ADF folders."])

    @app.exception_handler(Exception)
    async def _any(_r: Request, exc: Exception):  # noqa: BLE001
        return _err(500, f"Unexpected {type(exc).__name__}; see the server log.")

    # ---- authentication: who is calling ---------------------------------------------------------------------------------
    def _mode() -> str:
        return sec.effective_mode(users.has_users())

    def _client(request: Request) -> str:
        return getattr(request.state, "client", None) or addr_hash(request.client.host if request.client else None)

    def _rid(request: Request) -> str:
        return getattr(request.state, "request_id", "")

    def _principal(request: Request) -> Principal:
        cached = getattr(request.state, "principal", None)
        if cached is not None:
            return cached
        if _mode() == "disabled":
            request.state.principal = LOCAL
            return LOCAL
        client = _client(request)
        given = request.headers.get("authorization", "")
        token = given[7:].strip() if given.lower().startswith("bearer ") else ""
        principal: Optional[Principal] = None
        if token:
            try:  # per-user tokens are random 160-bit values: a guess cannot succeed, so a valid one is never held back by the address lockout
                principal = users.authenticate(token)
            except UserError:  # a damaged user store locks everyone out; it never lets anyone in
                audit.event("auth.unavailable", "failed", request_id=_rid(request))
                raise HTTPException(503, "Authentication is temporarily unavailable.") from None
        if principal is None:
            # Everything that CAN be guessed is behind the lockout: the static legacy token (which may be weak), and every bad or missing token.
            # Failures from one address lock out that address's guesses, never the real users who share it (a proxy, an office NAT).
            wait = limiter.blocked(f"ip:{client}", "authfail", sec.auth_fail_limit, sec.auth_fail_window)
            if wait is not None:
                audit.event("auth.locked_out", "denied", request_id=_rid(request), client=client)
                raise HTTPException(429, "Too many failed sign-ins. Try again later.", headers={"Retry-After": str(int(wait))})
            legacy = sec.legacy_token
            if token and legacy and hmac.compare_digest(token, legacy):
                principal = make_principal("legacy-token", sec.legacy_org, ["admin"],
                                           Grants(subscriptions=[WILDCARD], tenants=[WILDCARD], accounts=[WILDCARD], workspaces=[WILDCARD]), "legacy_token")
                limiter.clear(f"ip:{client}", "authfail")
        if principal is None:
            limiter.record(f"ip:{client}", "authfail")
            audit.event("auth.failure", "denied", request_id=_rid(request), client=client, reason="missing" if not token else "invalid")
            raise HTTPException(401, "A valid bearer token is required.", headers={"WWW-Authenticate": "Bearer"})
        request.state.principal = principal
        return principal

    SENSITIVE = {P.sources_import, P.migration_execute, P.provision_approve, P.credentials_manage, P.users_manage}

    def need(*perms: P):
        """A route dependency: authenticate, then require EVERY listed permission, then apply the per-principal rate limit."""
        def dep(request: Request) -> Principal:
            p = _principal(request)
            missing = [x for x in perms if not p.can(x)]
            if missing:
                audit.event("authz.denied", "denied", actor=p.id, org=p.org, request_id=_rid(request), path=request.url.path, method=request.method,
                            needs=",".join(x.value for x in missing))
                raise HTTPException(403, "Your role does not allow this action.")
            bucket, limit = ("sensitive", sec.sensitive_per_min) if (set(perms) & SENSITIVE and request.method != "GET") else ("default", sec.rate_per_min)
            wait = limiter.check(f"{p.org}:{p.id}", bucket, limit)
            if wait is not None:
                audit.event("rate.limited", "denied", actor=p.id, org=p.org, request_id=_rid(request), bucket=bucket)
                raise HTTPException(429, "Too many requests.", headers={"Retry-After": str(int(wait))})
            return p
        return Depends(dep)

    class Ctx:
        def __init__(self, request: Request):
            self.request = request
            self.principal: Principal = _principal(request)
            sid = request.headers.get(SESSION_HEADER, "default")
            if not _ID.match(sid):
                raise HTTPException(400, "X-Session-Id must be 1-64 letters, digits, '-' or '_'.")
            self.sid = sid  # what the client sent (shown back to it)
            self.key = f"{self.principal.org}|{self.principal.id}|{sid}"  # what the server stores under: sessions never cross principals
            mode = "live" if sec.is_production else app.state.modes.get(self.key, "live")
            c = app.state.containers.get(mode)
            if c is None:
                from adf2fabric.api.demo import build_demo_container

                c = app.state.containers[mode] = build_demo_container(settings)
            self.c: Container = c
            self.mode = mode
            self.session: Session = c.session(self.key)
            self.access = ProjectAccess(c.store, sec.legacy_org)

        def project(self, pid: str) -> str:
            """The project's id if it exists AND the caller's organisation owns it. Otherwise 404, identical either way."""
            try:
                slug = slugify(pid)
            except ValueError:
                raise HTTPException(404, f"Project '{guards.scrub(pid[:40])}' does not exist.") from None
            if not self.c.store.exists(slug) or not self.access.allowed(self.principal, slug):
                raise HTTPException(404, f"Project '{guards.scrub(pid[:40])}' does not exist.")
            return slug

        def require(self, perm: P) -> None:
            if not self.principal.can(perm):
                audit.event("authz.denied", "denied", actor=self.principal.id, org=self.principal.org, request_id=_rid(self.request), needs=perm.value,
                            path=self.request.url.path)
                raise HTTPException(403, "Your role does not allow this action.")

        def grant(self, kind: str, value: Optional[str], what: str) -> str:
            """The value if this principal may target it. A client-chosen subscription, tenant, account or workspace is never trusted."""
            if not value or not self.principal.granted(kind, value):
                audit.event("grant.denied", "denied", actor=self.principal.id, org=self.principal.org, request_id=_rid(self.request), kind=kind)
                raise HTTPException(403, f"That {what} is not granted to you.")
            return value

        def audit(self, action: str, outcome: str = "ok", resource: str = "", resource_id: str = "", **detail: Any) -> None:
            audit.event(action, outcome, actor=self.principal.id, org=self.principal.org, resource=resource, resource_id=resource_id,
                        request_id=_rid(self.request), **detail)

    def _guid(value: Optional[str], what: str) -> Optional[str]:
        return guards.valid_guid(value, what) if value else value

    # ---- health + identity + mode ---------------------------------------------------------------------------------------
    @app.get("/health")
    def health():
        return {"status": "ok", "version": app.version}

    @app.get("/me")
    def me(request: Request):
        p = _principal(request)
        return {**p.public(), "auth_mode": _mode(), "production": sec.is_production, "demo_available": not sec.is_production}

    @app.get("/mode")
    def get_mode(ctx: Ctx = Depends(), _: Principal = need(P.projects_view)):
        return {"mode": ctx.mode, "modes": ["live", "demo"]}

    @app.post("/mode")
    def set_mode(body: S.ModeBody, request: Request, _: Principal = need(P.projects_view)):
        if body.mode not in ("live", "demo"):
            raise HTTPException(422, "mode must be 'live' or 'demo'.")
        if body.mode == "demo" and sec.is_production:
            audit.event("mode.demo_refused", "denied", actor=_.id, org=_.org, request_id=_rid(request))
            raise HTTPException(403, "Demo data is not available in production mode.")
        ctx = Ctx(request)
        app.state.modes[ctx.key] = body.mode
        return {"mode": body.mode}

    # ---- sign-in to the server's Azure / Fabric CLI (machine-wide state) ------------------------------------------------
    def _provider_for(ctx: Ctx, which: str):
        if which == "azure_cli":
            return ctx.c.azure
        if which == "fabric_cli":
            return ctx.c.fabric_cli
        raise HTTPException(404, "Unknown provider. Use azure_cli or fabric_cli.")

    @app.get("/auth/{provider}/status")
    def auth_status(provider: str, refresh: bool = False, ctx: Ctx = Depends(), _: Principal = need(P.azure_use)):
        p = _provider_for(ctx, provider)
        return S.dump(p.status(refresh=refresh) if provider == "azure_cli" else p.status())

    @app.post("/auth/{provider}/login")
    def auth_login(provider: str, body: S.LoginBody, ctx: Ctx = Depends(), _: Principal = need(P.credentials_manage)):
        p = _provider_for(ctx, provider)
        if body.tenant_id:
            guards.valid_guid(body.tenant_id, "tenant id")
            ctx.grant("tenants", body.tenant_id, "tenant")
        if body.certificate_path is not None:
            raise HTTPException(422, "A certificate path cannot be supplied through the API; configure the Fabric CLI on the server instead.")
        if body.client_id:
            guards.valid_guid(body.client_id, "client id")
        ctx.audit("credentials.login", resource="provider", resource_id=provider)
        try:
            if provider == "azure_cli":
                attempt = p.start_login(tenant=body.tenant_id, device_code=body.device_code)
            else:
                attempt = p.start_login(body.method, tenant=body.tenant_id, client_id=body.client_id, certificate_path=None)
        except RuntimeError as exc:  # a sign-in is already running: a conflict, not a server fault
            return _err(409, str(exc), ["Finish the sign-in already open in the browser, or wait up to 5 minutes "
                                        "for it to time out, then try again."])
        except ValueError as exc:  # e.g. an unknown fab sign-in method
            return _err(422, str(exc))
        return attempt.public()

    @app.post("/auth/{provider}/logout")
    def auth_logout(provider: str, body: S.LogoutBody, ctx: Ctx = Depends(), _: Principal = need(P.credentials_manage)):
        p = _provider_for(ctx, provider)
        if body.account:
            ctx.grant("accounts", body.account, "account")
        ctx.audit("credentials.logout", resource="provider", resource_id=provider)
        try:
            st = p.logout(body.account) if provider == "azure_cli" else p.logout()
        except ValueError as exc:
            return _err(422, str(exc))
        return S.dump(st)

    @app.get("/auth/{provider}/login/{attempt_id}")
    def auth_login_state(provider: str, attempt_id: str, ctx: Ctx = Depends(), _: Principal = need(P.credentials_manage)):
        guards.valid_id(attempt_id, "attempt id")
        a = _provider_for(ctx, provider).logins.get(attempt_id)
        if a is None:
            raise HTTPException(404, "No such sign-in attempt.")
        return a.public()

    def _granted_only(items: list, kind: str, key, ctx: Ctx) -> list:
        return [i for i in items if ctx.principal.granted(kind, key(i))]

    @app.get("/auth/azure_cli/accounts")
    def accounts(refresh: bool = False, ctx: Ctx = Depends(), _: Principal = need(P.azure_use)):
        """The Azure CLI accounts signed in on this machine that THIS principal is granted. Others are not listed."""
        try:
            found = S.dump(ctx.c.azure.accounts(refresh=refresh))
        except ProviderError as exc:
            return _err(502, str(exc), exc.actions)
        names = lambda a: (a.get("user") or a.get("name") or a.get("username") or "") if isinstance(a, dict) else str(a)  # noqa: E731
        return {"accounts": _granted_only(found, "accounts", names, ctx)}

    @app.get("/auth/azure_cli/tenants")
    def tenants(ctx: Ctx = Depends(), _: Principal = need(P.azure_use)):
        found = ctx.c.azure.tenants()
        return {"tenants": [t for t in found if ctx.principal.granted("tenants", (t.get("id") or t.get("tenantId") or "") if isinstance(t, dict) else str(t))]}

    @app.get("/session")
    def session(ctx: Ctx = Depends(), _: Principal = need(P.projects_view)):
        return {"id": ctx.sid, "mode": ctx.mode, "source": ctx.session.source, "destination": ctx.session.destination, "tested": ctx.session.tested}

    # ---- source --------------------------------------------------------------------------------------------------------
    @app.get("/azure/subscriptions")
    def subscriptions(account: Optional[str] = None, refresh: bool = False, ctx: Ctx = Depends(), _: Principal = need(P.azure_use)):
        if account:
            ctx.grant("accounts", account, "account")
        found = S.dump(ctx.c.azure.subscriptions(account=account, refresh=refresh))
        sid = lambda s: (s.get("id") or s.get("subscription_id") or "") if isinstance(s, dict) else str(s)  # noqa: E731
        return {"subscriptions": _granted_only(found, "subscriptions", sid, ctx)}

    @app.get("/azure/discover")
    def azure_discover(subscription_id: str = Query(...), ctx: Ctx = Depends(), _: Principal = need(P.discovery_run, P.azure_use)):
        guards.valid_guid(subscription_id, "subscription id")
        ctx.grant("subscriptions", subscription_id, "subscription")
        ctx.audit("azure.discover", resource="subscription", resource_id=subscription_id)
        found = ctx.c.azure.discover(subscription_id)
        return {k: S.dump(v) for k, v in found.items()}

    def _source_checks(ctx: Ctx, src: S.SourceSelection) -> None:
        """Validate and authorise a client-chosen source before the server does anything with it."""
        if src.kind == "azure":
            if src.subscription_id:
                guards.valid_guid(src.subscription_id, "subscription id")
                ctx.grant("subscriptions", src.subscription_id, "subscription")
            for v, what in ((src.resource_group, "resource group"), (src.factory_name, "factory name")):
                if v:
                    guards.valid_resource_name(v, what)
        elif src.kind == "git":
            if src.repository_url:
                guards.check_public_https_url(src.repository_url, sec.allowed_git_hosts)
        if src.revision and not re.fullmatch(r"[A-Za-z0-9._/@^~-]{1,200}", src.revision) or (src.revision or "").startswith("-") or ".." in (src.revision or ""):
            raise guards.Rejected("The revision may use letters, digits and . _ / @ ^ ~ - only, and may not start with '-'.")
        if src.subdirectory and (".." in src.subdirectory.replace("\\", "/").split("/") or src.subdirectory.startswith(("/", "\\")) or ":" in src.subdirectory or len(src.subdirectory) > 200
                                 or any(ord(c) < 32 or ord(c) == 127 for c in src.subdirectory)):
            raise guards.Rejected("The subdirectory must be a relative path inside the source.")
        if src.upload_id:
            guards.valid_id(src.upload_id, "upload id")

    @app.put("/session/source")
    def select_source(body: S.SourceSelection, ctx: Ctx = Depends(), _: Principal = need(P.sources_import)):
        _source_checks(ctx, body)
        sel = body.model_dump(mode="json", exclude={"token"})  # the token is never kept on the session
        ctx.session.source, ctx.session.tested["source"] = sel, False
        return {"source": sel}

    def _git_auth(src: S.SourceSelection) -> GitAuth:
        return GitAuth(token=src.token.get_secret_value() if src.token else None, username=src.username)

    @app.post("/test/source")
    def test_source(body: S.SourceSelection, ctx: Ctx = Depends(), _: Principal = need(P.sources_import)):
        _source_checks(ctx, body)
        if body.kind == "azure":
            if not (body.subscription_id and body.resource_group and body.factory_name):
                raise HTTPException(422, "subscription_id, resource_group and factory_name are required.")
            rep = ctx.c.azure.validate_factory(body.subscription_id, body.resource_group, body.factory_name)
        elif body.kind == "git":
            if not body.repository_url:
                raise HTTPException(422, "repository_url is required.")
            rep = ctx.c.git.test(body.repository_url, body.revision, _git_auth(body))
        elif body.kind == "zip":
            path = _upload_path(ctx, body.upload_id)
            summary, report = inspect_zip(path, body.subdirectory)
            return {"kind": "zip", "summary": S.dump(summary), "extract": S.dump(report), "ok": summary.layout_ok}
        else:
            raise HTTPException(422, "kind must be azure, git or zip.")
        ctx.session.tested["source"] = rep.ok
        return S.dump(rep)

    # ---- zip upload (owner-bound, size-capped, magic-checked) -----------------------------------------------------------
    def _owner_file(ctx: Ctx, upload_id: str) -> Path:
        return ctx.c.upload_dir / f"{upload_id}.owner"

    def _upload_path(ctx: Ctx, upload_id: Optional[str]) -> Path:
        if not upload_id or not _ID.match(upload_id):
            raise HTTPException(422, "upload_id is required (upload the ZIP first).")
        p = ctx.c.upload_dir / f"{upload_id}.zip"
        o = _owner_file(ctx, upload_id)
        owner = o.read_text(encoding="utf-8").strip() if o.exists() else None
        if not p.exists() or owner != f"{ctx.principal.org}|{ctx.principal.id}":
            raise HTTPException(404, "That upload does not exist or has been cleaned up.")  # someone else's upload looks the same as none
        return p

    @app.put("/uploads/zip")
    async def upload_zip(request: Request, ctx: Ctx = Depends(), _: Principal = need(P.sources_import)):
        """Raw ZIP bytes in the body (no multipart). Size-capped while streaming; checked to be a ZIP; nothing is extracted here."""
        cap = settings.max_upload_bytes
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > cap:
            raise HTTPException(413, f"The upload is larger than the {cap // (1024 * 1024)} MB limit.")
        mine = f"{ctx.principal.org}|{ctx.principal.id}"
        pending = sum(1 for o in ctx.c.upload_dir.glob("*.owner") if o.read_text(encoding="utf-8").strip() == mine)
        if pending >= MAX_PENDING_UPLOADS:
            raise HTTPException(429, "Too many uploads are waiting. Import or discard them first.")
        upload_id = uuid.uuid4().hex[:16]
        target, size, head = ctx.c.upload_dir / f"{upload_id}.zip", 0, b""
        try:
            with target.open("wb") as fh:
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > cap:
                        raise HTTPException(413, f"The upload is larger than the {cap // (1024 * 1024)} MB limit.")
                    if len(head) < 4:
                        head = (head + chunk)[:4]
                    fh.write(chunk)
            if size == 0:
                raise HTTPException(422, "The upload is empty.")
            if head not in _ZIP_MAGIC:
                raise HTTPException(415, "The upload is not a ZIP archive.")
        except Exception:
            target.unlink(missing_ok=True)
            raise
        _owner_file(ctx, upload_id).write_text(mine, encoding="utf-8")
        ctx.audit("upload.zip", resource="upload", resource_id=upload_id, bytes=size)
        return {"upload_id": upload_id, "bytes": size}

    def _drop_upload(ctx: Ctx, upload_id: Optional[str]) -> None:
        if upload_id and _ID.match(upload_id):
            (ctx.c.upload_dir / f"{upload_id}.zip").unlink(missing_ok=True)
            _owner_file(ctx, upload_id).unlink(missing_ok=True)

    # ---- destination ---------------------------------------------------------------------------------------------------
    def _dest_checks(ctx: Ctx, provider: Optional[str], account: Optional[str], tenant: Optional[str], workspace: Optional[str]) -> None:
        if account:
            ctx.grant("accounts", account, "account")
        if tenant:
            guards.valid_guid(tenant, "tenant id")
            ctx.grant("tenants", tenant, "tenant")
        if workspace:
            guards.valid_guid(workspace, "workspace id")
            ctx.grant("workspaces", workspace, "workspace")

    @app.put("/session/destination")
    def select_destination(body: S.DestinationSelection, ctx: Ctx = Depends(), _: Principal = need(P.azure_use)):
        if body.provider not in ("azure_cli", "fabric_cli"):
            raise HTTPException(422, "provider must be azure_cli or fabric_cli.")
        _dest_checks(ctx, body.provider, body.account, body.tenant_id, body.workspace_id)
        ctx.session.destination, ctx.session.tested["destination"] = body.model_dump(mode="json"), False
        return {"destination": ctx.session.destination}

    @app.get("/fabric/workspaces")
    def fabric_workspaces(provider: Optional[str] = None, account: Optional[str] = None, ctx: Ctx = Depends(), _: Principal = need(P.azure_use)):
        _dest_checks(ctx, provider, account, None, None)
        found = S.dump(ctx.c.fabric(ctx.session, provider, account).workspaces())
        return {"workspaces": _granted_only(found, "workspaces", lambda w: (w.get("id") or "") if isinstance(w, dict) else str(w), ctx)}

    @app.get("/fabric/capacities")
    def fabric_capacities(provider: Optional[str] = None, account: Optional[str] = None, ctx: Ctx = Depends(), _: Principal = need(P.azure_use)):
        _dest_checks(ctx, provider, account, None, None)
        return {"capacities": ctx.c.fabric(ctx.session, provider, account).capacities()}

    @app.post("/test/destination")
    def test_destination(body: S.DestinationSelection, ctx: Ctx = Depends(), _: Principal = need(P.azure_use)):
        _dest_checks(ctx, body.provider, body.account, body.tenant_id, body.workspace_id)
        svc = ctx.c.fabric(ctx.session, body.provider, body.account)
        if not body.workspace_id:
            raise HTTPException(422, "workspace_id is required.")
        rep = svc.validate_workspace(body.workspace_id)
        ctx.session.tested["destination"] = rep.ok
        return S.dump(rep)

    # ---- projects ------------------------------------------------------------------------------------------------------
    @app.get("/projects")
    def projects(ctx: Ctx = Depends(), _: Principal = need(P.projects_view)):
        out = []
        for pid in ctx.access.visible(ctx.principal):  # only the caller's organisation's projects
            out.append({"id": pid, "has_semantic": ctx.c.store.has_adf_semantic_discovery(pid), "has_conversion": ctx.c.store.has_conversion(pid)})
        return {"projects": out}

    def _visible_source_url(ctx: Ctx, url: Optional[str]) -> Optional[str]:
        """A live-factory source URL names a subscription and resource group: show it only to callers granted that subscription."""
        if url and url.startswith("azure://"):
            m = re.match(r"azure://subscriptions/([^/]+)/", url)
            if not m or not ctx.principal.granted("subscriptions", m.group(1)):
                return "azure://(hidden: not in your granted subscriptions)"
        return url

    @app.get("/projects/{pid}")
    def project(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.projects_view)):
        slug = ctx.project(pid)
        p = ctx.c.store.load(slug)
        has = ctx.c.store.has_adf_semantic_discovery(slug)
        # A record made from an older commit than the project's source cannot be converted: report it as not run, stale.
        stale = bool(has and p.repository and ctx.c.store.load_adf_semantic_discovery(slug).source_commit_sha != p.repository.commit_sha)
        return {"id": slug, "name": p.name, "has_semantic": has and not stale, "semantic_stale": stale,
                "has_conversion": ctx.c.store.has_conversion(slug),
                "source": {"provider": p.repository.provider if p.repository else None,
                           "url": _visible_source_url(ctx, p.repository.repository_url if p.repository else None),
                           "commit": p.repository.commit_sha if p.repository else None}}

    @app.post("/projects", status_code=202)
    def import_project(body: S.ImportBody, ctx: Ctx = Depends(), _: Principal = need(P.sources_import)):
        src, c = body.source, ctx.c
        guards.valid_name(body.project_name, "project name")
        slug_new = slugify(body.project_name)
        if ctx.access.would_collide(ctx.principal, slug_new):
            raise HTTPException(409, "That project name is not available.")  # never overwrite another organisation's project
        if len(body.pipelines) > 500 or any(len(x) > 200 for x in body.pipelines):
            raise guards.Rejected("Too many pipeline names, or a name is too long.")
        _source_checks(ctx, src)
        if src.kind == "git" and not src.repository_url:
            raise HTTPException(422, "repository_url is required for a Git source.")
        if src.kind == "azure" and not (src.resource_group and src.factory_name):
            raise HTTPException(422, "resource_group and factory_name are required for an Azure source.")
        if src.kind == "azure" and src.subscription_id is None and not ctx.principal.granted("subscriptions", WILDCARD):
            raise HTTPException(422, "subscription_id is required for an Azure source.")
        zip_path = _upload_path(ctx, src.upload_id) if src.kind == "zip" else None
        if src.kind not in ("azure", "git", "zip"):
            raise HTTPException(422, "kind must be azure, git or zip.")
        token = src.token  # held by this closure for the fetch only
        principal = ctx.principal
        ctx.audit("project.import", resource="project", resource_id=slug_new, kind=src.kind)

        def work(log) -> dict[str, Any]:
            try:
                return _import(log)
            finally:  # an uploaded archive is not kept, whether the import worked or not
                if zip_path is not None:
                    _drop_upload(ctx, src.upload_id)

        def _import(log) -> dict[str, Any]:
            log(f"importing from {src.kind}")
            if src.kind == "azure":
                r = IntakeAgent(c.store).run_azure(AzureIntakeRequest(
                    project_name=body.project_name, resource_group=src.resource_group, factory_name=src.factory_name,
                    subscription_id=src.subscription_id, pipelines=body.pipelines))
            elif src.kind == "git":
                r = GitSourceIntake(c.store, c.git).run(GitIntakeRequest(
                    project_name=body.project_name, repository_url=src.repository_url, revision=src.revision,
                    subdirectory=src.subdirectory, pipelines=body.pipelines, token=token, username=src.username))
            else:
                r = ZipSourceIntake(c.store).run(ZipIntakeRequest(
                    project_name=body.project_name, zip_path=str(zip_path), subdirectory=src.subdirectory, pipelines=body.pipelines))
            if not r.success:
                raise StageError(r.error or "The import failed.")
            ProjectAccess(c.store, sec.legacy_org).record(r.project.id, principal)  # the importer's organisation owns it from now on
            log("import complete")
            return {"project_id": r.project.id, "message": r.message}

        return c.jobs.submit("import", slug_new, work, org=principal.org, owner=principal.id).public()

    @app.get("/projects/{pid}/conversion")
    def conversion(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        from adf2fabric.api.stages import conversion_summary

        slug = ctx.project(pid)
        if not ctx.c.store.has_conversion(slug):
            raise HTTPException(404, "This project has not been converted yet.")
        return conversion_summary(ctx.c.store, slug)

    @app.get("/projects/{pid}/handover")
    def handover(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        from adf2fabric.agent.handover import render

        slug = ctx.project(pid)
        if not ctx.c.store.has_conversion(slug):
            raise HTTPException(404, "This project has not been converted yet.")
        return {"markdown": render(ctx.c.store, slug)}

    @app.get("/projects/{pid}/linked-services")
    def linked_services(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.projects_view)):
        slug = ctx.project(pid)
        d = ctx.c.connections.semantic(slug)
        return {"linked_services": [{"name": x.name, "type": x.type} for x in d.linked_services],
                "integration_runtimes": [{"name": r.name, "kind": getattr(r.kind, "value", r.kind)} for r in d.integration_runtimes]}

    # ---- connections -----------------------------------------------------------------------------------------------------
    def _fabric_or_none(ctx: Ctx):
        try:
            return ctx.c.fabric(ctx.session)
        except Exception:  # noqa: BLE001 - cards still render without Fabric; the card says what is unverified
            return None

    def _fabric_or_why(ctx: Ctx):
        """The Fabric service, or None and a short reason (the Connect step says why its existing-connection list is empty)."""
        try:
            return ctx.c.fabric(ctx.session), ""
        except Exception as exc:  # noqa: BLE001
            from adf2fabric.connections.service import reason

            return None, reason(exc)

    def _ls(value: str) -> str:
        if not _LS.match(value or ""):
            raise guards.Rejected("The linked service name is not valid.")
        return value

    @app.get("/connectors")
    def connectors(_: Principal = need(P.projects_view)):
        return {"connectors": [{"id": s.id, "label": s.label, "adf_types": list(s.adf_types), "fabric_type": s.fabric_type,
                                "endpoint": [e.field.public() for e in s.endpoint], "methods": [m.public() for m in s.methods],
                                "notes": s.notes} for s in catalog.CONNECTORS]}

    @app.get("/projects/{pid}/connections")
    def cards(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = ctx.project(pid)
        fabric, why = _fabric_or_why(ctx)
        cards, status = ctx.c.connections.cards_and_status(slug, ctx.key, fabric, fabric_error=why)
        return {"cards": S.dump(cards), "fabric": status}

    @app.get("/projects/{pid}/bindings")
    def bindings(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        """The ids a person must supply after conversion (notebooks, Spark jobs, dataflows, free connections), with live candidates."""
        from adf2fabric.project import bindings as B

        slug = ctx.project(pid)
        items = B.open_items(ctx.c.store, slug)
        fabric, why = _fabric_or_why(ctx)
        found, read_error = B.candidates(fabric, ctx.session.destination.get("workspace_id"), items) if items else ({}, "")
        return {"items": [{**i, "candidates": found.get(i["key"], [])} for i in items], "fabric_note": why or read_error}

    @app.put("/projects/{pid}/bindings")
    def set_binding(pid: str, body: S.BindingBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        from adf2fabric.project import bindings as B

        slug = ctx.project(pid)
        items = B.open_items(ctx.c.store, slug)
        item = next((i for i in items if i["key"].lower() == body.key.lower()), None)
        verified: Optional[bool] = None
        if item is not None and item["kind"] != "connection":  # a notebook, job or dataflow must exist in the chosen workspace; a connection may be visible to others only
            fabric, _why = _fabric_or_why(ctx)
            found, _err = B.candidates(fabric, ctx.session.destination.get("workspace_id"), [item])
            if item["key"] in found:
                verified = body.value.strip().lower() in {c["id"].lower() for c in found[item["key"]]}
                if not verified:
                    raise guards.Rejected(f"That id is not a {item['label'].lower()} in the chosen Fabric workspace.")
        try:
            done = B.set_binding(ctx.c.store, slug, body.key, body.value)
        except B.BindingError as exc:
            raise guards.Rejected(str(exc)) from None
        ctx.audit("binding.set", resource="binding", resource_id=done["key"], changed=done["changed"], verified=verified)
        return {**done, "verified": verified}

    @app.put("/projects/{pid}/connections/{ls}/choice")
    def set_choice(pid: str, ls: str, body: S.ChoiceBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        saved = ctx.c.connections.set_choice(slug, ConnectionChoice(linked_service=_ls(ls), **body.model_dump()))
        return S.dump(saved)

    @app.put("/projects/{pid}/connections/{ls}/secrets")
    def put_secret(pid: str, ls: str, body: S.SecretBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        _ls(ls)
        if not _FIELD.match(body.field) or len(body.value.get_secret_value()) > 8192:
            raise guards.Rejected("The field name or the value is not acceptable.")
        ctx.c.connections.put_secret(ctx.key, slug, ls, body.field, body.value.get_secret_value())
        ctx.audit("connection.secret_held", resource="linked_service", resource_id=ls)  # the fact only: never the field value
        return {"held": ctx.c.vault.fields(ctx.key, SCOPE, next((x.name for x in ctx.c.connections.semantic(slug).linked_services if x.name.lower() == ls.lower()), ls))}  # field names only

    @app.delete("/projects/{pid}/connections/secrets")
    def clear_secrets(pid: str, ls: Optional[str] = None, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        ctx.project(pid)
        return {"cleared": ctx.c.connections.clear_secrets(ctx.key, _ls(ls) if ls else None)}

    @app.post("/projects/{pid}/connections/{ls}/test")
    def test_connection(pid: str, ls: str, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        return S.dump(ctx.c.connections.test(slug, ctx.key, _ls(ls), _fabric_or_none(ctx)))

    @app.post("/projects/{pid}/connections/apply")
    def apply(pid: str, body: S.ApplyBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        if not body.dry_run:
            ctx.require(P.migration_execute)  # creating connections in the target changes it
            if not body.confirm:
                raise HTTPException(422, "This changes Fabric. Confirm it (confirm: true) after reading the preview.")
        ws = body.workspace_id or ctx.session.destination.get("workspace_id")
        if ws:
            guards.valid_guid(ws, "workspace id")
            ctx.grant("workspaces", ws, "workspace")
        ctx.audit("connections.apply", "ok" if body.dry_run else "requested", resource="project", resource_id=slug, dry_run=body.dry_run, workspace=ws)
        res = ctx.c.connections.apply(slug, ctx.key, ctx.c.fabric(ctx.session), workspace_id=ws, names=[_ls(n) for n in body.names] or None,
                                      auto_match=body.auto_match, dry_run=body.dry_run)
        return S.dump(res)

    @app.post("/projects/{pid}/connections/workspace-identity")
    def identity(pid: str, body: S.IdentityBody, ctx: Ctx = Depends(), _: Principal = need(P.provision_approve, P.migration_execute)):
        slug = ctx.project(pid)
        ws = body.workspace_id or ctx.session.destination.get("workspace_id")
        if not ws:
            raise HTTPException(422, "workspace_id is required.")
        guards.valid_guid(ws, "workspace id")
        ctx.grant("workspaces", ws, "workspace")
        if not body.confirm:
            raise HTTPException(422, "This changes Fabric. Confirm it (confirm: true).")
        ctx.audit("workspace_identity.provision", "requested", resource="workspace", resource_id=ws, project=slug)
        return ctx.c.connections.provision_identity(ctx.c.fabric(ctx.session), ws)

    @app.get("/projects/{pid}/connections/report")
    def report(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = ctx.project(pid)
        paths = ctx.c.connections.write_report(slug, ctx.key, _fabric_or_none(ctx))
        base = ctx.c.store.conversion_dir(slug)
        return {"files": paths, "markdown": (base / paths["markdown"]).read_text(encoding="utf-8"),
                "rows": json.loads((base / paths["json"]).read_text(encoding="utf-8"))}

    # ---- migration stages ------------------------------------------------------------------------------------------------
    def _idem_key(ctx: Ctx, request: Request) -> Optional[tuple[str, str]]:
        raw = request.headers.get("idempotency-key")
        if raw is None:
            return None
        if not _IDEM.match(raw):
            raise HTTPException(400, "Idempotency-Key must be 8-64 letters, digits, '-' or '_'.")
        return (ctx.key, raw)

    def _idem_sig(pid: str, what: str, body: Any) -> str:
        """What a key stands for: the project, the action and the exact request. A key may repeat only THAT request."""
        import hashlib

        payload = body.model_dump() if hasattr(body, "model_dump") else body
        return hashlib.sha256(json.dumps([pid, what, payload], sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]

    def _idem_replay(k: Optional[tuple[str, str]], sig: str = "") -> Optional[JSONResponse]:
        import time as _t

        now = _t.time()
        with app.state.idem_lock:
            for old in [x for x, v in list(app.state.idem.items()) if now - v[0] > IDEMPOTENCY_TTL]:
                app.state.idem.pop(old, None)
            hit = app.state.idem.get(k) if k else None
        if hit and hit[2] != sig:  # the same key for a different request would hand back a job the caller did not ask for
            raise HTTPException(422, "This Idempotency-Key was already used for a different request. Use a new key for a new request.")
        return JSONResponse(status_code=202, content=hit[1], headers={"Idempotent-Replay": "true"}) if hit else None

    def _idem_remember(k: Optional[tuple[str, str]], body: dict, sig: str = "") -> dict:
        import time as _t

        if k:
            with app.state.idem_lock:
                app.state.idem[k] = (_t.time(), body, sig)
        return body

    @app.post("/projects/{pid}/stages/{stage}", status_code=202)
    def stage(pid: str, stage: str, body: S.StageBody, request: Request, ctx: Ctx = Depends(), _: Principal = need(P.projects_view)):
        slug = ctx.project(pid)
        if stage not in STAGES:
            raise HTTPException(404, f"Unknown stage. Use one of: {', '.join(STAGES)}.")
        ctx.require(STAGE_PERMISSION[stage])  # each stage needs ITS permission: planning does not imply executing
        if len(body.pipelines) > 500 or any(len(x) > 200 for x in body.pipelines):
            raise guards.Rejected("Too many pipeline names, or a name is too long.")
        if body.lakehouse and not re.match(r"^[A-Za-z0-9_.\-]{1,256}$", body.lakehouse):
            raise HTTPException(422, "A lakehouse name uses letters, digits, _ . - only (no spaces or commas).")
        if (body.lakehouse is not None or body.apply_reconstruction is not None) and stage != "convert":
            raise HTTPException(422, "lakehouse and apply_reconstruction apply to the convert stage only.")
        if stage == "verify":  # ADF run history is read with the project's own factory: that subscription must be one this caller may use
            from adf2fabric.agent.deployment.verify import Factory

            proj = ctx.c.store.load(slug)
            fac = Factory.from_source(proj.repository.repository_url if proj.repository else None)
            if fac is not None:
                ctx.grant("subscriptions", fac.subscription, "subscription")
        ws = body.workspace_id or (ctx.session.destination.get("workspace_id") if stage in ("deploy_dry_run", "deploy_apply", "verify") else None)
        if stage in ("deploy_dry_run", "deploy_apply", "verify") and ws:
            guards.valid_guid(ws, "workspace id")
            ctx.grant("workspaces", ws, "workspace")
        elif body.workspace_id:
            guards.valid_guid(body.workspace_id, "workspace id")
            ctx.grant("workspaces", body.workspace_id, "workspace")
        if stage == "deploy_apply" or (stage == "verify" and body.run):
            drift = brief_record.workspace_mismatch(ctx.c.store, slug, ws)  # an approval is for the workspace it was agreed for
            if drift:
                raise HTTPException(409, drift)
        if stage == "deploy_apply":
            if not body.confirm:
                raise HTTPException(422, "This changes Fabric. Confirm it (confirm: true) after reading the dry run.")
            if not ctx.c.store.has_conversion(slug):
                raise HTTPException(409, "Nothing is converted yet.", )
            conv = ctx.c.store.load_conversion(slug)
            wanted = {n.lower() for n in body.pipelines}
            names = [p.fabric_pipeline_name for p in conv.pipelines if not wanted or p.source_pipeline.lower() in wanted]
            if not names:
                raise HTTPException(409, "None of the chosen pipelines is converted.")
            if not deploy_record.dry_run_ok(ctx.c.store.project_dir(slug), ws or "", names, allow_unbound=body.allow_unbound):
                raise HTTPException(409, "Run a dry run of exactly these pipelines first, with the same options (it must finish with nothing refused, within the last hour).")
        if stage == "deploy_apply" and body.allow_unbound:
            ctx.require(P.migration_execute)
        if body.run:
            if stage != "verify":
                raise HTTPException(422, "run applies to the verify stage only.")
            ctx.require(P.migration_execute)  # a real run reads the pipeline's sources and writes its targets
            if not body.confirm:
                raise HTTPException(422, "This starts real runs in Fabric that read sources and write targets. Confirm it (confirm: true).")
            unique: dict[str, str] = {}
            for n in body.pipelines:  # one run per pipeline, however often a name is repeated
                unique.setdefault(n.strip().lower(), n.strip())
            if not unique or len(unique) > 20:
                raise HTTPException(422, "Name the pipelines to run (1 to 20): running everything is never the default.")
            if not ws:
                raise HTTPException(422, "Choose the Fabric workspace that holds the pipelines.")
            converted = {p.source_pipeline.lower() for p in ctx.c.store.load_conversion(slug).pipelines} if ctx.c.store.has_conversion(slug) else set()
            unknown = sorted(v for k, v in unique.items() if k not in converted)
            if unknown:
                raise HTTPException(422, f"Not converted pipelines: {', '.join(unknown[:5])}. Only converted, deployed pipelines can be run.")
            body = body.model_copy(update={"pipelines": list(unique.values())})
            agreed = brief_record.approved_settings(ctx.c.store, slug, ctx.session.destination)
            if agreed is not None and not agreed.get("allow_real_runs"):
                raise HTTPException(409, "The approved consultation brief does not allow real runs (validation depth, or data restrictions). Change the brief to allow them.")
        key = _idem_key(ctx, request)
        sig = _idem_sig(slug, f"stage.{stage}", body)
        with app.state.idem_lock:  # two requests with one key must not both start a job
            replay = _idem_replay(key, sig)
            if replay is not None:
                return replay
            opts = body.model_dump()
            if body.run:  # after the replay check: a repeated submission starts nothing and is not a new request
                ctx.audit("verify.run", "requested", resource="project", resource_id=slug, workspace=ws, pipelines=len(body.pipelines))
            ctx.audit(f"stage.{stage}", "requested", resource="project", resource_id=slug, workspace=ws or "")
            job = ctx.c.jobs.submit(stage, slug, lambda log: run_stage(ctx.c, ctx.session, slug, stage, log, opts),
                                    org=ctx.principal.org, owner=ctx.principal.id).public()
            return _idem_remember(key, job, sig)

    def _pub_job(j, since: int = 0) -> dict:
        """A job as the browser sees it: the result passes through the same redaction as every other view (paths, secrets and code are dropped)."""
        d = j.public(since)
        d["result"] = views.sanitize(d.get("result"), tuple(path_roots))
        return d

    @app.get("/jobs")
    def jobs(project_id: Optional[str] = None, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = ctx.project(project_id) if project_id else None
        return {"jobs": [_pub_job(j) for j in ctx.c.jobs.list(slug, org=ctx.principal.org)]}

    @app.get("/jobs/{job_id}")
    def job(job_id: str, since: int = 0, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        j = ctx.c.jobs.get(job_id)
        if j is None or (j.org is not None and j.org != ctx.principal.org):
            raise HTTPException(404, "No such job.")  # another organisation's job looks the same as none
        return _pub_job(j, max(0, since))

    # ---- views for the guided UI: redacted on the server ----------------------------------------------------------------
    def _roots() -> tuple[Path, ...]:
        return tuple(path_roots)

    @app.get("/projects/{pid}/inventory")
    def inventory_view(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = ctx.project(pid)
        if not ctx.c.store.has_adf_semantic_discovery(slug):
            raise HTTPException(409, "Discovery has not run for this project yet.")
        return views.inventory(ctx.c.store, slug, _roots())

    @app.get("/projects/{pid}/waves")
    def waves_view(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = ctx.project(pid)
        if not ctx.c.store.has_adf_semantic_discovery(slug):
            raise HTTPException(409, "Discovery has not run: the waves are built from what was discovered.")
        return views.waves(ctx.c.store, slug, _roots())

    @app.get("/projects/{pid}/confidence")
    def confidence_view(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = ctx.project(pid)
        return views.confidence(ctx.c.store, slug, _roots())

    @app.get("/projects/{pid}/compatibility")
    def compatibility_view(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = ctx.project(pid)
        return views.compatibility(ctx.c.store, slug, _roots())

    @app.get("/projects/{pid}/validation")
    def validation_view(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = ctx.project(pid)
        return views.validation_summary(ctx.c.store, slug, _roots())

    @app.get("/projects/{pid}/report")
    def report_view(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = ctx.project(pid)
        if not ctx.c.store.has_adf_semantic_discovery(slug):
            raise HTTPException(409, "Discovery has not run for this project yet.")
        return views.report(ctx.c.store, slug, _roots())

    # ---- target provisioning (plan: read-only; approve; apply: dry run unless confirmed; verify: read-only) ------------------
    from adf2fabric.agent.provisioning import service as prov
    from adf2fabric.agent.provisioning.executor import ApplyRefused
    from adf2fabric.project.provisioning import ProvisioningApproval, ProvisioningPlan, ProvisioningRun, TargetSpec

    # ---- journey: which steps a person confirmed, kept per project so a page reload does not reset the walk-through ------------
    JOURNEY_STEPS = ("discover", "review", "consult", "connect", "plan", "convert", "validate", "deploy", "report")

    def _journey_file(ctx: Ctx, slug: str) -> Path:
        return ctx.c.store.project_dir(slug) / "journey.json"

    def _journey_read(ctx: Ctx, slug: str) -> list[str]:
        f = _journey_file(ctx, slug)
        try:
            return [s for s in json.loads(f.read_text(encoding="utf-8")).get("confirmed", []) if s in JOURNEY_STEPS] if f.exists() else []
        except (OSError, ValueError):
            return []

    def _journey_write(ctx: Ctx, slug: str, steps: list[str]) -> None:
        write_atomic(_journey_file(ctx, slug), json.dumps({"confirmed": [s for s in JOURNEY_STEPS if s in steps]}))

    @app.get("/projects/{pid}/journey")
    def journey_get(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.projects_view)):
        return {"confirmed": _journey_read(ctx, ctx.project(pid))}

    @app.put("/projects/{pid}/journey")
    def journey_confirm(pid: str, body: S.JourneyBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        bad = [s for s in body.steps if s not in JOURNEY_STEPS]
        if bad:
            raise HTTPException(422, f"Unknown step(s): {', '.join(bad[:3])}.")
        with lock_for(_journey_file(ctx, slug)):
            merged = sorted(set(_journey_read(ctx, slug)) | set(body.steps))
            _journey_write(ctx, slug, merged)
        ctx.audit("journey.confirm", "ok", resource="project", resource_id=slug, steps=",".join(body.steps))
        return {"confirmed": _journey_read(ctx, slug)}

    # ---- model-assisted drafts (optional; off unless ADF2FABRIC_LLM=on): a model drafts corrected ADF source, a person accepts it, convert --assisted uses it ------
    def _assist(ctx: Ctx, pid: str):
        from adf2fabric.agent.assist.service import AssistService

        return AssistService(ctx.c.store, ctx.project(pid))

    def _assist_call(fn):
        from adf2fabric.agent.assist.service import AssistError

        try:
            return fn()
        except AssistError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.get("/projects/{pid}/assist")
    def assist_overview(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        svc = _assist(ctx, pid)
        try:
            found = svc.candidates()
        except Exception:  # noqa: BLE001 - nothing to draft until a conversion exists; the settings are still shown
            found = None
        return {"settings": svc.settings(), "candidates": found}

    @app.get("/projects/{pid}/assist/{name}")
    def assist_detail(pid: str, name: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        return _assist_call(lambda: _assist(ctx, pid).detail(name))

    @app.get("/projects/{pid}/assist/{name}/prompt")
    def assist_prompt(pid: str, name: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        return _assist_call(lambda: _assist(ctx, pid).prompt(name))

    @app.post("/projects/{pid}/assist/generate", status_code=202)
    def assist_generate(pid: str, body: S.AssistGenerateBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        result = _assist_call(lambda: _assist(ctx, pid).generate(body.names))
        ctx.audit("assist.generate", "ok", resource="project", resource_id=slug, pipelines=",".join(body.names)[:200])
        return result

    @app.post("/projects/{pid}/assist/{name}/accept")
    def assist_accept(pid: str, name: str, body: S.AssistAcceptBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        result = _assist_call(lambda: _assist(ctx, pid).accept(name, body.code, by=ctx.principal.id))
        ctx.audit("assist.accept", "ok", resource="project", resource_id=slug, pipeline=name, edited=bool(result["accepted"]["edited"]))
        return result

    @app.post("/projects/{pid}/assist/{name}/discard")
    def assist_discard(pid: str, name: str, body: S.AssistDiscardBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        result = _assist_call(lambda: _assist(ctx, pid).discard(name, body.scope))
        ctx.audit("assist.discard", "ok", resource="project", resource_id=slug, pipeline=name, scope=body.scope)
        return result

    @app.post("/projects/{pid}/journey/reset")
    def journey_reset(pid: str, body: S.JourneyBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        with lock_for(_journey_file(ctx, slug)):
            _journey_write(ctx, slug, [s for s in _journey_read(ctx, slug) if s not in set(body.steps)])
        ctx.audit("journey.reset", "ok", resource="project", resource_id=slug, steps=",".join(body.steps))
        return {"confirmed": _journey_read(ctx, slug)}

    # ---- consultation brief: how the migration should happen, agreed after discovery ----------------------------------------
    def _brief_project(ctx: Ctx, pid: str) -> str:
        slug = ctx.project(pid)
        if not ctx.c.store.has_adf_semantic_discovery(slug):
            raise HTTPException(409, "Discovery has not run: the consultation asks only about what discovery found.")
        return slug

    def _brief_view(ctx: Ctx, slug: str) -> dict:
        return brief_record.view(ctx.c.store, slug, ctx.session.destination)

    @app.get("/projects/{pid}/brief")
    def brief_view(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        return _brief_view(ctx, _brief_project(ctx, pid))

    @app.put("/projects/{pid}/brief/answers")
    def brief_answers(pid: str, body: S.BriefAnswersBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = _brief_project(ctx, pid)
        try:
            n = brief_record.set_answers(ctx.c.store, slug, body.answers, ctx.principal.id, ctx.session.destination)
        except brief_record.BriefError as exc:
            raise HTTPException(422, str(exc)) from None
        ctx.audit("brief.answers", "ok", resource="project", resource_id=slug, answers=n)
        return _brief_view(ctx, slug)

    @app.post("/projects/{pid}/brief/accept")
    def brief_accept(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = _brief_project(ctx, pid)
        n = brief_record.accept_recommendations(ctx.c.store, slug, ctx.principal.id, ctx.session.destination)
        ctx.audit("brief.accept", "ok", resource="project", resource_id=slug, answers=n)
        return {"accepted": n, **_brief_view(ctx, slug)}

    @app.post("/projects/{pid}/brief/submit")
    def brief_submit(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = _brief_project(ctx, pid)
        try:
            v = brief_record.submit(ctx.c.store, slug, ctx.principal.id, ctx.session.destination)
        except brief_record.BriefError as exc:
            raise HTTPException(409, str(exc)) from None
        ctx.audit("brief.submit", "ok", resource="project", resource_id=slug, version=v["version"])
        return _brief_view(ctx, slug)

    @app.post("/projects/{pid}/brief/approve")
    def brief_approve(pid: str, body: S.BriefApproveBody, ctx: Ctx = Depends(), _: Principal = need(P.provision_approve)):
        slug = _brief_project(ctx, pid)
        try:  # the approver is the signed-in person, never a name typed by the client
            v = brief_record.approve(ctx.c.store, slug, body.fingerprint, ctx.principal.id, ctx.session.destination)
        except brief_record.BriefError as exc:
            raise HTTPException(409, str(exc)) from None
        ctx.audit("brief.approve", "ok", resource="project", resource_id=slug, version=v["version"])
        return _brief_view(ctx, slug)

    @app.post("/projects/{pid}/brief/import")
    def brief_import(pid: str, body: S.BriefImportBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = _brief_project(ctx, pid)
        try:
            n = brief_record.import_answers(ctx.c.store, slug, body.questionnaire, ctx.principal.id, ctx.session.destination)
        except brief_record.BriefError as exc:
            raise HTTPException(422, str(exc)) from None
        ctx.audit("brief.import", "ok", resource="project", resource_id=slug, answers=n)
        return {"imported": n, **_brief_view(ctx, slug)}

    @app.post("/projects/{pid}/brief/notes")
    def brief_note(pid: str, body: S.BriefNoteBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = _brief_project(ctx, pid)
        try:
            brief_record.add_note(ctx.c.store, slug, body.text, ctx.principal.id)
        except brief_record.BriefError as exc:
            raise HTTPException(422, str(exc)) from None
        return {"notes": brief_record.load(ctx.c.store, slug)["notes"][-50:]}

    @app.get("/projects/{pid}/brief/markdown")
    def brief_markdown(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = _brief_project(ctx, pid)
        from adf2fabric.agent.consultation import brief as B

        facts = B.read_facts(ctx.c.store, slug, ctx.session.destination)
        v = _brief_view(ctx, slug)
        rs = B.resolve(facts, brief_record.load(ctx.c.store, slug)["answers"])
        cur = v["current_version"]
        return {"markdown": B.brief_markdown(facts, rs, slug, cur["version"] if cur else None, v["state"])}

    @app.get("/projects/{pid}/brief/questionnaire")
    def brief_questionnaire(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        slug = _brief_project(ctx, pid)
        from adf2fabric.agent.consultation import brief as B

        facts = B.read_facts(ctx.c.store, slug, ctx.session.destination)
        return B.questionnaire(facts, B.resolve(facts, brief_record.load(ctx.c.store, slug)["answers"]))

    @app.get("/projects/{pid}/provisioning")
    def provisioning_view(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.reports_view)):
        return views.provisioning(ctx.c.store, ctx.project(pid), _roots())

    def _target_spec(ctx: Ctx, body: S.ProvisionPlanBody) -> TargetSpec:
        if body.workspace_id:
            guards.valid_guid(body.workspace_id, "workspace id")
            ctx.grant("workspaces", body.workspace_id, "workspace")
        elif body.workspace_name:
            guards.valid_name(body.workspace_name, "workspace name")
            if not ctx.principal.granted("workspaces", WILDCARD):  # a name could select any workspace: only a caller with open workspace access may use one
                raise HTTPException(403, "Choose a workspace by id: workspaces are limited for your account.")
        else:
            dest_ws = ctx.session.destination.get("workspace_id")
            if not dest_ws:
                raise HTTPException(422, "Choose the target workspace (connect one on the Connections page).")
            ctx.grant("workspaces", dest_ws, "workspace")
            body = body.model_copy(update={"workspace_id": dest_ws})
        tenant = body.tenant_id or ctx.session.destination.get("tenant_id")
        if tenant:
            guards.valid_guid(tenant, "tenant id")
            ctx.grant("tenants", tenant, "tenant")
        for v, what in ((body.capacity_id, "capacity id"),):
            if v:
                guards.valid_guid(v, what)
        for n in [*body.lakehouses, *body.warehouses]:
            guards.valid_name(n, "item name")
        return TargetSpec(tenant_id=tenant, workspace_id=body.workspace_id, workspace_name=body.workspace_name, capacity_id=body.capacity_id,
                          capacity_name=body.capacity_name, lakehouse_names=list(body.lakehouses), warehouse_names=list(body.warehouses),
                          provision_workspace_identity=body.provision_identity)

    @app.post("/projects/{pid}/provisioning/plan", status_code=202)
    def provisioning_plan(pid: str, body: S.ProvisionPlanBody, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate, P.azure_use)):
        slug = ctx.project(pid)
        spec = _target_spec(ctx, body)
        ctx.audit("provisioning.plan", "requested", resource="project", resource_id=slug)

        def work(log) -> dict[str, Any]:
            fab, _arm = ctx.c.gateways(ctx.session)
            log("reading the target (read-only) and planning")
            plan = prov.plan(ctx.c.store, slug, spec, fab, None)
            return {"message": "Plan ready: nothing was created.", "steps": plan.summary.total, "to_create": plan.summary.to_create,
                    "blocked": plan.summary.blocked, "blockers": len(plan.blockers)}

        return ctx.c.jobs.submit("provision_plan", slug, work, org=ctx.principal.org, owner=ctx.principal.id).public()

    def _current_plan(ctx: Ctx, slug: str, plan_ref: str) -> ProvisioningPlan:
        if not ctx.c.store.has_provisioning(slug, "plan"):
            raise HTTPException(409, "There is no plan yet. Build the plan first.")
        plan = ctx.c.store.load_provisioning(slug, "plan", ProvisioningPlan)
        if len(plan_ref) < 12 or not plan.plan_hash.startswith(plan_ref):
            raise HTTPException(409, "The plan changed since you reviewed it. Reload it and review it again.")
        return plan

    @app.post("/projects/{pid}/provisioning/approve")
    def provisioning_approve(pid: str, body: S.ProvisionApproveBody, ctx: Ctx = Depends(), _: Principal = need(P.provision_approve)):
        slug = ctx.project(pid)
        plan = _current_plan(ctx, slug, body.plan_ref)
        chosen = None
        if body.steps is not None:  # the browser names steps by opaque ref; the server maps them to real step ids
            by_ref = {views.step_ref(s.id): s.id for s in plan.steps}
            unknown = [r for r in body.steps if r not in by_ref]
            if unknown:
                raise HTTPException(422, "Some of those steps are not in the current plan. Reload the plan.")
            chosen = [by_ref[r] for r in body.steps]
        try:
            a = prov.approve(ctx.c.store, slug, chosen, ctx.principal.id, body.note)
        except ApplyRefused as exc:
            raise HTTPException(422, str(exc)) from None
        ctx.audit("provisioning.approve", "ok", resource="project", resource_id=slug, steps=len(a.step_ids))
        return {"approved": len(a.step_ids), "by": a.approved_by}

    @app.post("/projects/{pid}/provisioning/apply", status_code=202)
    def provisioning_apply(pid: str, body: S.ProvisionApplyBody, request: Request, ctx: Ctx = Depends(), _: Principal = need(P.migration_execute)):
        slug = ctx.project(pid)
        plan = _current_plan(ctx, slug, body.plan_ref)
        if not ctx.c.store.has_provisioning(slug, "approval"):
            raise HTTPException(409, "Nothing is approved yet.")
        approval = ctx.c.store.load_provisioning(slug, "approval", ProvisioningApproval)
        if approval.plan_hash != plan.plan_hash:
            raise HTTPException(409, "The approval is for a different plan. Approve this plan first.")
        if body.apply:
            if not body.confirm:
                raise HTTPException(422, "This creates or changes resources in Fabric. Confirm it (confirm: true) after the dry run.")
            run = ctx.c.store.load_provisioning(slug, "run", ProvisioningRun) if ctx.c.store.has_provisioning(slug, "run") else None
            if not (run and run.dry_run and run.plan_hash == plan.plan_hash and run.finished_at is not None):
                raise HTTPException(409, "Run the dry run of this plan first.")
        key = _idem_key(ctx, request)
        sig = _idem_sig(slug, "provisioning.apply", body)
        with app.state.idem_lock:  # two requests with one key must not both start a job
            replay = _idem_replay(key, sig)
            if replay is not None:
                return replay
            ctx.audit("provisioning.apply", "requested" if body.apply else "dry_run", resource="project", resource_id=slug)

            def work(log) -> dict[str, Any]:
                fab, _arm = ctx.c.gateways(ctx.session)
                log("applying the approved steps" if body.apply else "dry run: nothing will be written")
                try:
                    run, report = prov.apply(ctx.c.store, slug, fab, None, do_apply=body.apply)
                except ApplyRefused as exc:
                    raise StageError(str(exc)) from None
                failed = sum(1 for r in run.results if r.status.value in ("failed", "mismatch"))
                return {"message": ("Applied. Read the verification below: only it says what exists." if body.apply else "Dry run finished: nothing was changed."),
                        "dry_run": run.dry_run, "failed": failed, "verified": bool(report and report.all_verified)}

            return _idem_remember(key, ctx.c.jobs.submit("provision_apply" if body.apply else "provision_dry_run", slug, work, org=ctx.principal.org,
                                                          owner=ctx.principal.id).public(), sig)

    @app.post("/projects/{pid}/provisioning/verify", status_code=202)
    def provisioning_verify(pid: str, ctx: Ctx = Depends(), _: Principal = need(P.plans_generate)):
        slug = ctx.project(pid)
        if not ctx.c.store.has_provisioning(slug, "plan"):
            raise HTTPException(409, "There is no plan to verify.")

        def work(log) -> dict[str, Any]:
            fab, _arm = ctx.c.gateways(ctx.session)
            report = prov.verify(ctx.c.store, slug, fab, None)
            return {"message": "Read back from the target.", "all_verified": report.all_verified}

        return ctx.c.jobs.submit("provision_verify", slug, work, org=ctx.principal.org, owner=ctx.principal.id).public()

    # ---- users (own organisation only) ---------------------------------------------------------------------------------
    def _require_token_mode() -> None:
        if _mode() != "token":
            raise HTTPException(409, "User management needs authentication to be enabled (token mode).")

    def _cannot_exceed(p: Principal, roles: list[str], grants: Optional[Grants]) -> None:
        """Privilege-escalation guard: nobody can hand out more than they hold."""
        want = permissions_for(roles)
        if not want <= p.permissions:
            raise HTTPException(403, "You cannot grant a role with permissions you do not hold.")
        if grants is not None:
            for kind in ("subscriptions", "tenants", "accounts", "workspaces"):
                for v in getattr(grants, kind):
                    if v == WILDCARD and not p.granted(kind, WILDCARD):
                        raise HTTPException(403, "You cannot grant access you do not hold.")
                    if v != WILDCARD and not p.granted(kind, v):
                        raise HTTPException(403, "You cannot grant access you do not hold.")

    def _public_user(u) -> dict:
        return {"id": u.id, "org": u.org, "roles": u.roles, "disabled": u.disabled, "token_hint": u.token_hint,
                "grants": u.grants.model_dump(), "created_at": u.created_at.isoformat()}

    @app.get("/admin/users")
    def list_users(ctx: Ctx = Depends(), _: Principal = need(P.users_manage)):
        _require_token_mode()
        return {"users": [_public_user(u) for u in users.list(ctx.principal.org)]}

    @app.post("/admin/users", status_code=201)
    def create_user(body: S.UserCreate, ctx: Ctx = Depends(), _: Principal = need(P.users_manage)):
        _require_token_mode()
        grants = Grants(**body.grants.model_dump()) if body.grants else Grants()
        _cannot_exceed(ctx.principal, body.roles, grants)
        rec, token = users.create(body.id, ctx.principal.org, body.roles, grants, created_by=ctx.principal.id)  # the organisation is the caller's, never client-chosen
        ctx.audit("user.create", resource="user", resource_id=rec.id, roles=",".join(rec.roles))
        return {**_public_user(rec), "token": token, "note": "Shown once. Store it in a secret manager; it cannot be shown again."}

    def _target(ctx: Ctx, uid: str):
        rec = users.get(uid)
        if rec is None or rec.org != ctx.principal.org:
            raise HTTPException(404, "No such user.")  # other organisations' users are invisible
        return rec

    @app.patch("/admin/users/{uid}")
    def update_user(uid: str, body: S.UserUpdate, ctx: Ctx = Depends(), _: Principal = need(P.users_manage)):
        _require_token_mode()
        rec = _target(ctx, uid)
        grants = Grants(**body.grants.model_dump()) if body.grants else None
        if body.roles is not None or grants is not None:
            _cannot_exceed(ctx.principal, body.roles or rec.roles, grants)
        if rec.id.lower() == ctx.principal.id.lower() and (body.disabled or (body.roles is not None and "admin" not in body.roles)):
            raise HTTPException(409, "You cannot disable yourself or remove your own admin role.")
        if (body.disabled or (body.roles is not None and "admin" not in body.roles)) and "admin" in rec.roles and users.admin_count(rec.org) <= 1:
            raise HTTPException(409, "That would leave the organisation without an administrator.")
        new = users.update(rec.id, roles=body.roles, disabled=body.disabled, grants=grants)
        ctx.audit("user.update", resource="user", resource_id=new.id, roles=",".join(new.roles), disabled=new.disabled)
        return _public_user(new)

    @app.post("/admin/users/{uid}/rotate-token")
    def rotate_token(uid: str, ctx: Ctx = Depends(), _: Principal = need(P.users_manage)):
        _require_token_mode()
        rec = _target(ctx, uid)
        if not set(permissions_for(rec.roles)) <= ctx.principal.permissions:
            raise HTTPException(403, "You cannot rotate the token of a user who holds more permissions than you.")
        new, token = users.rotate(rec.id)
        ctx.audit("user.rotate_token", resource="user", resource_id=new.id)
        return {**_public_user(new), "token": token, "note": "Shown once."}

    @app.get("/admin/audit")
    def read_audit(limit: int = 100, ctx: Ctx = Depends(), _: Principal = need(P.users_manage)):
        return {"events": audit.read(max(1, min(limit, 500)), org=ctx.principal.org), "chain_intact": audit.verify() is None}

    return app
