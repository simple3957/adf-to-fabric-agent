"""The FastAPI application. HTTP concerns only: every decision lives in the services it calls.

Security posture: binds to loopback by default (see ``__main__``), optional bearer token (``ADF2FABRIC_API_TOKEN``), CORS only for
the configured UI origins, validation errors never echo the submitted input (a secret field would otherwise be reflected back),
and unexpected errors return a generic message. Secrets arrive as ``SecretStr``, go to the in-memory vault and are never returned.
"""

import hmac
import json
import re
import shutil
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
from adf2fabric.api.container import Container, Session, Settings
from adf2fabric.api.jobs import JobConflict
from adf2fabric.api.stages import STAGES, StageError, run_stage
from adf2fabric.connections import catalog
from adf2fabric.connections.fabric import FabricApiError
from adf2fabric.connections.providers.azure_cli import ProviderError
from adf2fabric.connections.providers.git_source import GitAuth, GitSourceError
from adf2fabric.connections.providers.zip_source import ZipRejected
from adf2fabric.connections.redaction import redact
from adf2fabric.connections.resolution import ConnectionChoice
from adf2fabric.connections.service import SCOPE
from adf2fabric.project.store import ProjectNotFoundError, slugify

SESSION_HEADER = "x-session-id"
_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _err(status: int, message: str, actions: Optional[list[str]] = None) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": redact(message), "actions": [redact(a) for a in (actions or [])]})


def create_app(settings: Optional[Settings] = None, live: Optional[Container] = None, demo: Optional[Container] = None) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(title="ADF to Fabric Migration Accelerator API", version="0.1.0")
    app.state.containers = {"live": live or Container(settings), "demo": demo}
    app.state.modes = {}  # session id -> "live" | "demo"

    app.add_middleware(CORSMiddleware, allow_origins=settings.allowed_origins, allow_methods=["*"],
                       allow_headers=["Authorization", "Content-Type", "X-Session-Id"])

    # ---- errors: never echo input, never leak internals ----------------------------------------------------------------
    @app.exception_handler(RequestValidationError)
    async def _validation(_r: Request, exc: RequestValidationError):
        problems = [{"field": ".".join(str(p) for p in e.get("loc", ())), "problem": e.get("msg", "invalid")} for e in exc.errors()]
        return JSONResponse(status_code=422, content={"error": "The request is not valid.", "details": problems, "actions": []})

    @app.exception_handler(HTTPException)
    async def _http(_r: Request, exc: HTTPException):
        return _err(exc.status_code, str(exc.detail))

    @app.exception_handler(StageError)
    async def _stage(_r: Request, exc: StageError):
        return _err(400, str(exc), exc.actions)

    @app.exception_handler(catalog.ChoiceError)
    async def _choice(_r: Request, exc: catalog.ChoiceError):
        return JSONResponse(status_code=422, content={"error": "The choice is not valid.",
                                                      "details": [redact(str(p)) for p in getattr(exc, "problems", [str(exc)])],
                                                      "actions": []})

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

    @app.exception_handler(Exception)
    async def _any(_r: Request, exc: Exception):  # noqa: BLE001
        return _err(500, f"Unexpected {type(exc).__name__}; see the server log.")

    # ---- auth + context ------------------------------------------------------------------------------------------------
    def _auth(request: Request) -> None:
        token = settings.token
        if not token or request.url.path == "/health":
            return
        given = request.headers.get("authorization", "")
        if not given.lower().startswith("bearer ") or not hmac.compare_digest(given[7:].strip(), token):
            raise HTTPException(401, "A valid bearer token is required.")

    class Ctx:
        def __init__(self, request: Request):
            sid = request.headers.get(SESSION_HEADER, "default")
            if not _ID.match(sid):
                raise HTTPException(400, "X-Session-Id must be 1-64 letters, digits, '-' or '_'.")
            self.sid = sid
            mode = app.state.modes.get(sid, "live")
            c = app.state.containers.get(mode)
            if c is None:
                from adf2fabric.api.demo import build_demo_container

                c = app.state.containers[mode] = build_demo_container(settings)
            self.c: Container = c
            self.mode = mode
            self.session: Session = c.session(sid)

        def project(self, pid: str) -> str:
            slug = slugify(pid)
            if not self.c.store.exists(slug):
                raise HTTPException(404, f"Project '{pid}' does not exist.")
            return slug

    dep = [Depends(_auth)]

    # ---- health + mode -------------------------------------------------------------------------------------------------
    @app.get("/health")
    def health():
        return {"status": "ok", "version": app.version}

    @app.get("/mode", dependencies=dep)
    def get_mode(ctx: Ctx = Depends()):
        return {"mode": ctx.mode, "modes": ["live", "demo"]}

    @app.post("/mode", dependencies=dep)
    def set_mode(body: S.ModeBody, request: Request):
        if body.mode not in ("live", "demo"):
            raise HTTPException(422, "mode must be 'live' or 'demo'.")
        app.state.modes[request.headers.get(SESSION_HEADER, "default")] = body.mode
        return {"mode": body.mode}

    # ---- sign-in -------------------------------------------------------------------------------------------------------
    def _provider_for(ctx: Ctx, which: str):
        if which == "azure_cli":
            return ctx.c.azure
        if which == "fabric_cli":
            return ctx.c.fabric_cli
        raise HTTPException(404, "Unknown provider. Use azure_cli or fabric_cli.")

    @app.get("/auth/{provider}/status", dependencies=dep)
    def auth_status(provider: str, ctx: Ctx = Depends()):
        return S.dump(_provider_for(ctx, provider).status())

    @app.post("/auth/{provider}/login", dependencies=dep)
    def auth_login(provider: str, body: S.LoginBody, ctx: Ctx = Depends()):
        p = _provider_for(ctx, provider)
        try:
            if provider == "azure_cli":
                attempt = p.start_login(tenant=body.tenant_id, device_code=body.device_code)
            else:
                attempt = p.start_login(body.method, tenant=body.tenant_id, client_id=body.client_id,
                                        certificate_path=body.certificate_path)
        except RuntimeError as exc:  # a sign-in is already running: a conflict, not a server fault
            return _err(409, str(exc), ["Finish the sign-in already open in the browser, or wait up to 5 minutes "
                                        "for it to time out, then try again."])
        except ValueError as exc:  # e.g. an unknown fab sign-in method
            return _err(422, str(exc))
        return attempt.public()

    @app.post("/auth/{provider}/logout", dependencies=dep)
    def auth_logout(provider: str, body: S.LogoutBody, ctx: Ctx = Depends()):
        p = _provider_for(ctx, provider)
        try:
            st = p.logout(body.account) if provider == "azure_cli" else p.logout()
        except ValueError as exc:
            return _err(422, str(exc))
        return S.dump(st)

    @app.get("/auth/{provider}/login/{attempt_id}", dependencies=dep)
    def auth_login_state(provider: str, attempt_id: str, ctx: Ctx = Depends()):
        a = _provider_for(ctx, provider).logins.get(attempt_id)
        if a is None:
            raise HTTPException(404, "No such sign-in attempt.")
        return a.public()

    @app.get("/auth/azure_cli/tenants", dependencies=dep)
    def tenants(ctx: Ctx = Depends()):
        return {"tenants": ctx.c.azure.tenants()}

    @app.get("/session", dependencies=dep)
    def session(ctx: Ctx = Depends()):
        return {"id": ctx.sid, "mode": ctx.mode, "source": ctx.session.source, "destination": ctx.session.destination,
                "tested": ctx.session.tested}

    # ---- source --------------------------------------------------------------------------------------------------------
    @app.get("/azure/subscriptions", dependencies=dep)
    def subscriptions(ctx: Ctx = Depends()):
        return {"subscriptions": S.dump(ctx.c.azure.subscriptions())}

    @app.get("/azure/discover", dependencies=dep)
    def azure_discover(subscription_id: str = Query(...), ctx: Ctx = Depends()):
        found = ctx.c.azure.discover(subscription_id)
        return {k: S.dump(v) for k, v in found.items()}

    @app.put("/session/source", dependencies=dep)
    def select_source(body: S.SourceSelection, ctx: Ctx = Depends()):
        sel = body.model_dump(mode="json", exclude={"token"})  # the token is never kept on the session
        ctx.session.source, ctx.session.tested["source"] = sel, False
        return {"source": sel}

    def _git_auth(src: S.SourceSelection) -> GitAuth:
        return GitAuth(token=src.token.get_secret_value() if src.token else None, username=src.username)

    @app.post("/test/source", dependencies=dep)
    def test_source(body: S.SourceSelection, ctx: Ctx = Depends()):
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

    # ---- zip upload ----------------------------------------------------------------------------------------------------
    def _upload_path(ctx: Ctx, upload_id: Optional[str]) -> Path:
        if not upload_id or not _ID.match(upload_id):
            raise HTTPException(422, "upload_id is required (upload the ZIP first).")
        p = ctx.c.upload_dir / f"{upload_id}.zip"
        if not p.exists():
            raise HTTPException(404, "That upload does not exist or has been cleaned up.")
        return p

    @app.put("/uploads/zip", dependencies=dep)
    async def upload_zip(request: Request, ctx: Ctx = Depends()):
        """Raw ZIP bytes in the body (no multipart). Size-capped while streaming; nothing is extracted here."""
        cap = settings.max_upload_bytes
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > cap:
            raise HTTPException(413, f"The upload is larger than the {cap // (1024 * 1024)} MB limit.")
        upload_id = uuid.uuid4().hex[:16]
        target, size = ctx.c.upload_dir / f"{upload_id}.zip", 0
        try:
            with target.open("wb") as fh:
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > cap:
                        raise HTTPException(413, f"The upload is larger than the {cap // (1024 * 1024)} MB limit.")
                    fh.write(chunk)
            if size == 0:
                raise HTTPException(422, "The upload is empty.")
        except Exception:
            target.unlink(missing_ok=True)
            raise
        return {"upload_id": upload_id, "bytes": size}

    # ---- destination ---------------------------------------------------------------------------------------------------
    @app.put("/session/destination", dependencies=dep)
    def select_destination(body: S.DestinationSelection, ctx: Ctx = Depends()):
        if body.provider not in ("azure_cli", "fabric_cli"):
            raise HTTPException(422, "provider must be azure_cli or fabric_cli.")
        ctx.session.destination, ctx.session.tested["destination"] = body.model_dump(mode="json"), False
        return {"destination": ctx.session.destination}

    @app.get("/fabric/workspaces", dependencies=dep)
    def fabric_workspaces(provider: Optional[str] = None, ctx: Ctx = Depends()):
        return {"workspaces": S.dump(ctx.c.fabric(ctx.session, provider).workspaces())}

    @app.get("/fabric/capacities", dependencies=dep)
    def fabric_capacities(provider: Optional[str] = None, ctx: Ctx = Depends()):
        return {"capacities": ctx.c.fabric(ctx.session, provider).capacities()}

    @app.post("/test/destination", dependencies=dep)
    def test_destination(body: S.DestinationSelection, ctx: Ctx = Depends()):
        svc = ctx.c.fabric(ctx.session, body.provider)
        if not body.workspace_id:
            raise HTTPException(422, "workspace_id is required.")
        rep = svc.validate_workspace(body.workspace_id)
        ctx.session.tested["destination"] = rep.ok
        return S.dump(rep)

    # ---- projects ------------------------------------------------------------------------------------------------------
    @app.get("/projects", dependencies=dep)
    def projects(ctx: Ctx = Depends()):
        out = []
        for pid in ctx.c.store.list_ids():
            out.append({"id": pid, "has_semantic": ctx.c.store.has_adf_semantic_discovery(pid),
                        "has_conversion": ctx.c.store.has_conversion(pid)})
        return {"projects": out}

    @app.get("/projects/{pid}", dependencies=dep)
    def project(pid: str, ctx: Ctx = Depends()):
        slug = ctx.project(pid)
        p = ctx.c.store.load(slug)
        return {"id": slug, "name": p.name, "has_semantic": ctx.c.store.has_adf_semantic_discovery(slug),
                "has_conversion": ctx.c.store.has_conversion(slug),
                "source": {"provider": p.repository.provider if p.repository else None,
                           "url": p.repository.repository_url if p.repository else None,
                           "commit": p.repository.commit_sha if p.repository else None}}

    @app.post("/projects", dependencies=dep, status_code=202)
    def import_project(body: S.ImportBody, ctx: Ctx = Depends()):
        src, c = body.source, ctx.c
        if src.kind == "git" and not src.repository_url:
            raise HTTPException(422, "repository_url is required for a Git source.")
        if src.kind == "azure" and not (src.resource_group and src.factory_name):
            raise HTTPException(422, "resource_group and factory_name are required for an Azure source.")
        zip_path = _upload_path(ctx, src.upload_id) if src.kind == "zip" else None
        if src.kind not in ("azure", "git", "zip"):
            raise HTTPException(422, "kind must be azure, git or zip.")
        token = src.token  # held by this closure for the fetch only

        def work(log) -> dict[str, Any]:
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
            log("import complete")
            if src.kind == "zip":
                zip_path.unlink(missing_ok=True)
            return {"project_id": r.project.id, "message": r.message}

        return c.jobs.submit("import", slugify(body.project_name), work).public()

    @app.get("/projects/{pid}/linked-services", dependencies=dep)
    def linked_services(pid: str, ctx: Ctx = Depends()):
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

    @app.get("/connectors", dependencies=dep)
    def connectors():
        return {"connectors": [{"id": s.id, "label": s.label, "adf_types": list(s.adf_types), "fabric_type": s.fabric_type,
                                "endpoint": [e.field.public() for e in s.endpoint], "methods": [m.public() for m in s.methods],
                                "notes": s.notes} for s in catalog.CONNECTORS]}

    @app.get("/projects/{pid}/connections", dependencies=dep)
    def cards(pid: str, ctx: Ctx = Depends()):
        slug = ctx.project(pid)
        return {"cards": S.dump(ctx.c.connections.cards(slug, ctx.sid, _fabric_or_none(ctx)))}

    @app.put("/projects/{pid}/connections/{ls}/choice", dependencies=dep)
    def set_choice(pid: str, ls: str, body: S.ChoiceBody, ctx: Ctx = Depends()):
        slug = ctx.project(pid)
        saved = ctx.c.connections.set_choice(slug, ConnectionChoice(linked_service=ls, **body.model_dump()))
        return S.dump(saved)

    @app.put("/projects/{pid}/connections/{ls}/secrets", dependencies=dep)
    def put_secret(pid: str, ls: str, body: S.SecretBody, ctx: Ctx = Depends()):
        slug = ctx.project(pid)
        ctx.c.connections.put_secret(ctx.sid, slug, ls, body.field, body.value.get_secret_value())
        return {"held": ctx.c.vault.fields(ctx.sid, SCOPE, next((x.name for x in ctx.c.connections.semantic(slug).linked_services if x.name.lower() == ls.lower()), ls))}  # field names only

    @app.delete("/projects/{pid}/connections/secrets", dependencies=dep)
    def clear_secrets(pid: str, ls: Optional[str] = None, ctx: Ctx = Depends()):
        ctx.project(pid)
        return {"cleared": ctx.c.connections.clear_secrets(ctx.sid, ls)}

    @app.post("/projects/{pid}/connections/{ls}/test", dependencies=dep)
    def test_connection(pid: str, ls: str, ctx: Ctx = Depends()):
        slug = ctx.project(pid)
        return S.dump(ctx.c.connections.test(slug, ctx.sid, ls, _fabric_or_none(ctx)))

    @app.post("/projects/{pid}/connections/apply", dependencies=dep)
    def apply(pid: str, body: S.ApplyBody, ctx: Ctx = Depends()):
        slug = ctx.project(pid)
        ws = body.workspace_id or ctx.session.destination.get("workspace_id")
        res = ctx.c.connections.apply(slug, ctx.sid, ctx.c.fabric(ctx.session), workspace_id=ws, names=body.names or None,
                                      auto_match=body.auto_match, dry_run=body.dry_run)
        return S.dump(res)

    @app.post("/projects/{pid}/connections/workspace-identity", dependencies=dep)
    def identity(pid: str, body: S.IdentityBody, ctx: Ctx = Depends()):
        ctx.project(pid)
        ws = body.workspace_id or ctx.session.destination.get("workspace_id")
        if not ws:
            raise HTTPException(422, "workspace_id is required.")
        return ctx.c.connections.provision_identity(ctx.c.fabric(ctx.session), ws)

    @app.get("/projects/{pid}/connections/report", dependencies=dep)
    def report(pid: str, ctx: Ctx = Depends()):
        slug = ctx.project(pid)
        paths = ctx.c.connections.write_report(slug, ctx.sid, _fabric_or_none(ctx))
        base = ctx.c.store.conversion_dir(slug)
        return {"files": paths, "markdown": (base / paths["markdown"]).read_text(encoding="utf-8"),
                "rows": json.loads((base / paths["json"]).read_text(encoding="utf-8"))}

    # ---- migration stages ------------------------------------------------------------------------------------------------
    @app.post("/projects/{pid}/stages/{stage}", dependencies=dep, status_code=202)
    def stage(pid: str, stage: str, body: S.StageBody, ctx: Ctx = Depends()):
        slug = ctx.project(pid)
        if stage not in STAGES:
            raise HTTPException(404, f"Unknown stage. Use one of: {', '.join(STAGES)}.")
        opts = body.model_dump()
        return ctx.c.jobs.submit(stage, slug, lambda log: run_stage(ctx.c, ctx.session, slug, stage, log, opts)).public()

    @app.get("/jobs", dependencies=dep)
    def jobs(project_id: Optional[str] = None, ctx: Ctx = Depends()):
        return {"jobs": [j.public() for j in ctx.c.jobs.list(project_id)]}

    @app.get("/jobs/{job_id}", dependencies=dep)
    def job(job_id: str, since: int = 0, ctx: Ctx = Depends()):
        j = ctx.c.jobs.get(job_id)
        if j is None:
            raise HTTPException(404, "No such job.")
        return j.public(since)

    return app
