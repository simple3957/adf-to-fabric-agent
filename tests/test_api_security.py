"""Negative security tests and regressions for the API: authentication, authorisation, tenancy, request protection, SSRF, uploads, audit.

The permission table below is written by hand, independently of the application code: if a route's requirement changes, this table
must change with it on purpose. Every test builds a real app in token mode with real users in a real (temporary) user store.
"""

from __future__ import annotations

import io
import json
import re
import time
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from adf2fabric.api.app import create_app
from adf2fabric.api.container import Settings
from adf2fabric.api.demo import build_demo_container
from adf2fabric.api.security import Grants, SecuritySettings, UserError, UserStore
from adf2fabric.api.security import guards
from adf2fabric.api.security.audit import AuditLog
from adf2fabric.api.security.permissions import ROLES, Permission as P, permissions_for

SUB = "00000000-0000-0000-0000-0000000005b1"  # the demo subscription
WS = "11111111-1111-1111-1111-111111111111"
OTHER_WS = "22222222-2222-2222-2222-222222222222"
ALL = Grants(subscriptions=["*"], tenants=["*"], accounts=["*"], workspaces=["*"])

USERS = {  # id -> (org, roles)
    "vic": ("default", ["viewer"]), "ann": ("default", ["analyst"]), "abe": ("default", ["approver"]), "oscar": ("default", ["operator"]),
    "root": ("default", ["admin"]), "bob": ("other", ["admin"]), "bea": ("other", ["analyst"]),
}


class World:
    """An app in token mode, its users and their tokens."""

    def __init__(self, tmp_path: Path, **sec_kw):
        self.demo = build_demo_container()
        self.demo.upload_dir = tmp_path / "uploads"
        self.demo.upload_dir.mkdir()
        sec_kw.setdefault("rate_per_min", 100000)
        sec_kw.setdefault("sensitive_per_min", 100000)
        self.app = create_app(Settings(), live=self.demo, demo=self.demo, security=SecuritySettings(mode="token", **sec_kw))
        self.users: UserStore = self.app.state.users
        self.audit: AuditLog = self.app.state.audit
        self.tokens: dict[str, str] = {}
        for uid, (org, roles) in USERS.items():
            _, tok = self.users.create(uid, org, roles, ALL)
            self.tokens[uid] = tok
        self.http = TestClient(self.app, base_url="http://localhost", raise_server_exceptions=False)

    def call(self, who, method, path, *, json_body=None, headers=None, **kw):
        h = {"X-Session-Id": "s1", **(headers or {})}
        if who:
            h["Authorization"] = f"Bearer {self.tokens[who]}"
        if json_body is not None:
            kw["json"] = json_body
        elif method.upper() in ("POST", "PUT", "PATCH", "DELETE") and "content" not in kw and "data" not in kw:
            kw["json"] = {}
        return self.http.request(method, path, headers=h, **kw)

    def wait(self, who, job, timeout=60):
        end = time.time() + timeout
        while time.time() < end:
            j = self.call(who, "GET", f"/jobs/{job['id']}").json()
            if j["state"] in ("succeeded", "failed"):
                return j
            time.sleep(0.05)
        raise AssertionError("job did not finish")


@pytest.fixture()
def world(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    return World(tmp_path)


def make_zip(files: dict[str, str | bytes] | None = None, **extra) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in (files or {"pipeline/PL_A.json": json.dumps({"name": "PL_A", "properties": {"activities": []}})}).items():
            z.writestr(name, data)
        for name, info in extra.items():
            z.writestr(info[0], info[1])
    return buf.getvalue()


# =====================================================================================================================
# 1. authentication: nothing protected is reachable without a valid token
# =====================================================================================================================

PROTECTED = [("GET", "/me"), ("GET", "/mode"), ("POST", "/mode"), ("GET", "/session"), ("GET", "/projects"), ("GET", "/projects/demo-estate"),
             ("GET", "/projects/demo-estate/conversion"), ("GET", "/projects/demo-estate/handover"), ("GET", "/projects/demo-estate/linked-services"),
             ("GET", "/projects/demo-estate/connections"), ("GET", "/projects/demo-estate/connections/report"), ("GET", "/connectors"), ("GET", "/jobs"),
             ("GET", "/projects/demo-estate/bindings"), ("PUT", "/projects/demo-estate/bindings"),
             ("GET", "/jobs/abc"), ("GET", "/auth/azure_cli/status"), ("GET", "/auth/azure_cli/accounts"), ("GET", "/auth/azure_cli/tenants"),
             ("POST", "/auth/azure_cli/login"), ("POST", "/auth/azure_cli/logout"), ("GET", "/azure/subscriptions"), ("GET", "/azure/discover"),
             ("PUT", "/session/source"), ("POST", "/test/source"), ("PUT", "/uploads/zip"), ("PUT", "/session/destination"), ("GET", "/fabric/workspaces"),
             ("GET", "/fabric/capacities"), ("POST", "/test/destination"), ("POST", "/projects"), ("POST", "/projects/demo-estate/stages/discovery"),
             ("PUT", "/projects/demo-estate/connections/LS/choice"), ("PUT", "/projects/demo-estate/connections/LS/secrets"),
             ("DELETE", "/projects/demo-estate/connections/secrets"), ("POST", "/projects/demo-estate/connections/LS/test"),
             ("POST", "/projects/demo-estate/connections/apply"), ("POST", "/projects/demo-estate/connections/workspace-identity"),
             ("GET", "/admin/users"), ("POST", "/admin/users"), ("PATCH", "/admin/users/vic"), ("POST", "/admin/users/vic/rotate-token"), ("GET", "/admin/audit")]


@pytest.mark.parametrize("method,path", PROTECTED)
def test_every_protected_route_refuses_an_anonymous_caller(world, method, path):
    r = world.call(None, method, path)
    assert r.status_code == 401 and r.headers.get("www-authenticate") == "Bearer"
    assert "Traceback" not in r.text and "token" not in r.json()["error"].lower().replace("bearer token is required", "")


def test_only_health_is_public(world):
    assert world.call(None, "GET", "/health").json() == {"status": "ok", "version": "0.1.0"}


def test_a_wrong_malformed_or_disabled_token_is_refused(world):
    bad = [{"Authorization": "Bearer nope"}, {"Authorization": "Bearer "}, {"Authorization": "Basic abc"}, {"Authorization": world.tokens["vic"]},
           {"Authorization": f"Bearer {world.tokens['vic']}x"}]
    for h in bad:
        assert world.http.get("/me", headers=h).status_code == 401
    assert world.call("vic", "GET", "/me").status_code == 200
    world.users.update("vic", disabled=True)
    assert world.call("vic", "GET", "/me").status_code == 401  # a disabled user's token stops working immediately


def test_failed_sign_ins_lock_the_client_out_and_are_audited(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    w = World(tmp_path, auth_fail_limit=3, auth_fail_window=60)
    for _ in range(3):
        assert w.http.get("/me", headers={"Authorization": "Bearer wrong"}).status_code == 401
    guess = w.http.get("/me", headers={"Authorization": "Bearer wrong"})
    assert guess.status_code == 429 and int(guess.headers["retry-after"]) >= 1  # the address's further guesses wait: this is the brute-force defence
    assert w.http.get("/me", headers={"Authorization": f"Bearer {w.tokens['vic']}"}).status_code == 200  # a real user behind the same address is not locked out
    actions = [e["action"] for e in w.audit.read()]
    assert actions.count("auth.failure") == 3 and "auth.locked_out" in actions


def test_the_user_store_holds_only_hashes_and_shows_a_token_once(world):
    raw = (world.users.path).read_text(encoding="utf-8")
    assert all(t not in raw for t in world.tokens.values()) and "token_sha256" in raw
    rec, tok = world.users.create("newbie", "default", ["viewer"])
    assert tok.startswith("a2f_") and tok not in world.users.path.read_text(encoding="utf-8")
    listed = world.call("root", "GET", "/admin/users").json()["users"]
    assert all("token" not in u or u["token"] is None for u in listed) and all(set(u) >= {"token_hint"} for u in listed)


def test_a_damaged_user_store_locks_everyone_out(world):
    world.users.path.write_text("users: [ {id: x", encoding="utf-8")
    r = world.call("vic", "GET", "/me")
    assert r.status_code == 503 and "unavailable" in r.json()["error"]


def test_the_legacy_shared_token_still_works_as_one_admin_and_a_wrong_one_does_not(tmp_path, monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_API_TOKEN", "legacy-shared-token-value-123")
    demo = build_demo_container()
    app = create_app(Settings(), live=demo, demo=demo)
    c = TestClient(app, base_url="http://localhost", raise_server_exceptions=False)
    ok = c.get("/me", headers={"Authorization": "Bearer legacy-shared-token-value-123"})
    assert ok.status_code == 200 and ok.json()["method"] == "legacy_token" and ok.json()["auth_mode"] == "token"
    assert c.get("/me", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert c.get("/me").status_code == 401


def test_local_mode_is_one_implicit_user_and_says_so(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    monkeypatch.delenv("ADF2FABRIC_AUTH_MODE", raising=False)
    demo = build_demo_container()
    c = TestClient(create_app(Settings(), live=demo, demo=demo), base_url="http://localhost")
    me = c.get("/me").json()
    assert me["method"] == "local" and me["auth_mode"] == "disabled"


def test_the_server_refuses_to_listen_beyond_loopback_without_authentication(monkeypatch):
    import sys

    from adf2fabric.api import __main__ as entry

    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    monkeypatch.delenv("ADF2FABRIC_AUTH_MODE", raising=False)
    monkeypatch.setattr(sys, "argv", ["adf2fabric.api", "--host", "0.0.0.0"])
    monkeypatch.setattr("adf2fabric.api.app.Container", lambda settings: build_demo_container())
    with pytest.raises(SystemExit) as e:
        entry.main()
    assert "Refusing to bind beyond loopback" in str(e.value)


# =====================================================================================================================
# 2. authorisation: a hand-written permission table, checked for every role
# =====================================================================================================================

G = P  # short alias for the table
ROUTES = [  # (method, path, body, permissions required)
    ("GET", "/mode", None, {G.projects_view}), ("POST", "/mode", {"mode": "demo"}, {G.projects_view}), ("GET", "/session", None, {G.projects_view}),
    ("GET", "/projects", None, {G.projects_view}), ("GET", "/projects/demo-estate", None, {G.projects_view}),
    ("GET", "/projects/demo-estate/linked-services", None, {G.projects_view}), ("GET", "/connectors", None, {G.projects_view}),
    ("GET", "/projects/demo-estate/conversion", None, {G.reports_view}), ("GET", "/projects/demo-estate/handover", None, {G.reports_view}),
    ("GET", "/projects/demo-estate/connections", None, {G.reports_view}), ("GET", "/projects/demo-estate/connections/report", None, {G.reports_view}),
    ("GET", "/jobs", None, {G.reports_view}),
    ("GET", "/auth/azure_cli/status", None, {G.azure_use}), ("GET", "/auth/azure_cli/accounts", None, {G.azure_use}),
    ("GET", "/auth/azure_cli/tenants", None, {G.azure_use}), ("GET", "/azure/subscriptions", None, {G.azure_use}),
    ("GET", f"/azure/discover?subscription_id={SUB}", None, {G.discovery_run, G.azure_use}),
    ("GET", "/fabric/workspaces", None, {G.azure_use}), ("GET", "/fabric/capacities", None, {G.azure_use}),
    ("PUT", "/session/destination", {"provider": "azure_cli"}, {G.azure_use}),
    ("PUT", "/session/source", {"kind": "git", "repository_url": "https://github.com/o/r"}, {G.sources_import}),
    ("POST", "/test/source", {"kind": "zip", "upload_id": "nope"}, {G.sources_import}),
    ("POST", "/auth/azure_cli/login", {}, {G.credentials_manage}), ("POST", "/auth/azure_cli/logout", {}, {G.credentials_manage}),
    ("POST", "/projects/demo-estate/stages/discovery", {}, {G.projects_view, G.discovery_run}),
    ("POST", "/projects/demo-estate/stages/semantic", {}, {G.projects_view, G.discovery_run}),
    ("POST", "/projects/demo-estate/stages/convert", {}, {G.projects_view, G.plans_generate}),
    ("POST", "/projects/demo-estate/stages/deploy_dry_run", {"workspace_id": WS}, {G.projects_view, G.plans_generate}),
    ("POST", "/projects/demo-estate/stages/deploy_apply", {"workspace_id": WS}, {G.projects_view, G.migration_execute}),
    ("GET", "/projects/demo-estate/bindings", None, {G.reports_view}),
    ("PUT", "/projects/demo-estate/bindings", {"key": "notebook:Nope", "value": "00000000-0000-0000-0000-000000000001"}, {G.plans_generate}),
    ("PUT", "/projects/demo-estate/connections/LS_Sales_SQL/choice", {"method": "sql_login", "values": {"username": "u"}}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/connections/LS_Sales_SQL/test", {}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/connections/apply", {"dry_run": True, "workspace_id": WS}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/connections/apply", {"dry_run": False, "workspace_id": WS}, {G.plans_generate, G.migration_execute}),
    ("POST", "/projects/demo-estate/connections/workspace-identity", {"workspace_id": WS}, {G.provision_approve, G.migration_execute}),
    ("GET", "/projects/demo-estate/inventory", None, {G.reports_view}),
    ("GET", "/projects/demo-estate/waves", None, {G.reports_view}), ("GET", "/projects/demo-estate/confidence", None, {G.reports_view}), ("GET", "/projects/demo-estate/compatibility", None, {G.reports_view}),
    ("GET", "/projects/demo-estate/validation", None, {G.reports_view}), ("GET", "/projects/demo-estate/report", None, {G.reports_view}),
    ("GET", "/projects/demo-estate/provisioning", None, {G.reports_view}),
    ("POST", "/projects/demo-estate/stages/dependencies", {}, {G.projects_view, G.discovery_run}),
    ("POST", "/projects/demo-estate/stages/workloads", {}, {G.projects_view, G.plans_generate}),
    ("POST", "/projects/demo-estate/stages/validate", {"validate_stages": ["static"]}, {G.projects_view, G.plans_generate}),
    ("POST", "/projects/demo-estate/provisioning/plan", {"workspace_id": WS}, {G.plans_generate, G.azure_use}),
    ("POST", "/projects/demo-estate/provisioning/approve", {"plan_ref": "0" * 16}, {G.provision_approve}),
    ("POST", "/projects/demo-estate/provisioning/apply", {"plan_ref": "0" * 16}, {G.migration_execute}),
    ("POST", "/projects/demo-estate/provisioning/verify", {}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/stages/verify", {"workspace_id": WS}, {G.projects_view, G.plans_generate}),
    ("POST", "/projects/demo-estate/stages/verify", {"workspace_id": WS, "run": True, "confirm": True, "pipelines": ["PL"]},
     {G.projects_view, G.plans_generate, G.migration_execute}),
    ("GET", "/projects/demo-estate/journey", None, {G.projects_view}),
    ("PUT", "/projects/demo-estate/journey", {"steps": ["discover"]}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/journey/reset", {"steps": ["discover"]}, {G.plans_generate}),
    ("GET", "/projects/demo-estate/assist", None, {G.reports_view}), ("GET", "/projects/demo-estate/assist/PL_X", None, {G.reports_view}),
    ("GET", "/projects/demo-estate/assist/PL_X/prompt", None, {G.reports_view}),
    ("POST", "/projects/demo-estate/assist/generate", {"names": ["PL_X"]}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/assist/PL_X/accept", {}, {G.plans_generate}), ("POST", "/projects/demo-estate/assist/PL_X/discard", {}, {G.plans_generate}),
    ("GET", "/projects/demo-estate/brief", None, {G.reports_view}), ("GET", "/projects/demo-estate/brief/markdown", None, {G.reports_view}),
    ("GET", "/projects/demo-estate/brief/questionnaire", None, {G.reports_view}),
    ("PUT", "/projects/demo-estate/brief/answers", {"answers": {"scope.approach": "phased"}}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/brief/accept", {}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/brief/submit", {}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/brief/approve", {"fingerprint": "0" * 16}, {G.provision_approve}),
    ("POST", "/projects/demo-estate/brief/import", {"questionnaire": {}}, {G.plans_generate}),
    ("POST", "/projects/demo-estate/brief/notes", {"text": "note"}, {G.plans_generate}),
    ("GET", "/admin/users", None, {G.users_manage}), ("GET", "/admin/audit", None, {G.users_manage}),
]
ROLE_USERS = {"viewer": "vic", "analyst": "ann", "approver": "abe", "operator": "oscar", "admin": "root"}


@pytest.mark.parametrize("role", list(ROLE_USERS))
def test_each_role_can_do_exactly_what_the_table_says(world, role):
    held = permissions_for([role])
    wrong = []
    for method, path, body, needs in ROUTES:
        r = world.call(ROLE_USERS[role], method, path, json_body=body)
        allowed = needs <= held
        if allowed and r.status_code in (401, 403):
            wrong.append(f"{role} should be allowed {method} {path}, got {r.status_code}")
        if not allowed and r.status_code != 403:
            wrong.append(f"{role} should be refused {method} {path} (needs {sorted(p.value for p in needs)}), got {r.status_code}")
    assert wrong == []


def test_the_roles_are_least_privilege():
    assert permissions_for(["viewer"]) == {P.projects_view, P.reports_view}
    assert P.migration_execute not in permissions_for(["analyst"]) and P.provision_approve not in permissions_for(["analyst"])
    assert P.migration_execute not in permissions_for(["approver"])  # approving is not executing: separation of duties
    assert P.users_manage not in permissions_for(["operator"]) and P.credentials_manage not in permissions_for(["operator"])
    assert permissions_for(["admin"]) == set(P)
    assert set(ROLES) == {"viewer", "analyst", "approver", "operator", "admin"}


def test_a_denied_request_is_audited_without_leaking_what_it_would_have_returned(world):
    r = world.call("vic", "POST", "/projects/demo-estate/stages/deploy_apply", json_body={"workspace_id": WS})
    assert r.status_code == 403 and r.json()["error"] == "Your role does not allow this action."
    ev = [e for e in world.audit.read() if e["action"] == "authz.denied"][-1]
    assert ev["actor"] == "vic" and ev["outcome"] == "denied" and "migration:execute" in ev["detail"]["needs"]


def test_hiding_a_button_is_not_authorisation(world):
    # a viewer's /me lists no execute permission, yet calling the API directly is what the server refuses
    me = world.call("vic", "GET", "/me").json()
    assert "migration:execute" not in me["permissions"]
    assert world.call("vic", "POST", "/projects/demo-estate/connections/apply", json_body={"dry_run": False, "workspace_id": WS}).status_code == 403


# =====================================================================================================================
# 3. tenancy: projects, jobs, sessions and uploads belong to an organisation (IDOR)
# =====================================================================================================================


def test_another_organisation_cannot_see_or_touch_a_project(world):
    missing = world.call("bob", "GET", "/projects/does-not-exist")
    foreign = world.call("bob", "GET", "/projects/demo-estate")
    assert foreign.status_code == missing.status_code == 404
    assert foreign.json()["error"].replace("demo-estate", "X") == missing.json()["error"].replace("does-not-exist", "X")  # indistinguishable
    assert world.call("bob", "GET", "/projects").json()["projects"] == []
    assert [p["id"] for p in world.call("ann", "GET", "/projects").json()["projects"]] == ["demo-estate"]
    for method, path, body in [("GET", "/projects/demo-estate/conversion", None), ("GET", "/projects/demo-estate/handover", None),
                               ("GET", "/projects/demo-estate/linked-services", None), ("GET", "/projects/demo-estate/connections", None),
                               ("POST", "/projects/demo-estate/stages/discovery", {}), ("POST", "/projects/demo-estate/stages/deploy_dry_run", {"workspace_id": WS}),
                               ("PUT", "/projects/demo-estate/connections/LS_Sales_SQL/choice", {"method": "sql_login"}),
                               ("PUT", "/projects/demo-estate/connections/LS_Sales_SQL/secrets", {"field": "password", "value": "x"}),
                               ("POST", "/projects/demo-estate/connections/apply", {"dry_run": True}),
                               ("POST", "/projects/demo-estate/connections/workspace-identity", {"workspace_id": WS})]:
        assert world.call("bob", method, path, json_body=body).status_code == 404, (method, path)


def test_jobs_are_visible_only_to_their_organisation(world):
    job = world.call("ann", "POST", "/projects/demo-estate/stages/discovery", json_body={}).json()
    assert world.wait("ann", job)["state"] == "succeeded"
    assert world.call("oscar", "GET", f"/jobs/{job['id']}").status_code == 200  # same organisation
    assert world.call("bob", "GET", f"/jobs/{job['id']}").status_code == 404
    assert world.call("bob", "GET", "/jobs").json()["jobs"] == [] and world.call("bob", "GET", "/jobs?project_id=demo-estate").status_code == 404
    assert [j["id"] for j in world.call("ann", "GET", "/jobs").json()["jobs"]] == [job["id"]]


def test_sessions_never_cross_principals_even_with_the_same_session_id(world):
    world.call("ann", "PUT", "/session/source", json_body={"kind": "git", "repository_url": "https://github.com/o/r"}, headers={"X-Session-Id": "shared"})
    mine = world.call("ann", "GET", "/session", headers={"X-Session-Id": "shared"}).json()
    other = world.call("oscar", "GET", "/session", headers={"X-Session-Id": "shared"}).json()
    assert mine["source"]["repository_url"] == "https://github.com/o/r" and other["source"] == {}
    world.call("ann", "POST", "/mode", json_body={"mode": "demo"}, headers={"X-Session-Id": "shared"})


def test_held_secrets_are_per_principal_and_never_returned(world):
    secret = "Sup3r-S3cret-ZZ-991"
    world.call("ann", "PUT", "/projects/demo-estate/connections/LS_Sales_SQL/choice", json_body={"method": "sql_login", "values": {"username": "u"}})
    r = world.call("ann", "PUT", "/projects/demo-estate/connections/LS_Sales_SQL/secrets", json_body={"field": "password", "value": secret})
    assert r.status_code == 200 and secret not in r.text
    held = [c for c in world.call("ann", "GET", "/projects/demo-estate/connections").json()["cards"] if c["name"] == "LS_Sales_SQL"][0]["secrets_held"]
    seen_by_oscar = [c for c in world.call("oscar", "GET", "/projects/demo-estate/connections").json()["cards"] if c["name"] == "LS_Sales_SQL"][0]["secrets_held"]
    assert held == ["password"] and seen_by_oscar == []  # oscar shares the session id "s1" and the project, not the vault
    assert secret not in json.dumps(world.audit.read()) and secret not in world.users.path.read_text(encoding="utf-8")


def test_an_import_is_owned_by_the_importers_organisation_and_cannot_overwrite_another(world):
    up = world.call("ann", "PUT", "/uploads/zip", content=make_zip(), headers={"Content-Type": "application/zip"})
    assert up.status_code == 200
    job = world.call("ann", "POST", "/projects", json_body={"project_name": "Team Alpha", "source": {"kind": "zip", "upload_id": up.json()["upload_id"]}}).json()
    assert world.wait("ann", job)["state"] == "succeeded"
    assert "team-alpha" in [p["id"] for p in world.call("oscar", "GET", "/projects").json()["projects"]]
    assert world.call("bob", "GET", "/projects/team-alpha").status_code == 404
    up2 = world.call("bea", "PUT", "/uploads/zip", content=make_zip(), headers={"Content-Type": "application/zip"})
    clash = world.call("bea", "POST", "/projects", json_body={"project_name": "Team Alpha", "source": {"kind": "zip", "upload_id": up2.json()["upload_id"]}})
    assert clash.status_code == 409 and "not available" in clash.json()["error"]  # another organisation's project is never overwritten
    assert (world.demo.store.project_dir("team-alpha") / "access.yaml").read_text(encoding="utf-8").count("default") >= 1


def test_uploads_belong_to_their_uploader(world):
    up = world.call("ann", "PUT", "/uploads/zip", content=make_zip(), headers={"Content-Type": "application/zip"}).json()["upload_id"]
    body = {"kind": "zip", "upload_id": up}
    assert world.call("ann", "POST", "/test/source", json_body=body).status_code == 200
    assert world.call("oscar", "POST", "/test/source", json_body=body).status_code == 404  # same organisation, different user
    assert world.call("bea", "POST", "/test/source", json_body=body).status_code == 404  # another organisation


# =====================================================================================================================
# 4. what a client may point the server at
# =====================================================================================================================


def make_limited(world, uid="lim", **grants):
    _, tok = world.users.create(uid, "default", ["operator"], Grants(**grants))
    world.tokens[uid] = tok
    return uid


def test_subscriptions_tenants_accounts_and_workspaces_must_be_granted(world):
    lim = make_limited(world, subscriptions=[], workspaces=[WS])
    assert world.call(lim, "GET", f"/azure/discover?subscription_id={SUB}").status_code == 403
    assert world.call(lim, "GET", "/azure/subscriptions").json()["subscriptions"] == []  # not even listed
    assert world.call("ann", "GET", "/azure/subscriptions").json()["subscriptions"][0]["id"] == SUB
    assert world.call(lim, "PUT", "/session/destination", json_body={"provider": "azure_cli", "workspace_id": OTHER_WS}).status_code == 403
    assert world.call(lim, "PUT", "/session/destination", json_body={"provider": "azure_cli", "workspace_id": WS}).status_code == 200
    assert world.call(lim, "POST", "/projects/demo-estate/stages/deploy_dry_run", json_body={"workspace_id": OTHER_WS}).status_code == 403
    assert world.call(lim, "POST", "/projects/demo-estate/connections/apply", json_body={"dry_run": True, "workspace_id": OTHER_WS}).status_code == 403
    assert world.call(lim, "POST", "/test/destination", json_body={"provider": "azure_cli", "workspace_id": OTHER_WS}).status_code == 403
    assert world.call(lim, "GET", "/auth/azure_cli/accounts").json()["accounts"] == []
    assert any(e["action"] == "grant.denied" for e in world.audit.read())


def test_a_client_cannot_choose_an_arbitrary_subscription_when_importing_from_azure(world):
    lim = make_limited(world, subscriptions=[SUB])
    other = "33333333-3333-3333-3333-333333333333"
    r = world.call(lim, "POST", "/projects", json_body={"project_name": "Az One", "source": {"kind": "azure", "subscription_id": other, "resource_group": "rg", "factory_name": "f"}})
    assert r.status_code == 403
    r2 = world.call(lim, "POST", "/test/source", json_body={"kind": "azure", "subscription_id": "not-a-guid", "resource_group": "rg", "factory_name": "f"})
    assert r2.status_code == 422


def test_resource_names_and_ids_are_validated(world):
    bad_names = ["../x", "a/b", "rg;rm -rf", "x" * 200, "-lead"]
    for name in bad_names:
        r = world.call("ann", "POST", "/test/source", json_body={"kind": "azure", "subscription_id": SUB, "resource_group": name, "factory_name": "f"})
        assert r.status_code == 422, name
    assert world.call("ann", "GET", "/azure/discover?subscription_id=../../etc").status_code == 422
    assert world.call("ann", "GET", "/session", headers={"X-Session-Id": "../etc/passwd"}).status_code == 400


@pytest.mark.parametrize("url", ["http://github.com/o/r", "file:///etc/passwd", "ssh://git@github.com/o/r", "git@github.com:o/r.git", "https://127.0.0.1/r.git",
                                 "https://169.254.169.254/latest/meta-data", "https://localhost/r.git", "https://10.0.0.5/r.git", "https://[::1]/r.git",
                                 "https://user:pw@github.com/o/r", "https://github.com:8443/o/r", "https://metadata.google.internal/x", "https://db.corp/x",
                                 "ftp://host/r", "https://192.168.1.1/r.git", "https://[::ffff:10.0.0.1]/r.git", "https://0.0.0.0/r"])
def test_ssrf_urls_are_refused_for_git_sources(world, url):
    for method, path, body in [("POST", "/test/source", {"kind": "git", "repository_url": url}), ("PUT", "/session/source", {"kind": "git", "repository_url": url}),
                               ("POST", "/projects", {"project_name": "Ssrf", "source": {"kind": "git", "repository_url": url}})]:
        r = world.call("ann", method, path, json_body=body)
        assert r.status_code == 422, (method, path, url, r.text)
    assert not world.demo.store.exists("ssrf")


def test_the_ssrf_guard_resolves_names_and_honours_an_allow_list():
    pub = lambda h: ["140.82.112.3"]  # noqa: E731
    priv = lambda h: ["10.1.2.3"]  # noqa: E731
    mixed = lambda h: ["140.82.112.3", "169.254.169.254"]  # noqa: E731
    assert guards.check_public_https_url("https://github.com/o/r.git", resolver=pub)
    for resolver in (priv, mixed, lambda h: []):
        with pytest.raises(guards.Rejected):
            guards.check_public_https_url("https://github.com/o/r.git", resolver=resolver)
    with pytest.raises(guards.Rejected):
        guards.check_public_https_url("https://evil.example/o/r", allowed_hosts=["github.com"], resolver=pub)
    assert guards.check_public_https_url("https://dev.azure.com/o/p/_git/r", allowed_hosts=["dev.azure.com"], resolver=pub)
    assert guards.check_public_https_url("https://sub.github.com/o/r", allowed_hosts=["github.com"], resolver=pub)


def test_git_revisions_and_subdirectories_cannot_inject_options_or_escape(world):
    for rev in ("--upload-pack=evil", "-x", "a b", "$(id)", "x;y", "a" * 300):
        assert world.call("ann", "POST", "/test/source", json_body={"kind": "git", "repository_url": "https://github.com/o/r", "revision": rev}).status_code == 422, rev
    for sub in ("../..", "/etc", "C:\\Windows", "a/../../b", "x" * 300):
        assert world.call("ann", "PUT", "/session/source", json_body={"kind": "git", "repository_url": "https://github.com/o/r", "subdirectory": sub}).status_code == 422, sub


def test_a_certificate_path_cannot_be_supplied_through_the_api(world):
    r = world.call("root", "POST", "/auth/fabric_cli/login", json_body={"method": "service_principal_certificate", "certificate_path": "C:\\secrets\\x.pfx"})
    assert r.status_code == 422 and "cannot be supplied" in r.json()["error"]


# =====================================================================================================================
# 5. path traversal and malicious archives
# =====================================================================================================================


@pytest.mark.parametrize("pid", ["..", "../x", "..%2f..%2fetc", "....//....//etc", "%2e%2e", "C:%5Cwindows", "a%00b", "x" * 300])
def test_project_ids_cannot_traverse(world, pid):
    for path in (f"/projects/{pid}", f"/projects/{pid}/conversion", f"/projects/{pid}/connections"):
        assert world.call("root", "GET", path).status_code in (404, 405), path
    assert world.call("root", "POST", f"/projects/{pid}/stages/discovery", json_body={}).status_code in (404, 405)


def test_upload_ids_cannot_traverse(world):
    for up in ("../x", "..\\x", "a/b", "x" * 100, "C:evil"):
        assert world.call("ann", "POST", "/test/source", json_body={"kind": "zip", "upload_id": up}).status_code == 422, up


def test_an_upload_must_be_a_zip_and_is_capped_and_counted(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    w = World(tmp_path)
    assert w.call("ann", "PUT", "/uploads/zip", content=b"%PDF-1.4 not a zip", headers={"Content-Type": "application/zip"}).status_code == 415
    assert w.call("ann", "PUT", "/uploads/zip", content=b"", headers={"Content-Type": "application/zip"}).status_code in (415, 422)
    assert not list((tmp_path / "uploads").glob("*.zip")), "a rejected upload must not stay on disk"
    for _ in range(5):
        assert w.call("ann", "PUT", "/uploads/zip", content=make_zip(), headers={"Content-Type": "application/zip"}).status_code == 200
    assert w.call("ann", "PUT", "/uploads/zip", content=make_zip(), headers={"Content-Type": "application/zip"}).status_code == 429  # a disk-filling loop stops
    assert w.call("bea", "PUT", "/uploads/zip", content=make_zip(), headers={"Content-Type": "application/zip"}).status_code == 200  # per user, not global


def test_the_size_cap_applies_while_streaming(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    demo = build_demo_container()
    demo.upload_dir = tmp_path
    app = create_app(Settings(max_upload_bytes=100), live=demo, demo=demo, security=SecuritySettings(mode="token", rate_per_min=100000, sensitive_per_min=100000))
    _, tok = app.state.users.create("u", "default", ["analyst"], ALL)
    c = TestClient(app, base_url="http://localhost", raise_server_exceptions=False)
    r = c.put("/uploads/zip", content=make_zip({"pipeline/P.json": "x" * 5000}), headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/zip"})
    assert r.status_code == 413 and not list(tmp_path.glob("*.zip"))


def _evil_zip(kind: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("pipeline/PL_A.json", json.dumps({"name": "PL_A", "properties": {"activities": []}}))
        if kind == "slip":
            z.writestr("../evil.json", "{}")
        elif kind == "absolute":
            z.writestr("/tmp/evil.json", "{}")
        elif kind == "drive":
            z.writestr("C:/evil.json", "{}")
        elif kind == "symlink":
            info = zipfile.ZipInfo("pipeline/link.json")
            info.external_attr = 0o120777 << 16
            z.writestr(info, "/etc/passwd")
        elif kind == "bomb":
            z.writestr("pipeline/big.json", b"0" * (64 * 1024 * 1024))
        elif kind == "nested_dots":
            z.writestr("pipeline/../../evil.json", "{}")
    return buf.getvalue()


@pytest.mark.parametrize("kind", ["slip", "absolute", "drive", "symlink", "bomb", "nested_dots"])
def test_malicious_archives_are_rejected_before_anything_is_written(world, tmp_path, kind):
    up = world.call("ann", "PUT", "/uploads/zip", content=_evil_zip(kind), headers={"Content-Type": "application/zip"})
    assert up.status_code == 200  # stored untouched; nothing is extracted on upload
    uid = up.json()["upload_id"]
    t = world.call("ann", "POST", "/test/source", json_body={"kind": "zip", "upload_id": uid})
    assert t.status_code == 422 and "rejected" in t.json()["error"]
    job = world.call("ann", "POST", "/projects", json_body={"project_name": f"Evil {kind}", "source": {"kind": "zip", "upload_id": uid}}).json()
    assert world.wait("ann", job)["state"] == "failed"
    assert not world.demo.store.exists(f"evil-{kind}")
    assert not list(tmp_path.parent.glob("evil.json")) and not list(tmp_path.glob("**/evil.json"))
    assert not (world.demo.upload_dir / f"{uid}.zip").exists(), "the archive is discarded whether the import worked or not"


# =====================================================================================================================
# 6. CSRF, DNS rebinding, CORS, content type, size, headers
# =====================================================================================================================


def test_a_foreign_host_header_is_refused(world):
    r = world.http.get("/health", headers={"Host": "evil.example"})
    assert r.status_code == 400 and "host" in r.json()["error"].lower()
    assert world.http.get("/health", headers={"Host": "localhost:8765"}).status_code == 200
    assert world.http.get("/health", headers={"Host": "127.0.0.1:8765"}).status_code == 200


def test_a_state_changing_request_from_a_foreign_origin_is_refused_even_with_a_valid_token(world):
    h = {"Origin": "https://evil.example"}
    assert world.call("ann", "POST", "/mode", json_body={"mode": "demo"}, headers=h).status_code == 403
    assert world.call("ann", "PUT", "/uploads/zip", content=make_zip(), headers={**h, "Content-Type": "application/zip"}).status_code == 403
    assert world.call("ann", "POST", "/mode", json_body={"mode": "demo"}, headers={"Origin": "http://localhost:8501"}).status_code == 200  # the UI's origin


def test_state_changes_must_declare_json(world):
    for ctype in ("text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x", ""):
        h = {"Authorization": f"Bearer {world.tokens['ann']}", **({"Content-Type": ctype} if ctype else {})}
        r = world.http.post("/mode", content=b'{"mode":"demo"}', headers=h)
        assert r.status_code == 415, ctype  # the simple-request forms a cross-site page can send without a preflight
    r = world.http.post("/mode", headers={"Authorization": f"Bearer {world.tokens['ann']}"})  # body-less POST, no content type
    assert r.status_code == 415


def test_oversized_and_undeclared_json_bodies_are_refused(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    w = World(tmp_path, max_json_bytes=2000)
    big = {"project_name": "x", "source": {"kind": "git", "repository_url": "https://github.com/o/r"}, "pipelines": ["p" * 100] * 50}
    assert w.call("ann", "POST", "/projects", json_body=big).status_code == 413

    def chunks():
        yield b'{"mode":'
        yield b'"demo"}'

    r = w.http.post("/mode", content=chunks(), headers={"Authorization": f"Bearer {w.tokens['ann']}", "Content-Type": "application/json"})
    assert r.status_code == 411


def test_cors_allows_only_configured_origins_and_never_credentials(world):
    ok = world.http.options("/projects", headers={"Origin": "http://localhost:8501", "Access-Control-Request-Method": "GET"})
    assert ok.headers.get("access-control-allow-origin") == "http://localhost:8501" and "access-control-allow-credentials" not in ok.headers
    bad = world.http.options("/projects", headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"})
    assert "access-control-allow-origin" not in bad.headers
    methods = ok.headers.get("access-control-allow-methods", "")
    assert "TRACE" not in methods and "CONNECT" not in methods


def test_security_headers_and_no_api_documentation_surface(world):
    r = world.call("vic", "GET", "/me")
    assert r.headers["x-content-type-options"] == "nosniff" and r.headers["x-frame-options"] == "DENY" and r.headers["cache-control"] == "no-store"
    assert r.headers["referrer-policy"] == "no-referrer" and "default-src 'none'" in r.headers["content-security-policy"] and r.headers["x-request-id"]
    for p in ("/docs", "/redoc", "/openapi.json"):
        assert world.call("root", "GET", p).status_code == 404


# =====================================================================================================================
# 7. rate limiting
# =====================================================================================================================


def test_sensitive_operations_are_rate_limited_per_principal(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    w = World(tmp_path, sensitive_per_min=3)
    codes = [w.call("ann", "PUT", "/session/source", json_body={"kind": "git", "repository_url": "https://github.com/o/r"}).status_code for _ in range(5)]
    assert codes[:3] == [200, 200, 200] and codes[3:] == [429, 429]
    r = w.call("ann", "PUT", "/session/source", json_body={"kind": "git", "repository_url": "https://github.com/o/r"})
    assert int(r.headers["retry-after"]) >= 1
    assert w.call("oscar", "PUT", "/session/source", json_body={"kind": "git", "repository_url": "https://github.com/o/r"}).status_code == 200  # another principal is unaffected
    assert any(e["action"] == "rate.limited" for e in w.audit.read())


def test_ordinary_reads_have_their_own_higher_limit(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    w = World(tmp_path, rate_per_min=5)
    codes = [w.call("vic", "GET", "/projects").status_code for _ in range(7)]
    assert codes == [200] * 5 + [429] * 2


# =====================================================================================================================
# 8. nothing sensitive in responses, errors, jobs or the audit log
# =====================================================================================================================


def test_validation_errors_do_not_echo_what_was_submitted(world):
    secret = "ZZ-secret-token-VALUE-42"
    r = world.call("ann", "PUT", "/projects/demo-estate/connections/LS_Sales_SQL/secrets", json_body={"field": "password", "value": secret, "extra": secret})
    assert r.status_code == 422 and secret not in r.text
    r2 = world.call("ann", "POST", "/test/source", json_body={"kind": "git", "repository_url": "https://github.com/o/r", "token": secret, "bogus": secret})
    assert r2.status_code == 422 and secret not in r2.text


def test_unexpected_errors_are_generic_and_leak_no_trace_or_path(world, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError(f"failed reading {world.demo.store.projects_root}\\secret\\file and token ghp_abcdefghijklmnopqrstuvwxyz0123456789")

    monkeypatch.setattr(world.demo.connections, "cards_and_status", boom)
    r = world.call("ann", "GET", "/projects/demo-estate/connections")
    assert r.status_code == 500
    assert "Traceback" not in r.text and str(world.demo.store.projects_root) not in r.text and "ghp_" not in r.text and r.json()["error"].startswith("Unexpected RuntimeError")


def test_job_errors_and_logs_never_contain_server_paths_or_tokens(world, monkeypatch):
    import adf2fabric.api.stages as stages

    def boom(c, session, pid, stage, log, options=None):
        log(f"reading {world.demo.store.projects_root}\\x with Bearer abc.def.ghi")
        raise RuntimeError(f"cannot open {world.demo.store.projects_root}\\y token=ghp_abcdefghijklmnopqrstuvwxyz0123456789")

    monkeypatch.setattr("adf2fabric.api.app.run_stage", boom)
    job = world.call("ann", "POST", "/projects/demo-estate/stages/discovery", json_body={}).json()
    j = world.wait("ann", job)
    blob = json.dumps(j)
    assert j["state"] == "failed" and str(world.demo.store.projects_root) not in blob and "ghp_" not in blob and "abc.def.ghi" not in blob


def test_other_organisations_inventory_is_not_exposed_through_errors(world):
    r = world.call("bob", "POST", "/projects/demo-estate/stages/convert", json_body={})
    assert r.status_code == 404 and "demo-estate" not in json.dumps(world.call("bob", "GET", "/projects").json())


# =====================================================================================================================
# 9. audit
# =====================================================================================================================


def test_sensitive_actions_are_audited_with_who_what_and_outcome(world):
    up = world.call("ann", "PUT", "/uploads/zip", content=make_zip(), headers={"Content-Type": "application/zip"}).json()["upload_id"]
    job = world.call("ann", "POST", "/projects", json_body={"project_name": "Audited", "source": {"kind": "zip", "upload_id": up}}).json()
    world.wait("ann", job)
    world.call("oscar", "POST", "/projects/demo-estate/stages/deploy_dry_run", json_body={"workspace_id": WS})
    world.call("root", "POST", "/admin/users", json_body={"id": "carol", "roles": ["viewer"]})
    world.call("vic", "POST", "/projects/demo-estate/stages/deploy_apply", json_body={"workspace_id": WS})
    seen = {(e["action"], e["actor"], e["outcome"]) for e in world.audit.read(500)}
    assert ("upload.zip", "ann", "ok") in seen and ("project.import", "ann", "ok") in seen
    assert ("stage.deploy_dry_run", "oscar", "requested") in seen and ("user.create", "root", "ok") in seen and ("authz.denied", "vic", "denied") in seen
    assert all(e["request_id"] for e in world.audit.read(500) if e["action"].startswith(("authz", "stage")))


def test_the_audit_log_never_holds_secrets_bodies_or_client_addresses(world):
    secret, token = "Aud1t-S3cret-ZZ-77", "ghp_abcdefghijklmnopqrstuvwxyz0123456789"
    world.call("ann", "PUT", "/projects/demo-estate/connections/LS_Sales_SQL/secrets", json_body={"field": "password", "value": secret})
    world.call("ann", "POST", "/test/source", json_body={"kind": "git", "repository_url": "https://github.com/o/r", "token": token})
    world.http.get("/me", headers={"Authorization": f"Bearer {world.tokens['vic']}x"})
    blob = world.audit.path.read_text(encoding="utf-8")
    assert secret not in blob and token not in blob and world.tokens["ann"] not in blob and "testclient" not in blob and "127.0.0.1" not in blob
    assert all(set(e) == {"ts", "request_id", "actor", "org", "action", "outcome", "resource", "resource_id", "detail", "prev"} for e in world.audit.read(500))


def test_the_audit_chain_detects_edits_and_removals(world):
    for _ in range(4):
        world.call("vic", "POST", "/projects/demo-estate/stages/deploy_apply", json_body={"workspace_id": WS})
    assert world.audit.verify() is None
    lines = world.audit.path.read_text(encoding="utf-8").splitlines()
    tampered = list(lines)
    tampered[1] = tampered[1].replace('"denied"', '"allowed"')
    world.audit.path.write_text("\n".join(tampered) + "\n", encoding="utf-8")
    assert world.audit.verify() == 3  # the line AFTER the edited one no longer chains
    world.audit.path.write_text("\n".join(lines[:1] + lines[2:]) + "\n", encoding="utf-8")
    assert world.audit.verify() == 2


def test_audit_values_are_redacted_and_bounded(tmp_path):
    log = AuditLog(tmp_path)
    ev = log.event("x.y", "ok", actor="a", detail_token="Bearer abc.def.ghi", path=f"C:\\Users\\someone\\secret\\file.txt", long="z" * 1000)
    assert "abc.def.ghi" not in json.dumps(ev) and "someone" not in json.dumps(ev) and len(ev["detail"]["long"]) <= 200


# =====================================================================================================================
# 10. users: management stays inside the organisation and cannot escalate
# =====================================================================================================================


def test_only_admins_manage_users_and_only_their_own_organisation(world):
    for who in ("vic", "ann", "abe", "oscar"):
        assert world.call(who, "GET", "/admin/users").status_code == 403
    mine = {u["id"] for u in world.call("root", "GET", "/admin/users").json()["users"]}
    assert mine == {"vic", "ann", "abe", "oscar", "root"}  # never bob / bea
    assert world.call("root", "PATCH", "/admin/users/bob", json_body={"disabled": True}).status_code == 404
    assert world.call("root", "POST", "/admin/users/bob/rotate-token").status_code == 404
    assert world.call("bob", "PATCH", "/admin/users/vic", json_body={"disabled": True}).status_code == 404


def test_a_new_user_always_lands_in_the_creators_organisation_and_the_token_is_shown_once(world):
    r = world.call("bob", "POST", "/admin/users", json_body={"id": "dora", "roles": ["analyst"]})
    assert r.status_code == 201 and r.json()["org"] == "other" and r.json()["token"].startswith("a2f_")
    assert world.call("root", "POST", "/admin/users", json_body={"id": "dora", "roles": ["viewer"], "org": "other"}).status_code == 422  # org is not client-controlled
    again = world.call("bob", "GET", "/admin/users").json()["users"]
    assert "a2f_" not in json.dumps(again) and all("token" not in u for u in again) and any(u["id"] == "dora" for u in again)
    new = {"Authorization": f"Bearer {r.json()['token']}"}
    assert world.http.get("/me", headers=new).json()["org"] == "other"


def test_nobody_can_grant_more_than_they_hold(world):
    _, tok = world.users.create("lowadmin", "default", ["admin"], Grants(subscriptions=[SUB], workspaces=[WS]))
    world.tokens["lowadmin"] = tok
    assert world.call("lowadmin", "POST", "/admin/users", json_body={"id": "x1", "roles": ["viewer"], "grants": {"subscriptions": ["*"]}}).status_code == 403
    assert world.call("lowadmin", "POST", "/admin/users", json_body={"id": "x2", "roles": ["viewer"], "grants": {"workspaces": [OTHER_WS]}}).status_code == 403
    assert world.call("lowadmin", "POST", "/admin/users", json_body={"id": "x3", "roles": ["viewer"], "grants": {"subscriptions": [SUB]}}).status_code == 201
    assert world.call("lowadmin", "PATCH", "/admin/users/x3", json_body={"grants": {"tenants": ["*"]}}).status_code == 403


def test_privilege_escalation_through_role_names_is_refused(world):
    assert world.call("root", "POST", "/admin/users", json_body={"id": "z", "roles": ["superuser"]}).status_code == 422
    assert world.call("root", "POST", "/admin/users", json_body={"id": "../z", "roles": ["viewer"]}).status_code == 422
    assert world.call("root", "POST", "/admin/users", json_body={"id": "z", "roles": []}).status_code == 422
    assert world.call("ann", "POST", "/admin/users", json_body={"id": "z", "roles": ["admin"]}).status_code == 403


def test_the_last_admin_and_yourself_cannot_be_locked_out(world):
    assert world.call("root", "PATCH", "/admin/users/root", json_body={"disabled": True}).status_code == 409  # not yourself
    assert world.call("root", "PATCH", "/admin/users/root", json_body={"roles": ["viewer"]}).status_code == 409  # nor your own admin role
    world.users.create("root2", "default", ["admin"], ALL)
    assert world.users.admin_count("default") == 2
    assert world.call("root", "PATCH", "/admin/users/root2", json_body={"disabled": True}).status_code == 200  # another admin may be disabled
    assert world.users.admin_count("default") == 1
    assert world.call("root", "PATCH", "/admin/users/root", json_body={"disabled": True}).status_code == 409  # and the organisation keeps one


def test_rotating_a_token_invalidates_the_old_one_and_is_audited(world):
    old = world.tokens["ann"]
    r = world.call("root", "POST", "/admin/users/ann/rotate-token")
    assert r.status_code == 200 and r.json()["token"] != old
    assert world.http.get("/me", headers={"Authorization": f"Bearer {old}"}).status_code == 401
    assert world.http.get("/me", headers={"Authorization": f"Bearer {r.json()['token']}"}).status_code == 200
    assert any(e["action"] == "user.rotate_token" and e["resource_id"] == "ann" for e in world.audit.read())


def test_user_management_needs_authentication_to_be_on(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    monkeypatch.delenv("ADF2FABRIC_AUTH_MODE", raising=False)
    demo = build_demo_container()
    c = TestClient(create_app(Settings(), live=demo, demo=demo), base_url="http://localhost")
    assert c.get("/admin/users").status_code == 409


def test_a_user_cannot_be_created_twice_and_ids_are_case_insensitive(world):
    with pytest.raises(UserError):
        world.users.create("ANN", "default", ["viewer"])
    assert world.users.authenticate("a2f_" + "x" * 43) is None


# =====================================================================================================================
# 11. CLI: users, audit
# =====================================================================================================================


def test_cli_creates_users_and_prints_the_token_once(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from adf2fabric import cli as cli_mod

    monkeypatch.setattr(cli_mod, "projects_root", lambda config: tmp_path)
    r = CliRunner().invoke(cli_mod.cli, ["auth-user-create", "--id", "cli-user", "--org", "acme", "--role", "analyst", "--subscription", SUB])
    assert r.exit_code == 0 and re.search(r"a2f_[A-Za-z0-9_-]{40,}", r.output)
    listed = CliRunner().invoke(cli_mod.cli, ["auth-user-list"]).output
    assert "cli-user" in listed and "a2f_" not in listed
    assert CliRunner().invoke(cli_mod.cli, ["auth-user-create", "--id", "cli-user", "--org", "acme", "--role", "analyst"]).exit_code == 1
    assert CliRunner().invoke(cli_mod.cli, ["auth-user-create", "--id", "x", "--org", "a", "--role", "nobody"]).exit_code == 1
    assert CliRunner().invoke(cli_mod.cli, ["audit-verify"]).exit_code == 0


def test_sensitive_cli_commands_are_audited_only_when_they_change_something(tmp_path, monkeypatch):
    from adf2fabric import cli as cli_mod

    monkeypatch.setenv("ADF2FABRIC_AUDIT_DIR", str(tmp_path))
    calls = []

    @cli_mod._audited("demo")
    def command(apply_=True, project_name="p", workspace_id="w", token="SECRET-DO-NOT-LOG"):
        calls.append(apply_)
        return "done"

    assert command(apply_=False) == "done" and not (tmp_path / "_audit" / "audit.jsonl").exists()  # a dry run changes nothing and is not an event
    assert command(apply_=True, project_name="p", workspace_id="w", token="SECRET-DO-NOT-LOG") == "done"
    events = AuditLog(tmp_path).read()
    assert [(e["action"], e["outcome"]) for e in events] == [("cli.demo", "requested"), ("cli.demo", "ok")]
    assert events[0]["actor"].startswith("os:") and "SECRET-DO-NOT-LOG" not in json.dumps(events) and events[0]["detail"] == {"project_name": "p", "workspace_id": "w"}

    @cli_mod._audited("boom")
    def failing(apply_=True):
        raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        failing()
    assert AuditLog(tmp_path).read()[-1]["outcome"] == "failed"


def test_every_sensitive_cli_command_carries_the_audit_decorator():
    import inspect

    from adf2fabric import cli as cli_mod

    src = inspect.getsource(cli_mod)
    for fn in ("deploy_cmd", "run_cmd", "verify_cmd", "provision_apply", "provision_approve", "workloads_deploy", "workloads_approve",
               "deploy_notebook_cmd", "compute_deploy_cmd", "create_shortcut_cmd"):
        assert re.search(rf'@_audited\("[a-z_.]+"\)\ndef {fn}\(', src), fn


# =====================================================================================================================
# 12. credentials are never in source control
# =====================================================================================================================

_SECRET_PATTERNS = [r"ghp_[A-Za-z0-9]{36}", r"github_pat_[A-Za-z0-9_]{50,}", r"AKIA[0-9A-Z]{16}", r"-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----",
                    r"eyJ[A-Za-z0-9_-]{20,}\.eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}", r"(?i)(password|secret|api[_-]?key|token)\s*[:=]\s*[\"'][A-Za-z0-9+/=_-]{20,}[\"']"]


def test_no_credential_is_hard_coded_in_the_source_or_ui():
    root = Path(__file__).parents[1]
    hits = []
    for base in ("src", "ui", "config"):
        for f in (root / base).rglob("*"):
            if f.is_file() and f.suffix in (".py", ".yaml", ".yml", ".json", ".toml", ".md") and "__pycache__" not in f.parts:
                text = f.read_text(encoding="utf-8", errors="ignore")
                for pat in _SECRET_PATTERNS:
                    for m in re.finditer(pat, text):
                        hits.append(f"{f.relative_to(root)}: {m.group(0)[:20]}...")
    assert hits == []


def test_tokens_are_never_stored_by_the_ui_client_beyond_its_own_object():
    from importlib import import_module
    import sys

    sys.path.insert(0, str(Path(__file__).parents[1] / "ui"))
    try:
        api_client = import_module("api_client")
    finally:
        sys.path.pop(0)
    c = api_client.ApiClient(session_id="s", token="")
    c.set_token("a2f_abc")
    assert c._token == "a2f_abc"
    c.clear_token()
    assert c._token is None
    assert "token" not in repr(vars(c)).lower().replace("_token': none", "")


# =====================================================================================================================
# 13. live-factory sources: the subscription inside a project's source is gated like any client-chosen subscription
# =====================================================================================================================


def _point_project_at_factory(world, sub):
    p = world.demo.store.load("demo-estate")
    p.repository.repository_url = f"azure://subscriptions/{sub}/resourceGroups/rg/factories/f"
    world.demo.store.save(p)


def test_a_projects_azure_source_is_hidden_and_unusable_without_its_subscription_grant(world):
    other = "44444444-4444-4444-4444-444444444444"
    _point_project_at_factory(world, other)
    lim = make_limited(world, subscriptions=[SUB], workspaces=[WS])
    shown = world.call(lim, "GET", "/projects/demo-estate").json()["source"]["url"]
    assert other not in shown and "hidden" in shown
    assert other in world.call("ann", "GET", "/projects/demo-estate").json()["source"]["url"]  # ann holds a wildcard grant
    r = world.call(lim, "POST", "/projects/demo-estate/stages/verify", json_body={"workspace_id": WS})
    assert r.status_code == 403  # the ADF run history would be read with a subscription this caller was never granted


def test_the_rate_limiter_forgets_old_keys():
    from adf2fabric.api.security.ratelimit import RateLimiter

    t = [0.0]
    rl = RateLimiter(clock=lambda: t[0])
    for i in range(25000):
        assert rl.check(f"k{i}", "b", 5) is None
    t[0] = 7200.0
    assert rl.check("fresh", "b", 5) is None
    assert len(rl._hits) < 25000  # bounded: attackers cannot grow it without limit
