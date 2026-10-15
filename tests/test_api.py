"""API tests: the demo backend (real parsing/cards/persistence, canned providers) driven over HTTP, plus security behaviour."""

from __future__ import annotations

import io
import json
import time
import zipfile

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from adf2fabric.api.app import create_app  # noqa: E402
from adf2fabric.api.container import Container, Settings  # noqa: E402
from adf2fabric.api.demo import build_demo_container, demo_estate  # noqa: E402

SECRET = "S3cr3t-Value-ZZ9"


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    demo = build_demo_container()
    app = create_app(Settings(), live=demo, demo=demo)
    c = TestClient(app, raise_server_exceptions=False)
    c.headers["X-Session-Id"] = "t1"
    return c


def wait(client, job, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        j = client.get(f"/jobs/{job['id']}").json()
        if j["state"] in ("succeeded", "failed"):
            return j
        time.sleep(0.05)
    raise AssertionError("job did not finish")


def test_health_and_mode(client):
    assert client.get("/health").json()["status"] == "ok"
    assert client.post("/mode", json={"mode": "demo"}).json()["mode"] == "demo"
    assert client.post("/mode", json={"mode": "bogus"}).status_code == 422


def test_auth_status_and_subscriptions_demo(client):
    assert client.get("/auth/azure_cli/status").json()["authenticated"] is True
    assert client.get("/auth/nope/status").status_code == 404
    assert client.get("/azure/subscriptions").json()["subscriptions"][0]["name"] == "Contoso Demo"
    found = client.get("/azure/discover", params={"subscription_id": "x"}).json()
    assert found["factories"][0]["name"] == "contoso-adf"


def test_logout_demo(client):
    assert client.post("/auth/azure_cli/logout", json={"account": "x@y.example"}).json()["provider"] == "azure_cli"
    assert client.post("/auth/fabric_cli/logout", json={}).status_code == 200
    assert client.post("/auth/nope/logout", json={}).status_code == 404


def test_login_flow_demo(client):
    a = client.post("/auth/azure_cli/login", json={}).json()
    assert client.get(f"/auth/azure_cli/login/{a['id']}").json()["state"] == "succeeded"


def test_projects_cards_and_runtimes(client):
    assert "demo-estate" in [p["id"] for p in client.get("/projects").json()["projects"]]
    ls = client.get("/projects/demo-estate/linked-services").json()
    assert {x["name"] for x in ls["linked_services"]} >= {"LS_Sales_SQL", "LS_Lake", "LS_ERP_OnPrem"}
    cards = {c["name"]: c for c in client.get("/projects/demo-estate/connections").json()["cards"]}
    assert cards["LS_ERP_OnPrem"]["runtime"]["fabric_path"] == "on_premises_gateway"
    assert cards["LS_Tenant_DB"]["parameterized"] is True
    assert cards["LS_Cosmos"]["supported"] is False
    assert client.get("/projects/nope/connections").status_code == 404


def test_choice_secret_test_apply_report(client):
    p = "/projects/demo-estate/connections"
    methods = {m["id"] for c in client.get("/connectors").json()["connectors"] if c["id"] == "sql" for m in c["methods"]}
    assert {"sql_login", "sql_login_kv", "workspace_identity", "existing_connection"} <= methods
    sql_basic = "sql_login"
    r = client.put(f"{p}/LS_Sales_SQL/choice", json={"method": sql_basic, "values": {"username": "etl_user"}})
    assert r.status_code == 200, r.text
    # a secret sent as a plain value is refused
    bad = client.put(f"{p}/LS_Sales_SQL/choice", json={"method": sql_basic, "values": {"password": SECRET}})
    assert bad.status_code == 422 and SECRET not in bad.text
    held = client.put(f"{p}/LS_Sales_SQL/secrets", json={"field": "password", "value": SECRET})
    assert held.json()["held"] == ["password"] and SECRET not in held.text
    card = next(c for c in client.get(p).json()["cards"] if c["name"] == "LS_Sales_SQL")
    assert card["secrets_held"] == ["password"] and SECRET not in json.dumps(card)
    t = client.post(f"{p}/LS_Sales_SQL/test")
    assert t.status_code == 200 and SECRET not in t.text
    dry = client.post(f"{p}/apply", json={"names": ["LS_Sales_SQL"], "dry_run": True, "workspace_id": "w"})
    assert dry.status_code == 200 and dry.json()["items"][0]["action"] in ("would_create", "reused", "manual")
    real = client.post(f"{p}/apply", json={"names": ["LS_Sales_SQL"], "workspace_id": "w"})
    assert real.status_code == 200 and SECRET not in real.text
    rep = client.get(f"{p}/report")
    assert rep.status_code == 200 and SECRET not in rep.text


def test_validation_errors_do_not_echo_secret(client):
    r = client.put("/projects/demo-estate/connections/LS_Sales_SQL/secrets", json={"field": "password", "value": SECRET, "extra": 1})
    assert r.status_code == 422 and SECRET not in r.text
    r = client.put("/projects/demo-estate/connections/LS_Sales_SQL/secrets", json={"value": SECRET})
    assert r.status_code == 422 and SECRET not in r.text


def test_unknown_secret_field_rejected(client):
    r = client.put("/projects/demo-estate/connections/LS_Sales_SQL/secrets", json={"field": "nope", "value": SECRET})
    assert r.status_code == 422 and SECRET not in r.text


def test_stages_run_to_convert(client):
    for stage in ("discovery", "semantic", "convert"):
        j = wait(client, client.post(f"/projects/demo-estate/stages/{stage}", json={}).json())
        assert j["state"] == "succeeded", j
    assert client.post("/projects/demo-estate/stages/bogus", json={}).status_code == 404


def test_deploy_requires_workspace_and_azure_signin(client):
    j = wait(client, client.post("/projects/demo-estate/stages/deploy_dry_run", json={}).json())
    assert j["state"] == "failed" and "workspace" in j["error"].lower()
    client.put("/session/destination", json={"provider": "fabric_cli", "workspace_id": "w"})
    j = wait(client, client.post("/projects/demo-estate/stages/deploy_dry_run", json={}).json())
    assert j["state"] == "failed" and "Azure CLI" in j["error"]


def test_job_conflict(client):
    c = client.app.state.containers["demo"]
    import threading

    gate = threading.Event()
    c.jobs.submit("slow", "demo-estate", lambda log: (gate.wait(5), {})[1])
    r = client.post("/projects/demo-estate/stages/discovery", json={})
    gate.set()
    assert r.status_code == 409


def test_workspaces_and_destination_test(client):
    ws = client.get("/fabric/workspaces").json()["workspaces"]
    assert ws and "personal" not in ws[0]["display_name"].lower()
    client.put("/session/destination", json={"provider": "azure_cli", "workspace_id": ws[0]["id"]})
    r = client.post("/test/destination", json={"provider": "azure_cli", "workspace_id": ws[0]["id"]})
    assert r.status_code == 200


def _zip(files) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for k, v in files.items():
            z.writestr(k, json.dumps(v))
    return buf.getvalue()


def test_zip_upload_test_and_import(client):
    up = client.put("/uploads/zip", content=_zip(demo_estate()))
    uid = up.json()["upload_id"]
    t = client.post("/test/source", json={"kind": "zip", "upload_id": uid})
    assert t.status_code == 200 and t.json()["ok"] is True
    j = wait(client, client.post("/projects", json={"project_name": "Zip Proj", "source": {"kind": "zip", "upload_id": uid}}).json())
    assert j["state"] == "succeeded", j
    assert client.get("/projects/zip-proj").json()["source"]["provider"] == "zip-export"


def test_zip_slip_rejected_over_api(client):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("../evil.json", "{}")
    uid = client.put("/uploads/zip", content=buf.getvalue()).json()["upload_id"]
    j = wait(client, client.post("/projects", json={"project_name": "Bad", "source": {"kind": "zip", "upload_id": uid}}).json())
    assert j["state"] == "failed"


def test_upload_cap_and_empty(monkeypatch):
    demo = build_demo_container()
    c = TestClient(create_app(Settings(max_upload_bytes=100), live=demo, demo=demo), raise_server_exceptions=False)
    assert c.put("/uploads/zip", content=b"x" * 500).status_code == 413
    assert c.put("/uploads/zip", content=b"").status_code == 422
    assert c.post("/projects", json={"project_name": "x", "source": {"kind": "zip", "upload_id": "../../etc"}}).status_code == 422


def test_git_import_demo_and_token_not_leaked(client):
    t = client.post("/test/source", json={"kind": "git", "repository_url": "https://example.com/x.git", "token": SECRET})
    assert t.status_code == 200 and SECRET not in t.text
    j = wait(client, client.post("/projects", json={"project_name": "Git Proj", "source": {
        "kind": "git", "repository_url": "https://example.com/x.git", "token": SECRET}}).json())
    assert j["state"] == "succeeded", j
    assert SECRET not in json.dumps(j) and SECRET not in json.dumps(client.get("/session").json())
    proj = client.app.state.containers["demo"].store.project_dir("git-proj")
    assert not any(SECRET in f.read_text(errors="ignore") for f in proj.rglob("*") if f.is_file() and ".git" not in f.parts)


def test_bearer_token_enforced(monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_API_TOKEN", "tok123")
    demo = build_demo_container()
    c = TestClient(create_app(Settings(), live=demo, demo=demo), raise_server_exceptions=False)
    assert c.get("/health").status_code == 200
    assert c.get("/projects").status_code == 401
    assert c.get("/projects", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert c.get("/projects", headers={"Authorization": "Bearer tok123"}).status_code == 200


def test_cors_only_for_ui_origins(client):
    ok = client.options("/health", headers={"Origin": "http://localhost:8501", "Access-Control-Request-Method": "GET"})
    assert ok.headers.get("access-control-allow-origin") == "http://localhost:8501"
    bad = client.options("/health", headers={"Origin": "http://evil.example", "Access-Control-Request-Method": "GET"})
    assert "access-control-allow-origin" not in bad.headers


def test_bad_session_id_and_unexpected_error_generic(client):
    assert client.get("/projects", headers={"X-Session-Id": "../x"}).status_code == 400
    c = client.app.state.containers["demo"]
    c.store.list_ids = lambda: (_ for _ in ()).throw(RuntimeError(f"boom {SECRET}"))  # type: ignore[method-assign]
    r = client.get("/projects")
    assert r.status_code == 500 and SECRET not in r.text


def test_session_never_holds_token(client):
    client.put("/session/source", json={"kind": "git", "repository_url": "https://e.com/x.git", "token": SECRET})
    assert SECRET not in client.get("/session").text
