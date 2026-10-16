"""UI tests: the API client against the real API in demo mode, and a smoke run of every Streamlit page (no network)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("streamlit")
from fastapi.testclient import TestClient  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

from adf2fabric.api.app import create_app  # noqa: E402
from adf2fabric.api.container import Settings  # noqa: E402
from adf2fabric.api.demo import build_demo_container  # noqa: E402

UI = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(UI))
from api_client import ApiClient, ApiError  # noqa: E402
import journey  # noqa: E402

SECRET = "S3cr3t-UI-Value-77"
PAGES = ["setup", "migration", "jobs"]


class _Http:
    """Adapts FastAPI's TestClient to the ``requests``-style call ApiClient makes."""

    def __init__(self, tc: TestClient):
        self.tc = tc

    def request(self, method, url, *, json=None, params=None, data=None, headers=None, timeout=None):
        return self.tc.request(method, url, json=json, params=params, content=data, headers=headers)


@pytest.fixture()
def api(monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    demo = build_demo_container()
    tc = TestClient(create_app(Settings(), live=demo, demo=demo), raise_server_exceptions=False)
    return ApiClient(session_id="ui1", base_url="http://testserver", token="", http=_Http(tc))


def test_client_demo_flow(api):
    assert api.health()["status"] == "ok"
    assert api.set_mode("demo") == "demo"
    assert api.auth_status("azure_cli")["authenticated"] is True
    assert "demo-estate" in [p["id"] for p in api.projects()]

    for stage in ("discovery", "semantic"):
        assert api.wait_job(api.run_stage("demo-estate", stage), interval=0.05)["state"] == "succeeded"

    cards = {c["name"]: c for c in api.cards("demo-estate")}
    assert "LS_Sales_SQL" in cards
    api.set_choice("demo-estate", "LS_Sales_SQL", "sql_login", values={"username": "etl_user"})
    assert api.put_secret("demo-estate", "LS_Sales_SQL", "password", SECRET) == ["password"]
    card = next(c for c in api.cards("demo-estate") if c["name"] == "LS_Sales_SQL")
    assert card["secrets_held"] == ["password"] and SECRET not in str(card)
    assert "state" in api.test_connection("demo-estate", "LS_Sales_SQL")

    res = api.apply_connections("demo-estate", ["LS_Sales_SQL"], dry_run=True, workspace_id="w")
    assert res["dry_run"] is True and res["items"][0]["action"] in ("would_create", "reused", "manual")

    events = []
    job = api.wait_job(api.run_stage("demo-estate", "convert"), on_event=events.append, interval=0.05)
    assert job["state"] == "succeeded" and job["result"]["pipelines"] and events


def test_client_errors(api):
    with pytest.raises(ApiError) as err:  # a secret in the saved choice is refused and not echoed
        api.set_choice("demo-estate", "LS_Sales_SQL", "sql_login", values={"password": SECRET})
    assert err.value.status == 422 and SECRET not in str(err.value.details) + err.value.message

    with pytest.raises(ApiError) as err:
        api.run_stage("demo-estate", "nope")
    assert err.value.status == 404

    with pytest.raises(ApiError) as err:
        api.project("does-not-exist")
    assert err.value.status == 404


def test_client_unreachable_api():
    with pytest.raises(ApiError) as err:
        ApiClient(session_id="x", base_url="http://127.0.0.1:9", timeout=2).health()
    assert err.value.status == 0 and err.value.actions


def _app(api, **state):
    api.set_mode("demo")
    at = AppTest.from_file(str(UI / "app.py"), default_timeout=90)
    at.session_state["api"] = api
    for k, v in state.items():
        at.session_state[k] = v
    return at


@pytest.mark.parametrize("page", PAGES)
def test_pages_render_in_demo(api, page):
    at = _app(api, project_id="demo-estate")
    at.run()
    assert not at.exception, at.exception
    if page != "setup":
        at.switch_page(f"views/{page}.py").run()
        assert not at.exception, at.exception


@pytest.mark.parametrize("step", journey.KEYS)
def test_every_step_panel_renders(api, step):
    at = _app(api, project_id="demo-estate", confirmed={"demo-estate": set(journey.KEYS)}, step={"demo-estate": step})
    at.switch_page("views/migration.py").run()
    assert not at.exception, at.exception


def test_setup_without_project_shows_launch_bar_disabled(api):
    at = _app(api)
    at.run()
    assert not at.exception, at.exception
    start = [b for b in at.button if "migration" in str(b.label).lower()]
    assert start and start[0].disabled


def test_discover_then_confirm_opens_connect(api):
    at = _app(api, project_id="demo-estate")
    at.switch_page("views/migration.py").run()
    at.button(key="run-discover").click().run()
    assert not at.exception, at.exception
    nxt = at.button(key="go-next")
    assert not nxt.disabled and "Connect" in str(nxt.label)
    nxt.click().run()
    assert "discover" in at.session_state["confirmed"]["demo-estate"]
    assert at.session_state["step"]["demo-estate"] == "connect"
    assert not at.button(key="open-discover").disabled  # done steps can be reopened
    assert at.button(key="open-convert").disabled  # still locked until Connect is confirmed


def test_sign_in_is_asked_not_assumed_then_switch_and_sign_out(api):
    at = _app(api)
    at.run()
    assert not at.exception, at.exception
    # Nobody is assumed: the Fabric card offers "Continue as ..." and lists no workspaces yet.
    assert at.button(key="dst-azure_cli-continue") and not [s for s in at.selectbox if s.key == "dst-ws"]
    at.button(key="dst-azure_cli-continue").click().run()
    assert at.session_state["confirmed_ids"]["dst-azure_cli"] == "demo.user@contoso.example"
    assert [s for s in at.selectbox if s.key == "dst-ws"]  # workspaces appear once the account is confirmed
    at.button(key="dst-azure_cli-switch").click().run()  # switch opens the sign-in form
    assert at.button(key="dst-azure_cli-go") and at.button(key="dst-azure_cli-cancel")
    at.button(key="dst-azure_cli-cancel").click().run()
    at.button(key="dst-azure_cli-out").click().run()  # sign out forgets the confirmation
    assert "dst-azure_cli" not in at.session_state["confirmed_ids"]
    assert not at.exception, at.exception


def test_journey_rules():
    f = {"discover": journey.StepFacts(complete=True, metric="x"), "connect": journey.StepFacts(attention="1 need attention")}
    s = journey.statuses(f, set())
    assert [x.label for x in s] == ["Ready to confirm", "Locked", "Locked", "Locked"]
    s = journey.statuses(f, {"discover"})
    assert [x.label for x in s][:2] == ["Done", "Needs attention"] and journey.first_open(s) == "connect"
    s = journey.statuses({"discover": journey.StepFacts()}, {"discover"})  # confirmed, but results gone
    assert s[0].label == "Run again" and s[1].locked
    cards = [{"state": "connection_tested"}, {"state": "manual_action_required"}, {"state": "resource_discovered"}]
    c = journey.connect_facts(cards)
    assert not c.complete and c.metric == "1 of 3 ready" and c.attention == "1 need attention"
    assert journey.deploy_facts({"applied": False, "results": [{"action": "create"}]}).complete is False
