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
    tc = TestClient(create_app(Settings(), live=demo, demo=demo), base_url="http://localhost", raise_server_exceptions=False)
    return ApiClient(session_id="ui1", base_url="http://localhost", token="", http=_Http(tc))


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

    res = api.apply_connections("demo-estate", ["LS_Sales_SQL"], dry_run=True, workspace_id="11111111-1111-1111-1111-111111111111")
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


def approve_brief(api, pid="demo-estate"):
    """Answer what the consultation needs (recommendations, then the first option of any open required question), submit, approve."""
    api.brief_accept(pid)
    for _ in range(5):
        v = api.brief(pid)
        open_ = {q["id"]: (("LH_Test" if q["id"] == "storage.lakehouse" else "Test owner, platform lead") if q["kind"] == "text" else [q["options"][0]["value"]] if q["kind"] == "multi"
                           else q["options"][0]["value"]) for q in v["questions"] if q["required"] and q["source"] == "open"}
        if not open_:
            break
        api.brief_answers(pid, open_)
        api.brief_accept(pid)
    v = api.brief_submit(pid)
    return api.brief_approve(pid, v["fingerprint"])


def test_discover_then_confirm_opens_connect(api):
    at = _app(api, project_id="demo-estate")
    at.switch_page("views/migration.py").run()
    at.button(key="run-discover").click().run()
    assert not at.exception, at.exception
    nxt = at.button(key="go-next")
    assert not nxt.disabled and "Review" in str(nxt.label)
    nxt.click().run()
    assert "discover" in at.session_state["confirmed"]["demo-estate"]
    assert at.session_state["step"]["demo-estate"] == "review"
    at.button(key="go-next").click().run()  # the review is read, then the consultation opens
    assert at.session_state["step"]["demo-estate"] == "consult"
    assert at.button(key="go-next").disabled  # not until the brief is approved
    assert approve_brief(api)["state"] == "approved"
    at.run()
    at.button(key="go-next").click().run()
    assert at.session_state["step"]["demo-estate"] == "connect"
    assert not at.button(key="open-discover").disabled  # done steps can be reopened
    assert at.button(key="open-convert").disabled  # still locked until Connect and Plan are confirmed


DEMO = "demo.user@contoso.example"
NEW_LABEL = "＋ Sign in with another account…"


def _choose_new_account(at, key):
    box = at.selectbox(key=key)
    box.select_index(len(box.options) - 1).run()  # the last choice is always "sign in with another account"


def _counted(api, *names):
    """Wrap API methods so a test can count how often a page asks for them."""
    calls = {n: 0 for n in names}
    for n in names:
        real = getattr(api, n)

        def wrapper(*a, _real=real, _n=n, **k):
            calls[_n] += 1
            return _real(*a, **k)

        setattr(api, n, wrapper)
    return calls


def test_nobody_is_assumed_each_side_picks_its_own_account(api):
    at = _app(api)
    at.run()
    assert not at.exception, at.exception
    dst = at.selectbox(key="dst-account")
    assert dst.value is None and DEMO in dst.options and dst.options[-1] == NEW_LABEL
    assert not [s for s in at.selectbox if s.key == "dst-ws"]  # no workspaces until an account is chosen
    dst.select(DEMO).run()
    assert at.selectbox(key="dst-account").value == DEMO
    assert [s for s in at.selectbox if s.key == "dst-ws"]
    # the destination's choice does not choose for the source: that side still asks
    assert at.selectbox(key="src-account").value is None if [s for s in at.selectbox if s.key == "src-account"] else True
    assert not at.exception, at.exception


def test_source_and_destination_can_hold_different_accounts(api):
    at = _app(api)
    at.run()
    at.toggle(key="src-again").set_value(True).run() if [t for t in at.toggle if t.key == "src-again"] else None
    src = [s for s in at.selectbox if s.key == "src-account"]
    if src:  # no project selected, so the source card shows its picker
        src[0].select(DEMO).run()
    at.selectbox(key="dst-account").select(DEMO).run()
    assert at.selectbox(key="dst-account").value == DEMO
    if src:
        assert at.selectbox(key="src-account").value == DEMO
    assert not at.exception, at.exception


def test_signing_in_is_a_choice_in_the_dropdown_and_the_tenant_is_last(api):
    at = _app(api)
    at.run()
    _choose_new_account(at, "dst-account")
    assert at.button(key="dst-account-go") and at.button(key="dst-account-cancel")
    tenant = at.text_input(key="dst-account-tenant")
    assert tenant.value == "" and tenant.label == "Tenant ID"
    # start without any tenant: the sign-in works and the new account is selected
    at.button(key="dst-account-go").click().run()
    assert at.selectbox(key="dst-account").value == DEMO
    assert not at.exception, at.exception


def test_cancelling_a_sign_in_clears_the_choice_and_signing_out_forgets_the_account(api):
    at = _app(api)
    at.run()
    _choose_new_account(at, "dst-account")
    at.button(key="dst-account-cancel").click().run()
    assert at.selectbox(key="dst-account").value is None
    at.selectbox(key="dst-account").select(DEMO).run()
    at.button(key="dst-account-out").click().run()
    assert at.selectbox(key="dst-account").value is None
    assert not at.exception, at.exception


def test_slow_lookups_are_made_once_not_on_every_click(api):
    calls = _counted(api, "accounts", "workspaces", "subscriptions", "discover")
    at = _app(api)
    at.run()
    at.selectbox(key="dst-account").select(DEMO).run()
    for _ in range(4):  # unrelated clicks: opening Advanced, toggling, rerunning the page
        at.toggle(key="dst-fab").set_value(False).run()
        at.run()
    assert calls["accounts"] == 1, calls
    assert calls["workspaces"] == 1, calls
    at.button(key="dst-refresh").click().run()  # an explicit refresh looks again
    assert calls["workspaces"] == 2, calls


def test_a_failed_factory_lookup_is_shown_not_reported_as_no_factories(api):
    api.discover = lambda sub: {"factories": [], "_errors": [{"id": "factories", "name": "Azure blocked this sign-in", "type": "error"},
                                                                 {"id": "storage", "name": "Azure blocked this sign-in", "type": "error"}]}
    at = _app(api)
    at.run()
    at.selectbox(key="src-account").select(DEMO).run()
    shown = [e.value for e in at.error]
    assert shown.count("Azure blocked this sign-in") == 1  # the repeated message appears once
    assert not [i for i in at.info if "No data factories" in str(i.value)]
    assert not at.exception, at.exception


def test_the_launch_bar_is_one_line_not_three_checks(api):
    at = _app(api)
    at.run()
    text = " ".join(str(m.value) for m in at.markdown)
    assert "Still needed" in text and "Route chosen" not in text


def test_the_route_step_and_the_hero_are_gone(api):
    at = _app(api)
    at.run()
    assert not [s for s in at.selectbox if s.label in ("From · Source", "To · Destination")]
    assert "Move your Data Factory" not in " ".join(str(m.value) for m in at.markdown)


def test_the_connect_step_shows_one_editor_not_one_per_linked_service(api):
    at = _app(api, project_id="demo-estate")
    at.switch_page("views/migration.py").run()
    at.button(key="run-discover").click().run()
    approve_brief(api)
    at.run()
    for _ in range(3):  # discover -> review -> consult -> connect
        at.button(key="go-next").click().run()
    assert at.session_state["step"]["demo-estate"] == "connect"
    assert len(at.dataframe) >= 1 and not at.exception, at.exception
    forms = [k for k in (s.key for s in at.selectbox) if k and k.startswith("m-")]
    assert len(forms) == 1, forms  # a single sign-in method picker: only the selected linked service is editable
    assert not [e for e in at.expander if "LS_" in str(e.label)]  # no expander per linked service


def test_a_linked_service_can_be_skipped_and_the_step_accepts_it(api):
    from journey import connect_facts

    cards = [{"name": "A", "state": "skipped"}, {"name": "B", "state": "connection_configured"}]
    facts = connect_facts(cards)
    assert facts.complete and "2 of 2" in facts.metric  # a skip is a decision, not an open item
    assert not connect_facts([{"name": "A", "state": "resource_discovered"}]).complete


def test_the_editor_offers_to_skip_and_does_not_force_a_connection(api):
    at = _app(api, project_id="demo-estate")
    at.switch_page("views/migration.py").run()
    at.button(key="run-discover").click().run()
    approve_brief(api)
    at.run()
    for _ in range(3):  # discover -> review -> consult -> connect
        at.button(key="go-next").click().run()
    skip = [b for b in at.button if b.key and b.key.startswith("skip-")]
    assert len(skip) == 1 and not at.exception, at.exception
    skip[0].click().run()
    assert not at.exception, at.exception


def test_creating_connections_needs_a_preview_first(api):
    api.set_mode("demo")
    approve_brief(api)  # Connect opens only after an approved consultation brief
    at = _app(api, project_id="demo-estate", confirmed={"demo-estate": {"discover", "review", "consult"}}, step={"demo-estate": "connect"})
    at.switch_page("views/migration.py").run()
    at.button(key="run-discover").click().run() if [b for b in at.button if b.key == "run-discover"] else None
    go = [b for b in at.button if b.key == "apply-go"]
    assert go and go[0].disabled  # nothing to create until a preview has been looked at


def test_ttl_cache_keeps_an_answer_until_it_expires_and_never_keeps_an_error():
    from cache import TtlCache

    now = [0.0]
    c = TtlCache(clock=lambda: now[0])
    n = {"calls": 0}

    def load():
        n["calls"] += 1
        return n["calls"]

    assert c.get("a", 10, load) == 1 and c.get("a", 10, load) == 1 and n["calls"] == 1
    now[0] = 11
    assert c.get("a", 10, load) == 2
    c.forget("a")
    assert c.get("a", 10, load) == 3

    def boom():
        raise RuntimeError("down")

    with pytest.raises(RuntimeError):
        c.get("b", 10, boom)
    assert c.get("b", 10, lambda: "ok") == "ok"  # the failure was not stored


def test_forgetting_by_prefix_leaves_other_keys():
    from cache import TtlCache

    c = TtlCache()
    for k in ("subs:a", "subs:b", "disc:x", "ws:y"):
        c.get(k, 10, lambda k=k: k)
    c.forget("subs", "ws")
    assert len(c) == 1 and c.get("disc:x", 10, lambda: "reloaded") == "disc:x"
    c.forget()
    assert len(c) == 0


def test_journey_rules():
    f = {"discover": journey.StepFacts(complete=True, metric="x"), "review": journey.StepFacts(complete=True), "consult": journey.StepFacts(complete=True),
         "connect": journey.StepFacts(attention="1 need attention")}
    s = journey.statuses(f, set())
    assert [x.label for x in s] == ["Ready to confirm"] + ["Locked"] * 8
    s = journey.statuses(f, {"discover", "review", "consult"})
    assert [x.label for x in s][:4] == ["Done", "Done", "Done", "Needs attention"] and journey.first_open(s) == "connect"
    s = journey.statuses({"discover": journey.StepFacts()}, {"discover"})  # confirmed, but results gone
    assert s[0].label == "Run again" and s[1].locked
    cards = [{"state": "connection_tested"}, {"state": "manual_action_required"}, {"state": "resource_discovered"}]
    c = journey.connect_facts(cards)
    assert not c.complete and c.metric == "1 of 3 ready" and c.attention == "1 need attention"
    dry = journey.deploy_facts({"applied": False, "results": [{"action": "create"}]})  # a dry run can be passed, but never reads as a deploy
    assert dry.complete and dry.attention == "nothing deployed yet" and "nothing deployed" in dry.headline
    refused = journey.deploy_facts({"applied": False, "results": [{"action": "refused"}, {"action": "create"}]})
    assert refused.complete and refused.attention == "1 would be refused" and "refused" in refused.outcome
    assert journey.deploy_facts({"applied": True, "results": [{"action": "create"}]}).headline is None  # a real deploy keeps the plain wording


def test_importing_from_git_creates_and_selects_the_project(api):
    at = _app(api)
    at.session_state["src-method"] = "Git repository"
    at.run()
    at.text_input(key="src-git-url").set_value("https://github.com/org/demo-adf.git").run()
    next(b for b in at.button if b.label == "Import repository").click().run()
    assert not at.exception, at.exception
    assert at.session_state["project_id"] in [p["id"] for p in api.projects()]


def test_a_failed_azure_import_is_shown_and_selects_nothing(api):
    # demo mode has no real Azure behind it, so the import job fails: the page must say so, not stay silent
    at = _app(api)
    at.run()
    at.selectbox(key="src-account").select(DEMO).run()
    next(b for b in at.button if b.label == "Import factory").click().run()
    assert not at.exception, at.exception
    assert at.error and "token" in str(at.error[0].value).lower()
    assert "project_id" not in at.session_state or not at.session_state["project_id"]


def test_a_stale_discovery_is_shown_as_out_of_date_not_done():
    from journey import discover_facts

    f = discover_facts({"has_semantic": False, "semantic_stale": True}, None)
    assert not f.complete and f.metric == "Out of date"


def test_an_empty_existing_connection_list_says_why():
    import streamlit as st
    from streamlit.testing.v1 import AppTest

    def page():
        import panels

        panels.fabric_read_notice({"state": "not_connected", "detail": "AzureCliError: not signed in", "todo": "Sign in."})
        panels.fabric_read_notice({"state": "unreadable", "detail": "RuntimeError: 401", "todo": "Check access."})
        panels.fabric_read_notice({"state": "ok", "connections": 0, "todo": "Create one."})
        panels.fabric_read_notice({"state": "ok", "connections": 3})
        panels.fabric_read_notice(None)

    at = AppTest.from_function(page).run()
    warnings = [w.value for w in at.warning]
    assert len(warnings) == 3 and "not connected" in warnings[0] and "could not be read" in warnings[1] and "no connections" in warnings[2]
    assert any("AzureCliError" in c.value for c in at.caption) and not at.exception
