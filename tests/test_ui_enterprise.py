"""The guided UI with real (demo-backed) data behind every panel: states, confirmations, sign-in gate, demo labelling, production mode, no identifiers shown."""

from __future__ import annotations

import re
import sys
from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("streamlit")
from fastapi.testclient import TestClient  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

from adf2fabric.api.app import create_app  # noqa: E402
from adf2fabric.api.container import Container, Settings  # noqa: E402
from adf2fabric.api.demo import DEMO_WS, build_demo_container  # noqa: E402
from adf2fabric.api.security import SecuritySettings  # noqa: E402

UI = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(UI))
from api_client import ApiClient  # noqa: E402
import journey  # noqa: E402

GUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
PID = "demo-estate"


class _Http:
    def __init__(self, tc: TestClient):
        self.tc = tc

    def request(self, method, url, *, json=None, params=None, data=None, headers=None, timeout=None):
        return self.tc.request(method, url, json=json, params=params, content=data, headers=headers)


def _client(app) -> ApiClient:
    tc = TestClient(app, base_url="http://localhost", raise_server_exceptions=False)
    return ApiClient(session_id="ent1", base_url="http://localhost", token="", http=_Http(tc))


@pytest.fixture()
def demo(monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    monkeypatch.delenv("ADF2FABRIC_ENV", raising=False)
    d = build_demo_container()
    return d, _client(create_app(Settings(), live=d, demo=d))


@pytest.fixture()
def worked(demo):
    """The demo estate taken through discovery, dependencies and conversion, with the demo workspace chosen: real data for every panel."""
    _, api = demo
    api.set_mode("demo")
    for stage in ("discovery", "semantic", "dependencies", "convert"):
        assert api.wait_job(api.run_stage(PID, stage), interval=0.05)["state"] == "succeeded", stage
    api.set_destination("azure_cli", workspace_id=DEMO_WS)
    return api


def _at(api, **state) -> AppTest:
    at = AppTest.from_file(str(UI / "app.py"), default_timeout=90)
    at.session_state["api"] = api
    for k, v in state.items():
        at.session_state[k] = v
    return at


def _everything_shown(at: AppTest) -> str:
    parts = [str(e.value) for e in at.markdown] + [str(e.value) for e in at.caption] + [str(e.value) for e in at.info] + [str(e.value) for e in at.warning]
    parts += [str(e.value) for e in at.error] + [str(e.value) for e in at.success]
    for df in at.dataframe:
        parts.append(df.value.to_string())
    return "\n".join(parts)


@pytest.fixture()
def open_steps(monkeypatch):
    """Show any step's panel regardless of the walk-through gating (that gating is covered in test_ui): these tests are about what a panel shows and allows."""
    real = journey.statuses

    def statuses(facts, confirmed):
        return [replace(s, locked=False, confirmed=True, stale=False) for s in real(facts, confirmed)]

    monkeypatch.setattr(journey, "statuses", statuses)


def _step(api, step: str, **state) -> AppTest:
    at = _at(api, project_id=PID, confirmed={PID: set(journey.KEYS)}, step={PID: step}, **state)
    at.switch_page("views/migration.py").run()
    assert not at.exception, at.exception
    return at


# ---- demo labelling -------------------------------------------------------------------------------------------------------------


def test_demo_data_is_labelled_on_every_page(worked):
    for page in ("views/migration.py", "views/jobs.py"):
        at = _at(worked, project_id=PID)
        at.switch_page(page).run()
        assert not at.exception, at.exception
        assert "DEMO DATA" in _everything_shown(at)


def test_live_mode_shows_no_demo_banner(demo):
    _, api = demo
    at = _at(api)
    at.run()
    assert "DEMO DATA" not in _everything_shown(at)


# ---- panels render real data, and show nothing an ordinary view must not -------------------------------------------------------


@pytest.mark.parametrize("step", ["review", "plan", "validate", "report"])
def test_new_step_panels_render_with_data_and_show_no_identifiers(worked, step, open_steps):
    at = _step(worked, step)
    shown = _everything_shown(at)
    assert not GUID.search(shown), (step, GUID.search(shown).group(0))
    assert "Traceback" not in shown and not re.search(r"[A-Za-z]:\\\\|[A-Za-z]:\\", shown)
    assert not any("sha" in str(c).lower() for df in at.dataframe for c in df.value.columns)


def test_review_shows_a_state_for_every_pipeline_and_a_legend(worked, open_steps):
    at = _step(worked, "review")
    shown = _everything_shown(at)
    inv = worked.inventory(PID)
    assert inv["pipelines"] and all(p["name"] in shown for p in inv["pipelines"][:3])
    assert any(label in shown for label in ("Converted", "Discovered", "Inferred", "Blocked", "Unsupported"))


def test_report_step_never_says_verified_without_a_passing_runtime_validation(worked, open_steps):
    at = _step(worked, "report")
    shown = _everything_shown(at).lower()
    assert "migration verified" not in shown
    assert not any("Migration verified" in str(m.value) for m in at.markdown)  # the completion banner is the backend's word
    assert any("not verified" in str(m.value) for m in at.markdown)


# ---- typed confirmation before anything changes the target ---------------------------------------------------------------------


def _planned_and_approved(api) -> str:
    job = api.wait_job(api.plan_provisioning(PID, lakehouses=["lh_demo"], workspace_id=DEMO_WS), interval=0.05)
    assert job["state"] == "succeeded"
    view = api.provisioning(PID)
    api.approve_provisioning(PID, view["plan_ref_full"])
    return view["plan_ref_full"]


def test_apply_stays_disabled_until_a_dry_run_is_done_and_APPLY_is_typed(worked, open_steps):
    ref = _planned_and_approved(worked)
    at = _step(worked, "plan")
    assert at.button(key="plan-apply").disabled  # no dry run yet
    assert at.text_input(key="plan-apply-confirm").disabled
    at.button(key="plan-dry").click().run()
    assert not at.exception, at.exception
    assert not at.text_input(key="plan-apply-confirm").disabled
    assert at.button(key="plan-apply").disabled  # a dry run alone is not consent
    at.text_input(key="plan-apply-confirm").set_value("apply").run()
    assert at.button(key="plan-apply").disabled  # the word must match exactly
    at.text_input(key="plan-apply-confirm").set_value("APPLY").run()
    assert not at.button(key="plan-apply").disabled
    assert worked.provisioning(PID)["plan_ref_full"] == ref and "DRY RUN" in _everything_shown(at)


def test_a_dry_run_changes_nothing_and_the_apply_is_labelled_as_real(worked, demo, open_steps):
    d, _ = demo
    _planned_and_approved(worked)
    at = _step(worked, "plan")
    at.button(key="plan-dry").click().run()
    assert not d.fabric_http.items  # nothing was created by the dry run
    assert "DRY RUN, nothing changed" in _everything_shown(at)
    at.text_input(key="plan-apply-confirm").set_value("APPLY").run()
    at.button(key="plan-apply").click().run()
    assert not at.exception, at.exception
    assert [i["displayName"] for i in d.fabric_http.items] == ["lh_demo"]
    assert "APPLIED to Fabric" in _everything_shown(at)


def test_a_viewer_sees_the_plan_but_every_action_is_disabled(tmp_path, monkeypatch, open_steps):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    from tests.test_api_security import World

    w = World(tmp_path)
    P = f"/projects/{PID}"
    for who in ("oscar", "abe", "vic"):
        w.call(who, "POST", "/mode", json_body={"mode": "demo"})
    w.call("oscar", "PUT", "/session/destination", json_body={"provider": "azure_cli", "workspace_id": DEMO_WS})
    for stage in ("discovery", "semantic", "dependencies", "convert"):
        assert w.wait("oscar", w.call("oscar", "POST", f"{P}/stages/{stage}", json_body={}).json())["state"] == "succeeded"
    assert w.wait("oscar", w.call("oscar", "POST", f"{P}/provisioning/plan", json_body={"workspace_id": DEMO_WS, "lakehouses": ["lh_demo"]}).json())["state"] == "succeeded"
    ref = w.call("oscar", "GET", f"{P}/provisioning").json()["plan_ref_full"]
    assert w.call("abe", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": ref}).status_code == 200
    viewer = ApiClient(session_id="s1", base_url="http://localhost", token=w.tokens["vic"], http=_Http(w.http))
    at = _step(viewer, "plan")
    assert not at.exception, at.exception
    assert "lh_demo" in _everything_shown(at)  # a viewer may read the plan
    keys = {b.key: b.disabled for b in at.button if b.key in ("plan-dry", "plan-apply", "plan-approve", "plan-verify")}
    assert keys and all(keys.values()), keys  # and may not act on it (the API refuses too: test_api_views)


# ---- sign-in gate and production mode ------------------------------------------------------------------------------------------


def test_token_mode_shows_only_the_sign_in_until_a_token_is_accepted(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    from tests.test_api_security import World

    w = World(tmp_path)
    anon = ApiClient(session_id="g1", base_url="http://localhost", token="", http=_Http(w.http))
    at = _at(anon)
    at.run()
    assert not at.exception, at.exception
    assert any("Sign in" in str(m.value) for m in at.markdown) and at.text_input(key="signin-token") is not None
    assert not at.selectbox  # no project list, no navigation data before sign-in
    at.text_input(key="signin-token").set_value("a2f_wrong-token-value").run()
    next(b for b in at.button if b.label == "Sign in").click().run()
    assert any("not accepted" in str(e.value) for e in at.error)
    assert "a2f_wrong" not in _everything_shown(at)


def test_production_mode_hides_the_demo_toggle_and_shows_no_demo_data(tmp_path, monkeypatch):
    from adf2fabric.project.store import ProjectStore

    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    live = Container(Settings(projects_dir=tmp_path / "p"), store=ProjectStore(tmp_path / "p"))
    api = _client(create_app(Settings(), live=live, demo=None, security=SecuritySettings(mode="disabled", production=True)))
    at = _at(api, demo_available=True)
    at.run()
    assert not at.exception, at.exception
    toggles = [t for t in at.toggle if t.label == "Demo"]
    assert toggles and toggles[0].disabled and toggles[0].value is False
    assert "DEMO DATA" not in _everything_shown(at) and PID not in _everything_shown(at)


def test_running_pipelines_needs_a_selection_and_RUN_typed(worked, demo, open_steps):
    d, _ = demo
    assert worked.wait_job(worked.run_stage(PID, "deploy_dry_run", workspace_id=DEMO_WS, allow_unbound=True), interval=0.05)["state"] == "succeeded"
    assert worked.wait_job(worked.run_stage(PID, "deploy_apply", workspace_id=DEMO_WS, allow_unbound=True, confirm=True), interval=0.05)["state"] == "succeeded"
    at = _step(worked, "report")
    assert at.button(key="run-go").disabled and at.text_input(key="run-confirm").disabled  # nothing chosen yet
    assert "REAL RUNS" in _everything_shown(at)
    first = at.multiselect(key="run-pipes").options[0]
    at.multiselect(key="run-pipes").select(first).run()
    assert at.button(key="run-go").disabled  # chosen, but not confirmed
    at.text_input(key="run-confirm").set_value("RUN").run()
    assert not at.button(key="run-go").disabled
    before = len(d.fabric_http.runs)
    at.button(key="run-go").click().run()
    assert not at.exception, at.exception
    assert len(d.fabric_http.runs) == before + 1


def test_a_reload_keeps_the_confirmed_steps(worked, open_steps):
    at = _at(worked, project_id=PID)
    at.switch_page("views/migration.py").run()
    assert not at.exception, at.exception
    worked.journey_confirm(PID, ["discover"])  # what the Looks-good button records
    fresh = _at(worked, project_id=PID)  # a new browser session of the same project: nothing in its own session state
    fresh.switch_page("views/migration.py").run()
    assert "discover" in fresh.session_state["confirmed"][PID]


def test_a_running_job_is_shown_after_a_reload_and_can_be_watched(worked, monkeypatch):
    import threading

    release = threading.Event()

    def slow(c, session, pid, stage, log, options=None):
        log("working")
        release.wait(30)
        return {"message": "done"}

    monkeypatch.setattr("adf2fabric.api.app.run_stage", slow)
    job = worked.run_stage(PID, "dependencies")  # started by "an earlier visit"
    try:
        at = _at(worked, project_id=PID)  # the page is opened afresh
        at.switch_page("views/migration.py").run()
        assert not at.exception, at.exception
        assert any("job is running" in str(w.value) for w in at.warning), [str(w.value) for w in at.warning]
        assert at.button(key="watch-job") is not None
        other = worked.run_stage  # a second start is refused by the server, and the page says where to look
        try:
            other(PID, "convert")
        except Exception as exc:  # noqa: BLE001
            assert "already running" in str(exc)
    finally:
        release.set()
    worked.wait_job(job, interval=0.05)
    at.run()
    assert not any("job is running" in str(w.value) for w in at.warning)


def test_words_typed_by_another_person_are_shown_as_text_not_as_links_or_html(worked, open_steps):
    hostile = "[Approve here](http://evil.example/x) ![t](http://evil.example/p.png) <img src=x onerror=alert(1)> # shout"
    worked.brief_note(PID, hostile)
    worked.brief_answers(PID, {"context.owner": hostile})
    at = _step(worked, "consult")
    shown = [str(e.value) for e in at.caption] + [str(e.value) for e in at.info]
    mine = [s for s in shown if "evil" in s]
    assert len(mine) >= 2, "the hostile text should still be visible, as text (note and owner)"
    for s in mine:  # every markdown and HTML control character is backslash-escaped, so nothing renders as a link, an image or a tag
        for token in ("](", "![", "<img", "](http"):
            assert token not in s.replace("\\" + token[0], "") or "\\" in s, (token, s)
        assert "\[" in s and "\<" in s, s


def test_saving_a_topic_never_accepts_recommendations_nobody_chose(worked, open_steps):
    at = _step(worked, "consult")
    recs_before = {q["id"] for q in worked.brief(PID)["questions"] if q["source"] == "recommendation"}
    assert len(recs_before) > 3
    # pick ONE answer in the Scope topic and save that form
    scope = [r for r in at.radio if (r.key or "").startswith("consult-q-scope.")]
    assert scope and all(r.value is None for r in scope), "recommendations are shown, not pre-selected"
    at.radio(key="consult-q-scope.approach").set_value("phased")  # inside a form the choice is sent with the submit, not before
    next(b for b in at.button if b.label == "Save answers" and b.key and "scope" in b.key).click().run()
    assert not at.exception, at.exception
    after = {q["id"]: q for q in worked.brief(PID)["questions"]}
    assert after["scope.approach"]["source"] == "person" and after["scope.approach"]["value"] == "phased"
    still = {qid for qid in recs_before if qid != "scope.approach" and after[qid]["source"] == "recommendation"}
    assert still == recs_before - {"scope.approach"}  # every other recommendation is still only a recommendation
