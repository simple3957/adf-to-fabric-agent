"""The simple consultation (the default): only the questions that change the run, already answered, saved in one step with no second approver."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("streamlit")

from adf2fabric.agent.consultation import brief as B  # noqa: E402
from tests.test_ui import _app, api, journey  # noqa: E402,F401  (the api fixture and the AppTest helper)


@pytest.fixture(autouse=True)
def simple_mode(monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_CONSULT", "simple")


def _discovered(api):
    api.set_mode("demo")
    for stage in ("discovery", "semantic", "dependencies"):
        api.wait_job(api.run_stage("demo-estate", stage), interval=0.05)


def test_only_the_questions_that_change_the_run_are_asked(api):
    _discovered(api)
    v = api.brief("demo-estate")
    asked = {q["id"] for q in v["questions"]}
    assert v["mode"] == "simple" and asked <= B.ESSENTIAL and {q["effect"] for q in v["questions"]} == {"run"}
    assert len(asked) <= 10 and asked.isdisjoint({"context.owner", "context.notes", "scope.approach", "scope.rollback", "security.network", "validation.gate", "target.git", "security.sensitive"})
    assert v["counts"]["need_person"] == 0  # every one of them is already answered by its recommendation


def test_saving_the_settings_is_the_whole_step_and_the_run_follows_them(api):
    _discovered(api)
    assert api.brief("demo-estate")["state"] == "draft" and api.brief("demo-estate")["settings"] is None
    v = api.brief_answers("demo-estate", {"validation.depth": "deployed"})
    assert v["state"] == "approved" and v["settings"] is not None
    assert v["current_version"]["approved_by"] and v["current_version"]["submitted_by"] == v["current_version"]["approved_by"]  # one person, no second approver


def test_accepting_the_recommendations_also_saves_them(api):
    _discovered(api)
    api.brief_accept("demo-estate")
    assert api.brief("demo-estate")["state"] == "approved"


def test_the_person_who_saved_may_still_use_the_approve_call_but_nothing_requires_it(api):
    _discovered(api)
    api.brief_accept("demo-estate")
    assert api.brief("demo-estate")["settings"]["lakehouse"] is None  # nothing needed beyond saving, and no lakehouse is invented


def test_changing_a_setting_after_saving_saves_the_new_version(api):
    _discovered(api)
    first = api.brief_answers("demo-estate", {"validation.depth": "deployed"})["current_version"]["version"]
    second = api.brief_answers("demo-estate", {"validation.depth": "static"})
    assert second["state"] == "approved" and second["current_version"]["version"] == first + 1
    assert second["settings"]["validate_stages"] == ["static"]


def test_a_conflict_keeps_it_a_draft_and_says_why(api):
    _discovered(api)
    v = api.brief_answers("demo-estate", {"scope.mode": "selected", "scope.pipelines": []})  # selected pipelines, none chosen
    assert v["state"] == "draft" and v["conflicts"]


def test_the_full_consultation_is_still_there_behind_a_switch(api, monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_CONSULT", "full")
    _discovered(api)
    v = api.brief("demo-estate")
    assert v["mode"] == "full" and len(v["questions"]) > 12
    api.brief_accept("demo-estate")
    assert api.brief("demo-estate")["state"] == "draft"  # full mode still needs submit and an approver


def test_the_simple_screen_renders_the_form_and_no_approval_controls(api):
    _discovered(api)
    at = _app(api, project_id="demo-estate", confirmed={"demo-estate": {"discover", "review"}}, step={"demo-estate": "consult"})
    at.switch_page("views/migration.py").run()
    assert not at.exception, at.exception
    keys = {b.key for b in at.button}
    assert "consult-submit" not in keys and "consult-approve" not in keys and "consult-accept" not in keys
    assert any(str(b.label) == "Save settings" for b in at.get("button") if hasattr(b, "label")) or at.get("form_submit_button")
    assert not at.button(key="go-next").disabled  # nothing to approve: Next is open without saving


def test_simple_mode_never_repoints_storage_to_a_lakehouse_that_nobody_chose_or_bound(api):
    _discovered(api)
    v = api.brief("demo-estate")
    route = next(q for q in v["questions"] if q["id"] == "storage.route")
    assert route["recommendation"]["value"] == "connections"  # a lakehouse that does not exist would leave unbound placeholders and fail validation
    assert v["settings"] is None and v["draft_settings"]["lakehouse"] is None
    api.brief_accept("demo-estate")
    assert api.brief("demo-estate")["settings"]["lakehouse"] is None
