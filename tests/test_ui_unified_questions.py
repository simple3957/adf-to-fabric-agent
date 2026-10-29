"""One question, asked once: the pipeline scope is remembered across steps, and the Connect step offers one answer for every service of a type."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ui"))

KNOWN = ["PL_A", "PL_B", "PL_C"]


def _scope_page():
    import streamlit as st

    import options

    step = st.session_state.get("which", "convert")
    picked = options.scope_picker("p", ["PL_A", "PL_B", "PL_C"], f"pipes-{step}", st.session_state.get("brief"))
    st.session_state["picked_now"] = picked


def test_nothing_chosen_means_all_and_the_picker_stays_closed():
    at = AppTest.from_function(_scope_page).run()
    assert not at.exception
    assert "Scope: all 3 pipeline(s)" in at.caption[0].value
    assert at.session_state["picked_now"] == []


def test_the_approved_briefs_scope_is_the_first_default():
    at = AppTest.from_function(_scope_page)
    at.session_state["brief"] = {"pipelines": ["PL_B", "PL_X"]}  # PL_X is not in this estate: dropped, not an error
    at.run()
    assert at.session_state["picked_now"] == ["PL_B"]
    assert "Scope: 1 of 3" in at.caption[0].value


def test_a_choice_made_in_one_step_is_where_the_next_step_starts():
    at = AppTest.from_function(_scope_page).run()
    at.multiselect[0].set_value(["PL_A", "PL_C"]).run()
    assert at.session_state["scope"]["p"] == ["PL_A", "PL_C"]
    at.session_state["which"] = "deploy"  # the next step has its own widget key, and starts from what was chosen
    at.run()
    assert at.session_state["picked_now"] == ["PL_A", "PL_C"]
    assert "remembered from an earlier step" in at.caption[0].value


def test_a_sign_in_answer_is_offered_to_the_same_type_but_an_existing_connection_id_never_is():
    import panels

    sql = {"name": "LS_B", "auth_options": [{"id": "sql_login", "available": True}, {"id": "existing", "available": True}]}
    other = {"name": "LS_C", "auth_options": [{"id": "service_principal", "available": True}, {"id": "sql_login", "available": False}]}
    login = {"fields": [{"name": "username"}, {"name": "password"}]}
    existing = {"fields": [{"name": "connection_id"}]}
    assert panels.share_targets([sql, other], "sql_login", login) == [sql]  # LS_C does not offer that method
    assert panels.share_targets([sql, other], "existing", existing) == []  # one account's connection is not another's
    assert panels.share_targets(None, "sql_login", login) == []


def test_a_gateway_service_or_one_missing_a_required_server_is_never_given_the_shared_answer():
    import panels

    ok = {"name": "A", "auth_options": [{"id": "sql_login", "available": True}], "runtime": {"fabric_path": "cloud_connection"},
          "endpoint_fields": [{"name": "server", "required": True, "value": "s.database.windows.net"}]}
    gateway = {**ok, "name": "B", "runtime": {"fabric_path": "gateway"}}
    no_server = {**ok, "name": "C", "endpoint_fields": [{"name": "server", "required": True, "value": None}]}
    login = {"fields": [{"name": "username"}]}
    assert [s["name"] for s in panels.share_targets([ok, gateway, no_server], "sql_login", login)] == ["A"]


def test_a_missing_value_is_not_reported_as_a_failed_test():
    import panels

    assert panels.state_text({"state": "validation_failed", "missing": ["Server is required."]}) == "Needs details"
    assert panels.state_text({"state": "validation_failed", "missing": []}) == "Test failed"


def test_a_409_that_is_not_a_running_job_is_not_told_to_watch_the_banner():
    import components
    from api_client import ApiError

    shown = []

    class Box:
        def warning(self, text):
            shown.append(text)

    import streamlit as st

    real = st.warning
    st.warning = Box().warning
    try:
        def start():
            raise ApiError(409, "Run a dry run of exactly these pipelines first.")
        assert components.run_job(start, "x") is None
        def busy():
            raise ApiError(409, "A 'convert' job is already running for this project.")
        assert components.run_job(busy, "x") is None
    finally:
        st.warning = real
    assert shown[0] == "Run a dry run of exactly these pipelines first."
    assert "banner" in shown[1]
