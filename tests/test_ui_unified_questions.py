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


def test_every_permission_the_ui_asks_about_is_a_real_one():
    """`can("plans_generate")` (underscore) never matches the server's `plans:generate`, so the button it guards stays disabled for a signed-in person."""
    import re
    from pathlib import Path
    from adf2fabric.api.security.permissions import Permission

    real = {p.value for p in Permission}
    asked = {}
    for f in (Path(__file__).resolve().parents[1] / "ui").rglob("*.py"):
        for m in re.finditer(r"\bcan\(\s*[\"']([^\"']+)[\"']\s*\)", f.read_text(encoding="utf-8")):
            asked.setdefault(m.group(1), f.name)
    assert not {k: v for k, v in asked.items() if k not in real}, "can() was asked about a permission that does not exist"


def test_plan_step_can_be_passed_with_resources_still_to_create():
    """Provisioning a Spark pool is optional for pipelines: the step must not lock Convert until Fabric has been written to."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ui"))
    from journey import plan_facts

    prov = {"available": True, "blockers": [], "summary": {"to_create": 2, "to_configure": 0, "blocked": 0, "conflicts": 0}, "approval": {}, "verification": {}, "run": None}
    f = plan_facts(prov)
    assert f.complete and f.attention and "not created" in f.attention
    blocked = plan_facts({**prov, "blockers": ["no capacity"]})
    assert not blocked.complete  # a real blocker still stops the step


def test_the_connect_form_starts_where_the_person_said_secrets_come_from():
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ui"))
    import panels

    usable = [{"id": "sql_login", "key_vault": False}, {"id": "sql_login_kv", "key_vault": True}, {"id": "existing_connection", "key_vault": False}]
    assert panels.preferred_method(usable, "keyvault") == "sql_login_kv"
    assert panels.preferred_method(usable, "existing") == "existing_connection"
    assert panels.preferred_method(usable, "person") is None and panels.preferred_method([usable[0]], "keyvault") is None
    card = {"key_vault_hint": {"vault_url": "https://kv-prod.vault.azure.net", "secret_name": "sql-password"}}
    assert panels.vault_default("vault_url", card, {"key_vault_url": "https://other.vault.azure.net"}) == "https://kv-prod.vault.azure.net"  # ADF's own vault first
    assert panels.vault_default("vault_url", {}, {"key_vault_url": "https://kv1.vault.azure.net"}) == "https://kv1.vault.azure.net"
    assert panels.vault_default("secret_name", card, None) == "sql-password" and panels.vault_default("secret_name", {}, None) == ""


def test_a_page_read_is_reused_until_this_session_changes_something():
    """Every click reran the page and re-read all nine steps' facts (4.6 s on a real project, cards alone ask Fabric). A read now lasts until a change."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ui"))
    from streamlit.testing.v1 import AppTest

    def script():
        import streamlit as st
        from components import read

        if "api" not in st.session_state:
            class Api:
                version = 0
                n = 0

                def thing(self, pid):
                    self.n += 1
                    return {"n": self.n}

            st.session_state.api = Api()
        api = st.session_state.api
        st.session_state.out = [read(api, "thing", "p")["n"], read(api, "thing", "p")["n"]]

    at = AppTest.from_function(script).run()
    assert at.session_state.out == [1, 1]  # asked once, used twice
    at.run()
    assert at.session_state.out == [1, 1]  # a rerun (a click) asks nothing
    at.session_state.api.version += 1  # this session changed something (a save, a finished job)
    at.run()
    assert at.session_state.out == [2, 2]  # asked again, once


def test_the_client_counts_the_changes_it_makes_and_not_the_reads():
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ui"))
    from api_client import ApiClient

    class Http:
        def request(self, method, url, **kw):
            class R:
                status_code, content = 200, b"{}"
                headers: dict = {}

                def json(self):
                    return {}

            return R()

    api = ApiClient("s", base_url="http://x", http=Http())
    api._request("GET", "/a")
    api._request("GET", "/b")
    assert api.version == 0
    api._request("POST", "/c")
    api._request("PUT", "/d")
    assert api.version == 2
    api.bump()
    assert api.version == 3
