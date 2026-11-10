"""Deploy step: a generated notebook that is not in Fabric yet can be created from the UI (found 2026-11-10: for a Databricks notebook or a
Databricks job's notebook task the Convert step's list of ids never offers it, so the UI had no way to create it)."""

from __future__ import annotations

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from tests.test_ui import UI  # noqa: E402


def _script(ui_dir):
    import sys

    sys.path.insert(0, ui_dir)
    import streamlit as st

    import panels

    class Api:
        def __init__(self):
            self.calls = st.session_state.setdefault("calls", [])

        def workload_notebooks(self, pid):
            return {"items": [
                {"name": "NB_New", "used_by": ["PL_Spark/Run"], "generated": True, "pipeline_item_id": "", "bound_item_id": "",
                 "approval": "pending", "state": "not bound"},
                {"name": "NB_Done", "used_by": ["PL_Spark/Other"], "generated": True, "pipeline_item_id": "11111111-1111-1111-1111-111111111111",
                 "bound_item_id": "11111111-1111-1111-1111-111111111111", "approval": "approved", "state": "bound"}], "note": ""}

        def create_workload_notebook(self, pid, workload, confirm=False):
            self.calls.append((workload, confirm))
            return {"done": confirm, "detail": "Fabric would create a new notebook" if not confirm else "Created in Fabric: its id is saved. Convert again to use it.",
                    "exists": False}

    st.session_state.setdefault("principal", {"roles": ["admin"], "permissions": ["*"]})
    panels._notebooks(Api(), "test-proj")


def _create_buttons(at):
    return [b for b in at.button if (getattr(b, "label", "") or "").startswith("Create it in Fabric")]


def test_a_generated_notebook_not_in_fabric_can_be_created_and_a_bound_one_is_not_offered():
    at = AppTest.from_function(_script, args=(str(UI),), default_timeout=60).run()
    assert not at.exception, at.exception
    assert len(_create_buttons(at)) == 1  # NB_New only; NB_Done is bound
    _create_buttons(at)[0].click().run()
    assert at.session_state["calls"] == [("NB_New", False)]  # first click only asks Fabric what would happen
    assert any("You are recorded as accepting" in w.value for w in at.warning)
    yes = next(b for b in at.button if getattr(b, "label", "") == "Yes, create")
    yes.click().run()
    assert at.session_state["calls"] == [("NB_New", False), ("NB_New", True)]
    assert any("Created in Fabric" in s.value for s in at.success)


def _editor_script(ui_dir):
    import sys

    sys.path.insert(0, ui_dir)
    import streamlit as st

    import panels

    class Api:
        def set_choice(self, *a, **k):
            st.session_state.setdefault("saved", []).append(a)

    card = {"name": "AzureDatabricks1", "state": "skipped", "supported": True, "choice": {"method": "skip"}, "auth_options": [],
            "adf_type": "AzureDatabricks", "connector_label": "Azure Databricks workspace", "fabric_connection_type": "AzureDatabricksWorkspace"}
    panels._editor(Api(), "test-proj", card, {})


def test_a_reopened_skipped_linked_service_can_go_back_to_skipped_without_saving():
    # found 2026-11-10: after 'Set up a connection after all' the form asked for a connection id and had no way back to 'Skip'
    at = AppTest.from_function(_editor_script, args=(str(UI),), default_timeout=60).run()
    assert any("Skipped: no Fabric connection" in i.value for i in at.info)
    at.button(key="reopen-btn-AzureDatabricks1").click().run()
    assert not any("Skipped: no Fabric connection" in i.value for i in at.info)
    at.button(key="keep-skip-AzureDatabricks1").click().run()
    assert any("Skipped: no Fabric connection" in i.value for i in at.info)
    assert "saved" not in at.session_state  # nothing was saved
