"""UI tests: the journey rules and the API client's error handling.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402


UI = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(UI))
from api_client import ApiClient, ApiError  # noqa: E402
import journey  # noqa: E402

SECRET = "S3cr3t-UI-Value-77"


def test_client_unreachable_api():
    with pytest.raises(ApiError) as err:
        ApiClient(session_id="x", base_url="http://127.0.0.1:9", timeout=2).health()
    assert err.value.status == 0 and err.value.actions


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


def test_a_stale_discovery_is_shown_as_out_of_date_not_done():
    from journey import discover_facts

    f = discover_facts({"has_semantic": False, "semantic_stale": True}, None)
    assert not f.complete and f.metric == "Out of date"


def test_an_empty_existing_connection_list_says_why():

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
