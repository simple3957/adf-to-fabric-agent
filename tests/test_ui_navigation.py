"""What a step asks of the person (the strip over every step) and that the loader CSS is in the page."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ui"))

import journey  # noqa: E402
import theme  # noqa: E402
from journey import StepFacts, statuses  # noqa: E402


def _status(index, facts, confirmed=()):
    steps = statuses({s.key: facts.get(s.key, StepFacts()) for s in journey.STEPS}, set(confirmed))
    return steps, steps[index]


def test_why_and_what_to_do_shows_source_text_as_text():
    import panels_ext

    shown = panels_ext._plain("PL_[click](javascript:alert(1)) <img src=x> **bold** &amp; `code`")
    assert not any(c in shown for c in "[]()<>*`") and "javascript:alert" in shown  # the words stay, nothing is a link, tag or emphasis
    assert panels_ext.STEP_STAGES["deploy"] == ("unbound", "deploy_failed", "not_deployed", "deployed_differs")


def test_a_locked_step_says_which_step_opens_it():
    steps, locked = _status(1, {})
    assert journey.needed(locked, steps[0].step.title) == ("wait", "Confirm Discover first: this step opens after it.")


def test_an_open_step_not_yet_run_says_what_is_still_needed():
    _, s = _status(0, {"discover": StepFacts(waiting="Read the ADF source.")})
    assert journey.needed(s) == ("act", "Read the ADF source.")


def test_a_finished_step_with_a_warning_asks_for_a_look_before_continuing():
    _, s = _status(0, {"discover": StepFacts(complete=True, attention="2 need manual action")})
    tone, text = journey.needed(s, None, "Review")
    assert tone == "attention" and "2 need manual action" in text


def test_a_finished_clean_step_asks_only_for_the_confirmation_and_a_confirmed_one_for_nothing():
    _, s = _status(0, {"discover": StepFacts(complete=True)})
    assert journey.needed(s, None, "Review") == ("act", "Check the result, then confirm to continue to Review.")
    _, done = _status(0, {"discover": StepFacts(complete=True)}, confirmed={"discover"})
    assert journey.needed(done, None, "Review")[0] == "done"


def test_a_confirmed_step_whose_results_are_gone_is_asked_to_run_again():
    _, s = _status(0, {"discover": StepFacts(complete=False)}, confirmed={"discover"})
    assert journey.needed(s)[0] == "act" and "again" in journey.needed(s)[1]


def test_the_page_stops_the_stale_fade_and_draws_a_ring_while_the_script_runs():
    css = theme.CSS
    assert '[data-stale="true"] { opacity: 1 !important' in css
    assert '.stApp[data-test-script-state="running"]::after' in css and "ma-spin" in css
    assert "prefers-reduced-motion" in css
