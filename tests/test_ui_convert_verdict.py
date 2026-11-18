"""The Convert step's one-line verdict (ui/panels.convert_verdict, 2026-11-18): 'partial' was shown whenever an activity had a difference to
check, which read as 'not everything is converted' when nothing was blocked or left manual. The inputs are the real counts of the real
conversion of this project's estate shapes (the API's convert summary)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("streamlit")

UI_DIR = str(Path(__file__).resolve().parents[1] / "ui")


@pytest.fixture(scope="module")
def verdict():
    sys.path.insert(0, UI_DIR)
    import panels

    return panels.convert_verdict


def _pipes(*bad):
    return [{"name": f"PL_{i}", "structurally_valid": i not in bad, "runtime_ready": True, "status": "converted"} for i in range(3)]


def test_differences_to_check_do_not_make_a_conversion_partial(verdict):
    text, kind = verdict({"status": "partial", "pipelines": _pipes(),
                          "status_counts": {"converted": 60, "converted_with_differences": 3, "manual_required": 0, "blocked": 0}})
    assert kind == "success" and text == "Everything is converted: 3 pipeline(s), 63 activities; 3 activities have differences to check"


def test_one_difference_reads_in_the_singular(verdict):
    assert verdict({"pipelines": _pipes(), "status_counts": {"converted": 4, "converted_with_differences": 1}})[0].endswith("1 activity has differences to check")


def test_blocked_or_manual_work_is_named_never_hidden(verdict):
    text, kind = verdict({"pipelines": _pipes(), "status_counts": {"converted": 58, "manual_required": 2, "blocked": 1}})
    assert kind == "warning" and "1 blocked" in text and "2 need manual work" in text and text.startswith("Not everything is converted")


def test_a_pipeline_that_is_not_valid_is_not_converted_even_when_no_activity_is_blocked(verdict):
    text, kind = verdict({"pipelines": _pipes(1), "status_counts": {"converted": 9}})
    assert kind == "warning" and "1 pipeline(s) not valid" in text
