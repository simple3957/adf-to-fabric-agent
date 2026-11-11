"""Validate step (2026-11-11): only the static check is shown there, as a dropdown under the results table with every check it ran. What
Deploy read back and what the runs recorded are plain lines, never a red mark for a check this step does not make."""

from __future__ import annotations

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from tests.test_ui import UI  # noqa: E402


def _script(ui_dir, readback_current):
    import sys

    sys.path.insert(0, ui_dir)
    import panels_ext

    stage = lambda s, status, p=0, f=0, b=0, n=0: {"stage": s, "status": status, "passed": p, "failed": f, "blocked": b, "not_run": n}  # noqa: E731
    val = {
        "available": True, "overall": "NOT_VALIDATED", "reasons": ["10 required check(s) were NOT RUN: runtime.execution"], "full_success": False,
        "stages_selected": ["static", "api", "deployment", "runtime"] if readback_current else ["static", "runtime"],
        "stages": [stage("static", "PASS", p=2), stage("api", "PASS", p=1), stage("deployment", "PASS", p=1), stage("runtime", "NOT_RUN", n=1)],
        "checks_by_stage": {
            "static": [{"id": "target_schema.pipeline", "result": "PASS", "summary": "Valid.", "component": "", "remediation": "", "required": True},
                       {"id": "activity_graph.dependencies", "result": "PASS", "summary": "Same.", "component": "", "remediation": "", "required": True}],
            "runtime": [{"id": "runtime.execution", "result": "NOT_RUN", "summary": "Never run.", "component": "", "remediation": "", "required": True}]},
        "readback": {"at": "2026-11-11T10:20:41+00:00", "current": readback_current,
                     "note": "" if readback_current else "Nothing has been read back from Fabric yet: it is read back on the Deploy step, right after a deploy.",
                     "items": [{"kind": "Pipeline", "name": "PL_Copy_Test", "state": "In Fabric, same as generated", "ok": True, "detail": ""}]
                     if readback_current else []},
        "open_checks": [], "open_checks_total": 0, "components": {},
        "components_needing_review": [{"id": "pipeline:PL_X", "status": "missing", "note": "not deployed", "diff": []}]}
    panels_ext._validation_body(val, show=("static",))
    panels_ext._checked_elsewhere(val)


def _run(readback_current):
    at = AppTest.from_function(_script, args=(str(UI), readback_current), default_timeout=60).run()
    assert not at.exception, at.exception
    return at


def test_the_validate_step_shows_only_the_static_check_with_every_check_it_ran():
    at = _run(True)
    labels = [e.label for e in at.expander]
    assert labels == [labels[0]] and labels[0].startswith("Static:") and "2 check(s) performed" in labels[0]  # nothing about not-deployed items
    assert list(at.dataframe[0].value["Stage"]) == ["Static"]
    text = " ".join(m.value for m in at.markdown) + " ".join(c.value for c in at.caption)
    assert "Overall: NOT VALIDATED" in text and "Static: ✅ pass" in text
    assert "read back from Fabric on the Deploy step" in text and "1 of 1 item(s) as expected" in text
    assert "Runtime: recorded when the pipelines run in Fabric" in text


def test_without_a_read_back_the_validate_step_says_where_it_happens_and_shows_no_failure():
    at = _run(False)
    text = " ".join(c.value for c in at.caption)
    assert "it is read back on the Deploy step" in text
    assert not at.error and not any("❌" in (e.label or "") for e in at.expander)
