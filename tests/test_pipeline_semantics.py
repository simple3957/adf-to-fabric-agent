"""Pipeline semantics: source outcome rules and independent verification of the assembled graph."""

from __future__ import annotations

import copy

from adf2fabric.agent.conversion.agent import build_project_context
from adf2fabric.agent.conversion.context import Resolutions
from adf2fabric.agent.conversion.engine import ConversionEngine
from adf2fabric.agent.conversion.pipeline_semantics import outcome_notes, verify_graph
from tests.conversion_support import act, dep, expr, pipe, semantic


def _wait(name, *deps):
    return act(name, "Wait", {"waitTimeInSeconds": 1}, deps=list(deps))


def _build(p):
    d = semantic([p])
    source = d.pipelines[0]
    record, content = ConversionEngine(build_project_context(d, Resolutions())).convert_pipeline(p=source)
    return source, record, content


def _codes(issues):
    return sorted(i.code for i in issues)


MIXED = pipe(
    _wait("A"),
    _wait("B", dep("A", "Failed")),
    _wait("C", dep("A", "Succeeded")),
    _wait("D", dep("B", "Completed"), dep("C", "Succeeded")),
    act("Loop", "ForEach", {"items": expr("@createArray(1,2)"), "activities": [
        _wait("L1"), _wait("L2", dep("L1", "Skipped"))]}, deps=[dep("D")]),
    name="PL")


def test_a_correct_conversion_reproduces_the_source_graph():
    source, record, content = _build(MIXED)
    assert verify_graph(source, record.activities, content) == []
    assert record.structural_valid


def test_dropped_dependency_is_detected():
    source, record, content = _build(MIXED)
    bad = copy.deepcopy(content)
    next(a for a in bad["properties"]["activities"] if a["name"] == "D")["dependsOn"].pop()
    assert "graph_edge_missing" in _codes(verify_graph(source, record.activities, bad))


def test_changed_condition_is_detected():
    source, record, content = _build(MIXED)
    bad = copy.deepcopy(content)
    b = next(a for a in bad["properties"]["activities"] if a["name"] == "B")
    b["dependsOn"][0]["dependencyConditions"] = ["Succeeded"]  # Failed -> Succeeded would invert the routing
    issues = verify_graph(source, record.activities, bad)
    assert "graph_condition_changed" in _codes(issues)
    assert "Failed" in issues[0].message


def test_added_dependency_is_detected():
    source, record, content = _build(MIXED)
    bad = copy.deepcopy(content)
    next(a for a in bad["properties"]["activities"] if a["name"] == "B")["dependsOn"].append(
        {"activity": "C", "dependencyConditions": ["Succeeded"]})
    assert "graph_edge_added" in _codes(verify_graph(source, record.activities, bad))


def test_nested_dependency_is_verified_and_missing_activity_is_detected():
    source, record, content = _build(MIXED)
    bad = copy.deepcopy(content)
    loop = next(a for a in bad["properties"]["activities"] if a["name"] == "Loop")
    inner = loop["typeProperties"]["activities"]
    next(a for a in inner if a["name"] == "L2")["dependsOn"] = []
    assert "graph_edge_missing" in _codes(verify_graph(source, record.activities, bad))
    loop["typeProperties"]["activities"] = [a for a in inner if a["name"] != "L1"]
    assert "graph_activity_missing" in _codes(verify_graph(source, record.activities, bad))


def test_outcome_patterns_follow_the_documented_adf_rules():
    try_catch = pipe(_wait("Main"), _wait("Handler", dep("Main", "Failed")), name="TC")
    if_else = pipe(_wait("Main"), _wait("Ok", dep("Main", "Succeeded")), _wait("Err", dep("Main", "Failed")), name="IE")
    skip_else = pipe(_wait("Main"), _wait("Ok", dep("Main", "Succeeded")), _wait("Err", dep("Main", "Failed")),
                     _wait("Dummy", dep("Main", "Skipped")), name="SE")
    d = semantic([try_catch, if_else, skip_else])
    notes = {p.name: "\n".join(outcome_notes(p)) for p in d.pipelines}
    assert "Try-Catch" in notes["TC"] and "still succeeds" in notes["TC"]
    assert "Do-If-Else" in notes["IE"] and "FAILS" in notes["IE"]
    assert "Do-If-Skip-Else" in notes["SE"]
    assert "decided by leaf activit" in notes["TC"]  # leaves are the deciders: Handler, not Main
    assert "Handler" in notes["TC"].splitlines()[0]


def test_outcome_notes_reach_the_conversion_record_and_scopes_are_separate():
    _, record, _ = _build(MIXED)
    text = "\n".join(record.notes)
    assert "Outcome (pipeline)" in text and "Outcome (activities of Loop)" in text
