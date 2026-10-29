"""Migration confidence: a range from criteria met, never a guess. Bounds, gates, labels, staleness, and the real conversion records."""

from __future__ import annotations

import json

from click.testing import CliRunner

import adf2fabric.cli as cli_mod
from adf2fabric.agent.confidence import engine
from adf2fabric.agent.confidence.engine import MET, NA, UNKNOWN, UNMET, Criterion, _apply_gates, bounds, label_for
from tests.test_conversion_agent import project, run  # noqa: F401  (the fixture)


def c(id_, state, weight=1.0, needs=()):
    return Criterion(id_, id_, weight, state, needs=tuple(needs))


def test_bounds_count_only_evidence_in_the_low_end_and_unknowns_in_the_high_end():
    low, high, known = bounds([c("a", MET, 2), c("b", UNKNOWN, 1), c("d", UNMET, 1)])
    assert (low, high, known) == (0.5, 0.75, 0.75)
    assert low <= high


def test_not_applicable_is_left_out_and_nothing_applicable_gives_no_range():
    assert bounds([c("a", MET, 1), c("b", NA, 9)]) == (1.0, 1.0, 1.0)
    assert bounds([c("b", NA)]) is None


def test_an_unmet_gate_makes_what_depends_on_it_unmet_not_unknown():
    cs = [c("fully_converted", UNMET), c("deployed", UNKNOWN, needs=["fully_converted"]), c("ran_ok", MET, needs=["deployed"])]
    _apply_gates(cs)
    assert [x.state for x in cs] == [UNMET, UNMET, UNMET]
    assert "cannot be evidenced" in cs[1].evidence


def test_labels_name_the_worst_thing_in_the_way():
    assert label_for([c("fully_converted", UNMET), c("ran_ok", UNMET)]) == "blocked"
    assert label_for([c("ran_ok", UNMET), c("bindings_complete", UNMET)]) == "failing"
    assert label_for([c("bindings_complete", UNMET)]) == "needs_input"
    assert label_for([c("ran_ok", UNKNOWN), c("x", MET)]) == "unproven"
    assert label_for([c("x", MET)]) == "proven"


def test_a_project_without_conversion_says_so(tmp_path):
    from adf2fabric.project.store import ProjectStore

    d = engine.assess(ProjectStore(tmp_path), "nothing")
    assert d["ready"] is False and "convert" in d["message"]


def test_a_converted_project_gets_a_range_with_questions_and_no_proof_without_a_run(project):  # noqa: F811
    store, pid = project
    run(store)
    d = engine.assess(store, pid)
    o = d["overall"]
    assert d["ready"] and 0 <= o["low"] <= o["high"] <= 100
    assert o["label"] != "proven"  # nothing was ever run: converted files are not evidence that the pipelines work
    pipes = d["areas"]["pipelines"]["items"]
    for it in pipes:
        ran = next(x for x in it["criteria"] if x["id"] == "ran_ok")
        assert ran["state"] in ("unknown", "unmet")  # never 'met' without a recorded run
        assert it["high"] >= it["low"]
    assert all(q["why"] and q["question"] for q in d["questions"])
    lows = [q["raises_low_by"] for q in d["questions"]]
    assert lows == sorted(lows, reverse=True)


def test_a_recorded_matching_run_raises_the_range_and_a_failed_one_lowers_it(project):  # noqa: F811
    store, pid = project
    run(store)
    name = next(iter(store.load_conversion(pid).pipelines)).source_pipeline
    before = engine.assess(store, pid)["areas"]["pipelines"]
    vdir = store.project_dir(pid) / "verification"
    vdir.mkdir(exist_ok=True)

    def with_verdict(v):
        (vdir / "verification.json").write_text(json.dumps({name: {"verdict": v, "activities": {}}}), encoding="utf-8")
        return next(i for i in engine.assess(store, pid)["areas"]["pipelines"]["items"] if i["name"] == name)

    base = next(i for i in before["items"] if i["name"] == name)
    ok = with_verdict("matches")
    assert ok["low"] > base["low"]
    failed = with_verdict("failed")
    assert failed["label"] == "failing" and failed["high"] < ok["high"]


def test_a_changed_source_demotes_what_was_proven_to_unknown(project):  # noqa: F811
    store, pid = project
    run(store)
    assert engine.assess(store, pid)["stale"] is False
    p = store.load(pid)
    p.repository.commit_sha = "f" * 40  # the source moved on after the conversion
    store.save(p)
    d = engine.assess(store, pid)
    assert d["stale"] is True
    assert all(x["state"] != "met" for it in d["areas"]["pipelines"]["items"] for x in it["criteria"])


def test_cli_writes_the_report_and_says_it_is_a_range(project, monkeypatch):  # noqa: F811
    store, pid = project
    run(store)
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    out = CliRunner().invoke(cli_mod.cli, ["confidence", "--project", "test-proj"])
    assert out.exit_code == 0, out.output
    assert "CONFIDENCE" in out.output and "% to " in out.output
    md = (store.project_dir(pid) / "confidence" / "confidence.md").read_text(encoding="utf-8")
    assert "not a statistical probability" in md and "What to find out first" in md


def test_a_run_criterion_made_unmet_only_by_a_gate_is_not_called_failing():
    cs = [c("bindings_complete", UNMET), c("deployed", UNKNOWN, needs=["bindings_complete"]), c("ran_ok", UNKNOWN, needs=["deployed"])]
    _apply_gates(cs)
    assert cs[2].state == UNMET and cs[2].gated
    assert label_for(cs) == "needs_input"  # nothing ran: the cause is the open binding
    assert label_for([c("ran_ok", UNMET)]) == "failing"  # a run that really failed is still failing


def test_a_service_no_converted_pipeline_uses_is_not_called_proven():
    from types import SimpleNamespace as NS

    conv = NS(connections=[], connector_coverage=[NS(linked_service="LS_X", adf_type="AzureBlobFS", level="manual", note="")])
    (item,) = engine._service_items(conv, {})
    assert label_for(item.criteria) == "unproven"
    lo, hi, _ = bounds(item.criteria)
    assert lo < hi
