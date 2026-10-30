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


# ---- the final score: one number from the range, and written rules that can only lower its grade ----------------------------------------

def _d(low, high, labels=None, stale=False):
    return {"overall": {"low": low, "high": high, "gap": high - low}, "stale": stale,
            "areas": {"pipelines": {"title": "Pipelines", "low": low, "labels": labels or {"proven": 1}, "share": 100.0, "points": float(low)}}}


VERIFIED = {"overall": "MIGRATION_VERIFIED", "current": True}


def test_the_score_is_the_established_end_of_the_range_and_bands_give_the_grade():
    assert [(s["value"], s["grade"]) for s in (engine.final_score(_d(v, v + 5), VERIFIED) for v in (90, 85, 84, 60, 59))] == \
        [(90, "High"), (85, "High"), (84, "Medium"), (60, "Medium"), (59, "Low")]
    s = engine.final_score(_d(90, 95), VERIFIED)
    assert s["could_reach"] == 95 and all(r["passed"] for r in s["rules"]) and sum(b["points"] for b in s["breakdown"]) == 90


def test_high_needs_a_current_verified_validation():
    assert engine.final_score(_d(95, 95), None)["grade"] == "Medium"  # never validated
    assert engine.final_score(_d(95, 95), {"overall": "NOT_VALIDATED", "current": True})["grade"] == "Medium"
    assert engine.final_score(_d(95, 95), {"overall": "MIGRATION_VERIFIED", "current": False})["grade"] == "Medium"  # older than the conversion
    s = engine.final_score(_d(95, 95), {"overall": "FAILED", "current": True})
    assert s["grade"] == "Low" and s["band_grade"] == "High" and "FAILED" in s["validation"]


def test_blocked_or_failing_items_a_changed_source_or_a_wide_gap_lower_the_grade():
    assert engine.final_score(_d(90, 95, {"proven": 3, "failing": 1}), VERIFIED)["grade"] == "Low"
    assert engine.final_score(_d(90, 95, {"blocked": 1}), VERIFIED)["grade"] == "Low"
    assert engine.final_score(_d(90, 95, stale=True), VERIFIED)["grade"] == "Low"
    assert engine.final_score(_d(86, 100), VERIFIED)["grade"] == "High"  # 14 points assumed: within the limit
    s = engine.final_score(_d(86, 112), VERIFIED)  # 26 points assumed
    assert s["grade"] == "Medium" and [r["limits_to"] for r in s["rules"] if not r["passed"]] == ["Medium"]
    assert engine.final_score(_d(50, 60), VERIFIED)["grade"] == "Low"  # a rule never raises a grade


def test_the_score_comes_last_in_the_report_and_follows_the_validation_on_file(project):  # noqa: F811
    store, pid = project
    run(store)
    d = engine.assess(store, pid)
    assert d["score"]["value"] == d["overall"]["low"] and d["score"]["validation"] == "not run" and d["score"]["grade"] != "High"
    assert abs(sum(b["points"] for b in d["score"]["breakdown"]) - d["overall"]["low"]) < 1.5  # the areas add up to the score
    md = engine.render(d)
    assert md.rstrip().splitlines()[-1].startswith("| No more than")  # the score section is the end of the report
    assert md.index("## Confidence score") > md.index("## Pipelines")


# ---- a linked service the migration does not need is listed, not scored -----------------------------------------------------------------

def _cov(*names):
    from types import SimpleNamespace as NS

    return NS(connections=[], connector_coverage=[NS(linked_service=n, adf_type="AzureBlobFS", level="created", note="") for n in names])


def test_a_linked_service_replaced_by_the_lakehouse_or_used_by_no_pipeline_is_not_needed():
    uses = {"ls_lake": ["repointed_to_lakehouse", "not_used"], "ls_idle": ["not_used", "not_used"]}
    lake, idle = engine._service_items(_cov("LS_Lake", "LS_Idle"), {}, uses, all_converted=True)
    assert "replaced by the lakehouse" in lake.not_needed and not lake.criteria
    assert "no pipeline uses it" in idle.not_needed


def test_a_linked_service_is_still_scored_when_that_is_not_shown():
    cases = {
        "ls_inline": ["repointed_to_lakehouse", "inlined"],  # one dataset still reads through its connection
        "ls_held": ["not_used"],  # unused, but a pipeline was held back from conversion and may need it
        "ls_vault": [],  # no datasets (Key Vault, compute): its datasets say nothing
    }
    for ls, treatments in cases.items():
        (item,) = engine._service_items(_cov(ls), {}, {ls: treatments}, all_converted=False)
        assert not item.not_needed and item.criteria, ls
    (item,) = engine._service_items(_cov("ls_vault"), {}, {}, all_converted=True)
    assert not item.not_needed


def test_a_bound_linked_service_is_never_called_not_needed():
    from types import SimpleNamespace as NS

    conv = _cov("LS_Lake")
    conv.connections = [NS(name="LS_Lake", kind="connection", bound=True)]
    (item,) = engine._service_items(conv, {}, {"ls_lake": ["repointed_to_lakehouse"]}, all_converted=True)
    assert not item.not_needed  # a generated activity uses its connection: it is needed and scored


def test_a_not_needed_linked_service_leaves_the_score_and_is_listed_in_the_report(project):  # noqa: F811
    store, pid = project
    run(store)
    uses, all_converted = engine._dataset_use(store, pid, store.load_conversion(pid))
    assert all_converted and uses  # read from discovery and the conversion's dataset ledger
    conv = store.load_conversion(pid)
    ls = next(c.linked_service for c in conv.connector_coverage if uses.get(c.linked_service.lower()))
    sem = store.load_adf_semantic_discovery(pid)
    mine = {d.name.lower() for d in sem.datasets if d.linked_service_ref and d.linked_service_ref.name.lower() == ls.lower()}
    for e in conv.dataset_ledger:  # as convert --lakehouse records it: the datasets now read the Lakehouse, no activity binds the connection
        if e.name.lower() in mine:
            e.treatment = "repointed_to_lakehouse"
    conv.connections = [q for q in conv.connections if q.name.lower() != ls.lower()]
    store.save_conversion(pid, conv)
    d = engine.assess(store, pid)
    assert [n["name"] for n in d["not_needed"]] == [ls] and "replaced by the lakehouse" in d["not_needed"][0]["reason"]
    assert ls not in [it["name"] for a in d["areas"].values() for it in a["items"]]  # listed, not scored
    assert "Not needed by the migration" in engine.render(d)
