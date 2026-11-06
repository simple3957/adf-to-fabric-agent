"""The migration results after deploy: built from the deploy record, the run-and-compare record and the output checks; ADF beside Fabric."""

from __future__ import annotations

import json
from pathlib import Path

from adf2fabric.agent.deployment import run_report, verify

WS = "11111111-2222-3333-4444-555555555555"


def _copy(rows: int, files: int = 1, status: str = "Succeeded") -> dict:
    return {"type": "Copy", "status": status, "runs": 1, "metrics": {"rowsRead": rows, "rowsCopied": rows, "filesRead": files, "filesWritten": files}}


def _project(tmp_path: Path, runs: dict, deployed: dict, checks: dict | None = None) -> Path:
    (tmp_path / "deployment").mkdir()
    (tmp_path / "deployment" / "deployment-record.json").write_text(json.dumps({"pipelines": deployed}))
    (tmp_path / "verification").mkdir()
    (tmp_path / "verification" / "verification.json").write_text(json.dumps(runs))
    if checks:
        (tmp_path / "verification" / "output-checks.json").write_text(json.dumps(checks))
    return tmp_path


def test_a_matched_run_shows_adf_beside_fabric_and_the_totals_compare_like_for_like(tmp_path):
    runs = {"PL_A": {"verdict": "matches", "fabric_status": "Completed", "ran_at": "2026-11-04T06:18:06Z", "baseline": "adf",
                     "baseline_at": "2026-10-21T06:17:18Z", "matched": 6, "differences": [], "activities": {"Copy1": _copy(10)},
                     "baseline_activities": {"Copy1": _copy(10)}},
            "PL_B": {"verdict": "matches", "fabric_status": "Completed", "ran_at": "2026-11-04T06:20:00Z", "baseline": "fabric",
                     "matched": 4, "differences": [], "activities": {"Copy1": _copy(7)}}}  # compared before baseline figures were kept
    d = run_report.build(_project(tmp_path, runs, {"PL_A": {"ok": True, "at": 1793772000, "workspace": WS, "action": "update"},
                                                   "PL_B": {"ok": True, "at": 1793772000, "workspace": WS, "action": "create"}}),
                         ["PL_A", "PL_B", "PL_C"], WS)
    s = d["summary"]
    assert (s["pipelines"], s["deployed"], s["succeeded"], s["matches"], s["not_run"]) == (3, 2, 2, 2, 1)
    assert (s["compared_with_adf"], s["compared_with_fabric"], s["checks_matched"]) == (1, 1, 10)
    assert s["fabric_moved"]["rowsCopied"] == 17  # every run
    assert s["compared_moved"] == {"pipelines": 1, "fabric": {"rowsRead": 10, "rowsCopied": 10, "filesRead": 1, "filesWritten": 1},
                                   "baseline": {"rowsRead": 10, "rowsCopied": 10, "filesRead": 1, "filesWritten": 1}}
    a, b, c = d["pipelines"]
    assert a["activities"][0]["result"] == "matches" and a["activities"][0]["baseline_rowsCopied"] == 10
    assert b["baseline_moved"] is None and not b["baseline_figures"] and s["baseline_figures_missing"] == 1
    assert c["verdict"] == "not_run" and not c["deployed"]


def test_a_difference_names_the_fact_and_both_values(tmp_path):
    runs = {"PL_A": {"verdict": "differs", "fabric_status": "Completed", "ran_at": "2026-11-04T06:18:06Z", "baseline": "adf", "matched": 3,
                     "differences": [{"activity": "Copy1", "fact": "rowsCopied", "baseline": 10, "fabric": 9}],
                     "activities": {"Copy1": _copy(9)}, "baseline_activities": {"Copy1": _copy(10)}}}
    d = run_report.build(_project(tmp_path, runs, {"PL_A": {"ok": True, "at": 1793772000, "workspace": WS}}), ["PL_A"], WS)
    assert d["summary"]["differs"] == 1 and d["summary"]["differences_total"] == 1
    assert d["pipelines"][0]["activities"][0]["result"] == "differs: rowsCopied ADF 10 / Fabric 9"


def test_a_deploy_to_another_workspace_or_a_run_before_the_latest_deploy_does_not_count(tmp_path):
    runs = {"PL_A": {"verdict": "matches", "fabric_status": "Completed", "ran_at": "2026-11-01T00:00:00Z", "baseline": "adf", "matched": 2,
                     "differences": [], "activities": {"Copy1": _copy(1)}}}
    d = run_report.build(_project(tmp_path, runs, {"PL_A": {"ok": True, "at": 1793772000, "workspace": WS}}), ["PL_A"], WS)
    p = d["pipelines"][0]
    assert p["stale_run"] and p["verdict"] == "not_run" and d["summary"]["ran"] == 0
    d = run_report.build(tmp_path, ["PL_A"], "99999999-0000-0000-0000-000000000000")
    assert not d["pipelines"][0]["deployed"]


def test_output_checks_are_listed_with_their_row_counts(tmp_path):
    checks = {"PL_A": {"out": {"verdict": "matches", "source_summary": {"rows": 5}, "target_summary": {"rows": 5}, "checked_at": "2026-11-05T00:00:00+00:00"}}}
    d = run_report.build(_project(tmp_path, {}, {}, checks), ["PL_A"])
    assert d["pipelines"][0]["output_checks"][0]["target_rows"] == 5 and d["summary"]["output_checks_matching"] == 1


def test_verify_keeps_the_baselines_own_figures():
    v = verify.Verification("PL_A", "failed", fabric_status="Completed", activities={"Copy1": _copy(10)})
    verify._judge(v, {"Copy1": _copy(10)})
    assert v.verdict == "matches" and v.to_dict()["baseline_activities"] == {"Copy1": _copy(10)}


class _Client:
    def list_pipelines(self, workspace):
        return {}


class _Adf:
    def prefetch(self):
        pass

    def clone(self):
        return self


def _reread(monkeypatch, previous, baseline=""):
    """verify_all with a fake verify_pipeline: re-reading the same Fabric run, comparing it with ADF only when it is given ADF."""
    seen = {}

    def fake(client, workspace, name, *, start, adf, previous, **_kw):
        seen[name] = adf is not None
        if adf is not None:
            return verify.Verification(name, "differs", fabric_status="Completed", job_id="run-1", baseline="adf", matched=1,
                                       differences=[{"activity": "Copy1", "fact": "rowsCopied", "baseline": 19, "fabric": 6019}])
        return verify.Verification(name, "no_baseline", fabric_status="Completed", job_id="run-1")

    monkeypatch.setattr(verify, "verify_pipeline", fake)
    out = verify.verify_all(lambda: _Client(), WS, ["PL_Rig"], start=False, adf=_Adf(), previous=previous, baseline=baseline)
    return out[0], seen["PL_Rig"]


def test_a_chosen_fabric_baseline_survives_reading_the_runs_again(monkeypatch):
    prev = {"PL_Rig": {"pipeline": "PL_Rig", "verdict": "matches", "fabric_status": "Completed", "job_id": "run-1", "baseline": "fabric",
                       "matched": 14, "baseline_choice": "fabric"}}
    v, used_adf = _reread(monkeypatch, prev)
    assert not used_adf  # the newer ADF run, known not to be comparable, is not read
    assert (v.verdict, v.baseline, v.matched, v.baseline_choice) == ("matches", "fabric", 14, "fabric")  # the recorded comparison stands


def test_without_a_choice_the_adf_run_is_used_and_use_adf_undoes_a_choice(monkeypatch):
    prev = {"PL_Rig": {"pipeline": "PL_Rig", "verdict": "matches", "job_id": "run-1", "baseline": "fabric", "matched": 14}}
    v, used_adf = _reread(monkeypatch, prev)
    assert used_adf and v.verdict == "differs"
    v, used_adf = _reread(monkeypatch, {"PL_Rig": {**prev["PL_Rig"], "baseline_choice": "fabric"}}, baseline="adf")
    assert used_adf and v.baseline_choice == ""


def test_choosing_fabric_is_remembered_in_the_record(tmp_path, monkeypatch):
    v, used_adf = _reread(monkeypatch, {}, baseline="fabric")
    assert not used_adf and v.baseline_choice == "fabric"
    rec = verify.save_record(tmp_path / "verification" / "verification.json", [v])
    assert rec["PL_Rig"]["baseline_choice"] == "fabric"


def test_a_run_in_another_workspace_is_not_counted_and_an_old_run_belongs_where_the_pipeline_was_deployed(tmp_path):
    other = "99999999-0000-0000-0000-000000000000"
    runs = {"PL_A": {"verdict": "matches", "fabric_status": "Completed", "ran_at": "2026-11-04T06:18:06Z", "baseline": "adf", "matched": 6,
                     "differences": [], "activities": {"Copy1": _copy(10)}}}  # recorded before runs carried their workspace
    project = _project(tmp_path, runs, {"PL_A": {"ok": True, "at": 1793772000, "workspace": WS}})
    assert verify.current_record(project, WS)["PL_A"]["verdict"] == "matches"  # it ran where it was deployed
    seen = verify.current_record(project, other)["PL_A"]
    assert seen["verdict"] == "not_run" and seen["other_workspace"] and "another Fabric workspace" in seen["detail"]
    d = run_report.build(project, ["PL_A"], other)
    assert (d["summary"]["deployed"], d["summary"]["ran"], d["summary"]["matches"]) == (0, 0, 0)


def test_verify_records_the_workspace_each_run_was_in(monkeypatch):
    v, _ = _reread(monkeypatch, {})
    assert v.workspace == WS.lower()

