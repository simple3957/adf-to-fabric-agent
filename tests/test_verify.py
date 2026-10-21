"""Run-and-compare: a Fabric run against the last successful ADF run, or the previous Fabric run."""

from __future__ import annotations

import json

import pytest

from adf2fabric.agent.deployment import verify as V
from adf2fabric.agent.deployment.fabric_deploy import DeployError
from adf2fabric.agent.deployment.fabric_run import ActivityRun, RunResult

COPY = {"rowsRead": 10, "rowsCopied": 10, "filesWritten": 1, "copyDuration": 7, "usedDataIntegrationUnits": 4}


def act(name, kind="Copy", status="Succeeded", **output):
    return ActivityRun(name=name, type=kind, status=status, output=output)


def test_metrics_keep_counts_and_never_keep_data_values():
    assert V.metrics("Copy", COPY) == {"rowsRead": 10, "rowsCopied": 10, "filesWritten": 1}
    m = V.metrics("Lookup", {"count": 1, "firstRow": {"GreenRowCount": 42, "Name": "secret-ish"}})
    assert m["count"] == 1 and m["firstRow.GreenRowCount"] == 42 and "secret-ish" not in json.dumps(m)
    assert V.metrics("Lookup", {"count": 2, "value": [{"a": "x"}, {"a": "y"}]})["count"] == 2
    assert V.metrics("Wait", {"anything": 1}) == {} and V.metrics("Copy", None) == {}


def test_foreach_iterations_are_summed_and_order_does_not_matter():
    a = V.summarize([act("C", rowsRead=3), act("C", rowsRead=4), act("L", "Lookup", firstRow={"v": "x"}),
                     act("L", "Lookup", firstRow={"v": "y"})])
    b = V.summarize([act("C", rowsRead=4), act("C", rowsRead=3), act("L", "Lookup", firstRow={"v": "y"}),
                     act("L", "Lookup", firstRow={"v": "x"})])
    assert a["C"]["metrics"]["rowsRead"] == 7 and a["C"]["runs"] == 2
    assert V.compare(a, b) == ([], 4)


def test_a_failed_iteration_makes_the_activity_failed():
    items = [ActivityRun("C", "Copy", "Succeeded", iteration="i1"), ActivityRun("C", "Copy", "Failed", iteration="i2")]
    assert V.summarize(items)["C"]["status"] == "Failed"


def test_differences_name_the_activity_the_fact_and_both_values():
    diffs, matched = V.compare(V.summarize([act("C", rowsRead=9)]), V.summarize([act("C", rowsRead=10)]))
    assert diffs == [{"activity": "C", "fact": "rowsRead", "baseline": 10, "fabric": 9}] and matched == 1


def test_hash_differences_never_show_the_values():
    diffs, _ = V.compare(V.summarize([act("L", "Lookup", firstRow={"v": "a"})]),
                         V.summarize([act("L", "Lookup", firstRow={"v": "b"})]))
    assert diffs[0]["baseline"] == "(different values)"


def test_factory_is_read_from_an_azure_source_only():
    f = V.Factory.from_source("azure://subscriptions/s1/resourceGroups/rg/providers/Microsoft.DataFactory/factories/f1")
    assert (f.subscription, f.resource_group, f.name) == ("s1", "rg", "f1")
    assert V.Factory.from_source("https://github.com/x/y") is None and V.Factory.from_source(None) is None


def test_short_failure_keeps_what_the_data_source_said():
    raw = ("3121: Operation on target X failed: 'Type=A,Message=Cannot connect to SQL Database.,Source=B,''Type=C,"
           "Message=Cannot connect to database when it is paused.,Source=D,Errors=[{Message=Cannot connect to database "
           "when it is paused.,},],''")
    assert V.short_failure(raw) == "3121: Cannot connect to database when it is paused."
    assert V.short_failure("plain") == "plain" and len(V.short_failure("x" * 999)) == 400


class FakeAdf:
    def __init__(self, runs=None, fail=False):
        self.runs, self.fail = runs, fail

    def last_success(self, pipeline):
        if self.fail:
            raise DeployError("HTTP 403")
        return {"runId": "adf-run", "runEnd": "2026-10-15T00:00:00Z"} if self.runs is not None else None

    def activities(self, run):
        return self.runs


def runner(*acts, status="Completed", job="job-2"):
    return lambda *a, **k: RunResult(pipeline="P", item_id="i", job_id=job, status=status, ended="2026-10-21",
                                     activities=list(acts), failure="" if status == "Completed" else "boom")


def test_matches_against_adf_when_every_activity_lines_up():
    v = V.verify_pipeline(None, "ws", "P", start=True, adf=FakeAdf([act("C", rowsRead=10)]),
                          runner=runner(act("C", rowsRead=10)))
    assert (v.verdict, v.baseline, v.matched) == ("matches", "adf", 2)


def test_partial_when_an_activity_exists_on_one_side_only():
    v = V.verify_pipeline(None, "ws", "P", start=True, adf=FakeAdf([act("C"), act("Old")]),
                          runner=runner(act("C"), act("New")))
    assert v.verdict == "partial" and v.baseline_only == ["Old"] and v.fabric_only == ["New"]


def test_differs_when_counts_differ():
    v = V.verify_pipeline(None, "ws", "P", start=True, adf=FakeAdf([act("C", rowsRead=10)]),
                          runner=runner(act("C", rowsRead=11)))
    assert v.verdict == "differs" and v.differences[0]["fact"] == "rowsRead"


def test_without_adf_history_the_previous_fabric_run_is_the_baseline():
    prev = {"fabric_status": "Completed", "job_id": "job-1", "ran_at": "2026-10-20",
            "activities": V.summarize([act("C", rowsRead=5)])}
    v = V.verify_pipeline(None, "ws", "P", start=True, adf=FakeAdf(None), previous=prev, runner=runner(act("C", rowsRead=5)))
    assert (v.verdict, v.baseline, v.baseline_run) == ("matches", "fabric", "job-1")


def test_the_same_fabric_run_is_never_its_own_baseline_and_a_failed_one_never_is():
    same = {"fabric_status": "Completed", "job_id": "job-2", "activities": {}}
    assert V.verify_pipeline(None, "ws", "P", start=True, previous=same, runner=runner(act("C"))).verdict == "no_baseline"
    failed = {"fabric_status": "Failed", "job_id": "job-1", "activities": V.summarize([act("C")])}
    assert V.verify_pipeline(None, "ws", "P", start=True, previous=failed, runner=runner(act("C"))).verdict == "no_baseline"


def test_a_failed_fabric_run_is_failed_and_unreadable_adf_history_is_said():
    assert V.verify_pipeline(None, "ws", "P", start=True, runner=runner(status="Failed")).verdict == "failed"
    v = V.verify_pipeline(None, "ws", "P", start=True, adf=FakeAdf(fail=True), runner=runner(act("C")))
    assert v.verdict == "no_baseline" and "ADF history not read" in v.detail


def test_read_only_mode_starts_nothing(monkeypatch):
    monkeypatch.setattr(V, "latest_run", lambda *a: None)
    called = []
    v = V.verify_pipeline(None, "ws", "P", start=False, runner=lambda *a, **k: called.append(1))
    assert v.verdict == "not_run" and not called


def test_the_record_keeps_the_last_verified_run_and_skips_not_run(tmp_path):
    path = tmp_path / "v" / "verification.json"
    V.save_record(path, [V.Verification("A", "matches", fabric_status="Completed", job_id="j1")])
    rec = V.save_record(path, [V.Verification("A", "not_run"), V.Verification("B", "failed")])
    assert rec["A"]["job_id"] == "j1" and rec["B"]["verdict"] == "failed"
    assert "| `A` | **matches**" in V.render_report(rec)


def test_the_arm_token_is_never_sent_elsewhere():
    adf = V.AdfRuns(V.Factory("s", "r", "f"), "tok", session=object())
    with pytest.raises(DeployError, match="non-ARM"):
        adf._post("https://evil.example.com/x", {})


def test_the_verify_stage_reads_latest_runs_and_never_starts_one(monkeypatch, tmp_path):
    from adf2fabric.api import stages
    from adf2fabric.api.container import Session
    from adf2fabric.api.demo import build_demo_container

    c = build_demo_container()
    pid = "demo-estate"
    sess = Session(id="s", destination={"provider": "azure_cli", "workspace_id": "11111111-1111-1111-1111-111111111111"})
    for s in ("discovery", "semantic", "convert"):
        stages.run_stage(c, sess, pid, s, lambda *_: None)
    seen = []

    def fake(client, ws, name, *, start, adf=None, previous=None, **kw):
        seen.append((name, start))
        return V.Verification(name, "no_baseline", fabric_status="Completed", job_id="j")

    monkeypatch.setattr(V, "verify_pipeline", fake)
    with pytest.raises(stages.StageError, match="never requests a token"):  # a sign-in problem is a readable stage error
        stages.run_stage(c, sess, pid, "verify", lambda *_: None)
    monkeypatch.setattr(stages, "FabricClient", lambda token: object())
    monkeypatch.setattr(c.azure, "token", lambda **kw: "t")
    out = stages.run_stage(c, sess, pid, "verify", lambda *_: None)
    assert seen and all(start is False for _, start in seen)
    assert {r["pipeline"] for r in out["results"]} == {n for n, _ in seen} and "activities" not in out["results"][0]
    assert (c.store.project_dir(pid) / "verification" / "verification-report.md").exists()


def test_a_saved_conversion_is_served_and_a_stale_one_is_marked():
    from fastapi.testclient import TestClient

    from adf2fabric.api import stages
    from adf2fabric.api.app import create_app
    from adf2fabric.api.container import Session, Settings
    from adf2fabric.api.demo import build_demo_container

    demo = build_demo_container()
    tc = TestClient(create_app(Settings(), live=demo, demo=demo), raise_server_exceptions=False)
    h = {"X-Session-Id": "conv"}
    tc.post("/mode", json={"mode": "demo"}, headers=h)
    assert tc.get("/projects/demo-estate/conversion", headers=h).status_code == 404
    for s in ("discovery", "semantic", "convert"):
        stages.run_stage(demo, Session(id="x"), "demo-estate", s, lambda *_: None)
    got = tc.get("/projects/demo-estate/conversion", headers=h).json()
    assert got["pipelines"] and got["stale"] is False and "preflight" in got
    project = demo.store.load("demo-estate")
    project.repository.commit_sha = "0" * 40
    demo.store.save(project)
    assert tc.get("/projects/demo-estate/conversion", headers=h).json()["stale"] is True


# --- fixes from review --------------------------------------------------------------------------------------------------
def test_a_retry_that_recovered_is_not_a_failure_and_its_failed_attempt_is_not_counted():
    s = V.summarize([act("C", status="Failed", rowsRead=4), act("C", rowsRead=10)])
    assert s["C"]["status"] == "Succeeded" and s["C"]["metrics"]["rowsRead"] == 10
    items = [ActivityRun("C", "Copy", "Failed", iteration="i1"), ActivityRun("C", "Copy", "Succeeded", iteration="i2")]
    assert V.summarize(items)["C"]["status"] == "Failed"  # a different ForEach item that failed is a real failure


def test_one_pipelines_problem_is_its_own_verdict(monkeypatch):
    def boom(*a, **k):
        raise DeployError("No pipeline named 'P' in the workspace.")

    monkeypatch.setattr(V, "latest_run", boom)
    v = V.verify_pipeline(None, "ws", "P", start=False)
    assert v.verdict == "not_run" and "No pipeline named" in v.detail

    def net(*a, **k):
        raise ConnectionError("down")

    assert V.verify_pipeline(None, "ws", "P", start=True, runner=net).verdict == "not_run"


def test_an_adf_activity_read_error_is_a_detail_not_a_crash():
    class Adf(FakeAdf):
        def activities(self, run):
            raise DeployError("ADF returned HTTP 500 for its run history.")

    v = V.verify_pipeline(None, "ws", "P", start=True, adf=Adf([]), runner=runner(act("C")))
    assert v.verdict == "no_baseline" and "HTTP 500" in v.detail


def test_adf_network_and_json_errors_become_readable(monkeypatch):
    from adf2fabric.cloud import ARM_ENDPOINT

    class Down:
        def post(self, *a, **k):
            raise TimeoutError()

    class NotJson:
        status_code = 200

        def post(self, *a, **k):
            return self

        def json(self):
            raise ValueError()

    for s, msg in ((Down(), "could not be reached"), (NotJson(), "not JSON")):
        with pytest.raises(DeployError, match=msg):
            V.AdfRuns(V.Factory("s", "r", "f"), "t", session=s)._post(ARM_ENDPOINT + "/x", {})


def test_a_failed_run_keeps_the_last_completed_one_as_the_next_baseline(tmp_path):
    path = tmp_path / "verification.json"
    good = V.Verification("P", "no_baseline", fabric_status="Completed", job_id="j1", activities=V.summarize([act("C", rowsRead=5)]))
    V.save_record(path, [good])
    rec = V.save_record(path, [V.Verification("P", "failed", fabric_status="Failed", job_id="j2")])
    assert rec["P"]["verdict"] == "failed" and rec["P"]["last_completed"]["job_id"] == "j1"
    v = V.verify_pipeline(None, "ws", "P", start=True, previous=rec["P"], runner=runner(act("C", rowsRead=5), job="j3"))
    assert (v.verdict, v.baseline_run) == ("matches", "j1")


def test_a_weaker_reread_of_the_same_run_does_not_replace_a_real_comparison(tmp_path):
    path = tmp_path / "verification.json"
    V.save_record(path, [V.Verification("P", "matches", fabric_status="Completed", job_id="j1", baseline="adf")])
    rec = V.save_record(path, [V.Verification("P", "no_baseline", fabric_status="Completed", job_id="j1")])
    assert rec["P"]["verdict"] == "matches"


def test_a_time_without_a_zone_is_utc():
    from datetime import timezone

    assert V._parse("2026-10-21T05:19:29.14").tzinfo == timezone.utc
