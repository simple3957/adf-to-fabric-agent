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
    assert V.compare(a, b) == ([], 6)  # per activity: runs, status, and each fact


def test_a_failed_iteration_makes_the_activity_failed():
    items = [ActivityRun("C", "Copy", "Succeeded", iteration="i1"), ActivityRun("C", "Copy", "Failed", iteration="i2")]
    assert V.summarize(items)["C"]["status"] == "Failed"


def test_differences_name_the_activity_the_fact_and_both_values():
    diffs, matched = V.compare(V.summarize([act("C", rowsRead=9)]), V.summarize([act("C", rowsRead=10)]))
    assert diffs == [{"activity": "C", "fact": "rowsRead", "baseline": 10, "fabric": 9}] and matched == 2


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
    assert (v.verdict, v.baseline, v.matched) == ("matches", "adf", 3)


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


def test_a_run_is_pinned_only_to_a_file_saved_before_it(tmp_path):
    import os

    project_dir = tmp_path / "p"
    path = V.record_path(project_dir)
    for name in ("Old", "New"):
        f = V.artifact_file(project_dir, name)
        f.parent.mkdir(parents=True)
        f.write_text(json.dumps({"name": name}), encoding="utf-8")
    os.utime(V.artifact_file(project_dir, "New"), (4102444800, 4102444800))  # saved in 2100: after the run
    rec = V.save_record(path, [V.Verification(n, "matches", fabric_status="Completed", job_id=n, ran_at="2050-01-01T00:00:00Z") for n in ("Old", "New")])
    assert rec["Old"]["artifact_sha"] == V.file_sha(V.artifact_file(project_dir, "Old"))
    assert "artifact_sha" not in rec["New"]  # the run did not see this file: the save time decides, as before
    # a record written before pinning existed gets its pin on the next save, by the same rule
    raw = json.loads(path.read_text(encoding="utf-8"))
    del raw["Old"]["artifact_sha"]
    path.write_text(json.dumps(raw), encoding="utf-8")
    assert V.save_record(path, [])["Old"]["artifact_sha"] == V.file_sha(V.artifact_file(project_dir, "Old"))


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
    c.fabric_http = None  # without the demo stand-in the token comes from Azure, as in a real run
    with pytest.raises(stages.StageError, match="never requests a token"):  # a sign-in problem is a readable stage error
        stages.run_stage(c, sess, pid, "verify", lambda *_: None)
    monkeypatch.setattr(stages, "FabricClient", lambda token, **kw: object())
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
    tc = TestClient(create_app(Settings(), live=demo, demo=demo), base_url="http://localhost", raise_server_exceptions=False)
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


# --- a pipeline another one calls is compared as a batch ------------------------------------------------------------
def _child_runs(*rows):
    return [RunResult(pipeline="C", item_id="i", job_id=f"j{i}", status="Completed", started="2026-10-21T06:13:4{i}",
                      activities=[act("Copy", rowsCopied=n)]) for i, n in enumerate(rows)]


class BatchAdf(FakeAdf):
    def __init__(self, *rows):
        self.rows = rows

    def last_success(self, pipeline):
        return {"runId": "r0", "runEnd": "2026-10-21T06:11:00Z", "runStart": "2026-10-21T06:10:00Z", "pipelineName": pipeline,
                "invokedBy": {"invokedByType": "PipelineActivity", "pipelineRunId": "parent-1"}}

    def successes_near(self, run):
        return [{"runId": f"r{i}"} for i in range(len(self.rows))]

    def activities(self, run):
        return [act("Copy", rowsCopied=self.rows[int(run["runId"][1:])])]


def test_a_child_batch_matches_whatever_order_its_items_ran_in(monkeypatch):
    monkeypatch.setattr(V, "runs_started_by", lambda *a, **k: None)  # no parent run names it: start times
    monkeypatch.setattr(V, "recent_runs", lambda *a, **k: _child_runs(10, 4, 5))
    v = V.verify_pipeline(None, "ws", "C", start=True, adf=BatchAdf(4, 5, 10), called_by=["Parent"])
    assert v.verdict == "matches" and "batch of 3" in v.detail and "Parent" in v.detail


def test_a_child_batch_with_a_missing_item_differs_on_runs_and_rows(monkeypatch):
    monkeypatch.setattr(V, "runs_started_by", lambda *a, **k: None)  # no parent run names it: start times
    monkeypatch.setattr(V, "recent_runs", lambda *a, **k: _child_runs(10, 4))
    v = V.verify_pipeline(None, "ws", "C", start=False, adf=BatchAdf(4, 5, 10), called_by=["Parent"])
    assert v.verdict == "differs" and {d["fact"] for d in v.differences} == {"runs", "rowsCopied"}


def test_a_child_is_never_started_on_its_own(monkeypatch):
    monkeypatch.setattr(V, "runs_started_by", lambda *a, **k: None)  # no parent run names it: start times
    monkeypatch.setattr(V, "recent_runs", lambda *a, **k: [])
    started = []
    v = V.verify_pipeline(None, "ws", "C", start=True, called_by=["Parent"], runner=lambda *a, **k: started.append(1))
    assert v.verdict == "not_run" and "Parent" in v.detail and not started


def test_a_child_batch_is_the_runs_its_parents_latest_run_started(monkeypatch):
    seen = []
    monkeypatch.setattr(V, "runs_started_by", lambda c, w, n, parents, ids: (seen.append(parents), ("Parent", _child_runs(10, 4, 5)))[1])
    monkeypatch.setattr(V, "recent_runs", lambda *a, **k: pytest.fail("start times are only a fallback"))
    v = V.verify_pipeline(None, "ws", "C", start=False, adf=BatchAdf(4, 5, 10), called_by=["Parent"])
    assert v.verdict == "matches" and "3 run(s) that the latest run of Parent started" in v.detail and seen == [["Parent"]]


def _invoke(job_id, item="child-id", status="Succeeded", iteration="", job_status="Completed"):
    return ActivityRun(name="Run_Child", type="InvokePipeline", status=status, iteration=iteration,
                       output={"id": job_id, "itemId": item, "status": job_status, "startTimeUtc": f"2026-10-27T05:42:2{job_id[-1]}Z"})


def test_runs_started_by_takes_the_child_jobs_named_in_the_parents_invoke_outputs(monkeypatch):
    """Live case 2026-10-27: a child started by hand without parameters (Null fileName, Failed) two minutes before the
    parent's run was taken into the start-time batch. The parent's InvokePipeline outputs name exactly its three runs."""
    from adf2fabric.agent.deployment import fabric_run as FR

    g = lambda n: f"00000000-0000-0000-0000-00000000000{n}"  # noqa: E731
    parent_runs = {"parent": RunResult("Parent", "parent-id", job_id="p", status="Completed", started="2026-10-27T05:41:58",
                                       activities=[_invoke(g(1)), _invoke(g(2)), _invoke(g(3)),
                                                   _invoke(g(4), status="Failed", iteration="x", job_status="Failed"),
                                                   _invoke(g(5), iteration="x"),  # a retry that recovered: its failed attempt is left out
                                                   _invoke(g(6), item="other-id"), _invoke("not-a-guid")]),
                   "old": RunResult("Old", "old-id", job_id="o", status="Completed", started="2026-10-20T00:00:00",
                                    activities=[_invoke(g(7))])}
    monkeypatch.setattr(FR, "latest_run", lambda c, w, name, ids=None: parent_runs.get(name.lower()))
    monkeypatch.setattr(FR, "_query_activities", lambda c, w, job, started: [act("Copy", rowsCopied=1)])
    ids = {"child": "child-id", "parent": "parent-id", "old": "old-id"}
    parent, runs = FR.runs_started_by(None, "ws", "Child", ["Old", "Parent", "Unknown"], ids)
    assert parent == "Parent" and sorted(r.job_id for r in runs) == [g(1), g(2), g(3), g(5)]
    assert all(r.ok and r.item_id == "child-id" for r in runs)
    monkeypatch.setattr(FR, "latest_run", lambda *a, **k: None)
    assert FR.runs_started_by(None, "ws", "Child", ["Parent"], ids) is None  # no parent run: the caller falls back
    with pytest.raises(DeployError):
        FR.runs_started_by(None, "ws", "Missing", ["Parent"], ids)


def test_callers_are_read_from_the_converted_json(tmp_path):
    class P:
        def __init__(self, name, path):
            self.source_pipeline, self.artifact_path = name, path

    (tmp_path / "a.json").write_text(json.dumps({"x": "<item:pipeline:Child>"}), encoding="utf-8")
    (tmp_path / "b.json").write_text("{}", encoding="utf-8")
    assert V.callers(tmp_path, [P("Parent", "a.json"), P("Child", "b.json"), P("Gone", None)]) == {"Child": ["Parent"]}


def test_a_child_whose_id_is_bound_is_still_known_to_be_called(tmp_path):
    """Binding a child's id removes its <item:pipeline:..> placeholder; its caller must still be found (found live: the child was
    then compared run for run and a loop's other files were paired with the wrong baseline)."""
    class P:
        def __init__(self, name, path):
            self.source_pipeline, self.artifact_path = name, path

    cid = "ef6e47da-327e-4cc2-b2d5-c1a19777a3d7"
    nested = {"properties": {"activities": [{"type": "ForEach", "typeProperties": {"activities": [
        {"type": "InvokePipeline", "typeProperties": {"pipelineId": cid.upper()}}]}}]}}
    (tmp_path / "a.json").write_text(json.dumps(nested), encoding="utf-8")
    (tmp_path / "b.json").write_text("{}", encoding="utf-8")
    pipes = [P("Parent", "a.json"), P("Child", "b.json")]
    assert V.callers(tmp_path, pipes) == {}  # nothing says which pipeline that id is
    (tmp_path / "resolutions.json").write_text(json.dumps([{"type": "ItemToId", "key": "pipeline:Child", "value": cid},
                                                           {"type": "WorkspaceId", "key": "*", "value": "w"}]), encoding="utf-8")
    assert V.callers(tmp_path, pipes) == {"Child": ["Parent"]}
    (tmp_path / "a.json").write_text(json.dumps({"x": "<item:pipeline:Child>", **nested}), encoding="utf-8")
    assert V.callers(tmp_path, pipes) == {"Child": ["Parent"]}  # placeholder and bound id together: listed once


def test_a_child_bound_to_an_item_id_is_still_known_as_called(tmp_path):
    """--resolutions binds the child, so the JSON holds its id, not the placeholder: the prerequisite still names it.
    Without this the child (run once per file by its parent) was compared one run with one run, of different files."""
    from adf2fabric.project.conversion import Prerequisite

    class P:
        def __init__(self, name, path, prereqs=()):
            self.source_pipeline, self.artifact_path, self.prerequisites = name, path, list(prereqs)

    (tmp_path / "a.json").write_text(json.dumps({"pipelineId": "ef6e47da-0000-0000-0000-000000000000"}), encoding="utf-8")
    (tmp_path / "b.json").write_text(json.dumps({"x": "<item:pipeline:Child>"}), encoding="utf-8")
    (tmp_path / "c.json").write_text("{}", encoding="utf-8")
    bound = Prerequisite(kind="workspace_item", name="pipeline:Child", detail="", bound=True, needed_by=["Run_Child"])
    same = Prerequisite(kind="workspace_item", name="pipeline:child", detail="", bound=False, needed_by=["Run_Child"])
    other = Prerequisite(kind="workspace_item", name="workspace", detail="", bound=True, needed_by=["Run_Child"])
    got = V.callers(tmp_path, [P("Parent", "a.json", [bound, other]), P("Other", "b.json", [same]), P("Child", "c.json")])
    assert got == {"Child": ["Parent", "Other"]}  # counted once per caller, placeholder and prerequisite alike


def test_a_called_pipeline_last_run_by_hand_in_adf_is_compared_run_for_run(monkeypatch):
    seen = {}

    def runs(*a, window_minutes=10, **k):
        seen["window"] = window_minutes
        return _child_runs(10)

    class ByHand(BatchAdf):
        def last_success(self, pipeline):
            return {"runId": "r0", "runEnd": "x", "invokedBy": {"invokedByType": "Manual"}}

        def successes_near(self, run):
            return [run]

    monkeypatch.setattr(V, "recent_runs", runs)
    v = V.verify_pipeline(None, "ws", "C", start=False, adf=ByHand(10), called_by=["Parent"])
    assert v.verdict == "matches" and seen["window"] == 0 and "started by hand" in v.detail


def test_adf_groups_a_batch_by_its_parent_run():
    class S:
        def post(self, url, **kw):
            class R:
                status_code = 200

                def json(self):
                    return {"value": [{"runId": "a", "runStart": "2026-10-21T06:10:00Z", "invokedBy": {"pipelineRunId": "p1"}},
                                      {"runId": "b", "runStart": "2026-10-21T06:10:05Z", "invokedBy": {"pipelineRunId": "p1"}},
                                      {"runId": "c", "runStart": "2026-10-21T06:10:06Z", "invokedBy": {"pipelineRunId": "p2"}}]}
            return R()

    from adf2fabric.cloud import ARM_ENDPOINT  # noqa: F401 - the base URL is built from it
    adf = V.AdfRuns(V.Factory("s", "r", "f"), "t", session=S())
    run = {"runId": "a", "runStart": "2026-10-21T06:10:00Z", "pipelineName": "C",
           "invokedBy": {"invokedByType": "PipelineActivity", "pipelineRunId": "p1"}}
    assert [r["runId"] for r in adf.successes_near(run)] == ["a", "b"]
    assert adf.successes_near({"runId": "m", "invokedBy": {"invokedByType": "Manual"}}) == [{"runId": "m", "invokedBy": {"invokedByType": "Manual"}}]


# ---- a failure ADF reproduces -------------------------------------------------------------------------------------------------


def test_failure_signatures_ignore_codes_ids_and_spacing():
    # the wording of the real failures: Fabric prefixes the Fail activity's code and may carry request ids; ADF does neither
    fabric = "ROWS_LOST: Operation on target Fail_Row_Count_Mismatch failed: Rows lost converting Big_test.csv"
    adf = "Operation on target Fail_Row_Count_Mismatch failed: Rows  lost converting Big_test.csv"
    assert V.failure_signature(fabric) == V.failure_signature(adf) == ("fail_row_count_mismatch", "rows lost converting big_test.csv")
    a = V.failure_signature("Operation on target X failed: bad id 6c1485aa-525f-490f-b2a3-737d4421fd4d")
    b = V.failure_signature("Operation on target X failed: bad id 11111111-2222-3333-4444-555555555555")
    assert a == b
    assert V.failure_signature("something else")[0] == ""  # no recognisable failing activity: nothing to compare


class FailingAdf(FakeAdf):
    """ADF's history holds a Failed run of the pipeline near the Fabric run."""

    def __init__(self, message):
        super().__init__(None)
        self.message = message

    def failure_near(self, pipeline, around):
        return {"runId": "adf-failed", "runStart": "2026-10-30T07:25:47Z", "runEnd": "2026-10-30T07:26:10Z", "message": self.message} if self.message else None


def failing_runner(text):
    return lambda *a, **k: RunResult(pipeline="P", item_id="i", job_id="job-9", status="Failed", started="2026-10-30T07:25:45", ended="2026-10-30T07:26:10",
                                     activities=[], failure=text)


def test_a_fabric_failure_that_adf_reproduces_is_the_same_failure_not_a_defect():
    text = "Operation on target Fail_Too_Few_Rows failed: A Parquet file has fewer than 1000 rows"
    v = V.verify_pipeline(None, "ws", "P", start=True, adf=FailingAdf(text), runner=failing_runner("TOO_FEW_ROWS: " + text))
    assert v.verdict == "same_failure" and v.baseline == "adf" and v.baseline_run == "adf-failed"
    assert "not the conversion" in v.detail and "fewer than 1000 rows" in v.detail


def test_a_failure_adf_does_not_reproduce_stays_a_failure():
    text = "Operation on target Fail_Too_Few_Rows failed: A Parquet file has fewer than 1000 rows"
    assert V.verify_pipeline(None, "ws", "P", start=True, adf=FailingAdf(None), runner=failing_runner(text)).verdict == "failed"  # ADF did not fail
    other = V.verify_pipeline(None, "ws", "P", start=True, adf=FailingAdf("Operation on target Fail_Too_Few_Rows failed: a different problem"), runner=failing_runner(text))
    assert other.verdict == "failed" and "different message" in other.detail  # same activity, another cause: said, not hidden
    elsewhere = V.verify_pipeline(None, "ws", "P", start=True, adf=FailingAdf("Operation on target Some_Other failed: A Parquet file has fewer than 1000 rows"),
                                  runner=failing_runner(text))
    assert elsewhere.verdict == "failed"  # a different activity failed in ADF


def test_a_failure_with_unreadable_adf_history_stays_a_failure():
    class Broken(FailingAdf):
        def failure_near(self, pipeline, around):
            raise DeployError("HTTP 403")

    assert V.verify_pipeline(None, "ws", "P", start=True, adf=Broken("x"), runner=failing_runner("Operation on target T failed: m")).verdict == "failed"


def test_the_handover_gives_a_reproduced_failure_its_own_level():
    from adf2fabric.agent import handover

    assert handover._FROM_VERDICT["same_failure"] == "source_also_fails" and "source_also_fails" in handover._RANK
    assert handover._RANK["run_failed"] < handover._RANK["source_also_fails"] < handover._RANK["differs"]  # still needs a person, ahead of "results differ"
