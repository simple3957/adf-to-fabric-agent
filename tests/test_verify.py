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


def runner(*acts, status="Completed", job="job-2"):
    return lambda *a, **k: RunResult(pipeline="P", item_id="i", job_id=job, status=status, ended="2026-10-21",
                                     activities=list(acts), failure="" if status == "Completed" else "boom")


def test_the_same_fabric_run_is_never_its_own_baseline_and_a_failed_one_never_is():
    same = {"fabric_status": "Completed", "job_id": "job-2", "activities": {}}
    assert V.verify_pipeline(None, "ws", "P", start=True, previous=same, runner=runner(act("C"))).verdict == "no_baseline"
    failed = {"fabric_status": "Failed", "job_id": "job-1", "activities": V.summarize([act("C")])}
    assert V.verify_pipeline(None, "ws", "P", start=True, previous=failed, runner=runner(act("C"))).verdict == "no_baseline"


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


# --- fixes from review --------------------------------------------------------------------------------------------------
def test_a_retry_that_recovered_is_not_a_failure_and_its_failed_attempt_is_not_counted():
    s = V.summarize([act("C", status="Failed", rowsRead=4), act("C", rowsRead=10)])
    assert s["C"]["status"] == "Succeeded" and s["C"]["metrics"]["rowsRead"] == 10
    items = [ActivityRun("C", "Copy", "Failed", iteration="i1"), ActivityRun("C", "Copy", "Succeeded", iteration="i2")]
    assert V.summarize(items)["C"]["status"] == "Failed"  # a different ForEach item that failed is a real failure


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


def test_the_handover_gives_a_reproduced_failure_its_own_level():
    from adf2fabric.agent import handover

    assert handover._FROM_VERDICT["same_failure"] == "source_also_fails" and "source_also_fails" in handover._RANK
    assert handover._RANK["run_failed"] < handover._RANK["source_also_fails"] < handover._RANK["differs"]  # still needs a person, ahead of "results differ"
