"""Running a deployed pipeline on demand and reading the verdict (fake Fabric, no network)."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from adf2fabric.agent.deployment import fabric_deploy as fd
from adf2fabric.agent.deployment import fabric_run as fr
from tests.test_fabric_deploy import LIST, WS, Resp, client, pipelines

ITEM = "22222222-2222-2222-2222-222222222222"
JOB = "33333333-3333-3333-3333-333333333333"
START = f"{fd.FABRIC_API}/workspaces/{WS}/items/{ITEM}/jobs/Pipeline/instances"
LOC = f"{fd.FABRIC_API}/workspaces/{WS}/items/{ITEM}/jobs/instances/{JOB}"
ACT = ("POST", f"/workspaces/{WS}/datapipelines/pipelineruns/{JOB}/queryactivityruns")
ACCEPTED = Resp(202, {}, {"Location": LOC, "Retry-After": "7"})
STARTS = ("POST", f"/items/{ITEM}/jobs/Pipeline/instances")
POLL = ("GET", f"/jobs/instances/{JOB}")


def run(script, **kw):
    c, s = client(script)
    kw.setdefault("sleep", lambda _: None)
    return fr.run_pipeline(c, WS, "PL_Validation", **kw), s


def test_a_completed_run_reports_ok_and_per_activity_results():
    r, s = run({LIST: [pipelines((ITEM, "PL_Validation"))], STARTS: [ACCEPTED],
                POLL: [Resp(200, {"status": "InProgress"}),
                       Resp(200, {"status": "Completed", "startTimeUtc": "2026-10-16T10:00:00Z",
                                  "endTimeUtc": "2026-10-16T10:01:00Z", "failureReason": None})],
                ACT: [Resp(200, {"value": [{"activityName": "B", "activityType": "Lookup", "status": "Succeeded"},
                                           {"activityName": "A", "activityType": "Wait", "status": "Succeeded"}]})]})
    assert r.ok and r.job_id == JOB and r.started.endswith("10:00:00Z")
    assert [a.name for a in r.activities] == ["A", "B"]
    post = next(c for c in s.calls if c[0] == "POST" and c[1] == START)
    assert post[2] is None  # no parameters, no body


def test_a_failed_run_carries_the_failure_reason():
    r, _ = run({LIST: [pipelines((ITEM, "PL_Validation"))], STARTS: [ACCEPTED],
                POLL: [Resp(200, {"status": "Failed", "failureReason": {
                    "errorCode": "BadSql", "message": "Incorrect syntax near AS"}})],
                ACT: [Resp(200, {"value": [{"activityName": "Q", "activityType": "Lookup", "status": "Failed",
                                            "error": {"message": "syntax"}}]})]})
    assert not r.ok and r.status == "Failed" and r.failure == "BadSql: Incorrect syntax near AS"
    assert r.activities[0].error == "syntax"


def test_parameters_go_in_execution_data_and_polling_honours_retry_after():
    waits: list[float] = []
    c, s = client({LIST: [pipelines((ITEM, "PL_Validation"))], STARTS: [ACCEPTED],
                   POLL: [Resp(200, {"status": "Completed"})], ACT: [Resp(200, {"value": []})]})
    r = fr.run_pipeline(c, WS, "pl_validation", parameters={"Tables": ["a", "b"]}, sleep=waits.append)
    assert r.ok and waits[0] == 7.0  # the first poll waits the Retry-After from the start response
    post = next(x for x in s.calls if x[0] == "POST" and x[1] == START)
    assert post[2] == {"executionData": {"parameters": {"Tables": ["a", "b"]}}}


def test_no_wait_returns_after_starting_and_never_polls():
    r, s = run({LIST: [pipelines((ITEM, "PL_Validation"))], STARTS: [ACCEPTED]}, wait=False)
    assert r.status == "InProgress" and r.job_id == JOB and "not waited" in r.detail
    assert not any(c[1] == LOC for c in s.calls)


def test_a_run_that_outlasts_the_timeout_says_it_is_still_running():
    ticks = iter(range(0, 10_000, 100))
    r, _ = run({LIST: [pipelines((ITEM, "PL_Validation"))], STARTS: [ACCEPTED],
                POLL: [Resp(200, {"status": "InProgress"})]},
               timeout_seconds=250, clock=lambda: next(ticks))
    assert r.status == "Timeout" and "keeps running" in r.detail


@pytest.mark.parametrize("status", ["Deduped", "Cancelled"])
def test_deduped_and_cancelled_are_terminal_and_not_ok(status):
    r, _ = run({LIST: [pipelines((ITEM, "PL_Validation"))], STARTS: [ACCEPTED],
                POLL: [Resp(200, {"status": status})], ACT: [Resp(200, {"value": []})]})
    assert r.status == status and not r.ok


def test_activity_query_failure_does_not_hide_the_verdict():
    r, _ = run({LIST: [pipelines((ITEM, "PL_Validation"))], STARTS: [ACCEPTED],
                POLL: [Resp(200, {"status": "Completed"})], ACT: [Resp(500, {"errorCode": "x"})]})
    assert r.ok and r.activities == []


def test_unknown_pipeline_bad_workspace_and_refused_start_are_errors():
    c, _ = client({LIST: [pipelines((ITEM, "PL_Validation"))]})
    with pytest.raises(fd.DeployError, match="No pipeline named 'Nope'"):
        fr.run_pipeline(c, WS, "Nope")
    c, _ = client({})
    with pytest.raises(fd.DeployError, match="GUID"):
        fr.run_pipeline(c, "not-a-guid", "PL_Validation")
    with pytest.raises(fd.DeployError, match="HTTP 403"):
        run({LIST: [pipelines((ITEM, "PL_Validation"))],
             STARTS: [Resp(403, {"errorCode": "InsufficientPrivileges", "message": "no"})]})


def test_a_location_on_another_host_never_receives_the_token():
    foreign = "https://wabi-redirect.analysis.windows.net/v1/workspaces/x/items/y/jobs/instances/" + JOB
    r, s = run({LIST: [pipelines((ITEM, "PL_Validation"))], STARTS: [Resp(202, {}, {"Location": foreign})],
                POLL: [Resp(200, {"status": "Completed"})], ACT: [Resp(200, {"value": []})]})
    assert r.ok and r.job_id == JOB
    assert all(call[1].startswith(fd.FABRIC_API + "/") for call in s.calls)  # every request stayed on the Fabric host


def test_a_location_without_a_job_id_is_refused():
    with pytest.raises(fd.DeployError, match="no usable job instance"):
        run({LIST: [pipelines((ITEM, "PL_Validation"))], STARTS: [Resp(202, {}, {"Location": "https://x.example/none"})]})


def test_cli_is_a_dry_run_without_apply(monkeypatch):
    from adf2fabric import cli as cli_mod

    c, s = client({LIST: [pipelines((ITEM, "PL_Validation"))]})
    monkeypatch.setattr(fd, "FabricClient", lambda token: c)

    class Cred:
        def get_token(self, scope):
            return type("T", (), {"token": "TOKEN"})()

    monkeypatch.setattr("adf2fabric.connectivity._azure.build_credential", lambda ref: (Cred(), None))
    out = CliRunner().invoke(cli_mod.cli, ["run", "--workspace-id", WS, "--pipeline", "PL_Validation", "--param", "n=3"])
    assert out.exit_code == 0, out.output
    assert "dry run" in out.output and "would run with n" in out.output
    assert all(call[0] == "GET" for call in s.calls)
    bad = CliRunner().invoke(cli_mod.cli, ["run", "--workspace-id", WS, "--pipeline", "X", "--param", "oops"])
    assert bad.exit_code != 0 and "name=value" in bad.output
