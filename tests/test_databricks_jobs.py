"""ADF DatabricksJob activities: read the job (only when authorised), its notebook tasks become workloads, and a job whose
notebooks are deployed and bound converts to Fabric Notebook activities wired as the job's tasks.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from adf2fabric.agent.conversion.agent import build_project_context
from adf2fabric.agent.conversion.context import Resolutions
from adf2fabric.agent.conversion.engine import ConversionEngine
from adf2fabric.agent.discovery.databricks_notebooks import DatabricksClient
from adf2fabric.agent.workloads import databricks_jobs as J
from adf2fabric.agent.workloads.source import SourceResolver
from adf2fabric.project.conversion import ConversionStatus as S, ConversionStrategy as St
from tests.conversion_support import act, by_name, codes, dep, expr, find, linked_service, pipe, ref, semantic

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
HOST = "adb-7.7.azuredatabricks.net"


def _task(key, path=None, deps=(), run_if=None, kind="notebook", **extra):
    t = {"task_key": key, "depends_on": [{"task_key": d} for d in deps]}
    if kind == "notebook":
        t["notebook_task"] = {"notebook_path": path or f"/Workspace/Shared/x/{key}", "source": "WORKSPACE", **extra.pop("nb", {})}
    else:
        t[f"{kind}_task"] = {"query": {"query_id": "q"}}
    if run_if:
        t["run_if"] = run_if
    t.update(extra)
    return t


def _job(job_id, *tasks, **settings):
    return {"job_id": int(job_id), "creator_user_name": "someone@example.com",
            "settings": {"name": settings.pop("name", f"Job {job_id}"), "tasks": list(tasks),
                         "job_clusters": [{"job_cluster_key": "c", "new_cluster": {"spark_conf": {"fs.azure.account.key.x": "SECRET-VALUE"}}}],
                         **settings}}


# =====================================================================================================================
# 1. what is kept of a job, and what Fabric can run
# =====================================================================================================================


def test_only_the_structure_is_kept_never_values_or_cluster_settings():
    raw = _job(1, _task("a", nb={"base_parameters": {"day": "2024-01-01"}},
                        libraries=[{"pypi": {"package": "pandas==2.0", "repo": "https://user:pw@pypi.example"}}, {"whl": "dbfs:/x.whl"}]),
               parameters=[{"name": "env", "default": "prod-secret"}])
    s = J.structure(raw)
    text = json.dumps(s)
    for leaked in ("2024-01-01", "prod-secret", "SECRET-VALUE", "user:pw", "someone@example.com", "job_clusters"):
        assert leaked not in text
    t = s["tasks"][0]
    assert t["parameter_names"] == ["day"] and s["parameter_names"] == ["env"]
    assert t["libraries"] == [{"pypi": {"package": "pandas==2.0"}}, {"whl": "dbfs:/x.whl"}]
    assert t["notebook_path"] == "/Shared/x/a"  # /Workspace/Shared/x/a and /Shared/x/a are one notebook


@pytest.mark.parametrize("raw,expected", [({"jobId": "123"}, "123"), ({"jobId": 123}, "123"), ({"jobId": "@pipeline().parameters.j"}, None),
                                          ({"jobId": {"value": "@x", "type": "Expression"}}, None), ({}, None), ({"jobId": True}, None)])
def test_only_a_literal_job_id_is_taken(raw, expected):
    assert J.job_id_of(raw) == expected


def test_a_job_of_workspace_notebook_tasks_runs_in_dependency_order():
    s = J.structure(_job(1, _task("load", deps=["extract"]), _task("extract"), _task("report", deps=["load"], run_if="ALL_DONE"), _task("side")))
    shape = J.fabric_shape(s)
    assert shape.reasons == []
    assert [t["key"] for t in shape.tasks] == ["extract", "side", "load", "report"]
    assert J.terminal_tasks(shape) == ["side", "report"]


@pytest.mark.parametrize("task,reason", [
    (_task("q", kind="sql"), "task 'q' is a sql task"),
    (_task("g", nb={"source": "GIT"}), "from a Git repository"),
    (_task("p", nb={"base_parameters": {"d": "1"}}), "passes base parameters (d)"),
    (_task("l", libraries=[{"pypi": {"package": "x"}}]), "installs libraries"),
    (_task("r", run_if="AT_LEAST_ONE_SUCCESS"), "AT_LEAST_ONE_SUCCESS"),
    (_task("u", deps=["nowhere"]), "depends on 'nowhere'"),
])
def test_every_part_fabric_cannot_run_is_a_reason_never_a_guess(task, reason):
    shape = J.fabric_shape(J.structure(_job(1, task)))
    assert any(reason in r for r in shape.reasons), shape.reasons


def test_job_parameters_cycles_and_activity_parameters_are_reasons():
    assert "job parameters" in J.fabric_shape(J.structure(_job(1, _task("a"), parameters=[{"name": "p", "default": "1"}]))).reasons[0]
    assert "cycle" in " ".join(J.fabric_shape(J.structure(_job(1, _task("a", deps=["b"]), _task("b", deps=["a"])))).reasons)
    assert "activity passes job parameters" in J.fabric_shape(J.structure(_job(1, _task("a"))), activity_passes_parameters=True).reasons[0]
    assert J.fabric_shape({"tasks": []}).reasons == ["the job has no tasks"]


# =====================================================================================================================
# 2. reading the job: the client, and only as authorised
# =====================================================================================================================


# =====================================================================================================================
# 3. workloads: each notebook task is a workload; a job Fabric cannot run is one entry with why
# =====================================================================================================================


def _job_act(name, job_id, **kw):
    tp = {"jobId": job_id, **kw}
    return {"name": name, "type": "DatabricksJob", "linkedServiceName": {"referenceName": "LS_Job", "type": "LinkedServiceReference"},
            "typeProperties": tp}


ESTATE = {
    "linkedService/LS_Job.json": {"name": "LS_Job", "properties": {"type": "AzureDatabricks", "typeProperties": {
        "domain": f"https://{HOST}/?o=7&l=en"}}},
    "pipeline/PL_Spark.json": {"name": "PL_Spark", "properties": {"activities": [
        _job_act("Generate", "101"),
        {**_job_act("Transform", "102"), "dependsOn": [{"activity": "Generate", "dependencyConditions": ["Succeeded"]}]},
        _job_act("Mixed", "103"), _job_act("Unreadable", "104"), _job_act("Dynamic", {"value": "@pipeline().parameters.j", "type": "Expression"})]}},
}
NB = "# Databricks notebook source\ndf = spark.read.parquet('abfss://c@acct.dfs.core.windows.net/in')\ndf.count()\n"


# =====================================================================================================================
# 4. conversion: bound notebook tasks -> Fabric Notebook activities wired as the job's tasks
# =====================================================================================================================


WS = "53d67e72-8124-4ca3-ae55-c4b266935faa"
IDS = {n: f"00000000-0000-0000-0000-00000000000{i}" for i, n in enumerate(("NB_A", "NB_B", "NB_C", "NB_D"), 1)}


def _bound(*names):
    return [{"type": "ItemToId", "key": f"notebook:{n}", "value": IDS[n]} for n in names] + [{"type": "WorkspaceId", "key": "*", "value": WS}]


def _convert(pipeline, jobs, resolutions):
    d = semantic([pipeline], [], [linked_service("LS_Job", "AzureDatabricks", {"domain": f"https://{HOST}"})])
    ctx = build_project_context(d, Resolutions(resolutions))
    ctx.databricks_jobs = {k: J.structure(v) for k, v in jobs.items()}
    rec, content = ConversionEngine(ctx).convert_pipeline(d.pipelines[0])
    return rec, content


def _jact(name, job_id="7", **kw):
    return act(name, "DatabricksJob", {"jobId": job_id, **kw.pop("tp", {})}, linkedServiceName=ref("LinkedService", "LS_Job"), **kw)


def test_a_single_notebook_job_becomes_one_fabric_notebook_activity_with_the_same_name():
    p = pipe(act("Before", "Wait", {"waitTimeInSeconds": 1}), _jact("Job", dependsOn=[dep("Before")]),
             act("After", "Wait", {"waitTimeInSeconds": 1}, dependsOn=[dep("Job")]))
    rec, content = _convert(p, {"7": _job(7, _task("only", "/Workspace/Shared/NB_A"))}, _bound("NB_A"))
    job = find(content, "Job")
    assert job["type"] == "TridentNotebook" and job["typeProperties"] == {"notebookId": IDS["NB_A"], "workspaceId": WS}
    assert job["dependsOn"] == [{"activity": "Before", "dependencyConditions": ["Succeeded"]}]
    assert find(content, "After")["dependsOn"] == [{"activity": "Job", "dependencyConditions": ["Succeeded"]}]
    a = by_name(rec)["Job"]
    assert a.status is not S.blocked and a.runtime_disposition == "replace" and a.strategy is St.alternative
    assert "Fail" not in json.dumps(content)


def test_a_multi_task_job_is_wired_as_the_job_wires_its_tasks():
    job = _job(7, _task("a", "/Shared/NB_A"), _task("b", "/Shared/NB_B", deps=["a"]),
               _task("c", "/Shared/NB_C", deps=["a", "b"], run_if="ALL_DONE"), _task("d", "/Shared/NB_D"))
    p = pipe(act("Before", "Wait", {"waitTimeInSeconds": 1}), _jact("Job", dependsOn=[dep("Before")]),
             act("After", "Wait", {"waitTimeInSeconds": 1}, dependsOn=[dep("Job")]))
    rec, content = _convert(p, {"7": job}, _bound("NB_A", "NB_B", "NB_C", "NB_D"))
    a, b, c, d = (find(content, f"Job_{k}") for k in "abcd")
    assert all(x["type"] == "TridentNotebook" for x in (a, b, c, d))
    assert a["dependsOn"] == d["dependsOn"] == [{"activity": "Before", "dependencyConditions": ["Succeeded"]}]  # the job's start
    assert b["dependsOn"] == [{"activity": "Job_a", "dependencyConditions": ["Succeeded"]}]
    assert c["dependsOn"] == [{"activity": "Job_a", "dependencyConditions": ["Completed"]},
                              {"activity": "Job_b", "dependencyConditions": ["Completed"]}]  # ALL_DONE
    after = {x["activity"] for x in find(content, "After")["dependsOn"]}
    assert after == {"Job_c", "Job_d"}  # what waited for the job waits for the tasks that end it
    assert by_name(rec)["Job"].strategy is St.composite


@pytest.mark.parametrize("jobs,bound,needle", [
    ({}, ("NB_A",), "has not been read (workloads-assess"),
    ({"7": _job(7, _task("only", "/Shared/NB_A"))}, (), "not deployed and bound yet"),
    ({"7": _job(7, _task("only", "/Shared/NB_A"), _task("q", kind="sql"))}, ("NB_A",), "task 'q' is a sql task"),
    ({"7": _job(7, _task("only", "/Shared/NB_A", nb={"base_parameters": {"d": "1"}}))}, ("NB_A",), "passes base parameters"),
])
def test_anything_short_of_that_stays_a_fail_placeholder_with_the_reason(jobs, bound, needle):
    rec, content = _convert(pipe(_jact("Job")), jobs, _bound(*bound))
    a = by_name(rec)["Job"]
    assert find(content, "Job")["type"] == "Fail" and a.status is S.blocked and "databricks_job" in codes(a)
    assert any(needle in (w.message or "") for w in a.warnings), [w.message for w in a.warnings]


def test_job_parameters_on_the_activity_keep_the_placeholder():
    rec, content = _convert(pipe(_jact("Job", tp={"jobParameters": {"day": "1"}})), {"7": _job(7, _task("o", "/Shared/NB_A"))}, _bound("NB_A"))
    assert find(content, "Job")["type"] == "Fail"


def test_a_downstream_read_of_the_job_output_is_a_manual_step():
    p = pipe(_jact("Job"), act("Use", "SetVariable", {"variableName": "v", "value": expr("@activity('Job').output.runPageUrl")},
                               dependsOn=[dep("Job")]), variables={"v": "String"})
    rec, _ = _convert(p, {"7": _job(7, _task("o", "/Shared/NB_A"))}, _bound("NB_A"))
    assert "databricks_job_output" in codes(by_name(rec)["Job"])


def test_a_task_activity_name_that_is_already_taken_blocks_the_job_never_renames_it():
    job = _job(7, _task("a", "/Shared/NB_A"), _task("b", "/Shared/NB_B", deps=["a"]))
    p = pipe(_jact("Job"), act("Job_b", "Wait", {"waitTimeInSeconds": 1}))
    rec, content = _convert(p, {"7": job}, _bound("NB_A", "NB_B"))
    assert find(content, "Job")["type"] == "Fail" and find(content, "Job_b")["type"] == "Wait"
    assert any("already used in the pipeline" in w.message for w in by_name(rec)["Job"].warnings)


def test_the_real_client_is_given_a_url_for_the_bare_host_the_linked_service_resolves_to(tmp_path):
    # found by the first live run: the host reached the client without https:// and every real read was refused as a bad URL
    client = SourceResolver(tmp_path)._client(HOST, "tok")
    assert isinstance(client, DatabricksClient) and client.host == f"https://{HOST}"


