"""ADF DatabricksJob activities: read the job (only when authorised), its notebook tasks become workloads, and a job whose
notebooks are deployed and bound converts to Fabric Notebook activities wired as the job's tasks. Offline, fakes only."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from adf2fabric.agent.conversion.agent import build_project_context
from adf2fabric.agent.conversion.context import Resolutions
from adf2fabric.agent.conversion.compute_plan import build_compute_plan
from adf2fabric.agent.conversion.engine import ConversionEngine
from adf2fabric.agent.dependencies.access import TokenSource
from adf2fabric.agent.discovery.databricks_notebooks import DatabricksClient, DatabricksError
from adf2fabric.agent.workloads import databricks_jobs as J
from adf2fabric.agent.workloads.engine import Options, build_manifest
from adf2fabric.agent.workloads.source import SourceResolver
from adf2fabric.project.conversion import ConversionStatus as S, ConversionStrategy as St
from adf2fabric.project.workloads import Classification as K, CodeStatus as C, WorkloadKind as W
from tests.conversion_support import act, by_name, codes, dep, expr, find, linked_service, pipe, ref, semantic
from tests.test_discovery_activity_inventory import _discover, _project

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


class _Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body, self.headers = status, body or {}, {}
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


class _Session:
    def __init__(self, pages):
        self.pages, self.urls = list(pages), []

    def request(self, method, url, headers=None, timeout=None):
        self.urls.append(url)
        return self.pages.pop(0)


def test_the_client_reads_every_page_of_tasks():
    s = _Session([_Resp(200, {"job_id": 5, "settings": {"name": "J", "tasks": [_task("a")]}, "next_page_token": "t2"}),
                  _Resp(200, {"job_id": 5, "settings": {"tasks": [_task("b")]}})])
    job = DatabricksClient(f"https://{HOST}", "tok", session=s).get_job("5")
    assert [t["task_key"] for t in job["settings"]["tasks"]] == ["a", "b"]
    assert "/api/2.2/jobs/get?job_id=5" in s.urls[0] and "page_token=t2" in s.urls[1]


@pytest.mark.parametrize("resp,code", [(_Resp(404), "not_found"), (_Resp(400, {"message": "Job 5 does not exist."}), "not_found"),
                                       (_Resp(403), "forbidden"), (_Resp(401), "unauthorized")])
def test_client_failures_are_named(resp, code):
    with pytest.raises(DatabricksError) as e:
        DatabricksClient(f"https://{HOST}", "tok", session=_Session([resp])).get_job("5")
    assert e.value.code == code


class FakeDbx:
    calls: list = []
    jobs: dict = {}
    notebooks: dict = {}

    def __init__(self, host, token):
        self.host, self.token = host, token

    def get_job(self, job_id):
        FakeDbx.calls.append(("job", self.host, job_id))
        if job_id not in FakeDbx.jobs:
            raise DatabricksError("forbidden", f"The token is not allowed to read 'job {job_id}' (HTTP 403).")
        return FakeDbx.jobs[job_id]

    def export_source(self, path):
        FakeDbx.calls.append(("export", self.host, path))
        return FakeDbx.notebooks[path].encode("utf-8"), "PYTHON"


def _resolver(tmp_path, fetch=True):
    FakeDbx.calls = []
    return SourceResolver(tmp_path, TokenSource(env_names={"databricks": "JOB_TOKEN"}), fetch=fetch, client_factory=FakeDbx)


def test_a_job_is_not_read_without_being_asked_or_without_a_token(tmp_path, monkeypatch):
    FakeDbx.jobs = {"5": _job(5, _task("a"))}
    monkeypatch.setenv("JOB_TOKEN", "tok")
    struct, src = _resolver(tmp_path, fetch=False).databricks_job(HOST, "5")
    assert struct is None and src.status is C.not_requested and "CAN VIEW on job 5" in src.permission and FakeDbx.calls == []
    monkeypatch.delenv("JOB_TOKEN")
    struct, src = _resolver(tmp_path).databricks_job(HOST, "5")
    assert struct is None and src.status is C.missing_credential and FakeDbx.calls == []


def test_a_read_job_is_kept_in_the_project_and_reused(tmp_path, monkeypatch):
    monkeypatch.setenv("JOB_TOKEN", "tok")
    FakeDbx.jobs = {"5": _job(5, _task("a"))}
    struct, src = _resolver(tmp_path).databricks_job(HOST, "5")
    assert src.status is C.retrieved and struct["tasks"][0]["notebook_path"] == "/Shared/x/a"
    kept = (tmp_path / "databricks" / "jobs" / "5.json").read_text(encoding="utf-8")
    assert "tok" not in kept and "SECRET-VALUE" not in kept
    FakeDbx.jobs = {}
    struct, src = _resolver(tmp_path).databricks_job(HOST, "5")
    assert src.status is C.cached and struct["tasks"][0]["key"] == "a" and FakeDbx.calls == []


def test_a_refused_read_names_the_permission_and_a_dynamic_id_is_not_guessed(tmp_path, monkeypatch):
    monkeypatch.setenv("JOB_TOKEN", "sekrit-123")
    FakeDbx.jobs = {}
    struct, src = _resolver(tmp_path).databricks_job(HOST, "9")
    assert struct is None and src.status is C.denied and "CAN VIEW on job 9" in src.permission and "sekrit" not in src.model_dump_json()
    struct, src = _resolver(tmp_path).databricks_job(HOST, None)
    assert struct is None and src.status is C.dynamic


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


@pytest.fixture()
def job_estate(tmp_path, monkeypatch):
    store, pid = _project(tmp_path, ESTATE)
    _discover(store)
    monkeypatch.setenv("JOB_TOKEN", "tok")
    FakeDbx.calls = []
    FakeDbx.jobs = {"101": _job(101, _task("read", "/Workspace/Shared/adf/NB_Read")),
                    "102": _job(102, _task("transform", "/Workspace/Shared/adf/NB_Transform")),
                    "103": _job(103, _task("nb", "/Workspace/Shared/adf/NB_Other"), _task("q", kind="sql", deps=["nb"]))}
    FakeDbx.notebooks = {"/Shared/adf/NB_Read": NB, "/Shared/adf/NB_Transform": NB, "/Shared/adf/NB_Other": NB}
    return store, pid


def _assess(store, pid, fetch=True):
    return build_manifest(store, pid, Options(tokens=TokenSource(env_names={"databricks": "JOB_TOKEN"}), fetch=fetch, client_factory=FakeDbx), NOW)


def test_each_notebook_task_of_a_readable_job_becomes_a_workload(job_estate):
    m = _assess(*job_estate)
    by_item = {w.source_item: w for w in m.workloads}
    read = by_item["/Shared/adf/NB_Read"]
    assert read.kind is W.databricks_notebook and read.used_by == ["PL_Spark/Generate"] and read.code.status is C.retrieved
    assert read.classification in (K.directly_convertible, K.convertible_with_changes)
    assert by_item["/Shared/adf/NB_Transform"].used_by == ["PL_Spark/Transform"]
    assert read.environment.workspace == HOST and read.environment.mode == "job_defined"
    assert ("export", HOST, "/Shared/adf/NB_Read") in FakeDbx.calls


def test_a_job_fabric_cannot_run_is_one_entry_with_the_reason_and_no_artifact(job_estate):
    m = _assess(*job_estate)
    mixed = next(w for w in m.workloads if w.source_item == "job 103")
    assert mixed.kind is W.databricks_job and mixed.classification is K.unsupported
    assert any("task 'q' is a sql task" in r for r in mixed.classification_reasons)
    assert mixed.artifact.kind == "none" and mixed.generation.status == "not_generated"
    assert any("Keep running it where it is" in a.summary for a in mixed.alternatives)
    assert not any(w.source_item == "/Shared/adf/NB_Other" for w in m.workloads)  # its notebook is not half-migrated


def test_without_being_asked_no_job_is_read_and_each_is_blocked(job_estate):
    m = _assess(*job_estate, fetch=False)
    jobs = [w for w in m.workloads if w.source_item.startswith("job ")]
    assert len(jobs) == 4 and all(w.kind is W.databricks_job and w.classification is K.blocked for w in jobs)
    assert all(w.code.status is C.not_requested for w in jobs) and FakeDbx.calls == []


def test_an_unread_or_dynamic_job_is_blocked_with_what_it_needs(job_estate):
    m = _assess(*job_estate)
    unread = next(w for w in m.workloads if w.source_item == "job 104")
    assert unread.classification is K.blocked and unread.code.status is C.denied and "CAN VIEW on job 104" in unread.code.permission
    dyn = next(w for w in m.workloads if w.source_item.startswith("(dynamic"))
    assert dyn.classification is K.blocked and dyn.code.status is C.dynamic


def test_the_compute_plan_counts_a_job_as_compute_the_job_defines(job_estate):
    store, pid = job_estate
    plan = build_compute_plan(store.load_adf_semantic_discovery(pid))
    use = next(u for u in plan.inventory if u.mode == "job_defined")
    assert "PL_Spark/Generate" in use.activities and not use.sizing_known
    assert any(g.code == "job_defined_compute" for g in plan.gaps)
    t = next(t for t in plan.targets if t.activity == "Generate")
    assert t.source_item == "job 101" and t.target_execution == "FabricNotebook"


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


def test_multi_task_workloads_name_the_fabric_activity_that_runs_them(tmp_path, monkeypatch):
    estate = {"linkedService/LS_Job.json": ESTATE["linkedService/LS_Job.json"],
              "pipeline/PL_Multi.json": {"name": "PL_Multi", "properties": {"activities": [_job_act("Run", "201")]}}}
    store, pid = _project(tmp_path, estate)
    _discover(store)
    monkeypatch.setenv("JOB_TOKEN", "tok")
    FakeDbx.jobs = {"201": _job(201, _task("first task", "/Shared/m/NB_1"), _task("second", "/Shared/m/NB_2", deps=["first task"]))}
    FakeDbx.notebooks = {"/Shared/m/NB_1": NB, "/Shared/m/NB_2": NB}
    m = _assess(store, pid)
    assert {w.source_item: w.used_by for w in m.workloads} == {"/Shared/m/NB_1": ["PL_Multi/Run_first_task"],
                                                               "/Shared/m/NB_2": ["PL_Multi/Run_second"]}


def test_the_real_client_is_given_a_url_for_the_bare_host_the_linked_service_resolves_to(tmp_path):
    # found by the first live run: the host reached the client without https:// and every real read was refused as a bad URL
    client = SourceResolver(tmp_path)._client(HOST, "tok")
    assert isinstance(client, DatabricksClient) and client.host == f"https://{HOST}"


def test_a_job_whose_notebooks_are_generated_and_approved_is_not_held_back_as_unconvertible(job_estate):
    # found 2026-11-10: the brief's 'hold back' removed PL_Spark_Test, so the deploy that creates its notebooks never saw it
    from adf2fabric.agent.consultation.brief import read_facts
    from adf2fabric.agent.workloads import lifecycle

    store, pid = job_estate
    before = read_facts(store, pid)
    assert "PL_Spark" in before.blocked_pipelines and before.blocked_activities == 5  # every DatabricksJob activity, by type
    plain = "# Databricks notebook source\nprint(spark.range(3).count())\n"  # nothing left open, so it is 'generated' and can be created
    FakeDbx.notebooks = {k: plain for k in FakeDbx.notebooks}
    m = build_manifest(store, pid, Options(tokens=TokenSource(env_names={"databricks": "JOB_TOKEN"}), fetch=True, client_factory=FakeDbx,
                                           generate=True), NOW)
    store.save_workload_manifest(pid, m)
    lifecycle.approve(store, pid, "NB_Read", by="Pat")
    after = read_facts(store, pid)
    assert after.blocked_activities == 4  # Generate's only notebook is approved; Transform's is not yet, Mixed/Unreadable/Dynamic stay blocked
    lifecycle.approve(store, pid, "NB_Transform", by="Pat")
    assert read_facts(store, pid).blocked_activities == 3 and "PL_Spark" in read_facts(store, pid).blocked_pipelines  # Mixed etc. still there
