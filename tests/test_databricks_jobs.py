"""ADF DatabricksJob activities: read the job (only when authorised), its notebook tasks become workloads, and a job whose
notebooks are deployed and bound converts to Fabric Notebook activities wired as the job's tasks.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import base64
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


def test_parameter_values_are_kept_but_never_secrets_or_cluster_settings():
    # values are kept since 2026-11-13 (the user's decision: they cannot be mapped without them); secrets never are
    raw = _job(1, _task("a", nb={"base_parameters": {"day": "2024-01-01", "db_password": "hunter2-hunter2", "key": "{{secrets/s/k}}"}},
                        libraries=[{"pypi": {"package": "pandas==2.0", "repo": "https://user:pw@pypi.example"}}, {"whl": "dbfs:/x.whl"}]),
               _task("py", kind="spark_python", spark_python_task={"python_file": "/Workspace/Shared/x/run.py", "source": "WORKSPACE",
                                                                   "parameters": ["{{job.parameters.env}}", "10", "dapi" + "0" * 32]}),
               parameters=[{"name": "env", "default": "prod"}])
    s = J.structure(raw)
    text = json.dumps(s)
    for leaked in ("hunter2", "secrets/s/k", "dapi" + "0" * 32, "SECRET-VALUE", "user:pw", "someone@example.com", "job_clusters"):
        assert leaked not in text
    a, py = s["tasks"]
    assert a["base_parameters"] == {"day": "2024-01-01"} and a["parameter_names"] == ["day", "db_password", "key"]
    assert s["parameters"] == {"env": "prod"} and s["parameter_names"] == ["env"]
    assert s["withheld"] == ["task 'a' base parameter 'db_password'", "task 'a' base parameter 'key'", "task 'py' argument 3"]
    assert py["kind"] == "spark_python" and py["python_file"] == "/Shared/x/run.py" and py["python_source"] == "WORKSPACE"
    assert py["arguments"] == ["{{job.parameters.env}}", "10"] and py["argument_count"] == 3
    assert a["libraries"] == [{"pypi": {"package": "pandas==2.0"}}, {"whl": "dbfs:/x.whl"}]
    assert a["notebook_path"] == "/Shared/x/a"  # /Workspace/Shared/x/a and /Shared/x/a are one notebook
    assert any("looks like one" in r for r in J.fabric_shape(s).reasons)  # a withheld value blocks the job, never a blank value


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


def _py(key, file="/Workspace/Shared/x/run.py", args=(), source="WORKSPACE", **extra):
    return _task(key, kind="spark_python", spark_python_task={"python_file": file, "source": source, "parameters": list(args)}, **extra)


@pytest.mark.parametrize("task,reason", [
    (_task("q", kind="sql"), "task 'q' is a sql task"),
    (_task("g", nb={"source": "GIT"}), "from a Git repository"),
    (_task("p", nb={"base_parameters": {"d": "{{job.start_time.iso_date}}"}}), "dynamic value reference {{job.start_time.iso_date}}"),
    (_task("p", nb={"base_parameters": {"d": "{{job.parameters.nowhere}}"}}), "job parameter 'nowhere', which the job does not define"),
    (_py("y", file="dbfs:/scripts/run.py", source=""), "not in the workspace (dbfs:/scripts/run.py)"),
    (_py("y", args=["x-{{job.parameters.env}}"]), "dynamic value reference {{job.parameters.env}}"),  # only a WHOLE reference is mapped
    (_task("l", libraries=[{"pypi": {"package": "x"}}]), "installs libraries"),
    (_task("r", run_if="AT_LEAST_ONE_SUCCESS"), "AT_LEAST_ONE_SUCCESS"),
    (_task("u", deps=["nowhere"]), "depends on 'nowhere'"),
])
def test_every_part_fabric_cannot_run_is_a_reason_never_a_guess(task, reason):
    shape = J.fabric_shape(J.structure(_job(1, task, parameters=[{"name": "env", "default": "dev"}])))
    assert any(reason in r for r in shape.reasons), shape.reasons


def test_what_each_task_receives_job_parameters_by_name_base_parameters_and_arguments_by_position():
    s = J.structure(_job(1, _task("nb", nb={"base_parameters": {"min": "10", "day": "{{job.parameters.run_date}}"}}),
                         _py("py", args=["{{job.parameters.run_date}}", "@literal"], deps=["nb"]),
                         parameters=[{"name": "run_date", "default": "2026-01-01"}]))
    shape = J.fabric_shape(s, {"run_date": {"value": "@pipeline().parameters.d", "type": "Expression"}})
    assert shape.reasons == []
    assert shape.parameters["nb"] == {"run_date": ("job", "run_date"), "min": ("literal", "10"), "day": ("job", "run_date")}
    assert shape.parameters["py"] == {"argv_1": ("job", "run_date"), "argv_2": ("literal", "@literal")}  # no job parameter pushed
    assert J.fabric_value(("job", "run_date"), shape, {"run_date": "x"}) == "x"  # what the ADF activity passes wins
    assert J.fabric_value(("job", "run_date"), shape, {}) == "2026-01-01"  # else the job's default
    assert J.code_path(s["tasks"][1]) == "/Shared/x/run.py" and J.notebook_name("/Shared/x/run.py") == "run"


def test_job_parameter_problems_cycles_and_old_records_are_reasons():
    both = _job(1, _task("a", nb={"base_parameters": {"p": "1"}}), parameters=[{"name": "p", "default": "1"}])
    assert "has the name of a job parameter" in J.fabric_shape(J.structure(both)).reasons[0]
    one = J.structure(_job(1, _task("a"), parameters=[{"name": "p", "default": "1"}]))
    assert "passes job parameter 'q', which the job does not define" in J.fabric_shape(one, {"q": "1"}).reasons[0]
    assert "are an expression" in J.fabric_shape(one, {"value": "@pipeline().parameters.all", "type": "Expression"}).reasons[0]
    old = {"job_id": "1", "parameter_names": ["p"], "tasks": [{"key": "a", "kind": "notebook", "notebook_path": "/a", "notebook_source": "WORKSPACE",
                                                               "parameter_names": [], "run_if": "ALL_SUCCESS"}]}
    assert "read before parameter values were kept" in J.fabric_shape(old).reasons[0]
    assert "cycle" in " ".join(J.fabric_shape(J.structure(_job(1, _task("a", deps=["b"]), _task("b", deps=["a"])))).reasons)
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


def _workload(path, *reads):
    """The approved, deployed workload record of a task's notebook: which parameters its generated notebook reads."""
    from adf2fabric.project.workloads import (Classification, CodeSource, CodeStatus, ExecutionEnvironment, ParameterContract,
                                              WorkloadEntry, WorkloadKind)

    return WorkloadEntry(id=f"databricks_notebook:{path.lower()}", kind=WorkloadKind.databricks_notebook, name=J.notebook_name(path),
                         source_item=path, environment=ExecutionEnvironment(platform="databricks"),
                         code=CodeSource(status=CodeStatus.cached, location=path), classification=Classification.convertible_with_changes,
                         parameters=[ParameterContract(name=n, passed_by_pipeline=True, declared_in_code=True, mapped=True) for n in reads])


def _convert(pipeline, jobs, resolutions, workloads=()):
    d = semantic([pipeline], [], [linked_service("LS_Job", "AzureDatabricks", {"domain": f"https://{HOST}"})])
    ctx = build_project_context(d, Resolutions(resolutions))
    ctx.databricks_jobs = {k: J.structure(v) for k, v in jobs.items()}
    ctx.workloads = {w.source_item.lower(): w for w in workloads}
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
    ({"7": _job(7, _task("only", "/Shared/NB_A", nb={"base_parameters": {"d": "1"}}))}, ("NB_A",), "is not an approved, deployed workload"),
])
def test_anything_short_of_that_stays_a_fail_placeholder_with_the_reason(jobs, bound, needle):
    rec, content = _convert(pipe(_jact("Job")), jobs, _bound(*bound))
    a = by_name(rec)["Job"]
    assert find(content, "Job")["type"] == "Fail" and a.status is S.blocked and "databricks_job" in codes(a)
    assert any(needle in (w.message or "") for w in a.warnings), [w.message for w in a.warnings]


def test_job_parameters_base_parameters_and_python_arguments_reach_the_notebooks_that_read_them():
    """The shape of the live test job (2026-11-13): job parameter run_date, a notebook task with base parameter min_amount, then a
    Python file task given run_date and a literal as arguments."""
    job = _job(7, _task("nb", "/Shared/NB_A", nb={"base_parameters": {"min_amount": "10000", "unused": "x"}}),
               _py("py", "/Workspace/Shared/NB_B.py", args=["{{job.parameters.run_date}}", "10000"], deps=["nb"]),
               parameters=[{"name": "run_date", "default": "2026-01-01"}, {"name": "region", "default": "@in"}])
    passed = expr("@pipeline().parameters.day")
    p = pipe(_jact("Job", tp={"jobParameters": {"run_date": passed}}), parameters={"day": {"type": "String"}})
    wl = (_workload("/Shared/NB_A", "run_date", "min_amount", "region"), _workload("/Shared/NB_B.py", "argv_1", "argv_2"))
    rec, content = _convert(p, {"7": job}, _bound("NB_A", "NB_B"), wl)
    nb, py = find(content, "Job_nb"), find(content, "Job_py")
    assert nb["type"] == py["type"] == "TridentNotebook" and py["typeProperties"]["notebookId"] == IDS["NB_B"]
    assert nb["typeProperties"]["parameters"] == {
        "run_date": {"value": {"value": "@pipeline().parameters.day", "type": "Expression"}, "type": "string"},  # what ADF passes
        "min_amount": {"value": "10000", "type": "string"},
        "region": {"value": "@@in", "type": "string"}}  # a literal starting with @ is escaped, never read as an expression
    assert py["typeProperties"]["parameters"] == {
        "argv_1": {"value": {"value": "@pipeline().parameters.day", "type": "Expression"}, "type": "string"},
        "argv_2": {"value": "10000", "type": "string"}}
    a = by_name(rec)["Job"]
    assert a.status is not S.blocked
    assert any("unused" in (w.message or "") for w in a.warnings)  # given explicitly but never read: reported, not passed


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




# =====================================================================================================================
# 5. a Python file task's code becomes a notebook: sys.argv[N] -> parameter-cell variable argv_N
# =====================================================================================================================

PY_TASK = '''"""The live test job's Python file task (2026-11-13)."""
import sys

from pyspark.sql import functions as F

run_date, min_amount = sys.argv[1], int(sys.argv[2])
print(run_date, min_amount)
'''


def _as_notebook(text, argv=True):
    from adf2fabric.agent.workloads import analyzer, notebook_format, transform

    cells, _ = notebook_format.load(text, "run.py")
    an = analyzer.analyze(cells, "databricks", argv=argv)
    return an, transform.transform(cells, an, "databricks")


def test_a_python_file_tasks_arguments_become_parameters_argv_n():
    an, tr = _as_notebook(PY_TASK)
    assert tr.parameters == [("argv_1", ""), ("argv_2", "")]
    param_cell, body = tr.cells[0], "".join(tr.cells[1]["source"])
    assert param_cell["metadata"] == {"tags": ["parameters"]} and "argv_1 = ''" in "".join(param_cell["source"])
    assert "run_date, min_amount = argv_1, int(argv_2)" in body and "sys.argv" not in body
    assert {r.code for r in tr.rewrites} == {"argv_get"}
    assert not any(f.code == "argv_other" for f in an.findings)


@pytest.mark.parametrize("code", ["n = len(sys.argv)", "args = sys.argv[1:]", "from sys import argv", "i = 1\nx = sys.argv[i]"])
def test_any_other_use_of_the_argument_list_stays_a_finding_never_a_guess(code):
    an, tr = _as_notebook("import sys\n" + code + "\n")
    assert any(f.code == "argv_other" for f in an.findings)


def test_a_notebook_task_never_has_its_sys_argv_rewritten():
    an, tr = _as_notebook(PY_TASK, argv=False)
    assert tr.parameters == [] and "sys.argv[1]" in "".join(tr.cells[0]["source"])


# =====================================================================================================================
# 6. `x = f(dbutils.widgets.get("x"))`: mapped only when it is the single read and the only binding of x (2026-11-13)
# =====================================================================================================================


def test_a_widget_converted_into_a_variable_of_its_own_name_becomes_a_parameter():
    an, tr = _as_notebook('min_amount = int(dbutils.widgets.get("min_amount"))\nprint(min_amount)\n', argv=False)
    assert tr.parameters == [("min_amount", "")]
    assert "min_amount = int(min_amount)" in "".join(tr.cells[1]["source"])
    assert not any(f.code == "widget_not_convertible" for f in an.findings)


@pytest.mark.parametrize("extra", [
    'min_amount = 5\n',                                       # bound again
    'for min_amount in range(3):\n    pass\n',                 # bound by a loop
    'def f(min_amount):\n    return min_amount\n',            # bound as an argument
    'y = dbutils.widgets.get("min_amount")\n',                # the widget is read again: it would now get the int
    'min_amount += 1\n',
])
def test_it_stays_a_clash_when_the_name_is_bound_elsewhere_or_the_widget_is_read_again(extra):
    from adf2fabric.agent.workloads import analyzer, notebook_format

    cells, _ = notebook_format.load('min_amount = int(dbutils.widgets.get("min_amount"))\n' + extra, "nb.py")
    an = analyzer.analyze(cells, "databricks")
    assert "min_amount" in an.assigned_names


# =====================================================================================================================
# 7. a serverless job ENVIRONMENT's libraries -> one Fabric Environment per job environment, approved on its own (2026-11-13)
# =====================================================================================================================

WHEEL = "/Workspace/Shared/x/libs/mylib-0.1.0-py3-none-any.whl"


def _env_job(*deps, key="libs"):
    return _job(9, _py("t", "/Workspace/Shared/x/run.py", environment_key=key),
                environments=[{"environment_key": key, "spec": {"client": "2", "dependencies": list(deps)}}], name="Lib Job")


@pytest.mark.parametrize("dep,kind", [("humanize==4.9.0", "public"), ("pandas", "public"), ("requests[socks]>=2,<3", "public"),
                                      (WHEEL, "custom"), ("/Volumes/c/s/v/x.whl", None), ("dbfs:/x.whl", None),
                                      ("-r /Workspace/req.txt", None), ("git+https://h/r", None), ("/Workspace/../x.whl", None)])
def test_only_pypi_requirements_and_workspace_wheels_are_migrated(dep, kind):
    assert J.dependency_kind(dep) == kind


def test_a_job_environment_is_kept_and_a_dependency_with_credentials_is_withheld():
    s = J.structure(_env_job("humanize==4.9.0", WHEEL, "--index-url https://user:secretpw@pypi.example/simple"))
    assert s["environments"] == {"libs": ["humanize==4.9.0", WHEEL]} and s["tasks"][0]["environment_key"] == "libs"
    assert "secretpw" not in json.dumps(s) and s["withheld"] == ["environment 'libs' dependency 3"]
    assert J.fabric_shape(J.structure(_env_job("humanize==4.9.0", WHEEL))).reasons == []
    assert any("neither a PyPI requirement nor a .whl" in r for r in J.fabric_shape(J.structure(_env_job("/Volumes/c/s/v/x.whl"))).reasons)


def test_each_job_environment_becomes_one_fabric_environment_with_the_wheel_read_and_the_packages_listed(tmp_path):
    from adf2fabric.agent.workloads import environments as E

    s = J.structure(_env_job("humanize==4.9.0", WHEEL))
    envs = E.from_jobs({"9": s}, {"9:libs": ["databricks_notebook:/shared/x/run.py"]})
    assert [(e.id, e.name, [lib.kind for lib in e.libraries]) for e in envs] == [("9:libs", "Lib_Job_libs", ["public", "custom"])]
    e = envs[0]
    E.generate(e, tmp_path, None)  # not read yet and no credential: said, never a blank library
    assert e.definition_file is None and "has not been read from Databricks" in e.problems[0]
    cached = tmp_path / "databricks" / "files" / WHEEL.lstrip("/")
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"PK-wheel-bytes")
    e = E.from_jobs({"9": s}, {"9:libs": ["w"]})[0]
    E.generate(e, tmp_path, None)
    parts = {p["path"]: base64.b64decode(p["payload"]) for p in json.loads((tmp_path / e.definition_file).read_text())["parts"]}
    assert parts == {"Libraries/CustomLibraries/mylib-0.1.0-py3-none-any.whl": b"PK-wheel-bytes",
                     "Libraries/PublicLibraries/environment.yml": b"dependencies:\n  - pip:\n      - humanize==4.9.0\n"}
    assert E.current(e, tmp_path) and e.sha256
    assert E.attach({"metadata": {"language_info": {"name": "python"}}}, "env-id", "WS")["metadata"] == {
        "language_info": {"name": "python"}, "dependencies": {"environment": {"environmentId": "env-id", "workspaceId": "ws"}}}


def test_an_environment_approval_holds_only_for_the_same_definition(tmp_path):
    from datetime import datetime, timezone

    from adf2fabric.agent.workloads import environments as E

    cached = tmp_path / "databricks" / "files" / WHEEL.lstrip("/")
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"v1")

    def made():
        e = E.from_jobs({"9": J.structure(_env_job("humanize==4.9.0", WHEEL))}, {"9:libs": ["w"]})[0]
        E.generate(e, tmp_path, None)
        return e

    old = made()
    E.approve(old, "Sathvik", True, "", datetime(2026, 11, 13, tzinfo=timezone.utc))
    old.fabric_item_ids["ws"] = "env-1"
    again = E.keep_decisions([made()], [old])[0]
    assert again.approval.status == "approved" and again.fabric_item_ids == {"ws": "env-1"}
    cached.write_bytes(b"v2")  # the wheel changed in Databricks
    changed = E.keep_decisions([made()], [old])[0]
    assert changed.approval.status == "pending" and changed.fabric_item_ids == {"ws": "env-1"}  # the item is there, the approval is not
