"""The diagnosis: for each pipeline and service, why it is not yet running perfectly in Fabric and what to do. Read-only."""

from __future__ import annotations

import json

import pytest

from adf2fabric.agent.diagnose import engine as E
from adf2fabric.agent.diagnose import remedies as R
from adf2fabric.agent.structure.compare import Parity
from tests.test_conversion_agent import FILES
from tests.test_partial_source import _convert, _without

G = lambda n: f"{n:08d}-0000-0000-0000-000000000000"  # noqa: E731


def _bound(tmp_path):
    f = tmp_path / "res.json"
    f.write_text(json.dumps([{"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": G(1)}, {"type": "LinkedServiceToConnectionId", "key": "LS_LAKE", "value": G(2)},
                             {"type": "ItemToId", "key": "connection:invoke-pipeline", "value": G(3)}, {"type": "ItemToId", "key": "pipeline:PL_Child", "value": G(4)},
                             {"type": "WorkspaceId", "key": "*", "value": G(5)}]), encoding="utf-8")
    return str(f)


def _row(name, deployed=True, parity="equivalent", run="completed", failure="", activities=()):
    p = Parity(name, 1, 1)
    if parity == "differs":
        p.differences.append("activities missing in Fabric: X")
    r = None if run == "never" else {"status": "Completed" if run == "completed" else "Failed", "failure": failure, "activities": list(activities)}
    return {"pipeline": name, "deployed": deployed, "parity": p if deployed else None, "run": r}


def _by(d):
    return {p["pipeline"]: p for p in d["pipelines"]}


# --- the remedy tables ------------------------------------------------------------------------------------------------------------------


def test_every_remedy_names_a_cause_and_a_fix():
    for code, r in R.CONVERSION.items():
        assert r.cause.strip() and len(r.fix) > 20, code
    for kind, r in R.BINDING.items():
        assert r.cause.strip() and len(r.fix) > 20, kind
    for _, r in R.RUN_FAILURES:
        assert r.cause.strip() and len(r.fix) > 20


def test_the_codes_the_converter_gives_are_covered():
    for code in ("linked_service_not_found", "dataset_not_found", "synapse_notebook_migrate", "ssis_unsupported", "dataflow_reauthor", "unsupported_trigger", "no_conversion_rule",
                 "stored_procedure_linked_service_not_in_source", "nesting_too_deep", "web_dynamic_url", "databricks_cluster"):
        assert code in R.CONVERSION, code


@pytest.mark.parametrize("message,expect", [
    ("Operation on target Copy failed: 401 Unauthorized", "credential or permissions"),
    ("AuthorizationPermissionMismatch: This request is not authorized", "credential or permissions"),
    ("Login failed for user 'etl'", "credential or permissions"),
    ("CapacityNotActive: the capacity is paused", "capacity"),
    ("Cannot open server 'x' requested by the login", "unreachable"),
    ("Incorrect syntax near the keyword 'AS'", "SQL does not run"),
    ("The expression 'formatNumber(...)' cannot be evaluated", "expression fails"),
    ("ConnectionNotFound: the connection id is invalid", "connection id"),
    ("The gateway is offline", "gateway"),
    ("BlobNotFound: the specified blob does not exist", "missing"),
    ("Livy session failed to start", "notebook failed"),
    ("The operation timed out", "time limit"),
])
def test_a_failed_runs_message_is_matched_to_the_usual_cause(message, expect):
    r = R.for_run_failure(message)
    assert r is not None and expect.lower() in (r.cause + r.fix).lower()


def test_an_unrecognised_message_has_no_remedy_so_the_diagnosis_shows_it_raw():
    assert R.for_run_failure("something nobody has seen before") is None


# --- the stages --------------------------------------------------------------------------------------------------------------------------


def test_without_a_conversion_it_says_to_convert_first(tmp_path):
    from tests.test_adf_semantic_discovery import _make_project

    store, pid = _make_project(tmp_path, FILES)
    d = E.diagnose(store, pid)
    assert d["ready"] is False and "convert" in d["message"] and "convert" in E.render(d)


def test_a_missing_linked_service_is_not_converted_and_the_fix_names_every_way_out(tmp_path):
    store, pid, _ = _convert(tmp_path, _without("linkedService/"))
    d = E.diagnose(store, pid)
    p = _by(d)["PL_Main"]
    assert p["stage"] == "not_converted"
    codes = {x["code"] for x in p["problems"]}
    assert "linked_service_not_found" in codes and "stored_procedure_linked_service_not_in_source" in codes
    fix = next(x for x in p["problems"] if x["code"] == "linked_service_not_found")["fix"]
    assert "linkedService/" in fix and "ARMTemplateForFactory.json" in fix and "bind" in fix
    assert d["fix_first"][0]["pipelines"] == ["PL_Main"] and "linked service" in d["fix_first"][0]["cause"].lower()
    assert any(s["state"] != "bound" for s in d["services"])


def test_converted_but_unbound_says_what_to_create_and_how_to_bind_it(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)  # converted, but nothing bound
    d = E.diagnose(store, pid)
    p = _by(d)["PL_Main"]
    assert p["stage"] == "unbound"
    assert "connection" in {x["binding_kind"] for x in p["problems"]}
    assert any("resolutions" in x["command"] for x in p["problems"] if x["binding_kind"] == "connection")
    assert not any(x["binding"] in ("workspace", "pipeline:PL_Child") for x in p["problems"])  # deploy fills those in itself


def test_bound_and_offline_is_ready_to_deploy_and_says_no_workspace_was_read(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES, resolutions_path=_bound(tmp_path))
    d = E.diagnose(store, pid)
    assert {p["stage"] for p in d["pipelines"]} == {"ready_to_deploy"} and d["workspace_read"] is False
    assert "add --workspace-id" in E.render(d)


@pytest.mark.parametrize("rows,stage", [
    ([], "not_deployed"),
    ([_row("PL_Main", deployed=False)], "not_deployed"),
    ([_row("PL_Main", parity="differs")], "deployed_differs"),
    ([_row("PL_Main", run="never")], "never_run"),
    ([_row("PL_Main", run="failed", failure="401 Unauthorized")], "run_failed"),
    ([_row("PL_Main")], "run_ok_no_baseline"),
])
def test_what_is_in_the_workspace_decides_the_stage(tmp_path, rows, stage):
    store, pid, _ = _convert(tmp_path, FILES, resolutions_path=_bound(tmp_path))
    d = E.diagnose(store, pid, {"pipelines": rows, "workspace_pipelines": []})
    assert _by(d)["PL_Main"]["stage"] == stage


def test_a_failed_run_gets_the_cause_the_raw_message_and_the_failed_activity(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES, resolutions_path=_bound(tmp_path))
    row = _row("PL_Main", run="failed", failure="Operation failed", activities=[{"name": "Copy_Customer", "status": "Failed", "error": "ErrorCode=Unauthorized, 403 Forbidden"}])
    pr = _by(E.diagnose(store, pid, {"pipelines": [row], "workspace_pipelines": []}))["PL_Main"]["problems"][0]
    assert pr["recognised"] and "credential" in pr["cause"] and "403" in pr["detail"] and pr["failed_activities"] == ["Copy_Customer"]
    unknown = _row("PL_Main", run="failed", failure="zzz mystery")
    pr = _by(E.diagnose(store, pid, {"pipelines": [unknown], "workspace_pipelines": []}))["PL_Main"]["problems"][0]
    assert pr["recognised"] is False and "Monitor" in pr["fix"] and "zzz mystery" in pr["detail"]


def test_a_run_that_matched_its_baseline_is_proven_and_one_that_differs_is_not(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES, resolutions_path=_bound(tmp_path))
    (store.project_dir(pid) / "verification").mkdir()
    f = store.project_dir(pid) / "verification" / "verification.json"
    f.write_text(json.dumps({"PL_Main": {"verdict": "matches"}}), encoding="utf-8")
    assert _by(E.diagnose(store, pid, {"pipelines": [_row("PL_Main")], "workspace_pipelines": []}))["PL_Main"]["stage"] == "proven"
    f.write_text(json.dumps({"PL_Main": {"verdict": "differs", "differences": [{"activity": "Copy_Customer", "fact": "rowsCopied", "baseline": 4, "fabric": 3}]}}), encoding="utf-8")
    p = _by(E.diagnose(store, pid, {"pipelines": [_row("PL_Main")], "workspace_pipelines": []}))["PL_Main"]
    assert p["stage"] == "differs_from_baseline" and "rowsCopied" in p["problems"][0]["detail"]


def test_upstream_problems_come_first_and_a_stale_deployment_is_noted(tmp_path):
    store, pid, _ = _convert(tmp_path, _without("linkedService/"))
    d = E.diagnose(store, pid, {"pipelines": [_row("PL_Main")], "workspace_pipelines": []})
    p = _by(d)["PL_Main"]
    assert p["stage"] == "not_converted" and any("deployed in the workspace from an earlier conversion" in n for n in p["notes"])


def test_workspace_pipelines_the_project_does_not_own_are_listed_and_never_touched(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES, resolutions_path=_bound(tmp_path))
    d = E.diagnose(store, pid, {"pipelines": [_row("PL_Main")], "workspace_pipelines": ["PL_Main", "PL_Child", "PL_Stale_Sql", "ZZ_Probe"]})
    assert d["extras"] == ["PL_Stale_Sql", "ZZ_Probe"]
    text = E.render(d)
    assert "never deletes or changes a Fabric item" in text and "PL_Stale_Sql" in text


def test_services_say_what_exists_what_is_missing_and_what_to_do(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)
    unbound = {s["service"]: s for s in E.diagnose(store, pid)["services"]}
    assert unbound["LS_SQL"]["state"] == "not bound" and "bind" in unbound["LS_SQL"]["todo"]
    store2, pid2, _ = _convert(tmp_path / "b", FILES, resolutions_path=_bound(tmp_path))
    bound = {s["service"]: s for s in E.diagnose(store2, pid2)["services"]}
    assert bound["LS_SQL"]["state"] == "bound" and "test the connection" in bound["LS_SQL"]["todo"]


def test_the_rendered_report_has_the_ranking_the_pipelines_and_the_services(tmp_path):
    store, pid, _ = _convert(tmp_path, _without("linkedService/"))
    text = E.render(E.diagnose(store, pid))
    for part in ("## Where the pipelines are", "## Fix these first", "## Each pipeline", "### PL_Main", "Do:"):
        assert part in text
