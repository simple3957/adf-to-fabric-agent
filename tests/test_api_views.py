"""The API behind the guided UI: authorisation, server-side redaction, failure states, progress, duplicate submissions, confirmations,
production mode. Every UI promise ("a hidden button is not protection", "nothing is shown that an ordinary view must not show") is
checked here, on the server, against real responses."""

from __future__ import annotations

import json
import re
import time

import pytest
from fastapi.testclient import TestClient

from adf2fabric.agent.deployment import record as deploy_record
from adf2fabric.api import views
from adf2fabric.api.app import create_app
from adf2fabric.api.container import Container, Settings
from adf2fabric.api.demo import DEMO_WS, build_demo_container
from adf2fabric.api.security import SecuritySettings
from tests.test_api_security import ALL, World  # noqa: F401  (a token-mode app with named users in two organisations)

P = "/projects/demo-estate"
FOREIGN_WS = "22222222-2222-2222-2222-222222222222"


@pytest.fixture()
def world(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    monkeypatch.delenv("ADF2FABRIC_ENV", raising=False)
    w = World(tmp_path)
    raw, entered = w.call, set()

    def call(who, method, path, **kw):
        # a session is per person: everyone who acts in these tests uses the demo estate and the demo workspace
        if who and who not in entered and path != "/mode":
            entered.add(who)
            raw(who, "POST", "/mode", json_body={"mode": "demo"})
            raw(who, "PUT", "/session/destination", json_body={"provider": "azure_cli", "workspace_id": DEMO_WS})
        return raw(who, method, path, **kw)

    w.call = call
    return w


def stage(w: World, who: str, name: str, body: dict | None = None, **kw):
    r = w.call(who, "POST", f"{P}/stages/{name}", json_body=body or {}, **kw)
    return r


def run(w: World, who: str, name: str, body: dict | None = None):
    r = stage(w, who, name, body)
    assert r.status_code == 202, (name, r.status_code, r.text)
    j = w.wait(who, r.json())
    return j


def prepare(w: World, who: str = "oscar", through: str = "convert"):
    w.call(who, "POST", "/mode", json_body={"mode": "demo"})
    for name in ("discovery", "semantic", "dependencies", "workloads", "convert"):
        assert run(w, who, name)["state"] == "succeeded", name
        if name == through:
            break
    assert w.call(who, "PUT", "/session/destination", json_body={"provider": "azure_cli", "workspace_id": DEMO_WS}).status_code == 200


def plan(w: World, who: str = "ann"):
    r = w.call(who, "POST", f"{P}/provisioning/plan", json_body={"lakehouses": ["lh_demo"]})
    assert r.status_code == 202, r.text
    assert w.wait(who, r.json())["state"] == "succeeded"
    return w.call(who, "GET", f"{P}/provisioning").json()


# =====================================================================================================================
# 1. authorisation of every new route (server-side; the UI hiding a button is not the protection)
# =====================================================================================================================

VIEW_ROUTES = [f"{P}/inventory", f"{P}/compatibility", f"{P}/validation", f"{P}/report", f"{P}/provisioning"]


@pytest.mark.parametrize("path", VIEW_ROUTES)
def test_views_need_authentication_and_the_report_permission(world, path):
    prepare(world)
    assert world.call(None, "GET", path).status_code == 401
    assert world.call("vic", "GET", path).status_code == 200  # a viewer holds reports:view
    assert world.call("bob", "GET", path).status_code == 404  # another organisation: not found, not forbidden


def test_provisioning_actions_need_their_own_permissions(world):
    prepare(world)
    body = {"lakehouses": ["lh_demo"]}
    assert world.call("vic", "POST", f"{P}/provisioning/plan", json_body=body).status_code == 403  # a viewer cannot even plan
    assert world.call("abe", "POST", f"{P}/provisioning/plan", json_body=body).status_code == 403  # an approver is not an analyst
    view = plan(world, "ann")
    ref = view["plan_ref_full"]
    steps = [s["ref"] for s in view["steps"] if s["needs_approval"]]
    assert world.call("ann", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": ref, "steps": steps}).status_code == 403  # analysts cannot approve
    assert world.call("oscar", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": ref, "steps": steps}).status_code == 403  # nor can operators
    assert world.call("abe", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": ref, "steps": steps}).status_code == 200
    assert world.call("abe", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref}).status_code == 403  # approving is not executing
    assert world.call("ann", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref}).status_code == 403
    assert world.call("oscar", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref}).status_code == 202  # executing needs migration:execute
    assert world.call("vic", "POST", f"{P}/provisioning/verify", json_body={}).status_code == 403


def test_stages_added_for_the_ui_have_their_permissions(world):
    prepare(world, through="discovery")
    for name, role_ok, role_no in (("dependencies", "ann", "vic"), ("workloads", "ann", "abe"), ("validate", "ann", "vic")):
        assert stage(world, role_no, name).status_code == 403, name
        r = stage(world, role_ok, name)
        assert r.status_code in (202, 409), (name, r.status_code)
        if r.status_code == 202:
            world.wait(role_ok, r.json())


def test_no_view_leaks_across_organisations_even_with_a_guessed_job_or_step(world):
    prepare(world)
    view = plan(world, "ann")
    ref = view["plan_ref_full"]
    steps = [s["ref"] for s in view["steps"] if s["needs_approval"]]
    for method, path, body in [("POST", f"{P}/provisioning/plan", {}), ("POST", f"{P}/provisioning/approve", {"plan_ref": ref, "steps": steps}),
                               ("POST", f"{P}/provisioning/apply", {"plan_ref": ref}), ("POST", f"{P}/provisioning/verify", {})]:
        assert world.call("bob", method, path, json_body=body).status_code == 404, path


# =====================================================================================================================
# 2. server-side redaction
# =====================================================================================================================

PATH_RE = re.compile(r"([A-Za-z]:[\\/]|/(?:home|Users|tmp|var|etc|private)/|\\\\)")
SECRET_RE = re.compile(r"(?i)(bearer\s+\S{8,}|eyJ[\w-]{20,}|accountkey=|sharedaccesssignature=|password=\S|ghp_[A-Za-z0-9]{20,})")


def test_sanitize_drops_paths_secrets_and_code_and_bounds_strings(tmp_path):
    data = {"name": "PL_A", "source_path": "C:\\data\\x.json", "token": "abc", "connection_string": "Server=x;Password=hunter2", "artifact": "a/b",
            "nested": [{"file": "x", "message": f"opened {tmp_path}\\secret.txt with Bearer abcdefghijkl", "configuration": {"q": "SELECT 1"}}],
            "long": "x" * 5000, "count": 3}
    out = views.sanitize(data, (tmp_path,))
    blob = json.dumps(out)
    assert out["name"] == "PL_A" and out["count"] == 3 and len(out["long"]) <= 600
    assert "source_path" not in out and "token" not in out and "connection_string" not in out and "artifact" not in out
    assert "configuration" not in out["nested"][0] and "file" not in out["nested"][0]
    assert str(tmp_path) not in blob and "abcdefghijkl" not in blob and "hunter2" not in blob and "SELECT" not in blob


def test_every_view_is_free_of_paths_secrets_internal_ids_and_traces(world):
    prepare(world)
    plan(world, "ann")
    run(world, "ann", "validate")
    root = str(world.demo.store.projects_root)
    for path in VIEW_ROUTES + ["/jobs", f"/jobs?project_id=demo-estate", "/session", "/me", "/projects"]:
        text = world.call("root", "GET", path).text
        assert root not in text and root.replace("\\", "/") not in text, path
        assert not PATH_RE.search(text), (path, PATH_RE.search(text).group(0))
        assert not SECRET_RE.search(text), path
        assert "Traceback" not in text and "File \"" not in text, path


def test_views_expose_names_counts_and_states_not_definitions(world):
    prepare(world)
    inv = world.call("vic", "GET", f"{P}/inventory").json()
    assert set(inv) == {"completeness", "pipelines", "activity_types", "confidence", "linked_services", "integration_runtimes", "triggers", "states", "state_help",
                        "dependencies", "compute"}
    first = inv["pipelines"][0]
    assert set(first) == {"name", "folder", "activities", "top_level", "calls", "worst_confidence", "unknown_types", "issues", "reconciled", "state", "flags", "reasons"}
    assert all(set(ls) == {"name", "type"} for ls in inv["linked_services"])  # never the connection settings behind them
    comp = world.call("vic", "GET", f"{P}/compatibility").json()
    blob = json.dumps(comp)
    assert "typeProperties" not in blob and "sqlReaderQuery" not in blob and "notebookPath" not in blob and "configuration" not in comp


def test_provisioning_view_uses_opaque_step_refs_and_names_not_resource_ids(world):
    prepare(world)
    view = plan(world, "ann")
    text = json.dumps(view)
    assert all("id" not in s and len(s["ref"]) == 10 for s in view["steps"])
    assert DEMO_WS not in text  # the workspace is shown by its display name
    assert any(s["name"] == "Demo Fabric workspace" for s in view["steps"])
    assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text)


def test_job_results_are_redacted_before_they_leave_the_server(world, monkeypatch):
    prepare(world)
    root = str(world.demo.store.projects_root)

    def fake(c, session, pid, stage, log, options=None):
        log(f"wrote {root}\\x.json")
        return {"message": "ok", "output_path": root, "token": "ghp_abcdefghijklmnopqrstuvwxyz0123456789", "count": 2, "note": f"see {root}\\y.txt"}

    monkeypatch.setattr("adf2fabric.api.app.run_stage", fake)
    j = run(world, "ann", "discovery")
    blob = json.dumps(j)
    assert j["result"]["count"] == 2 and "output_path" not in j["result"] and "token" not in j["result"]
    assert root not in blob and "ghp_" not in blob


# =====================================================================================================================
# 3. failure states are explicit and say what to do
# =====================================================================================================================


def test_views_before_their_prerequisites_say_what_to_run_first(world):
    store = world.demo.store
    store.save(store.load("demo-estate").model_copy(update={"id": "empty-estate"}))  # a project where nothing has run
    E = "/projects/empty-estate"
    r = world.call("vic", "GET", f"{E}/inventory")
    assert r.status_code == 409 and "Discovery has not run" in r.json()["error"]
    assert world.call("vic", "GET", f"{E}/report").status_code == 409
    c = world.call("vic", "GET", f"{E}/compatibility").json()
    assert c["available"] is False and "convert" in c["needs"].lower()
    assert world.call("vic", "GET", f"{E}/validation").json()["available"] is False
    assert world.call("vic", "GET", f"{E}/provisioning").json()["available"] is False


def test_approve_and_apply_refuse_without_a_current_plan_and_approval(world):
    prepare(world)
    assert world.call("abe", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": "0" * 16}).status_code == 409  # no plan
    view = plan(world, "ann")
    ref = view["plan_ref_full"]
    assert world.call("abe", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": "f" * 16}).status_code == 409  # a plan the person did not review
    assert world.call("oscar", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref}).status_code == 409  # nothing approved yet
    assert world.call("abe", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": ref, "steps": ["nonsense000"]}).status_code == 422


def test_apply_needs_a_confirmation_and_a_dry_run_first(world):
    prepare(world)
    view = plan(world, "ann")
    ref = view["plan_ref_full"]
    assert world.call("abe", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": ref}).status_code == 200
    r = world.call("oscar", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref, "apply": True})
    assert r.status_code == 422 and "Confirm" in r.json()["error"]
    r = world.call("oscar", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref, "apply": True, "confirm": True})
    assert r.status_code == 409 and "dry run" in r.json()["error"].lower()
    dry = world.call("oscar", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref, "apply": False}).json()
    assert world.wait("oscar", dry)["state"] == "succeeded"
    v = world.call("oscar", "GET", f"{P}/provisioning").json()
    assert v["run"]["dry_run"] is True and not any(s["result"] == "applied" for s in v["steps"])  # a dry run created nothing
    assert not world.demo.fabric_http.items  # and the (demo) target really is unchanged
    real = world.call("oscar", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref, "apply": True, "confirm": True})
    assert real.status_code == 202
    done = world.wait("oscar", real.json())
    assert done["state"] == "succeeded" and done["result"]["verified"] is True
    assert [i["displayName"] for i in world.demo.fabric_http.items] == ["lh_demo"]


def test_an_approval_is_for_one_plan_only(world):
    prepare(world)
    first = plan(world, "ann")
    world.call("abe", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": first["plan_ref_full"]})
    world.demo.fabric_http.items.append({"id": "x", "displayName": "lh_demo", "type": "Lakehouse"})  # the target changed meanwhile
    second = plan(world, "ann")
    assert second["plan_ref"] != first["plan_ref"]
    assert second["approval"] is None or second["approval"]["current"] is False
    r = world.call("oscar", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": first["plan_ref_full"]})
    assert r.status_code == 409  # the plan that was approved is not the plan on record any more


def test_the_plan_needs_a_workspace_and_only_a_granted_one(world):
    prepare(world)
    world.call("ann", "PUT", "/session/destination", json_body={"provider": "azure_cli"})  # no workspace chosen
    assert world.call("ann", "POST", f"{P}/provisioning/plan", json_body={}).status_code == 422
    _, tok = world.users.create("narrow", "default", ["analyst"], world.users.get("ann").grants.model_copy(update={"workspaces": [DEMO_WS]}))
    world.tokens["narrow"] = tok
    assert world.call("narrow", "POST", f"{P}/provisioning/plan", json_body={"workspace_id": FOREIGN_WS}).status_code == 403
    assert world.call("narrow", "POST", f"{P}/provisioning/plan", json_body={"workspace_name": "Any Workspace"}).status_code == 403  # a name could select any workspace


def test_an_unreadable_target_is_unknown_never_absent_and_nothing_raw_is_shown(world):
    prepare(world)
    from adf2fabric.api.demo_target import _Resp

    real = world.demo.fabric_http.request
    world.demo.fabric_http.request = lambda m, u, **k: _Resp(403, {"errorCode": "Forbidden", "message": f"denied for {world.demo.store.projects_root}"}) if "/items" in u else real(m, u, **k)
    view = plan(world, "ann")
    assert any(s["action"] == "unknown" for s in view["steps"])
    assert str(world.demo.store.projects_root) not in json.dumps(view)


def test_connections_and_identity_need_an_explicit_confirmation(world):
    prepare(world)
    body = {"names": ["LS_Sales_SQL"], "dry_run": False, "workspace_id": DEMO_WS}
    assert world.call("oscar", "POST", f"{P}/connections/apply", json_body=body).status_code == 422
    assert world.call("root", "POST", f"{P}/connections/workspace-identity", json_body={"workspace_id": DEMO_WS}).status_code == 422
    assert world.call("oscar", "POST", f"{P}/connections/apply", json_body={**body, "dry_run": True}).status_code == 200  # a preview needs none


# =====================================================================================================================
# 4. deploy: dry run first, confirmation, honest states
# =====================================================================================================================


def test_deploy_needs_confirmation_and_a_dry_run_of_the_same_pipelines(world):
    prepare(world)
    body = {"workspace_id": DEMO_WS, "allow_unbound": True}
    r = stage(world, "oscar", "deploy_apply", body)
    assert r.status_code == 422 and "Confirm" in r.json()["error"]
    r = stage(world, "oscar", "deploy_apply", {**body, "confirm": True})
    assert r.status_code == 409 and "dry run" in r.json()["error"].lower()
    dry = run(world, "oscar", "deploy_dry_run", {"workspace_id": DEMO_WS, "allow_unbound": True})
    assert dry["state"] == "succeeded" and dry["result"]["applied"] is False and not world.demo.fabric_http.pipes
    other = stage(world, "oscar", "deploy_apply", {**body, "confirm": True, "pipelines": ["Some Other Pipeline"]})
    assert other.status_code in (400, 409)  # a dry run of different pipelines does not license this deploy
    ok = stage(world, "oscar", "deploy_apply", {**body, "confirm": True})
    assert ok.status_code == 202
    done = world.wait("oscar", ok.json())
    assert done["state"] == "succeeded" and done["result"]["applied"] is True
    assert "item_id" not in json.dumps(done)  # the identifier of what was created is not part of an ordinary view
    assert len(world.demo.fabric_http.pipes) >= 1


def test_a_stale_dry_run_does_not_license_a_deploy(world, monkeypatch):
    prepare(world)
    run(world, "oscar", "deploy_dry_run", {"workspace_id": DEMO_WS, "allow_unbound": True})
    real = deploy_record.dry_run_ok
    monkeypatch.setattr(deploy_record, "dry_run_ok", lambda d, w, p, max_age=3600.0, now=None: real(d, w, p, max_age, now=time.time() + 7200))
    r = stage(world, "oscar", "deploy_apply", {"workspace_id": DEMO_WS, "allow_unbound": True, "confirm": True})
    assert r.status_code == 409


def test_states_move_only_on_evidence_and_never_skip_ahead(world):
    prepare(world)
    states = lambda: {p["name"]: p["state"] for p in world.call("vic", "GET", f"{P}/inventory").json()["pipelines"]}  # noqa: E731
    name = next(iter(states()))
    assert states()[name] == "converted"  # generated, and nothing more is claimed
    run(world, "oscar", "deploy_dry_run", {"workspace_id": DEMO_WS, "allow_unbound": True})
    assert states()[name] == "converted"  # a dry run is not a deployment
    stage_r = stage(world, "oscar", "deploy_apply", {"workspace_id": DEMO_WS, "allow_unbound": True, "confirm": True})
    world.wait("oscar", stage_r.json())
    assert states()[name] == "deployed"
    from adf2fabric.agent.deployment import verify as verify_mod

    path = verify_mod.record_path(world.demo.store.project_dir("demo-estate"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({name: {"pipeline": name, "verdict": "differs", "fabric_status": "Completed"}}), encoding="utf-8")
    assert states()[name] == "deployed"  # a run that differs from its baseline is not validation
    path.write_text(json.dumps({name: {"pipeline": name, "verdict": "matches", "fabric_status": "Completed"}}), encoding="utf-8")
    assert states()[name] == "runtime_validated"


def test_the_report_never_claims_success_without_a_passing_runtime_validation(world):
    prepare(world)
    rep = world.call("vic", "GET", f"{P}/report").json()
    assert rep["full_success"] is False and rep["overall"] != "MIGRATION_VERIFIED" and "not" in rep["headline"].lower() or "No validation" in rep["headline"]
    run(world, "ann", "validate", {"validate_stages": ["static"]})
    rep = world.call("vic", "GET", f"{P}/report").json()
    assert rep["full_success"] is False and rep["tone"] in ("warning", "error")
    assert any(u["issue"] for u in rep["unresolved"]) and rep["unresolved_total"] == len(rep["unresolved"]) or rep["unresolved_total"] >= len(rep["unresolved"])
    assert set(rep["states"]) == set(views.STATES)


def test_validation_of_the_deployment_needs_a_granted_workspace(world):
    prepare(world)
    _, tok = world.users.create("narrow2", "default", ["analyst"], world.users.get("ann").grants.model_copy(update={"workspaces": [DEMO_WS]}))
    world.tokens["narrow2"] = tok
    r = world.call("narrow2", "POST", f"{P}/stages/validate", json_body={"validate_stages": ["api", "deployment"], "workspace_id": FOREIGN_WS})
    assert r.status_code == 403
    no_ws = world.call("ann", "PUT", "/session/destination", json_body={"provider": "azure_cli"})
    j = run(world, "ann", "validate", {"validate_stages": ["api"]})
    assert j["state"] == "failed" and "workspace" in j["error"].lower()
    assert no_ws.status_code == 200


# =====================================================================================================================
# 5. progress
# =====================================================================================================================


def test_jobs_report_state_elapsed_time_and_incremental_events(world):
    prepare(world, through="discovery")
    job = stage(world, "ann", "convert").json() if False else stage(world, "ann", "dependencies").json()
    assert job["state"] in ("queued", "running", "succeeded") and "elapsed" in job and "created" in job
    done = world.wait("ann", job)
    assert done["state"] == "succeeded" and done["finished"] and done["elapsed"] >= 0 and done["events"] and done["next_event"] == len(done["events"])
    later = world.call("ann", "GET", f"/jobs/{job['id']}", params={"since": done["next_event"]}).json()
    assert later["events"] == [] and later["next_event"] == done["next_event"]
    assert world.call("ann", "GET", f"/jobs/{job['id']}", params={"since": -5}).json()["events"]  # a negative offset cannot index backwards
    listed = world.call("ann", "GET", "/jobs").json()["jobs"]
    assert listed and all("elapsed" in j for j in listed)


def test_a_failed_job_shows_a_short_actionable_error_not_a_trace(world, monkeypatch):
    prepare(world)

    def boom(*a, **k):
        raise RuntimeError(f"cannot read {world.demo.store.projects_root}\\x: Traceback (most recent call last)")

    monkeypatch.setattr("adf2fabric.api.app.run_stage", boom)
    j = run(world, "ann", "discovery")
    assert j["state"] == "failed" and str(world.demo.store.projects_root) not in j["error"] and len(j["error"]) <= 400


# =====================================================================================================================
# 6. duplicate submissions
# =====================================================================================================================


def test_a_repeated_idempotency_key_returns_the_first_job_and_starts_nothing_new(world):
    prepare(world, through="discovery")
    h = {"Idempotency-Key": "intent-0001-abcdef"}
    first = stage(world, "ann", "dependencies", headers=h)
    again = stage(world, "ann", "dependencies", headers=h)
    assert first.status_code == again.status_code == 202 and first.json()["id"] == again.json()["id"]
    assert again.headers["idempotent-replay"] == "true" and "idempotent-replay" not in first.headers
    world.wait("ann", first.json())
    jobs = [j for j in world.call("ann", "GET", "/jobs").json()["jobs"] if j["kind"] == "dependencies"]
    assert len(jobs) == 1  # one intent, one job
    assert world.call("oscar", "GET", f"/jobs/{first.json()['id']}").status_code == 200


def test_the_same_key_from_another_person_is_a_different_intent(world):
    prepare(world, through="discovery")
    h = {"Idempotency-Key": "shared-key-12345678"}
    a = stage(world, "ann", "dependencies", headers=h)
    world.wait("ann", a.json())
    b = stage(world, "oscar", "dependencies", headers=h)
    assert b.status_code == 202 and b.json()["id"] != a.json()["id"] and "idempotent-replay" not in b.headers


@pytest.mark.parametrize("key", ["short", "has space in it 123", "../../etc/passwd1", "x" * 100, "bad;key;1234567"])
def test_malformed_idempotency_keys_are_refused(world, key):
    prepare(world, through="discovery")
    assert stage(world, "ann", "dependencies", headers={"Idempotency-Key": key}).status_code == 400


def test_a_second_job_while_one_runs_is_refused_not_queued(world, monkeypatch):
    prepare(world, through="discovery")
    release = []

    def slow(c, session, pid, stage_name, log, options=None):
        log("working")
        while not release:
            time.sleep(0.01)
        return {"message": "done"}

    monkeypatch.setattr("adf2fabric.api.app.run_stage", slow)
    first = stage(world, "ann", "dependencies", headers={"Idempotency-Key": "first-intent-0001"})
    assert first.status_code == 202
    second = stage(world, "oscar", "convert", headers={"Idempotency-Key": "second-intent-0002"})
    assert second.status_code == 409 and "already running" in second.json()["error"]
    replay = stage(world, "ann", "dependencies", headers={"Idempotency-Key": "first-intent-0001"})  # the first click again: the same job, not an error
    assert replay.status_code == 202 and replay.json()["id"] == first.json()["id"]
    release.append(1)
    world.wait("ann", first.json())
    assert stage(world, "oscar", "convert", headers={"Idempotency-Key": "third-intent-0003"}).status_code == 202


def test_provisioning_apply_is_idempotent_too(world):
    prepare(world)
    view = plan(world, "ann")
    ref = view["plan_ref_full"]
    world.call("abe", "POST", f"{P}/provisioning/approve", json_body={"plan_ref": ref})
    h = {"Idempotency-Key": "dry-run-intent-0001"}
    a = world.call("oscar", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref}, headers=h)
    b = world.call("oscar", "POST", f"{P}/provisioning/apply", json_body={"plan_ref": ref}, headers=h)
    assert a.json()["id"] == b.json()["id"] and b.headers["idempotent-replay"] == "true"


# =====================================================================================================================
# 7. production mode: no demo data, no stand-in Fabric
# =====================================================================================================================


def test_production_mode_refuses_demo_and_serves_only_real_state(tmp_path, monkeypatch):
    from adf2fabric.project.store import ProjectStore

    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    store = ProjectStore(tmp_path / "projects")
    live = Container(Settings(projects_dir=tmp_path / "projects"), store=store)  # the real container: no canned providers, no stand-in Fabric
    app = create_app(Settings(), live=live, demo=None, security=SecuritySettings(mode="disabled", production=True))
    c = TestClient(app, base_url="http://localhost", raise_server_exceptions=False)
    h = {"X-Session-Id": "p1"}
    assert c.get("/me", headers=h).json()["production"] is True and c.get("/me", headers=h).json()["demo_available"] is False
    r = c.post("/mode", json={"mode": "demo"}, headers=h)
    assert r.status_code == 403 and "production" in r.json()["error"]
    assert c.get("/mode", headers=h).json()["mode"] == "live"
    assert c.get("/projects", headers=h).json()["projects"] == []  # no sample project appears
    assert c.get("/projects/demo-estate", headers=h).status_code == 404
    assert live.fabric_http is None  # nothing in this container can answer for Fabric except Fabric
    assert app.state.containers["demo"] is None


def test_demo_mode_is_labelled_by_the_api_everywhere_it_applies(world):
    world.call("ann", "POST", "/mode", json_body={"mode": "demo"})
    assert world.call("ann", "GET", "/session").json()["mode"] == "demo"
    assert world.call("ann", "GET", "/mode").json()["mode"] == "demo"
    assert world.call("bea", "GET", "/mode").json()["mode"] == "live"  # one person's demo switch never changes another's


def test_other_people_never_see_data_from_a_demo_session_they_did_not_start(world):
    world.call("ann", "POST", "/mode", json_body={"mode": "demo"})
    assert world.call("bea", "GET", "/projects").json()["projects"] == []  # another organisation, live mode: nothing
