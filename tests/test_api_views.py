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


# =====================================================================================================================
# 8. running pipelines to prove them: opt-in, authorised, confirmed, audited
# =====================================================================================================================


def _deployed(world):
    prepare(world)
    run(world, "oscar", "deploy_dry_run", {"workspace_id": DEMO_WS, "allow_unbound": True})
    world.wait("oscar", stage(world, "oscar", "deploy_apply", {"workspace_id": DEMO_WS, "allow_unbound": True, "confirm": True}).json())
    return next(p["name"] for p in world.call("vic", "GET", f"{P}/inventory").json()["pipelines"] if p["state"] == "deployed")


def test_starting_runs_needs_execute_permission_a_confirmation_and_named_pipelines(world):
    name = _deployed(world)
    body = {"workspace_id": DEMO_WS, "run": True, "pipelines": [name]}
    assert stage(world, "ann", "verify", {**body, "confirm": True}).status_code == 403  # an analyst may compare, not start runs
    assert stage(world, "abe", "verify", {**body, "confirm": True}).status_code == 403
    r = stage(world, "oscar", "verify", body)
    assert r.status_code == 422 and "Confirm" in r.json()["error"]
    r = stage(world, "oscar", "verify", {"workspace_id": DEMO_WS, "run": True, "confirm": True})
    assert r.status_code == 422 and "Name the pipelines" in r.json()["error"]  # never "run everything" by default
    assert stage(world, "oscar", "verify", {**body, "confirm": True, "pipelines": [f"P{i}" for i in range(21)]}).status_code == 422
    assert stage(world, "oscar", "deploy_dry_run", {**body, "confirm": True}).status_code == 422  # run belongs to verify only


def test_a_confirmed_run_starts_the_pipeline_compares_it_and_is_audited(world):
    name = _deployed(world)
    before = len(world.demo.fabric_http.runs)
    j = run(world, "oscar", "verify", {"workspace_id": DEMO_WS, "run": True, "confirm": True, "pipelines": [name]})
    assert j["state"] == "succeeded" and len(world.demo.fabric_http.runs) == before + 1  # exactly one run was started
    res = j["result"]["results"][0]
    assert res["pipeline"] == name and res["fabric_status"] == "Completed"
    assert res["verdict"] != "matches"  # the demo has no ADF baseline: a completed run alone is not proof
    state = next(p["state"] for p in world.call("vic", "GET", f"{P}/inventory").json()["pipelines"] if p["name"] == name)
    assert state == "deployed"  # not runtime_validated without a matching baseline
    ev = [e for e in world.audit.read(500) if e["action"] == "verify.run"]
    assert ev and ev[-1]["actor"] == "oscar" and ev[-1]["detail"]["pipelines"] == 1


def test_comparing_without_run_starts_nothing(world):
    name = _deployed(world)
    before = len(world.demo.fabric_http.runs)
    j = run(world, "ann", "verify", {"workspace_id": DEMO_WS, "pipelines": [name]})
    assert j["state"] == "succeeded" and len(world.demo.fabric_http.runs) == before


def test_repeated_names_start_one_run_and_unknown_names_are_refused(world):
    name = _deployed(world)
    before = len(world.demo.fabric_http.runs)
    j = run(world, "oscar", "verify", {"workspace_id": DEMO_WS, "run": True, "confirm": True, "pipelines": [name, name.lower(), f" {name} "]})
    assert j["state"] == "succeeded" and len(world.demo.fabric_http.runs) == before + 1
    r = stage(world, "oscar", "verify", {"workspace_id": DEMO_WS, "run": True, "confirm": True, "pipelines": ["NotAPipeline"]})
    assert r.status_code == 422 and "Not converted" in r.json()["error"]
    assert stage(world, "oscar", "verify", {"workspace_id": DEMO_WS, "run": True, "confirm": True, "pipelines": [name], "run_timeout": 7200}).status_code == 422


def test_a_replayed_run_request_is_not_audited_twice(world):
    name = _deployed(world)
    h = {"Idempotency-Key": "run-intent-000001"}
    body = {"workspace_id": DEMO_WS, "run": True, "confirm": True, "pipelines": [name]}
    a = stage(world, "oscar", "verify", body, headers=h)
    world.wait("oscar", a.json())
    b = stage(world, "oscar", "verify", body, headers=h)
    assert b.headers["idempotent-replay"] == "true"
    assert len([e for e in world.audit.read(500) if e["action"] == "verify.run"]) == 1


# =====================================================================================================================
# 9. the consultation brief: who may answer, submit and approve; the run follows only an approved, current brief
# =====================================================================================================================

B = f"{P}/brief"


def _fill_brief(w: World, who: str = "ann") -> dict:
    w.call(who, "POST", f"{B}/accept")
    for _ in range(5):
        v = w.call(who, "GET", B).json()
        open_ = {q["id"]: (("LH_Test" if q["id"] == "storage.lakehouse" else "Owner, platform lead") if q["kind"] == "text" else [q["options"][0]["value"]] if q["kind"] == "multi" else q["options"][0]["value"])
                 for q in v["questions"] if q["required"] and q["source"] == "open"}
        if not open_:
            return v
        assert w.call(who, "PUT", f"{B}/answers", json_body={"answers": open_}).status_code == 200
        w.call(who, "POST", f"{B}/accept")
    return w.call(who, "GET", B).json()


def test_the_brief_needs_discovery_and_stays_inside_the_organisation(world):
    store = world.demo.store
    store.save(store.load("demo-estate").model_copy(update={"id": "empty-estate"}))
    assert world.call("vic", "GET", "/projects/empty-estate/brief").status_code == 409
    prepare(world, through="discovery")
    assert world.call("vic", "GET", B).status_code == 200
    assert world.call("bob", "GET", B).status_code == 404
    assert world.call(None, "GET", B).status_code == 401


def test_analysts_answer_and_submit_approvers_approve_viewers_only_read(world):
    prepare(world, through="discovery")
    assert world.call("vic", "PUT", f"{B}/answers", json_body={"answers": {"scope.approach": "phased"}}).status_code == 403
    v = _fill_brief(world, "ann")
    assert v["counts"]["need_person"] == 0
    assert world.call("abe", "POST", f"{B}/submit").status_code == 403  # an approver does not submit
    sub = world.call("ann", "POST", f"{B}/submit")
    assert sub.status_code == 200 and sub.json()["state"] == "submitted"
    fp = sub.json()["fingerprint"]
    assert world.call("ann", "POST", f"{B}/approve", json_body={"fingerprint": fp}).status_code == 403  # nor does the analyst approve
    assert world.call("oscar", "POST", f"{B}/approve", json_body={"fingerprint": fp}).status_code == 403
    ok = world.call("abe", "POST", f"{B}/approve", json_body={"fingerprint": fp})
    assert ok.status_code == 200 and ok.json()["state"] == "approved"
    assert ok.json()["current_version"]["approved_by"] == "abe"  # the signed-in approver, never a typed name
    acts = [e["action"] for e in world.audit.read(500)]
    assert {"brief.answers", "brief.submit", "brief.approve"} <= set(acts)


def test_bad_answers_and_stale_approvals_are_refused(world):
    prepare(world, through="discovery")
    for body in ({"answers": {"scope.mode": "everything"}}, {"answers": {"made.up": "x"}}, {"answers": {"scope.pipelines": ["NotAPipeline"]}}):
        r = world.call("ann", "PUT", f"{B}/answers", json_body=body)
        assert r.status_code == 422, body
    assert world.call("ann", "POST", f"{B}/submit").status_code == 409  # required questions still open
    v = _fill_brief(world, "ann")
    world.call("ann", "POST", f"{B}/submit")
    world.call("ann", "PUT", f"{B}/answers", json_body={"answers": {"scope.approach": "parallel" if v["questions"][0] else "phased"}})
    r = world.call("abe", "POST", f"{B}/approve", json_body={"fingerprint": v["fingerprint"]})
    assert r.status_code == 409  # the answers changed after submission: approve what was read, or nothing


def test_the_brief_view_holds_no_paths_or_secrets(world):
    prepare(world, through="discovery")
    root = str(world.demo.store.projects_root)
    for path in (B, f"{B}/markdown", f"{B}/questionnaire"):
        text = world.call("vic", "GET", path).text
        assert root not in text and not PATH_RE.search(text) and not SECRET_RE.search(text), path


def test_conversion_follows_only_an_approved_current_brief(world):
    prepare(world, through="semantic")
    names = sorted(p["name"] for p in world.call("vic", "GET", f"{P}/inventory").json()["pipelines"]) if world.call("vic", "GET", f"{P}/inventory").status_code == 200 else []
    v = _fill_brief(world, "ann")
    pipes = next(q for q in v["questions"] if q["id"] == "scope.mode")["options"]
    world.call("ann", "PUT", f"{B}/answers", json_body={"answers": {"scope.mode": "selected"}})
    v = world.call("ann", "GET", B).json()
    first = next(q for q in v["questions"] if q["id"] == "scope.pipelines")["options"][0]["value"]
    world.call("ann", "PUT", f"{B}/answers", json_body={"answers": {"scope.pipelines": [first]}})
    fp = world.call("ann", "POST", f"{B}/submit").json()["fingerprint"]
    conv = run(world, "ann", "convert")
    assert conv["state"] == "succeeded" and conv["result"].get("brief") is None  # submitted is not approved: nothing is followed
    world.call("abe", "POST", f"{B}/approve", json_body={"fingerprint": fp})
    conv = run(world, "ann", "convert")
    assert conv["state"] == "succeeded" and conv["result"]["brief"] == 1
    assert [p.source_pipeline for p in world.demo.store.load_conversion("demo-estate").pipelines] == [first]
    assert world.call("vic", "GET", B).json()["state"] == "approved"  # carrying out the brief never takes its approval away
    assert run(world, "ann", "convert")["result"]["brief"] == 1
    assert pipes and (not names or first in names)


def test_real_runs_are_refused_when_the_approved_brief_does_not_allow_them(world):
    name = _deployed(world)
    _fill_brief(world, "ann")
    world.call("ann", "PUT", f"{B}/answers", json_body={"answers": {"validation.depth": "evidence"}})
    fp = world.call("ann", "POST", f"{B}/submit").json()["fingerprint"]
    world.call("abe", "POST", f"{B}/approve", json_body={"fingerprint": fp})
    r = stage(world, "oscar", "verify", {"workspace_id": DEMO_WS, "run": True, "confirm": True, "pipelines": [name]})
    assert r.status_code == 409 and "brief" in r.json()["error"]
    world.call("ann", "PUT", f"{B}/answers", json_body={"answers": {"validation.depth": "run"}})
    fp = world.call("ann", "POST", f"{B}/submit").json()["fingerprint"]
    world.call("abe", "POST", f"{B}/approve", json_body={"fingerprint": fp})
    assert stage(world, "oscar", "verify", {"workspace_id": DEMO_WS, "run": True, "confirm": True, "pipelines": [name]}).status_code == 202


def test_a_key_may_repeat_only_the_request_it_was_first_used_for(world):
    prepare(world, through="discovery")
    h = {"Idempotency-Key": "reused-key-000001"}
    first = stage(world, "ann", "dependencies", headers=h)
    world.wait("ann", first.json())
    again = stage(world, "ann", "dependencies", headers=h)  # the same request: the same job
    assert again.status_code == 202 and again.json()["id"] == first.json()["id"]
    other = stage(world, "ann", "convert", headers=h)  # a different request under the same key is refused, never answered with the old job
    assert other.status_code == 422 and "different request" in other.json()["error"]
    other_body = stage(world, "ann", "dependencies", {"pipelines": ["PL_X"]}, headers=h)
    assert other_body.status_code == 422
    jobs = [j for j in world.call("ann", "GET", "/jobs").json()["jobs"] if j["kind"] == "convert"]
    assert jobs == []  # and nothing was started


def test_deploy_and_real_runs_are_refused_for_another_workspace_than_the_brief_was_agreed_for(world):
    name = _deployed(world)
    _fill_brief(world, "ann")
    world.call("ann", "PUT", f"{B}/answers", json_body={"answers": {"validation.depth": "run"}})
    fp = world.call("ann", "POST", f"{B}/submit").json()["fingerprint"]
    world.call("abe", "POST", f"{B}/approve", json_body={"fingerprint": fp})
    other = "99999999-9999-9999-9999-999999999999"
    world.users.get("oscar")  # oscar's grants are open (ALL): the brief is what stops this, not a grant
    for body in ({"workspace_id": other, "confirm": True, "allow_unbound": True}, ):
        r = stage(world, "oscar", "deploy_apply", body)
        assert r.status_code == 409 and "different Fabric workspace" in r.json()["error"] and other not in r.json()["error"]
    r = stage(world, "oscar", "verify", {"workspace_id": other, "run": True, "confirm": True, "pipelines": [name]})
    assert r.status_code == 409 and "different Fabric workspace" in r.json()["error"]
    # a dry run reads only: it is never refused for this
    assert stage(world, "oscar", "deploy_dry_run", {"workspace_id": other, "allow_unbound": True}).status_code in (202, 403, 409)
    v = world.call("oscar", "GET", B).json()
    assert v["state"] == "approved" and not PATH_RE.search(json.dumps(v)) and DEMO_WS not in json.dumps(v)


def test_confirmed_steps_are_kept_on_the_server_per_project_and_only_known_steps(world):
    prepare(world, through="discovery")
    J = f"{P}/journey"
    assert world.call("vic", "GET", J).json() == {"confirmed": []}
    assert world.call("vic", "PUT", J, json_body={"steps": ["discover"]}).status_code == 403  # a viewer reads, never confirms
    assert world.call("ann", "PUT", J, json_body={"steps": ["review", "discover"]}).json() == {"confirmed": ["discover", "review"]}
    assert world.call("ann", "PUT", J, json_body={"steps": ["bogus"]}).status_code == 422
    assert world.call("oscar", "GET", J).json() == {"confirmed": ["discover", "review"]}  # another person, another browser: the same walk-through
    assert world.call("bob", "GET", J).status_code == 404 and world.call(None, "GET", J).status_code == 401
    assert world.call("ann", "POST", f"{J}/reset", json_body={"steps": ["review"]}).json() == {"confirmed": ["discover"]}
    assert {"journey.confirm", "journey.reset"} <= {e["action"] for e in world.audit.read(500)}


def test_a_brief_that_holds_back_every_pipeline_never_becomes_convert_everything(world, monkeypatch):
    prepare(world, through="semantic")
    monkeypatch.setattr("adf2fabric.api.stages._approved_brief", lambda c, s, pid, log: {"pipelines": [], "version": 1, "lakehouse": None})
    j = run(world, "ann", "convert")
    assert j["state"] == "failed" and "no pipeline to convert" in j["error"]
    assert not world.demo.store.has_conversion("demo-estate")  # nothing was converted
    j = run(world, "ann", "convert", {"pipelines": [next(p["name"] for p in world.call("vic", "GET", f"{P}/inventory").json()["pipelines"])]})
    assert j["state"] == "succeeded"  # an explicit choice by the person still works


# =====================================================================================================================
# 10. waves and execution order
# =====================================================================================================================


def test_waves_need_discovery_then_the_dependencies_and_stay_inside_the_organisation(world):
    store = world.demo.store
    store.save(store.load("demo-estate").model_copy(update={"id": "empty-estate"}))
    assert world.call("vic", "GET", "/projects/empty-estate/waves").status_code == 409
    prepare(world, through="semantic")
    v = world.call("vic", "GET", f"{P}/waves").json()
    assert v["available"] is False and "Resolve dependencies" in v["needs"]  # asked for, not guessed
    run(world, "ann", "dependencies")
    assert world.call("vic", "GET", f"{P}/waves").status_code == 200
    assert world.call("bob", "GET", f"{P}/waves").status_code == 404 and world.call(None, "GET", f"{P}/waves").status_code == 401


def test_the_order_puts_every_pipeline_after_the_ones_it_calls_and_needs_mirror_needed_by(world):
    prepare(world, through="semantic")
    run(world, "ann", "dependencies")
    v = world.call("vic", "GET", f"{P}/waves").json()
    assert v["stats"]["waves"] == len(v["waves"]) and v["stats"]["objects"] == len(v["nodes"])
    assert [w["number"] for w in v["waves"]] == sorted(w["number"] for w in v["waves"])
    pos = {o["pipeline"]: o["position"] for o in v["order"]}
    assert sorted(pos.values()) == list(range(1, len(pos) + 1))
    for o in v["order"]:
        assert all(pos[c] < o["position"] for c in o["calls"] if c in pos), o  # children first
    by_name = {(n["type"], n["name"]): n for n in v["nodes"]}
    for n in v["nodes"]:
        for other in n["needs"]:  # what A needs lists A among what the other is needed by
            peers = [m for m in v["nodes"] if m["name"] == other and n["name"] in m["needed_by"]]
            assert peers, (n["name"], other)
    assert all(t["count"] >= 1 for w in v["waves"] for t in w["types"]) and sum(w["count"] for w in v["waves"]) >= len(v["order"])


def test_the_waves_view_holds_no_paths_secrets_or_ids(world):
    prepare(world, through="semantic")
    run(world, "ann", "dependencies")
    text = world.call("vic", "GET", f"{P}/waves").text
    assert str(world.demo.store.projects_root) not in text and not PATH_RE.search(text) and not SECRET_RE.search(text) and DEMO_WS not in text


def test_convert_takes_a_persons_choices_over_the_briefs_and_checks_them(world):
    prepare(world, through="semantic")
    assert stage(world, "ann", "convert", {"lakehouse": "Dana Lee, lead"}).status_code == 422  # a name, not a sentence
    assert stage(world, "ann", "convert", {"lakehouse": "LH_X../x"}).status_code in (202, 422)
    assert stage(world, "ann", "deploy_dry_run", {"lakehouse": "LH_X"}).status_code == 422  # convert-only choices
    assert stage(world, "ann", "validate", {"apply_reconstruction": True}).status_code == 422
    j = run(world, "ann", "convert", {"lakehouse": "LH_Chosen"})
    assert j["state"] == "succeeded" and j["result"]["lakehouse"] == "LH_Chosen"
    j = run(world, "ann", "convert", {"lakehouse": ""})  # "keep connections" is a choice too
    assert j["result"]["lakehouse"] is None
    assert run(world, "ann", "convert", {"apply_reconstruction": False})["state"] == "succeeded"


def test_an_explicit_choice_beats_the_approved_brief_and_none_follows_it(world, monkeypatch):
    prepare(world, through="semantic")
    seen = []
    real = __import__("adf2fabric.api.stages", fromlist=["x"])
    monkeypatch.setattr(real, "_approved_brief", lambda c, s, pid, log: {"pipelines": None, "version": 7, "lakehouse": "LH_Brief", "apply_reconstruction": False})
    orig = real._write_storage_map
    monkeypatch.setattr(real, "_write_storage_map", lambda c, pid, lh: (seen.append(lh), orig(c, pid, lh))[1])
    assert run(world, "ann", "convert")["result"]["lakehouse"] == "LH_Brief"  # nothing chosen: the brief's value
    assert run(world, "ann", "convert", {"lakehouse": "LH_Mine"})["result"]["lakehouse"] == "LH_Mine"  # chosen: the person's
    assert run(world, "ann", "convert", {"lakehouse": ""})["result"]["lakehouse"] is None  # "none" is chosen too
    assert seen == ["LH_Brief", "LH_Mine"]
