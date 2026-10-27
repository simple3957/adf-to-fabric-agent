"""Creating the proposed Spark pools and environments, against a fake REST session (no network, no credentials)."""

from __future__ import annotations

import base64

import pytest
import yaml

from adf2fabric.agent.deployment import compute_deploy as cd
from adf2fabric.agent.deployment import fabric_deploy as fd
from adf2fabric.project.compute import ComputePlan, FabricEnvironmentProposal, FabricPoolProposal
from tests.test_fabric_deploy import Fake, Resp

WS = "11111111-1111-1111-1111-111111111111"
POOLS = ("GET", f"/workspaces/{WS}/spark/pools")
NEW_POOL = ("POST", f"/workspaces/{WS}/spark/pools")
ENVS = ("GET", f"/workspaces/{WS}/environments")
NEW_ENV = ("POST", f"/workspaces/{WS}/environments")


def body(name="ADF-Medium-n3-7", size="Medium", lo=3, hi=7):
    scaling = lo != hi
    return {"name": name, "nodeFamily": "MemoryOptimized", "nodeSize": size,
            "autoScale": {"enabled": True, "minNodeCount": lo, "maxNodeCount": hi},
            "dynamicExecutorAllocation": {"enabled": scaling, "minExecutors": max(1, lo - 1), "maxExecutors": max(1, hi - 1)}}


def pool(name="ADF-Medium-n3-7", **kw):
    b = body(name, **kw)
    return FabricPoolProposal(name=name, status="proposed", node_size=b["nodeSize"], request_body=b, replaces=["c1"])


def env(name="ENV-1.3-1", pool_name="ADF-Medium-n3-7", libs=("pypi:requests==2.31.0",), props=None):
    return FabricEnvironmentProposal(name=name, runtime_version="1.3", runtime_spark_version="3.5.5", pool=pool_name,
                                     spark_properties=props or {"spark.sql.shuffle.partitions": "64"},
                                     libraries=list(libs), replaces=["c1"])


PLAN = ComputePlan(pools=[pool()], environments=[env()])


def client(script):
    s = Fake(script)
    return fd.FabricClient("TOKEN", session=s, sleep=lambda _: None), s


def writes(s):
    return [(m, u) for m, u, *_ in s.calls if m != "GET"]


def test_dry_run_only_lists_and_never_writes():
    c, s = client({POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=False)
    assert [(r.kind, r.action) for r in res] == [("pool", "create"), ("environment", "create")]
    assert writes(s) == []


def test_apply_creates_the_pool_then_an_environment_pointing_at_it():
    c, s = client({
        POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})],
        NEW_POOL: [Resp(201, {"id": "pool-1", "name": "ADF-Medium-n3-7"})],
        NEW_ENV: [Resp(201, {"id": "env-1", "displayName": "ENV-1.3-1"})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True)
    assert [(r.kind, r.action, r.item_id) for r in res] == [("pool", "create", "pool-1"), ("environment", "create", "env-1")]
    pool_call = next(b for m, u, b, _ in s.calls if m == "POST" and u.endswith("/spark/pools"))
    assert pool_call == body()  # exactly the documented request body
    env_call = next(b for m, u, b, _ in s.calls if m == "POST" and u.endswith("/environments"))
    assert env_call["displayName"] == "ENV-1.3-1"
    parts = {p["path"]: yaml.safe_load(base64.b64decode(p["payload"])) for p in env_call["definition"]["parts"]}
    assert parts["Setting/Sparkcompute.yml"] == {
        "enable_native_execution_engine": False, "instance_pool_id": "pool-1", "driver_cores": 8, "driver_memory": "56g",
        "executor_cores": 8, "executor_memory": "56g",
        "dynamic_executor_allocation": {"enabled": True, "min_executors": 2, "max_executors": 6},
        "spark_conf": {"spark.sql.shuffle.partitions": "64"}, "runtime_version": "1.3"}
    assert parts["Libraries/PublicLibraries/environment.yml"] == {"dependencies": [{"pip": ["requests==2.31.0"]}]}
    assert all(p["payloadType"] == "InlineBase64" for p in env_call["definition"]["parts"])
    assert all(h["Authorization"] == "Bearer TOKEN" for *_, h in s.calls)
    assert not any(m == "POST" and "publish" in u for m, u, *_ in s.calls)  # publishing is opt-in


def test_runtime_version_is_written_as_a_string():
    text = base64.b64decode(cd.environment_definition(env(), pool(), "pid")["parts"][0]["payload"]).decode()
    assert "runtime_version: '1.3'" in text


def test_only_what_is_named_is_created():
    plan = ComputePlan(pools=[pool(), pool("ADF-Large-n3-9", size="Large", lo=3, hi=9)], environments=[])
    c, s = client({POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})],
                   NEW_POOL: [Resp(201, {"id": "p2"})]})
    res = cd.deploy_compute(c, WS, plan, apply=True, pools={"adf-large-n3-9"}, environments=set())
    assert {(r.name, r.action) for r in res} == {("ADF-Medium-n3-7", "not_selected"), ("ADF-Large-n3-9", "create")}
    assert len(writes(s)) == 1


def test_an_existing_pool_is_left_alone_and_a_different_one_is_a_conflict():
    same = {"id": "p1", "name": "ADF-Medium-n3-7", **{k: body()[k] for k in
            ("nodeFamily", "nodeSize", "autoScale", "dynamicExecutorAllocation")}}
    c, s = client({POOLS: [Resp(200, {"value": [same]})], ENVS: [Resp(200, {"value": []})],
                   NEW_ENV: [Resp(201, {"id": "e1"})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True)
    assert [(r.kind, r.action) for r in res] == [("pool", "exists"), ("environment", "create")]
    assert not any(u.endswith("/spark/pools") for m, u in writes(s))  # the pool was not created again

    different = dict(same, nodeSize="Large")
    c, s = client({POOLS: [Resp(200, {"value": [different]})], ENVS: [Resp(200, {"value": []})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True)
    assert res[0].action == "conflict" and "not modified" in res[0].detail
    assert res[1].action == "refused" and "unavailable" in res[1].detail
    assert writes(s) == []


def test_an_existing_environment_is_not_overwritten():
    c, s = client({POOLS: [Resp(200, {"value": []})], NEW_POOL: [Resp(201, {"id": "p1"})],
                   ENVS: [Resp(200, {"value": [{"id": "e9", "displayName": "env-1.3-1"}]})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True)
    assert res[1].action == "exists" and res[1].item_id == "e9"
    assert not any(u.endswith("/environments") for m, u in writes(s))


def test_an_environment_needs_its_pool_to_be_selected_or_present():
    c, s = client({POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True, pools=set(), environments={"env-1.3-1"})
    env_result = next(r for r in res if r.kind == "environment")
    assert env_result.action == "refused" and "select the pool" in env_result.detail
    assert writes(s) == []


def test_unsized_and_impossible_proposals_are_never_created():
    plan = ComputePlan(pools=[FabricPoolProposal(name="(needs sizing: x)", status="needs_input", replaces=["x"]),
                              FabricPoolProposal(name="(no equivalent: y)", status="not_possible", replaces=["y"])],
                       environments=[env(pool_name=None)])
    c, s = client({POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})]})
    res = cd.deploy_compute(c, WS, plan, apply=True)
    assert {r.action for r in res} == {"skipped"} and writes(s) == []


@pytest.mark.parametrize("change,fragment", [
    ({"name": "bad/name"}, "name must be"), ({"name": "Starter Pool"}, "reserved"),
    ({"nodeFamily": "GpuOptimized"}, "nodeFamily"), ({"nodeSize": "Huge"}, "nodeSize"),
    ({"autoScale": {"enabled": True, "minNodeCount": 5, "maxNodeCount": 2}}, "greater than"),
    ({"autoScale": {"enabled": True, "minNodeCount": 0, "maxNodeCount": 2}}, "at least 1"),
    ({"dynamicExecutorAllocation": {"minExecutors": 1, "maxExecutors": 1}}, "enabled is required")])
def test_a_pool_body_that_breaks_the_documented_schema_is_refused(change, fragment):
    bad = {**body(), **change}
    assert any(fragment in p for p in cd.validate_pool_body(bad))
    plan = ComputePlan(pools=[FabricPoolProposal(name="P", status="proposed", request_body=bad)], environments=[])
    c, s = client({POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})]})
    res = cd.deploy_compute(c, WS, plan, apply=True)
    assert res[0].action == "refused" and writes(s) == []


def test_a_failed_pool_stops_its_environment_but_not_the_run():
    c, s = client({POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})],
                   NEW_POOL: [Resp(403, {"errorCode": "Forbidden", "message": "admin role required"})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True)
    assert res[0].action == "failed" and "admin role" in res[0].detail
    assert res[1].action == "refused" and "creation failed" in res[1].detail


def test_environment_creation_that_is_a_long_running_operation_resolves_its_id_by_name():
    op = f"{fd.FABRIC_API}/operations/op-1"
    c, s = client({
        POOLS: [Resp(200, {"value": []})], NEW_POOL: [Resp(201, {"id": "p1"})],
        ENVS: [Resp(200, {"value": []}), Resp(200, {"value": [{"id": "e-lro", "displayName": "ENV-1.3-1"}]})],
        NEW_ENV: [Resp(202, {}, {"Location": op, "Retry-After": "1"})],
        ("GET", "/operations/op-1"): [Resp(200, {"status": "Succeeded"})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True)
    assert res[1].action == "create" and res[1].item_id == "e-lro"


def test_publish_is_opt_in_and_uses_the_release_api():
    c, s = client({POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})],
                   NEW_POOL: [Resp(201, {"id": "p1"})], NEW_ENV: [Resp(201, {"id": "e1"})],
                   ("POST", f"/workspaces/{WS}/environments/e1/staging/publish"): [Resp(200, {})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True, publish=True)
    assert res[-1].kind == "publish" and res[-1].action == "publish"
    url = next(u for m, u, *_ in s.calls if m == "POST" and "publish" in u)
    assert url.endswith("/staging/publish?beta=false")


def test_a_bad_workspace_id_is_refused_before_any_call():
    c, s = client({})
    with pytest.raises(fd.DeployError):
        cd.deploy_compute(c, "not-a-guid", PLAN, apply=True)
    assert s.calls == []


def test_the_fixture_plan_produces_valid_pool_bodies(tmp_path):
    from tests.test_compute_plan import _convert_estate

    _, _, conv = _convert_estate(tmp_path, with_sources=True)
    proposed = [p for p in conv.compute_plan.pools if p.status == "proposed"]
    assert proposed and all(cd.validate_pool_body(p.request_body) == [] for p in proposed)
    for e in conv.compute_plan.environments:
        pool_ = next(p for p in proposed if p.name == e.pool)
        parts = cd.environment_definition(e, pool_, "pid")["parts"]
        compute = yaml.safe_load(base64.b64decode(parts[0]["payload"]))
        assert compute["driver_cores"] == pool_.vcores_per_node and compute["runtime_version"] == e.runtime_version


# --- the CLI and its approval gate ----------------------------------------------------------------------------------


@pytest.fixture
def estate(tmp_path, monkeypatch):
    from adf2fabric import cli as cli_mod
    from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
    from tests.test_adf_semantic_discovery import _make_project
    from tests.test_compute_plan import FIXTURE, _estate_files

    store, pid = _make_project(tmp_path, _estate_files())
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    return cli_mod, store, str(FIXTURE / "compute-sources.json")


def run_cli(cli_mod, *args):
    from click.testing import CliRunner

    return CliRunner().invoke(cli_mod.cli, list(args))


def fake_fabric(monkeypatch, script):
    """Replace sign-in and the REST client so the CLI talks to a fake workspace."""
    from adf2fabric.connectivity import _azure

    class Cred:
        def get_token(self, scope):
            return type("T", (), {"token": "TOKEN"})()

    sessions = []
    monkeypatch.setattr(_azure, "build_credential", lambda ref: (Cred(), None))
    real = fd.FabricClient

    def make(token, **kw):
        s = Fake(script)
        sessions.append(s)
        return real(token, session=s, sleep=lambda _: None)

    monkeypatch.setattr(fd, "FabricClient", make)
    return sessions


def test_convert_cli_reports_the_compute_plan(estate):
    cli_mod, store, sources = estate
    out = run_cli(cli_mod, "convert", "--project", "test-proj", "--compute-sources", sources)
    assert out.exit_code == 0, out.output
    assert "spark compute: 4 in use" in out.output and "ADF-Large-n3-9: proposed" in out.output
    assert (store.conversion_dir("test-proj") / "compute-plan.json").exists()


def test_apply_without_approval_is_refused_before_signing_in(estate, monkeypatch):
    cli_mod, store, sources = estate
    run_cli(cli_mod, "convert", "--project", "test-proj", "--compute-sources", sources)
    sessions = fake_fabric(monkeypatch, {})
    out = run_cli(cli_mod, "compute-deploy", "--project", "test-proj", "--workspace-id", WS, "--apply")
    assert out.exit_code == 1 and "needs you to approve" in out.output and sessions == []


def test_an_unknown_pool_name_is_refused_and_lists_the_choices(estate, monkeypatch):
    cli_mod, store, sources = estate
    run_cli(cli_mod, "convert", "--project", "test-proj", "--compute-sources", sources)
    fake_fabric(monkeypatch, {})
    out = run_cli(cli_mod, "compute-deploy", "--project", "test-proj", "--workspace-id", WS, "--pool", "nope", "--apply")
    assert out.exit_code == 1 and "not in the plan: nope" in out.output and "ADF-Large-n3-9" in out.output


def test_dry_run_shows_everything_and_changes_nothing(estate, monkeypatch):
    cli_mod, store, sources = estate
    run_cli(cli_mod, "convert", "--project", "test-proj", "--compute-sources", sources)
    sessions = fake_fabric(monkeypatch, {POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})]})
    out = run_cli(cli_mod, "compute-deploy", "--project", "test-proj", "--workspace-id", WS)
    assert out.exit_code == 0, out.output
    assert "dry run - nothing changed" in out.output and "pool ADF-Large-n3-9: create" in out.output
    assert writes(sessions[0]) == []


def test_approved_pool_is_created_through_the_cli(estate, monkeypatch):
    cli_mod, store, sources = estate
    run_cli(cli_mod, "convert", "--project", "test-proj", "--compute-sources", sources)
    sessions = fake_fabric(monkeypatch, {POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})],
                                         NEW_POOL: [Resp(201, {"id": "pool-9"})]})
    out = run_cli(cli_mod, "compute-deploy", "--project", "test-proj", "--workspace-id", WS,
                  "--pool", "ADF-Large-n3-9", "--apply")
    assert out.exit_code == 0, out.output
    assert "pool ADF-Large-n3-9: create [pool-9]" in out.output
    assert writes(sessions[0]) == [("POST", f"{fd.FABRIC_API}/workspaces/{WS}/spark/pools")]


# --- reusing pools that already exist -------------------------------------------------------------------------------


def existing_pool(name, **kw):
    return {"id": f"id-{name}", "name": name, "type": "Workspace",
            **{k: v for k, v in body(name, **kw).items() if k != "name"}}


def test_an_identical_pool_with_another_name_is_reused_not_recreated():
    other = existing_pool("team-medium")  # same four settings as the proposal, different name
    c, s = client({POOLS: [Resp(200, {"value": [other]})], ENVS: [Resp(200, {"value": []})], NEW_ENV: [Resp(201, {"id": "e1"})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True)
    assert res[0].action == "reuse" and res[0].item_id == "id-team-medium" and "identical settings" in res[0].detail
    assert not any(u.endswith("/spark/pools") for m, u in writes(s))
    env_call = next(b for m, u, b, _ in s.calls if m == "POST" and u.endswith("/environments"))
    compute = yaml.safe_load(base64.b64decode(env_call["definition"]["parts"][0]["payload"]))
    assert compute["instance_pool_id"] == "id-team-medium"  # the environment points at the reused pool


def test_a_compatible_pool_is_only_a_hint_until_reuse_is_requested():
    wide = existing_pool("shared-medium", lo=1, hi=20)  # same node size, covers 3-7

    def script():
        return {POOLS: [Resp(200, {"value": [wide]})], ENVS: [Resp(200, {"value": []})],
                NEW_POOL: [Resp(201, {"id": "new"})], NEW_ENV: [Resp(201, {"id": "e1"})]}

    c, s = client(script())
    res = cd.deploy_compute(c, WS, PLAN, apply=False)
    assert res[0].action == "create" and "shared-medium" in res[0].detail and "--reuse" in res[0].detail

    c, s = client(script())
    res = cd.deploy_compute(c, WS, PLAN, apply=True, reuse_compatible=True)
    assert res[0].action == "reuse" and "1-20 nodes" in res[0].detail and "scale further" in res[0].detail
    assert not any(u.endswith("/spark/pools") for m, u in writes(s))


@pytest.mark.parametrize("other", [existing_pool("small", size="Small", lo=1, hi=20),  # different node size
                                   existing_pool("narrow", lo=4, hi=7),  # does not reach down to 3 nodes
                                   existing_pool("short", lo=1, hi=5)])  # cannot reach 7 nodes
def test_incompatible_pools_are_never_reused(other):
    c, s = client({POOLS: [Resp(200, {"value": [other]})], ENVS: [Resp(200, {"value": []})],
                   NEW_POOL: [Resp(201, {"id": "new"})], NEW_ENV: [Resp(201, {"id": "e1"})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=True, reuse_compatible=True)
    assert res[0].action == "create"


def test_results_are_recorded_into_the_plan():
    from adf2fabric.project.compute import ActivityTarget

    target = ActivityTarget(pipeline="P", activity="A", activity_type="DatabricksNotebook", source_compute_id="c1",
                            conversion_today="reuse_externally", target_execution="FabricNotebook",
                            spark_pool="ADF-Medium-n3-7", environment="ENV-1.3-1")
    plan = ComputePlan(pools=[pool()], environments=[env()], targets=[target])
    out = cd.record_results(plan, [cd.ComputeResult("pool", "ADF-Medium-n3-7", "create", "p-1"),
                                   cd.ComputeResult("environment", "ENV-1.3-1", "create", "e-1")])
    assert out.pools[0].fabric_id == "p-1" and out.environments[0].fabric_id == "e-1"
    assert out.targets[0].provisioning_status == "created"
    assert plan.targets[0].provisioning_status == "pending_approval"  # the input is not modified
    reused = cd.record_results(plan, [cd.ComputeResult("pool", "ADF-Medium-n3-7", "reuse", "p-2")])
    assert reused.targets[0].provisioning_status == "exists"
    dry = cd.record_results(plan, [cd.ComputeResult("pool", "ADF-Medium-n3-7", "create", detail="dry run")])
    assert dry.pools[0].fabric_id is None and dry.targets[0].provisioning_status == "pending_approval"


def test_applying_through_the_cli_writes_the_record_and_updates_the_plan(estate, monkeypatch):
    import json

    cli_mod, store, sources = estate
    run_cli(cli_mod, "convert", "--project", "test-proj", "--compute-sources", sources)
    fake_fabric(monkeypatch, {POOLS: [Resp(200, {"value": []})], ENVS: [Resp(200, {"value": []})],
                              NEW_POOL: [Resp(201, {"id": "pool-9"})]})
    out = run_cli(cli_mod, "compute-deploy", "--project", "test-proj", "--workspace-id", WS,
                  "--pool", "ADF-Large-n3-9", "--apply")
    assert out.exit_code == 0, out.output
    base = store.conversion_dir("test-proj")
    record = json.loads((base / "compute-provisioning.json").read_text("utf-8"))
    assert record["workspace_id"] == WS and any(r["item_id"] == "pool-9" for r in record["results"])
    plan = ComputePlan.model_validate_json((base / "compute-plan.json").read_text("utf-8"))
    assert next(p for p in plan.pools if p.name == "ADF-Large-n3-9").fabric_id == "pool-9"
    assert {t.provisioning_status for t in plan.targets if t.spark_pool == "ADF-Large-n3-9"} == {"created"}


def test_a_pool_without_dynamic_allocation_matches_although_fabric_drops_its_executor_limits():
    """Live 2026-10-27: ADF-Small-n1, created from this very body, read back as {"enabled": false} and was planned as a conflict."""
    sent = body("ADF-Small-n1", size="Small", lo=1, hi=1)
    assert sent["dynamicExecutorAllocation"] == {"enabled": False, "minExecutors": 1, "maxExecutors": 1}
    live = dict(sent, id="pool-1", type="Workspace", dynamicExecutorAllocation={"enabled": False})
    assert cd.pool_matches(live, sent)
    assert cd.find_reusable({"adf-small-n1": live}, sent) == (live, "identical")
    on = dict(live, dynamicExecutorAllocation={"enabled": True, "minExecutors": 1, "maxExecutors": 1})
    assert not cd.pool_matches(on, sent)  # allocation on is a different setting
    scaling = body()
    assert not cd.pool_matches(dict(scaling, dynamicExecutorAllocation=dict(scaling["dynamicExecutorAllocation"], maxExecutors=9)), scaling)


def test_the_workspace_starter_pool_is_never_a_reuse_candidate():
    starter = {"id": "00000000-0000-0000-0000-000000000000", "name": "Starter Pool", "type": "Workspace",
               "nodeFamily": "MemoryOptimized", "nodeSize": "Medium",
               "autoScale": {"enabled": True, "minNodeCount": 1, "maxNodeCount": 10},
               "dynamicExecutorAllocation": {"enabled": True, "minExecutors": 1, "maxExecutors": 9}}
    assert cd.find_reusable({"starter pool": starter}, body()) == (None, "")
    identical = dict(starter, **{k: body()[k] for k in ("autoScale", "dynamicExecutorAllocation")})
    assert cd.find_reusable({"starter pool": identical}, body()) == (None, "")
    c, s = client({POOLS: [Resp(200, {"value": [starter]})], ENVS: [Resp(200, {"value": []})]})
    res = cd.deploy_compute(c, WS, PLAN, apply=False, reuse_compatible=True)
    assert res[0].action == "create" and "Starter Pool" not in res[0].detail
