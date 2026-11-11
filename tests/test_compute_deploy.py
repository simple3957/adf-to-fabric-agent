"""Creating the proposed Spark pools and environments.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import base64

import yaml

from adf2fabric.agent.deployment import compute_deploy as cd
from adf2fabric.project.compute import ComputePlan, FabricEnvironmentProposal, FabricPoolProposal

WS = "11111111-1111-1111-1111-111111111111"


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


def writes(s):
    return [(m, u) for m, u, *_ in s.calls if m != "GET"]


def test_runtime_version_is_written_as_a_string():
    text = base64.b64decode(cd.environment_definition(env(), pool(), "pid")["parts"][0]["payload"]).decode()
    assert "runtime_version: '1.3'" in text


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


def run_cli(cli_mod, *args):
    from click.testing import CliRunner

    return CliRunner().invoke(cli_mod.cli, list(args))


# --- reusing pools that already exist -------------------------------------------------------------------------------


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


