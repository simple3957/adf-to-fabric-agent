"""Spark compute inventory and Fabric pool/environment proposals (offline, deterministic)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from adf2fabric.agent.conversion.compute_plan import (
    azure_vm_vcores,
    build_compute_plan,
    fabric_node_size,
    load_sources,
    parse_databricks_runtime,
    render_compute_section,
    suggest_runtime,
)
from adf2fabric.agent.discovery.adf_semantic import parser
from tests.conversion_support import act, linked_service, pipe, ref, semantic, trigger


def dbx_ls(name="LS_DBX", **tp):
    return linked_service(name, "AzureDatabricks", {"domain": "https://adb-1.azuredatabricks.net", **tp})


def dbx_activity(name="Nb", ls="LS_DBX", **tp):
    return act(name, "DatabricksNotebook", {"notebookPath": "/Shared/etl", **tp},
               linkedServiceName=ref("LinkedService", ls))


def synapse_activity(name="Sn", pool="spark1", **tp):
    body = {"notebook": ref("NotebookReference", "nb1"), **tp}
    if pool:
        body["sparkPool"] = ref("BigDataPoolReference", pool)
    return act(name, "SynapseNotebook", body)


def plan_for(pipelines, linked_services=(), triggers=(), sources=None):
    d = semantic(pipelines, linked_services=list(linked_services), triggers=list(triggers))
    return build_compute_plan(d, None, sources)


NEW_CLUSTER = dict(newClusterNodeType="Standard_DS3_v2", newClusterNumOfWorker="2:8",
                   newClusterVersion="14.3.x-scala2.12")


# --- reading the linked service -------------------------------------------------------------------------------------


def test_databricks_linked_service_cluster_is_read_and_secrets_are_masked():
    ls = parser.parse_linked_service(dbx_ls(
        **NEW_CLUSTER, newClusterDriverNodeType="Standard_DS4_v2",
        newClusterSparkConf={"spark.sql.shuffle.partitions": "64", "spark.hadoop.fs.azure.account.key": "abc123"},
        newClusterInitScripts=["dbfs:/a.sh", "dbfs:/b.sh"], accessToken={"type": "SecureString", "value": "dapi-secret"}),
        "linkedService/LS_DBX.json")
    c = ls.compute
    assert (c.mode, c.min_workers, c.max_workers, c.node_type) == ("new_cluster", 2, 8, "Standard_DS3_v2")
    assert c.driver_node_type == "Standard_DS4_v2" and c.init_script_count == 2
    assert c.spark_conf["spark.sql.shuffle.partitions"] == "64"
    assert c.spark_conf["spark.hadoop.fs.azure.account.key"] == "[masked]"
    assert "dapi-secret" not in ls.model_dump_json()


def test_fixed_worker_count_and_existing_cluster_and_parameterized_values():
    fixed = parser.parse_linked_service(dbx_ls(newClusterNodeType="Standard_E8ds_v5", newClusterNumOfWorker="4"), "p")
    assert (fixed.compute.min_workers, fixed.compute.max_workers) == (4, 4)
    existing = parser.parse_linked_service(dbx_ls(existingClusterId="0123-456789-abcdef"), "p")
    assert existing.compute.mode == "existing_cluster" and existing.compute.existing_cluster_id == "0123-456789-abcdef"
    dynamic = parser.parse_linked_service(dbx_ls(
        newClusterNodeType={"type": "Expression", "value": "@linkedService().node"}, newClusterNumOfWorker="3"), "p")
    assert dynamic.compute.node_type is None and "newClusterNodeType" in dynamic.compute.unresolved_fields


def test_non_databricks_linked_service_has_no_compute():
    assert parser.parse_linked_service(linked_service("LS_SQL", "AzureSqlDatabase"), "p").compute is None


# --- sizing rules ---------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("name,vcores,gpu", [
    ("Standard_E8ds_v5", 8, False), ("Standard_D4s_v3", 4, False), ("Standard_F16s_v2", 16, False),
    ("Standard_DS3_v2", 4, False), ("Standard_DS4_v2", 8, False), ("Standard_DS5_v2", 16, False),
    ("Standard_DS13_v2", 8, False), ("Standard_E8-4ds_v5", 4, False), ("Standard_NC6s_v3", 6, True),
    ("i3.xlarge", None, False), (None, None, False)])
def test_azure_vm_vcores(name, vcores, gpu):
    assert azure_vm_vcores(name) == (vcores, gpu)


@pytest.mark.parametrize("vcores,size", [(2, "Small"), (4, "Small"), (6, "Medium"), (8, "Medium"), (16, "Large"),
                                         (20, "XLarge"), (64, "XXLarge"), (65, None)])
def test_fabric_node_size_rounds_up(vcores, size):
    assert fabric_node_size(vcores)[0] == size


def test_new_cluster_becomes_a_pool_with_the_documented_request_body():
    plan = plan_for([pipe(dbx_activity())], [dbx_ls(**NEW_CLUSTER)])
    (use,) = plan.inventory
    assert use.sizing_known and use.vcores_per_node == 4 and use.source_peak_vcores == 4 + 8 * 4
    (pool,) = plan.pools
    assert pool.status == "proposed" and pool.node_size == "Small"
    assert (pool.min_nodes, pool.max_nodes) == (3, 9)  # workers + the driver node
    assert pool.peak_spark_vcores == 36 and pool.peak_capacity_units == 18
    assert pool.request_body == {
        "name": "ADF-Small-n3-9", "nodeFamily": "MemoryOptimized", "nodeSize": "Small",
        "autoScale": {"enabled": True, "minNodeCount": 3, "maxNodeCount": 9},
        "dynamicExecutorAllocation": {"enabled": True, "minExecutors": 2, "maxExecutors": 8}}


def test_fixed_size_cluster_is_a_fixed_pool():
    plan = plan_for([pipe(dbx_activity())],
                    [dbx_ls(newClusterNodeType="Standard_E8ds_v5", newClusterNumOfWorker="4",
                            newClusterVersion="15.4.x-scala2.12")])
    (pool,) = plan.pools
    assert pool.node_size == "Medium" and (pool.min_nodes, pool.max_nodes) == (5, 5)
    assert pool.request_body["autoScale"] == {"enabled": True, "minNodeCount": 5, "maxNodeCount": 5}
    assert pool.request_body["dynamicExecutorAllocation"]["enabled"] is False


def test_computes_with_the_same_sizing_share_one_pool():
    plan = plan_for([pipe(dbx_activity("A", "LS_ONE"), dbx_activity("B", "LS_TWO"))],
                    [dbx_ls("LS_ONE", **NEW_CLUSTER), dbx_ls("LS_TWO", **NEW_CLUSTER)])
    assert len(plan.inventory) == 2 and len(plan.pools) == 1 and len(plan.pools[0].replaces) == 2


def test_a_larger_driver_is_flagged_and_workers_set_the_node_size():
    plan = plan_for([pipe(dbx_activity())], [dbx_ls(**NEW_CLUSTER, newClusterDriverNodeType="Standard_DS5_v2")])
    assert plan.pools[0].node_size == "Small"
    assert any(g.code == "driver_larger_than_workers" for g in plan.gaps)


def test_gpu_nodes_have_no_fabric_pool():
    plan = plan_for([pipe(dbx_activity())], [dbx_ls(newClusterNodeType="Standard_NC6s_v3", newClusterNumOfWorker="2")])
    assert plan.pools[0].status == "not_possible" and plan.environments == []
    assert any(g.code == "gpu_not_available" for g in plan.gaps)


def test_a_node_larger_than_xxlarge_is_not_possible():
    plan = plan_for([pipe(dbx_activity())], [dbx_ls(newClusterNodeType="Standard_E96ds_v5", newClusterNumOfWorker="2")])
    assert plan.pools[0].status == "not_possible"
    assert any(g.code == "exceeds_largest_node" for g in plan.gaps)


# --- computes ADF only references -----------------------------------------------------------------------------------


def test_existing_cluster_needs_sizing_until_the_sources_file_supplies_it(tmp_path):
    ls, pl = dbx_ls(existingClusterId="c-1"), pipe(dbx_activity())
    plan = plan_for([pl], [ls])
    assert plan.pools[0].status == "needs_input" and plan.pools[0].request_body is None
    assert any(g.code == "sizing_unavailable" for g in plan.gaps)

    f = tmp_path / "sources.json"
    f.write_text(json.dumps({"databricks_clusters": {"c-1": {
        "node_type_id": "Standard_DS4_v2", "autoscale": {"min_workers": 1, "max_workers": 4},
        "spark_version": "15.4.x-photon-scala2.12", "runtime_engine": "PHOTON",
        "spark_conf": {"spark.sql.shuffle.partitions": "200", "spark.databricks.delta.preview.enabled": "true"},
        "init_scripts": [{}]}}}), encoding="utf-8")
    sources = load_sources(str(f))
    assert sources.problems == []
    plan = plan_for([pl], [ls], sources=sources)
    (pool,) = plan.pools
    assert pool.status == "proposed" and pool.node_size == "Medium" and (pool.min_nodes, pool.max_nodes) == (2, 5)
    (use,) = plan.inventory
    assert use.sizing_source == "compute_sources_file" and use.photon
    (env,) = plan.environments
    assert env.runtime_version == "1.3" and "EOSA" in " ".join(env.notes)
    assert env.spark_properties == {"spark.sql.shuffle.partitions": "200"}  # spark.databricks.* is not carried
    codes = {g.code for g in plan.gaps}
    assert {"platform_spark_conf", "init_scripts"} <= codes


def test_missing_or_broken_sources_file_is_reported_not_raised(tmp_path):
    assert "Could not read" in load_sources(str(tmp_path / "nope.json")).problems[0]
    bad = tmp_path / "bad.json"
    bad.write_text("[1]", encoding="utf-8")
    assert load_sources(str(bad)).problems == ["The compute sources file must be a JSON object."]


def test_synapse_pool_without_a_definition_needs_input_and_with_one_is_sized(tmp_path):
    pl = pipe(synapse_activity())
    plan = plan_for([pl])
    assert plan.pools[0].status == "needs_input"
    assert plan.inventory[0].platform == "synapse" and plan.inventory[0].source_ref == "spark1"

    f = tmp_path / "s.json"
    f.write_text(json.dumps({"synapse_spark_pools": {"spark1": {"properties": {
        "nodeSize": "Medium", "sparkVersion": "3.4",
        "autoScale": {"enabled": True, "minNodeCount": 3, "maxNodeCount": 10}}}}}), encoding="utf-8")
    plan = plan_for([pl], sources=load_sources(str(f)))
    (pool,) = plan.pools
    assert pool.node_size == "Medium" and (pool.min_nodes, pool.max_nodes) == (3, 10)
    (env,) = plan.environments
    assert env.runtime_version == "1.3" and "Spark upgrade" in " ".join(env.notes)


def test_synapse_activity_override_is_sized_from_the_activity():
    plan = plan_for([pipe(synapse_activity(pool=None, executorSize="Large", driverSize="Large", numExecutors=4))])
    (pool,) = plan.pools
    assert pool.status == "proposed" and pool.node_size == "Large" and (pool.min_nodes, pool.max_nodes) == (5, 5)
    assert any(g.code == "synapse_session_override" for g in plan.gaps)


# --- runtime mapping ------------------------------------------------------------------------------------------------


def test_databricks_runtime_parsing_uses_only_the_documented_table():
    assert parse_databricks_runtime("14.3.x-photon-scala2.12")[:3] == ("14.3", "3.5.0", True)
    assert parse_databricks_runtime("17.3.x-scala2.13")[1] == "4.0.0"
    assert parse_databricks_runtime("18.0.x-scala2.13")[1] == "4.1.0"
    assert parse_databricks_runtime("13.3.x-scala2.12")[1] is None  # not in the supported-releases table
    assert parse_databricks_runtime("15.4.x-gpu-ml-scala2.12")[3:] == (True, True)


@pytest.mark.parametrize("spark,runtime", [("3.5.0", "1.3"), ("4.0.0", "2.0"), ("4.2.0", "2.0"), ("3.4", "1.3"), (None, "2.0")])
def test_suggested_fabric_runtime(spark, runtime):
    assert suggest_runtime(spark)[0] == runtime


# --- usage, triggers and the report ---------------------------------------------------------------------------------


def test_usage_includes_pipelines_and_the_triggers_that_start_them():
    child = pipe(dbx_activity("Nb"), name="PL_Child")
    parent = pipe(act("Run", "ExecutePipeline", {"pipeline": ref("PipelineReference", "PL_Child")}), name="PL_Parent")
    plan = plan_for([child, parent], [dbx_ls(**NEW_CLUSTER)], triggers=[trigger("T_Daily", "PL_Parent")])
    (use,) = plan.inventory
    assert use.activities == ["PL_Child/Nb"] and use.triggers == ["T_Daily"]


def test_no_spark_compute_means_no_plan():
    assert plan_for([pipe(act("W", "Wait", {"waitTimeInSeconds": 1}))]) is None


def test_report_section_lists_use_pools_gaps_and_basis():
    plan = plan_for([pipe(dbx_activity())], [dbx_ls(**NEW_CLUSTER, newClusterInitScripts=["a"])])
    text = "\n".join(render_compute_section(plan))
    for expected in ("## Spark compute plan", "ADF-Small-n3-9", "Peak Spark vCores", "init_scripts", "### Basis",
                     "Nothing here is created"):
        assert expected in text


# --- end to end on the fixture estate (real discovery + conversion agents) ------------------------------------------

FIXTURE = Path(__file__).parent / "fixtures" / "spark_estate"


def _estate_files() -> dict[str, dict]:
    src = FIXTURE / "source"
    return {p.relative_to(src).as_posix(): json.loads(p.read_text(encoding="utf-8")) for p in sorted(src.rglob("*.json"))}


def _convert_estate(tmp_path, with_sources: bool):
    from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
    from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
    from tests.test_adf_semantic_discovery import _make_project

    store, pid = _make_project(tmp_path, _estate_files())
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    result = ConversionAgent(store).run(ConversionRequest(
        project_name="test-proj", compute_sources_path=str(FIXTURE / "compute-sources.json") if with_sources else None))
    assert result.success, result.error
    return store, pid, result.conversion


def test_fixture_estate_end_to_end_with_the_sources_file(tmp_path):
    store, pid, conv = _convert_estate(tmp_path, with_sources=True)
    plan = conv.compute_plan
    by_id = {u.id: u for u in plan.inventory}
    assert len(plan.inventory) == 4  # new cluster, existing cluster, GPU cluster, Synapse pool
    etl = by_id["databricks:LS_DBX_ETL:new:Standard_DS4_v2:2-6"]
    assert etl.activities == ["PL_Nightly_ETL/Ingest_Raw", "PL_Nightly_ETL/Transform"]
    assert etl.triggers == ["TRG_Nightly"] and etl.spark_version == "3.5.0" and etl.init_script_count == 1
    assert etl.spark_conf["spark.hadoop.fs.azure.account.key.lake.dfs.core.windows.net"] == "[masked]"
    ml = by_id["databricks:LS_DBX_ML:existing:0912-101500-abc123"]
    assert ml.sizing_source == "compute_sources_file" and ml.vcores_per_node == 16 and ml.photon
    assert by_id["databricks:LS_DBX_GPU:new:Standard_NC6s_v3:2-2"].gpu
    assert any(u.platform == "synapse" and u.sizing_known for u in plan.inventory)

    sizes = {p.name: (p.status, p.node_size, p.min_nodes, p.max_nodes) for p in plan.pools}
    assert sizes["ADF-Medium-n3-7"] == ("proposed", "Medium", 3, 7)  # DS4_v2 = 8 vCPU, 2-6 workers
    assert sizes["ADF-Large-n3-9"] == ("proposed", "Large", 3, 9)  # E16ds_v5 = 16 vCPU, 2-8 workers
    assert sizes["ADF-Medium-n3-10"] == ("proposed", "Medium", 3, 10)  # the Synapse pool definition wins over the override
    assert any(p.status == "not_possible" for p in plan.pools)
    assert plan.max_single_pool_vcores == 9 * 16

    codes = {g.code for g in plan.gaps}
    assert {"gpu_not_available", "init_scripts", "platform_spark_conf", "secret_in_spark_conf", "custom_library",
            "synapse_session_override"} <= codes
    assert "[masked]" not in json.dumps([e.spark_properties for e in plan.environments])

    report = (store.conversion_dir(pid) / "conversion-report.md").read_text("utf-8")
    assert "## Spark compute plan" in report and "ADF-Large-n3-9" in report
    saved = json.loads((store.conversion_dir(pid) / "compute-plan.json").read_text("utf-8"))
    assert saved["pools"][0]["request_body"] is not None or any(p["request_body"] for p in saved["pools"])


def test_fixture_estate_without_the_sources_file_asks_for_sizes(tmp_path):
    _, _, conv = _convert_estate(tmp_path, with_sources=False)
    plan = conv.compute_plan
    needs = [p for p in plan.pools if p.status == "needs_input"]
    assert len(needs) == 1 and "0912-101500-abc123" in needs[0].replaces[0]
    assert any(g.code == "sizing_unavailable" for g in plan.gaps)


# --- per-activity targets -------------------------------------------------------------------------------------------


def test_each_spark_activity_gets_a_target_with_what_is_still_needed():
    nb = dbx_activity("Nb", baseParameters={"run_date": "x", "env": "y"}, libraries=[{"pypi": {"package": "requests"}}])
    jar = act("Jar", "DatabricksSparkJar", {"mainClassName": "x.Main"}, linkedServiceName=ref("LinkedService", "LS_DBX"))
    plan = plan_for([pipe(nb, jar)], [dbx_ls(**NEW_CLUSTER)])
    t = {x.activity: x for x in plan.targets}
    n = t["Nb"]
    assert (n.pipeline, n.activity_type, n.source_item) == ("PL", "DatabricksNotebook", "/Shared/etl")
    assert n.conversion_today == "reuse_externally" and n.target_execution == "FabricNotebook"
    assert n.parameters == ["env", "run_date"]  # names only, never values
    assert n.requires_notebook_migration and not n.requires_job_definition and n.requires_compatibility_review
    assert n.spark_pool == "ADF-Small-n3-9" and n.environment == plan.environments[0].name
    assert n.provisioning_status == "pending_approval" and n.blockers == []
    assert any("not scanned" in r for r in n.review_reasons) and any("pypi:requests" in r for r in n.review_reasons)
    j = t["Jar"]
    assert j.target_execution == "FabricSparkJobDefinition" and j.requires_job_definition and j.source_item == "x.Main"


def test_targets_carry_blockers_and_unsized_status():
    gpu = plan_for([pipe(dbx_activity())], [dbx_ls(newClusterNodeType="Standard_NC6s_v3", newClusterNumOfWorker="2")])
    (g,) = gpu.targets
    assert g.provisioning_status == "not_possible" and g.spark_pool is None and "GPU" in g.blockers[0]
    unsized = plan_for([pipe(dbx_activity())], [dbx_ls(existingClusterId="c-1")])
    (u,) = unsized.targets
    assert u.provisioning_status == "needs_input" and "not known" in u.blockers[0]


def test_synapse_notebook_target_and_fixture_report_table(tmp_path):
    plan = plan_for([pipe(synapse_activity(parameters={"p1": "v"}))])
    (t,) = plan.targets
    assert t.conversion_today == "replace" and t.source_item == "nb1" and t.parameters == ["p1"]
    assert any("mssparkutils" in r for r in t.review_reasons)
    _, _, conv = _convert_estate(tmp_path, with_sources=True)
    text = "\n".join(render_compute_section(conv.compute_plan))
    assert "### Where each Spark activity would run in Fabric" in text and "FabricNotebook" in text
    assert len(conv.compute_plan.targets) == 5  # Ingest_Raw, Transform, Score_Model, Aggregate_Synapse, Train
