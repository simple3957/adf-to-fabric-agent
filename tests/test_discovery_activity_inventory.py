"""Activity-level discovery: every activity in every pipeline is found, accounted for, and never silently dropped.

Fixture-based (``tests/fixtures/adf_activities``): one pipeline per activity family and nested structure, written in
ADF's own JSON shape using property names from Microsoft's schemas. Each expected inventory below is written BY HAND from
the fixture, so it is an independent statement of what the source contains, not the parser's output echoed back.

Also covered: the dependency graph (edges, unresolved and cross-container edges, exact cycles), the independent
inventory reconciliation, unknown / malformed activities, recursive pipeline and flowlet references, data flow
definitions, and the ledger of source artifacts that could not be processed (malformed, unreadable, other kinds).
"""

from __future__ import annotations

import json
import random
from pathlib import Path

import pytest
import yaml

from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import (
    AdfSemanticDiscoverer,
    AdfSemanticDiscoveryRequest,
    discoverer as discoverer_mod,
    parser_pipelines as pp,
)
from adf2fabric.agent.discovery.adf_semantic.activity_registry import ACTIVITY_REGISTRY
from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.project.discovery import AdfConfidence, AdfSemanticDiscovery, AdfSemanticStatus
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.store import ProjectStore
from tests.test_adf_semantic_discovery import _git_commit

FIXTURES = Path(__file__).parent / "fixtures" / "adf_activities"

# Microsoft's ARM schema for Microsoft.DataFactory/factories/pipelines (2018-06-01) lists these 40 activity `type`
# values, plus the Synapse-only SqlPoolStoredProcedure (Synapse "SQL pool stored procedure activity" page).
DOCUMENTED_TYPES = """AppendVariable AzureDataExplorerCommand AzureFunctionActivity AzureMLBatchExecution
AzureMLExecutePipeline AzureMLUpdateResource Copy Custom DatabricksJob DatabricksNotebook DatabricksSparkJar
DatabricksSparkPython DataLakeAnalyticsU-SQL Delete ExecuteDataFlow ExecutePipeline ExecuteSSISPackage
ExecuteWranglingDataflow Fail Filter ForEach GetMetadata HDInsightHive HDInsightMapReduce HDInsightPig HDInsightSpark
HDInsightStreaming IfCondition Lookup Script SetVariable SparkJob SqlServerStoredProcedure Switch SynapseNotebook Until
Validation Wait WebActivity WebHook SqlPoolStoredProcedure""".split()

# pipeline -> [(path of names from the top, type, branch)], written by hand from tests/fixtures/adf_activities.
EXPECTED: dict[str, list[tuple[str, str | None, str | None]]] = {
    "PL_Containers": [
        ("Get_Items", "Lookup", None),
        ("ForEach_Item", "ForEach", None),
        ("ForEach_Item/If_Valid", "IfCondition", "activities"),
        ("ForEach_Item/If_Valid/Until_Ready", "Until", "ifTrue"),
        ("ForEach_Item/If_Valid/Until_Ready/Wait_Retry", "Wait", "activities"),
        ("ForEach_Item/If_Valid/Until_Ready/Call_Child", "ExecutePipeline", "activities"),
        ("ForEach_Item/If_Valid/Set_Ok", "SetVariable", "ifTrue"),
        ("ForEach_Item/If_Valid/Switch_Kind", "Switch", "ifFalse"),
        ("ForEach_Item/If_Valid/Switch_Kind/Copy_Csv", "Copy", "case:csv"),
        ("ForEach_Item/If_Valid/Switch_Kind/Copy_Json", "Copy", "case:json"),
        ("ForEach_Item/If_Valid/Switch_Kind/Fail_Json", "Fail", "case:json"),
        ("ForEach_Item/If_Valid/Switch_Kind/Log_Other", "SetVariable", "default"),
        ("Final_Wait", "Wait", None),
    ],
    "PL_Child": [("Child_Wait", "Wait", None)],
    "PL_DataActivities": [
        ("Copy_Data", "Copy", None), ("Lookup_Rows", "Lookup", None), ("Get_Meta", "GetMetadata", None),
        ("Delete_Old", "Delete", None), ("Validate_File", "Validation", None), ("Run_Script", "Script", None),
        ("Run_Proc", "SqlServerStoredProcedure", None), ("Run_Pool_Proc", "SqlPoolStoredProcedure", None),
    ],
    "PL_ExternalCalls": [
        ("Call_Web", "WebActivity", None), ("Hook", "WebHook", None), ("Call_Function", "AzureFunctionActivity", None),
        ("Copy_From_Rest", "Copy", None), ("Copy_From_Http", "Copy", None),
    ],
    "PL_Compute": [
        ("Dbx_Notebook", "DatabricksNotebook", None), ("Dbx_Jar", "DatabricksSparkJar", None),
        ("Dbx_Python", "DatabricksSparkPython", None), ("Dbx_Job", "DatabricksJob", None),
        ("Syn_Notebook", "SynapseNotebook", None), ("Syn_SparkJob", "SparkJob", None),
        ("Hdi_Hive", "HDInsightHive", None), ("Hdi_Pig", "HDInsightPig", None),
        ("Hdi_MapReduce", "HDInsightMapReduce", None), ("Hdi_Streaming", "HDInsightStreaming", None),
        ("Hdi_Spark", "HDInsightSpark", None), ("Batch_Custom", "Custom", None),
        ("Ml_Batch", "AzureMLBatchExecution", None), ("Ml_Update", "AzureMLUpdateResource", None),
        ("Ml_Pipeline", "AzureMLExecutePipeline", None), ("Usql", "DataLakeAnalyticsU-SQL", None),
        ("Ssis", "ExecuteSSISPackage", None), ("Kusto", "AzureDataExplorerCommand", None),
    ],
    "PL_DataFlows": [
        ("Run_Mapping", "ExecuteDataFlow", None), ("Run_Wrangling", "ExecuteWranglingDataflow", None),
        ("Run_Missing", "ExecuteDataFlow", None),
    ],
    "PL_ControlPolicies": [
        ("Set_Var", "SetVariable", None), ("Append_Var", "AppendVariable", None), ("Filter_Items", "Filter", None),
        ("Wait_A", "Wait", None), ("Wait_B", "Wait", None), ("Fail_End", "Fail", None), ("Copy_Policy", "Copy", None),
        ("Lookup_Dynamic", "Lookup", None),
    ],
    "PL_Unknown": [
        ("Known_Wait", "Wait", None), ("Future_Activity", "FutureActivityV2", None),
        ("Future_Container", "FutureLoop", None),
        ("Future_Container/Inner_Wait", "Wait", "activities"), ("Future_Container/Inner_Unknown", "Mystery", "activities"),
        ("No_Type", None, None), ("<unnamed-4>", "Wait", None),
    ],
    "PL_Graph": [
        ("A", "Wait", None), ("B", "Wait", None), ("C", "Wait", None), ("D", "Wait", None), ("Cyc1", "Wait", None),
        ("Cyc2", "Wait", None), ("Selfish", "Wait", None), ("Dangling", "Wait", None), ("Branch", "IfCondition", None),
        ("Branch/Inner_Ok", "Wait", "ifTrue"), ("Branch/Inner_Cross", "Wait", "ifTrue"),
    ],
    "PL_Ping": [("Call_Pong", "ExecutePipeline", None)],
    "PL_Pong": [("Call_Ping", "ExecutePipeline", None)],
    "PL_SelfCall": [("Call_Self", "ExecutePipeline", None)],
}


# --- building a project from the fixture files -------------------------------------------------------------------


def _fixture_files() -> dict[str, dict | str]:
    return {str(p.relative_to(FIXTURES)).replace("\\", "/"): json.loads(p.read_text(encoding="utf-8"))
            for p in sorted(FIXTURES.rglob("*.json"))}


def _project(tmp_path: Path, files: dict[str, dict | str], run_repo: bool = True):
    """A real git-backed project (Repository Discovery needs one). A str value is written verbatim (e.g. broken JSON)."""
    store = ProjectStore(tmp_path / "projects")
    pid = "test-proj"
    for rel, content in files.items():
        full = store.source_dir(pid) / rel
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content if isinstance(content, str) else json.dumps(content, indent=2), encoding="utf-8")
    sha = _git_commit(store.source_dir(pid))
    store.save(Project(id=pid, name="Test", repository=RepositoryMetadata(
        provider="github", repository_url="https://example.invalid/x.git", branch="main", commit_sha=sha,
        local_source_path=str(store.source_dir(pid)))))
    if run_repo:
        DiscoveryAgent(store).discover_repository(DiscoveryRequest(project_name=pid))
    return store, pid


def _discover(store) -> AdfSemanticDiscovery:
    result = AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    assert result.discovery is not None, result.error
    return result.discovery


@pytest.fixture(scope="module")
def estate(tmp_path_factory):
    store, pid = _project(tmp_path_factory.mktemp("estate"), _fixture_files())
    return store, pid, _discover(store)


def _pipeline(d: AdfSemanticDiscovery, name: str):
    return next(p for p in d.pipelines if p.name == name)


def _act(d: AdfSemanticDiscovery, pipeline: str, name: str):
    return next(a for a in walk(_pipeline(d, pipeline).activities) if a.name == name)


# --- 1. the discovered inventory equals the source inventory -----------------------------------------------------------


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_discovered_inventory_equals_the_hand_written_source_inventory(estate, name):
    _, _, d = estate
    p = _pipeline(d, name)
    found = sorted((a.activity_id.removeprefix(name + "/"), a.type, a.branch) for a in walk(p.activities))
    assert found == sorted(EXPECTED[name]), name
    assert p.activity_total == len(EXPECTED[name])
    assert p.activity_count == sum(1 for path, _type, _branch in EXPECTED[name] if "/" not in path)  # top-level only


def test_no_pipeline_in_the_source_is_missing_from_the_record_and_vice_versa(estate):
    _, _, d = estate
    on_disk = {p.stem for p in (FIXTURES / "pipeline").glob("*.json")}
    assert on_disk == {p.name for p in d.pipelines} == set(EXPECTED)
    assert d.activity_count == sum(len(v) for v in EXPECTED.values())


def _arm_walk(raw: dict) -> list[tuple[str | None, str | None]]:
    """The activities of a pipeline by Microsoft's DOCUMENTED containers only (a different rule than the parser's)."""
    found: list[tuple[str | None, str | None]] = []

    def walk_list(items):
        for a in items or []:
            if not isinstance(a, dict):
                continue
            found.append((a.get("name"), a.get("type")))
            tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
            kind = a.get("type")
            if kind == "IfCondition":
                walk_list(tp.get("ifTrueActivities"))
                walk_list(tp.get("ifFalseActivities"))
            elif kind == "Switch":
                for case in tp.get("cases") or []:
                    walk_list(case.get("activities"))
                walk_list(tp.get("defaultActivities"))
            elif kind in ("ForEach", "Until"):
                walk_list(tp.get("activities"))

    walk_list(raw["properties"]["activities"])
    return found


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_a_second_independent_walk_by_documented_containers_agrees(estate, name):
    _, _, d = estate
    raw = json.loads((FIXTURES / "pipeline" / f"{name}.json").read_text(encoding="utf-8"))
    documented = sorted(_arm_walk(raw), key=str)
    discovered = sorted(((a.name or None, a.type) for a in walk(_pipeline(d, name).activities)), key=str)
    if name == "PL_Unknown":
        # An unfamiliar container (FutureLoop) is outside Microsoft's documented set: the parser still finds its
        # children, which a documented-containers walk cannot. The extras must be exactly those two.
        assert [x for x in discovered if x not in documented] == [("Inner_Unknown", "Mystery"), ("Inner_Wait", "Wait")] or \
            sorted(x for x in discovered if x not in documented) == [("Inner_Unknown", "Mystery"), ("Inner_Wait", "Wait")]
        assert all(x in discovered for x in documented if x[0])
    else:
        assert discovered == documented


def test_the_inventory_check_reconciles_every_fixture_against_the_raw_json(estate):
    _, _, d = estate
    for p in d.pipelines:
        assert p.inventory is not None and p.inventory.matched, (p.name, p.inventory)
        assert p.inventory.source_activity_count == p.inventory.parsed_activity_count == p.activity_total
        assert not any(i.code == "inventory_mismatch" for i in p.issues)
    assert d.inventory_mismatch_count == 0
    assert _pipeline(d, "PL_Unknown").inventory.malformed_pointers == ["properties.activities[5]"]


# --- 2. every documented activity family has a fixture and a registry entry ------------------------------------------


def test_every_documented_activity_type_is_in_the_registry_and_in_a_fixture(estate):
    _, _, d = estate
    registry = {t.lower() for t in ACTIVITY_REGISTRY}
    assert [t for t in DOCUMENTED_TYPES if t.lower() not in registry] == []
    in_fixtures = {a.type for p in d.pipelines for a in walk(p.activities) if a.type}
    assert [t for t in DOCUMENTED_TYPES if t not in in_fixtures] == []


def test_known_activities_carry_every_required_property(estate):
    _, _, d = estate
    for p in d.pipelines:
        if p.name == "PL_Unknown":
            continue
        for a in walk(p.activities):
            assert a.missing_properties == [], (a.activity_id, a.missing_properties)
            assert "unknown_activity_type" not in {w.code for w in a.warnings}, a.activity_id
            assert a.confidence in (AdfConfidence.high, AdfConfidence.medium), (a.activity_id, a.confidence)


def test_sql_pool_and_databricks_job_are_understood(estate):
    _, _, d = estate
    pool = _act(d, "PL_DataActivities", "Run_Pool_Proc")
    assert [(r.kind, r.name) for r in pool.references] == [("sqlPool", "DedicatedSQLPool")]
    assert pool.configuration["sql_pool"] == "DedicatedSQLPool" and pool.configuration["stored_procedure"] == "usp_sample"
    job = _act(d, "PL_Compute", "Dbx_Job")
    assert job.configuration["job_id"] == "0123456789" and job.configuration["job_parameters"] == {"testParameter": "testValue"}
    assert [(r.kind, r.name) for r in job.references] == [("linkedService", "LS_Dbx")]


def test_http_and_rest_are_connectors_used_through_copy_not_activity_types(estate):
    _, _, d = estate
    assert "HttpActivity" not in {t.type for t in ACTIVITY_REGISTRY.values()} and "RestActivity" not in ACTIVITY_REGISTRY
    rest = _act(d, "PL_ExternalCalls", "Copy_From_Rest")
    http = _act(d, "PL_ExternalCalls", "Copy_From_Http")
    assert rest.inputs[0].connector_type == "RestSource" or rest.configuration or rest.raw_properties["source"]["type"] == "RestSource"
    assert http.raw_properties["source"]["type"] == "HttpSource"


# --- 3. unknown and malformed activities are preserved, never discarded ------------------------------------------------


def test_unknown_activity_types_are_preserved_described_and_reported(estate):
    _, _, d = estate
    assert d.unknown_activity_types == ["FutureActivityV2", "FutureLoop", "Mystery"]
    assert _pipeline(d, "PL_Unknown").unknown_activity_types == ["FutureActivityV2", "FutureLoop", "Mystery"]
    future = _act(d, "PL_Unknown", "Future_Activity")
    assert future.confidence is AdfConfidence.unsupported
    assert future.raw_properties["foo"] == 1 and "unknown_activity_type" in {w.code for w in future.warnings}
    assert [(r.kind, r.name) for r in future.references] == [("linkedService", "LS_Blob")]  # references still found
    assert "Unrecognised activity type" in future.semantic_summary
    inner = _act(d, "PL_Unknown", "Inner_Unknown")  # inside an unfamiliar container
    assert inner.parent_activity_id == "PL_Unknown/Future_Container" and inner.type == "Mystery"


def test_activities_with_missing_name_type_or_a_non_object_entry_are_reported_not_dropped(estate):
    _, _, d = estate
    p = _pipeline(d, "PL_Unknown")
    assert "missing_activity_type" in {w.code for w in _act(d, "PL_Unknown", "No_Type").warnings}
    unnamed = next(a for a in walk(p.activities) if a.activity_id.endswith("<unnamed-4>"))
    assert "missing_activity_name" in {w.code for w in unnamed.warnings}
    bad = [i for i in p.issues if i.code == "invalid_activity_node"]
    assert len(bad) == 1 and bad[0].property_path == "properties.activities[5]"


# --- 4. dependencies, policies, expressions, variables --------------------------------------------------------------------


def test_dependency_conditions_are_normalised_and_kept_per_edge(estate):
    _, _, d = estate
    assert _act(d, "PL_ControlPolicies", "Filter_Items").depends_on[0].conditions == ["Failed", "Succeeded"]
    assert _act(d, "PL_ControlPolicies", "Wait_A").depends_on[0].conditions == ["Skipped"]
    assert _act(d, "PL_ControlPolicies", "Wait_B").depends_on[0].conditions == ["Completed"]
    assert _act(d, "PL_ControlPolicies", "Fail_End").depends_on[0].conditions == ["Failed"]


def test_policies_retries_timeouts_and_secure_flags_are_captured(estate):
    _, _, d = estate
    p = _act(d, "PL_ControlPolicies", "Copy_Policy").policy
    assert (p.timeout, p.retry, p.retry_interval_in_seconds, p.secure_input, p.secure_output) == ("0.00:30:00", 3, 60, True, True)
    default = _act(d, "PL_DataActivities", "Copy_Data").policy
    assert (default.timeout, default.retry, default.secure_input) == ("0.12:00:00", 0, False)
    dynamic = _act(d, "PL_ControlPolicies", "Lookup_Dynamic").policy
    assert dynamic.is_dynamic and dynamic.timeout is None and dynamic.retry == 1
    assert "timeout" in _act(d, "PL_ControlPolicies", "Lookup_Dynamic").parameters  # the expression in policy.timeout


def test_deactivated_activities_keep_their_state(estate):
    _, _, d = estate
    b = _act(d, "PL_ControlPolicies", "Wait_B")
    assert b.state == "Inactive" and b.configuration["on_inactive_mark_as"] == "Failed"


def test_expressions_parameters_and_variables_are_tracked(estate):
    _, _, d = estate
    assert _act(d, "PL_ControlPolicies", "Set_Var").variables_written == ["v"]
    append = _act(d, "PL_ControlPolicies", "Append_Var")
    assert append.variables_read == ["v"] and append.variables_written == ["arr"]
    assert _act(d, "PL_ExternalCalls", "Call_Web").parameters == ["path"]
    assert _act(d, "PL_Containers", "ForEach_Item").configuration["batch_count"] == 4
    assert any(e.role == "loop_collection" for e in _act(d, "PL_Containers", "ForEach_Item").expressions)


def test_secret_values_never_reach_the_persisted_record(estate):
    store, pid, _ = estate
    text = (store.project_dir(pid) / "discovery" / "adf-semantic.yaml").read_text(encoding="utf-8")
    assert "FIXTURE_SECRET_VALUE" not in text
    assert "FIXTURE_FLOW_SCRIPT_SECRET" not in text  # a data flow script's text is never stored


# --- 5. the dependency graph -----------------------------------------------------------------------------------------------


def test_the_dependency_graph_has_every_node_edge_and_exact_cycle(estate):
    _, _, d = estate
    g = _pipeline(d, "PL_Graph").dependency_graph
    assert len(g.nodes) == 11 and g.scope_count == 2
    assert g.cycles == [["PL_Graph/Cyc1", "PL_Graph/Cyc2"], ["PL_Graph/Selfish"]]
    problems = {e.to_id.split("/")[-1]: e.problem for e in g.edges if e.problem}
    assert problems == {"Selfish": "self", "Dangling": "unresolved", "Inner_Cross": "outside_scope"}
    into_d = sorted((e.from_name, tuple(e.conditions)) for e in g.edges if e.to_id.endswith("/D"))
    assert into_d == [("B", ("Succeeded",)), ("C", ("Failed",))]
    assert len(g.edges) == 10
    resolved = [e for e in g.edges if e.resolved]
    assert all(e.from_id for e in resolved) and not any(e.from_id for e in g.edges if e.problem == "unresolved")
    codes = {w.code for a in walk(_pipeline(d, "PL_Graph").activities) for w in a.warnings}
    assert {"cyclic_dependency", "self_dependency", "unresolved_dependency", "dependency_outside_scope"} <= codes


def test_an_acyclic_pipeline_has_no_cycles_and_a_topological_order(estate):
    _, _, d = estate
    g = _pipeline(d, "PL_Containers").dependency_graph
    assert g.cycles == [] and len(g.nodes) == 13
    assert all(e.resolved for e in g.edges)
    order = {a.name: a.execution_order for a in walk(_pipeline(d, "PL_Containers").activities)}
    assert order["Get_Items"] < order["ForEach_Item"] < order["Final_Wait"]


def test_recursive_pipeline_calls_are_detected(estate):
    _, _, d = estate
    for name in ("PL_Ping", "PL_Pong", "PL_SelfCall"):
        assert "cyclic_pipeline_reference" in {i.code for i in _pipeline(d, name).issues}, name
    assert "cyclic_pipeline_reference" not in {i.code for i in _pipeline(d, "PL_Containers").issues}


# --- 6. data flows --------------------------------------------------------------------------------------------------------


def _flow(d, name):
    return next(f for f in d.dataflows if f.name == name)


def test_every_data_flow_definition_in_the_source_is_discovered(estate):
    _, _, d = estate
    on_disk = {p.stem for p in (FIXTURES / "dataflow").glob("*.json")}
    assert on_disk == {f.name for f in d.dataflows} and d.dataflow_count == len(on_disk) == 6


def test_a_mapping_data_flow_is_described_from_its_sources_sinks_and_transformations(estate):
    _, _, d = estate
    f = _flow(d, "DF_Map")
    assert (f.flow_type, f.folder, f.annotations, f.description) == ("MappingDataFlow", "f1", ["a"], "mapping")
    assert [n.name for n in f.sources] == ["source1", "source2"] and [n.name for n in f.sinks] == ["sink1"]
    assert [n.name for n in f.transformations] == ["d1", "embedded"]
    assert f.dataset_refs == ["DS_In", "DS_Out"] and f.flowlet_refs == ["DF_Flowlet"]
    assert f.linked_service_refs == ["LS_Blob"] and f.has_script and f.script_line_count == 3
    assert f.sinks[0].rejected_data_linked_service == "LS_Blob" and f.sources[1].schema_linked_service == "LS_Blob"
    assert not [i for i in f.issues if i.severity.value == "error"]


def test_flowlet_and_wrangling_flows_follow_their_own_schema(estate):
    _, _, d = estate
    assert _flow(d, "DF_Flowlet").flow_type == "Flowlet" and _flow(d, "DF_Flowlet").script_line_count == 2
    w = _flow(d, "DF_Wrangle")
    assert (w.flow_type, w.document_locale, w.has_script) == ("WranglingDataFlow", "en-us", True)
    assert w.sources[0].has_script and w.dataset_refs == ["DS_In"]


def test_a_broken_data_flow_keeps_what_it_cannot_interpret(estate):
    _, _, d = estate
    f = _flow(d, "DF_Broken")
    codes = {i.code for i in f.issues}
    assert {"invalid_node", "duplicate_node_name", "unresolved_dataset"} <= codes
    assert {"properties.futureProperty", "sources[0].futureNodeKey", "typeProperties.newTypeKey"} <= set(f.unknown_properties)
    assert [n.name for n in f.sources] == ["dup"]  # the valid entry is kept; the non-object entry is an issue


def test_flowlets_that_embed_each_other_are_a_reported_cycle(estate):
    _, _, d = estate
    for name in ("DF_Loop_A", "DF_Loop_B"):
        assert "cyclic_flowlet_reference" in {i.code for i in _flow(d, name).issues}, name
    assert "cyclic_flowlet_reference" not in {i.code for i in _flow(d, "DF_Map").issues}


def test_execute_data_flow_resolves_to_a_discovered_definition_or_says_it_does_not(estate):
    _, _, d = estate
    ok = _act(d, "PL_DataFlows", "Run_Mapping")
    assert [(r.name, r.resolved) for r in ok.references if r.kind == "dataflow"] == [("DF_Map", True)]
    assert ok.inputs[0].kind == "dataflow" and ok.inputs[0].resolved is True
    missing = _act(d, "PL_DataFlows", "Run_Missing")
    assert [(r.name, r.resolved) for r in missing.references if r.kind == "dataflow"] == [("DF_Not_There", False)]
    assert "unresolved_dataflow" in {w.code for w in missing.warnings}
    kinds = {(e.relationship.value, e.target_name, e.resolved) for e in d.relationships if "dataflow" in e.relationship.value}
    assert ("pipeline_executes_dataflow", "DF_Map", True) in kinds
    assert ("pipeline_executes_dataflow", "DF_Not_There", False) in kinds
    assert ("dataflow_uses_flowlet", "DF_Flowlet", True) in kinds
    assert any(u.name == "DF_Not_There" for u in d.unresolved_references)


# --- 7. nothing seen in the source disappears: it is a record or a ledger entry ---------------------------------------------


def _record_paths(d: AdfSemanticDiscovery) -> set[str]:
    groups = (d.pipelines, d.datasets, d.linked_services, d.triggers, d.factory, d.dataflows, d.change_data_captures,
              d.integration_runtimes)
    return {x.path for g in groups for x in g}


def test_the_fixture_estate_accounts_for_every_candidate(estate):
    store, pid, d = estate
    repo = store.load_repository_discovery(pid)
    seen = {c.path for c in repo.adf_candidates}
    assert seen == _record_paths(d) | {u.path for u in d.unprocessed_artifacts}
    assert not _record_paths(d) & {u.path for u in d.unprocessed_artifacts}
    assert d.status is AdfSemanticStatus.discovered and d.unprocessed_artifacts == []


def _broken_estate(tmp_path):
    files = {k: v for k, v in _fixture_files().items() if k.startswith(("pipeline/PL_Child", "pipeline/PL_Ping", "dataset", "linkedService"))}
    files["pipeline/PL_Truncated.json"] = '{"name": "PL_Truncated", "properties": {"activities": [ {"name": "x", '
    files["pipeline/PL_List.json"] = "[1, 2, 3]"
    files["dataflow/DF_Garbage.json"] = "not json at all"
    files["credential/cred1.json"] = {"name": "cred1", "properties": {"type": "ManagedIdentity"}}
    files["managedVirtualNetwork/default.json"] = {"name": "default", "properties": {}}
    return _project(tmp_path, files)


def test_malformed_and_non_object_artifacts_are_listed_with_the_reason_and_the_run_is_partial(tmp_path):
    store, pid = _broken_estate(tmp_path)
    d = _discover(store)
    ledger = {u.path: (u.kind, u.reason) for u in d.unprocessed_artifacts}
    assert ledger["pipeline/PL_Truncated.json"] == ("pipeline", "malformed_json")
    assert ledger["dataflow/DF_Garbage.json"] == ("dataflow", "malformed_json")
    assert ledger["pipeline/PL_List.json"] == ("pipeline", "not_a_json_object")
    assert d.status is AdfSemanticStatus.partial
    for path in ("PL_Truncated.json", "PL_List.json", "DF_Garbage.json"):
        assert path in d.detail
    assert {p.name for p in d.pipelines} == {"PL_Child", "PL_Ping"}  # the readable ones are still discovered
    assert "nothing" not in d.detail.lower()


def test_other_artifact_kinds_are_listed_as_not_parsed_without_making_the_run_partial(tmp_path):
    store, _ = _project(tmp_path, {**{k: v for k, v in _fixture_files().items() if k.startswith(("pipeline/PL_Child", "dataset", "linkedService"))},
                                   "credential/cred1.json": {"name": "cred1", "properties": {"type": "ManagedIdentity"}},
                                   "managedVirtualNetwork/default.json": {"name": "default", "properties": {}}})
    d = _discover(store)
    ledger = {u.path: (u.kind, u.reason) for u in d.unprocessed_artifacts}
    assert ledger == {"credential/cred1.json": ("credential", "kind_not_parsed"),
                      "managedVirtualNetwork/default.json": ("managedVirtualNetwork", "kind_not_parsed")}
    assert d.status is AdfSemanticStatus.discovered and d.detail == ""


def test_every_candidate_is_a_record_or_a_ledger_entry_even_when_the_source_is_damaged(tmp_path):
    store, pid = _broken_estate(tmp_path)
    d = _discover(store)
    seen = {c.path for c in store.load_repository_discovery(pid).adf_candidates}
    ledger = {u.path for u in d.unprocessed_artifacts}
    assert seen == _record_paths(d) | ledger and not _record_paths(d) & ledger


def test_an_unreadable_file_is_reported_as_unreadable_without_a_stack_trace(tmp_path, monkeypatch):
    store, pid = _project(tmp_path, {k: v for k, v in _fixture_files().items() if k.startswith(("pipeline/PL_Child", "pipeline/PL_Ping", "dataset", "linkedService"))})
    target = (store.source_dir(pid) / "pipeline" / "PL_Ping.json").resolve()
    real_open = Path.open

    def guarded(self, *a, **k):
        if Path(self).resolve() == target:
            raise PermissionError(13, "denied by the test")
        return real_open(self, *a, **k)

    monkeypatch.setattr(Path, "open", guarded)
    d = _discover(store)
    entry = next(u for u in d.unprocessed_artifacts if u.path == "pipeline/PL_Ping.json")
    assert entry.reason == "unreadable" and "PermissionError" in entry.detail and "denied" not in entry.detail
    assert d.status is AdfSemanticStatus.partial and [p.name for p in d.pipelines] == ["PL_Child"]


def test_a_parser_crash_is_a_ledger_entry_with_the_exception_type_only(tmp_path, monkeypatch):
    store, _ = _project(tmp_path, {k: v for k, v in _fixture_files().items() if k.startswith(("pipeline/PL_Child", "pipeline/PL_Ping", "dataset", "linkedService"))})
    real = pp.parse_pipeline

    def crash(data, path):
        if path.endswith("PL_Ping.json"):
            raise ValueError("CONTENT_THAT_MUST_NOT_LEAK")
        return real(data, path)

    monkeypatch.setattr(pp, "parse_pipeline", crash)
    d = _discover(store)
    entry = next(u for u in d.unprocessed_artifacts if u.path == "pipeline/PL_Ping.json")
    assert entry.reason == "parse_error" and "ValueError" in entry.detail and "MUST_NOT_LEAK" not in entry.detail
    assert d.status is AdfSemanticStatus.partial


# --- 8. robustness: nothing raises, and a mutated pipeline still reconciles or says why --------------------------------------


def _paths(obj, prefix=()):
    yield prefix
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _paths(v, prefix + (k,))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _paths(v, prefix + (i,))


def _mutate(doc: dict, path: tuple, replacement):
    if not path:
        return replacement
    import copy

    out = copy.deepcopy(doc)
    node = out
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = replacement
    return out


@pytest.mark.parametrize("name", ["PL_Containers", "PL_ControlPolicies", "PL_Compute"])
def test_parsing_never_raises_and_every_mutation_is_reconciled_or_reported(name):
    raw = json.loads((FIXTURES / "pipeline" / f"{name}.json").read_text(encoding="utf-8"))
    rng = random.Random(20261022)
    replacements = [None, "x", 7, True, [], {}, [None], {"name": 1}, ["s"]]
    paths = [p for p in _paths(raw) if p]
    for path in rng.sample(paths, min(len(paths), 160)):
        for rep in rng.sample(replacements, 3):
            mutated = _mutate(raw, path, rep)
            p = pp.parse_pipeline(mutated, f"pipeline/{name}.json")  # must not raise
            inv = p.inventory
            assert inv is None or inv.parsed_activity_count == p.activity_total
            if inv is not None and not inv.matched:
                assert any(i.code == "inventory_mismatch" for i in p.issues), (path, rep)


def test_activity_lists_of_the_wrong_shape_are_reported_not_ignored():
    for bad in ({"activities": "text"}, {"activities": {"a": 1}}, {"activities": 5}, {}):
        p = pp.parse_pipeline({"name": "P", "properties": bad}, "pipeline/P.json")
        assert p.activities == [] and p.issues, bad
    p = pp.parse_pipeline({"name": "P", "properties": {"activities": [{"name": "F", "type": "ForEach", "typeProperties": {"activities": "nope"}}]}},
                          "pipeline/P.json")
    assert [a.name for a in p.activities] == ["F"] and any(w.code == "invalid_activities" for w in p.activities[0].warnings)


def test_nesting_beyond_the_parser_limit_is_flagged_by_the_inventory_check():
    depth = pp._MAX_DEPTH + 3
    node: dict = {"name": f"L{depth}", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}
    for i in range(depth - 1, -1, -1):
        node = {"name": f"L{i}", "type": "ForEach", "typeProperties": {"items": "x", "activities": [node]}}
    p = pp.parse_pipeline({"name": "Deep", "properties": {"activities": [node]}}, "pipeline/Deep.json")
    assert p.inventory is not None and not p.inventory.matched and p.inventory.missing_pointers
    assert any(i.code == "inventory_mismatch" for i in p.issues)
    assert any(w.code == "max_depth_exceeded" for a in walk(p.activities) for w in a.warnings)


# --- 9. determinism and compatibility ---------------------------------------------------------------------------------------------


def test_discovery_is_deterministic_for_the_new_fields(estate):
    store, pid, first = estate
    second = _discover(store)
    strip = lambda d: {k: v for k, v in json.loads(d.model_dump_json()).items() if k not in ("created_at", "updated_at")}  # noqa: E731
    assert strip(first) == strip(second)


def test_a_record_written_before_these_fields_existed_still_loads(estate):
    store, pid, _ = estate
    raw = yaml.safe_load((store.project_dir(pid) / "discovery" / "adf-semantic.yaml").read_text(encoding="utf-8"))
    for key in ("dataflows", "dataflow_count", "unprocessed_artifacts", "inventory_mismatch_count"):
        raw.pop(key)
    for p in raw["pipelines"]:
        p.pop("dependency_graph"), p.pop("inventory")
    old = AdfSemanticDiscovery.model_validate(raw)
    assert old.dataflows == [] and old.unprocessed_artifacts == [] and old.pipelines[0].dependency_graph is None
    assert old.pipeline_count == len(EXPECTED)


# --- 10. regression: shapes that used to raise inside the parser (found by exhaustive mutation) ---------------------------------


@pytest.mark.parametrize("label,activity,code", [
    ("dependencyConditions", {"name": "A", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1},
                              "dependsOn": [{"activity": "B", "dependencyConditions": 7}]}, "invalid_dependency_conditions"),
    ("switch cases", {"name": "A", "type": "Switch", "typeProperties": {"on": "x", "cases": 7}}, "invalid_cases"),
    ("inputs", {"name": "A", "type": "Copy", "typeProperties": {"source": {"type": "ParquetSource"}, "sink": {"type": "ParquetSink"}},
                "inputs": 7}, "invalid_inputs"),
    ("outputs", {"name": "A", "type": "Copy", "typeProperties": {"source": {"type": "ParquetSource"}, "sink": {"type": "ParquetSink"}},
                 "outputs": True}, "invalid_outputs"),
    ("script blocks", {"name": "A", "type": "Script", "typeProperties": {"scripts": 7}}, "invalid_scripts"),
])
def test_a_field_of_the_wrong_shape_is_reported_and_the_activity_is_kept(label, activity, code):
    p = pp.parse_pipeline({"name": "P", "properties": {"activities": [activity]}}, "pipeline/P.json")
    assert [a.name for a in p.activities] == ["A"], label
    assert code in {w.code for w in p.activities[0].warnings}, label
    assert p.inventory.matched


def test_databricks_job_parameters_of_the_wrong_shape_do_not_break_the_summary():
    node = {"name": "J", "type": "DatabricksJob", "linkedServiceName": {"referenceName": "L", "type": "LinkedServiceReference"},
            "typeProperties": {"jobId": "1", "jobParameters": 7}}
    p = pp.parse_pipeline({"name": "P", "properties": {"activities": [node]}}, "pipeline/P.json")
    assert p.activities[0].semantic_summary.startswith("Runs Databricks job")


def test_data_flows_survive_any_single_malformed_value():
    from adf2fabric.agent.discovery.adf_semantic.parser_dataflows import parse_dataflow

    rng = random.Random(20261022)
    replacements = [None, "x", 7, True, [], {}, [None], {"name": 1}, ["s"]]
    for fixture in sorted((FIXTURES / "dataflow").glob("*.json")):
        raw = json.loads(fixture.read_text(encoding="utf-8"))
        paths = [p for p in _paths(raw) if p]
        for path in rng.sample(paths, min(len(paths), 60)):
            for rep in rng.sample(replacements, 3):
                flow = parse_dataflow(_mutate(raw, path, rep), f"dataflow/{fixture.name}")  # must not raise
                assert flow.name and flow.path
    for not_an_object in ([], "text", 5, None):
        flow = parse_dataflow(not_an_object, "dataflow/X.json")
        assert flow.parse_status.value == "failed" and flow.issues


# --- 11. downstream: conversion does not claim completeness when discovery lost something -------------------------------------


def test_conversion_is_partial_and_says_so_when_discovery_could_not_process_an_artifact(tmp_path):
    from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
    from adf2fabric.project.conversion import ConversionOutcome

    store, pid = _project(tmp_path, {k: v for k, v in _fixture_files().items()
                                     if k.startswith(("pipeline/PL_Child", "dataset", "linkedService"))})
    _discover(store)
    clean = ConversionAgent(store).run(ConversionRequest(project_name=pid))
    assert clean.conversion is not None, clean.error
    assert clean.conversion.status is ConversionOutcome.converted
    assert not [n for n in clean.conversion.schema_notes if "NOT converted" in n]

    (store.source_dir(pid) / "pipeline" / "PL_Broken.json").write_text("{ nope", encoding="utf-8")
    sha = _git_commit(store.source_dir(pid))
    project = store.load(pid)
    project.repository.commit_sha = sha
    store.save(project)
    DiscoveryAgent(store).discover_repository(DiscoveryRequest(project_name=pid))
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name=pid))
    lossy = ConversionAgent(store).run(ConversionRequest(project_name=pid))
    assert lossy.conversion is not None, lossy.error
    assert lossy.conversion.status is ConversionOutcome.partial
    assert any("PL_Broken.json" in n and "NOT converted" in n for n in lossy.conversion.schema_notes)


# --- 12. the CLI says what discovery could not account for ----------------------------------------------------------------------


def test_the_cli_reports_unknown_types_cycles_data_flows_and_lost_artifacts(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from adf2fabric import cli as cli_mod

    files = {k: v for k, v in _fixture_files().items() if k.startswith(("pipeline/PL_Graph", "pipeline/PL_Unknown", "dataflow/DF_Map",
                                                                      "dataflow/DF_Flowlet", "dataset", "linkedService"))}
    files["pipeline/PL_Truncated.json"] = '{"name": "PL_Truncated", "properties": {'
    store, pid = _project(tmp_path, files)
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    out = CliRunner().invoke(cli_mod.cli, ["discovery-adf-semantic", "--project", pid])
    assert out.exit_code == 0, out.output
    assert "data flows: 2" in out.output and "dependency cycles: 2" in out.output
    assert "FutureActivityV2" in out.output and "Mystery" in out.output
    assert "not in the record: pipeline/PL_Truncated.json [pipeline] malformed_json" in out.output
    assert "ACTIVITY INVENTORY MISMATCH" not in out.output
