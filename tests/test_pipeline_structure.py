"""Pipeline composition: read iteratively (any depth), compared ADF against the deployed Fabric twin, and reported with the latest run."""

from __future__ import annotations

import base64
import json

import pytest

from adf2fabric.agent.deployment.fabric_deploy import FabricClient
from adf2fabric.agent.structure import report as R
from adf2fabric.agent.structure.compare import compare
from adf2fabric.agent.structure.tree import adf_structure, fabric_structure, iter_activities
from tests.test_conversion_agent import FILES
from tests.test_partial_source import _convert

G = lambda n: f"{n:08d}-0000-0000-0000-000000000000"  # noqa: E731


def _act(name, type_="Wait", deps=(), **tp):
    a = {"name": name, "type": type_, "typeProperties": tp}
    if deps:
        a["dependsOn"] = [{"activity": d, "dependencyConditions": [c]} for d, c in deps]
    return a


def _recursive(items, parent=None, depth=0, branch=""):  # the reference the iterative walker must agree with
    for a in items:
        yield a["name"], parent, depth, branch
        tp = a.get("typeProperties", {})
        for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
            if isinstance(tp.get(key), list):
                yield from _recursive(tp[key], a["name"], depth + 1, key)
        for i, c in enumerate(tp.get("cases", [])):
            yield from _recursive(c["activities"], a["name"], depth + 1, f"cases[{i}]")


def test_the_iterative_walk_gives_the_same_order_as_the_recursive_one():
    tree = [_act("A"), _act("If", "IfCondition", ifTrueActivities=[_act("T1"), _act("FE", "ForEach", activities=[_act("Deep")])], ifFalseActivities=[_act("F1")]),
            _act("Sw", "Switch", cases=[{"value": "1", "activities": [_act("C1")]}, {"value": "2", "activities": [_act("C2")]}], defaultActivities=[_act("D1")]), _act("Z")]
    assert [(a["name"], p, d, b) for a, p, d, b in iter_activities(tree)] == list(_recursive(tree))


def test_a_pipeline_nested_thousands_of_levels_deep_is_read_without_recursion():
    inner = _act("Leaf")
    for i in range(5000):
        inner = _act(f"If{i}", "IfCondition", ifTrueActivities=[inner])
    s = adf_structure({"name": "P", "properties": {"activities": [inner]}})
    assert len(s.nodes) == 5001 and s.max_depth == 5001 and s.nodes[-1].name == "Leaf"


def test_adf_structure_names_what_each_activity_uses():
    p = {"name": "PL", "properties": {"parameters": {"a": {}}, "variables": {"v": {}}, "activities": [
        {"name": "Run", "type": "ExecutePipeline", "typeProperties": {"pipeline": {"referenceName": "PL_Child", "type": "PipelineReference"}}},
        {"name": "Cp", "type": "Copy", "dependsOn": [{"activity": "Run", "dependencyConditions": ["Succeeded", "Failed"]}],
         "inputs": [{"referenceName": "DS_In", "type": "DatasetReference"}], "outputs": [{"referenceName": "DS_Out", "type": "DatasetReference"}], "typeProperties": {}},
        {"name": "Nb", "type": "SynapseNotebook", "typeProperties": {"notebook": {"referenceName": "NB_X"}}, "linkedServiceName": {"referenceName": "LS_S"}}]}}
    s = adf_structure(p)
    run, cp, nb = s.nodes
    assert run.calls == ["PL_Child"] and cp.datasets == ["DS_In", "DS_Out"] and cp.depends == [("Run", ("Failed", "Succeeded"))]
    assert nb.items == ["NB_X"] and nb.connections == ["LS_S"] and s.parameters == ["a"] and s.variables == ["v"] and s.called == ["PL_Child"]


def _pair():
    adf = adf_structure({"name": "PL", "properties": {"activities": [
        {"name": "Run", "type": "ExecutePipeline", "typeProperties": {"pipeline": {"referenceName": "PL_Child"}}},
        {"name": "Cp", "type": "Copy", "dependsOn": [{"activity": "Run", "dependencyConditions": ["Succeeded"]}],
         "inputs": [{"referenceName": "DS_In"}], "outputs": [{"referenceName": "DS_Out"}], "typeProperties": {}}], "parameters": {"p": {}}}})
    content = {"properties": {"parameters": {"p": {}}, "activities": [
        {"name": "Run", "type": "InvokePipeline", "typeProperties": {"pipelineId": G(7)}, "externalReferences": {"connection": G(1)}},
        {"name": "Cp", "type": "Copy", "dependsOn": [{"activity": "Run", "dependencyConditions": ["Succeeded"]}], "typeProperties": {
            "source": {"datasetSettings": {"type": "DelimitedText", "externalReferences": {"connection": G(2)}}},
            "sink": {"datasetSettings": {"type": "Parquet", "externalReferences": {"connection": G(2)}}}}}]}}
    return adf, content


def _fab(content):
    return fabric_structure("PL", content, {G(7): "PL_Child"})


def test_matching_pipelines_are_equivalent_and_the_conversion_differences_are_documented():
    adf, content = _pair()
    p = compare(adf, _fab(content))
    assert p.verdict == "equivalent_with_documented_differences" and not p.differences and all(c.ok for c in p.checks)
    assert any("ExecutePipeline became InvokePipeline" in d for d in p.documented)


@pytest.mark.parametrize("mutate,check", [
    (lambda c: c["properties"]["activities"].pop(1), "activities present"),
    (lambda c: c["properties"]["activities"][1]["dependsOn"][0].update(dependencyConditions=["Failed"]), "dependencies"),
    (lambda c: c["properties"]["activities"][1].pop("dependsOn"), "dependencies"),
    (lambda c: c["properties"]["activities"][0]["typeProperties"].update(pipelineId=G(99)), "pipelines started"),
    (lambda c: c["properties"]["activities"][1]["typeProperties"]["sink"].pop("datasetSettings"), "datasets inlined"),
    (lambda c: c["properties"]["activities"][1]["typeProperties"]["source"]["datasetSettings"]["externalReferences"].update(connection="<connection:LS_X>"), "connections bound"),
    (lambda c: c["properties"]["activities"][0].update(type="Wait"), "activity types"),
    (lambda c: c["properties"].pop("parameters"), "parameters"),
    (lambda c: c["properties"]["activities"][0].update(name="Other"), "activities present"),
])
def test_every_kind_of_drift_is_caught_and_named(mutate, check):
    adf, content = _pair()
    mutate(content)
    p = compare(adf, _fab(content))
    assert p.verdict == "differs" and p.differences
    assert next(c for c in p.checks if c.name == check).ok is False


def test_a_container_moved_to_another_parent_is_a_nesting_difference():
    adf = adf_structure({"name": "PL", "properties": {"activities": [_act("If", "IfCondition", ifTrueActivities=[_act("In")])]}})
    fab = fabric_structure("PL", {"properties": {"activities": [_act("If", "IfCondition", ifTrueActivities=[]), _act("In")]}})
    assert next(c for c in compare(adf, fab).checks if c.name == "nesting").ok is False


# --- end to end through a fake workspace ---------------------------------------------------------------------------------------


class FakeFabric(FabricClient):
    """Serves the generated pipelines back as 'deployed', with one Completed run each."""

    def __init__(self, store, pid, ids, async_definition=False):
        self.store, self.pid, self.ids, self.async_definition = store, pid, ids, async_definition
        self._sleep = lambda _s: None
        self.calls = []

    class _R:
        def __init__(self, code, body=None, headers=None):
            self.status_code, self._b, self.headers, self.content = code, body, headers or {}, b"x"

        def json(self):
            return self._b

    def _call(self, method, url, body=None):
        self.calls.append((method, url))
        R_ = self._R
        if url.endswith("/dataPipelines") or "/dataPipelines?" in url:
            return R_(200, {"value": [{"id": i, "displayName": n} for n, i in self.ids.items()]})
        if url.endswith("/items"):
            return R_(200, {"value": [{"id": i, "displayName": n} for n, i in self.ids.items()]})
        if "/getDefinition" in url:
            item = url.split("/dataPipelines/")[1].split("/")[0]
            name = next(n for n, i in self.ids.items() if i == item)
            text = (self.store.conversion_dir(self.pid) / "pipelines" / f"{name}.DataPipeline" / "pipeline-content.json").read_text("utf-8")
            text = text.replace("<item:pipeline:PL_Child>", self.ids.get("PL_Child", G(11))).replace("<workspace-id>", "w")
            payload = {"definition": {"parts": [{"path": "pipeline-content.json", "payload": base64.b64encode(text.encode()).decode()}]}}
            if self.async_definition:
                self._pending = payload
                return R_(202, {}, {"Location": "https://api.fabric.microsoft.com/v1/operations/" + G(5), "Retry-After": "0"})
            return R_(200, payload)
        if url.endswith(G(5)):
            return R_(200, {"status": "Succeeded"})
        if url.endswith(G(5) + "/result"):
            return R_(200, self._pending)
        if "/jobs/instances" in url:
            return R_(200, {"value": [{"id": G(40), "status": "Completed", "startTimeUtc": "2026-10-26T01:00:00Z", "endTimeUtc": "2026-10-26T01:00:30Z"}]})
        if "queryactivityruns" in url:
            return R_(200, [{"activityName": "Copy_Customer", "activityType": "Copy", "status": "Succeeded", "durationInMs": 4200,
                             "activityRunStart": "2026-10-26T01:00:05Z", "output": {"rowsRead": 4, "rowsCopied": 4}}])
        raise AssertionError(f"unexpected call {method} {url}")


def _deployed_project(tmp_path):
    res = tmp_path / "res.json"
    res.write_text(json.dumps([{"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": G(1)}, {"type": "LinkedServiceToConnectionId", "key": "LS_LAKE", "value": G(2)},
                               {"type": "ItemToId", "key": "connection:invoke-pipeline", "value": G(3)}, {"type": "ItemToId", "key": "pipeline:PL_Child", "value": G(11)},
                               {"type": "WorkspaceId", "key": "*", "value": G(9)}]), encoding="utf-8")
    store, pid, conv = _convert(tmp_path, FILES, resolutions_path=str(res))
    return store, pid, conv


@pytest.mark.parametrize("async_definition", [False, True])
def test_the_report_reads_the_deployed_pipelines_and_finds_them_equal(tmp_path, async_definition):
    store, pid, _ = _deployed_project(tmp_path)
    ids = {"PL_Main": G(10), "PL_Child": G(11)}
    data = R.collect(store, pid, "w", lambda: FakeFabric(store, pid, ids, async_definition), workers=3)
    rows = {r["pipeline"]: r for r in data["pipelines"]}
    assert rows["PL_Main"]["fabric"].called == ["PL_Child"] and rows["PL_Main"]["adf"].called == ["PL_Child"]
    assert rows["PL_Main"]["parity"].verdict.startswith("equivalent"), rows["PL_Main"]["parity"].differences
    assert data["progress"]["structure_equivalent"] == 2 and data["progress"]["last_run_completed"] == 2
    text = R.render(data)
    assert "`PL_Main` -> `PL_Child`" in text and "rowsRead 4" in text and "4.2s" in text and "Behaviour in the latest Fabric run" in text
    assert json.loads(R.to_json(data))["pipelines"][0]["parity"]["verdict"]


def test_a_pipeline_that_is_not_deployed_is_reported_as_such_not_hidden(tmp_path):
    store, pid, _ = _deployed_project(tmp_path)
    data = R.collect(store, pid, "w", lambda: FakeFabric(store, pid, {"PL_Main": G(10)}), workers=2)
    row = next(r for r in data["pipelines"] if r["pipeline"] == "PL_Child")
    assert row["deployed"] is False and "Not deployed" in R.render(data)
    assert data["progress"]["deployed"] == 1


def test_nothing_in_the_report_run_writes_to_fabric(tmp_path):
    store, pid, _ = _deployed_project(tmp_path)
    fake = FakeFabric(store, pid, {"PL_Main": G(10), "PL_Child": G(11)})
    R.collect(store, pid, "w", lambda: fake)
    assert all(m == "GET" or url.endswith(("/getDefinition", "/queryactivityruns")) for m, url in fake.calls)  # the two POSTs are reads
