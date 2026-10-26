"""A source that lacks linked services or datasets (a partial ZIP or Git folder): what is blocked, what is said, and how it is resolved."""

from __future__ import annotations

import json

import pytest

from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from tests.test_adf_semantic_discovery import _make_project
from tests.test_conversion_agent import FILES

G = lambda n: f"{n:08d}-0000-0000-0000-000000000000"  # noqa: E731


def _convert(tmp_path, files, **kw):
    store, pid = _make_project(tmp_path, files)
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj", **kw))
    assert r.success, r.error
    return store, pid, r.conversion


def _without(prefix):
    return {k: v for k, v in FILES.items() if not k.startswith(prefix)}


def _statuses(conv):
    return {p.source_pipeline: p.status.value for p in conv.pipelines}


def test_a_source_without_linked_services_says_which_are_missing_and_how_to_get_them(tmp_path):
    _, _, conv = _convert(tmp_path, _without("linkedService/"))
    f = next(x for x in conv.preflight if x.code == "missing_linked_services")
    assert "LS_SQL" in f.subject and "LS_LAKE" in f.subject and f.pipelines
    assert all(w in f.fix for w in ("linkedService/", "ARMTemplateForFactory.json", "--artifact-kind linkedservice", "--resolutions"))
    acts = {a.source_name: a for p in conv.pipelines for a in p.activities}
    assert acts["Copy_Customer"].status.value == "manual_required"  # nothing is guessed: the connection cannot be described
    assert acts["Log"].status.value == "blocked"  # a stored procedure's store type comes from the linked service


def test_the_resolutions_template_asks_for_a_missing_linked_service_the_way_the_converter_reads_it(tmp_path):
    store, pid, _ = _convert(tmp_path, _without("linkedService/"))
    template = json.loads((store.conversion_dir(pid) / "resolutions.template.json").read_text("utf-8"))
    assert {"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": "<Fabric connection GUID>"} in template
    assert not any(e["key"] == "connection:LS_SQL" for e in template)  # the old template wrote this, and the converter ignored it


@pytest.mark.parametrize("entry", [lambda n, g: {"type": "LinkedServiceToConnectionId", "key": n, "value": g},
                                   lambda n, g: {"type": "ItemToId", "key": f"connection:{n}", "value": g}])  # the second is what old templates wrote
def test_binding_a_connection_resolves_a_missing_linked_service_with_an_honest_difference(tmp_path, entry):
    f = tmp_path / "res.json"
    f.write_text(json.dumps([entry("LS_SQL", G(1)), entry("LS_LAKE", G(2))]), encoding="utf-8")
    store, pid, conv = _convert(tmp_path, _without("linkedService/"), resolutions_path=str(f))
    main = next(p for p in conv.pipelines if p.source_pipeline == "PL_Main")
    acts = {a.source_name: a for a in main.activities}
    assert acts["Copy_Customer"].status.value in ("converted", "converted_with_differences") and main.structural_valid
    assert acts["Log"].status.value == "blocked" and any(w.code == "stored_procedure_linked_service_not_in_source" for w in acts["Log"].warnings)
    text = (store.conversion_dir(pid) / "pipelines" / "PL_Main.DataPipeline" / "pipeline-content.json").read_text("utf-8")
    assert G(1) in text
    report = (store.conversion_dir(pid) / "conversion-report.md").read_text("utf-8")
    assert "not in the source" in report and "connector type is not checked" in report  # the gap stays visible, never silently closed


def test_a_source_without_datasets_blocks_the_activities_that_need_them_and_says_so(tmp_path):
    _, _, conv = _convert(tmp_path, _without("dataset/"))
    f = next(x for x in conv.preflight if x.code == "missing_datasets")
    assert f.severity == "error" and "DS_SQL" in f.subject and "dataset/" in f.fix
    assert _statuses(conv)["PL_Main"] in ("blocked", "manual_required")
    assert _statuses(conv)["PL_Main"] != "converted"


def test_a_complete_source_has_no_missing_artifact_finding(tmp_path):
    _, _, conv = _convert(tmp_path, FILES)
    assert not [x for x in conv.preflight if x.code.startswith("missing_")]
