"""ADF change data capture resources: recognised, described, reported with the Fabric Copy job path (never converted)."""

from __future__ import annotations

from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.conversion import compatibility as compat
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest, parser
from adf2fabric.agent.discovery.repository.artifacts import _ADF_DIR_KIND, _kind_from_type
from adf2fabric.project.discovery import AdfArtifactKind
from tests.test_adf_semantic_discovery import _make_project
from tests.test_conversion_agent import FILES

CDC = {
    "name": "CDC_Orders",
    "type": "Microsoft.DataFactory/factories/adfcdcs",
    "properties": {
        "description": "orders",
        "status": "Running",
        "policy": {"mode": "Microbatch", "recurrence": {"frequency": "Minute", "interval": 15}},
        "sourceConnectionsInfo": [{
            "connection": {"linkedService": {"referenceName": "LS_SQL", "type": "LinkedServiceReference"},
                           "linkedServiceType": "AzureSqlDatabase", "type": "linkedservicetype"},
            "sourceEntities": [{"name": "dbo.Orders", "properties": {}}, {"name": "dbo.Customers", "properties": {}}]}],
        "targetConnectionsInfo": [{
            "connection": {"linkedService": {"referenceName": "LS_LAKE", "type": "LinkedServiceReference"},
                           "linkedServiceType": "AzureBlobFS", "type": "linkedservicetype"},
            "targetEntities": [{"name": "orders", "properties": {}}],
            "dataMapperMappings": [{"sourceEntityName": "dbo.Orders", "targetEntityName": "orders"},
                                   {"sourceEntityName": "dbo.Customers", "targetEntityName": "customers"}]}],
    },
}


def test_the_artifact_kind_is_recognised_by_its_arm_type_and_by_the_intake_folder_name():
    assert _kind_from_type("Microsoft.DataFactory/factories/adfcdcs") == AdfArtifactKind.change_data_capture
    assert _ADF_DIR_KIND["changedatacapture"] == AdfArtifactKind.change_data_capture


def test_parser_describes_the_resource_without_inventing_anything():
    c = parser.parse_change_data_capture(CDC, "changeDataCapture/CDC_Orders.json")
    assert (c.name, c.status, c.mode, c.frequency, c.interval) == ("CDC_Orders", "Running", "Microbatch", "Minute", 15)
    assert c.source_linked_services == ["LS_SQL"] and c.target_linked_services == ["LS_LAKE"]
    assert c.source_tables == ["dbo.Customers", "dbo.Orders"] and c.target_tables == ["orders"] and c.mapping_count == 2
    empty = parser.parse_change_data_capture({"name": "X", "properties": {}}, "p")
    assert empty.source_tables == [] and empty.mode is None and empty.mapping_count == 0


def _project(tmp_path):
    files = dict(FILES)
    files["changeDataCapture/CDC_Orders.json"] = CDC
    store, pid = _make_project(tmp_path, files)
    result = AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    return store, pid, result.discovery


def test_semantic_discovery_records_cdc_resources_and_does_not_count_them_as_pipelines(tmp_path):
    _, _, d = _project(tmp_path)
    assert [c.name for c in d.change_data_captures] == ["CDC_Orders"]
    assert d.pipeline_count == 2 and d.failed_count == 0


def test_scan_reports_the_copy_job_path_for_each_cdc_resource_even_when_pipelines_are_filtered(tmp_path):
    _, _, d = _project(tmp_path)
    (f,) = [x for x in compat.scan(d, {"PL_Main"}) if x.id == "artifact.adf_cdc"]
    assert f.subject == "CDC_Orders" and f.status == "replaced" and f.kind == "artifact" and not f.emitted
    assert "Copy jobs" in f.summary and "2 table(s)" in f.summary and "LS_SQL" in f.summary and "dbo.Orders" in f.summary
    assert any("Copy job" in a for a in f.alternatives) and f.source_url.startswith("https://learn.microsoft.com/")


def test_the_conversion_report_lists_it_and_the_estate_still_converts(tmp_path):
    store, pid, _ = _project(tmp_path)
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    assert r.success and any(f.id == "artifact.adf_cdc" for f in r.conversion.compatibility)
    text = (store.conversion_dir(pid) / "conversion-report.md").read_text(encoding="utf-8")
    assert "## Compatibility findings" in text and "`CDC_Orders`" in text and "Copy job" in text


def test_no_cdc_means_no_finding(tmp_path):
    store, pid = _make_project(tmp_path, dict(FILES))
    d = AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj")).discovery
    assert d.change_data_captures == [] and not [f for f in compat.scan(d) if f.kind == "artifact"]
