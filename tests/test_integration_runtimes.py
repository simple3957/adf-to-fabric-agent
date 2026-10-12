"""Integration runtimes: classification, and when a Fabric gateway is (not) required."""

from __future__ import annotations

import pytest

from adf2fabric.agent.conversion.agent import build_project_context
from adf2fabric.agent.conversion.context import Resolutions, gateway_need
from adf2fabric.agent.conversion.engine import ConversionEngine
from adf2fabric.agent.discovery.adf_semantic.parser import classify_integration_runtime
from tests.conversion_support import DS_LAKE, LAKE_LS, copy_activity, dataset, linked_service, pipe, ref, semantic
from tests.test_adf_semantic_discovery import _make_project, _run


def ir(type_, tp=None, **props):
    return {"name": "IR", "properties": {"type": type_, "typeProperties": tp or {}, **props}}


@pytest.mark.parametrize("definition, expected", [
    (ir("SelfHosted"), "self_hosted"),
    (ir("Managed", {"computeProperties": {"location": "AutoResolve"}}), "azure"),
    (ir("Managed", {"computeProperties": {}}, managedVirtualNetwork={"referenceName": "default"}), "managed_vnet"),
    (ir("Managed", {"ssisProperties": {}}), None),  # Azure-SSIS: not a data-store gateway question
    (ir("SomethingNew"), None),
    ({"name": "IR"}, None),
])
def test_runtime_classification(definition, expected):
    assert classify_integration_runtime(definition) == expected


def test_gateway_need_by_kind():
    assert gateway_need(None, None) is None
    assert gateway_need("AutoResolveIntegrationRuntime", None) is None
    assert gateway_need("IR", "azure") is None  # a Fabric cloud connection replaces it
    assert "on-premises data gateway" in gateway_need("IR", "self_hosted")
    assert "virtual network data gateway" in gateway_need("IR", "managed_vnet")
    assert "could not be classified" in gateway_need("IR", None)  # unknown stays a cautious prerequisite


def _convert(kind):
    ls = linked_service("LS_SRC", "SqlServer", connect_via="IR")
    ds = dataset("DS_SRC", "SqlServerTable", "LS_SRC", {"table": "t"})
    c = copy_activity()
    c["inputs"] = [ref("Dataset", "DS_SRC")]
    d = semantic([pipe(c, name="PL")], datasets=[ds, DS_LAKE], linked_services=[ls, LAKE_LS])
    for x in d.linked_services:
        if x.integration_runtime:
            x.integration_runtime_kind = kind
    record, _ = ConversionEngine(build_project_context(d, Resolutions())).convert_pipeline(d.pipelines[0])
    return {(q.kind, q.name) for q in record.prerequisites}, record


def test_plain_azure_runtime_needs_no_gateway_but_still_needs_the_connection():
    prereqs, record = _convert("azure")
    assert not any(k == "gateway" for k, _ in prereqs)
    assert ("connection", "LS_SRC") in prereqs


@pytest.mark.parametrize("kind, phrase", [("self_hosted", "on-premises"), ("managed_vnet", "virtual network"),
                                          (None, "could not be classified")])
def test_other_runtimes_still_require_a_gateway(kind, phrase):
    prereqs, record = _convert(kind)
    assert ("gateway", "IR") in prereqs
    detail = next(q.detail for q in record.prerequisites if q.kind == "gateway")
    assert phrase in detail


def test_discovery_stamps_the_runtime_kind_onto_the_linked_services_that_use_it(tmp_path):
    files = {
        "integrationRuntime/IR.json": ir("Managed", {"computeProperties": {"location": "AutoResolve"}}),
        "integrationRuntime/Shir.json": {"name": "Shir", "properties": {"type": "SelfHosted"}},
        "linkedService/LS_A.json": {"name": "LS_A", "properties": {
            "type": "AzureBlobFS", "typeProperties": {"url": "https://a.dfs.core.windows.net"},
            "connectVia": {"referenceName": "IR", "type": "IntegrationRuntimeReference"}}},
        "linkedService/LS_B.json": {"name": "LS_B", "properties": {
            "type": "SqlServer", "typeProperties": {},
            "connectVia": {"referenceName": "Shir", "type": "IntegrationRuntimeReference"}}},
        "linkedService/LS_C.json": {"name": "LS_C", "properties": {
            "type": "SqlServer", "typeProperties": {},
            "connectVia": {"referenceName": "Missing", "type": "IntegrationRuntimeReference"}}},
        "linkedService/LS_D.json": {"name": "LS_D", "properties": {"type": "AzureBlobFS", "typeProperties": {}}},
    }
    store, _ = _make_project(tmp_path, files)
    d = _run(store).discovery
    kinds = {x.name: x.integration_runtime_kind for x in d.linked_services}
    assert kinds == {"LS_A": "azure", "LS_B": "self_hosted", "LS_C": None, "LS_D": None}
