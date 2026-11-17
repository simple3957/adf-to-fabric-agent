"""The master compatibility catalog (adf2fabric/catalog/compatibility.json) and the project's activity mapping that follows it.

The catalog is the one place the converter's facts come from (rules, options, switches, lifecycle, connectors, Fabric schema). These
tests check its shape, that the code reads it, and that Convert follows a person's per-activity choice and refuses anything else."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("fastapi")

import adf2fabric.agent.conversion.handlers  # noqa: E402,F401 - registers every rule
from adf2fabric.agent.conversion import compatibility, connectors, fabric_schema, mapping_catalog  # noqa: E402
from adf2fabric.agent.conversion import project_mapping as PM  # noqa: E402
from adf2fabric.agent.conversion.agent import ConversionAgent  # noqa: E402
from adf2fabric.agent.conversion.models import ConversionRequest  # noqa: E402
from adf2fabric.agent.conversion.registry import REGISTRY  # noqa: E402
from adf2fabric.agent.discovery.adf_semantic.activity_registry import ACTIVITY_REGISTRY  # noqa: E402
from adf2fabric.catalog import master  # noqa: E402
from tests.test_conversion_agent import FILES  # noqa: E402
from tests.test_partial_source import _convert  # noqa: E402

STATUSES = {"converted", "converted_with_differences", "manual_required", "blocked"}


def test_every_activity_type_discovery_reads_has_one_capability_and_the_registry_is_built_from_it():
    m = master()
    types = {t.lower() for c in m["capabilities"].values() for t in c["adf_types"]}
    assert types == set(REGISTRY) == {s.type.lower() for s in ACTIVITY_REGISTRY.values()}
    for name, cap in m["capabilities"].items():
        rule = REGISTRY[cap["adf_types"][0].lower()]
        assert rule.name == name and list(rule.fabric_types) == [f["type"] for f in cap["fabric_activities"]]
        assert rule.strategy.value == cap["strategy"] and list(rule.unsupported) == cap["not_carried"]


def test_each_option_has_a_valid_status_and_names_only_fabric_activities_the_schema_documents():
    m = master()
    documented = set(m["fabric_schema"]["activities"])
    for cap in m["capabilities"].values():
        for adf_type, e in cap["by_adf_type"].items():
            assert e["options"], adf_type
            for o in e["options"]:
                assert o["status"] in STATUSES and o["when"] and o["how"], (adf_type, o)
                assert (o["status"] == "blocked") == (o["fabric"] == []), (adf_type, o)  # blocked = a Fail placeholder only
                assert set(o["fabric"]) <= documented, (adf_type, o["fabric"])
                if o.get("switch"):
                    sw = m["switches"][o["switch"]["kind"]]
                    assert o["switch"]["value"] in sw["values"] and adf_type in sw["applies_to"], (adf_type, o["switch"])


def test_every_source_a_section_cites_exists_with_a_date():
    m = master()
    cited = {s for c in m["concepts"] for s in c["sources"]} | set(m["connectors"]["sources"]) | set(m["fabric_schema"]["sources"])
    assert cited <= set(m["sources"])
    assert all(v.get("read_on") for v in m["sources"].values())


def test_the_modules_read_the_catalog():
    m = master()
    assert set(mapping_catalog.CATALOG) == {t for c in m["capabilities"].values() for t in c["by_adf_type"]}
    assert set(fabric_schema.FABRIC_ACTIVITIES) == set(m["fabric_schema"]["activities"])
    ls = m["connectors"]["adf_linked_services"]
    assert connectors.verdict("AzureBlobFS")[0] == "supported" and ls["azureblobfs"]["verdict"] == "supported"
    assert connectors.verdict("AzureDataLakeAnalytics")[0] == "unsupported"
    assert compatibility.for_activity("DataLakeAnalyticsU-SQL")  # the ADF type as written (an older key never matched it)


def test_the_project_mapping_lists_every_activity_with_its_options_and_keeps_a_choice(tmp_path):
    store, pid, conv = _convert(tmp_path, FILES)
    data = PM.load(store, pid)  # Convert wrote it
    ids = {r["id"] for r in data["activities"]}
    assert ids == {a.source_activity_id for p in conv.pipelines for a in p.activities}
    copy = next(r for r in data["activities"] if r["adf_type"] == "Copy")
    assert copy["switch"] == "storage" and copy["allowed"] == ["connection", "lakehouse"] and copy["chosen"] is None
    assert any("Copy" in o["fabric"] for o in copy["options"])
    copy["chosen"] = "connection"
    (store.conversion_dir(pid) / PM.FILE).write_text(json.dumps(data), encoding="utf-8")
    again = PM.build(store, pid)
    assert next(r for r in again["activities"] if r["id"] == copy["id"])["chosen"] == "connection"  # kept
    assert PM.check(again, conv) == []  # the converter's output is what the catalog lists


def test_convert_refuses_a_choice_the_catalog_does_not_allow_and_follows_an_allowed_one(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)
    data = PM.load(store, pid)
    copy = next(r for r in data["activities"] if r["adf_type"] == "Copy")
    copy["chosen"] = "somewhere-else"
    (store.conversion_dir(pid) / PM.FILE).write_text(json.dumps(data), encoding="utf-8")
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    assert not r.success and "not allowed" in r.error and copy["id"] in r.error
    copy["chosen"] = "lakehouse"  # allowed, but this project has no storage map or lakehouse: stays on its connection, says so
    (store.conversion_dir(pid) / PM.FILE).write_text(json.dumps(data), encoding="utf-8")
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    assert r.success
    a = next(a for p in r.conversion.pipelines for a in p.activities if a.source_activity_id == copy["id"])
    assert any(w.code == "storage_choice_unavailable" for w in a.warnings)
