"""Compatibility catalog: shape, lookups, engine hooks, project scan, report, and the skill playbook staying in sync."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from adf2fabric.agent.conversion import compatibility as compat
from adf2fabric.agent.conversion.report import render_report
from adf2fabric.agent.discovery.adf_semantic import parser
from adf2fabric.project.conversion import ConversionOutcome, ConversionStatus as S, Impact, ProjectConversion
from tests.conversion_support import (
    DS_LAKE, DS_SQL, LAKE_LS, SQL_LS, act, convert, copy_activity, dataset, linked_service, pipe, ref, semantic,
)

ROOT = Path(__file__).resolve().parent.parent
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


# --- catalog shape (each entry must stay sourced and dated) -------------------------------------------------


def test_catalog_entries_are_well_formed_and_sourced():
    ids = [e.id for e in compat.CATALOG]
    assert len(ids) == len(set(ids)) and len(ids) > 40
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", compat.VERIFIED_ON)
    for e in compat.CATALOG:
        assert e.kind in ("activity", "linked_service", "artifact")
        assert e.status in (compat.REMOVED, compat.DEPRECATED, compat.PREVIEW, compat.NOT_AVAILABLE, compat.REPLACED)
        assert e.source_url.startswith("https://learn.microsoft.com/")
        assert e.types and all(t == t.lower() for t in e.types)
        assert e.summary and e.alternatives
        assert e.date is None or re.fullmatch(r"\d{4}-\d{2}-\d{2}", e.date)
        assert e.versions is None or e.kind == "linked_service"
        assert compat.issue_code(e).startswith("compat_") and "." not in compat.issue_code(e)


def test_every_catalog_id_is_documented_in_the_skill_playbook():
    text = (ROOT / "skills" / "conversion" / "compatibility-playbook.md").read_text(encoding="utf-8")
    missing = [e.id for e in compat.CATALOG if e.id not in text and not _covered_by_group(e.id, text)]
    assert not missing, f"document these catalog ids in skills/conversion/compatibility-playbook.md: {missing}"


def _covered_by_group(entry_id: str, text: str) -> bool:
    """Connector families share one playbook section: connector.<type>.<removed|v1> is covered by its pattern."""
    m = re.fullmatch(r"connector\.(\w+)\.(removed|v1)", entry_id)
    return bool(m) and f"connector.<type>.{m.group(2)}" in text


# --- lookups ------------------------------------------------------------------------------------------------


def test_linked_service_lookup_uses_version_where_the_connector_has_versions():
    assert compat.for_linked_service("Oracle", "2.0") == []
    v1 = compat.for_linked_service("Oracle", "1.0")
    assert [e.id for e in v1] == ["connector.oracle.v1"] and v1[0].date == "2026-03-31"
    unstated = compat.for_linked_service("Oracle", None)
    assert [e.id for e in unstated] == ["connector.oracle.v1"]
    assert "does not state a version" in compat.describe(unstated[0], None)
    assert "does not state a version" not in compat.describe(v1[0], "1.0")


def test_v1_type_names_removed_outright_and_current_types_are_clean():
    assert [e.id for e in compat.for_linked_service("Salesforce")] == ["connector.salesforce.v1"]
    assert "SalesforceV2" in compat.for_linked_service("Salesforce")[0].summary
    assert compat.for_linked_service("SalesforceV2") == []
    concur = compat.for_linked_service("Concur")[0]
    assert concur.impact is Impact.blocker and concur.date == "2024-12-31"
    for ok in ("AzureSqlDatabase", "AzureSqlDW", "AzureBlobFS", "SqlServer", "SnowflakeV2"):
        assert compat.for_linked_service(ok, "1.0") == [] and compat.for_linked_service(ok, None) == []


def test_activity_lookup_and_emit_flags():
    assert compat.for_activity("ExecuteWranglingDataflow")[0].status == compat.DEPRECATED
    assert compat.for_activity("ExecuteDataFlow")[0].status == compat.PREVIEW
    # components the handlers already block are listed but do not raise a second issue
    assert [e.emit for e in compat.for_activity("ExecuteSSISPackage")] == [False]
    assert [e.emit for e in compat.for_activity("AzureMLBatchExecution")] == [False]
    assert compat.for_activity("Copy") == [] and compat.for_activity(None) == []


# --- parser -------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("tp, expected", [({"version": "2.0"}, "2.0"), ({"version": 1.0}, "1.0"), ({}, None),
                                          ({"version": True}, None), ({"version": "  "}, None)])
def test_parser_reads_the_connector_version_as_written(tp, expected):
    ls = parser.parse_linked_service({"name": "L", "properties": {"type": "Oracle", "typeProperties": tp}}, "p")
    assert ls.connector_version == expected


# --- engine hooks -------------------------------------------------------------------------------------------


def _copy_from(ls_type, version=None):
    tp = {"version": version} if version else {}
    ls = linked_service("LS_SRC", ls_type, tp)
    ds = dataset("DS_SRC", "OracleTable", "LS_SRC", {"table": "T"})
    c = copy_activity("Cp")
    c["inputs"] = [ref("Dataset", "DS_SRC")]
    return convert([pipe(c, name="PL")], datasets=[ds, DS_LAKE], linked_services=[ls, LAKE_LS])["PL"][0]


def _codes(rec):
    return {i.code for a in rec.activities for i in a.warnings}


def test_removed_v1_connector_makes_the_copy_need_manual_work_with_a_dated_message():
    rec = _copy_from("Oracle", "1.0")
    a = rec.activities[0]
    issue = next(i for i in a.warnings if i.code == "compat_connector_oracle_v1")
    assert issue.impact is Impact.manual and a.status is S.manual_required
    assert "2026-03-31" in issue.message and "learn.microsoft.com" in issue.message and "2.0" in issue.action


def test_current_version_and_modern_connectors_raise_nothing():
    assert not any(c.startswith("compat_") for c in _codes(_copy_from("Oracle", "2.0")))
    assert not any(c.startswith("compat_") for c in _codes(convert(
        [pipe(copy_activity("Cp"), name="PL")], datasets=[DS_SQL, DS_LAKE], linked_services=[SQL_LS, LAKE_LS])["PL"][0]))


def test_outright_removed_connector_blocks_the_activity():
    rec = _copy_from("Concur")
    a = rec.activities[0]
    assert a.status is S.blocked and "compat_connector_concur_removed" in {i.code for i in a.warnings}


def test_one_issue_per_activity_even_when_source_and_sink_share_the_linked_service():
    ls = linked_service("LS_BOTH", "Oracle", {"version": "1.0"})
    src = dataset("DS_A", "OracleTable", "LS_BOTH", {"table": "A"})
    snk = dataset("DS_B", "OracleTable", "LS_BOTH", {"table": "B"})
    c = act("Cp", "Copy", {"source": {"type": "OracleSource"}, "sink": {"type": "OracleSink"}},
            inputs=[ref("Dataset", "DS_A")], outputs=[ref("Dataset", "DS_B")])
    rec = convert([pipe(c, name="PL")], datasets=[src, snk], linked_services=[ls])["PL"][0]
    assert [i.code for i in rec.activities[0].warnings].count("compat_connector_oracle_v1") == 1


def test_deprecated_and_preview_activities_are_flagged_with_alternatives_without_being_dropped():
    wr = act("Wr", "ExecuteWranglingDataflow", {"dataflow": {"referenceName": "WDF", "type": "DataFlowReference"}})
    md = act("Md", "ExecuteDataFlow", {"dataflow": {"referenceName": "MDF1", "type": "DataFlowReference"}})
    rec = convert([pipe(wr, md, name="PL")])["PL"][0]
    by = {a.source_name: a for a in rec.activities}
    assert "compat_activity_wrangling_dataflow" in {i.code for i in by["Wr"].warnings}
    assert any("Dataflow Gen2" in x for x in by["Wr"].alternatives)
    md_issue = next(i for i in by["Md"].warnings if i.code == "compat_activity_mapping_dataflow")
    assert "flowlets" in md_issue.message and "preview" in md_issue.message
    assert by["Md"].status is S.manual_required  # still needs the user to re-author / upgrade it


def test_blocked_by_handler_components_get_no_duplicate_compat_issue():
    ss = act("Ss", "ExecuteSSISPackage", {"packageLocation": {"type": "SSISDB"}})
    rec = convert([pipe(ss, name="PL")])["PL"][0]
    codes = {i.code for i in rec.activities[0].warnings}
    assert "ssis_unsupported" in codes and not any(c.startswith("compat_") for c in codes)


# --- project scan + record + report -------------------------------------------------------------------------


def _estate():
    ls_old = linked_service("LS_ORA", "Oracle", {"version": "1.0"})
    ls_new = linked_service("LS_ORA2", "Oracle", {"version": "2.0"})
    ds_old = dataset("DS_ORA", "OracleTable", "LS_ORA", {"table": "T"})
    ds_new = dataset("DS_ORA2", "OracleTable", "LS_ORA2", {"table": "T"})
    c1 = copy_activity("FromOld")
    c1["inputs"] = [ref("Dataset", "DS_ORA")]
    c2 = copy_activity("FromNew")
    c2["inputs"] = [ref("Dataset", "DS_ORA2")]
    wr = act("Wr", "ExecuteWranglingDataflow", {"dataflow": {"referenceName": "W", "type": "DataFlowReference"}})
    ss = act("Ss", "ExecuteSSISPackage", {"packageLocation": {"type": "SSISDB"}})
    return semantic([pipe(c1, wr, name="PL_A"), pipe(c2, ss, name="PL_B")], datasets=[ds_old, ds_new, DS_LAKE],
                    linked_services=[ls_old, ls_new, LAKE_LS])


def test_scan_finds_every_hit_with_its_pipelines_and_respects_the_selection():
    d = _estate()
    found = {(f.id, f.subject): f for f in compat.scan(d)}
    assert set(found) == {("activity.wrangling_dataflow", "Wr"), ("activity.ssis", "Ss"),
                          ("connector.oracle.v1", "LS_ORA")}  # LS_ORA2 (version 2.0) is clean
    assert found[("connector.oracle.v1", "LS_ORA")].pipelines == ["PL_A"]
    assert found[("activity.ssis", "Ss")].emitted is False and found[("activity.wrangling_dataflow", "Wr")].emitted
    only_b = compat.scan(d, {"PL_B"})
    assert [f.id for f in only_b] == ["activity.ssis"]
    assert compat.scan(d) == compat.scan(d)  # deterministic order


def test_findings_round_trip_and_the_report_lists_them_with_sources():
    d = _estate()
    findings = compat.scan(d)
    conv = ProjectConversion(project_id="t", source_commit_sha="x", status=ConversionOutcome.partial, created_at=NOW,
                             updated_at=NOW, compatibility=findings)
    assert ProjectConversion.model_validate(conv.model_dump()).compatibility == findings
    text = render_report(conv)
    assert "## Compatibility findings" in text and "`LS_ORA`" in text and "2026-03-31" in text
    assert "Source: <https://learn.microsoft.com/" in text and "Way forward:" in text
    clean = ProjectConversion(project_id="t", source_commit_sha="x", status=ConversionOutcome.converted,
                              created_at=NOW, updated_at=NOW)
    assert "Compatibility findings" not in render_report(clean)


def test_an_older_conversion_record_without_the_field_still_loads():
    old = ProjectConversion(project_id="t", source_commit_sha="x", status=ConversionOutcome.converted,
                            created_at=NOW, updated_at=NOW).model_dump()
    old.pop("compatibility")
    assert ProjectConversion.model_validate(old).compatibility == []
