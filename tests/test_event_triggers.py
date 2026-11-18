"""ADF storage event trigger -> Fabric Activator rule: the plan, the Activator definition (checked against one made in the Fabric portal),
the files convert writes, the report and validation. Creating the Activator in Fabric was checked live on 2026-11-18 (writes are not tested
automatically: docs/testing.md)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.conversion import event_triggers as ET
from adf2fabric.agent.deployment.activator_deploy import plan_from_file
from adf2fabric.agent.discovery.adf_semantic import parser
from adf2fabric.agent.validation import engine
from adf2fabric.project.discovery import AdfEventTrigger, AdfTrigger
from adf2fabric.project.validation import Result as R
from tests.test_triggers import _project

PORTAL = json.loads((Path(__file__).parent / "fixtures" / "activator" / "portal_onelake_trigger.json").read_text(encoding="utf-8"))["entities"]
SCOPE = "/subscriptions/sub-1/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/acct"
TENANT, WS, LH, PL = "tenant-id", "workspace-id", "lakehouse-id", "pipeline-id"


def blob(begins="/landing/blobs/in/", ends=".csv", events=("Microsoft.Storage.BlobCreated",), *, ignore=True, state="Stopped",
         pipelines=("PL_Main",), params=None, type_="BlobEventsTrigger") -> AdfTrigger:
    return AdfTrigger(name="TR_Files", path="trigger/TR_Files.json", type=type_, runtime_state=state, pipeline_refs=list(pipelines),
                      pipeline_parameters=params or {},
                      event=AdfEventTrigger(scope=SCOPE, events=list(events), blob_path_begins_with=begins, blob_path_ends_with=ends,
                                            ignore_empty_blobs=ignore))


def subject(p: ET.EventTriggerPlan) -> dict[str, str]:
    return {f["operatorType"]: f["values"][0] for f in p.filters}


# --- the plan ----------------------------------------------------------------------------------------------------------------------


def test_files_landing_in_a_lakehouse_give_a_generated_onelake_rule():
    p = ET.plan(blob(), ET.OneLakeTarget("LH_Main"))
    assert (p.status, p.target, p.lakehouse) == ("generated_with_differences", "onelake", "LH_Main")
    assert p.event_types == ["Microsoft.Fabric.OneLake.FileCreated"]
    assert subject(p) == {"StringBeginsWith": "/Files/landing/in/", "StringEndsWith": ".csv"}
    # the watched store changes, and that is said first; the container -> folder guess and the empty-file difference are said too
    assert "must now be WRITTEN to OneLake" in p.notes[0] and "even behind a shortcut" in p.notes[0]
    assert any("Container 'landing'" in n for n in p.notes) and any("empty files" in n for n in p.notes)


def test_a_folder_given_by_a_person_is_used_as_is():
    p = ET.plan(blob(begins="/landing/blobs/", ends=None, ignore=False), ET.OneLakeTarget.parse("LH_Main:Files/raw/landing"))
    assert subject(p) == {"StringBeginsWith": "/Files/raw/landing/"}
    assert not any("Container" in n for n in p.notes) and not any("empty files" in n for n in p.notes)
    with pytest.raises(ValueError):
        ET.OneLakeTarget.parse("LH_Main:Tables/x")
    with pytest.raises(ValueError):
        ET.OneLakeTarget.parse(":Files/x")


def test_a_container_given_in_ends_with_narrows_the_path_and_a_delete_maps_too():
    p = ET.plan(blob(begins=None, ends="/landing/blobs/done.flag", events=("Microsoft.Storage.BlobDeleted",)), ET.OneLakeTarget("LH"))
    assert p.event_types == ["Microsoft.Fabric.OneLake.FileDeleted"]
    assert subject(p) == {"StringBeginsWith": "/Files/landing/", "StringEndsWith": "done.flag"}
    clash = ET.plan(blob(begins="/a/blobs/x", ends="/b/blobs/y.csv"), ET.OneLakeTarget("LH"))
    assert clash.status == "manual" and "never fire" in clash.notes[0]


def test_any_container_watches_the_whole_files_area_and_says_so():
    p = ET.plan(blob(begins=None, ends=".csv"), ET.OneLakeTarget("LH"))
    assert subject(p) == {"StringBeginsWith": "/Files/", "StringEndsWith": ".csv"}
    assert any("every container" in n for n in p.notes)


def test_files_staying_in_azure_storage_get_the_wizard_steps_not_a_guessed_source():
    p = ET.plan(blob())
    assert (p.status, p.target) == ("manual", "azure_blob")
    assert p.event_types == ["Microsoft.Storage.BlobCreated"]
    assert subject(p) == {"StringBeginsWith": "/blobServices/default/containers/landing/blobs/in/", "StringEndsWith": ".csv"}
    steps = " ".join(p.steps)
    assert "same Entra tenant" in steps and "Azure Blob Storage events" in steps and "sub-1" in steps and "'acct'" in steps
    assert "switched off" in steps and "not documented" in p.notes[0]
    with pytest.raises(ValueError):  # no Activator definition is ever built for it
        ET.reflex_entities(p, tenant_id=TENANT, workspace_id=WS, lakehouse_id=LH, pipeline_ids={"PL_Main": PL})


def test_trigger_parameters_are_explained_never_silently_dropped():
    p = ET.plan(blob(params={"PL_Main": {"fileName": "@triggerBody().fileName", "folder": "@triggerBody().folderPath", "mode": "full"}}),
                ET.OneLakeTarget("LH"))
    text = " ".join(p.notes)
    assert "@pipeline()?.TriggerEvent?.FileName" in text and "folder" in text and "Trigger parameters tab" in text
    assert "'mode'" in text and "default value" in text


def _custom_json(params=None):
    return {"name": "TR_Orders", "properties": {
        "type": "CustomEventsTrigger", "runtimeState": "Started",
        "pipelines": [{"pipelineReference": {"referenceName": "PL_Main", "type": "PipelineReference"},
                       "parameters": params or {"orderId": "@triggerBody().event.data.orderId", "subj": "@triggerBody().event.subject",
                                                "mode": "full"}}],
        "typeProperties": {
            "scope": "/subscriptions/sub-1/resourceGroups/rg/providers/Microsoft.EventGrid/topics/orders-topic",
            "events": ["OrderReady", "OrderFixed"], "subjectBeginsWith": "orders/", "subjectEndsWith": ".json",
            "advancedFilters": [{"key": "data.department", "operatorType": "StringIn", "values": ["Finance", "Sales"]},
                                {"key": "data.amount", "operatorType": "NumberGreaterThan", "values": [100]}]}}}


def test_a_custom_event_trigger_gets_both_routes_and_the_exact_event_grid_filter_never_a_generated_source():
    t = parser.parse_trigger(_custom_json(), "trigger/TR_Orders.json")
    assert t.event.advanced_filters == [{"key": "data.department", "operatorType": "StringIn", "values": ["Finance", "Sales"]},
                                        {"key": "data.amount", "operatorType": "NumberGreaterThan", "values": [100]}]
    p = ET.plan(t)
    assert (p.status, p.target) == ("manual", "custom_event") and p.scope.endswith("topics/orders-topic")
    # the ADF trigger's filters, as an Event Grid subscription filter (single-value operators take 'value')
    assert p.event_grid_filter == {"includedEventTypes": ["OrderReady", "OrderFixed"], "subjectBeginsWith": "orders/", "subjectEndsWith": ".json",
                                   "advancedFilters": [{"operatorType": "StringIn", "key": "data.department", "values": ["Finance", "Sales"]},
                                                       {"operatorType": "NumberGreaterThan", "key": "data.amount", "value": 100}]}
    steps, notes = " ".join(p.steps), " ".join(p.notes)
    assert "Route A" in steps and "Event Hub" in steps and "Route B" in steps and "orders-topic" in steps and '"includedEventTypes"' in steps
    assert "Azure Event Grid Namespace" in steps and "switched off" in steps and "stop the ADF trigger" in steps
    assert "NAMESPACE topics only" in notes and "2 advanced filter(s)" in notes
    assert "`data.orderId`" in notes and "`subject`" in notes and "'mode'" in notes and "default" in notes
    with pytest.raises(ValueError):
        ET.reflex_entities(p, tenant_id=TENANT, workspace_id=WS, lakehouse_id=LH, pipeline_ids={"PL_Main": PL})


def test_custom_event_trigger_in_convert_report_and_validation(tmp_path):
    store, pid = _project(tmp_path, {**_custom_json(), "name": "TR_Nightly"})
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    note = next(t for p in r.conversion.pipelines for t in p.triggers if t.name == "TR_Nightly")
    assert (note.activator_status, note.activator_target, note.activator_file) == ("manual", "custom_event", "activators/TR_Nightly.json")
    assert "Eventstream" in note.action and any("Advanced filter: `data.amount`" in d for d in note.details)
    assert "Activator rule's action passes the event field" in next(d for d in note.details if "`orderId`" in d)
    report = (store.conversion_dir(pid) / "conversion-report.md").read_text(encoding="utf-8")
    assert "Fabric Activator rule: **manual** (custom_event)" in report and "Route A" in report
    plan = plan_from_file(json.loads((store.conversion_dir(pid) / note.activator_file).read_text(encoding="utf-8")))
    assert plan.event_grid_filter["subjectBeginsWith"] == "orders/"
    v = engine.validate(store, pid, stages={"static"})
    rows = [c for c in v.checks if c.id == "execution_semantics.trigger" and c.component == "trigger:TR_Nightly"]
    assert {c.result for c in rows} == {R.BLOCKED} and "custom event trigger" in rows[0].remediation


def test_unreadable_triggers_are_not_generated():
    assert ET.plan(blob(type_="CustomEventsTrigger", pipelines=())).status == "manual"
    assert ET.plan(blob(events=("Microsoft.Storage.BlobRenamed",))).status == "manual"
    assert ET.plan(blob(pipelines=())).status == "manual"
    assert ET.plan(blob(begins="landing/in")).status == "manual"  # not ADF's /<container>/blobs/<path> form


# --- the Activator definition ------------------------------------------------------------------------------------------------------

_CONSTANT_KEYS = {"type", "kind", "templateId", "templateVersion", "scope", "eventGroupType", "operatorType", "key", "itemType", "jobType"}


def _shape(x, key=None):
    """The structure of an entity: keys, list lengths, and the values Fabric fixes (types, kinds, template ids, row names), not ids or names."""
    if isinstance(x, dict):
        return {k: _shape(v, k) for k, v in x.items() if k not in ("description",)}
    if isinstance(x, list):
        out = [_shape(v, key) for v in x]
        return sorted(out, key=json.dumps) if key == "filters" else out  # the filters of a source are a set (the portal wrote ends-with first)
    if isinstance(x, str) and key == "instance":
        return _shape(json.loads(x))
    if isinstance(x, str) and key in _CONSTANT_KEYS:
        return x
    return type(x).__name__


def _by_type(entities):
    out = {}
    for e in entities:
        t = e["type"] + ("/" + e["payload"]["definition"]["type"] if e["type"] == "timeSeriesView-v1" else "")
        out[t] = e
    return out


def test_the_definition_has_the_shape_the_fabric_portal_writes():
    p = ET.plan(blob(begins="/migrationcontainer/blobs/migration-test/events/", ends=".csv", ignore=False, pipelines=("PL_GlobalParams_Test",)),
                ET.OneLakeTarget("LH_adf2fabricv1"))
    ours = _by_type(ET.reflex_entities(p, tenant_id=TENANT, workspace_id=WS, lakehouse_id=LH, pipeline_ids={"pl_globalparams_test": PL}))
    portal = _by_type(PORTAL)
    assert set(ours) == set(portal) == {"container-v1", "fabricItemAction-v1", "realTimeHubSource-v1", "timeSeriesView-v1/Event",
                                        "timeSeriesView-v1/Rule"}
    for t in portal:
        assert _shape(ours[t]) == _shape(portal[t]), t
    # the rule's rows name the same steps the portal wrote, and it is switched off
    rule = json.loads(ours["timeSeriesView-v1/Rule"]["payload"]["definition"]["instance"])
    assert [s["name"] for s in rule["steps"]] == ["FieldsDefaultsStep", "EventDetectStep", "ActStep"]
    assert ours["timeSeriesView-v1/Rule"]["payload"]["definition"]["settings"] == {"shouldRun": False, "shouldApplyRuleOnUpdate": False}
    src = ours["realTimeHubSource-v1"]["payload"]
    assert src["connection"] == {"scope": "Artifact", "tenantId": TENANT, "workspaceId": WS, "artifactId": LH,
                                 "eventGroupType": "Microsoft.Fabric.OneLakeEvents"}
    assert src["filterSettings"]["filters"] == [
        {"key": "subject", "operatorType": "StringBeginsWith", "values": ["/Files/migrationcontainer/migration-test/events/"]},
        {"key": "subject", "operatorType": "StringEndsWith", "values": [".csv"]}]


def test_entities_are_wired_by_their_ids_and_stable_between_runs():
    p = ET.plan(blob(), ET.OneLakeTarget("LH"))
    a = ET.reflex_entities(p, tenant_id=TENANT, workspace_id=WS, lakehouse_id=LH, pipeline_ids={"PL_Main": PL})
    assert a == ET.reflex_entities(p, tenant_id=TENANT, workspace_id=WS, lakehouse_id=LH, pipeline_ids={"PL_Main": PL})
    ids = {e["uniqueIdentifier"] for e in a}
    by = _by_type(a)
    container = by["container-v1"]["uniqueIdentifier"]
    assert all(e["payload"]["parentContainer"]["targetUniqueIdentifier"] == container for e in a if e["type"] != "container-v1")
    event = json.loads(by["timeSeriesView-v1/Event"]["payload"]["definition"]["instance"])
    assert event["steps"][0]["rows"][0]["arguments"][0]["value"] == by["realTimeHubSource-v1"]["uniqueIdentifier"]
    act = json.loads(by["timeSeriesView-v1/Rule"]["payload"]["definition"]["instance"])["steps"][2]["rows"][0]["arguments"]
    vals = {x["name"]: x.get("value") for x in act}
    assert vals["itemId"] == PL and vals["fabricJobConnectionDocumentId"] == by["fabricItemAction-v1"]["uniqueIdentifier"]
    assert vals["fabricJobConnectionDocumentId"] in ids
    assert len(ids) == len(a)


def test_one_rule_per_pipeline_and_switched_on_only_when_asked_and_started():
    p = ET.plan(blob(pipelines=("PL_A", "PL_B"), state="Started"), ET.OneLakeTarget("LH"))
    ids = {"pl_a": "a", "pl_b": "b"}
    off = ET.reflex_entities(p, tenant_id=TENANT, workspace_id=WS, lakehouse_id=LH, pipeline_ids=ids)
    assert sum(e["type"] == "fabricItemAction-v1" for e in off) == 2 and ET.rules_running(off) == []
    on = ET.reflex_entities(p, tenant_id=TENANT, workspace_id=WS, lakehouse_id=LH, pipeline_ids=ids, enable=True)
    assert sorted(ET.rules_running(on)) == ["TR_Files: PL_A", "TR_Files: PL_B"]
    stopped = ET.plan(blob(state="Stopped"), ET.OneLakeTarget("LH"))
    assert ET.rules_running(ET.reflex_entities(stopped, tenant_id=TENANT, workspace_id=WS, lakehouse_id=LH, pipeline_ids={"PL_Main": PL},
                                               enable=True)) == []
    with pytest.raises(ValueError):
        ET.reflex_entities(p, tenant_id=TENANT, workspace_id=WS, lakehouse_id=LH, pipeline_ids={"pl_a": "a"})


# --- convert, report, validation ------------------------------------------------------------------------------------------------------


def _event_trigger():
    return {"name": "TR_Nightly", "properties": {
        "type": "BlobEventsTrigger", "runtimeState": "Stopped",
        "pipelines": [{"pipelineReference": {"referenceName": "PL_Main", "type": "PipelineReference"},
                       "parameters": {"fileName": "@triggerBody().fileName"}}],
        "typeProperties": {"scope": SCOPE, "events": ["Microsoft.Storage.BlobCreated"], "blobPathBeginsWith": "/landing/blobs/in/",
                           "blobPathEndsWith": ".csv", "ignoreEmptyBlobs": True}}}


def test_convert_writes_the_plan_only_for_the_trigger_a_person_pointed_at_a_lakehouse(tmp_path):
    store, pid = _project(tmp_path, _event_trigger())
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj", onelake_triggers={"tr_nightly": "LH_Main"}))
    assert r.success, r.error
    note = next(t for p in r.conversion.pipelines for t in p.triggers if t.name == "TR_Nightly")
    assert (note.activator_status, note.activator_target, note.activator_file) == ("generated_with_differences", "onelake",
                                                                                     "activators/TR_Nightly.json")
    data = json.loads((store.conversion_dir(pid) / note.activator_file).read_text(encoding="utf-8"))
    p = plan_from_file(data)
    assert (p.target, p.lakehouse, p.pipelines, p.started_in_adf) == ("onelake", "LH_Main", ["PL_Main"], False)
    report = (store.conversion_dir(pid) / "conversion-report.md").read_text(encoding="utf-8")
    assert "Fabric Activator rule: **generated_with_differences** (onelake)" in report and "TriggerEvent?.FileName" in report
    v = engine.validate(store, pid, stages={"static"})
    rows = [c for c in v.checks if c.id == "execution_semantics.trigger" and c.component == "trigger:TR_Nightly"]
    assert {c.result for c in rows} == {R.PASS} and "watches OneLake" in rows[0].summary


def test_without_a_person_s_choice_the_trigger_stays_on_azure_storage_and_validation_blocks(tmp_path):
    store, pid = _project(tmp_path, _event_trigger())
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj", lakehouse=None))
    note = next(t for p in r.conversion.pipelines for t in p.triggers if t.name == "TR_Nightly")
    assert (note.activator_status, note.activator_target) == ("manual", "azure_blob") and note.activator_steps
    assert "Azure Blob Storage events" in (store.conversion_dir(pid) / "conversion-report.md").read_text(encoding="utf-8")
    v = engine.validate(store, pid, stages={"static"})
    assert {c.result for c in v.checks if c.id == "execution_semantics.trigger" and c.component == "trigger:TR_Nightly"} == {R.BLOCKED}


def test_a_wrong_trigger_name_or_folder_is_refused(tmp_path):
    store, _ = _project(tmp_path, _event_trigger())
    bad = ConversionAgent(store).run(ConversionRequest(project_name="test-proj", onelake_triggers={"TR_Other": "LH"}))
    assert not bad.success and "TR_Nightly" in bad.error
    bad = ConversionAgent(store).run(ConversionRequest(project_name="test-proj", onelake_triggers={"TR_Nightly": "LH:Tables/x"}))
    assert not bad.success and "under Files" in bad.error


def test_the_parser_feeds_the_plan():
    t = parser.parse_trigger(_event_trigger(), "trigger/TR_Nightly.json")
    p = ET.plan(t, ET.OneLakeTarget("LH"))
    assert subject(p) == {"StringBeginsWith": "/Files/landing/in/", "StringEndsWith": ".csv"} and p.scope == SCOPE
