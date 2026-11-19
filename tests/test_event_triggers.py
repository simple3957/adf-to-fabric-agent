"""ADF storage / custom event trigger -> Event Grid subscription (the same filters) + Logic App that starts the Fabric pipeline with the same
parameters: the plan, the ARM template, the files deploy writes, the report and validation. Deploying the bridge in Azure is checked live
(writes are not tested automatically: docs/testing.md)."""

from __future__ import annotations

import json

import pytest

from adf2fabric import cloud
from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.conversion import event_triggers as ET
from adf2fabric.agent.deployment.event_bridge import pipeline_param_types, write_bridges
from adf2fabric.agent.discovery.adf_semantic import parser
from adf2fabric.agent.validation import engine
from adf2fabric.project.discovery import AdfEventTrigger, AdfTrigger
from adf2fabric.project.validation import Result as R
from tests.test_triggers import _project

STORAGE = "/subscriptions/sub-1/resourceGroups/rg-data/providers/Microsoft.Storage/storageAccounts/acct"
TOPIC = "/subscriptions/sub-1/resourceGroups/rg-events/providers/Microsoft.EventGrid/topics/orders-topic"
WS, PL = "workspace-id", "pipeline-id"


def blob(begins="/landing/blobs/in/", ends=".csv", events=("Microsoft.Storage.BlobCreated",), *, ignore=True, state="Stopped",
         pipelines=("PL_Main",), params=None) -> AdfTrigger:
    return AdfTrigger(name="TR_Files", path="trigger/TR_Files.json", type="BlobEventsTrigger", runtime_state=state, pipeline_refs=list(pipelines),
                      pipeline_parameters=params or {},
                      event=AdfEventTrigger(scope=STORAGE, events=list(events), blob_path_begins_with=begins, blob_path_ends_with=ends,
                                            ignore_empty_blobs=ignore))


def _custom_json(params=None):
    return {"name": "TR_Orders", "properties": {
        "type": "CustomEventsTrigger", "runtimeState": "Started",
        "pipelines": [{"pipelineReference": {"referenceName": "PL_Main", "type": "PipelineReference"},
                       "parameters": params or {"orderId": "@triggerBody().event.data.orderId", "subj": "@triggerBody().event.subject",
                                                "mode": "full"}}],
        "typeProperties": {
            "scope": TOPIC, "events": ["OrderReady", "OrderFixed"], "subjectBeginsWith": "orders/", "subjectEndsWith": ".json",
            "advancedFilters": [{"key": "data.department", "operatorType": "StringIn", "values": ["Finance", "Sales"]},
                                {"key": "data.amount", "operatorType": "NumberGreaterThan", "values": [100]}]}}}


# --- the plan: the ADF filters and parameters, exactly ----------------------------------------------------------------------------


def test_a_storage_trigger_keeps_its_filters_as_an_event_grid_subscription_filter():
    p = ET.plan(blob(params={"PL_Main": {"file": "@triggerBody().fileName", "folder": "@triggerBody().folderPath", "mode": "full"}}))
    assert (p.status, p.kind, p.scope) == ("generated", "storage", STORAGE)
    # what ADF's own subscription for such a trigger held, read live on 2026-11-19 (data.api list + ignoreEmptyBlobs as contentLength > 0)
    assert p.event_grid_filter == {
        "includedEventTypes": ["Microsoft.Storage.BlobCreated"],
        "subjectBeginsWith": "/blobServices/default/containers/landing/blobs/in/",
        "subjectEndsWith": ".csv",
        "advancedFilters": [{"operatorType": "StringIn", "key": "data.api",
                             "values": ["CopyBlob", "PutBlob", "PutBlockList", "FlushWithClose", "DeleteBlob", "CreateFile", "DeleteFile"]},
                            {"operatorType": "NumberGreaterThan", "key": "data.contentLength", "value": 0}]}
    assert p.pipelines == [{"name": "PL_Main", "parameters": {"file": {"source": "fileName"}, "folder": {"source": "folderPath"},
                                                              "mode": {"source": "literal", "value": "full"}}}]
    assert any("started on demand" in n for n in p.notes) and any("cutover" in s for s in p.steps)


def test_container_only_and_a_container_in_ends_with():
    p = ET.plan(blob(begins="/landing/", ends=None, ignore=False))
    assert p.event_grid_filter["subjectBeginsWith"] == "/blobServices/default/containers/landing/" and "subjectEndsWith" not in p.event_grid_filter
    assert [f["key"] for f in p.event_grid_filter["advancedFilters"]] == ["data.api"]  # not ignoring empty files: no size filter
    q = ET.plan(blob(begins=None, ends="/landing/blobs/done.flag", events=("Microsoft.Storage.BlobDeleted",), ignore=False))
    assert q.event_grid_filter["subjectBeginsWith"] == "/blobServices/default/containers/landing/"
    assert q.event_grid_filter["subjectEndsWith"] == "done.flag" and q.status == "generated_with_differences"
    clash = ET.plan(blob(begins="/a/blobs/x", ends="/b/blobs/y.csv"))
    assert clash.status == "manual" and "never fire" in clash.notes[0]


def test_a_custom_trigger_keeps_event_types_subject_and_advanced_filters():
    t = parser.parse_trigger(_custom_json(), "trigger/TR_Orders.json")
    p = ET.plan(t)
    assert (p.status, p.kind, p.scope) == ("generated", "custom", TOPIC)
    assert p.event_grid_filter == {
        "includedEventTypes": ["OrderReady", "OrderFixed"], "subjectBeginsWith": "orders/", "subjectEndsWith": ".json",
        "advancedFilters": [{"operatorType": "StringIn", "key": "data.department", "values": ["Finance", "Sales"]},
                            {"operatorType": "NumberGreaterThan", "key": "data.amount", "value": 100}]}  # single-value operators take 'value'
    assert p.pipelines[0]["parameters"] == {"orderId": {"source": "data", "path": ["orderId"]}, "subj": {"source": "event", "field": "subject"},
                                            "mode": {"source": "literal", "value": "full"}}


def test_an_expression_the_bridge_cannot_compute_is_named_never_guessed():
    p = ET.plan(blob(params={"PL_Main": {"when": "@trigger().startTime"}}))
    assert p.status == "generated_with_differences" and p.pipelines[0]["parameters"] == {}
    assert any("'when'" in n and "cannot compute" in n for n in p.notes)


def test_unreadable_or_unrelated_triggers_are_not_generated():
    assert ET.plan(blob(pipelines=())).status == "manual"
    assert ET.plan(blob(begins="landing/in")).status == "manual"
    assert ET.plan(AdfTrigger(name="T", path="t", type="ScheduleTrigger")).status == "manual"


# --- the ARM template ------------------------------------------------------------------------------------------------------------


def _resources(template):
    return {r["type"]: r for r in template["resources"]}


def test_the_storage_bridge_with_a_managed_identity():
    p = ET.plan(blob(params={"PL_Main": {"file": "@triggerBody().fileName", "folder": "@triggerBody().folderPath", "n": "3"}}))
    template, params = ET.arm_template(p, workspace_id=WS, pipeline_ids={"pl_main": PL}, param_types={"pl_main": {"n": "int"}})
    res = _resources(template)
    wf, sub = res["Microsoft.Logic/workflows"], res["Microsoft.EventGrid/eventSubscriptions"]
    assert wf["identity"] == {"type": "SystemAssigned"} and "logicAppPrincipalId" in template["outputs"]
    # the subscription: on the storage account, the plan's filter, delivered to the Logic App's callback URL, Event Grid schema
    assert sub["scope"] == "[format('Microsoft.Storage/storageAccounts/{0}', parameters('eventSourceName'))]"
    assert sub["properties"]["filter"] == p.event_grid_filter and sub["properties"]["eventDeliverySchema"] == "EventGridSchema"
    assert "listCallbackUrl" in sub["properties"]["destination"]["properties"]["endpointUrl"]
    d = wf["properties"]["definition"]
    v = d["actions"]["Is_validation"]
    assert "SubscriptionValidationEvent" in json.dumps(v["expression"])  # Event Grid's validation handshake is answered
    assert v["actions"]["Validation_response"]["inputs"]["body"] == {"validationResponse": "@first(triggerBody())?['data']?['validationCode']"}
    per_event = v["else"]["actions"]
    # one event per call (maxEventsPerBatch 1): no loop, so a Terminate step is allowed (refused inside a For each, live 2026-11-19)
    assert "Foreach" not in json.dumps(d) and sub["properties"]["destination"]["properties"]["maxEventsPerBatch"] == 1
    assert per_event["Blob_path"]["runAfter"] == {"Accepted": ["Succeeded"]}
    assert per_event["Blob_path"]["inputs"] == "@substring(first(triggerBody())?['subject'], 33)"
    gate = per_event["Start"]
    assert gate["expression"] == {"and": [{"equals": ["@parameters('runPipelines')", True]}]}
    run = gate["actions"]["Run_pipeline_1"]["inputs"]
    assert run["uri"] == f"{cloud.FABRIC_API}/workspaces/{WS}/items/{PL}/jobs/instances?jobType=Pipeline"
    assert run["authentication"] == {"type": "ManagedServiceIdentity", "audience": cloud.FABRIC_ENDPOINT}
    # inside "inputs": a top-level retryPolicy is refused by Logic Apps (found deploying the bridge live, 2026-11-19)
    assert run["retryPolicy"]["type"] == "exponential" and "retryPolicy" not in gate["actions"]["Run_pipeline_1"]
    assert run["body"] == {"executionData": {"parameters": {"file": "@outputs('File_name')", "folder": "@outputs('Folder_path')", "n": 3}}}
    assert params["parameters"] == {"eventSourceName": {"value": "acct"}, "runPipelines": {"value": False}}  # switched off until cutover


def test_the_service_principal_mode_takes_its_secret_from_key_vault():
    p = ET.plan(blob())
    template, params = ET.arm_template(p, workspace_id=WS, pipeline_ids={"PL_Main": PL}, param_types={}, identity="service_principal",
                                       tenant_id="tenant-id", client_id="client-id")
    wf = _resources(template)["Microsoft.Logic/workflows"]
    assert "identity" not in wf and template["outputs"] == {}
    auth = wf["properties"]["definition"]["actions"]["Is_validation"]["else"]["actions"]["Start"]["actions"][
        "Run_pipeline_1"]["inputs"]["authentication"]
    assert auth == {"type": "ActiveDirectoryOAuth", "authority": "https://login.microsoftonline.com", "tenant": "tenant-id",
                    "audience": cloud.FABRIC_ENDPOINT, "clientId": "client-id", "secret": "@parameters('fabricClientSecret')"}
    assert "keyVault" in params["parameters"]["fabricClientSecret"]["reference"]  # a reference, never a value
    assert "body" not in wf["properties"]["definition"]["actions"]["Is_validation"]["else"]["actions"]["Start"][
        "actions"]["Run_pipeline_1"]["inputs"]  # no parameters: no body
    with pytest.raises(ValueError):
        ET.arm_template(p, workspace_id=WS, pipeline_ids={"PL_Main": PL}, param_types={}, identity="service_principal")


def test_the_custom_bridge_starts_no_pipeline_when_a_data_key_is_missing_as_in_adf():
    p = ET.plan(parser.parse_trigger(_custom_json(), "trigger/TR_Orders.json"))
    template, params = ET.arm_template(p, workspace_id=WS, pipeline_ids={"PL_Main": PL}, param_types={})
    res = _resources(template)
    assert res["Microsoft.EventGrid/eventSubscriptions"]["scope"] == "[format('Microsoft.EventGrid/topics/{0}', parameters('eventSourceName'))]"
    per_event = res["Microsoft.Logic/workflows"]["properties"]["definition"]["actions"]["Is_validation"]["else"]["actions"]
    check = per_event["Data_keys_present"]
    assert "@first(triggerBody())?['data']?['orderId']" in json.dumps(check["expression"])
    assert check["else"]["actions"]["Missing_data_key"]["type"] == "Terminate"
    body = check["actions"]["Start"]["actions"]["Run_pipeline_1"]["inputs"]["body"]["executionData"]["parameters"]
    assert body == {"orderId": "@first(triggerBody())?['data']?['orderId']", "subj": "@first(triggerBody())?['subject']", "mode": "full"}
    assert params["parameters"]["eventSourceName"] == {"value": "orders-topic"}


def test_a_pipeline_not_in_the_workspace_has_no_template():
    with pytest.raises(ValueError):
        ET.arm_template(ET.plan(blob()), workspace_id=WS, pipeline_ids={}, param_types={})


# --- convert, report, validation, deploy files ------------------------------------------------------------------------------------


def _event_trigger():
    return {"name": "TR_Nightly", "properties": {
        "type": "BlobEventsTrigger", "runtimeState": "Stopped",
        "pipelines": [{"pipelineReference": {"referenceName": "PL_Main", "type": "PipelineReference"},
                       "parameters": {"fileName": "@triggerBody().fileName"}}],
        "typeProperties": {"scope": STORAGE, "events": ["Microsoft.Storage.BlobCreated"], "blobPathBeginsWith": "/landing/blobs/in/",
                           "blobPathEndsWith": ".csv", "ignoreEmptyBlobs": True}}}


def test_convert_plans_the_bridge_and_validation_checks_the_filters(tmp_path):
    store, pid = _project(tmp_path, _event_trigger())
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    assert r.success, r.error
    note = next(t for p in r.conversion.pipelines for t in p.triggers if t.name == "TR_Nightly")
    assert (note.event_status, note.event_kind, note.event_file) == ("generated", "storage", "event-triggers/TR_Nightly.json")
    assert "Event Grid subscription" in note.action and note.activator_status is None
    plan = ET.EventBridgePlan.from_dict(json.loads((store.conversion_dir(pid) / note.event_file).read_text(encoding="utf-8")))
    assert plan.event_grid_filter["subjectEndsWith"] == ".csv"
    report = (store.conversion_dir(pid) / "conversion-report.md").read_text(encoding="utf-8")
    assert "Event bridge (Event Grid -> Logic App -> Fabric): **generated** (storage)" in report
    v = engine.validate(store, pid, stages={"static"})
    rows = [c for c in v.checks if c.id == "execution_semantics.trigger" and c.component == "trigger:TR_Nightly"]
    assert {c.result for c in rows} == {R.PASS} and "same filters" in rows[0].summary
    # deploy --triggers: the files, with this workspace's pipeline id and the converted pipeline's parameter types
    types = pipeline_param_types(store.conversion_dir(pid), ["PL_Main"])
    assert "pl_main" in types
    out = write_bridges(tmp_path / "bridges", store.conversion_dir(pid), [plan], workspace_id=WS, pipeline_ids={"pl_main": PL},
                        identity="managed_identity", tenant_id=None, client_id=None)
    assert out[0].action == "written"
    folder = tmp_path / "bridges" / WS / "TR_Nightly"
    t = json.loads((folder / "template.json").read_text(encoding="utf-8"))
    assert PL in json.dumps(t) and (folder / "parameters.json").exists() and "az deployment group create" in (folder / "README.md").read_text()
    assert "--resource-group rg-data" in out[0].detail
    missing = write_bridges(tmp_path / "b2", store.conversion_dir(pid), [plan], workspace_id=WS, pipeline_ids={}, identity="managed_identity",
                            tenant_id=None, client_id=None)
    assert missing[0].action == "skipped" and "not in the workspace" in missing[0].detail


def test_a_pipeline_an_event_trigger_starts_gets_a_trigger_event_activity_visible_in_fabric(tmp_path):
    # the bridge is not visible in Fabric's UI, so each run shows which trigger and event started it (the user's request, 2026-11-19)
    store, pid = _project(tmp_path, _event_trigger())
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    rec = next(p for p in r.conversion.pipelines if p.source_pipeline == "PL_Main")
    content = json.loads((store.conversion_dir(pid) / rec.artifact_path).read_text(encoding="utf-8"))["properties"]
    act = next(a for a in content["activities"] if a["name"] == "Trigger_event")
    assert ET.is_trigger_event(act) and act["dependsOn"] == []  # runs on its own: the pipeline's order is unchanged
    assert act["typeProperties"]["variableName"] == "trigger_event" and "trigger_event" in content["variables"]
    expr_ = act["typeProperties"]["value"]["value"]
    assert "Started by hand or by another pipeline" in expr_ and "pipeline().parameters.triggerEventSubject" in expr_
    assert all(content["parameters"][k] == {"type": "String", "defaultValue": ""} for k in ET.TRIGGER_PARAMS)
    assert set(ET.TRIGGER_PARAMS) <= set(rec.parameters_added) and rec.structural_valid
    assert any("Trigger_event" in n for n in rec.notes)
    others = [p for p in r.conversion.pipelines if p.source_pipeline != "PL_Main"]
    assert others and all("Trigger_event" not in (store.conversion_dir(pid) / p.artifact_path).read_text(encoding="utf-8") for p in others)
    # validation: a known tool addition, not an "extra" activity
    v = engine.validate(store, pid, stages={"static"})
    inv = [c for c in v.checks if c.id == "activity_graph.inventory" and c.component == "pipeline:PL_Main"]
    assert {c.result for c in inv} == {R.PASS}
    comp = {x.id: x for x in v.components}["pipeline:PL_Main/activity:Trigger_event"]
    assert comp.status.value == "transformed" and "added by the tool" in comp.note
    # the bridge passes the four values because the converted pipeline declares them
    plan = ET.EventBridgePlan.from_dict(json.loads((store.conversion_dir(pid) / "event-triggers/TR_Nightly.json").read_text(encoding="utf-8")))
    template, _ = ET.arm_template(plan, workspace_id=WS, pipeline_ids={"pl_main": PL},
                                  param_types=pipeline_param_types(store.conversion_dir(pid), ["PL_Main"]))
    body = json.dumps(template)
    assert '"triggerName": "TR_Nightly"' in body and "\"triggerEventSubject\": \"@first(triggerBody())?['subject']\"" in body


def test_verify_does_not_count_the_trigger_event_activity_as_a_difference():
    from adf2fabric.agent.deployment.verify import Verification, _judge

    base = {"Set_fileName": {"type": "SetVariable", "status": "Succeeded", "runs": 1, "metrics": {}}}
    v = Verification("PL_Main", "failed")
    v.activities = {**base, "Trigger_event": {"type": "SetVariable", "status": "Succeeded", "runs": 1, "metrics": {}}}
    assert _judge(v, base).verdict == "matches" and v.fabric_only == []
    w = Verification("PL_Main", "failed")
    w.activities = {**base, "Something_else": {"type": "SetVariable", "status": "Succeeded", "runs": 1, "metrics": {}}}
    assert _judge(w, base).verdict == "partial"  # any other extra activity still counts
