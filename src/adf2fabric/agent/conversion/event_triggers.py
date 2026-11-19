"""ADF storage / custom event trigger -> an Event Grid subscription with the same filters + a Logic App that starts the Fabric pipeline.

Why this shape (the user's decision, 2026-11-19): an ADF event trigger IS an Event Grid subscription with filters, delivering to ADF.
Keeping the subscription and changing only the last hop reproduces it most exactly: the same event types, subject filters and
advanced filters (Event Grid evaluates them), and the same pipeline parameters (``fileName``, ``folderPath``, ``data.<key>`` ...), so the
pipeline itself does not change. The last hop is a Logic App (Consumption) that calls Fabric's documented Run On Demand Item Job API
(``POST /v1/workspaces/{ws}/items/{id}/jobs/instances?jobType=Pipeline`` with ``executionData.parameters``; users, service principals
and managed identities may call it; run live 2026-11-18). It replaced an earlier Activator-based form (2026-11-18), which needed the
pipeline rewritten to ``pipeline()?.TriggerEvent`` and could not reach storage in another tenant or an Event Grid custom topic.

The tool generates, never deploys: per trigger an ARM template (the Logic App and the Event Grid subscription, deployed into the
resource group of the storage account or topic) and its parameters file. ``runPipelines`` is false by default: the Logic App answers
Event Grid and records each event but starts nothing until a person sets it to true at cutover (the "created switched off" rule).

Sign-in to Fabric: the Logic App's managed identity when Azure and Fabric share an Entra tenant (no secret), otherwise a service
principal of Fabric's tenant whose secret comes from Key Vault through the parameters file (never written by the tool).
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from adf2fabric import cloud
from adf2fabric.project.discovery import AdfTrigger

STORAGE_SUBJECT = "/blobServices/default/containers/"  # Event Grid subject of a blob event: <this><container>/blobs/<path>
_SUBJECT_LEN = len(STORAGE_SUBJECT)  # 33
# The storage operations ADF's own Event Grid subscription for a storage event trigger lets through (read live 2026-11-19).
ADF_BLOB_APIS = ("CopyBlob", "PutBlob", "PutBlockList", "FlushWithClose", "DeleteBlob", "CreateFile", "DeleteFile")
EVENT_FIELDS = {"subject": "subject", "eventtype": "eventType", "id": "id", "topic": "topic", "eventtime": "eventTime"}
LOGIC_API, GRID_API = "2019-05-01", "2022-06-15"
WORKFLOW_SCHEMA = "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#"
# Event Grid advanced filter operators that take one "value" instead of "values" (Event Grid event subscription schema)
_SINGLE_VALUE = {"numberlessthan", "numbergreaterthan", "numberlessthanorequals", "numbergreaterthanorequals", "boolequals"}
_LITERAL_TYPES = {"int": int, "float": float, "bool": bool}


@dataclass
class EventBridgePlan:
    status: str  # generated | generated_with_differences | manual | not_supported
    kind: str  # storage | custom | none
    trigger: str = ""
    scope: Optional[str] = None  # the storage account or Event Grid topic resource id the ADF trigger listened to
    started_in_adf: bool = False
    event_grid_filter: dict[str, Any] = field(default_factory=dict)
    # [{"name": pipeline, "parameters": {param: {"source": fileName|folderPath|data|event|literal, "path"/"field"/"value": ...}}}]
    pipelines: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    steps: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "EventBridgePlan":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


def _split_blob_path(path: Optional[str]) -> Optional[tuple[str, str]]:
    """ADF writes ``/<container>/blobs/<path>`` (or ``/<container>/``); None for any other form (a plain suffix such as ``.csv``)."""
    m = re.match(r"^/([^/]+)/(?:blobs/?(.*))?$", path or "")
    return (m.group(1), m.group(2) or "") if m else None


def _advanced(filters: list[dict]) -> list[dict]:
    out = []
    for f in filters:
        one = f["operatorType"].lower() in _SINGLE_VALUE and len(f["values"]) == 1
        out.append({"operatorType": f["operatorType"], "key": f["key"], **({"value": f["values"][0]} if one else {"values": f["values"]})})
    return out


def _parameters(t: AdfTrigger, pipeline: str, kind: str, notes: list[str]) -> dict[str, dict]:
    """How each parameter the ADF trigger passed to ``pipeline`` is produced from the event (the same value, never guessed)."""
    out: dict[str, dict] = {}
    params = next((v for k, v in t.pipeline_parameters.items() if k.lower() == pipeline.lower()), {})
    for name, value in sorted(params.items()):
        text = value.replace(" ", "")
        low = text.lower()
        if kind == "storage" and low == "@triggerbody().filename":
            out[name] = {"source": "fileName"}
        elif kind == "storage" and low == "@triggerbody().folderpath":
            out[name] = {"source": "folderPath"}
        elif kind == "custom" and (m := re.fullmatch(r"@triggerbody\(\)\.event\.data((?:\.\w+)+)", text, re.I)):
            out[name] = {"source": "data", "path": m.group(1).strip(".").split(".")}
        elif kind == "custom" and (m := re.fullmatch(r"@triggerbody\(\)\.event\.(\w+)", low)) and m.group(1) in EVENT_FIELDS:
            out[name] = {"source": "event", "field": EVENT_FIELDS[m.group(1)]}
        elif not text.startswith("@"):
            out[name] = {"source": "literal", "value": value}
        else:
            notes.append(f"Parameter '{name}' of '{pipeline}' was set to `{value[:120]}`, which the bridge cannot compute; it is not passed. "
                         "Give the pipeline parameter a default, or add it to the Logic App by hand.")
    return out


def plan(t: AdfTrigger) -> EventBridgePlan:
    """The Event Grid + Logic App form of one ADF event trigger."""
    kind = (t.type or "").lower()
    started = (t.runtime_state or "").lower() == "started"
    base = EventBridgePlan("manual", "none", trigger=t.name, started_in_adf=started)
    if kind not in ("blobeventstrigger", "customeventstrigger"):
        base.notes = [f"Trigger type '{t.type}' is not an event trigger."]
        return base
    base.kind = "storage" if kind == "blobeventstrigger" else "custom"
    e = t.event
    if e is None or not e.scope:
        base.notes = ["The trigger's storage account or topic could not be read."]
        return base
    if not t.pipeline_refs:
        base.notes = ["The trigger starts no pipeline."]
        return base
    base.scope = e.scope
    flt: dict[str, Any] = {}
    notes: list[str] = []
    if base.kind == "storage":
        if not e.events:
            base.notes = ["The trigger's events could not be read."]
            return base
        begins = _split_blob_path(e.blob_path_begins_with)
        if e.blob_path_begins_with and begins is None:
            base.notes = [f"'Blob path begins with' {e.blob_path_begins_with!r} is not in ADF's /<container>/blobs/<path> form."]
            return base
        ends_text = e.blob_path_ends_with or ""
        ends = _split_blob_path(ends_text) if ends_text.startswith("/") else None
        if ends and begins and begins[0].lower() != ends[0].lower():
            base.notes = ["'Blob path begins with' and 'ends with' name different containers; the ADF trigger can never fire."]
            return base
        flt["includedEventTypes"] = list(e.events)
        container = begins[0] if begins else (ends[0] if ends else None)
        if container:
            flt["subjectBeginsWith"] = STORAGE_SUBJECT + container + "/" + (f"blobs/{begins[1]}" if begins and begins[1] else "")
        if ends:
            flt["subjectEndsWith"] = ends[1]
            notes.append("'Blob path ends with' named a container; its container narrows the subject start and the rest is the suffix.")
        elif ends_text:
            flt["subjectEndsWith"] = ends_text
        # what ADF's own subscription holds (read live 2026-11-19): only these storage operations count, and empty files are skipped
        flt["advancedFilters"] = [{"operatorType": "StringIn", "key": "data.api", "values": list(ADF_BLOB_APIS)}]
        if e.ignore_empty_blobs:
            flt["advancedFilters"].append({"operatorType": "NumberGreaterThan", "key": "data.contentLength", "value": 0})
    else:
        if e.events:
            flt["includedEventTypes"] = list(e.events)
        if e.subject_begins_with:
            flt["subjectBeginsWith"] = e.subject_begins_with
        if e.subject_ends_with:
            flt["subjectEndsWith"] = e.subject_ends_with
        if e.advanced_filters:
            flt["advancedFilters"] = _advanced(e.advanced_filters)
    base.event_grid_filter = flt
    base.pipelines = [{"name": p, "parameters": _parameters(t, p, base.kind, notes)} for p in sorted(t.pipeline_refs)]
    source = "storage account" if base.kind == "storage" else "Event Grid topic"
    name = e.scope.rstrip("/").rsplit("/", 1)[-1]
    rg = re.search(r"/resourceGroups/([^/]+)", e.scope, re.I)
    where = f"resource group '{rg.group(1)}'" if rg else f"the resource group of the {source}"
    base.status = "generated_with_differences" if notes else "generated"
    base.notes = notes + [
        "The pipeline run shows in Fabric as started on demand by the Logic App's identity, not by a trigger.",
        "Event Grid delivers at least once, as it did for ADF: an event can rarely arrive twice."]
    base.steps = [
        "Run `deploy --triggers` for a workspace: it writes this trigger's ARM template and parameters file with the workspace's pipeline ids.",
        f"Deploy them into {where} ({source} '{name}') "
        "(`az deployment group create`); a service principal's secret is referenced from Key Vault in the parameters file.",
        "Give the Logic App's managed identity (or the service principal) Contributor access to the Fabric workspace.",
        "At cutover set runPipelines to true (redeploy with it, or edit the Logic App's parameter) and stop the ADF trigger, so the "
        "pipeline does not run twice.",
    ]
    return base


# --- the Trigger_event activity: which trigger and event started a run, visible in Fabric's UI ------------------------------------

# Parameters the bridge passes and the converter adds to a pipeline an event trigger starts (the user's request, 2026-11-19: the bridge is
# not visible in Fabric, so each run shows which trigger and event started it).
TRIGGER_PARAMS = ("triggerName", "triggerEventType", "triggerEventSubject", "triggerEventTime")
TRIGGER_EVENT_MARK = {"name": "adf2fabric", "value": "trigger_event"}  # userProperties marker: validation knows the tool added it


def bridged_pipelines(triggers: list[AdfTrigger]) -> dict[str, list[str]]:
    """Pipeline (lower) -> the event triggers with a generated bridge that start it."""
    out: dict[str, list[str]] = {}
    for t in triggers:
        if (t.type or "").lower() in ("blobeventstrigger", "customeventstrigger") and plan(t).status in ("generated", "generated_with_differences"):
            for p in t.pipeline_refs:
                out.setdefault(p.lower(), []).append(t.name)
    return out


def is_trigger_event(activity: dict) -> bool:
    return TRIGGER_EVENT_MARK in (activity.get("userProperties") or []) and activity.get("type") == "SetVariable"


def add_trigger_event(content: dict, triggers: list[str], activity_name: str, limit: int) -> tuple[list[str], list[str]]:
    """Add the four trigger parameters, a variable and a ``Trigger_event`` Set variable activity (no dependencies, so the pipeline's own order is
    unchanged) to ``content`` in place. Returns (parameters added, notes)."""
    props = content.setdefault("properties", {})
    params, variables, acts = props.setdefault("parameters", {}), props.setdefault("variables", {}), props.setdefault("activities", [])
    taken = [k for k in TRIGGER_PARAMS if k.lower() in {x.lower() for x in params}]
    if taken:
        return [], [f"No Trigger_event activity: the pipeline already has parameter(s) {', '.join(taken)}."]
    if len(acts) + 1 > limit:
        return [], [f"No Trigger_event activity: the pipeline would exceed Fabric's {limit} activities."]
    var = "trigger_event"
    while var.lower() in {x.lower() for x in variables}:
        var += "_"
    for k in TRIGGER_PARAMS:
        params[k] = {"type": "String", "defaultValue": ""}
    variables[var] = {"type": "String"}
    a, b, c, d = (f"pipeline().parameters.{k}" for k in TRIGGER_PARAMS)
    acts.append({
        "name": activity_name, "type": "SetVariable", "dependsOn": [], "userProperties": [dict(TRIGGER_EVENT_MARK)],
        "description": "Added by adf2fabric: which event trigger (Event Grid -> Logic App bridge) started this run. Not in the ADF source.",
        "typeProperties": {"variableName": var, "value": {"type": "Expression", "value": (
            f"@if(empty({a}), 'Started by hand or by another pipeline (no trigger)', "
            f"concat({a}, ' | ', {b}, ' | ', {c}, ' | event time ', {d}))")}}})
    return list(TRIGGER_PARAMS), [f"Activity '{activity_name}' was added: it records which event trigger ({', '.join(sorted(triggers))}) and which "
                                  "event started each run, because the Event Grid -> Logic App bridge is not visible in Fabric."]


# --- the ARM template ------------------------------------------------------------------------------------------------------------

def _slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9-]+", "-", name).strip("-").lower()[:60] or "trigger"


def _value(spec: dict, ptype: Optional[str]) -> Any:
    """The Logic App expression (or literal) producing one pipeline parameter."""
    src = spec["source"]
    if src == "fileName":
        return "@outputs('File_name')"
    if src == "folderPath":
        return "@outputs('Folder_path')"
    if src == "event":
        return f"@first(triggerBody())?['{spec['field']}']"
    if src == "data":
        return "@first(triggerBody())?['data']" + "".join(f"?['{k}']" for k in spec["path"])
    cast = _LITERAL_TYPES.get((ptype or "").lower())
    if cast is bool:
        return str(spec["value"]).lower() == "true"
    if cast is not None:
        try:
            return cast(spec["value"])
        except ValueError:
            return spec["value"]
    return spec["value"]


def _http(workspace_id: str, pipeline_id: str, params: dict[str, Any], auth: dict) -> dict:
    # the retry policy sits inside "inputs" (a top-level one is refused: "Could not find member 'retryPolicy'", live 2026-11-19)
    inputs: dict[str, Any] = {"method": "POST", "uri": f"{cloud.FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
                              "authentication": auth, "retryPolicy": {"type": "exponential", "count": 4, "interval": "PT10S"}}
    if params:
        inputs["body"] = {"executionData": {"parameters": params}}
    return {"type": "Http", "inputs": inputs, "runAfter": {}}


def workflow_definition(p: EventBridgePlan, *, workspace_id: str, pipeline_ids: dict[str, str], param_types: dict[str, dict[str, str]],
                        auth: dict) -> dict:
    """The Logic App: answer Event Grid's validation; work out the event's parameters, then (if runPipelines) start each pipeline.

    One event per call: the subscription sets maxEventsPerBatch = 1, so the event is ``first(triggerBody())`` and no loop is needed (a loop
    would also forbid the Terminate step that stops a run whose data key is missing; Logic Apps refused it, live 2026-11-19)."""
    ids = {k.lower(): v for k, v in pipeline_ids.items()}
    runs: dict[str, Any] = {}
    for i, pl in enumerate(p.pipelines):
        types = {k.lower(): v for k, v in (param_types.get(pl["name"].lower()) or {}).items()}
        params = {name: _value(spec, types.get(name.lower())) for name, spec in pl["parameters"].items()}
        if all(k.lower() in types for k in TRIGGER_PARAMS):  # the converted pipeline records the trigger (Trigger_event activity)
            params.update({"triggerName": p.trigger, "triggerEventType": "@first(triggerBody())?['eventType']",
                           "triggerEventSubject": "@first(triggerBody())?['subject']", "triggerEventTime": "@first(triggerBody())?['eventTime']"})
        runs[f"Run_pipeline_{i + 1}"] = _http(workspace_id, ids[pl["name"].lower()], params, auth)
    per_event: dict[str, Any] = {}
    after: dict[str, Any] = {}
    if p.kind == "storage":  # subject: /blobServices/default/containers/<container>/blobs/<folders>/<file>
        per_event = {
            "Blob_path": {"type": "Compose", "inputs": f"@substring(first(triggerBody())?['subject'], {_SUBJECT_LEN})", "runAfter": {}},
            "Container": {"type": "Compose", "inputs": "@first(split(outputs('Blob_path'), '/'))", "runAfter": {"Blob_path": ["Succeeded"]}},
            "Blob_name": {"type": "Compose", "inputs": "@substring(outputs('Blob_path'), add(length(outputs('Container')), 7))",
                          "runAfter": {"Container": ["Succeeded"]}},
            "File_name": {"type": "Compose", "inputs": "@last(split(outputs('Blob_name'), '/'))", "runAfter": {"Blob_name": ["Succeeded"]}},
            "Folder_path": {"type": "Compose", "runAfter": {"File_name": ["Succeeded"]},
                            "inputs": "@if(greater(lastIndexOf(outputs('Blob_name'), '/'), 0), concat(outputs('Container'), '/', "
                                      "substring(outputs('Blob_name'), 0, lastIndexOf(outputs('Blob_name'), '/'))), outputs('Container'))"},
        }
        after = {"Folder_path": ["Succeeded"]}
    gate: dict[str, Any] = {"type": "If", "expression": {"and": [{"equals": ["@parameters('runPipelines')", True]}]},
                            "actions": runs, "runAfter": after,
                            "else": {"actions": {"Not_started": {"type": "Compose", "runAfter": {},
                                                                 "inputs": "runPipelines is false: no pipeline was started for this event"}}}}
    needed = sorted({tuple(spec["path"]) for pl in p.pipelines for spec in pl["parameters"].values() if spec["source"] == "data"})
    if needed:  # ADF fails the trigger run, and starts no pipeline, when a data key a parameter reads is missing
        cond = [{"not": {"equals": ["@first(triggerBody())?['data']" + "".join(f"?['{k}']" for k in path), "@null"]}} for path in needed]
        per_event["Data_keys_present"] = {
            "type": "If", "expression": {"and": cond}, "runAfter": after, "actions": {"Start": {**gate, "runAfter": {}}},
            "else": {"actions": {"Missing_data_key": {"type": "Terminate", "runAfter": {}, "inputs": {
                "runStatus": "Failed", "runError": {"code": "MissingDataKey",
                                                    "message": "A data key a pipeline parameter reads is missing: no pipeline was started (as in ADF)."}}}}}}
    else:
        per_event["Start"] = gate
    return {
        "$schema": WORKFLOW_SCHEMA, "contentVersion": "1.0.0.0",
        "parameters": {"runPipelines": {"type": "Bool", "defaultValue": False},
                       "fabricClientSecret": {"type": "SecureString", "defaultValue": ""}},
        "triggers": {"manual": {"type": "Request", "kind": "Http", "inputs": {"schema": {"type": "array"}}}},
        "actions": {"Is_validation": {
            "type": "If", "runAfter": {},
            "expression": {"and": [{"equals": ["@first(triggerBody())?['eventType']", "Microsoft.EventGrid.SubscriptionValidationEvent"]}]},
            "actions": {"Validation_response": {"type": "Response", "kind": "Http", "runAfter": {}, "inputs": {
                "statusCode": 200, "body": {"validationResponse": "@first(triggerBody())?['data']?['validationCode']"}}}},
            "else": {"actions": {
                "Accepted": {"type": "Response", "kind": "Http", "runAfter": {}, "inputs": {"statusCode": 200}},
                **{k: ({**v, "runAfter": {"Accepted": ["Succeeded"]}} if not v.get("runAfter") else v) for k, v in per_event.items()}}}}},
        "outputs": {},
    }


def arm_template(p: EventBridgePlan, *, workspace_id: str, pipeline_ids: dict[str, str], param_types: dict[str, dict[str, str]],
                 identity: str = "managed_identity", tenant_id: Optional[str] = None, client_id: Optional[str] = None) -> tuple[dict, dict]:
    """(ARM template, parameters file) for one trigger. ``identity``: managed_identity | service_principal (then tenant_id, client_id)."""
    if p.kind not in ("storage", "custom") or p.status not in ("generated", "generated_with_differences"):
        raise ValueError("this trigger has no generated bridge")
    missing = [pl["name"] for pl in p.pipelines if pl["name"].lower() not in {k.lower() for k in pipeline_ids}]
    if missing:
        raise ValueError(f"pipeline(s) not in the workspace: {', '.join(missing)}")
    resource = cloud.FABRIC_ENDPOINT
    if identity == "managed_identity":
        auth = {"type": "ManagedServiceIdentity", "audience": resource}
    elif identity == "service_principal":
        if not (tenant_id and client_id):
            raise ValueError("a service principal needs its tenant id and client id")
        auth = {"type": "ActiveDirectoryOAuth", "authority": "https://login.microsoftonline.com", "tenant": tenant_id, "audience": resource,
                "clientId": client_id, "secret": "@parameters('fabricClientSecret')"}
    else:
        raise ValueError(f"unknown identity {identity!r}")
    definition = workflow_definition(p, workspace_id=workspace_id, pipeline_ids=pipeline_ids, param_types=param_types, auth=auth)
    scope_type = "Microsoft.Storage/storageAccounts" if p.kind == "storage" else "Microsoft.EventGrid/topics"
    workflow: dict[str, Any] = {
        "type": "Microsoft.Logic/workflows", "apiVersion": LOGIC_API, "name": "[parameters('logicAppName')]",
        "location": "[parameters('location')]",
        "properties": {"state": "Enabled", "definition": definition, "parameters": {
            "runPipelines": {"value": "[parameters('runPipelines')]"}, "fabricClientSecret": {"value": "[parameters('fabricClientSecret')]"}}}}
    if identity == "managed_identity":
        workflow["identity"] = {"type": "SystemAssigned"}
    template = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#", "contentVersion": "1.0.0.0",
        "metadata": {"description": f"ADF {('storage' if p.kind == 'storage' else 'custom')} event trigger '{p.trigger}' -> Event Grid "
                                    "subscription (the same filters) -> Logic App -> Fabric Run On Demand Item Job. Generated by adf2fabric."},
        "parameters": {
            "logicAppName": {"type": "string", "defaultValue": f"la-adf2fabric-{_slug(p.trigger)}"},
            "location": {"type": "string", "defaultValue": "[resourceGroup().location]"},
            "eventSourceName": {"type": "string", "metadata": {"description": f"The {scope_type.rsplit('/', 1)[-1][:-1]} the ADF trigger listened to"}},
            "runPipelines": {"type": "bool", "defaultValue": False, "metadata": {"description": "false until cutover: events are received, no pipeline starts"}},
            "fabricClientSecret": {"type": "securestring", "defaultValue": ""},
        },
        "resources": [workflow, {
            "type": "Microsoft.EventGrid/eventSubscriptions", "apiVersion": GRID_API, "name": f"adf2fabric-{_slug(p.trigger)}",
            "scope": f"[format('{scope_type}/{{0}}', parameters('eventSourceName'))]",
            "dependsOn": ["[resourceId('Microsoft.Logic/workflows', parameters('logicAppName'))]"],
            "properties": {
                "destination": {"endpointType": "WebHook", "properties": {
                    "endpointUrl": f"[listCallbackUrl(resourceId('Microsoft.Logic/workflows/triggers', parameters('logicAppName'), 'manual'), '{LOGIC_API}').value]",
                    "maxEventsPerBatch": 1}},
                "filter": p.event_grid_filter, "eventDeliverySchema": "EventGridSchema",
                "retryPolicy": {"maxDeliveryAttempts": 30, "eventTimeToLiveInMinutes": 1440}}}],
        "outputs": ({"logicAppPrincipalId": {"type": "string", "value": "[reference(resourceId('Microsoft.Logic/workflows', parameters('logicAppName')), "
                                                                         f"'{LOGIC_API}', 'full').identity.principalId]"}}
                    if identity == "managed_identity" else {}),
    }
    name = (p.scope or "").rstrip("/").rsplit("/", 1)[-1]
    params: dict[str, Any] = {"eventSourceName": {"value": name}, "runPipelines": {"value": False}}
    if identity == "service_principal":
        params["fabricClientSecret"] = {"reference": {"keyVault": {"id": "<the Key Vault's resource id>"}, "secretName": "<the secret's name>"}}
    parameters = {"$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#", "contentVersion": "1.0.0.0",
                  "parameters": params}
    return template, parameters
