"""ADF storage event trigger (BlobEventsTrigger) -> a Fabric Activator (Reflex) rule that runs the pipeline.

Fabric starts a pipeline on a file event through an Activator rule (Fabric "Run, schedule, or use events to trigger a pipeline",
page updated 2026-09-21). The Activator item's definition, ``ReflexEntities.json``, is documented by the Fabric REST "Reflex
definition" page (updated 2026-02-26) for containers, Real-Time hub sources, Fabric item actions, event views and rules. The exact
entities a pipeline trigger needs were read back from a trigger made in the Fabric portal on 2026-11-18, and an Activator created
from code in that shape was accepted and saved unchanged (projects/<p>/verification/event-trigger, not committed). Live findings:

* A OneLake events source (``realTimeHubSource-v1``, scope ``Artifact``, ``Microsoft.Fabric.OneLakeEvents``) needs no Eventstream; the
  rule ran the pipeline 4 seconds after a file was written to the lakehouse folder.
* OneLake events are NOT raised for a file written to ADLS behind a OneLake shortcut (no run in 6 minutes). A trigger is therefore
  pointed at a lakehouse only when a person says the files now land there (``convert --onelake-trigger``), never because datasets
  were repointed through shortcuts (``convert --lakehouse``).
* The Azure Blob Storage events source could not be read (the storage account was in another Entra tenant, which the Fabric wizard
  does not list). It is not generated: for a trigger that keeps watching Azure storage the plan says exactly what to set in the
  Fabric Trigger wizard, and the tool never guesses that entity.
* A custom event trigger (Event Grid custom topic) has no Fabric source at all: Fabric's Event Grid source reads namespace topics only
  (Fabric "Connect an Event Grid namespace source", updated 2026-06-30). The plan gives the two routes (an Event Hub fed by a new Event
  Grid subscription that carries the ADF filters exactly, or a namespace) and the Activator rule to add; nothing is generated.

Rules: the rule is created switched off (``shouldRun`` false) unless deploy is asked to enable rules whose ADF trigger was started;
an existing Activator of the same name is never modified. Custom event triggers have no generated form.

Statuses: ``generated_with_differences`` (OneLake: the watched store itself changes) | ``manual`` | ``not_supported``.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from adf2fabric.project.discovery import AdfTrigger

TEMPLATE_VERSION = "1.3.1"  # what the portal wrote on 2026-11-18 (the documentation's examples show 1.1)
ONELAKE_EVENT_GROUP = "Microsoft.Fabric.OneLakeEvents"
ONELAKE_EVENTS = {"microsoft.storage.blobcreated": "Microsoft.Fabric.OneLake.FileCreated",
                  "microsoft.storage.blobdeleted": "Microsoft.Fabric.OneLake.FileDeleted"}
AZURE_EVENTS = {"microsoft.storage.blobcreated": "Microsoft.Storage.BlobCreated", "microsoft.storage.blobdeleted": "Microsoft.Storage.BlobDeleted"}
# The event fields the portal's pipeline trigger passes to the pipeline (it reads them as pipeline()?.TriggerEvent).
EVENT_FIELDS = (("Type", "___type"), ("Subject", "___subject"), ("Source", "___source"))
UNSUPPORTED_CAPACITY_REGIONS = ("Central US", "Germany West Central", "South Central US", "West US 2", "West US 3", "West India")
_NS = uuid.UUID("5f0e9a4c-3b1d-4c6e-8a2f-adf2fab0e7e1")
_SEGMENTS = re.compile(r"^/?([^/]+)/(?:blobs/?(.*))?$")


@dataclass
class OneLakeTarget:
    """Where the files land in Fabric: a lakehouse, and the folder that stands for the ADF container (default ``Files/<container>``)."""

    lakehouse: str
    folder: Optional[str] = None

    @classmethod
    def parse(cls, text: str) -> "OneLakeTarget":
        """``LAKEHOUSE`` or ``LAKEHOUSE:Files/<folder>``."""
        lh, _, folder = text.partition(":")
        if not lh.strip():
            raise ValueError("a lakehouse name is required")
        folder = folder.strip().strip("/")
        if folder and not folder.lower().startswith("files"):
            raise ValueError(f"the folder must be under Files (got '{folder}')")
        return cls(lh.strip(), folder or None)


@dataclass
class EventTriggerPlan:
    status: str
    target: str  # onelake | azure_blob | custom_event | none
    trigger: str = ""
    lakehouse: Optional[str] = None
    event_types: list[str] = field(default_factory=list)
    filters: list[dict[str, Any]] = field(default_factory=list)  # Real-Time hub filters: {key, operatorType, values}
    pipelines: list[str] = field(default_factory=list)
    started_in_adf: bool = False
    scope: Optional[str] = None  # the ADF trigger's storage account (or custom topic) resource id (read-only checks use it)
    # custom events: the Event Grid event subscription filter that carries the ADF trigger's filters exactly (route A)
    event_grid_filter: Optional[dict[str, Any]] = None
    notes: list[str] = field(default_factory=list)  # differences from the ADF trigger
    steps: list[str] = field(default_factory=list)  # what a person still does, in order

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _container_and_rest(path: Optional[str]) -> Optional[tuple[str, str]]:
    """ADF writes ``/<container>/blobs/<path>`` (or ``/<container>/``); None for any other form (a plain suffix such as ``.csv``)."""
    if not path or not path.startswith("/"):
        return None
    m = _SEGMENTS.match(path)
    if not m:
        return None
    return m.group(1), m.group(2) or ""


def _parameter_notes(t: AdfTrigger) -> list[str]:
    out = []
    for pipeline, params in sorted(t.pipeline_parameters.items()):
        for name, value in sorted(params.items()):
            low = value.replace(" ", "").lower()
            where = f"parameter '{name}' of '{pipeline}' (ADF passed `{value[:120]}`)"
            if low.startswith("@triggerbody().filename"):
                out.append(f"The rule does not set {where}: in the Fabric pipeline read the file name with `@pipeline()?.TriggerEvent?.FileName` "
                           "(documented) instead of the parameter.")
            elif low.startswith("@triggerbody().folderpath"):
                out.append(f"The rule does not set {where}: Fabric also parses the folder from the event (the expression builder's Trigger "
                           "parameters tab); its exact property name was not checked by this tool, so pick it there.")
            elif low.startswith(("@trigger", "@triggerbody", "@triggeroutputs")):
                out.append(f"The rule does not set {where}: it has no Fabric trigger equivalent.")
            else:
                out.append(f"The rule does not set {where}: give the pipeline parameter that default value, or add it to the rule in Activator.")
    return out


def plan(t: AdfTrigger, onelake: Optional[OneLakeTarget] = None) -> EventTriggerPlan:
    """The Fabric form of one ADF event trigger (see the module docstring for the rules)."""
    kind = (t.type or "").lower()
    started = (t.runtime_state or "").lower() == "started"
    base = EventTriggerPlan("manual", "none", trigger=t.name, pipelines=sorted(t.pipeline_refs), started_in_adf=started)
    if kind == "customeventstrigger":
        return _custom(t, base)
    if kind != "blobeventstrigger":
        base.notes = [f"Trigger type '{t.type}' is not a storage event trigger."]
        return base
    e = t.event
    if e is None or not e.events:
        base.notes = ["The trigger's events could not be read."]
        return base
    if not t.pipeline_refs:
        base.notes = ["The trigger starts no pipeline."]
        return base
    table = ONELAKE_EVENTS if onelake else AZURE_EVENTS
    unknown = [x for x in e.events if x.lower() not in table]
    if unknown:
        base.notes = [f"Event type(s) {', '.join(unknown)} have no mapping."]
        return base

    begins = _container_and_rest(e.blob_path_begins_with)
    if e.blob_path_begins_with and begins is None:
        base.notes = [f"'Blob path begins with' {e.blob_path_begins_with!r} is not in ADF's /<container>/blobs/<path> form."]
        return base
    ends_text = e.blob_path_ends_with or ""
    ends = _container_and_rest(ends_text) if ends_text.startswith("/") else None
    container, prefix = begins if begins else (ends[0] if ends else None, "")
    if ends:  # '/<container>/blobs/<file>': the container narrows the begins filter, the rest is the suffix
        if begins and begins[0].lower() != ends[0].lower():
            base.notes = ["'Blob path begins with' and 'ends with' name different containers; the ADF trigger can never fire."]
            return base
        ends_text = ends[1]
    if not container and not ends_text:
        base.notes = ["The trigger names neither a container nor a file suffix."]
        return base

    notes = _parameter_notes(t)
    if e.ignore_empty_blobs:
        notes.append("ADF ignored empty files; the Fabric rule also fires for them (OneLake and Blob events carry data.contentLength, but a filter on "
                     "it was not verified). Add that filter in Activator, or let the pipeline check the size (Get Metadata).")
    events = [table[x.lower()] for x in e.events]
    filters: list[dict[str, Any]] = []
    if onelake:
        folder = (onelake.folder or (f"Files/{container}" if container else "Files")).strip("/")
        start = "/" + folder + "/" + prefix if prefix else "/" + folder + "/"
        filters.append({"key": "subject", "operatorType": "StringBeginsWith", "values": [start]})
        if ends_text:
            filters.append({"key": "subject", "operatorType": "StringEndsWith", "values": [ends_text]})
        if not container:
            notes.append("The ADF trigger watched every container of the storage account; the rule watches the whole Files area of the lakehouse.")
        if not onelake.folder:
            notes.append(f"Container '{container}' was taken to be the lakehouse folder '{folder}': confirm the files are written there.")
        notes.insert(0, f"The rule watches lakehouse '{onelake.lakehouse}' ({folder}): the files must now be WRITTEN to OneLake there. OneLake events "
                        "are not raised for files written to Azure storage, even behind a shortcut (tested 2026-11-18).")
        # always a difference: ADF watched Azure storage, the rule watches the lakehouse
        return EventTriggerPlan("generated_with_differences", "onelake", t.name, onelake.lakehouse, events, filters,
                                sorted(t.pipeline_refs), started, scope=e.scope, notes=notes,
                                steps=["Deploy it (deploy --triggers); the rule is created switched off.",
                                       "Switch the rule on in Activator (or deploy --enable-triggers) when the files land in the lakehouse."])

    # Azure storage stays the source: the Azure Blob Storage events source is not generated (its definition was never read).
    if container:
        subj = f"/blobServices/default/containers/{container}/" + (f"blobs/{prefix}" if prefix else "")
        filters.append({"key": "subject", "operatorType": "StringBeginsWith", "values": [subj]})
    if ends_text:
        filters.append({"key": "subject", "operatorType": "StringEndsWith", "values": [ends_text]})
    account = (e.scope or "").rstrip("/").rsplit("/", 1)[-1] or "(the storage account in the trigger's scope)"
    sub = re.search(r"/subscriptions/([^/]+)", e.scope or "", re.I)
    lines = [f"{f['operatorType']} `{f['values'][0]}`" for f in filters]
    steps = [
        "Check first: the storage account's subscription must be in the same Entra tenant as the Fabric workspace (Fabric lists only those), "
        "the account must be StorageV2, BlockBlobStorage or BlobStorage, and you need rights to create an Event Grid subscription on it.",
        f"In Fabric open each pipeline ({', '.join(sorted(t.pipeline_refs))}) and select Home > Trigger > Azure Blob Storage events.",
        f"Choose subscription {sub.group(1) if sub else '(from the trigger scope)'} and storage account '{account}'.",
        f"Event types: {', '.join(events)} only.",
        "Filters on the Subject field: " + ("; ".join(lines) if lines else "none") + ".",
        "Save the Activator rule switched off; switch it on at cutover.",
    ]
    return EventTriggerPlan("manual", "azure_blob", t.name, None, events, filters, sorted(t.pipeline_refs), started, scope=e.scope,
                            notes=["The Azure Blob Storage events source is created in the Fabric Trigger wizard: its definition is not documented "
                                   "and was not generated. Everything to type is listed in the steps."] + notes, steps=steps)


# --- custom event triggers -------------------------------------------------------------------------------------------------------

# Event Grid advanced filter operators that take ONE ``value`` instead of ``values`` (Event Grid event subscription schema).
_SINGLE_VALUE = {"numberlessthan", "numbergreaterthan", "numberlessthanorequals", "numbergreaterthanorequals", "boolequals"}


def _event_grid_filter(e) -> dict[str, Any]:
    """The ADF trigger's filters as an Event Grid event subscription ``filter``: ADF's own trigger is an Event Grid subscription with these
    same filters, so on a new subscription that delivers to an Event Hub they behave exactly as before."""
    out: dict[str, Any] = {}
    if e.events:
        out["includedEventTypes"] = list(e.events)
    if e.subject_begins_with:
        out["subjectBeginsWith"] = e.subject_begins_with
    if e.subject_ends_with:
        out["subjectEndsWith"] = e.subject_ends_with
    adv = []
    for f in e.advanced_filters:
        one = f["operatorType"].lower() in _SINGLE_VALUE and len(f["values"]) == 1
        adv.append({"operatorType": f["operatorType"], "key": f["key"], **({"value": f["values"][0]} if one else {"values": f["values"]})})
    if adv:
        out["advancedFilters"] = adv
    return out


def _custom_parameter_notes(t: AdfTrigger) -> list[str]:
    out = []
    for pipeline, params in sorted(t.pipeline_parameters.items()):
        for name, value in sorted(params.items()):
            low = value.replace(" ", "").lower()
            m = re.match(r"@triggerbody\(\)\.event\.(data\.[\w.]+|subject|eventtype|id|topic|eventtime)$", value.replace(" ", ""), re.I)
            where = f"parameter '{name}' of '{pipeline}' (ADF passed `{value[:120]}`)"
            if m:
                out.append(f"Map {where} in the Activator rule's 'run pipeline' action to the event field `{m.group(1)}` (a rule passes event fields "
                           "as pipeline parameters). ADF refused the run when a data key was missing; check what the rule does with a missing field.")
            elif low.startswith(("@trigger", "@triggerbody", "@triggeroutputs")):
                out.append(f"{where[0].upper()}{where[1:]} has no Fabric trigger equivalent.")
            else:
                out.append(f"Give {where} that value as its default, or add it to the rule's action in Activator.")
    return out


def _custom(t: AdfTrigger, base: EventTriggerPlan) -> EventTriggerPlan:
    """A custom event trigger: Fabric has no source for an Event Grid custom topic, so the plan gives the two documented routes and the exact
    filter to carry; nothing is generated."""
    e = t.event
    base.target = "custom_event"
    if e is None or not e.scope:
        base.notes = ["The trigger's topic could not be read."]
        return base
    if not t.pipeline_refs:
        base.notes = ["The trigger starts no pipeline."]
        return base
    flt = _event_grid_filter(e)
    topic = e.scope.rstrip("/").rsplit("/", 1)[-1]
    pipes = ", ".join(sorted(t.pipeline_refs))
    base.scope, base.event_grid_filter = e.scope, flt
    base.notes = [
        "Fabric has no source for an Event Grid custom topic: its Event Grid source reads Event Grid NAMESPACE topics only. The events reach "
        "Fabric through an Eventstream (from an Event Hub, or from a namespace), and an Activator rule on it runs the pipeline.",
        *([f"The ADF trigger had {len(e.advanced_filters)} advanced filter(s); route A keeps them on the Event Grid subscription."]
          if e.advanced_filters else []),
        *_custom_parameter_notes(t),
    ]
    base.steps = [
        f"Route A (the publishers do not change): on topic '{topic}' ({e.scope}) add an Event Grid event subscription that delivers to an Azure "
        f"Event Hub, with this filter, which carries the ADF trigger's filters exactly: `{json.dumps(flt, separators=(',', ':'))}`. "
        "Route B instead: move the publishers to an Event Grid namespace topic.",
        "In Fabric create an Eventstream with the source 'Azure Event Hubs' (route A; Shared Access Key or workspace identity, data format JSON) "
        "or 'Azure Event Grid Namespace' (route B; the namespace needs a managed identity).",
        f"On the Eventstream add an Activator rule 'on each event' whose action runs {pipes}"
        + (", mapping the event fields to the pipeline parameters listed in the notes." if t.pipeline_parameters else "."),
        "Keep the rule switched off until cutover; then switch it on and stop the ADF trigger, so the pipeline does not run twice.",
    ]
    return base


# --- the Activator definition (Reflex) -------------------------------------------------------------------------------------------

def _id(trigger: str, *parts: str) -> str:
    """Stable ids: the same trigger always gives the same definition."""
    return str(uuid.uuid5(_NS, "/".join((trigger.lower(),) + parts)))


def _arg(name: str, value: str) -> dict:
    return {"name": name, "type": "string", "value": value}


def item_name(plan_: EventTriggerPlan) -> str:
    """The Activator item: one per ADF trigger, named after it."""
    return plan_.trigger


def reflex_entities(p: EventTriggerPlan, *, tenant_id: str, workspace_id: str, lakehouse_id: str, pipeline_ids: dict[str, str],
                    enable: bool = False) -> list[dict]:
    """``ReflexEntities.json`` for a ``onelake`` plan, in the shape the Fabric portal writes. ``pipeline_ids``: pipeline name (any case) -> item
    id in ``workspace_id``. The rules are switched on only when ``enable`` and the ADF trigger was started."""
    if p.target != "onelake":
        raise ValueError("only a OneLake trigger has a generated Activator definition")
    ids = {k.lower(): v for k, v in pipeline_ids.items()}
    missing = [n for n in p.pipelines if n.lower() not in ids]
    if missing:
        raise ValueError(f"pipeline(s) not in the workspace: {', '.join(missing)}")
    t = p.trigger
    container, source, event = _id(t, "container"), _id(t, "source"), _id(t, "event")
    parent = {"targetUniqueIdentifier": container}
    out: list[dict] = [
        {"uniqueIdentifier": container, "payload": {"name": p.lakehouse or t, "description": f"Migrated from ADF trigger '{t}'", "type": "rthSubscriptions"},
         "type": "container-v1"},
        {"uniqueIdentifier": source, "payload": {
            "name": f"{t} events",
            "connection": {"scope": "Artifact", "tenantId": tenant_id, "workspaceId": workspace_id, "artifactId": lakehouse_id,
                           "eventGroupType": ONELAKE_EVENT_GROUP},
            "filterSettings": {"eventTypes": [{"name": n} for n in p.event_types], "filters": [dict(f) for f in p.filters]},
            "parentContainer": parent}, "type": "realTimeHubSource-v1"},
        {"uniqueIdentifier": event, "payload": {"name": f"{t} events", "parentContainer": parent, "definition": {"type": "Event", "instance": json.dumps({
            "templateId": "SourceEvent", "templateVersion": TEMPLATE_VERSION,
            "steps": [{"name": "SourceEventStep", "id": _id(t, "event", "step"),
                       "rows": [{"name": "SourceSelector", "kind": "SourceReference", "arguments": [_arg("entityId", source)]}]}]})}},
         "type": "timeSeriesView-v1"},
    ]
    run = enable and p.started_in_adf
    for name in p.pipelines:
        pid, action = ids[name.lower()], _id(t, "action", name)
        out.append({"uniqueIdentifier": action, "payload": {
            "name": f"Run {name}", "fabricItem": {"itemId": pid, "workspaceId": workspace_id, "itemType": "Pipeline"}, "jobType": "Pipeline",
            "parentContainer": parent}, "type": "fabricItemAction-v1"})
        params = [{"kind": "FabricItemParameter", "type": "complex", "arguments": [
            _arg("parameterName", label),
            {"name": "parameterValue", "type": "complexArray", "values": [
                {"kind": "EventFieldReference", "type": "complexReference", "arguments": [_arg("fieldName", fld)]}]},
            _arg("parameterType", "String")]} for label, fld in EVENT_FIELDS]
        instance = {"templateId": "EventTrigger", "templateVersion": TEMPLATE_VERSION, "steps": [
            {"name": "FieldsDefaultsStep", "id": _id(t, "rule", name, "fields"), "rows": [{"name": "EventSelector", "kind": "Event", "arguments": [
                {"kind": "EventReference", "type": "complex", "arguments": [_arg("entityId", event)], "name": "event"}]}]},
            {"name": "EventDetectStep", "id": _id(t, "rule", name, "detect"), "rows": [{"name": "OnEveryValue", "kind": "OnEveryValue", "arguments": []}]},
            {"name": "ActStep", "id": _id(t, "rule", name, "act"), "rows": [{"name": "FabricItemBinding", "kind": "FabricItemInvocation", "arguments": [
                _arg("workspaceId", workspace_id), _arg("itemId", pid), _arg("itemType", "Pipeline"), _arg("jobType", "Pipeline"),
                _arg("fabricJobConnectionDocumentId", action),
                {"name": "parameters", "type": "array", "values": params},
                {"name": "additionalInformation", "type": "array", "values": []}]}]}]}
        out.append({"uniqueIdentifier": _id(t, "rule", name), "payload": {
            "name": t if len(p.pipelines) == 1 else f"{t}: {name}", "parentContainer": parent,
            "definition": {"type": "Rule", "instance": json.dumps(instance), "settings": {"shouldRun": run, "shouldApplyRuleOnUpdate": False}}},
            "type": "timeSeriesView-v1"})
    return out


def definition_parts(entities: list[dict]) -> dict[str, str]:
    return {"ReflexEntities.json": json.dumps(entities, ensure_ascii=False)}


def rules_running(entities: list[dict]) -> list[str]:
    """Names of the rules that are switched on."""
    return [e["payload"]["name"] for e in entities if e.get("type") == "timeSeriesView-v1"
            and (e["payload"].get("definition") or {}).get("type") == "Rule"
            and ((e["payload"]["definition"].get("settings") or {}).get("shouldRun"))]
