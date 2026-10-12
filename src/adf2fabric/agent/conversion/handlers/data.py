"""Data movement and data retrieval conversions: Copy, Lookup, GetMetadata, Delete, Validation."""

from __future__ import annotations

from typing import Any, Optional

from adf2fabric.agent.conversion.context import ActivityBuilder
from adf2fabric.agent.conversion.expr_convert import expr_obj, is_expression
from adf2fabric.agent.conversion.handlers._common import block, carry, dataset_settings
from adf2fabric.agent.conversion.registry import ConversionRule, register
from adf2fabric.project.conversion import ConversionStrategy
from adf2fabric.project.discovery import AdfOperationKind as Op

_COPY_DOCUMENTED = (
    "translator", "enableStaging", "parallelCopies", "dataIntegrationUnits", "enableSkipIncompatibleRow",
    "preserveRules", "preserve", "validateDataConsistency", "skipErrorFile", "resumeId", "adaptivePerformanceTuning",
)
# ADF settings whose Fabric structure is not documented: they reference a linked service (a
# connection in Fabric), so they are reported and must be re-created, not guessed.
_COPY_LOGGING = ("logSettings", "logStorageSettings", "redirectIncompatibleRowSettings", "sessionLog")


def _fail(cx: ActivityBuilder, code: str, reason: str, *alternatives: str) -> None:
    block(cx, code, reason, *alternatives)


# --- Copy --------------------------------------------------------------------------------------------


def convert_copy(cx: ActivityBuilder) -> None:
    act, raw = cx.act, cx.act.raw_properties
    source, sink = raw.get("source"), raw.get("sink")
    if not isinstance(source, dict) or not isinstance(sink, dict):
        cx.blocker("copy_incomplete", "A Copy activity needs both a source and a sink; the source defines "
                   f"{'neither' if not (source or sink) else 'only one'}.", "Complete the source activity.")
        return _fail(cx, "copy_incomplete", "the source or sink is missing.")
    if len(act.inputs) > 1 or len(act.outputs) > 1:
        cx.manual("copy_multiple_datasets", "The Copy activity lists more than one input or output dataset.",
                  "Split it into one Copy activity per source/sink dataset pair.")
    src_io = act.inputs[0] if act.inputs else None
    snk_io = act.outputs[0] if act.outputs else None
    ds_src = dataset_settings(cx, src_io, "inputs[0]", "typeProperties.source.datasetSettings")
    ds_snk = dataset_settings(cx, snk_io, "outputs[0]", "typeProperties.sink.datasetSettings")
    if ds_src is None or ds_snk is None:
        cx.alternative("Use a Fabric Copy job (guided source/destination setup) once the datasets are available.")
        return _fail(cx, "copy_dataset_unavailable", "its source or sink dataset could not be inlined.")

    tp: dict[str, Any] = {
        "source": {**cx.tree(source, "typeProperties.source", "typeProperties.source"), "datasetSettings": ds_src},
        "sink": {**cx.tree(sink, "typeProperties.sink", "typeProperties.sink"), "datasetSettings": ds_snk},
    }
    cx.mapping("typeProperties.source", "typeProperties.source", "copy_source",
               f"{source.get('type')} kept; dataset '{src_io.name}' inlined")
    cx.mapping("typeProperties.sink", "typeProperties.sink", "copy_sink",
               f"{sink.get('type')} kept; dataset '{snk_io.name}' inlined")
    cx.mapping("inputs[0]", "typeProperties.source.datasetSettings", "dataset_inlined")
    cx.mapping("outputs[0]", "typeProperties.sink.datasetSettings", "dataset_inlined")

    for key, value in raw.items():
        if key in ("source", "sink"):
            continue
        if key == "stagingSettings":
            st = value if isinstance(value, dict) else {}
            ls = (st.get("linkedServiceName") or {}).get("referenceName") if isinstance(
                st.get("linkedServiceName"), dict) else None
            out: dict[str, Any] = {}
            if ls:
                out["externalReferences"] = {"connection": cx.connection(ls, "staging store")}
                cx.mapping("typeProperties.stagingSettings.linkedServiceName",
                           "typeProperties.stagingSettings.externalReferences.connection",
                           "linked_service_to_connection")
            for k in ("path", "enableCompression"):
                if k in st:
                    out[k] = cx.tree(st[k], f"typeProperties.stagingSettings.{k}",
                                     f"typeProperties.stagingSettings.{k}")
                    cx.mapping(f"typeProperties.stagingSettings.{k}", f"typeProperties.stagingSettings.{k}",
                               "passthrough")
            for k in st:
                if k not in ("linkedServiceName", "path", "enableCompression"):
                    cx.dropped(f"typeProperties.stagingSettings.{k}", "not a documented Fabric staging property")
            tp["stagingSettings"] = out
        elif key in _COPY_LOGGING:
            cx.dropped(f"typeProperties.{key}",
                       "its Fabric structure is not documented and it references a linked service (a connection in "
                       "Fabric)", manual="Re-enable copy logging / fault-tolerance logging in the Fabric activity "
                       "and bind its storage connection.")
            cx.manual("copy_logging_reconfigure",
                      f"'{key}' (copy logging / incompatible-row redirection) must be re-created in Fabric.",
                      f"Reconfigure '{key}' on the Fabric Copy activity.")
        elif key in _COPY_DOCUMENTED:
            tp[key] = cx.tree(value, f"typeProperties.{key}", f"typeProperties.{key}")
            cx.mapping(f"typeProperties.{key}", f"typeProperties.{key}", "passthrough")
        else:
            cx.dropped(f"typeProperties.{key}", "not a documented Fabric Copy property")

    diu = tp.get("dataIntegrationUnits")
    if isinstance(diu, int) and 0 < diu < 4:
        cx.difference("diu_below_fabric_minimum",
                      f"dataIntegrationUnits={diu}: Fabric documents custom throughput values of 4-256.",
                      "Set a documented value or remove the property to use Auto.")
    if "translator" not in raw:
        cx.mapping("typeProperties.translator", None, "default_mapping",
                   "no explicit mapping in the source; Fabric uses its default tabular mapping like ADF")
    if "incremental_filter" in act.semantic_tags:
        cx.info("incremental_pattern", "This Copy reads with a watermark filter derived from an upstream activity; the "
                "query expression is preserved as written.",
                "Fabric Copy jobs offer built-in incremental copy as a possible simplification.")
    cx.output_notes.append("Copy output fields (rowsCopied, copyDuration, ...) are produced by the Fabric service.")

    name = cx.add(cx.envelope(act.name, "Copy", tp))
    cx.cover(Op.read, name)
    cx.cover(Op.write, name)
    cx.strategy = ConversionStrategy.transformed
    cx.summary = (f"Copy '{act.name}' -> Fabric Copy with inline datasets "
                  f"'{src_io.name}' -> '{snk_io.name}' (datasets inlined, connections bound by reference).")


register(ConversionRule(
    name="copy", adf_types=("Copy",), fabric_types=("Copy",),
    variants=("source/sink connector pairs", "explicit translator", "staged copy", "query/stored-procedure source",
              "parameterized datasets", "incremental (watermark) filter"),
    strategy=ConversionStrategy.transformed, handler=convert_copy, runtime_disposition="replace",
    property_rules=("source/sink objects kept (type, query, partitioning, store/format settings)",
                    "documented Copy settings carried (parallelCopies, dataIntegrationUnits, validateDataConsistency, "
                    "skipErrorFile, enableStaging, translator, preserve)",
                    "stagingSettings.linkedServiceName -> stagingSettings.externalReferences.connection"),
    io_rules=("input dataset -> source.datasetSettings", "output dataset -> sink.datasetSettings",
              "dataset linkedServiceName -> datasetSettings.externalReferences.connection",
              "dataset parameters substituted with the activity's arguments (or declared defaults)"),
    expression_rules=("dataset().x inlined; pipeline().Pipeline -> PipelineName; globalParameters -> gp_ parameters",
                      "all other expressions pass through unchanged"),
    dependency_rules=("dependsOn mapped 1:1 onto the single generated Copy",),
    parameter_variable_rules=("parameters/variables referenced unchanged; globals become gp_ parameters",),
    unsupported=("logSettings / logStorageSettings / redirectIncompatibleRowSettings (undocumented Fabric structure)",
                 "any ADF Copy property not in the documented Fabric Copy table"),
    manual_configuration=("one Fabric connection per linked service", "re-create copy logging if used",
                         "on-premises data gateway for self-hosted integration runtimes"),
    validation_rules=("source and sink present with a type and datasetSettings", "documented typeProperties only",
                      "connection bound"),
))


# --- Lookup -----------------------------------------------------------------------------------------------


def convert_lookup(cx: ActivityBuilder) -> None:
    act, raw = cx.act, cx.act.raw_properties
    src = raw.get("source")
    io = act.inputs[0] if act.inputs else None
    if not isinstance(src, dict):
        cx.blocker("lookup_incomplete", "A Lookup activity needs a source.")
        return _fail(cx, "lookup_incomplete", "the source is missing.")
    ds = dataset_settings(cx, io, "typeProperties.dataset", "typeProperties.datasetSettings")
    if ds is None:
        cx.alternative("Use a Script activity (Query) when the data lives in a supported SQL store.")
        return _fail(cx, "lookup_dataset_unavailable", "its dataset could not be inlined.")
    tp: dict[str, Any] = {"source": cx.tree(src, "typeProperties.source", "typeProperties.source"),
                          "datasetSettings": ds}
    cx.mapping("typeProperties.source", "typeProperties.source", "lookup_source", f"{src.get('type')} kept")
    cx.mapping("typeProperties.dataset", "typeProperties.datasetSettings", "dataset_inlined")
    if "firstRowOnly" in raw:
        tp["firstRowOnly"] = cx.tree(raw["firstRowOnly"], "typeProperties.firstRowOnly", "typeProperties.firstRowOnly")
        cx.mapping("typeProperties.firstRowOnly", "typeProperties.firstRowOnly", "passthrough")
    else:
        cx.mapping("typeProperties.firstRowOnly", None, "default_preserved",
                   "not set in the source; both ADF and Fabric default to true")
    for key in raw:
        if key not in ("source", "dataset", "firstRowOnly"):
            cx.dropped(f"typeProperties.{key}", "not a documented Fabric Lookup property")
    name = cx.add(cx.envelope(act.name, "Lookup", tp))
    cx.cover(Op.read, name)
    cx.strategy = ConversionStrategy.transformed
    cx.summary = f"Lookup '{act.name}' -> Fabric Lookup over inline dataset '{io.name}'."


register(ConversionRule(
    name="lookup", adf_types=("Lookup",), fabric_types=("Lookup",),
    variants=("table", "query", "stored procedure", "first row only", "all rows"),
    strategy=ConversionStrategy.transformed, handler=convert_lookup, runtime_disposition="replace",
    property_rules=("source kept as authored", "firstRowOnly kept (default true on both platforms)"),
    io_rules=("typeProperties.dataset -> typeProperties.datasetSettings (inlined, connection bound)",),
    expression_rules=("query/dataset expressions converted like Copy; downstream activity('x').output.firstRow/.value "
                      "references are unchanged",),
    dependency_rules=("dependsOn mapped 1:1",),
    parameter_variable_rules=("unchanged",),
    unsupported=("properties outside the documented Lookup table",),
    manual_configuration=("Fabric connection for the dataset's linked service",),
    validation_rules=("source and datasetSettings present",),
))


# --- GetMetadata / Delete -------------------------------------------------------------------------------------


def convert_dataset_activity(cx: ActivityBuilder) -> None:
    act = cx.act
    t = (act.type or "").lower()
    ftype = "GetMetadata" if t == "getmetadata" else "Delete"
    io = act.inputs[0] if act.inputs else None
    ds = dataset_settings(cx, io, "typeProperties.dataset", "typeProperties.datasetSettings")
    if ds is None:
        return _fail(cx, f"{t}_dataset_unavailable", "its dataset could not be inlined.")
    documented = {"GetMetadata": ("fieldList", "storeSettings", "formatSettings"),
                  "Delete": ("recursive", "maxConcurrentConnections", "enableLogging", "storeSettings")}[ftype]
    tp = {"datasetSettings": ds, **carry(cx, documented, skip=("dataset", "logStorageSettings"))}
    cx.mapping("typeProperties.dataset", "typeProperties.datasetSettings", "dataset_inlined")
    if "logStorageSettings" in act.raw_properties:
        cx.dropped("typeProperties.logStorageSettings", "references a linked service; its Fabric structure is "
                   "undocumented", manual="Re-enable delete logging in the Fabric activity and bind its storage connection.")
    if ftype == "GetMetadata":
        unusual = [f for f in (act.raw_properties.get("fieldList") or [])
                   if isinstance(f, str) and f not in ("exists", "childItems", "itemName", "itemType", "size",
                                                       "lastModified", "created", "structure", "columnCount",
                                                       "contentMD5")]
        if unusual:
            cx.difference("metadata_field_unverified", f"Metadata field(s) {unusual} are not recognised.", "")
        cx.info("metadata_fields_documented", "Fabric documents the exists, structure and column-count fields; other "
                "fields are carried as written.", "Run once and check the output before depending on a field.")
        cx.output_notes.append("GetMetadata output fields depend on the connector.")
    name = cx.add(cx.envelope(act.name, ftype, tp))
    cx.cover(Op.read if ftype == "GetMetadata" else Op.delete, name)
    cx.strategy = ConversionStrategy.transformed
    cx.summary = f"{act.type} '{act.name}' -> Fabric {ftype} over inline dataset '{io.name}'."


register(ConversionRule(
    name="dataset_activity", adf_types=("GetMetadata", "Delete"), fabric_types=("GetMetadata", "Delete"),
    variants=("fieldList metadata", "store/format settings", "recursive delete", "delete logging"),
    strategy=ConversionStrategy.transformed, handler=convert_dataset_activity, runtime_disposition="replace",
    property_rules=("fieldList/storeSettings/formatSettings (GetMetadata) and recursive/maxConcurrentConnections/"
                    "enableLogging/storeSettings (Delete) carried",),
    io_rules=("typeProperties.dataset -> typeProperties.datasetSettings (inlined, connection bound)",),
    expression_rules=("converted like Copy; metadata output references unchanged",),
    dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",),
    unsupported=("Delete logStorageSettings (linked-service reference, undocumented Fabric structure)",),
    manual_configuration=("Fabric connection for the dataset's linked service",),
    validation_rules=("datasetSettings present",),
))


# --- Validation (composite) ------------------------------------------------------------------------------------


def _inner(value: Any) -> Optional[str]:
    """Expression text without the leading '@', or a literal rendered as text; None if not safely reusable."""
    if is_expression(value):
        t = value["value"]
        return t[1:] if t.startswith("@") and not t.startswith(("@@", "@{")) else None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return None


def convert_validation(cx: ActivityBuilder) -> None:
    act, raw = cx.act, cx.act.raw_properties
    io = act.inputs[0] if act.inputs else None
    ds = dataset_settings(cx, io, "typeProperties.dataset", "typeProperties.datasetSettings")
    if ds is None:
        return _fail(cx, "validation_dataset_unavailable", "its dataset could not be inlined.")

    q = lambda n: n.replace("'", "''")  # noqa: E731
    chk = cx.pc.alloc(f"{act.name}_check")
    waitn = cx.pc.alloc(f"{act.name}_wait")
    ifn = cx.pc.alloc(f"{act.name}_wait_if_not_ready")
    ref = f"activity('{q(chk)}').output"

    fields = ["exists"]
    conds = [f"equals({ref}.exists, true)"]
    min_size = raw.get("minimumSize")
    if min_size is not None:
        inner = _inner(min_size)
        if inner is None:
            cx.manual("validation_dynamic_minimum_size", "minimumSize is dynamic content that cannot be reused safely.",
                      "Rewrite the readiness check by hand.")
        else:
            fields.append("size")
            conds.append(f"greaterOrEquals({ref}.size, {inner})")
            cx.difference("validation_size_check",
                          "minimumSize is checked with GetMetadata 'size'; if the dataset does not exist yet, "
                          "requesting 'size' may fail the check instead of reporting exists=false (ADF Validation "
                          "keeps waiting).", "Test the not-yet-existing case, or check existence in a separate step first.")
    child = raw.get("childItems")
    if isinstance(child, bool):
        fields.append("childItems")
        conds.append(f"greater(length({ref}.childItems), 0)" if child else f"equals(length({ref}.childItems), 0)")
        cx.difference("validation_child_items", "childItems is evaluated from the GetMetadata childItems list.",
                      "Verify folder-emptiness behavior on a test folder.")
    ready = conds[0] if len(conds) == 1 else f"and({', '.join(conds)})"

    sleep = raw.get("sleep")
    sleep_v = cx.conv(sleep, "typeProperties.sleep", f"{waitn}.typeProperties.waitTimeInSeconds") if sleep is not None else 10
    if sleep is None:
        cx.mapping("typeProperties.sleep", f"{waitn}.typeProperties.waitTimeInSeconds", "default_made_explicit",
                   "ADF's documented default polling interval is 10 seconds")
    else:
        cx.mapping("typeProperties.sleep", f"{waitn}.typeProperties.waitTimeInSeconds", "passthrough")
    timeout = raw.get("timeout")
    if timeout is None:
        timeout_v: Any = "0.12:00:00"
        cx.mapping("typeProperties.timeout", "typeProperties.timeout", "default_made_explicit",
                   "ADF Validation times out after 12 hours by default; Fabric's Until defaults to 7 days, so the "
                   "ADF value is made explicit")
    else:
        timeout_v = cx.conv(timeout, "typeProperties.timeout", "typeProperties.timeout")
        cx.mapping("typeProperties.timeout", "typeProperties.timeout", "passthrough")

    check = cx.envelope(chk, "GetMetadata", {"datasetSettings": ds, "fieldList": fields}, primary=False)
    wait = cx.envelope(waitn, "Wait", {"waitTimeInSeconds": sleep_v}, primary=False)
    gate = cx.envelope(ifn, "IfCondition", {"expression": expr_obj(f"@not({ready})"), "ifTrueActivities": [wait]},
                       primary=False)
    gate["dependsOn"] = [{"activity": chk, "dependencyConditions": ["Succeeded"]}]
    until = cx.envelope(act.name, "Until", {"expression": expr_obj("@" + ready), "timeout": timeout_v,
                                            "activities": [check, gate]})
    name = cx.add(until)
    cx.add(check, "helper")
    cx.add(gate, "helper")
    cx.add(wait, "helper")
    cx.entry, cx.exit = [name], [name]
    cx.cover(Op.validate, name, chk, ifn, waitn)
    cx.strategy = ConversionStrategy.composite
    cx.alternative("Microsoft's guidance for the ADF Validation activity is GetMetadata + If Condition; this "
                   "conversion polls inside an Until loop to keep the wait-until-ready behavior.")
    cx.summary = (f"Validation '{act.name}' -> Until loop [GetMetadata '{chk}' + If-not-ready Wait '{waitn}'] "
                  f"polling every {sleep_v}s until the dataset is ready, timing out after {timeout_v}.")


register(ConversionRule(
    name="validation", adf_types=("Validation",), fabric_types=("Until", "GetMetadata", "IfCondition", "Wait"),
    variants=("exists", "minimumSize", "childItems"),
    strategy=ConversionStrategy.composite, handler=convert_validation, runtime_disposition="replace",
    property_rules=("timeout -> Until.timeout (default made explicit)", "sleep -> Wait.waitTimeInSeconds",
                    "minimumSize / childItems -> readiness expression over GetMetadata output"),
    io_rules=("dataset -> GetMetadata.datasetSettings",),
    expression_rules=("generated expressions reference only generated helper activities",),
    dependency_rules=("the Until loop keeps the original name and carries all dependsOn / dependents",),
    parameter_variable_rules=("unchanged",),
    unsupported=("exact ADF Validation retry semantics",),
    manual_configuration=("Fabric connection for the dataset",),
    validation_rules=("helper activities named uniquely", "Until contains only same-scope dependencies"),
))
