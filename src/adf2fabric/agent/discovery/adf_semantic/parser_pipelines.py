"""The pipeline parser for ADF Semantic Discovery.

Every pipeline — whether Intake obtained it from a GitHub repository or a live Azure
Data Factory — reaches Discovery as the same normalized JSON and is parsed here.
There is no per-source path and no inference engine: every field of the output is a
deterministic function of the pipeline JSON (plus, in :func:`resolve_pipelines`, the
other discovered artifacts).

Stages (``parse_pipeline`` runs 1-6, 8-10; ``resolve_pipelines`` runs 7):

    1  JSON loading + schema validation          (this module)
    2  pipeline inventory + identity resolution  (this module)
    3  recursive activity discovery              (this module)
    4  activity type identification              (activity_registry)
    5  property + expression extraction          (this module, expressions)
    6  dependency + control-flow analysis        (pipeline_analysis)
    7  dataset / linked-service resolution       (pipeline_analysis.resolve_pipelines)
    8  semantic operation classification         (activity_semantics)
    9  intermediate representation               (project.discovery.AdfActivity)
    10 unsupported-feature + ambiguity reporting (AdfParseIssue on activities/pipelines)

The parser DESCRIBES the pipeline. It contains no Fabric knowledge — a separate
converter consumes the representation. Original property values and expressions are
preserved next to their interpretation; unknown properties are kept, never dropped;
unknown activity types are reported, never skipped; secret-looking values are masked.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

from adf2fabric.agent.discovery.adf_semantic import activity_semantics, inventory, pipeline_analysis
from adf2fabric.agent.discovery.adf_semantic.activity_registry import (
    NESTED_SLOTS,
    REQUIRES_LINKED_SERVICE,
    TOP_LEVEL_KEYS,
    ActivitySpec,
    get_spec,
    snake_case,
)
from adf2fabric.agent.discovery.adf_semantic.expressions import analyze_expression
from adf2fabric.agent.discovery.adf_semantic.parser import (
    _SECRET_VALUE_MARKERS,
    SECRET_KEY_HINTS,
    _folder_name,
    _parse_param_map,
    _parse_variables,
)
from adf2fabric.project.discovery import (
    AdfActivity,
    AdfActivityReference,
    AdfConfidence,
    AdfDataset,
    AdfDependency,
    AdfIO,
    AdfIssueSeverity,
    AdfLinkedService,
    AdfOperationKind,
    AdfParseIssue,
    AdfPipeline,
    AdfPolicy,
    AdfSourceLocation,
)

REDACTED = "[REDACTED]"
_MAX_DEPTH = 30
_CONDITIONS = {"succeeded": "Succeeded", "failed": "Failed", "skipped": "Skipped", "completed": "Completed"}
_QUERY_KEYS = ("sqlReaderQuery", "query", "oracleReaderQuery", "sqlReaderStoredProcedureName")
_PROC_KEYS = ("sqlReaderStoredProcedureName", "sqlWriterStoredProcedureName", "storedProcedureName")
_REF_KINDS = {
    "pipeline": "pipeline", "dataset": "dataset", "linkedservice": "linkedService",
    "dataflow": "dataflow", "integrationruntime": "integrationRuntime", "credential": "credential",
}
_STATEMENT_RE = re.compile(
    r"(?im)^\s*(select|insert|update|delete|merge|exec(?:ute)?|create|alter|drop|truncate|declare|with)\b")
_WRITE_STATEMENTS = {"INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE"}
_UNWRAPPED_RE = re.compile(r"^@[A-Za-z]+\(|@\{")

_ROLE_RULES: list[tuple[re.Pattern, Optional[str]]] = [(re.compile(p, re.I), r) for p, r in [
    (r"^typeProperties\.items$", "loop_collection"),
    (r"^typeProperties\.(expression|condition)$", None),  # decided by activity type
    (r"^typeProperties\.on$", "switch_selector"),
    (r"^typeProperties\.cases\[\d+\]\.value$", "switch_case_value"),
    (r"^typeProperties\.waitTimeInSeconds$", "wait_duration"),
    (r"^typeProperties\.(message|errorCode)$", "failure_message"),
    (r"^typeProperties\.value$", "variable_value"),
    (r"^typeProperties\.(url|relativeUrl)$", "request_url"),
    (r"^typeProperties\.(headers|body)", "request_payload"),
    (r"^typeProperties\.storedProcedureName$", "stored_procedure_name"),
    (r"^typeProperties\.storedProcedureParameters", "sql_parameter"),
    (r"^typeProperties\.scripts\[\d+\]\.text$", "script_body"),
    (r"^typeProperties\.scripts\[\d+\]\.parameters", "sql_parameter"),
    (r"^typeProperties\.source\..*(query|sql)", "source_query"),
    (r"^typeProperties\.source\..*(folderpath|filename|path|prefix|wildcard)", "source_path"),
    (r"^typeProperties\.source\.", "source_setting"),
    (r"^typeProperties\.sink\..*(precopyscript|storedprocedure)", "sink_script"),
    (r"^typeProperties\.sink\..*(folderpath|filename|path|prefix)", "destination_path"),
    (r"^typeProperties\.sink\.", "sink_setting"),
    (r"^typeProperties\.dataset\.parameters", "source_selection"),
    (r"^inputs\[\d+\]\.parameters", "source_selection"),
    (r"^outputs\[\d+\]\.parameters", "destination_selection"),
    (r"^typeProperties\.translator", "column_mapping"),
    (r"^typeProperties\.(notebookPath|baseParameters)", "notebook_argument"),
    (r"^typeProperties\.dataflow\.parameters", "dataflow_argument"),
    (r"^typeProperties\.parameters", "argument"),
    (r"^policy\.", "execution_policy"),
]]


# --- secret-safe preservation --------------------------------------------------------------


def unsafe_text(value: str) -> bool:
    """True if ``value`` carries a secret-value marker (the connection-string SHAPE rule
    is deliberately not applied: SQL scripts legitimately contain ``;`` and ``=``)."""
    low = value.lower()
    return any(m in low for m in _SECRET_VALUE_MARKERS)


class _Sanitizer:
    """Copies JSON while masking secret VALUES. Keys and structure are preserved."""

    def __init__(self) -> None:
        self.redacted: list[str] = []

    def clean(self, node: Any, path: str = "", depth: int = 0) -> Any:
        if depth > _MAX_DEPTH * 2:
            return node
        if isinstance(node, dict):
            secure = node.get("type") == "SecureString"
            out: dict = {}
            for k, v in node.items():
                sub = f"{path}.{k}" if path else str(k)
                if (secure and k == "value" and isinstance(v, str)) or (
                        isinstance(k, str) and k.lower() in SECRET_KEY_HINTS and isinstance(v, str)):
                    if not _is_mask(v):
                        self.redacted.append(sub)
                    out[k] = v if _is_mask(v) else REDACTED
                else:
                    out[k] = self.clean(v, sub, depth + 1)
            return out
        if isinstance(node, list):
            return [self.clean(v, f"{path}[{i}]", depth + 1) for i, v in enumerate(node)]
        if isinstance(node, str) and unsafe_text(node):
            self.redacted.append(path)
            return REDACTED
        return node


def _is_mask(v: str) -> bool:
    return v == REDACTED or (bool(v) and set(v) <= {"*"})


# --- small helpers -----------------------------------------------------------------------------


def _is_expr(v: Any) -> bool:
    return isinstance(v, dict) and v.get("type") == "Expression" and isinstance(v.get("value"), str)


def _text(v: Any) -> tuple[Optional[str], bool]:
    """(authored text, is_expression) of a scalar/Expression value."""
    if _is_expr(v):
        return v["value"], True
    if isinstance(v, bool):
        return str(v).lower(), False
    if isinstance(v, (str, int, float)):
        return str(v), False
    return None, False


def _get(node: Any, path: str) -> tuple[bool, Any]:
    cur = node
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return False, None
        cur = cur[part]
    return True, cur


def _ref_name(node: Any) -> Optional[str]:
    if isinstance(node, dict) and isinstance(node.get("referenceName"), str) and node["referenceName"]:
        return node["referenceName"]
    return None


def _kind_of_ref(ref_type: Any) -> Optional[str]:
    if not isinstance(ref_type, str) or not ref_type.endswith("Reference"):
        return None
    base = ref_type[: -len("Reference")]
    return _REF_KINDS.get(base.lower(), base[:1].lower() + base[1:])


def _strip_settings(t: Any) -> Optional[str]:
    return re.sub(r"(Read|Write)Settings$", "", t) if isinstance(t, str) and t else None


class _Ctx:
    """Per-pipeline parse state."""

    def __init__(self, name: str, path: str, params: set[str], variables: dict[str, Optional[str]]):
        self.name = name
        self.path = path
        self.params = params  # lowercase names
        self.variables = variables  # lowercase -> declared type
        self.issues: list[AdfParseIssue] = []
        self.ids: dict[str, int] = {}
        self.flat: list[AdfActivity] = []

    def issue(self, code: str, message: str, path: Optional[str] = None,
              severity: AdfIssueSeverity = AdfIssueSeverity.warning) -> None:
        self.issues.append(AdfParseIssue(code=code, severity=severity, message=message, property_path=path))


# --- stages 1-3: pipeline ---------------------------------------------------------------------------


def parse_pipeline(data: Any, path: str) -> AdfPipeline:
    """Parse one pipeline definition into the semantic representation.

    Accepts the ADF Git layout and an ARM resource item (``id``/``type``/``etag`` are
    ignored). Never raises for malformed content: problems become ``issues``.
    """
    issues_pre: list[AdfParseIssue] = []
    if not isinstance(data, dict):
        return AdfPipeline(
            name=Path(path).stem, path=path,
            issues=[AdfParseIssue(code="invalid_pipeline_json", severity=AdfIssueSeverity.error,
                                  message="Top-level JSON is not an object.")])

    # Stage 2 - identity.
    name = data.get("name") if isinstance(data.get("name"), str) and data.get("name") else ""
    if not name:
        name = Path(path).stem
        issues_pre.append(AdfParseIssue(
            code="missing_pipeline_name", message=f"No 'name'; using the file name '{name}'.",
            property_path="name"))
    elif Path(path).stem.lower() != name.lower():
        issues_pre.append(AdfParseIssue(
            code="name_file_mismatch", severity=AdfIssueSeverity.info,
            message=f"Pipeline name '{name}' differs from file name '{Path(path).stem}'.",
            property_path="name"))
    rtype = data.get("type")
    if isinstance(rtype, str) and not rtype.lower().endswith("/pipelines"):
        issues_pre.append(AdfParseIssue(
            code="unexpected_resource_type", message=f"Resource type '{rtype}' is not a pipeline.",
            property_path="type"))

    # Stage 1 - schema.
    props = data.get("properties")
    if not isinstance(props, dict):
        issues_pre.append(AdfParseIssue(
            code="missing_properties", severity=AdfIssueSeverity.error,
            message="No 'properties' object; the pipeline has no readable definition.",
            property_path="properties"))
        props = {}

    parameters = _parse_param_map(props.get("parameters"))
    variables = _parse_variables(props.get("variables"))
    ctx = _Ctx(name, path, {p.name.lower() for p in parameters},
               {v.name.lower(): v.type for v in variables})
    ctx.issues.extend(issues_pre)

    san = _Sanitizer()
    raw_acts = props.get("activities")
    if "activities" not in props:
        ctx.issue("activities_missing", "The pipeline defines no 'activities' property.",
                  "properties.activities")
        raw_acts = []
    elif not isinstance(raw_acts, list):
        ctx.issue("invalid_activities", "'activities' is not a list.", "properties.activities",
                  AdfIssueSeverity.error)
        raw_acts = []
    elif not raw_acts:
        ctx.issue("activities_empty", "The pipeline has an empty 'activities' list.",
                  "properties.activities", AdfIssueSeverity.info)

    # Stage 3 - recursive activity discovery.
    activities: list[AdfActivity] = []
    for i, node in enumerate(raw_acts):
        act = _parse_activity(node, ctx, None, "activities", f"properties.activities[{i}]", i, [], 0)
        if act is not None:
            activities.append(act)

    known_props = {"activities", "parameters", "variables", "folder", "annotations", "description",
                   "concurrency", "policy", "runDimensions", "lastPublishTime", "isRunnable"}
    unknown = {k: san.clean(v, k) for k, v in props.items() if k not in known_props}
    annotations = [a for a in (props.get("annotations") or []) if isinstance(a, str)] \
        if isinstance(props.get("annotations"), list) else []
    concurrency = props.get("concurrency") if isinstance(props.get("concurrency"), int) \
        and not isinstance(props.get("concurrency"), bool) else None
    raw_settings = {k: san.clean(props[k], k) for k in ("policy", "runDimensions") if k in props}

    pipeline = AdfPipeline(
        name=name, path=path, folder=_folder_name(props), parameters=parameters, variables=variables,
        activities=activities, activity_count=len(activities),
        pipeline_dependencies=sorted(_collect_pipeline_refs(activities)),
        description=props.get("description") if isinstance(props.get("description"), str) else None,
        annotations=annotations, concurrency=concurrency,
        unknown_properties=unknown, raw_properties=raw_settings,
    )

    # Stages 6, 8-10: control flow, classification, summaries, reporting.
    pipeline_analysis.analyze_pipeline(pipeline, ctx)
    activity_semantics.finalize_pipeline(pipeline)

    # Reconcile what was parsed with an independent scan of the raw JSON: a mismatch means a component was missed.
    pipeline.inventory = inventory.reconcile(pipeline.activities, data)
    if not pipeline.inventory.matched:
        pipeline_analysis.add_issue(pipeline.issues, AdfParseIssue(
            code="inventory_mismatch", severity=AdfIssueSeverity.error,
            message=(f"The source holds {pipeline.inventory.source_activity_count} activit(ies) but "
                     f"{pipeline.inventory.parsed_activity_count} were recorded. Not recorded: "
                     f"{', '.join(pipeline.inventory.missing_pointers[:5]) or 'none'}"
                     f"{' ...' if len(pipeline.inventory.missing_pointers) > 5 else ''}. "
                     f"Recorded but not in the source scan: "
                     f"{', '.join(pipeline.inventory.unexpected_pointers[:5]) or 'none'}.")))
    if pipeline.inventory.suspect_pointers:
        shown = ", ".join(pipeline.inventory.suspect_pointers[:5])
        more = " ..." if len(pipeline.inventory.suspect_pointers) > 5 else ""
        pipeline_analysis.add_issue(pipeline.issues, AdfParseIssue(
            code="possible_unparsed_activities", severity=AdfIssueSeverity.info,
            message=(f"{len(pipeline.inventory.suspect_pointers)} object(s) shaped like activities sit outside the "
                     f"documented activity lists ({shown}{more}). Free-form content (a request body, a default value) "
                     "is expected here; an unfamiliar container would be too, and is preserved as unknown properties.")))
    return pipeline


def parse_pipeline_file(source_root: Path, relative_path: str) -> AdfPipeline:
    """Read ``source_root/relative_path`` (never modified) and parse it."""
    try:
        with (Path(source_root) / relative_path).open(encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return AdfPipeline(
            name=Path(relative_path).stem, path=relative_path,
            issues=[AdfParseIssue(code="invalid_json", severity=AdfIssueSeverity.error,
                                  message=f"Not valid JSON ({type(exc).__name__}).")])
    return parse_pipeline(data, relative_path)


def resolve_pipelines(
    pipelines: list[AdfPipeline],
    datasets: list[AdfDataset],
    linked_services: list[AdfLinkedService],
    dataflows: "list | tuple" = (),
) -> None:
    """Stage 7: resolve dataset / linked-service / child-pipeline references in place.

    Fills each endpoint with the dataset's type, format, location and linked-service
    connection type, flags references that do not resolve, checks dataset and child
    pipeline arguments, detects cyclic pipeline calls and recurring pipeline shapes,
    then regenerates the semantic summaries with the richer context.
    """
    pipeline_analysis.resolve_pipelines(pipelines, datasets, linked_services, dataflows)
    for p in pipelines:
        activity_semantics.finalize_pipeline(p)


def _collect_pipeline_refs(activities: list[AdfActivity]) -> set[str]:
    refs: set[str] = set()
    for a in activities:
        refs.update(a.pipeline_refs)
        refs.update(_collect_pipeline_refs(a.activities))
    return refs


# --- stages 3-5: one activity ---------------------------------------------------------------------------


def _parse_activity(node: Any, ctx: _Ctx, parent: Optional[AdfActivity], branch: str,
                    pointer: str, index: int, nesting: list[str], depth: int) -> Optional[AdfActivity]:
    if not isinstance(node, dict):
        ctx.issue("invalid_activity_node", "An entry in an activities list is not an object; skipped.",
                  pointer, AdfIssueSeverity.error)
        return None

    san = _Sanitizer()
    name = node.get("name") if isinstance(node.get("name"), str) else ""
    id_name = name or f"<unnamed-{index}>"
    base_id = "/".join([ctx.name, *nesting, id_name])
    n = ctx.ids.get(base_id, 0) + 1
    ctx.ids[base_id] = n
    activity_id = base_id if n == 1 else f"{base_id}#{n}"

    type_raw = node.get("type") if isinstance(node.get("type"), str) and node.get("type") else None
    spec = get_spec(type_raw)
    tp_raw = node.get("typeProperties")
    tp = tp_raw if isinstance(tp_raw, dict) else {}

    act = AdfActivity(
        name=name, type=type_raw, activity_id=activity_id,
        parent_activity_id=parent.activity_id if parent else None,
        nesting_path=list(nesting), branch=branch if parent else None,
        source_location=AdfSourceLocation(file=ctx.path, pointer=pointer),
        description=node.get("description") if isinstance(node.get("description"), str) else None,
    )
    if not name:
        _warn(act, "missing_activity_name", "Activity has no 'name'.", "name", AdfIssueSeverity.error)
    if tp_raw is not None and not isinstance(tp_raw, dict):
        _warn(act, "invalid_type_properties", "'typeProperties' is not an object.", "typeProperties",
              AdfIssueSeverity.error)
    if n > 1:
        _warn(act, "duplicate_activity_id", f"Another activity at this position is also named '{name}'.")
    ctx.flat.append(act)

    # Stage 4 - type identification.
    if type_raw is None:
        _warn(act, "missing_activity_type", "Activity has no 'type'.", "type", AdfIssueSeverity.error)
    elif spec is None:
        _warn(act, "unknown_activity_type",
              f"Activity type '{type_raw}' is not in the registry; described structurally only.", "type")
    if spec is not None:
        act.semantic_category = spec.category
        act.operations = list(spec.operations)
        act.operation = spec.operations[0]
        act.confidence = AdfConfidence.high
    else:
        act.confidence = AdfConfidence.unsupported

    _parse_state(act, node)
    act.depends_on = _parse_dependencies(act, node)
    act.policy = _parse_policy(act, node, san)
    _legacy_refs(act, node, tp)

    # Stage 5 - properties, references, expressions.
    _scan(node, act, ctx, san)
    _check_properties(act, node, spec, tp, san)
    if spec is not None and spec.handler:
        _HANDLERS[spec.handler](act, node, tp, ctx, san)
    elif spec is not None:
        _generic_endpoints(act, node, tp, san)
    _generic_config(act, spec, tp, san)
    _derive_usage(act, ctx)

    for path_ in san.redacted:
        _warn(act, "value_redacted", "A secret-looking value was masked in this record.",
              path_, AdfIssueSeverity.info)

    # Stage 3 - nested activities.
    if depth < _MAX_DEPTH:
        child_nesting = [*nesting, id_name]
        for key, label in NESTED_SLOTS.items():
            if key in tp:
                _parse_children(tp[key], act, ctx, label, f"{pointer}.typeProperties.{key}", child_nesting, depth)
        cases = tp.get("cases")
        if isinstance(cases, list):
            for ci, case in enumerate(cases):
                if not isinstance(case, dict):
                    _warn(act, "invalid_case", "A Switch case is not an object.",
                          f"typeProperties.cases[{ci}]", AdfIssueSeverity.error)
                    continue
                value, _ = _text(case.get("value"))
                _parse_children(case.get("activities"), act, ctx, f"case:{value}",
                                f"{pointer}.typeProperties.cases[{ci}].activities", child_nesting, depth)
    else:
        _warn(act, "max_depth_exceeded", f"Nesting deeper than {_MAX_DEPTH}; children not parsed.", None,
              AdfIssueSeverity.error)
    return act


def _parse_children(items: Any, parent: AdfActivity, ctx: _Ctx, label: str, pointer: str,
                    nesting: list[str], depth: int) -> None:
    if items is None:
        return
    if not isinstance(items, list):
        _warn(parent, "invalid_activities", f"Nested activities for '{label}' are not a list.", pointer,
              AdfIssueSeverity.error)
        return
    for i, child in enumerate(items):
        act = _parse_activity(child, ctx, parent, label, f"{pointer}[{i}]", i, nesting, depth + 1)
        if act is not None:
            parent.activities.append(act)


def _warn(act: AdfActivity, code: str, message: str, path: Optional[str] = None,
          severity: AdfIssueSeverity = AdfIssueSeverity.warning) -> None:
    act.warnings.append(AdfParseIssue(code=code, severity=severity, message=message, property_path=path))


def _list_of(act: AdfActivity, container: dict, key: str, path: str) -> list:
    """``container[key]`` as a list. Absent is empty; any other shape is reported (never iterated, never crashes)."""
    value = container.get(key)
    if value is None or isinstance(value, list):
        return value or []
    _warn(act, f"invalid_{snake_case(key)}",
          f"'{key}' should be a list but is a {type(value).__name__}; it is preserved but not read.", path,
          AdfIssueSeverity.error)
    return []


def _parse_state(act: AdfActivity, node: dict) -> None:
    state = node.get("state")
    if isinstance(state, str):
        act.state = state
        if state.lower() == "inactive":
            mark = node.get("onInactiveMarkAs")
            act.configuration["on_inactive_mark_as"] = mark if isinstance(mark, str) else "Succeeded"
            _warn(act, "activity_inactive",
                  "Activity is deactivated: it does not run and is marked "
                  f"{act.configuration['on_inactive_mark_as']}.", "state", AdfIssueSeverity.info)


def _parse_dependencies(act: AdfActivity, node: dict) -> list[AdfDependency]:
    raw = node.get("dependsOn")
    if raw is None:
        return []
    if not isinstance(raw, list):
        _warn(act, "invalid_depends_on", "'dependsOn' is not a list.", "dependsOn", AdfIssueSeverity.error)
        return []
    out: list[AdfDependency] = []
    for i, dep in enumerate(raw):
        if not isinstance(dep, dict) or not isinstance(dep.get("activity"), str) or not dep["activity"]:
            _warn(act, "invalid_dependency", "A dependency has no activity name.", f"dependsOn[{i}]",
                  AdfIssueSeverity.error)
            continue
        conds, unknown = [], []
        for c in _list_of(act, dep, "dependencyConditions", f"dependsOn[{i}].dependencyConditions"):
            norm = _CONDITIONS.get(str(c).lower())
            (conds if norm else unknown).append(norm or str(c))
        if "dependencyConditions" not in dep or not (dep.get("dependencyConditions")):
            _warn(act, "dependency_without_condition",
                  f"Dependency on '{dep['activity']}' lists no condition.", f"dependsOn[{i}]")
        if unknown:
            _warn(act, "unknown_dependency_condition",
                  f"Unrecognised dependency condition(s) {unknown} on '{dep['activity']}'.",
                  f"dependsOn[{i}].dependencyConditions")
        out.append(AdfDependency(activity=dep["activity"], conditions=sorted(set(conds)),
                                 unknown_conditions=unknown))
    return out


def _parse_policy(act: AdfActivity, node: dict, san: _Sanitizer) -> Optional[AdfPolicy]:
    raw = node.get("policy")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        _warn(act, "invalid_policy", "'policy' is not an object.", "policy", AdfIssueSeverity.error)
        return None

    def scalar(key: str, cast):
        v = raw.get(key)
        if v is None or _is_expr(v):
            return None
        try:
            return cast(v)
        except (TypeError, ValueError):
            _warn(act, "invalid_policy_value", f"policy.{key} has an unexpected value.", f"policy.{key}")
            return None

    def boolean(v):
        if isinstance(v, bool):
            return v
        raise ValueError

    return AdfPolicy(
        timeout=scalar("timeout", str),
        retry=scalar("retry", int),
        retry_interval_in_seconds=scalar("retryIntervalInSeconds", int),
        secure_input=scalar("secureInput", boolean),
        secure_output=scalar("secureOutput", boolean),
        is_dynamic=any(_is_expr(v) for v in raw.values()),
        raw=san.clean(raw, "policy"),
    )


def _legacy_refs(act: AdfActivity, node: dict, tp: dict) -> None:
    """The original structural reference fields (unchanged semantics)."""
    datasets: list[str] = []
    for key in ("inputs", "outputs"):
        for ref in _list_of(act, node, key, key):
            n = _ref_name(ref)
            if n:
                datasets.append(n)
    ds = _ref_name(tp.get("dataset"))
    if ds:
        datasets.append(ds)
    act.dataset_refs = sorted(set(datasets))
    ls = _ref_name(node.get("linkedServiceName"))
    act.linked_service_refs = [ls] if ls else []
    ls_node = node.get("linkedServiceName")
    ls_args = ls_node.get("parameters") if isinstance(ls_node, dict) else None
    act.linked_service_arguments = _Sanitizer().clean(ls_args) if isinstance(ls_args, dict) else {}
    pl = _ref_name(tp.get("pipeline"))
    act.pipeline_refs = [pl] if pl else []
    source = tp.get("source") if isinstance(tp.get("source"), dict) else {}
    sink = tp.get("sink") if isinstance(tp.get("sink"), dict) else {}
    act.source_type = source.get("type") if isinstance(source.get("type"), str) else None
    act.sink_type = sink.get("type") if isinstance(sink.get("type"), str) else None
    procs = []
    for sp in (tp.get("storedProcedureName"), source.get("sqlReaderStoredProcedureName")):
        if isinstance(sp, str) and sp:
            procs.append(sp)
    act.stored_procedures = sorted(set(procs))


def _role_for(act: AdfActivity, path: str) -> str:
    atype = (act.type or "").lower()
    for rx, role in _ROLE_RULES:
        if rx.search(path):
            if role is not None:
                if role == "argument" and atype != "executepipeline":
                    return "general"
                return role
            return {"ifcondition": "branch_condition", "until": "loop_condition",
                    "filter": "filter_condition"}.get(atype, "condition")
    return "general"


def _scan(node: dict, act: AdfActivity, ctx: _Ctx, san: _Sanitizer) -> None:
    """One pass over the activity (nested activities excluded): expressions + references."""

    def walk(obj: Any, path: str, depth: int) -> None:
        if depth > _MAX_DEPTH * 2:
            return
        if isinstance(obj, dict):
            if _is_expr(obj):
                _add_expression(act, obj["value"], path, san)
                return
            kind = _kind_of_ref(obj.get("type"))
            rname = obj.get("referenceName")
            if kind and isinstance(rname, str) and rname:
                act.references.append(AdfActivityReference(kind=kind, name=rname, role=path))
            for k, v in obj.items():
                if path == "typeProperties" and k in NESTED_SLOTS:
                    continue
                if k == "activities" and re.search(r"^typeProperties\.cases\[\d+\]$", path):
                    continue
                if k == "dependsOn" and path == "":
                    continue
                walk(v, f"{path}.{k}" if path else str(k), depth + 1)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                walk(v, f"{path}[{i}]", depth + 1)
        elif isinstance(obj, str) and _UNWRAPPED_RE.search(obj) and not path.endswith(".text"):
            _warn(act, "unwrapped_expression_like_string",
                  "A plain string looks like dynamic content but is not wrapped as an Expression; "
                  "it is treated as a literal.", path)

    walk({k: v for k, v in node.items() if k != "name"}, "", 0)
    act.references.sort(key=lambda r: (r.kind, r.name, r.role))


def _add_expression(act: AdfActivity, raw: str, path: str, san: _Sanitizer) -> None:
    expr = analyze_expression(raw, path, _role_for(act, path))
    if unsafe_text(raw):
        san.redacted.append(path)
        expr = expr.model_copy(update={"raw": REDACTED, "literal_text": ""})
    act.expressions.append(expr)
    if expr.parse_status == "unparsed":
        _warn(act, "expression_unparsed", f"Expression could not be parsed: {expr.error}", path)
    if expr.unknown_functions:
        _warn(act, "unknown_expression_function",
              f"Function(s) not in the documented set: {', '.join(expr.unknown_functions)}; "
              "the expression is preserved verbatim.", path)
    for ref in expr.references:
        if ref.kind in ("variable", "activity_output") and ref.name is None:
            _warn(act, "dynamic_reference_name",
                  f"A {ref.kind.replace('_', ' ')} reference uses a computed name ({ref.raw}); "
                  "its target cannot be resolved statically.", path)


def _derive_usage(act: AdfActivity, ctx: _Ctx) -> None:
    params, gparams, reads = set(), set(), set()
    for e in act.expressions:
        for r in e.references:
            if r.kind == "pipeline_parameter" and r.name:
                params.add(r.name)
            elif r.kind == "global_parameter" and r.name:
                gparams.add(r.name)
            elif r.kind == "variable" and r.name:
                reads.add(r.name)
    act.parameters = sorted(params)
    act.global_parameters = sorted(gparams)
    act.variables_read = sorted(reads)


# --- property completeness ---------------------------------------------------------------------------------


def _check_properties(act: AdfActivity, node: dict, spec: Optional[ActivitySpec], tp: dict,
                      san: _Sanitizer) -> None:
    nested_keys = set(NESTED_SLOTS)
    cleaned = {k: v for k, v in tp.items() if k not in nested_keys}
    if isinstance(cleaned.get("cases"), list):
        cleaned["cases"] = [
            {k: v for k, v in c.items() if k != "activities"} if isinstance(c, dict) else c
            for c in cleaned["cases"]]
    act.raw_properties = san.clean(cleaned, "typeProperties")

    allowed_top = TOP_LEVEL_KEYS | (set(spec.top_level) if spec is not None else set())
    extra = {f"activity.{k}": san.clean(v, k) for k, v in node.items() if k not in allowed_top}
    if spec is None:
        act.unknown_properties = {**{k: v for k, v in act.raw_properties.items()}, **extra}
    else:
        known = set(spec.known) | {alt.rsplit(".", 1)[-1] for _, alt in spec.aliases}
        act.unknown_properties = {
            **{k: v for k, v in act.raw_properties.items() if k not in known}, **extra}
        if act.unknown_properties:
            _warn(act, "unknown_properties",
                  f"Preserved but not interpreted: {', '.join(sorted(act.unknown_properties))}.")

    if spec is None:
        return
    required = list(spec.required)
    if spec.type.lower() in REQUIRES_LINKED_SERVICE:
        required.append("linkedServiceName")
    alias_of = dict(spec.aliases)
    for path in required:
        found, value = _get(node, path)
        if not found and path in alias_of:
            found, value = _get(node, alias_of[path])
            if found:
                _warn(act, "property_alias_used",
                      f"Written as '{alias_of[path].rsplit('.', 1)[-1]}'; the documented name is "
                      f"'{path.rsplit('.', 1)[-1]}'. Both are read.", alias_of[path], AdfIssueSeverity.info)
        if not found:
            act.missing_properties.append(path)
        elif value in (None, "", [], {}) or (_is_expr(value) and not value["value"].strip()):
            act.empty_properties.append(path)
    if act.missing_properties:
        _warn(act, "missing_required_properties",
              f"Required propert{'y' if len(act.missing_properties) == 1 else 'ies'} absent: "
              f"{', '.join(act.missing_properties)}.")
    if act.empty_properties:
        _warn(act, "empty_required_properties",
              f"Present but explicitly empty: {', '.join(act.empty_properties)}.")


def _generic_config(act: AdfActivity, spec: Optional[ActivitySpec], tp: dict, san: _Sanitizer) -> None:
    """Scalar known settings (and expression-valued ones) under snake_case names."""
    if spec is None:
        return
    for key in spec.known:
        if key not in tp or key in NESTED_SLOTS:
            continue
        v = tp[key]
        name = snake_case(key)
        if name in act.configuration:
            continue
        if _is_expr(v):
            act.configuration[name] = {"expression": REDACTED if unsafe_text(v["value"]) else v["value"]}
        elif isinstance(v, (str, int, float, bool)) or v is None:
            act.configuration[name] = san.clean(v, key)


# --- endpoints ---------------------------------------------------------------------------------------------


def _dataset_io(ref: Any, role: str, san: _Sanitizer, path: str) -> Optional[AdfIO]:
    name = _ref_name(ref)
    if not name:
        return None
    params = ref.get("parameters") if isinstance(ref.get("parameters"), dict) else {}
    return AdfIO(role=role, kind="dataset", name=name, parameters=san.clean(params, path + ".parameters"))


def _details(settings: Any, io: AdfIO, san: _Sanitizer, path: str) -> None:
    """Fold a source/sink settings object into an endpoint (connector, store, format, location)."""
    if not isinstance(settings, dict):
        return
    io.connector_type = settings.get("type") if isinstance(settings.get("type"), str) else io.connector_type
    store = settings.get("storeSettings")
    fmt = settings.get("formatSettings")
    if isinstance(store, dict):
        io.store = _strip_settings(store.get("type"))
    if isinstance(fmt, dict):
        io.format = _strip_settings(fmt.get("type"))
    for key in _QUERY_KEYS:
        if key in settings and key != "sqlReaderStoredProcedureName":
            text, is_expr = _text(settings[key])
            if text is not None:
                io.location["query"] = REDACTED if unsafe_text(text) else text
                io.location["query_is_expression"] = is_expr
                break
    for key in _PROC_KEYS:
        if isinstance(settings.get(key), (str, dict)):
            text, is_expr = _text(settings[key])
            if text:
                io.location["stored_procedure"] = text
                io.location["stored_procedure_is_expression"] = is_expr
                break
    for key, name in (("preCopyScript", "pre_copy_script"), ("writeBehavior", "write_behavior"),
                      ("tableOption", "table_option"), ("partitionOption", "partition_option"),
                      ("copyBehavior", "copy_behavior")):
        src = settings if key in settings else (store if isinstance(store, dict) and key in store else {})
        if key in src:
            text, is_expr = _text(src[key])
            if text is not None:
                io.location[name] = REDACTED if unsafe_text(text) else text
    io.settings = san.clean(settings, path)


def _generic_endpoints(act: AdfActivity, node: dict, tp: dict, san: _Sanitizer) -> None:
    for i, ref in enumerate(_list_of(act, node, "inputs", "inputs")):
        io = _dataset_io(ref, "input", san, f"inputs[{i}]")
        if io:
            act.inputs.append(io)
    for i, ref in enumerate(_list_of(act, node, "outputs", "outputs")):
        io = _dataset_io(ref, "output", san, f"outputs[{i}]")
        if io:
            act.outputs.append(io)


# --- handlers (activity-specific interpretation; all produce DESCRIPTIONS only) ----------------------------------


def _h_copy(act, node, tp, ctx, san):
    _generic_endpoints(act, node, tp, san)
    for ios, key, role in ((act.inputs, "source", "source"), (act.outputs, "sink", "sink")):
        settings = tp.get(key)
        if not ios and isinstance(settings, dict):
            ios.append(AdfIO(role=role, kind="inline"))
        for io in ios:
            io.role = role
        if ios:
            _details(settings, ios[0], san, f"typeProperties.{key}")
    if len(act.inputs) > 1 or len(act.outputs) > 1:
        _warn(act, "multiple_copy_endpoints", "More than one input/output dataset on a Copy activity.")

    tr = tp.get("translator")
    if _is_expr(tr):
        act.configuration["translator"] = {"dynamic": True, "expression": tr["value"]}
        _warn(act, "dynamic_mapping", "Column mapping is a dynamic expression; it cannot be resolved "
              "statically.", "typeProperties.translator")
    elif isinstance(tr, dict):
        mappings = []
        for m in tr.get("mappings") or []:
            if isinstance(m, dict):
                s, k = m.get("source") or {}, m.get("sink") or {}
                mappings.append({
                    "source": s.get("name") or s.get("path") or (f"#{s['ordinal']}" if "ordinal" in s else None),
                    "sink": k.get("name") or k.get("path") or (f"#{k['ordinal']}" if "ordinal" in k else None),
                    "source_type": s.get("type"), "sink_type": k.get("type")})
        act.configuration["translator"] = {
            "type": tr.get("type"), "mapping_count": len(mappings), "mappings": san.clean(mappings),
            "type_conversion": tr.get("typeConversion"),
            "collection_reference": tr.get("collectionReference")}
    elif "translator" not in tp:
        act.configuration["translator"] = {"auto_mapping": True}

    if tp.get("enableStaging") is True or _is_expr(tp.get("enableStaging")):
        staging = tp.get("stagingSettings") if isinstance(tp.get("stagingSettings"), dict) else {}
        act.configuration["staging"] = {
            "linked_service": _ref_name(staging.get("linkedServiceName")), "path": staging.get("path")}


def _h_lookup(act, node, tp, ctx, san):
    io = _dataset_io(tp.get("dataset"), "source", san, "typeProperties.dataset")
    settings = tp.get("source")
    if io is None and isinstance(settings, dict):
        io = AdfIO(role="source", kind="inline")
    if io is not None:
        _details(settings, io, san, "typeProperties.source")
        act.inputs.append(io)
    first = tp.get("firstRowOnly")
    act.configuration["first_row_only"] = first if isinstance(first, bool) else (True if first is None else None)
    act.configuration["first_row_only_explicit"] = "firstRowOnly" in tp


def _h_dataset_only(act, node, tp, ctx, san):
    role = "target" if (act.type or "").lower() == "delete" else "input"
    io = _dataset_io(tp.get("dataset"), role, san, "typeProperties.dataset")
    if io:
        _details(tp.get("storeSettings"), io, san, "typeProperties.storeSettings")
        act.inputs.append(io)
    if "fieldList" in tp:
        fl = tp["fieldList"]
        act.configuration["field_list"] = {"expression": fl["value"]} if _is_expr(fl) else san.clean(fl)


def _h_filter(act, node, tp, ctx, san):
    act.configuration["items"] = _expr_text(tp.get("items"))
    act.configuration["condition"] = _expr_text(tp.get("condition"))


def _h_foreach(act, node, tp, ctx, san):
    act.configuration["items"] = _expr_text(tp.get("items"))
    seq = tp.get("isSequential")
    act.configuration["is_sequential"] = seq if isinstance(seq, bool) else False
    act.configuration["is_sequential_explicit"] = "isSequential" in tp
    if "batchCount" in tp:
        act.configuration["batch_count"] = tp["batchCount"]
    elif not act.configuration["is_sequential"]:
        act.configuration["batch_count"] = 20  # ADF's documented default
        act.configuration["batch_count_is_default"] = True


def _h_if(act, node, tp, ctx, san):
    act.configuration["expression"] = _expr_text(tp.get("expression"))
    true_n = len(tp.get("ifTrueActivities") or []) if isinstance(tp.get("ifTrueActivities"), list) else 0
    false_n = len(tp.get("ifFalseActivities") or []) if isinstance(tp.get("ifFalseActivities"), list) else 0
    act.configuration["true_branch_count"] = true_n
    act.configuration["false_branch_count"] = false_n
    if not true_n and not false_n:
        _warn(act, "if_without_branches", "IfCondition has no activities in either branch.")


def _h_switch(act, node, tp, ctx, san):
    act.configuration["on"] = _expr_text(tp.get("on"))
    values = []
    for c in _list_of(act, tp, "cases", "typeProperties.cases"):
        if isinstance(c, dict):
            v, _ = _text(c.get("value"))
            values.append(v)
    act.configuration["cases"] = values
    has_default = bool(tp.get("defaultActivities"))
    act.configuration["has_default"] = has_default
    if not values and not has_default:
        _warn(act, "switch_without_cases", "Switch has neither cases nor a default branch.")
    dupes = sorted({v for v in values if values.count(v) > 1 and v is not None})
    if dupes:
        _warn(act, "duplicate_switch_case", f"Duplicate case value(s): {', '.join(dupes)}.")


def _h_until(act, node, tp, ctx, san):
    act.configuration["expression"] = _expr_text(tp.get("expression"))


def _h_wait(act, node, tp, ctx, san):
    v = tp.get("waitTimeInSeconds")
    act.configuration["wait_seconds"] = {"expression": v["value"]} if _is_expr(v) else v


def _h_set_variable(act, node, tp, ctx, san):
    name = tp.get("variableName") if isinstance(tp.get("variableName"), str) else None
    act.configuration["variable_name"] = name
    system = tp.get("setSystemVariable") is True
    act.configuration["is_pipeline_return_value"] = system or name == "pipelineReturnValue"
    if system or name == "pipelineReturnValue":
        act.operations = [AdfOperationKind.set_variable]
        act.configuration["return_values"] = san.clean(tp.get("value"), "typeProperties.value")
    else:
        act.configuration["value"] = _expr_text(tp.get("value"))
        if name and name.lower() not in ctx.variables:
            _warn(act, "undeclared_variable", f"Variable '{name}' is not declared by the pipeline.",
                  "typeProperties.variableName")
        elif name:
            act.configuration["variable_type"] = ctx.variables[name.lower()]
    if name:
        act.variables_written = [name]


def _h_append_variable(act, node, tp, ctx, san):
    name = tp.get("variableName") if isinstance(tp.get("variableName"), str) else None
    act.configuration["variable_name"] = name
    act.configuration["value"] = _expr_text(tp.get("value"))
    if name:
        act.variables_written = [name]
        declared = ctx.variables.get(name.lower(), "__missing__")
        if declared == "__missing__":
            _warn(act, "undeclared_variable", f"Variable '{name}' is not declared by the pipeline.",
                  "typeProperties.variableName")
        elif (declared or "").lower() != "array":
            _warn(act, "append_to_non_array",
                  f"AppendVariable targets '{name}' whose declared type is {declared!r}, not Array.",
                  "typeProperties.variableName")
        else:
            act.configuration["variable_type"] = declared


def _h_execute_pipeline(act, node, tp, ctx, san):
    act.configuration["pipeline"] = _ref_name(tp.get("pipeline"))
    params = tp.get("parameters") if isinstance(tp.get("parameters"), dict) else {}
    act.configuration["parameters"] = {k: _expr_text(v) for k, v in sorted(params.items())}
    act.configuration["wait_on_completion"] = tp.get("waitOnCompletion") if "waitOnCompletion" in tp else None


def _h_web(act, node, tp, ctx, san):
    url, dyn = _text(tp.get("url"))
    act.configuration["url"] = REDACTED if (url and unsafe_text(url)) else url
    act.configuration["url_is_dynamic"] = dyn
    if url and not dyn:
        host = urlparse(url).netloc
        act.configuration["host"] = host or None
    if "functionName" in tp:
        act.configuration["function_name"], _ = _text(tp.get("functionName"))
    method, _ = _text(tp.get("method"))
    act.configuration["method"] = method.upper() if method else None
    headers = tp.get("headers")
    act.configuration["header_names"] = sorted(headers) if isinstance(headers, dict) else []
    act.configuration["has_body"] = "body" in tp
    auth = tp.get("authentication")
    if isinstance(auth, dict):
        act.configuration["authentication"] = {
            "type": auth.get("type") if isinstance(auth.get("type"), str) else None,
            "resource": _text(auth.get("resource"))[0]}
    _generic_endpoints(act, node, tp, san)
    for key in ("datasets", "linkedServices"):
        if isinstance(tp.get(key), list):
            act.configuration[snake_case(key)] = [_ref_name(r) for r in tp[key] if _ref_name(r)]


def _h_stored_procedure(act, node, tp, ctx, san):
    text, dyn = _text(tp.get("storedProcedureName"))
    act.configuration["stored_procedure"] = text
    act.configuration["stored_procedure_is_dynamic"] = dyn
    params = tp.get("storedProcedureParameters") if isinstance(tp.get("storedProcedureParameters"), dict) else {}
    out = {}
    for k, v in sorted(params.items()):
        if isinstance(v, dict):
            val, is_expr = _text(v.get("value"))
            out[k] = {"value": REDACTED if (val and unsafe_text(val)) else val,
                      "is_expression": is_expr, "type": v.get("type")}
    act.configuration["parameters"] = out
    _generic_endpoints(act, node, tp, san)


def _h_sql_pool_stored_procedure(act, node, tp, ctx, san):
    _h_stored_procedure(act, node, tp, ctx, san)
    pool = _ref_name(node.get("sqlPool"))
    act.configuration["sql_pool"] = pool
    if node.get("sqlPool") is not None and pool is None:
        _warn(act, "invalid_sql_pool_reference", "'sqlPool' has no referenceName.", "sqlPool", AdfIssueSeverity.error)


def _h_databricks_job(act, node, tp, ctx, san):
    text, dyn = _text(tp["jobId"] if "jobId" in tp else tp.get("jobID"))
    act.configuration["job_id"] = text
    act.configuration["job_id_is_dynamic"] = dyn
    params = tp.get("jobParameters")
    if isinstance(params, dict):
        act.configuration["job_parameters"] = {k: _expr_text(v) for k, v in sorted(params.items())}
    elif isinstance(params, list):  # Microsoft's property table calls it "an array of key-value pairs"
        act.configuration["job_parameters"] = san.clean(params, "jobParameters")
    elif params is not None:
        _warn(act, "invalid_job_parameters", "'jobParameters' is neither an object nor a list.",
              "typeProperties.jobParameters", AdfIssueSeverity.error)


def _h_script(act, node, tp, ctx, san):
    blocks, kinds_all = [], set()
    for b in _list_of(act, tp, "scripts", "typeProperties.scripts"):
        if not isinstance(b, dict):
            continue
        text, dyn = _text(b.get("text"))
        kinds = sorted({m.upper() if not m.lower().startswith("exec") else "EXEC"
                        for m in _STATEMENT_RE.findall(text or "")})
        kinds_all.update(kinds)
        blocks.append({
            "type": b.get("type"), "text": REDACTED if (text and unsafe_text(text)) else text,
            "is_dynamic": dyn, "statement_kinds": kinds,
            "parameters": [san.clean(p) for p in (b.get("parameters") if isinstance(b.get("parameters"), list) else [])
                           if isinstance(p, dict)]})
    act.configuration["scripts"] = blocks
    act.configuration["statement_kinds"] = sorted(kinds_all)
    writes = bool(kinds_all & _WRITE_STATEMENTS)
    reads = "SELECT" in kinds_all or any(b.get("type") == "Query" for b in blocks)
    ops = [AdfOperationKind.transform]
    if reads:
        ops.insert(0, AdfOperationKind.read)
    if writes:
        ops.append(AdfOperationKind.write)
    act.operations = ops
    act.operation = ops[0] if not (reads and writes) else AdfOperationKind.transform
    _generic_endpoints(act, node, tp, san)


def _h_dataflow(act, node, tp, ctx, san):
    act.configuration["dataflow"] = _ref_name(tp.get("dataflow"))
    df = tp.get("dataflow") if isinstance(tp.get("dataflow"), dict) else {}
    params = df.get("parameters") if isinstance(df.get("parameters"), dict) else {}
    act.configuration["parameters"] = {k: _expr_text(v) for k, v in sorted(params.items())}
    comp = tp.get("compute")
    if isinstance(comp, dict):
        act.configuration["compute"] = {"core_count": comp.get("coreCount"), "compute_type": comp.get("computeType")}
    staging = tp.get("staging") if isinstance(tp.get("staging"), dict) else {}
    if staging:
        act.configuration["staging"] = {"linked_service": _ref_name(staging.get("linkedService")),
                                        "folder_path": _text(staging.get("folderPath"))[0]}
    io = AdfIO(role="target", kind="dataflow", name=_ref_name(tp.get("dataflow")))
    act.inputs.append(io)


def _expr_text(v: Any) -> Any:
    """Authored value: literal as-is, expression as {'expression': raw}."""
    if _is_expr(v):
        return {"expression": REDACTED if unsafe_text(v["value"]) else v["value"]}
    if isinstance(v, str) and unsafe_text(v):
        return REDACTED
    return v


_HANDLERS = {
    "copy": _h_copy, "lookup": _h_lookup, "dataset_only": _h_dataset_only, "filter": _h_filter,
    "foreach": _h_foreach, "if": _h_if, "switch": _h_switch, "until": _h_until, "wait": _h_wait,
    "set_variable": _h_set_variable, "append_variable": _h_append_variable,
    "execute_pipeline": _h_execute_pipeline, "web": _h_web, "stored_procedure": _h_stored_procedure,
    "script": _h_script, "dataflow": _h_dataflow, "databricks_job": _h_databricks_job,
    "sql_pool_stored_procedure": _h_sql_pool_stored_procedure,
}
