"""Control-flow, orchestration and variable conversions.

ForEach, IfCondition, Switch, Until, Filter, Wait, Fail, SetVariable, AppendVariable and
ExecutePipeline (-> InvokePipeline). All of these have documented Fabric equivalents with the
same property names, so conversion is property carry-over plus expression conversion; nested
activities are converted by the engine and embedded under the same keys (``activities``,
``ifTrueActivities``, ``ifFalseActivities``, ``cases[].activities``, ``defaultActivities``).
"""

from __future__ import annotations

from typing import Any

from adf2fabric.agent.conversion.context import ActivityBuilder
from adf2fabric.agent.conversion.expr_convert import expr_obj, global_param_name
from adf2fabric.agent.conversion.handlers._common import block, carry
from adf2fabric.agent.conversion.registry import ConversionRule, register
from adf2fabric.project.conversion import ConversionStrategy
from adf2fabric.project.discovery import AdfOperationKind as Op


def _simple(cx: ActivityBuilder, ftype: str, keys: tuple[str, ...], op: Op, summary: str, *,
            secure_policy_only: bool = False, extra_tp: dict | None = None, skip: tuple[str, ...] = ()) -> str:
    tp = carry(cx, keys, skip=skip)
    if extra_tp:
        tp.update(extra_tp)
    name = cx.add(cx.envelope(cx.act.name, ftype, tp, secure_policy_only=secure_policy_only))
    cx.cover(op, name)
    cx.strategy = ConversionStrategy.direct
    cx.summary = summary
    return name


# --- ForEach / If / Switch / Until / Filter -----------------------------------------------------------


def convert_foreach(cx: ActivityBuilder) -> None:
    a = cx.act
    kids = cx.children("activities")
    _simple(cx, "ForEach", ("items", "isSequential", "batchCount"), Op.loop,
                   f"ForEach '{a.name}' -> Fabric ForEach over the same collection with {len(kids)} nested "
                   f"activit{'y' if len(kids) == 1 else 'ies'}; "
                   + ("sequential." if a.configuration.get("is_sequential") else "parallel."),
                   extra_tp={"activities": kids})
    cx.mapping("typeProperties.activities", "typeProperties.activities", "nested_activities_converted",
               f"{len(kids)} nested activities converted recursively")
    if not kids:
        cx.difference("foreach_empty", "The ForEach has no nested activities.", "")


def convert_if(cx: ActivityBuilder) -> None:
    extra: dict[str, Any] = {}
    # (nested activity lists are not part of raw_properties; the engine supplies each branch)
    for key, branch in (("ifTrueActivities", "ifTrue"), ("ifFalseActivities", "ifFalse")):
        kids = cx.children(branch)
        if kids:
            extra[key] = kids
    _simple(cx, "IfCondition", ("expression",), Op.branch,
            f"IfCondition '{cx.act.name}' -> Fabric IfCondition on the same expression; "
            f"{len(extra.get('ifTrueActivities', []))} activity(ies) when true, "
            f"{len(extra.get('ifFalseActivities', []))} when false.", extra_tp=extra)


def convert_switch(cx: ActivityBuilder) -> None:
    a, raw = cx.act, cx.act.raw_properties
    cases: list[dict] = []
    for i, case in enumerate(raw.get("cases") or []):
        if not isinstance(case, dict):
            continue
        value = case.get("value")
        label = value if isinstance(value, str) else str(value)
        cases.append({"value": label, "activities": cx.children(f"case:{label}")})
        cx.mapping(f"typeProperties.cases[{i}]", f"typeProperties.cases[{i}]", "switch_case",
                   f"case '{label}' with {len(cases[-1]['activities'])} activities")
    extra: dict[str, Any] = {}
    if cases:
        extra["cases"] = cases
    default = cx.children("default") if a.configuration.get("has_default") else []
    if default:
        extra["defaultActivities"] = default
    _simple(cx, "Switch", ("on",), Op.branch,
            f"Switch '{a.name}' -> Fabric Switch on the same expression with {len(cases)} case(s)"
            f"{' and a default branch' if default else ''}.", extra_tp=extra, skip=("cases",))
    if default:
        cx.mapping("typeProperties.defaultActivities", "typeProperties.defaultActivities", "switch_default")
    else:
        cx.mapping("typeProperties.defaultActivities", None, "switch_no_default",
                   "no default branch in the source; none is generated, so unmatched values do nothing in both")


def convert_until(cx: ActivityBuilder) -> None:
    kids = cx.children("activities")
    raw = cx.act.raw_properties
    _simple(cx, "Until", ("expression", "timeout"), Op.loop,
            f"Until '{cx.act.name}' -> Fabric Until repeating {len(kids)} nested activity(ies) until the same "
            "condition is true.", extra_tp={"activities": kids})
    if "timeout" not in raw:
        cx.difference("until_default_timeout",
                      "No timeout is set: ADF and Fabric document different defaults for the Until activity's "
                      "timeout (ADF: 12 hours in the activity policy; Fabric: 7 days).",
                      "Set an explicit timeout if the loop must stop earlier.")


def convert_filter(cx: ActivityBuilder) -> None:
    _simple(cx, "Filter", ("items", "condition"), Op.filter,
            f"Filter '{cx.act.name}' -> Fabric Filter with the same input array and condition.")


def convert_wait(cx: ActivityBuilder) -> None:
    _simple(cx, "Wait", ("waitTimeInSeconds",), Op.wait,
            f"Wait '{cx.act.name}' -> Fabric Wait for the same number of seconds.")


def convert_fail(cx: ActivityBuilder) -> None:
    _simple(cx, "Fail", ("message", "errorCode"), Op.fail,
            f"Fail '{cx.act.name}' -> Fabric Fail with the same message and error code.")


for _name, _types, _ftype, _handler, _props in (
    ("foreach", ("ForEach",), "ForEach", convert_foreach,
     ("items, isSequential, batchCount carried", "activities converted recursively")),
    ("if_condition", ("IfCondition",), "IfCondition", convert_if,
     ("expression carried", "ifTrueActivities / ifFalseActivities converted recursively")),
    ("switch", ("Switch",), "Switch", convert_switch,
     ("on carried", "cases[].activities and defaultActivities converted recursively")),
    ("until", ("Until",), "Until", convert_until,
     ("expression, timeout carried", "activities converted recursively")),
    ("filter", ("Filter",), "Filter", convert_filter, ("items, condition carried",)),
    ("wait", ("Wait",), "Wait", convert_wait, ("waitTimeInSeconds carried",)),
    ("fail", ("Fail",), "Fail", convert_fail, ("message, errorCode carried",)),
):
    register(ConversionRule(
        name=_name, adf_types=_types, variants=("static value", "dynamic expression"), fabric_types=(_ftype,),
        strategy=ConversionStrategy.direct, handler=_handler, runtime_disposition="replace",
        property_rules=_props,
        io_rules=("none (control-flow activity)",),
        expression_rules=("expressions pass through unchanged unless a named rewrite applies "
                          "(pipeline().Pipeline, globalParameters)",),
        dependency_rules=("dependsOn mapped 1:1; nested activities keep their own scope",),
        parameter_variable_rules=("item(), variables() and parameters referenced unchanged",),
        unsupported=("properties outside the documented Fabric table",),
        manual_configuration=(),
        validation_rules=("documented typeProperties only", "nested activities valid in their own scope",
                          "total activities <= 120"),
    ))


# --- Variables ------------------------------------------------------------------------------------------------


def convert_set_variable(cx: ActivityBuilder) -> None:
    a = cx.act
    _simple(cx, "SetVariable", ("variableName", "value", "setSystemVariable"), Op.set_variable,
                   f"SetVariable '{a.name}' -> Fabric SetVariable assigning "
                   + ("the pipeline return value." if a.configuration.get("is_pipeline_return_value")
                      else f"variable '{a.configuration.get('variable_name')}'."),
                   secure_policy_only=True)
    if a.configuration.get("is_pipeline_return_value"):
        cx.info("pipeline_return_value", "This sets the pipeline return value (setSystemVariable); Fabric documents "
                "the same property.", "Check how the invoking pipeline reads the value.")


def convert_append_variable(cx: ActivityBuilder) -> None:
    _simple(cx, "AppendVariable", ("variableName", "value"), Op.append_variable,
            f"AppendVariable '{cx.act.name}' -> Fabric AppendVariable on array variable "
            f"'{cx.act.configuration.get('variable_name')}'.")


register(ConversionRule(
    name="set_variable", adf_types=("SetVariable",), fabric_types=("SetVariable",),
    variants=("pipeline variable", "pipeline return value (setSystemVariable)"),
    strategy=ConversionStrategy.direct, handler=convert_set_variable, runtime_disposition="replace",
    property_rules=("variableName, value, setSystemVariable carried",),
    io_rules=("none",), expression_rules=("value expression converted; unchanged unless a named rewrite applies",),
    dependency_rules=("dependsOn mapped 1:1",),
    parameter_variable_rules=("the variable declaration is carried to the pipeline's variables",),
    unsupported=("policy other than secureInput/secureOutput (SetVariable accepts only those)",),
    manual_configuration=(), validation_rules=("variable declared on the pipeline",)))
register(ConversionRule(
    name="append_variable", adf_types=("AppendVariable",), fabric_types=("AppendVariable",),
    variants=("array variable",), strategy=ConversionStrategy.direct, handler=convert_append_variable,
    runtime_disposition="replace",
    property_rules=("variableName, value carried",), io_rules=("none",),
    expression_rules=("value expression converted",), dependency_rules=("dependsOn mapped 1:1",),
    parameter_variable_rules=("the Array variable declaration is carried to the pipeline's variables",),
    unsupported=(), manual_configuration=(), validation_rules=("variable declared on the pipeline",)))


# --- ExecutePipeline -> InvokePipeline ---------------------------------------------------------------------------


def convert_execute_pipeline(cx: ActivityBuilder) -> None:
    a, raw = cx.act, cx.act.raw_properties
    ref = raw.get("pipeline") if isinstance(raw.get("pipeline"), dict) else {}
    child = ref.get("referenceName") or a.configuration.get("pipeline")
    if not child:
        block(cx, "execute_pipeline_no_target", "no child pipeline is referenced.")
        return

    project = cx.pc.project
    known = child.lower() in project.pipelines
    params: dict[str, Any] = {}
    for k, v in (raw.get("parameters") or {}).items():
        params[k] = cx.tree(v, f"typeProperties.parameters.{k}", f"typeProperties.parameters.{k}")
        cx.mapping(f"typeProperties.parameters.{k}", f"typeProperties.parameters.{k}", "passthrough")
    # Global parameters become pipeline parameters (gp_*): the callee needs them passed down.
    for g in project.globals_needed.get(child.lower(), []):
        pname = global_param_name(g, cx.pc.param_names)
        params.setdefault(pname, expr_obj(f"@pipeline().parameters.{pname}"))
        cx.mapping(f"globalParameters.{g}", f"typeProperties.parameters.{pname}", "global_parameter_passed_down",
                   f"'{child}' reads global parameter '{g}'; its gp_ parameter is passed from this pipeline")
    cx.pc.used_globals.update(project.globals_needed.get(child.lower(), []))

    wait = raw.get("waitOnCompletion")
    wait_v = cx.tree(wait, "typeProperties.waitOnCompletion", "typeProperties.waitOnCompletion") if wait is not None else False
    if wait is None:
        cx.mapping("typeProperties.waitOnCompletion", "typeProperties.waitOnCompletion", "default_made_explicit",
                   "ADF and Fabric both default to false; written explicitly")
    else:
        cx.mapping("typeProperties.waitOnCompletion", "typeProperties.waitOnCompletion", "passthrough")

    tp: dict[str, Any] = {
        "pipelineId": cx.item("pipeline", child,
                              f"The Fabric pipeline converted from ADF pipeline '{child}'"
                              + ("" if known else " (NOT among the discovered pipelines - it must already exist)")),
        "workspaceId": cx.workspace(),
        "waitOnCompletion": wait_v,
    }
    if params:
        tp["parameters"] = params
    cx.mapping("typeProperties.pipeline", "typeProperties.pipelineId", "pipeline_reference_to_item_id",
               f"pipeline '{child}' -> Fabric pipeline item id (bound at deployment)")
    for key in raw:
        if key not in ("pipeline", "parameters", "waitOnCompletion"):
            cx.dropped(f"typeProperties.{key}", "not a documented Fabric InvokePipeline property")

    conn = cx.free_connection("invoke-pipeline",
                              "A Fabric connection used by Invoke pipeline activities (authentication: organizational "
                              "account, service principal or workspace identity).")
    name = cx.add(cx.envelope(a.name, "InvokePipeline", tp, connection=conn))
    cx.cover(Op.orchestrate, name)
    cx.strategy = ConversionStrategy.transformed
    cx.manual_steps.append("Open the Invoke pipeline activity in Fabric once and confirm it targets a Fabric pipeline "
                           "(the activity's operation type is not set by this tool).")
    cx.info("invoke_pipeline_operation_type", "The InvokePipeline 'operationType' is not set: its permitted values "
            "are not documented.", "Confirm the invoke type in the Fabric UI.")
    cx.output_notes.append("InvokePipeline output differs from ExecutePipeline (e.g. pipelineRunId); verify "
                           "expressions that read it.")
    if not known:
        cx.manual("child_pipeline_unknown", f"Child pipeline '{child}' is not among the discovered pipelines.",
                  f"Create or convert '{child}' and bind its item id.")
    cx.summary = (f"ExecutePipeline '{a.name}' -> Fabric InvokePipeline of '{child}' "
                  f"({'waiting' if wait_v is True else 'not waiting'}), passing {len(params)} parameter(s).")


register(ConversionRule(
    name="execute_pipeline", adf_types=("ExecutePipeline",), fabric_types=("InvokePipeline",),
    variants=("wait on completion", "fire and continue", "parameterized child", "per-item child (inside ForEach)"),
    strategy=ConversionStrategy.transformed, handler=convert_execute_pipeline, runtime_disposition="replace",
    property_rules=("pipeline reference -> pipelineId (+ workspaceId)", "parameters carried",
                    "waitOnCompletion made explicit"),
    io_rules=("child pipeline name -> Fabric pipeline item id placeholder",),
    expression_rules=("parameter expressions converted; gp_ parameters appended for the child's global parameters",),
    dependency_rules=("dependsOn mapped 1:1",),
    parameter_variable_rules=("global parameters are passed down as gp_ pipeline parameters",),
    unsupported=("any property outside parameters/waitOnCompletion/pipeline",),
    manual_configuration=("a Fabric connection for Invoke pipeline", "the child pipeline item ids and workspace id",
                         "confirm the invoke operation type in the UI"),
    validation_rules=("externalReferences.connection present", "pipelineId and workspaceId present"),
))
