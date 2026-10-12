"""Validation of generated Fabric pipeline content.

Two things are deliberately kept apart:

* STRUCTURAL validity — is the JSON consistent with the documented Fabric schema
  (``fabric_schema``)? Envelope, required/allowed properties, policy ranges, dependency graph,
  activity limit, expression syntax and function set, references to parameters / variables /
  activities. A failure here means the file would be rejected or is internally inconsistent.
* RUNTIME readiness — can it actually run? Placeholder connection / item / gateway ids that are
  still unbound are reported as ``unbound_reference`` (info), never as structural errors, because
  they are prerequisites a person supplies, not defects in the generated JSON.

Nothing here talks to Fabric: "valid" means "consistent with the documented schema".
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from adf2fabric.agent.conversion.fabric_schema import (
    ACTIVITY_LIMIT,
    ACTIVITY_STATES,
    CONDITIONS,
    ENVELOPE_KEYS,
    FABRIC_ACTIVITIES,
    FABRIC_FUNCTIONS,
    FABRIC_PIPELINE_SYSTEM,
    ON_INACTIVE_MARK_AS,
    PIPELINE_PROPERTY_KEYS,
    POLICY_KEYS,
    RETRY_INTERVAL_MAX,
    RETRY_INTERVAL_MIN,
    SECURE_POLICY_KEYS,
)
from adf2fabric.agent.discovery.adf_semantic.expressions import analyze_expression
from adf2fabric.project.conversion import ConversionIssue, Impact, IssueSeverity

_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_TOKEN = re.compile(r"^<(connection|item|workspace-id|gateway|database)[:>].*>?$")
_TIMESPAN = re.compile(r"^(\d+\.)?\d{1,2}:\d{2}:\d{2}$")
_NESTED = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")


@dataclass
class Validation:
    issues: list[tuple[Optional[str], ConversionIssue]] = field(default_factory=list)  # (component, issue)
    unbound: dict[str, set[str]] = field(default_factory=dict)  # component -> placeholder tokens

    def error(self, component: Optional[str], code: str, message: str, action: str = "", path: str = "") -> None:
        self.issues.append((component, ConversionIssue(
            code=code, severity=IssueSeverity.error, impact=Impact.none, message=message, action=action,
            target_path=path or None)))

    def warn(self, component: Optional[str], code: str, message: str, action: str = "", path: str = "") -> None:
        self.issues.append((component, ConversionIssue(
            code=code, severity=IssueSeverity.warning, impact=Impact.none, message=message, action=action,
            target_path=path or None)))

    @property
    def errors(self) -> list[tuple[Optional[str], ConversionIssue]]:
        return [(c, i) for c, i in self.issues if i.severity is IssueSeverity.error]


def _children(activity: dict) -> Iterator[tuple[str, list]]:
    """(label, nested activity list) pairs of one activity."""
    tp = activity.get("typeProperties")
    if not isinstance(tp, dict):
        return
    for key in _NESTED:
        if isinstance(tp.get(key), list):
            yield key, tp[key]
    if isinstance(tp.get("cases"), list):
        for i, case in enumerate(tp["cases"]):
            if isinstance(case, dict) and isinstance(case.get("activities"), list):
                yield f"cases[{i}].activities", case["activities"]


def _iter_expressions(node: Any, path: str = "") -> Iterator[tuple[str, str]]:
    if isinstance(node, dict):
        if node.get("type") == "Expression" and isinstance(node.get("value"), str):
            yield path, node["value"]
            return
        for k, v in node.items():
            yield from _iter_expressions(v, f"{path}.{k}" if path else str(k))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from _iter_expressions(v, f"{path}[{i}]")


def _iter_strings(node: Any) -> Iterator[str]:
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for v in node.values():
            yield from _iter_strings(v)
    elif isinstance(node, list):
        for v in node:
            yield from _iter_strings(v)


def _count(activities: list) -> int:
    n = 0
    for a in activities:
        if isinstance(a, dict):
            n += 1
            for _, kids in _children(a):
                n += _count(kids)
    return n


def _all_names(activities: list) -> list[str]:
    out: list[str] = []
    for a in activities:
        if isinstance(a, dict):
            out.append(str(a.get("name", "")))
            for _, kids in _children(a):
                out.extend(_all_names(kids))
    return out


def validate_content(content: dict) -> Validation:
    v = Validation()
    props = content.get("properties") if isinstance(content, dict) else None
    if not isinstance(props, dict):
        v.error(None, "missing_properties", "pipeline-content.json has no 'properties' object.")
        return v
    for key in props:
        if key not in PIPELINE_PROPERTY_KEYS:
            v.warn(None, "undocumented_pipeline_property", f"Pipeline property '{key}' is not in the encoded schema.")
    activities = props.get("activities")
    if not isinstance(activities, list):
        v.error(None, "missing_activities", "'properties.activities' must be a list.")
        return v

    total = _count(activities)
    if total > ACTIVITY_LIMIT:
        v.error(None, "activity_limit_exceeded",
                f"The pipeline has {total} activities including nested ones; Fabric allows {ACTIVITY_LIMIT} per pipeline.",
                "Split the pipeline into child pipelines invoked with Invoke pipeline.")

    names = _all_names(activities)
    seen: set[str] = set()
    for n in names:
        if n.lower() in seen:
            v.error(n, "duplicate_activity_name", f"Activity name '{n}' is used more than once.",
                    "Activity names must be unique within the pipeline.")
        seen.add(n.lower())

    params = {k.lower() for k in (props.get("parameters") or {})}
    variables = {k.lower() for k in (props.get("variables") or {})}
    all_names = {n.lower() for n in names}
    _scope(v, activities, params, variables, all_names, in_loop=False)
    return v


def _scope(v: Validation, activities: list, params: set[str], variables: set[str], all_names: set[str],
           in_loop: bool) -> None:
    scope_names = {str(a.get("name", "")).lower() for a in activities if isinstance(a, dict)}
    graph: dict[str, list[str]] = {}
    for a in activities:
        if not isinstance(a, dict):
            v.error(None, "invalid_activity", "A list entry is not an activity object.")
            continue
        name = str(a.get("name", ""))
        for dep in a.get("dependsOn") or []:
            target = str(dep.get("activity", "")) if isinstance(dep, dict) else ""
            if target.lower() == name.lower():
                v.error(name, "self_dependency", f"'{name}' depends on itself.")
            elif target.lower() not in scope_names:
                v.error(name, "dependency_not_in_scope",
                        f"'{name}' depends on '{target}', which is not in the same activities list.",
                        "Dependencies cannot cross container boundaries.")
            else:
                graph.setdefault(name.lower(), []).append(target.lower())
            conds = dep.get("dependencyConditions") if isinstance(dep, dict) else None
            if not isinstance(conds, list) or not conds or any(c not in CONDITIONS for c in conds):
                v.error(name, "invalid_dependency_conditions",
                        f"'{name}' has missing or invalid dependency conditions {conds!r}.",
                        f"Use one or more of {', '.join(CONDITIONS)}.")
        _activity(v, a, params, variables, all_names, in_loop)
    # cycles
    state: dict[str, int] = {}

    def dfs(n: str) -> bool:
        state[n] = 1
        for m in graph.get(n, []):
            if state.get(m) == 1 or (state.get(m) is None and dfs(m)):
                return True
        state[n] = 2
        return False

    for n in sorted(graph):
        if state.get(n) is None and dfs(n):
            v.error(n, "dependency_cycle", f"The dependency graph containing '{n}' has a cycle.")
            break


def _activity(v: Validation, a: dict, params: set[str], variables: set[str], all_names: set[str],
              in_loop: bool) -> None:
    name = str(a.get("name", ""))
    atype = a.get("type")
    schema = FABRIC_ACTIVITIES.get(atype) if isinstance(atype, str) else None
    for req in ("name", "type", "typeProperties"):
        if req not in a:
            v.error(name or None, "missing_envelope_key", f"Activity '{name}' lacks required '{req}'.")
    if schema is None:
        v.error(name, "unknown_fabric_type", f"'{atype}' is not a Fabric activity type the converter may emit.")
        return

    allowed_env = ENVELOPE_KEYS | schema.envelope_extra | schema.inferred_envelope | set(schema.envelope_required)
    for key in a:
        if key not in allowed_env:
            v.warn(name, "undocumented_envelope_key", f"'{key}' is not a documented envelope key for {atype}.")
    for key in schema.envelope_required:
        if key not in a:
            v.error(name, "missing_envelope_reference", f"{atype} requires '{key}' (a connection reference).",
                    "Bind a Fabric connection.")
    ext = a.get("externalReferences")
    if ext is not None:
        conn = ext.get("connection") if isinstance(ext, dict) else None
        if not isinstance(conn, str):
            v.error(name, "invalid_connection_reference", "externalReferences.connection must be a string.")
        else:
            _bind(v, name, conn)

    tp = a.get("typeProperties")
    if isinstance(tp, dict):
        for key in schema.required:
            if key not in tp:
                v.error(name, "missing_required_property", f"{atype} requires typeProperties.{key}.",
                        path=f"typeProperties.{key}")
        for key in tp:
            if key not in schema.allowed:
                v.warn(name, "undocumented_property", f"typeProperties.{key} is not in the documented {atype} table.",
                       path=f"typeProperties.{key}")
        for s in _iter_strings(tp):
            if _TOKEN.match(s):
                _bind(v, name, s)
        _expressions(v, name, tp, params, variables, all_names, in_loop)
        _specific(v, name, atype, tp, variables)
    elif tp is not None:
        v.error(name, "invalid_type_properties", "typeProperties must be an object.")

    if "state" in a and a["state"] not in ACTIVITY_STATES:
        v.error(name, "invalid_state", f"state '{a['state']}' is not one of {ACTIVITY_STATES}.")
    if "onInactiveMarkAs" in a and a["onInactiveMarkAs"] not in ON_INACTIVE_MARK_AS:
        v.error(name, "invalid_on_inactive_mark_as", f"onInactiveMarkAs '{a['onInactiveMarkAs']}' is invalid.")
    _policy(v, name, a.get("policy"), schema.secure_policy_only)

    nested_loop = in_loop or atype == "ForEach"
    for label, kids in _children(a):
        _scope(v, kids, params, variables, all_names, nested_loop)
    if atype == "Switch" and isinstance(tp, dict):
        for i, case in enumerate(tp.get("cases") or []):
            if not isinstance(case, dict) or not isinstance(case.get("value"), str) \
                    or not isinstance(case.get("activities"), list):
                v.error(name, "invalid_switch_case", f"cases[{i}] needs a string 'value' and an 'activities' list.")


def _bind(v: Validation, component: str, token: str) -> None:
    if _GUID.match(token):
        return
    if _TOKEN.match(token):
        v.unbound.setdefault(component, set()).add(token)


def _policy(v: Validation, name: str, policy: Any, secure_only: bool) -> None:
    if policy is None:
        return
    if not isinstance(policy, dict):
        v.error(name, "invalid_policy", "policy must be an object.")
        return
    allowed = SECURE_POLICY_KEYS if secure_only else POLICY_KEYS
    for k in policy:
        if k not in allowed:
            v.error(name, "undocumented_policy_key", f"policy.{k} is not documented for this activity.")
    rv = policy.get("retryIntervalInSeconds")
    if isinstance(rv, int) and not (RETRY_INTERVAL_MIN <= rv <= RETRY_INTERVAL_MAX):
        v.error(name, "retry_interval_out_of_range",
                f"retryIntervalInSeconds={rv}; Fabric documents {RETRY_INTERVAL_MIN}-{RETRY_INTERVAL_MAX}.")
    rt = policy.get("retry")
    if isinstance(rt, int) and rt < 0:
        v.error(name, "retry_negative", "policy.retry must be >= 0.")
    to = policy.get("timeout")
    if isinstance(to, str) and not _TIMESPAN.match(to):
        v.warn(name, "timeout_format", f"policy.timeout '{to}' is not in D.HH:MM:SS form.")


def _specific(v: Validation, name: str, atype: str, tp: dict, variables: set[str]) -> None:
    if atype in ("SetVariable", "AppendVariable"):
        var = tp.get("variableName")
        if isinstance(var, str) and var.lower() not in variables and var != "pipelineReturnValue" \
                and not tp.get("setSystemVariable"):
            v.error(name, "undeclared_variable", f"Variable '{var}' is not declared on the pipeline.",
                    "Declare it under properties.variables.")
    if atype == "InvokePipeline":
        for k in ("pipelineId", "workspaceId"):
            if k not in tp:
                v.warn(name, "invoke_missing_target", f"InvokePipeline has no {k}.")
    if atype == "WebActivity" and isinstance(tp.get("method"), str) \
            and tp["method"].upper() not in ("GET", "POST", "PUT", "DELETE", "PATCH"):
        v.error(name, "invalid_http_method", f"HTTP method '{tp['method']}' is not supported by the Web activity.")


def _expressions(v: Validation, name: str, tp: dict, params: set[str], variables: set[str], all_names: set[str],
                 in_loop: bool) -> None:
    for path, raw in _iter_expressions(tp):
        e = analyze_expression(raw, path)
        if e.parse_status == "unparsed":
            v.error(name, "expression_syntax", f"Expression at {path} does not parse: {e.error}.", path=path)
            continue
        for fn in e.functions:
            if fn.lower() not in FABRIC_FUNCTIONS:
                v.error(name, "unsupported_function",
                        f"Expression at {path} uses '{fn}', which is not a documented Fabric function.",
                        "Rewrite it with documented functions.", path)
        for r in e.references:
            if r.kind == "global_parameter":
                v.error(name, "unsupported_reference", f"Expression at {path} reads a global parameter; Fabric has none.",
                        path=path)
            elif r.kind in ("dataset_parameter", "linked_service_parameter", "trigger"):
                v.error(name, "unsupported_reference", f"Expression at {path} uses {r.kind.replace('_', ' ')}, which "
                        "has no documented Fabric equivalent.", path=path)
            elif r.kind in ("pipeline_system", "pipeline_property") and r.name                     and r.name.lower() not in FABRIC_PIPELINE_SYSTEM and r.name.lower() != "parameters":
                v.error(name, "unsupported_system_variable",
                        f"pipeline().{r.name} is not a documented Fabric system variable.", path=path)
            elif r.kind == "pipeline_parameter" and r.name and r.name.lower() not in params:
                v.error(name, "undeclared_parameter", f"Expression at {path} reads parameter '{r.name}', which the "
                        "pipeline does not declare.", path=path)
            elif r.kind == "variable" and r.name and r.name.lower() not in variables:
                v.error(name, "undeclared_variable", f"Expression at {path} reads variable '{r.name}', which the "
                        "pipeline does not declare.", path=path)
            elif r.kind == "activity_output" and r.name and r.name.lower() not in all_names:
                v.error(name, "unknown_activity_reference", f"Expression at {path} reads the output of '{r.name}', "
                        "which is not an activity of this pipeline.", path=path)
            elif r.kind == "loop_item" and not in_loop:
                v.warn(name, "item_outside_loop", f"item() at {path} is used outside a ForEach.", path=path)
