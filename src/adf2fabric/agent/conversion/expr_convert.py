"""ADF expression -> Fabric expression conversion.

Fabric's expression language is "nearly identical" to ADF's (Microsoft's own wording), so
most expressions pass through UNCHANGED, character for character. Only an explicit, named
rule ever rewrites one; every other difference is reported, never papered over:

    rule                                  action
    ------------------------------------  ----------------------------------------------------
    pipeline().Pipeline                   -> pipeline().PipelineName   (ADF: name; Fabric: ID)
    pipeline().DataFactory                kept, DIFFERENCE reported    (ADF: factory name; Fabric: workspace ID)
    pipeline().globalParameters.X         -> pipeline().parameters.gp_X (global parameters do not exist)
    dataset().P                           -> the argument/default passed to the dataset (inlining)
    pipeline().TriggerType, trigger*(),   UNSUPPORTED (no documented Fabric equivalent), kept verbatim
      linkedService().P
    function not in the Fabric function   UNSUPPORTED, kept verbatim — never replaced by a "similar" one
      list (e.g. decimal, formatNumber)
    activity('A') where A was renamed     -> activity('<component>')

The text is parsed with the same deterministic parser the semantic stage uses, rewritten at
the syntax-tree level (never with string substitution) and re-rendered ONLY when a rule
fired. Nothing is evaluated and no value is ever invented.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional

from adf2fabric.agent.conversion.fabric_schema import (
    FABRIC_FUNCTIONS,
    FABRIC_PIPELINE_SYSTEM,
    UNSUPPORTED_ACCESSORS,
)
from adf2fabric.agent.discovery.adf_semantic.expressions import (
    Call,
    ExpressionSyntaxError,
    Index,
    Literal,
    Member,
    parse_expression,
    render,
    split_template,
)
from adf2fabric.project.conversion import (
    ConversionIssue,
    ExpressionConversion,
    Impact,
    IssueSeverity,
)

GLOBAL_PARAM_PREFIX = "gp_"


@dataclass
class ExprEnv:
    """Everything an expression conversion may depend on."""

    dataset_name: Optional[str] = None
    dataset_args: Optional[dict[str, Any]] = None  # lowercase param -> already-converted argument
    dataset_defaults: dict[str, Any] = field(default_factory=dict)  # lowercase param -> authored default
    activity_rename: dict[str, str] = field(default_factory=dict)  # lowercase source name -> component
    used_globals: set[str] = field(default_factory=set)  # original names of global parameters used
    existing_params: set[str] = field(default_factory=set)  # lowercase pipeline parameter names


@dataclass
class Converted:
    value: Any  # the JSON value to emit (Expression object, or a plain literal after substitution)
    status: str  # unchanged | rewritten | unsupported
    source: str
    target: Optional[str] = None
    changes: list[str] = field(default_factory=list)
    issues: list[ConversionIssue] = field(default_factory=list)


def is_expression(v: Any) -> bool:
    return isinstance(v, dict) and v.get("type") == "Expression" and isinstance(v.get("value"), str)


def expr_obj(text: str) -> dict:
    return {"value": text, "type": "Expression"}


def global_param_name(name: str, existing: set[str]) -> str:
    """Target pipeline parameter name for a global parameter (never collides with a real one)."""
    candidate = GLOBAL_PARAM_PREFIX + name
    while candidate.lower() in existing:
        candidate += "_"
    return candidate


# --- syntax-tree transformation ---------------------------------------------------------------------


def _flatten(node):
    segs: list[tuple] = []  # ('prop', name, optional) | ('index', node, optional)
    while True:
        if isinstance(node, Member):
            segs.append(("prop", node.prop, node.optional))
            node = node.obj
        elif isinstance(node, Index):
            segs.append(("index", node.index, node.optional))
            node = node.obj
        else:
            break
    segs.reverse()
    return node, segs


def _build(root, segs):
    node = root
    for seg in segs:
        node = Member(node, seg[1], seg[2]) if seg[0] == "prop" else Index(node, seg[1], seg[2])
    return node


def _value_to_node(value: Any):
    """A syntax node producing ``value`` (a converted argument), for dataset() substitution."""
    if is_expression(value):
        form, parts = split_template(value["value"])
        if form == "expression":
            return parse_expression(parts[0][1])
        if form == "interpolated":
            args = []
            for kind, text in parts:
                args.append(Literal(text, "string") if kind == "text" else parse_expression(text))
            return Call("concat", tuple(args))
        return Literal("".join(t for _, t in parts), "string")
    if isinstance(value, bool):
        return Literal(value, "bool")
    if isinstance(value, (int, float)):
        return Literal(value, "number")
    if value is None:
        return Literal(None, "null")
    if isinstance(value, str):
        return Literal(value, "string")
    return Call("json", (Literal(json.dumps(value, sort_keys=True), "string"),))  # array/object literal


class _Transformer:
    def __init__(self, env: ExprEnv, source_text: str):
        self.env = env
        self.changes: list[str] = []
        self.issues: list[ConversionIssue] = []
        self.unsupported = False
        self.source_text = source_text

    # -- reporting -------------------------------------------------------------------------------
    def _issue(self, code: str, message: str, impact: Impact, action: str = "",
               severity: IssueSeverity = IssueSeverity.warning) -> None:
        if not any(i.code == code and i.message == message for i in self.issues):
            self.issues.append(ConversionIssue(code=code, severity=severity, impact=impact,
                                               message=message, action=action))

    def _unsupported(self, code: str, message: str, action: str) -> None:
        self.unsupported = True
        self._issue(code, message, Impact.manual, action)

    # -- traversal -------------------------------------------------------------------------------
    def visit(self, node):
        if isinstance(node, Literal):
            return node
        if isinstance(node, (Member, Index)):
            return self._chain(node)
        if isinstance(node, Call):
            return self._chain(node)
        return node

    def _chain(self, node):
        root, segs = _flatten(node)
        new_segs = [(s[0], self.visit(s[1]) if s[0] == "index" else s[1], s[2]) for s in segs]
        if not isinstance(root, Call):
            return _build(self.visit(root), new_segs)
        name = root.name.lower()
        args = tuple(self.visit(a) for a in root.args)
        root = Call(root.name, args)

        if name == "pipeline":
            return self._pipeline(root, new_segs)
        if name == "dataset":
            return self._dataset(root, new_segs)
        if name in UNSUPPORTED_ACCESSORS:
            self._unsupported(
                f"unsupported_{name}",
                f"{root.name}() has no documented Fabric equivalent: {UNSUPPORTED_ACCESSORS[name]}.",
                "Replace it with a pipeline parameter supplied by the caller or the schedule, then re-convert.")
            return _build(root, new_segs)
        if name == "activity":
            return self._activity(root, new_segs)
        if name not in FABRIC_FUNCTIONS:
            self._unsupported(
                "unsupported_function",
                f"Function '{root.name}' is not in the documented Fabric function set; it is kept verbatim "
                "and was NOT replaced by a similar function (behavior could differ).",
                "Rewrite the expression with documented functions, or move the logic into a notebook/script.")
        return _build(root, new_segs)

    def _pipeline(self, root: Call, segs):
        if segs and segs[0][0] == "prop":
            prop = str(segs[0][1])
            low = prop.lower()
            if low == "pipeline":
                self.changes.append("pipeline().Pipeline -> pipeline().PipelineName "
                                    "(ADF returns the pipeline name; Fabric's Pipeline is the pipeline ID)")
                segs = [("prop", "PipelineName", segs[0][2]), *segs[1:]]
            elif low == "datafactory":
                self._issue("system_variable_semantics",
                            "pipeline().DataFactory is the factory NAME in ADF but the WORKSPACE ID in Fabric; "
                            "the expression is kept but its value will differ.",
                            Impact.difference, "Review every use; compare against the workspace id, not a factory name.")
            elif low == "triggertype":
                self._unsupported("unsupported_trigger_type",
                                  "pipeline().TriggerType is not a documented Fabric system variable.",
                                  "Pass the trigger kind as a pipeline parameter from the schedule/trigger.")
            elif low == "globalparameters" and len(segs) > 1 and segs[1][0] == "prop":
                gname = str(segs[1][1])
                target = global_param_name(gname, self.env.existing_params)
                self.env.used_globals.add(gname)
                self.changes.append(f"pipeline().globalParameters.{gname} -> pipeline().parameters.{target} "
                                    "(Fabric has no global parameters; a pipeline parameter carries the value)")
                self._issue("global_parameter_converted",
                            f"Global parameter '{gname}' became pipeline parameter '{target}' with the factory's "
                            "default value; values are no longer shared factory-wide or overridden per environment.",
                            Impact.difference,
                            "Supply the per-environment value when the pipeline is scheduled or invoked.")
                segs = [("prop", "parameters", segs[0][2]), ("prop", target, segs[1][2]), *segs[2:]]
            elif low == "parameters" or low in FABRIC_PIPELINE_SYSTEM:
                pass
            else:
                self._unsupported("unsupported_system_variable",
                                  f"pipeline().{prop} is not a documented Fabric system variable.",
                                  "Replace it with a pipeline parameter.")
        return _build(root, segs)

    def _dataset(self, root: Call, segs):
        env = self.env
        if env.dataset_args is None:
            self._unsupported("unsupported_dataset_reference",
                              UNSUPPORTED_ACCESSORS["dataset"] + ".",
                              "Move the logic into the activity that uses the dataset.")
            return _build(root, segs)
        if not segs or segs[0][0] != "prop":
            self._unsupported("unsupported_dataset_reference", "dataset() used without a parameter name.", "")
            return _build(root, segs)
        pname = str(segs[0][1])
        low = pname.lower()
        if low in env.dataset_args:
            value = env.dataset_args[low]
            origin = "argument"
        elif low in env.dataset_defaults:
            value = env.dataset_defaults[low]
            origin = "declared default"
        else:
            self._unsupported(
                "dataset_parameter_unresolved",
                f"dataset().{pname} has no argument from the calling activity and no default"
                + (f" (dataset '{env.dataset_name}')" if env.dataset_name else "") + ".",
                "Pass a value for this dataset parameter in the activity, then re-convert.")
            return _build(root, segs)
        self.changes.append(f"dataset().{pname} inlined from the {origin}"
                            + (f" of dataset '{env.dataset_name}'" if env.dataset_name else ""))
        try:
            node = _value_to_node(value)
        except ExpressionSyntaxError as exc:
            self._unsupported("dataset_argument_unparsed",
                              f"The {origin} for dataset().{pname} could not be parsed: {exc}.", "")
            return _build(root, segs)
        return _build(node, segs[1:])

    def _activity(self, root: Call, segs):
        if root.args and isinstance(root.args[0], Literal) and root.args[0].kind == "string":
            old = str(root.args[0].value)
            new = self.env.activity_rename.get(old.lower())
            if new and new != old:
                self.changes.append(f"activity('{old}') -> activity('{new}') "
                                    "(the output now comes from a different generated component)")
                root = Call(root.name, (Literal(new, "string"), *root.args[1:]))
        return _build(root, segs)


# --- public conversion ---------------------------------------------------------------------------------


def _static_text(node) -> Optional[str]:
    if isinstance(node, Literal):
        if node.kind == "string":
            return str(node.value)
        if node.kind == "bool":
            return "true" if node.value else "false"
        if node.kind == "number":
            return str(node.value)
    return None


def convert_expression(raw: str, env: ExprEnv) -> Converted:
    """Convert one dynamic-content string. Never raises; never alters ``raw`` unless a rule fired."""
    unchanged = Converted(value=expr_obj(raw), status="unchanged", source=raw, target=raw)
    try:
        form, parts = split_template(raw)
    except ExpressionSyntaxError as exc:
        unchanged.status = "unsupported"
        unchanged.issues.append(ConversionIssue(
            code="expression_unparsed", severity=IssueSeverity.warning, impact=Impact.manual,
            message=f"The expression could not be parsed ({exc}); it is kept verbatim.",
            action="Fix the expression syntax in the source, then re-convert."))
        return unchanged
    if form in ("literal", "escaped_literal"):
        return unchanged

    tx = _Transformer(env, raw)
    nodes: list[tuple[str, Any]] = []
    try:
        for kind, text in parts:
            nodes.append((kind, tx.visit(parse_expression(text)) if kind == "expr" else text))
    except ExpressionSyntaxError as exc:
        unchanged.status = "unsupported"
        unchanged.issues.append(ConversionIssue(
            code="expression_unparsed", severity=IssueSeverity.warning, impact=Impact.manual,
            message=f"The expression could not be parsed ({exc}); it is kept verbatim.",
            action="Fix the expression syntax in the source, then re-convert."))
        return unchanged

    if not tx.changes:  # no rule fired: the original text stays exactly as authored
        unchanged.issues = tx.issues
        unchanged.status = "unsupported" if tx.unsupported else "unchanged"
        return unchanged

    if form == "expression":
        node = nodes[0][1]
        static = _static_text(node)
        if static is not None and isinstance(node, Literal):
            value: Any = node.value  # type-preserving literal after substitution
            target = None
        else:
            target = "@" + render(node)
            value = expr_obj(target)
    else:
        pieces: list[str] = []
        plain: list[str] = []
        dynamic = False
        for kind, item in nodes:
            if kind == "text":
                pieces.append(item.replace("@", "@@"))
                plain.append(item)
                continue
            static = _static_text(item)
            if static is not None:
                pieces.append(static.replace("@", "@@"))
                plain.append(static)
            else:
                dynamic = True
                pieces.append("@{" + render(item) + "}")
        if dynamic:
            target = "".join(pieces)
            value = expr_obj(target)
        else:
            target = None
            value = "".join(plain)

    return Converted(value=value, status="unsupported" if tx.unsupported else "rewritten", source=raw,
                     target=target, changes=tx.changes, issues=tx.issues)


def convert_value(value: Any, env: ExprEnv, source_path: str, target_path: str,
                  sink: list[ExpressionConversion], issues: list[ConversionIssue]) -> Any:
    """Convert a single value: an Expression object is converted and recorded; anything else is returned."""
    if not is_expression(value):
        return value
    conv = convert_expression(value["value"], env)
    for i in conv.issues:
        issues.append(i.model_copy(update={"source_path": source_path, "target_path": target_path}))
    sink.append(ExpressionConversion(
        source_path=source_path, target_path=target_path, source=conv.source, target=conv.target,
        status=conv.status, changes=conv.changes,
        issues=[i.model_copy(update={"source_path": source_path, "target_path": target_path}) for i in conv.issues]))
    return conv.value


def convert_tree(obj: Any, env: ExprEnv, source_path: str, target_path: str,
                 sink: list[ExpressionConversion], issues: list[ConversionIssue], _depth: int = 0) -> Any:
    """Convert every Expression object inside ``obj``; the rest of the structure is preserved."""
    if _depth > 60:
        return obj
    if is_expression(obj):
        return convert_value(obj, env, source_path, target_path, sink, issues)
    if isinstance(obj, dict):
        return {k: convert_tree(v, env, f"{source_path}.{k}" if source_path else str(k),
                                f"{target_path}.{k}" if target_path else str(k), sink, issues, _depth + 1)
                for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_tree(v, env, f"{source_path}[{i}]", f"{target_path}[{i}]", sink, issues, _depth + 1)
                for i, v in enumerate(obj)]
    return obj
