"""Split a generated pipeline over Fabric's activity limit into child pipelines (2026-11-17, the catalog's workaround for ForEach,
If Condition, Switch and Until, done by the tool at the user's request).

Fabric allows ``ACTIVITY_LIMIT`` (120) activities per pipeline, nested ones included ("Activity overview"). ADF's own limit is lower,
so only conversion can push a pipeline over it (composites: a Validation becomes four activities, a Databricks job one notebook per
task ...). Then the largest *self-contained* inner block of a container (a ForEach's or Until's activities, an If branch, a Switch
case or default) moves to a child pipeline, and an Invoke pipeline activity (wait on completion, so the container fails when the
child does) takes its place. Repeated until the pipeline fits or no block can move.

A block moves only when the child can compute exactly what the block computed:

* what it reads from the parent is passed as a parameter at the call: ``item()`` (a ForEach's current item),
  ``activity('X').output`` of an activity outside the block, ``variables('v')``, ``pipeline().parameters.p``; the Factory's
  Variable Library declaration is copied;
* it writes no parent variable (Set / Append variable inside), and reads no run-level value that differs in a child run
  (``pipeline().RunId``, ``TriggerTime``, ``TriggerName`` ..., ``activity('X')`` other than ``.output``).

Otherwise it stays, and the ``activity_limit_exceeded`` error stays with it: never a split that changes what the pipeline does.
``merge`` puts the child activities back, with the parameters replaced by what the call passed, so validation compares the source with
one pipeline, as it was authored.
"""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

CONTAINERS = {"ForEach": ("activities",), "Until": ("activities",), "IfCondition": ("ifTrueActivities", "ifFalseActivities"),
              "Switch": ("defaultActivities",)}
ANNOTATION = "adf2fabric:split-from:"
_RUN_LEVEL = re.compile(r"pipeline\(\)\.(RunId|TriggerTime|TriggerName|TriggerType|TriggerId|TriggeredByPipelineName|"
                        r"TriggeredByPipelineRunId|Pipeline|PipelineName|GroupId)\b", re.I)
_ACT = re.compile(r"activity\('((?:[^']|'')+)'\)(\.output)?")
_VAR = re.compile(r"variables\('((?:[^']|'')+)'\)")
_PARAM = re.compile(r"pipeline\(\)\.parameters\.([A-Za-z_][\w]*)")
_VAR_TYPES = {"string": "string", "boolean": "bool", "bool": "bool", "array": "array", "integer": "int", "int": "int"}


def count(acts: Any) -> int:
    n = 0
    for a in acts or []:
        if isinstance(a, dict):
            n += 1 + sum(count(b) for _, _, b in _blocks(a))
    return n


def _blocks(a: dict):
    """(key, case index or None, block) for each inner activity list of a container."""
    tp = a.get("typeProperties") or {}
    for key in CONTAINERS.get(a.get("type"), ()):
        if isinstance(tp.get(key), list):
            yield key, None, tp[key]
    if a.get("type") == "Switch":
        for i, case in enumerate(tp.get("cases") or []):
            if isinstance(case, dict) and isinstance(case.get("activities"), list):
                yield "cases", i, case["activities"]


def _names(acts) -> set[str]:
    out: set[str] = set()
    for a in acts or []:
        if isinstance(a, dict):
            out.add(str(a.get("name", "")).lower())
            for _, _, b in _blocks(a):
                out |= _names(b)
    return out


def _types(acts) -> set[str]:
    out: set[str] = set()
    for a in acts or []:
        if isinstance(a, dict):
            out.add(str(a.get("type")))
            for _, _, b in _blocks(a):
                out |= _types(b)
    return out


@dataclass
class Candidate:
    owner: dict  # the container activity
    key: str
    case: Optional[int]
    block: list
    in_foreach: bool
    size: int


@dataclass
class Child:
    name: str
    container: str
    branch: str
    content: dict
    activities: int


@dataclass
class Result:
    content: dict
    children: list[Child] = field(default_factory=list)
    kept: list[str] = field(default_factory=list)  # why a large block could not move


def _candidates(acts, in_foreach: bool = False):
    for a in acts or []:
        if not isinstance(a, dict):
            continue
        for key, case, block in _blocks(a):
            fe = in_foreach or a.get("type") == "ForEach"
            yield Candidate(a, key, case, block, fe, count(block))
            yield from _candidates(block, fe)


def _plan(cand: Candidate, parent: dict) -> tuple[Optional[dict], str]:
    """The parameters the child needs ({name: (type, value expression, replaced text)}), or (None, why it cannot move)."""
    text = json.dumps(cand.block)
    if _types(cand.block) & {"SetVariable", "AppendVariable"}:
        return None, "it sets a variable of the pipeline"
    if _RUN_LEVEL.search(text):
        return None, "it reads a run-level value (RunId, trigger ...) that differs in a child run"
    inside = _names(cand.block)
    params: dict[str, tuple[str, str, str]] = {}
    for m in _ACT.finditer(text):
        name = m.group(1).replace("''", "'")
        if name.lower() in inside:
            continue
        if not m.group(2):
            return None, f"it reads activity('{name}') other than its output"
        p = "out_" + re.sub(r"\W", "_", name)
        params[p] = ("object", f"@activity('{m.group(1)}').output", m.group(0))
    pvars = (parent.get("properties") or {}).get("variables") or {}
    for m in _VAR.finditer(text):
        v = m.group(1).replace("''", "'")
        kind = _VAR_TYPES.get(str((pvars.get(v) or {}).get("type", "")).lower())
        if kind is None:
            return None, f"it reads variable '{v}', whose type is not known"
        params["var_" + re.sub(r"\W", "_", v)] = (kind, f"@variables('{m.group(1)}')", m.group(0))
    if "item()" in text and "ForEach" not in _types(cand.block):  # a ForEach inside the block owns its item(); ForEach cannot nest
        if not cand.in_foreach:
            return None, "it reads item() outside a ForEach"
        params["item"] = ("object", "@item()", "item()")
    return params, ""


def _replace_refs(block: list, params: dict) -> list:
    text = json.dumps(block)
    for p, (_, _, old) in sorted(params.items(), key=lambda kv: -len(kv[1][2])):
        text = text.replace(old, f"pipeline().parameters.{p}")
    return json.loads(text)


def _unique(name: str, taken: set[str]) -> str:
    base = re.sub(r"[^A-Za-z0-9_-]", "_", name)[:200] or "child"
    out, i = base, 2
    while out.lower() in taken:
        out, i = f"{base}_{i}", i + 1
    taken.add(out.lower())
    return out


def split(name: str, content: dict, limit: int, invoke: Callable[[str, str], dict], taken: set[str]) -> Result:
    """``invoke(child_name, activity_name)``: the Invoke pipeline activity's envelope (typeProperties without parameters, connection,
    policy) from the project context; ``taken``: pipeline names already used (children get unique ones)."""
    content = copy.deepcopy(content)
    res = Result(content)
    props = content.setdefault("properties", {})
    tried: set[int] = set()
    while count(props.get("activities")) > limit:
        cands = sorted((c for c in _candidates(props.get("activities")) if c.size >= 2 and id(c.block) not in tried), key=lambda c: -c.size)
        moved = False
        for cand in cands:
            tried.add(id(cand.block))
            params, why = _plan(cand, content) if cand.size <= limit else (None, f"the block alone is over the {limit}-activity limit")
            if params is None:
                res.kept.append(f"{cand.owner.get('name')}.{cand.key}{'' if cand.case is None else f'[{cand.case}]'} ({cand.size} activities): {why}")
                continue
            # the parent's own parameters the block reads are passed through under the same name
            pdecl = props.get("parameters") or {}
            through = {m.group(1) for m in _PARAM.finditer(json.dumps(cand.block))}
            branch = cand.key if cand.case is None else f"case_{cand.case}"
            child_name = _unique(f"{name}__{cand.owner.get('name')}__{branch}", taken)
            child_params = {p: {"type": t} for p, (t, _, _) in params.items()}
            child_params.update({p: copy.deepcopy(pdecl[p]) for p in through if p in pdecl})
            child_props: dict[str, Any] = {
                "description": f"Split from pipeline '{name}' by adf2fabric: the {branch} of '{cand.owner.get('name')}' "
                               f"({cand.size} activities), because '{name}' was over Fabric's {limit}-activity limit.",
                "activities": _replace_refs(cand.block, params),
                "annotations": [f"{ANNOTATION}{name}"],
            }
            if child_params:
                child_props["parameters"] = child_params
            if props.get("libraryVariables"):
                child_props["libraryVariables"] = copy.deepcopy(props["libraryVariables"])
            call = invoke(child_name, _unique(f"Run_{child_name}", _names(props.get("activities"))))
            call["typeProperties"]["parameters"] = {
                **{p: {"value": v, "type": "Expression"} for p, (_, v, _) in params.items()},
                **{p: {"value": f"@pipeline().parameters.{p}", "type": "Expression"} for p in through if p in pdecl}}
            cand.block[:] = [call]
            res.children.append(Child(child_name, str(cand.owner.get("name")), branch, {"properties": child_props}, cand.size))
            moved = True
            break
        if not moved:
            break
    return res


def merge(content: dict, children: dict[str, dict]) -> dict:
    """The pipeline as authored: each Invoke pipeline that calls a split-off child (``children``: name -> its content) replaced by the
    child's activities, with the parameters the call passed put back in place. Used by validation, never deployed."""
    out = copy.deepcopy(content)

    def fix(acts):
        for a in acts or []:
            if not isinstance(a, dict):
                continue
            for _, _, block in _blocks(a):
                fix(block)
                if len(block) == 1 and block[0].get("type") == "InvokePipeline":
                    pid = str((block[0].get("typeProperties") or {}).get("pipelineId", ""))
                    m = re.fullmatch(r"<item:pipeline:(.+)>", pid)
                    child = children.get(m.group(1).lower()) if m else None
                    if child is None:
                        continue
                    text = json.dumps((child.get("properties") or {}).get("activities") or [])
                    for p, v in ((block[0].get("typeProperties") or {}).get("parameters") or {}).items():
                        if isinstance(v, dict) and str(v.get("value", "")).startswith("@"):
                            text = re.sub(r"pipeline\(\)\.parameters\." + re.escape(p) + r"\b", lambda _m, s=v["value"][1:]: s, text)
                    block[:] = json.loads(text)
                    fix(block)

    fix((out.get("properties") or {}).get("activities"))
    return out


def split_children(contents: dict[str, dict]) -> dict[str, dict[str, dict]]:
    """parent (lower) -> {child (lower): content}, from the children's annotation (generated files only)."""
    out: dict[str, dict[str, dict]] = {}
    for name, c in contents.items():
        for ann in ((c or {}).get("properties") or {}).get("annotations") or []:
            if isinstance(ann, str) and ann.startswith(ANNOTATION):
                out.setdefault(ann[len(ANNOTATION):].lower(), {})[name.lower()] = c
    return out
