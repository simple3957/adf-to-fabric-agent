"""Read the source files and the generated artifacts straight from disk into comparable views.

Independence: nothing here uses the converter's records or success flags, and nothing here uses the semantic discovery parser. The
raw JSON is walked by a small, separate piece of code, so a defect in the converter or in discovery cannot hide itself by being
trusted twice. The only shared knowledge is reference data (the documented Fabric activity set and function set).

Stable identities (used everywhere in the report):
  pipeline:<P>   pipeline:<P>/activity:<A>   dataset:<D>   linked_service:<L>   trigger:<T>   integration_runtime:<I>
  dataflow:<F>   global_parameter:<G>
Activity names are unique within a pipeline, so ``pipeline:P/activity:A`` is stable whatever the nesting.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

import yaml

SOURCE_FOLDERS = {"pipeline": "pipeline", "dataset": "dataset", "linkedService": "linked_service", "trigger": "trigger",
                  "integrationRuntime": "integration_runtime", "dataflow": "dataflow", "factory": "factory"}
_NESTED_LISTS = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")
_GP = re.compile(r"pipeline\(\)\.globalParameters\.([A-Za-z_]\w*)")


@dataclass
class RawFile:
    kind: str  # pipeline | dataset | ...
    name: str  # the artifact's own name (from the JSON; the file name when unreadable)
    path: str  # relative to the source root
    data: Optional[dict] = None
    error: str = ""  # why it could not be read


@dataclass
class SourceScan:
    root: Optional[Path]
    files: list[RawFile] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    def of(self, kind: str) -> list[RawFile]:
        return [f for f in self.files if f.kind == kind]

    def names(self, kind: str) -> dict[str, RawFile]:
        return {f.name.lower(): f for f in self.of(kind) if f.data is not None}


def find_root(source_dir: Path) -> Optional[Path]:
    """The directory that holds the ADF folders (pipeline/, dataset/ ...): the source root, a subdirectory of it, or the extracted publish."""
    candidates = [source_dir, source_dir / "_adf_publish_extracted"]
    candidates += [p for p in sorted(source_dir.glob("*")) if p.is_dir() and not p.name.startswith((".", "_"))]
    for c in candidates:
        if c.is_dir() and any((c / f).is_dir() for f in ("pipeline", "dataset", "linkedService")):
            return c
    return None


def scan_source(source_dir: Path) -> SourceScan:
    root = find_root(source_dir) if source_dir.is_dir() else None
    scan = SourceScan(root=root)
    if root is None:
        scan.problems.append(f"No ADF folders (pipeline/, dataset/, linkedService/ ...) were found under {source_dir.name or source_dir}.")
        return scan
    for folder, kind in SOURCE_FOLDERS.items():
        d = root / folder
        if not d.is_dir():
            continue
        for f in sorted(d.rglob("*.json")):
            rel = f.relative_to(root).as_posix()
            try:
                data = json.loads(f.read_text(encoding="utf-8-sig"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                scan.files.append(RawFile(kind, f.stem, rel, None, f"{type(exc).__name__}"))
                continue
            if not isinstance(data, dict):
                scan.files.append(RawFile(kind, f.stem, rel, None, "not a JSON object"))
                continue
            name = data.get("name") if isinstance(data.get("name"), str) else f.stem
            scan.files.append(RawFile(kind, name, rel, data))
    return scan


# --- activities ----------------------------------------------------------------------------------------------------


@dataclass
class ActView:
    name: str
    type: str
    parent: Optional[str]  # the container activity's name, None at the top level
    depends: dict[str, tuple[str, ...]]  # predecessor name (lower) -> sorted conditions
    type_properties: dict
    policy: dict
    raw: dict


def _kids(a: dict) -> Iterator[tuple[str, list]]:
    tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
    for key in _NESTED_LISTS:
        v = tp.get(key)
        if isinstance(v, list):
            yield key, v
    cases = tp.get("cases")
    if isinstance(cases, list):
        for i, c in enumerate(cases):
            if isinstance(c, dict) and isinstance(c.get("activities"), list):
                yield f"cases[{i}]", c["activities"]


def walk_activities(activities: Any, parent: Optional[str] = None) -> Iterator[ActView]:
    """Pre-order, a container before its children. Iterative: a pipeline nested to any depth cannot raise RecursionError."""
    stack: list[tuple[list, Optional[str]]] = [(activities if isinstance(activities, list) else [], parent)]
    while stack:
        items, par = stack.pop()
        for i, a in enumerate(items):
            if not isinstance(a, dict):
                continue
            name = str(a.get("name", ""))
            deps: dict[str, tuple[str, ...]] = {}
            for d in a.get("dependsOn", []) if isinstance(a.get("dependsOn"), list) else []:
                if isinstance(d, dict) and isinstance(d.get("activity"), str):
                    conds = d.get("dependencyConditions")
                    deps[d["activity"].lower()] = tuple(sorted(str(c) for c in conds)) if isinstance(conds, list) else ()
            yield ActView(name, str(a.get("type", "")), par, deps, a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {},
                          a.get("policy") if isinstance(a.get("policy"), dict) else {}, a)
            kids = [k for _, k in _kids(a)]
            if kids:
                stack.append((items[i + 1:], par))
                for k in reversed(kids):
                    stack.append((k, name))
                break


def pipeline_activities(content: dict) -> list[ActView]:
    props = content.get("properties") if isinstance(content.get("properties"), dict) else {}
    return list(walk_activities(props.get("activities")))


def expressions(node: Any, path: str = "") -> Iterator[tuple[str, str]]:
    """(property path, expression text) for every dynamic-content value: an Expression object or a string starting with '@'."""
    if isinstance(node, dict):
        if node.get("type") == "Expression" and isinstance(node.get("value"), str):
            yield path, node["value"]
            return
        for k, v in node.items():
            yield from expressions(v, f"{path}.{k}" if path else str(k))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from expressions(v, f"{path}[{i}]")
    elif isinstance(node, str) and (node.startswith("@") and not node.startswith("@@") or "@{" in node):
        yield path, node


def own_properties(a: dict) -> dict:
    """The activity's JSON without the activities nested inside it: a container's own settings, not its children's (those are compared as activities)."""
    out = json.loads(json.dumps(a))
    tp = out.get("typeProperties")
    if isinstance(tp, dict):
        for key in _NESTED_LISTS:
            tp.pop(key, None)
        if isinstance(tp.get("cases"), list):
            for case in tp["cases"]:
                if isinstance(case, dict):
                    case.pop("activities", None)
    return out


def normalise_expression(text: str) -> str:
    """Remove differences Fabric requires so equal meaning compares equal: global parameters become pipeline parameters gp_<name>."""
    out = _GP.sub(lambda m: f"pipeline().parameters.gp_{m.group(1)}", text)
    return re.sub(r"\s+", " ", out).strip()


# --- the generated artifacts -----------------------------------------------------------------------------------------


@dataclass
class TargetPipeline:
    name: str  # the pipeline's Fabric name (folder name without .DataPipeline)
    path: str  # relative to the conversion directory
    content: Optional[dict] = None
    error: str = ""


@dataclass
class TargetScan:
    pipelines: list[TargetPipeline] = field(default_factory=list)
    schedules: list[dict] = field(default_factory=list)
    name_map: dict[str, str] = field(default_factory=dict)  # lower source pipeline name -> artifact path (from conversion.yaml, names only)
    problems: list[str] = field(default_factory=list)


def scan_target(conversion_dir: Path) -> TargetScan:
    scan = TargetScan()
    pdir = conversion_dir / "pipelines"
    if not pdir.is_dir():
        scan.problems.append("No generated pipelines were found (conversion/pipelines is missing).")
        return scan
    for d in sorted(pdir.glob("*.DataPipeline")):
        f = d / "pipeline-content.json"
        name = d.name[: -len(".DataPipeline")]
        rel = f.relative_to(conversion_dir).as_posix()
        try:
            data = json.loads(f.read_text(encoding="utf-8-sig"))
            scan.pipelines.append(TargetPipeline(name, rel, data if isinstance(data, dict) else None, "" if isinstance(data, dict) else "not a JSON object"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            scan.pipelines.append(TargetPipeline(name, rel, None, type(exc).__name__))
    sdir = conversion_dir / "schedules"
    for f in sorted(sdir.glob("*.json")) if sdir.is_dir() else []:
        try:
            data = json.loads(f.read_text(encoding="utf-8-sig"))
            if isinstance(data, dict):
                scan.schedules.append(data)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            scan.problems.append(f"schedule file {f.name} is not valid JSON")
    # which generated file belongs to which source pipeline: the conversion record, read for NAMES only (never for success flags)
    cy = conversion_dir / "conversion.yaml"
    if cy.is_file():
        try:
            doc = yaml.load(cy.read_text(encoding="utf-8"), Loader=getattr(yaml, "CSafeLoader", yaml.SafeLoader)) or {}  # nosec B506: a safe loader
            for o in doc.get("pipelines", []) if isinstance(doc, dict) else []:
                if isinstance(o, dict) and o.get("source_pipeline") and o.get("artifact_path"):
                    scan.name_map[str(o["source_pipeline"]).lower()] = str(o["artifact_path"])
        except Exception:  # noqa: BLE001
            scan.problems.append("conversion.yaml could not be read; generated files are paired with source pipelines by file name")
    return scan


def pair_pipelines(src: SourceScan, tgt: TargetScan) -> tuple[dict[str, TargetPipeline], list[str]]:
    """source pipeline name (lower) -> generated pipeline, and the source pipelines with no generated file."""
    by_path = {p.path: p for p in tgt.pipelines}
    by_name = {p.name.lower(): p for p in tgt.pipelines}
    paired: dict[str, TargetPipeline] = {}
    missing: list[str] = []
    for f in src.of("pipeline"):
        low = f.name.lower()
        hit = by_path.get(tgt.name_map.get(low, "").replace("\\", "/")) or by_name.get(low)
        if hit is None:
            missing.append(f.name)
        else:
            paired[low] = hit
    return paired, missing
