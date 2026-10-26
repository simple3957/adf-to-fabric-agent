"""What a pipeline consists of, read iteratively from ADF source JSON or from a Fabric pipeline definition.

Both walkers use an explicit stack, never recursion, so a pipeline nested to any depth is read without a RecursionError. Each activity
becomes a ``Node``: its type, its parent container, what it waits for (``dependsOn`` with conditions), and what it uses: the pipelines it
starts, its datasets, its connections or linked services, and the items (notebooks, data flows) it runs. ``agent/structure/compare.py``
puts the ADF side and the deployed Fabric side next to each other.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

NESTED_LISTS = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")
_GUID = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")


@dataclass
class Node:
    name: str
    type: str
    parent: Optional[str]  # the container activity's name; None at the top level
    depth: int  # 0 at the top level
    branch: str = ""  # which list of the parent it sits in (activities, ifTrueActivities, cases[0], ...)
    depends: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)  # (predecessor, sorted conditions)
    calls: list[str] = field(default_factory=list)  # pipelines it starts
    datasets: list[str] = field(default_factory=list)  # ADF: dataset names. Fabric: the inline dataset's type
    connections: list[str] = field(default_factory=list)  # ADF: linked services. Fabric: connection ids (or unbound <connection:...> tokens)
    items: list[str] = field(default_factory=list)  # notebooks, data flows, Spark jobs it runs


@dataclass
class Structure:
    pipeline: str
    side: str  # adf | fabric
    nodes: list[Node] = field(default_factory=list)  # pre-order: a container, then its children
    parameters: list[str] = field(default_factory=list)
    variables: list[str] = field(default_factory=list)

    @property
    def by_type(self) -> dict[str, int]:
        return dict(sorted(Counter(n.type for n in self.nodes).items()))

    @property
    def max_depth(self) -> int:
        return max((n.depth for n in self.nodes), default=-1) + 1

    @property
    def edges(self) -> int:
        return sum(len(n.depends) for n in self.nodes)

    @property
    def called(self) -> list[str]:
        return sorted({c for n in self.nodes for c in n.calls}, key=str.lower)


def _children(a: dict) -> list[tuple[str, list]]:
    tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
    out: list[tuple[str, list]] = []
    for key in NESTED_LISTS:
        if isinstance(tp.get(key), list):
            out.append((key, tp[key]))
    cases = tp.get("cases")
    if isinstance(cases, list):
        for i, c in enumerate(cases):
            if isinstance(c, dict) and isinstance(c.get("activities"), list):
                out.append((f"cases[{i}]", c["activities"]))
    return out


def iter_activities(activities: Any) -> Iterator[tuple[dict, Optional[str], int, str]]:
    """(activity JSON, parent name, depth, branch) in source order, a container before its children. Iterative."""
    stack: list[tuple[list, Optional[str], int, str]] = [(activities if isinstance(activities, list) else [], None, 0, "")]
    while stack:
        items, parent, depth, branch = stack.pop()
        # a list is read front to back; children of item i are read before item i+1, so push the rest of the list under them
        for i, a in enumerate(items):
            if not isinstance(a, dict):
                continue
            yield a, parent, depth, branch
            kids = _children(a)
            if kids:
                stack.append((items[i + 1:], parent, depth, branch))
                for key, lst in reversed(kids):
                    stack.append((lst, str(a.get("name", "")), depth + 1, key))
                break


def _depends(a: dict) -> list[tuple[str, tuple[str, ...]]]:
    out = []
    for d in a.get("dependsOn", []) if isinstance(a.get("dependsOn"), list) else []:
        if isinstance(d, dict) and isinstance(d.get("activity"), str):
            conds = d.get("dependencyConditions")
            out.append((d["activity"], tuple(sorted(str(c) for c in conds)) if isinstance(conds, list) else ()))
    return out


def _ref(v: Any) -> Optional[str]:
    return v["referenceName"] if isinstance(v, dict) and isinstance(v.get("referenceName"), str) else None


def adf_structure(pipeline_json: dict) -> Structure:
    props = pipeline_json.get("properties") if isinstance(pipeline_json.get("properties"), dict) else {}
    s = Structure(str(pipeline_json.get("name", "")), "adf",
                  parameters=sorted(props.get("parameters", {})) if isinstance(props.get("parameters"), dict) else [],
                  variables=sorted(props.get("variables", {})) if isinstance(props.get("variables"), dict) else [])
    for a, parent, depth, branch in iter_activities(props.get("activities")):
        tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
        n = Node(str(a.get("name", "")), str(a.get("type", "")), parent, depth, branch, depends=_depends(a))
        called = _ref(tp.get("pipeline"))
        if called:
            n.calls.append(called)
        for key in ("inputs", "outputs"):
            for r in a.get(key, []) if isinstance(a.get(key), list) else []:
                if _ref(r):
                    n.datasets.append(_ref(r))  # type: ignore[arg-type]
        if _ref(tp.get("dataset")):
            n.datasets.append(_ref(tp.get("dataset")))  # type: ignore[arg-type]
        if _ref(a.get("linkedServiceName")):
            n.connections.append(_ref(a.get("linkedServiceName")))  # type: ignore[arg-type]
        for key in ("dataflow", "dataFlow", "notebook", "sparkJob"):
            if _ref(tp.get(key)):
                n.items.append(_ref(tp.get(key)))  # type: ignore[arg-type]
        if isinstance(tp.get("notebookPath"), str):
            n.items.append(tp["notebookPath"] if len(tp["notebookPath"]) < 120 else tp["notebookPath"][:117] + "...")
        s.nodes.append(n)
    return s


def _inline_datasets(tp: dict) -> list[dict]:
    found = []
    for key in ("source", "sink"):
        side = tp.get(key)
        if isinstance(side, dict) and isinstance(side.get("datasetSettings"), dict):
            found.append(side["datasetSettings"])
    if isinstance(tp.get("datasetSettings"), dict):
        found.append(tp["datasetSettings"])
    return found


def fabric_structure(pipeline_name: str, content: dict, names_by_id: Optional[dict[str, str]] = None) -> Structure:
    """``names_by_id``: lower-cased item id -> display name, to say which pipeline or notebook an id is."""
    ids = {k.lower(): v for k, v in (names_by_id or {}).items()}
    props = content.get("properties") if isinstance(content.get("properties"), dict) else {}
    s = Structure(pipeline_name, "fabric",
                  parameters=sorted(props.get("parameters", {})) if isinstance(props.get("parameters"), dict) else [],
                  variables=sorted(props.get("variables", {})) if isinstance(props.get("variables"), dict) else [])

    def name_of(v: Any) -> str:
        return ids.get(str(v).lower(), str(v)) if isinstance(v, str) else ""

    for a, parent, depth, branch in iter_activities(props.get("activities")):
        tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
        n = Node(str(a.get("name", "")), str(a.get("type", "")), parent, depth, branch, depends=_depends(a))
        if isinstance(tp.get("pipelineId"), str):
            n.calls.append(name_of(tp["pipelineId"]) or tp["pipelineId"])
        for ds in _inline_datasets(tp):
            n.datasets.append(str(ds.get("type", "?")))
            ext = ds.get("externalReferences")
            if isinstance(ext, dict) and isinstance(ext.get("connection"), str):
                n.connections.append(ext["connection"])
            ls = ds.get("linkedService")  # a Lakehouse inline connection (repointed storage)
            if isinstance(ls, dict) and isinstance((ls.get("properties") or {}).get("typeProperties"), dict):
                art = ls["properties"]["typeProperties"].get("artifactId")
                if isinstance(art, str):
                    n.connections.append(f"lakehouse:{name_of(art) or art}")
        ext = a.get("externalReferences")
        if isinstance(ext, dict) and isinstance(ext.get("connection"), str):
            n.connections.append(ext["connection"])
        for key in ("notebookId", "dataflowId", "sparkJobDefinitionId"):
            if isinstance(tp.get(key), str):
                n.items.append(name_of(tp[key]) or tp[key])
        s.nodes.append(n)
    return s


def is_bound(connection: str) -> bool:
    """A real connection id, not an unbound ``<connection:...>`` placeholder."""
    return bool(_GUID.match(connection)) or connection.startswith("lakehouse:")
