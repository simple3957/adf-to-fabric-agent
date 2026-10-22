"""Control-flow, reference and cross-artifact analysis of parsed pipelines.

Pure functions over the already-parsed :class:`AdfPipeline` records:

* :func:`analyze_pipeline` — per pipeline: duplicate names, dependency graph per scope
  (topological order, parallel stages, cycles, unresolved dependencies), expression
  reference checks, and the map of where each activity's output is consumed.
* :func:`resolve_pipelines` — across artifacts: dataset / linked-service / child-pipeline
  resolution, argument checks, cyclic pipeline calls, and recurring pipeline shapes.

Nothing here evaluates an expression or guesses a value.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterator
from typing import Optional

from adf2fabric.project.discovery import (
    AdfActivity,
    AdfActivityUsage,
    AdfDataFlow,
    AdfDataset,
    AdfDependencyEdge,
    AdfDependencyGraph,
    AdfIssueSeverity,
    AdfLinkedService,
    AdfParseIssue,
    AdfPattern,
    AdfPipeline,
)

_FORMAT_DATASETS = {"delimitedtext", "parquet", "json", "orc", "avro", "binary", "excel", "xml", "delta"}
CROSS_PIPELINE_PATTERNS = frozenset({"recurring_pipeline_shape"})


def walk(activities: list[AdfActivity]) -> Iterator[AdfActivity]:
    """Every activity, nested included, in deterministic pre-order."""
    for a in activities:
        yield a
        yield from walk(a.activities)


def add_issue(target_list: list[AdfParseIssue], issue: AdfParseIssue) -> None:
    """Append unless an identical issue is already present (keeps re-runs idempotent)."""
    key = (issue.code, issue.property_path, issue.message)
    if not any((i.code, i.property_path, i.message) == key for i in target_list):
        target_list.append(issue)


def _warn(act: AdfActivity, code: str, message: str, path: Optional[str] = None,
          severity: AdfIssueSeverity = AdfIssueSeverity.warning) -> None:
    add_issue(act.warnings, AdfParseIssue(code=code, severity=severity, message=message, property_path=path))


# --- per-pipeline analysis ---------------------------------------------------------------------------


def analyze_pipeline(pipeline: AdfPipeline, ctx) -> None:
    """Stage 6: dependency + control-flow analysis and reference checks (in place)."""
    flat = list(walk(pipeline.activities))
    by_id = {a.activity_id: a for a in flat}

    # Activity names are unique pipeline-wide (case-insensitively) in ADF.
    seen: dict[str, AdfActivity] = {}
    for a in flat:
        if not a.name:
            continue
        low = a.name.lower()
        if low in seen:
            add_issue(ctx.issues, AdfParseIssue(
                code="duplicate_activity_name", severity=AdfIssueSeverity.error,
                message=f"Activity name '{a.name}' is used more than once in the pipeline "
                        f"({seen[low].activity_id} and {a.activity_id}).",
                property_path=a.source_location.pointer if a.source_location else None))
        else:
            seen[low] = a

    # Dependency graph, one scope per (parent, branch) list.
    scopes: dict[tuple[Optional[str], Optional[str]], list[AdfActivity]] = {}
    for a in flat:
        scopes.setdefault((a.parent_activity_id, a.branch), []).append(a)
    for members in scopes.values():
        _analyze_scope(members, seen, ctx)

    # Reference checks + consumers.
    upstream_cache: dict[str, set[str]] = {}
    for a in flat:
        chain = _ancestors(a, by_id)
        for e in a.expressions:
            for ref in e.references:
                _check_reference(a, e, ref, seen, by_id, pipeline, chain, flat, upstream_cache)
    for a in flat:
        a.output_consumers = _dedupe_usage(a.output_consumers)

    pipeline.dependency_graph = build_dependency_graph(pipeline.name, flat, scopes, seen)
    pipeline.issues = list(ctx.issues)


def _scope_key(pipeline_name: str, parent_id: Optional[str], branch: Optional[str]) -> str:
    return f"{parent_id or pipeline_name}|{branch or 'activities'}"


def build_dependency_graph(pipeline_name: str, flat: list[AdfActivity],
                           scopes: dict[tuple[Optional[str], Optional[str]], list[AdfActivity]],
                           all_names: dict[str, AdfActivity]) -> AdfDependencyGraph:
    """The explicit dependency graph: one node per activity (nested included), one edge per ``dependsOn`` entry.

    ADF dependencies live inside one activities list (a scope) and never cross a container boundary, so each scope is
    its own graph. An entry that names an activity not in its scope is kept as an unresolved edge, not dropped.
    ``cycles`` holds the activities of every strongly connected component with more than one member, and every
    self-dependency: exactly the activities that can never start.
    """
    edges: list[AdfDependencyEdge] = []
    cycles: list[list[str]] = []
    for (parent_id, branch), members in scopes.items():
        scope = _scope_key(pipeline_name, parent_id, branch)
        index = {a.name.lower(): i for i, a in enumerate(members) if a.name}
        succ: dict[int, set[int]] = {i: set() for i in range(len(members))}
        self_loops: set[int] = set()
        for i, a in enumerate(members):
            for d in a.depends_on:
                j = index.get(d.activity.lower())
                base = dict(scope=scope, from_name=d.activity, to_id=a.activity_id, conditions=list(d.conditions),
                            unknown_conditions=list(d.unknown_conditions))
                if j is None:
                    problem = "outside_scope" if d.activity.lower() in all_names else "unresolved"
                    edges.append(AdfDependencyEdge(from_id=None, resolved=False, problem=problem, **base))
                elif j == i:
                    self_loops.add(i)
                    edges.append(AdfDependencyEdge(from_id=a.activity_id, resolved=False, problem="self", **base))
                else:
                    succ[j].add(i)
                    edges.append(AdfDependencyEdge(from_id=members[j].activity_id, **base))
        for component in _components(succ):
            if len(component) > 1:
                cycles.append(sorted(members[i].activity_id for i in component))
        cycles.extend([members[i].activity_id] for i in sorted(self_loops))
    edges.sort(key=lambda e: (e.scope, e.to_id, e.from_name.lower()))
    return AdfDependencyGraph(nodes=[a.activity_id for a in flat], edges=edges, cycles=sorted(cycles),
                              scope_count=len(scopes))


def _components(succ: dict[int, set[int]]) -> list[list[int]]:
    """Strongly connected components (Tarjan), iterative so a very long chain cannot hit the recursion limit."""
    index_of: dict[int, int] = {}
    low: dict[int, int] = {}
    on_stack: set[int] = set()
    stack: list[int] = []
    out: list[list[int]] = []
    counter = 0
    for root in sorted(succ):
        if root in index_of:
            continue
        work: list[tuple[int, list[int]]] = [(root, sorted(succ[root], reverse=True))]
        index_of[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, todo = work[-1]
            if todo:
                nxt = todo.pop()
                if nxt not in index_of:
                    index_of[nxt] = low[nxt] = counter
                    counter += 1
                    stack.append(nxt)
                    on_stack.add(nxt)
                    work.append((nxt, sorted(succ[nxt], reverse=True)))
                elif nxt in on_stack:
                    low[node] = min(low[node], index_of[nxt])
            else:
                work.pop()
                if work:
                    parent = work[-1][0]
                    low[parent] = min(low[parent], low[node])
                if low[node] == index_of[node]:
                    component = []
                    while True:
                        member = stack.pop()
                        on_stack.discard(member)
                        component.append(member)
                        if member == node:
                            break
                    out.append(sorted(component))
    return out


def _analyze_scope(members: list[AdfActivity], all_names: dict[str, AdfActivity], ctx) -> None:
    index = {a.name.lower(): i for i, a in enumerate(members) if a.name}
    deps: dict[int, set[int]] = {i: set() for i in range(len(members))}
    for i, a in enumerate(members):
        for d in a.depends_on:
            j = index.get(d.activity.lower())
            if j is None:
                d.resolved = False
                elsewhere = d.activity.lower() in all_names
                _warn(a, "dependency_outside_scope" if elsewhere else "unresolved_dependency",
                      (f"Depends on '{d.activity}', which exists but not in the same activities list; "
                       "dependencies cannot cross container boundaries.") if elsewhere else
                      f"Depends on '{d.activity}', which is not defined.", "dependsOn",
                      AdfIssueSeverity.error)
            elif j == i:
                _warn(a, "self_dependency", "Activity depends on itself.", "dependsOn", AdfIssueSeverity.error)
            else:
                deps[i].add(j)
                members[j].dependents.append(a.name)

    indeg = {i: len(d) for i, d in deps.items()}
    succ: dict[int, list[int]] = {i: [] for i in deps}
    for i, d in deps.items():
        for j in d:
            succ[j].append(i)
    ready = [i for i, n in indeg.items() if n == 0]
    heapq.heapify(ready)
    order = 0
    stage: dict[int, int] = {}
    while ready:
        i = heapq.heappop(ready)
        stage[i] = 1 + max((stage[j] for j in deps[i]), default=-1)
        members[i].execution_order = order
        members[i].stage = stage[i]
        order += 1
        for k in succ[i]:
            indeg[k] -= 1
            if indeg[k] == 0:
                heapq.heappush(ready, k)
    stuck = [members[i] for i in deps if i not in stage]
    if stuck:
        names = ", ".join(a.name for a in stuck)
        add_issue(ctx.issues, AdfParseIssue(
            code="cyclic_dependency", severity=AdfIssueSeverity.error,
            message=f"Activities form or depend on a dependency cycle: {names}."))
        for a in stuck:
            _warn(a, "cyclic_dependency", "Part of, or downstream of, a dependency cycle; "
                  "no execution order exists.", "dependsOn", AdfIssueSeverity.error)
    for a in members:
        a.dependents = sorted(set(a.dependents), key=lambda n: index.get(n.lower(), 0))


def _ancestors(act: AdfActivity, by_id: dict[str, AdfActivity]) -> list[AdfActivity]:
    out, cur = [], act
    while cur.parent_activity_id and cur.parent_activity_id in by_id:
        cur = by_id[cur.parent_activity_id]
        out.append(cur)
    return out


def _upstream(act: AdfActivity, by_id: dict[str, AdfActivity], siblings_cache: dict) -> set[str]:
    """Lowercase names of every activity that must complete before ``act`` starts."""
    key = act.activity_id
    if key in siblings_cache:
        return siblings_cache[key]
    result: set[str] = set()
    scope = [a for a in by_id.values()
             if a.parent_activity_id == act.parent_activity_id and a.branch == act.branch]
    by_name = {a.name.lower(): a for a in scope if a.name}
    stack = [d.activity.lower() for d in act.depends_on]
    while stack:
        n = stack.pop()
        if n in result:
            continue
        result.add(n)
        target = by_name.get(n)
        if target:
            stack.extend(d.activity.lower() for d in target.depends_on)
    if act.parent_activity_id and act.parent_activity_id in by_id:
        result |= _upstream(by_id[act.parent_activity_id], by_id, siblings_cache)
    siblings_cache[key] = result
    return result


def _check_reference(act, expr, ref, names, by_id, pipeline, ancestors, flat, cache) -> None:
    path = expr.property_path

    if ref.kind == "activity_output" and ref.name:
        target = names.get(ref.name.lower())
        if target is None:
            _warn(act, "unresolved_activity_reference",
                  f"Expression reads the output of activity '{ref.name}', which is not defined.", path)
            return
        if target is act:
            _warn(act, "self_reference", "Expression reads this activity's own output.", path)
        elif ref.name.lower() not in _upstream(act, by_id, cache):
            _warn(act, "activity_reference_not_upstream",
                  f"Expression reads the output of '{ref.name}', which this activity does not depend on.", path)
        target.output_consumers.append(AdfActivityUsage(
            activity_id=act.activity_id, property_path=path, via="activity_output", detail=ref.path or ""))
    elif ref.kind == "variable" and ref.name:
        declared = {v.name.lower() for v in pipeline.variables}
        if ref.name.lower() not in declared:
            _warn(act, "undeclared_variable", f"Expression reads variable '{ref.name}', which is not declared.", path)
        for w in flat:
            if any(v.lower() == ref.name.lower() for v in w.variables_written) and w is not act:
                w.output_consumers.append(AdfActivityUsage(
                    activity_id=act.activity_id, property_path=path, via="variable", detail=ref.name))
    elif ref.kind == "pipeline_parameter" and ref.name:
        if ref.name.lower() not in {p.name.lower() for p in pipeline.parameters}:
            _warn(act, "undeclared_parameter",
                  f"Expression reads pipeline parameter '{ref.name}', which is not declared.", path)
    elif ref.kind == "loop_item":
        in_loop = any((a.type or "").lower() == "foreach" for a in ancestors)
        in_filter = (act.type or "").lower() == "filter" and path.startswith("typeProperties.condition")
        if not (in_loop or in_filter):
            _warn(act, "item_outside_loop", "item() is used outside a ForEach (or Filter condition).", path)


def _dedupe_usage(items: list[AdfActivityUsage]) -> list[AdfActivityUsage]:
    seen, out = set(), []
    for u in sorted(items, key=lambda u: (u.activity_id, u.property_path, u.via, u.detail)):
        k = (u.activity_id, u.property_path, u.via, u.detail)
        if k not in seen:
            seen.add(k)
            out.append(u)
    return out


# --- cross-artifact resolution (stage 7) ----------------------------------------------------------------


def resolve_pipelines(pipelines: list[AdfPipeline], datasets: list[AdfDataset],
                      linked_services: list[AdfLinkedService], dataflows: "list[AdfDataFlow] | tuple" = ()) -> None:
    ds_by = {d.name.lower(): d for d in datasets}
    ls_by = {s.name.lower(): s for s in linked_services}
    pl_by = {p.name.lower(): p for p in pipelines}
    df_by = {f.name.lower(): f for f in dataflows}

    for p in pipelines:
        for act in walk(p.activities):
            for io in [*act.inputs, *act.outputs]:
                _resolve_io(act, io, ds_by, ls_by)
                if io.kind == "dataflow" and io.name:
                    io.resolved = io.name.lower() in df_by
            for r in act.references:
                table = {"pipeline": pl_by, "dataset": ds_by, "linkedService": ls_by, "dataflow": df_by}.get(r.kind)
                if table is not None:
                    r.resolved = r.name.lower() in table
                    if r.kind == "dataflow" and not r.resolved:
                        _warn(act, "unresolved_dataflow",
                              f"Data flow '{r.name}' is not among the discovered artifacts.", r.role)
            for name in act.linked_service_refs:
                ls = ls_by.get(name.lower())
                act.configuration["linked_service"] = {
                    "name": name, "type": ls.type if ls else None, "resolved": ls is not None}
                if ls is None:
                    _warn(act, "unresolved_linked_service",
                          f"Linked service '{name}' is not among the discovered artifacts.", "linkedServiceName")
            if (act.type or "").lower() == "executepipeline":
                _resolve_child(act, pl_by)

    _resolve_dataflows(dataflows, ds_by, ls_by, df_by)
    _pipeline_cycles(pipelines, pl_by)
    _recurring_shapes(pipelines)
    for p in pipelines:
        p.issues = sorted(p.issues, key=lambda i: (i.severity.value, i.code, i.message))


def _resolve_dataflows(dataflows, ds_by, ls_by, df_by) -> None:
    """Resolve what each data flow references, and detect flowlets that (directly or not) embed themselves."""
    for f in dataflows:
        for name in f.dataset_refs:
            if name.lower() not in ds_by:
                add_issue(f.issues, AdfParseIssue(
                    code="unresolved_dataset", message=f"Dataset '{name}' is not among the discovered artifacts."))
        for name in f.linked_service_refs:
            if name.lower() not in ls_by:
                add_issue(f.issues, AdfParseIssue(
                    code="unresolved_linked_service",
                    message=f"Linked service '{name}' is not among the discovered artifacts."))
        for name in f.flowlet_refs:
            if name.lower() not in df_by:
                add_issue(f.issues, AdfParseIssue(
                    code="unresolved_flowlet", message=f"Flowlet '{name}' is not among the discovered artifacts."))
    graph = {f.name.lower(): sorted({r.lower() for r in f.flowlet_refs if r.lower() in df_by}) for f in dataflows}
    state: dict[str, int] = {}
    in_cycle: set[str] = set()

    def dfs(n: str, stack: list[str]) -> None:
        state[n] = 1
        stack.append(n)
        for m in graph.get(n, []):
            if state.get(m) == 1:
                in_cycle.update(stack[stack.index(m):])
            elif state.get(m) is None:
                dfs(m, stack)
        stack.pop()
        state[n] = 2

    for n in sorted(graph):
        if state.get(n) is None:
            dfs(n, [])
    for f in dataflows:
        if f.name.lower() in in_cycle:
            add_issue(f.issues, AdfParseIssue(
                code="cyclic_flowlet_reference", severity=AdfIssueSeverity.error,
                message=f"Data flow '{f.name}' takes part in a cycle of flowlet references."))
        f.issues = sorted(f.issues, key=lambda i: (i.severity.value, i.code, i.message))


def _resolve_io(act: AdfActivity, io, ds_by, ls_by) -> None:
    if io.kind != "dataset" or not io.name:
        return
    ds = ds_by.get(io.name.lower())
    io.resolved = ds is not None
    if ds is None:
        _warn(act, "unresolved_dataset", f"Dataset '{io.name}' is not among the discovered artifacts.",
              f"{io.role}:{io.name}")
        return
    io.dataset_type = ds.type
    if ds.type and ds.type.lower() in _FORMAT_DATASETS and not io.format:
        io.format = ds.type
    if ds.columns:
        io.declared_columns = len(ds.columns)
    if ds.asset is not None:
        a = ds.asset
        if a.sql_table or a.sql_schema:
            io.location.setdefault("table", ".".join(x for x in (a.sql_schema, a.sql_table) if x))
        if a.adls_filesystem or a.adls_path:
            io.location.setdefault("path", "/".join(x for x in (a.adls_filesystem, a.adls_path) if x))
        if a.rest_relative_url:
            io.location.setdefault("relative_url", a.rest_relative_url)
        if a.is_expression_driven:
            io.location["location_is_dynamic"] = True
    if ds.linked_service_ref is not None:
        io.linked_service = ds.linked_service_ref.name
        ls = ls_by.get(ds.linked_service_ref.name.lower())
        if ls is not None:
            io.connection_type = ls.type
        else:
            _warn(act, "unresolved_linked_service",
                  f"Dataset '{ds.name}' uses linked service '{ds.linked_service_ref.name}', which is not "
                  "among the discovered artifacts.", f"{io.role}:{io.name}")
    declared = {p.name.lower(): p for p in ds.parameters}
    supplied = {k.lower(): k for k in io.parameters}
    for low, p in sorted(declared.items()):
        if low not in supplied and p.default_value is None and not p.is_expression:
            _warn(act, "dataset_parameter_not_supplied",
                  f"Dataset '{ds.name}' parameter '{p.name}' has no default and is not supplied.",
                  f"{io.role}:{io.name}")
    for low, k in sorted(supplied.items()):
        if low not in declared:
            _warn(act, "dataset_parameter_undeclared",
                  f"Argument '{k}' is passed to dataset '{ds.name}', which declares no such parameter.",
                  f"{io.role}:{io.name}")


def _resolve_child(act: AdfActivity, pl_by: dict[str, AdfPipeline]) -> None:
    name = act.configuration.get("pipeline")
    if not name:
        return
    child = pl_by.get(name.lower())
    if child is None:
        _warn(act, "unresolved_child_pipeline",
              f"Executes pipeline '{name}', which is not among the discovered artifacts.", "typeProperties.pipeline")
        return
    supplied = {k.lower(): k for k in (act.configuration.get("parameters") or {})}
    declared = {p.name.lower(): p for p in child.parameters}
    for low, p in sorted(declared.items()):
        if low not in supplied and p.default_value is None and not p.is_expression:
            _warn(act, "child_parameter_not_supplied",
                  f"Child pipeline '{child.name}' parameter '{p.name}' has no default and is not supplied.",
                  "typeProperties.parameters")
    for low, k in sorted(supplied.items()):
        if low not in declared:
            _warn(act, "child_parameter_undeclared",
                  f"Argument '{k}' is passed to '{child.name}', which declares no such parameter.",
                  "typeProperties.parameters")


def _pipeline_cycles(pipelines: list[AdfPipeline], pl_by: dict[str, AdfPipeline]) -> None:
    graph = {p.name.lower(): sorted({d.lower() for d in p.pipeline_dependencies if d.lower() in pl_by})
             for p in pipelines}
    state: dict[str, int] = {}
    in_cycle: set[str] = set()

    def dfs(n: str, stack: list[str]) -> None:
        state[n] = 1
        stack.append(n)
        for m in graph.get(n, []):
            if state.get(m) == 1:
                in_cycle.update(stack[stack.index(m):])
            elif state.get(m) is None:
                dfs(m, stack)
        stack.pop()
        state[n] = 2

    for n in sorted(graph):
        if state.get(n) is None:
            dfs(n, [])
    for n in sorted(in_cycle):
        p = pl_by[n]
        add_issue(p.issues, AdfParseIssue(
            code="cyclic_pipeline_reference", severity=AdfIssueSeverity.error,
            message=f"Pipeline '{p.name}' takes part in a cycle of ExecutePipeline calls."))


def _signature(activities: list[AdfActivity], depth: int = 0) -> list[str]:
    out: list[str] = []
    for a in activities:
        out.append(f"{depth}:{a.type or '?'}")
        out.extend(_signature(a.activities, depth + 1))
    return out


def _recurring_shapes(pipelines: list[AdfPipeline]) -> None:
    for p in pipelines:
        p.patterns = [x for x in p.patterns if x.name not in CROSS_PIPELINE_PATTERNS]
    groups: dict[tuple, list[AdfPipeline]] = {}
    for p in pipelines:
        sig = tuple(_signature(p.activities))
        if len(sig) >= 2:
            groups.setdefault(sig, []).append(p)
    for sig, members in groups.items():
        if len(members) < 2:
            continue
        names = sorted(m.name for m in members)
        shape = " > ".join(s.split(":", 1)[1] if s.startswith("0:") else "(" + s.split(":", 1)[1] + ")"
                           for s in sig)
        for m in members:
            m.patterns.append(AdfPattern(
                name="recurring_pipeline_shape",
                description="Pipelines with an identical activity structure (likely one template).",
                activities=[],
                evidence=[f"shared structure: {shape}", f"pipelines: {', '.join(names)}"]))
