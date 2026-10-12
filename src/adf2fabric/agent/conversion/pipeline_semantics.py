"""Pipeline semantics: how activities interact, and whether the generated pipeline preserves it.

Activity conversion says what each activity becomes. This module covers the other half:

1. ``outcome_notes`` - how success/failure paths decide the pipeline's result in the SOURCE, using
   Microsoft's documented ADF rule (azure/data-factory/tutorial-pipeline-failure-error-handling):
   the outcome of every leaf activity is evaluated (a skipped leaf is evaluated through its
   parent) and the pipeline succeeds if and only if all evaluated nodes succeed. Documented
   patterns are recognised per activity: Try-Catch (only an Upon-Failure path), Do-If-Else
   (Failure + Success paths) and Do-If-Skip-Else (those plus an Upon-Skip path).

2. ``verify_graph`` - an independent check that the assembled Fabric pipeline reproduces the
   source dependency graph. It re-reads the GENERATED JSON and compares it with the source
   ``dependsOn`` edges, so a bug in the assembler cannot hide behind the engine's own records:

   * every source activity has at least one generated component,
   * for each source edge ``A dependsOn B [conditions]``, every entry component of A depends on
     every exit component of B with exactly those conditions,
   * no edge exists between two different source activities that the source does not have.

Because the dependency conditions and edges are preserved, the pipeline result follows the same
rule only if Fabric applies the same leaf-evaluation rule as ADF; this module checks the graph,
it does not execute anything, and the Fabric rule itself is not verified here.
"""

from __future__ import annotations

from typing import Any, Iterator, Optional

from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.project.conversion import ActivityConversion, ConversionIssue, Impact, IssueSeverity
from adf2fabric.project.discovery import AdfActivity, AdfPipeline

_CHILD_KEYS = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")


# --- 1. source outcome semantics ---------------------------------------------------------------------------


def _scopes(p: AdfPipeline) -> dict[tuple[Optional[str], Optional[str]], list[AdfActivity]]:
    scopes: dict[tuple[Optional[str], Optional[str]], list[AdfActivity]] = {}
    for a in walk(p.activities):
        scopes.setdefault((a.parent_activity_id, a.branch), []).append(a)
    return scopes


def _pattern(conds_by_dependent: list[set[str]]) -> Optional[str]:
    seen = set().union(*conds_by_dependent) if conds_by_dependent else set()
    if "Completed" in seen:
        return "best-effort (Upon Completion)"
    if "Failed" in seen and "Succeeded" in seen and "Skipped" in seen:
        return "Do-If-Skip-Else"
    if "Failed" in seen and "Succeeded" in seen:
        return "Do-If-Else"
    if "Failed" in seen:
        return "Try-Catch"
    return None


_PATTERN_RESULT = {
    "Try-Catch": "the pipeline still succeeds when '{n}' fails if its failure path succeeds",
    "Do-If-Else": "the pipeline FAILS when '{n}' fails, even if its failure path succeeds",
    "Do-If-Skip-Else": "the pipeline succeeds when '{n}' fails if its failure path succeeds",
    "best-effort (Upon Completion)": "'{n}' failing does not stop its Upon-Completion dependents",
}


def outcome_notes(p: AdfPipeline) -> list[str]:
    """Human-readable outcome semantics of the source pipeline (one line per fact, deterministic)."""
    notes: list[str] = []
    for (parent, branch), members in sorted(_scopes(p).items(), key=lambda kv: (kv[0][0] or "", kv[0][1] or "")):
        names = {m.name.lower(): m for m in members}
        dependents: dict[str, list[set[str]]] = {n: [] for n in names}
        for m in members:
            for d in m.depends_on:
                if d.activity.lower() in dependents:
                    dependents[d.activity.lower()].append(set(d.conditions))
        where = "pipeline" if parent is None else f"{branch or 'activities'} of {next((a.name for a in walk(p.activities) if a.activity_id == parent), parent)}"
        leaves = sorted(names[n].name for n, deps in dependents.items() if not deps)
        notes.append(f"Outcome ({where}): decided by leaf activit{'y' if len(leaves) == 1 else 'ies'} "
                     f"{', '.join(leaves)}; a skipped leaf is judged by its parent.")
        for n, conds in dependents.items():
            pat = _pattern(conds)
            if pat:
                notes.append(f"Outcome ({where}): '{names[n].name}' uses {pat} - "
                             + _PATTERN_RESULT[pat].format(n=names[n].name) + ".")
    return notes


# --- 2. generated-graph verification -----------------------------------------------------------------------


def _generated(content: dict) -> dict[str, dict]:
    index: dict[str, dict] = {}

    def visit(acts: list) -> Iterator[dict]:
        for a in acts or []:
            if not isinstance(a, dict):
                continue
            yield a
            tp = a.get("typeProperties") or {}
            for key in _CHILD_KEYS:
                yield from visit(tp.get(key))
            for case in tp.get("cases") or []:
                yield from visit((case or {}).get("activities"))

    for a in visit((content.get("properties") or {}).get("activities")):
        index.setdefault(a.get("name", ""), a)
    return index


def _issue(code: str, message: str, action: str, severity: IssueSeverity = IssueSeverity.error) -> ConversionIssue:
    return ConversionIssue(code=code, severity=severity, impact=Impact.none, message=message, action=action)


def verify_graph(p: AdfPipeline, records: list[ActivityConversion], content: dict) -> list[ConversionIssue]:
    """Compare the source dependency graph with the generated pipeline JSON."""
    gen = _generated(content)
    rec = {r.source_name.lower(): r for r in records}
    owner: dict[str, str] = {}  # generated component name -> source activity name
    issues: list[ConversionIssue] = []

    for r in records:
        for c in r.components:
            owner.setdefault(c.name, r.source_name)
        if not any(c.name in gen for c in r.components):
            issues.append(_issue("graph_activity_missing",
                                 f"Source activity '{r.source_name}' has no component in the generated pipeline.",
                                 "Re-run the conversion; the assembler dropped it."))

    source_edges: set[tuple[str, str]] = set()  # (dependent, dependency) as lower-case source names
    for a in walk(p.activities):
        mine = rec.get(a.name.lower())
        for d in a.depends_on:
            dep = rec.get(d.activity.lower())
            source_edges.add((a.name.lower(), d.activity.lower()))
            if mine is None or dep is None or not d.resolved:
                continue  # unresolved in the source; the engine already reports it
            want = sorted(d.conditions) if d.conditions and not d.unknown_conditions else None
            for entry in mine.entry_components:
                have = {x.get("activity"): sorted(x.get("dependencyConditions") or [])
                        for x in (gen.get(entry, {}).get("dependsOn") or [])}
                for exit_ in dep.exit_components:
                    if exit_ not in have:
                        issues.append(_issue(
                            "graph_edge_missing",
                            f"'{a.name}' depends on '{d.activity}' in the source, but generated '{entry}' does not "
                            f"wait for '{exit_}'.", "Fix the assembler; the execution order would differ."))
                    elif want is not None and have[exit_] != want:
                        issues.append(_issue(
                            "graph_condition_changed",
                            f"Dependency '{a.name}' -> '{d.activity}' runs on {want} in the source but "
                            f"{have[exit_]} in the generated '{entry}' -> '{exit_}'.",
                            "Fix the assembler; success/failure routing would differ."))

    for name, node in gen.items():
        for x in node.get("dependsOn") or []:
            a, b = owner.get(name), owner.get(x.get("activity", ""))
            if a and b and a != b and (a.lower(), b.lower()) not in source_edges:
                issues.append(_issue(
                    "graph_edge_added",
                    f"Generated '{name}' waits for '{x.get('activity')}', but the source has no dependency of "
                    f"'{a}' on '{b}'.", "Remove the added edge or explain it; execution order would differ."))
    return issues
