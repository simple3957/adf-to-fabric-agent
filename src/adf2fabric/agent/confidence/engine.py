"""How much of this migration is established, and how much is still an assumption. Read-only, deterministic, no model.

Every migrated thing (a pipeline, a linked service, a trigger, a Spark workload, a source cluster) is judged against a short list of
criteria. Each criterion has a weight, a reason it matters, and one of three states taken from what the earlier stages recorded:

* ``met``      the project holds evidence for it
* ``unmet``    the project holds evidence against it (or an earlier criterion it depends on is unmet)
* ``unknown``  nothing recorded says either way (no run, no baseline, a draft nobody reviewed)

From the states comes a range, not a single number:

* ``low``   the share of the weight that is *met*. Everything not evidenced counts against it.
* ``high``  the share that is *met or unknown*. What it would be if every open question turned out well.
* ``known`` the share that is met or unmet: how much of the picture has been looked at.

So the width of the range is exactly the unproven part, and it narrows only when evidence is recorded: a real run, a reviewed draft, a
tested connection. This is a bound from evidence, not a statistical probability: there is no sampling behind it, and the report says so.

The questions it returns are the open criteria grouped by cause, each with the reason to ask and the command that answers it, ranked by how
far answering them would move the range (found by recomputing with those criteria set to met).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from adf2fabric.agent.deployment.verify import current_record

MET, UNMET, UNKNOWN, NA = "met", "unmet", "unknown", "not_applicable"

# What the project is called when its evidence is only partly there. Order matters: the first that applies is the label.
LABELS = ("blocked", "failing", "needs_input", "unproven", "proven")
HARD_GATES = {"source_complete", "fully_converted", "schema_valid", "connectors_supported", "compatibility", "code_available", "classification",
              "connector_supported", "schedule_converted"}
RUN_CRITERIA = {"ran_ok", "matches_baseline", "run_verified"}

EVIDENCE_NOTE = ("A range from the evidence on file, not a statistical probability. The low end counts only what is proven; the high end also counts "
                 "what nobody has checked yet. The gap between them is the part still assumed.")

# Weight of each area in the project-wide range. An area the estate does not have is left out, never scored as perfect.
AREA_WEIGHT = {"pipelines": 4.0, "linked_services": 2.5, "compute": 2.0, "triggers": 1.0}
AREA_LABEL = {"pipelines": "Pipelines", "linked_services": "Linked services and connections", "compute": "Spark workloads and compute clusters",
              "triggers": "Triggers"}


@dataclass
class Criterion:
    id: str
    label: str
    weight: float
    state: str
    evidence: str = ""
    why: str = ""  # why this matters: the reason behind the question it raises
    needs: tuple[str, ...] = ()  # criteria that must hold first: if one is unmet, this one cannot be evidenced
    ask: str = ""  # the question a person can answer to move it
    do: str = ""  # the command or action that answers it
    gated: bool = False  # unmet only because something it needs is unmet: the cause is counted once, on that criterion


@dataclass
class Item:
    area: str
    name: str
    weight: float = 1.0  # how much this item counts inside its area (a pipeline by its activities)
    criteria: list[Criterion] = field(default_factory=list)


def _apply_gates(criteria: list[Criterion]) -> None:
    by_id = {c.id: c for c in criteria}
    for c in criteria:  # criteria are listed in dependency order, so one pass is enough
        if c.state in (MET, UNKNOWN) and any(by_id[n].state == UNMET for n in c.needs if n in by_id):
            blocked_by = next(n for n in c.needs if n in by_id and by_id[n].state == UNMET)
            c.state, c.evidence, c.gated = UNMET, f"cannot be evidenced while '{by_id[blocked_by].label}' is unmet", True


def bounds(criteria: Iterable[Criterion]) -> Optional[tuple[float, float, float]]:
    """(low, high, known) as fractions, or None when nothing applies."""
    cs = [c for c in criteria if c.state != NA]
    total = sum(c.weight for c in cs)
    if not total:
        return None
    met = sum(c.weight for c in cs if c.state == MET)
    unknown = sum(c.weight for c in cs if c.state == UNKNOWN)
    return met / total, (met + unknown) / total, (total - unknown) / total


def label_for(criteria: Iterable[Criterion]) -> str:
    cs = [c for c in criteria if c.state != NA]
    unmet = {c.id for c in cs if c.state == UNMET and not c.gated}  # a gated criterion says nothing new: "failing" needs a run that failed
    if unmet & HARD_GATES:
        return "blocked"
    if unmet & RUN_CRITERIA:
        return "failing"
    if unmet:
        return "needs_input"
    if any(c.state == UNKNOWN for c in cs):
        return "unproven"
    return "proven" if cs else "unproven"


def _pct_low(x: float) -> int:
    return int(math.floor(x * 100 + 1e-9))  # rounded the cautious way: the low end down, the high end up


def _pct_high(x: float) -> int:
    return int(math.ceil(x * 100 - 1e-9))


# ---- criteria per kind of thing ---------------------------------------------------------------------------------------------------------

_BLOCKING_SOURCE = {"missing_linked_services", "missing_datasets", "source_activities_unaccounted"}
_RUN_CMD = "adf2fabric verify --project {p} --workspace-id <workspace> --pipeline {n} --apply"


def _pipeline_item(p: Any, conv: Any, level_of: dict[str, str], errors: dict[str, list[Any]], assisted: set[str], verdict: Optional[dict],
                   project: str) -> Item:
    name = p.source_pipeline
    counts = p.status_counts or {}
    mine = errors.get(name, [])
    blocking = [f for f in mine if f.code in _BLOCKING_SOURCE]
    other = [f for f in mine if f.code not in _BLOCKING_SOURCE]
    inferred = [e for a in p.activities for e in a.inferred_elements if e.review_required]
    unbound = sorted({q.name for q in p.prerequisites if not q.bound and not q.name.lower().startswith("pipeline:") and q.name.lower() != "workspace"})
    levels = {level_of.get(q.name.lower(), "") for q in p.prerequisites if q.kind == "connection" and q.name.lower() in level_of}
    compat = [c for c in conv.compatibility if name in c.pipelines]
    cs: list[Criterion] = []

    cs.append(Criterion("source_complete", "Source read completely", 2.0,
                        UNMET if blocking or not p.coverage.complete else MET,
                        "; ".join(sorted({f.subject for f in blocking}))[:200] or (f"{len(p.coverage.missing)} activity(ies) not accounted for" if not p.coverage.complete else
                                                                                   f"all {len(p.activities)} activity(ies) accounted for"),
                        "Everything downstream is built from what discovery read; a missing linked service, dataset or activity makes the rest a guess.",
                        ask=f"Is the source of '{name}' complete (its linked services, datasets and every activity)?",
                        do="Add the missing artifacts to the repository or ARM export, then run discovery again (docs/partial-source.md)."))
    cs.append(Criterion("no_inference", "Nothing inferred or machine-drafted", 1.5,
                        UNKNOWN if inferred or name in assisted else MET,
                        (f"{len(inferred)} element(s) inferred by reconstruction" if inferred else "a reviewed model-assisted draft was used" if name in assisted else "everything was authored in the source"),
                        "Content that was not in the source may not do what the original did, until a person has checked it against it.",
                        ask=f"Does the inferred content in '{name}' match what the pipeline is meant to do?",
                        do="Review the proposals (reconstruction report) and fix the source, or accept them knowingly."))
    cs.append(Criterion("fully_converted", "Every activity converted", 3.0, UNMET if counts.get("blocked") or counts.get("manual_required") else MET,
                        (f"{counts.get('blocked', 0)} blocked, {counts.get('manual_required', 0)} need manual work" if counts.get("blocked") or counts.get("manual_required")
                         else f"all {len(p.activities)} activity(ies) have a Fabric equivalent"),
                        "A blocked or manual activity is a part of the pipeline that will not run in Fabric until a person builds it.",
                        ask=f"What replaces the blocked or manual activities in '{name}'?",
                        do="Read the diagnosis (adf2fabric diagnose) for each activity's cause and fix."))
    cs.append(Criterion("faithful", "No documented behaviour differences", 1.0, UNKNOWN if counts.get("converted_with_differences") else MET,
                        f"{counts.get('converted_with_differences', 0)} activity(ies) converted with a documented difference" if counts.get("converted_with_differences") else "no differences recorded",
                        "A documented difference is a place where Fabric behaves differently from ADF; it is acceptable only if someone has decided so.",
                        ask=f"Are the documented differences in '{name}' acceptable?", do="Read the conversion report's limitations for the pipeline."))
    cs.append(Criterion("schema_valid", "Generated JSON passes Fabric's schema checks", 2.0, MET if p.structural_valid and p.validation.valid else UNMET,
                        "structurally valid" if p.structural_valid and p.validation.valid else f"{sum(1 for i in p.validation.issues if i.severity.value == 'error')} schema error(s)",
                        "A file Fabric rejects cannot be deployed at all.", do="Fix the cause named in the diagnosis and convert again."))
    cs.append(Criterion("source_checks", "No source problems that fail in Fabric", 1.0, UNMET if other and any(f.severity == "error" for f in other) else MET,
                        ", ".join(sorted({f.subject for f in other}))[:200] or "none found",
                        "Some mistakes in the source only show when the pipeline runs in Fabric (reserved SQL aliases, empty parameters).",
                        do="Fix the finding in the source (see 'Problems in the source' in the handover)."))
    cs.append(Criterion("connectors_supported", "Its connectors exist in Fabric", 1.5,
                        NA if not levels else UNMET if "unsupported" in levels else UNKNOWN if "unlisted" in levels else MET,
                        "an unsupported connector" if "unsupported" in levels else "a connector Fabric does not list" if "unlisted" in levels else "all connectors are supported",
                        "Fabric cannot read or write a store it has no connector for.", ask=f"Can the unsupported store used by '{name}' be moved to one Fabric supports?",
                        do="Move the data to a supported store, or keep that step outside Fabric."))
    cs.append(Criterion("compatibility", "No retired or preview components", 1.5, NA if not compat else UNMET if any(c.status in ("removed", "not_available") for c in compat) else UNKNOWN,
                        ", ".join(sorted({c.subject for c in compat}))[:200] or "none found",
                        "A removed or retired component stops working whatever the conversion did; a preview one may change.",
                        do="Replace the component (alternatives are in the compatibility report)."))
    cs.append(Criterion("bindings_complete", "Connections and items bound", 2.0, UNMET if unbound else MET,
                        ("unbound: " + ", ".join(unbound[:5])) if unbound else "everything it needs is bound",
                        "A placeholder left in the file makes the deploy refuse it.", ask=f"Which Fabric connection serves {', '.join(unbound[:3]) or 'each unbound item'}?",
                        do="adf2fabric connections / the Connect step, then convert again with --resolutions."))
    gates = ("fully_converted", "schema_valid", "bindings_complete")
    ran = verdict.get("verdict") if verdict else None
    cs.append(Criterion("deployed", "Deployed in the Fabric workspace", 2.0, MET if ran else UNKNOWN,
                        "a run of it was read from the workspace" if ran else "no run was read from a workspace, so deployment is not shown",
                        "Generated files are not a deployed pipeline.", needs=gates, ask=f"Is '{name}' deployed in the target workspace?",
                        do="adf2fabric deploy --project %s --workspace-id <workspace> --apply" % project))
    cs.append(Criterion("ran_ok", "Ran to completion in Fabric", 3.0,
                        UNMET if ran == "failed" else MET if ran in ("matches", "partial", "differs", "no_baseline") else UNKNOWN,
                        {"failed": "its latest Fabric run did not complete", None: "never run through this tool"}.get(ran, "its latest Fabric run completed"),
                        "A clean static check and a 200 from the API say nothing about whether the pipeline runs. Only a real run does.",
                        needs=gates + ("deployed",), ask=f"May '{name}' be run in Fabric, and is it safe for it to write its targets?",
                        do=_RUN_CMD.format(p=project, n=name)))
    cs.append(Criterion("matches_baseline", "Result matches the ADF run", 3.0,
                        MET if ran == "matches" else UNMET if ran in ("differs", "failed") else UNKNOWN,
                        {"matches": "its Fabric run matched the ADF run", "differs": "its Fabric run differs from the ADF run", "partial": "only some activities could be compared",
                         "no_baseline": "there is no earlier run to compare with", "failed": "the run failed"}.get(ran, "no comparison made"),
                        "Parity is the aim: the same activities, the same counts, the same lookups as ADF.", needs=("ran_ok",),
                        ask=f"Is there an ADF run of '{name}' to compare with?", do="Run it in ADF too (or twice in Fabric), then verify."))
    _apply_gates(cs)
    return Item("pipelines", name, max(1.0, float(len(p.activities))), cs)


def _service_items(conv: Any, applied: dict[str, dict]) -> list[Item]:
    items: list[Item] = []
    bound = {q.name.lower(): q for q in conv.connections if q.kind == "connection"}
    for c in conv.connector_coverage:
        q = bound.get(c.linked_service.lower())
        ap = applied.get(c.linked_service.lower())
        cs = [Criterion("connector_supported", "Fabric has a connector for it", 3.0,
                        UNMET if c.level == "unsupported" else UNKNOWN if c.level == "unlisted" else MET,
                        {"created": "the tool can create this connection", "manual": "Fabric supports it; the connection is made by hand", "unsupported": "Fabric has no connector for it",
                         "unlisted": "not named in Fabric's connector list"}.get(c.level, c.level),
                        "Without a Fabric connector the data store cannot be reached from a pipeline.", ask=f"Can '{c.linked_service}' ({c.adf_type or 'unknown type'}) be replaced by a supported store?",
                        do="Check Fabric's connector list, or move the data first."),
              Criterion("connection_bound", "A Fabric connection is bound to it", 3.0, UNKNOWN if q is None else MET if q.bound else UNMET,
                        "no converted pipeline uses it yet, so nothing says it is bound or needed" if q is None else "its Fabric connection id is bound" if q.bound else "no Fabric connection id is bound yet",
                        "Pipelines reach a store through a Fabric connection; an unbound one blocks the deploy.", ask=f"Which Fabric connection should '{c.linked_service}' use?",
                        do="Connect step (or `adf2fabric connections`), then bind its id and convert again."),
              Criterion("credentials_validated", "Its credentials were checked", 1.5, UNKNOWN if q is None else
                        MET if ap and (ap.get("fabric_validated") or ap.get("state") == "connection_tested") else UNKNOWN,
                        "Fabric tested the credentials when it created the connection" if ap and ap.get("fabric_validated") else
                        "a reused or hand-made connection: its credentials were not checked by this tool",
                        "A bound connection can still hold a wrong or expired credential; only a test shows it works.", needs=("connection_bound",),
                        ask=f"Does the connection for '{c.linked_service}' work with the credentials Fabric holds?", do="Test the connection in Fabric (Manage connections and gateways).")]
        _apply_gates(cs)
        items.append(Item("linked_services", c.linked_service, 1.0, cs))
    for q in conv.connections:
        if q.kind != "gateway":
            continue
        cs = [Criterion("connection_bound", "An on-premises data gateway is bound", 3.0, MET if q.bound else UNMET,
                        "bound" if q.bound else "not bound", "A self-hosted integration runtime has no Fabric equivalent; Fabric reaches on-premises data through a gateway.",
                        ask=f"Which on-premises data gateway serves '{q.name}'?", do="Install or choose a gateway, then bind its id.")]
        items.append(Item("linked_services", q.name, 1.0, cs))
    return items


def _trigger_items(conv: Any) -> list[Item]:
    seen: dict[str, Any] = {}
    for p in conv.pipelines:
        for t in p.triggers:
            seen.setdefault(t.name.lower(), t)
    out = []
    for t in seen.values():
        st = t.schedule_status
        state = MET if st == "generated" else UNKNOWN if st in ("generated_with_differences", None) else UNMET
        cs = [Criterion("schedule_converted", "Converted to a Fabric schedule", 3.0, state,
                        {"generated": "a schedule definition was generated", "generated_with_differences": "generated, with differences from the ADF trigger",
                         "manual": "must be recreated by hand", "not_supported": "no Fabric equivalent"}.get(st or "", "no schedule result recorded"),
                        "A trigger that is not recreated means the pipeline stops running on its own after cut-over.",
                        ask=f"How should trigger '{t.name}' ({t.trigger_type or 'unknown type'}) run in Fabric?", do=t.action or "Recreate it by hand (for example a Fabric Activator rule).")]
        out.append(Item("triggers", t.name, 1.0, cs))
    return out


def _workload_items(manifest: Any) -> list[Item]:
    out: list[Item] = []
    for w in manifest.workloads:
        code = w.code.status.value
        have = code in ("retrieved", "cached", "in_project", "supplied")
        waiting = code in ("not_requested", "missing_credential", "denied")
        cls = w.classification.value
        gen = w.generation.status
        cs = [Criterion("code_available", "Source code read", 2.0, MET if have else UNKNOWN if waiting else UNMET, f"code is {code.replace('_', ' ')}",
                        "A notebook cannot be assessed or converted without its code.", ask=f"May the agent read the code of '{w.name}', or will you supply it?",
                        do="adf2fabric workloads-assess with an authorised token, or supply the file."),
              Criterion("classification", "Convertible to Fabric", 3.0, MET if cls in ("directly_convertible", "convertible_with_changes") else UNMET,
                        cls.replace("_", " "), "Some constructs have no Fabric equivalent and need a different approach.", needs=("code_available",),
                        ask=f"What is the Fabric approach for '{w.name}'?", do="Read the workload report's findings for the construct."),
              Criterion("generated", "Notebook or job definition generated", 2.0, MET if gen == "generated" else UNKNOWN if gen == "generated_with_open_issues" else UNMET,
                        f"generation: {gen.replace('_', ' ')}", "Only a generated artifact can be deployed.", needs=("code_available", "classification"),
                        do="adf2fabric workloads-generate"),
              Criterion("approved", "Approved by a person", 1.0, MET if w.approval.status == "approved" else UNMET if w.approval.status == "rejected" else UNKNOWN,
                        f"approval: {w.approval.status}", "Deploying generated code to a workspace needs a person's approval.", needs=("generated",),
                        ask=f"Who approves '{w.name}' before it is deployed?", do="adf2fabric workloads-approve"),
              Criterion("run_verified", "Ran successfully in Fabric", 4.0, MET if w.execution.status == "succeeded" else UNMET if w.execution.status == "failed" else UNKNOWN,
                        f"run: {w.execution.status.replace('_', ' ')}", "A generated notebook is not a verified one; only a recorded real run is.", needs=("generated",),
                        ask=f"Has '{w.name}' been run in Fabric?", do="Run it, then adf2fabric workloads-record-run")]
        _apply_gates(cs)
        out.append(Item("compute", f"workload: {w.name}", 1.0, cs))
    return out


def _cluster_items(conv: Any) -> list[Item]:
    plan = conv.compute_plan
    if plan is None:
        return []
    out: list[Item] = []
    gaps: dict[str, list[Any]] = {}
    for g in plan.gaps:
        gaps.setdefault(g.compute_id, []).append(g)
    for use in plan.inventory:
        pools = [p for p in plan.pools if use.id in p.replaces]
        cs = [Criterion("sizing_known", "Cluster size known", 2.0, MET if use.sizing_known else UNMET,
                        f"sizing from {use.sizing_source.replace('_', ' ')}" if use.sizing_known else "ADF refers to the cluster by id only",
                        "A Fabric pool cannot be sized from a cluster whose node size and worker count are not known.",
                        ask=f"What are the node type and worker range of {use.platform} cluster '{use.source_ref}'?", do="Supply them in the compute sources file, then re-run the assessment."),
              Criterion("no_gap", "No open compute gaps", 2.0, UNMET if gaps.get(use.id) else MET,
                        "; ".join(g.message for g in gaps.get(use.id, []))[:200] or "none recorded", "Gaps are settings the source uses that Fabric cannot carry over as they are.",
                        ask=f"How should '{use.source_ref or use.id}' be handled in Fabric given its open gap?",
                        do=(gaps[use.id][0].action if gaps.get(use.id) else "")),
              Criterion("pool_proposed", "A Fabric pool is sized for it", 2.0, NA if not pools else MET if all(p.status == "proposed" for p in pools) else UNMET,
                        ", ".join(f"{p.name}: {p.status.replace('_', ' ')}" for p in pools) or "the cluster stays where it is",
                        "The Fabric pool is what replaces the cluster for notebooks that move.", do="Answer the pool's open input in the compute plan."),
              Criterion("pool_deployed", "The pool exists in Fabric", 1.0, NA if not pools else MET if all(p.fabric_id for p in pools) else UNKNOWN,
                        "created or found in the workspace" if pools and all(p.fabric_id for p in pools) else "not created yet",
                        "A proposal is not a resource.", needs=("pool_proposed",), ask="Should the proposed pool be created now?", do="adf2fabric compute-deploy --pool <name> (dry run first)")]
        _apply_gates(cs)
        out.append(Item("compute", f"cluster: {use.source_ref or use.id}", 1.0, cs))
    return out


# ---- aggregation ------------------------------------------------------------------------------------------------------------------------

def _item_json(it: Item) -> dict[str, Any]:
    b = bounds(it.criteria)
    return {"name": it.name, "label": label_for(it.criteria), "low": _pct_low(b[0]) if b else None, "high": _pct_high(b[1]) if b else None,
            "known": _pct_low(b[2]) if b else None,
            "criteria": [{"id": c.id, "label": c.label, "weight": c.weight, "state": c.state, "evidence": c.evidence, "why": c.why} for c in it.criteria if c.state != NA]}


def _pool(items: list[Item]) -> Optional[tuple[float, float, float]]:
    """Weighted mean of the items' bounds (a pipeline weighs by its activities)."""
    rows = [(it.weight, bounds(it.criteria)) for it in items]
    rows = [(w, b) for w, b in rows if b]
    total = sum(w for w, _ in rows)
    if not total:
        return None
    return tuple(sum(w * b[i] for w, b in rows) / total for i in range(3))  # type: ignore[return-value]


def _overall(items: list[Item]) -> Optional[tuple[float, float, float]]:
    by_area: dict[str, list[Item]] = {}
    for it in items:
        by_area.setdefault(it.area, []).append(it)
    rows = [(AREA_WEIGHT[a], _pool(v)) for a, v in by_area.items()]
    rows = [(w, b) for w, b in rows if b]
    total = sum(w for w, _ in rows)
    if not total:
        return None
    return tuple(sum(w * b[i] for w, b in rows) / total for i in range(3))  # type: ignore[return-value]


def _questions(items: list[Item], base: tuple[float, float, float], limit: int = 12) -> list[dict[str, Any]]:
    """Open criteria grouped by (area, criterion): what to ask, why, and how far the range moves if they all turn out well."""
    groups: dict[tuple[str, str], list[tuple[Item, Criterion]]] = {}
    for it in items:
        for c in it.criteria:
            if c.state in (UNKNOWN, UNMET) and not c.evidence.startswith("cannot be evidenced"):
                groups.setdefault((it.area, c.id), []).append((it, c))
    out = []
    for (area, cid), members in groups.items():
        saved = [(c, c.state) for _, c in members]
        for _, c in members:
            c.state = MET
        after = _overall(items)
        for c, s in saved:
            c.state = s
        if after is None:
            continue
        first = members[0][1]
        names = [it.name for it, _ in members]
        out.append({"area": area, "criterion": cid, "state": UNMET if any(s == UNMET for _, s in saved) else UNKNOWN, "count": len(members), "items": names[:8],
                    "question": first.ask or first.label, "why": first.why, "do": first.do,
                    "raises_low_by": round((after[0] - base[0]) * 100, 1), "raises_high_by": round((after[1] - base[1]) * 100, 1)})
    return sorted(out, key=lambda q: (-q["raises_low_by"], -q["raises_high_by"], q["criterion"]))[:limit]


def assess(store: Any, pid: str, now: Optional[datetime] = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    if not store.has_conversion(pid):
        return {"project": pid, "ready": False, "message": "Nothing is converted yet: run `convert` first. Confidence is judged on what conversion produced.", "note": EVIDENCE_NOTE}
    conv = store.load_conversion(pid)
    project = store.load(pid)
    stale = bool(project.repository and conv.source_commit_sha != project.repository.commit_sha)
    verification = current_record(store.project_dir(pid))
    errors: dict[str, list[Any]] = {}
    for f in conv.preflight:
        if f.severity == "error":
            for pl in f.pipelines:
                errors.setdefault(pl, []).append(f)
    level_of = {c.linked_service.lower(): c.level for c in conv.connector_coverage}
    assisted = {n.pipeline for n in conv.assisted}
    items: list[Item] = [_pipeline_item(p, conv, level_of, errors, assisted, verification.get(p.source_pipeline), pid) for p in conv.pipelines]
    items += _service_items(conv, _applied_connections(store, pid))
    items += _trigger_items(conv)
    if store.has_workload_manifest(pid):
        items += _workload_items(store.load_workload_manifest(pid))
    items += _cluster_items(conv)
    if stale:  # everything was judged from a conversion of older source: nothing it proved is known to hold now
        for it in items:
            for c in it.criteria:
                if c.state == MET:
                    c.state, c.evidence = UNKNOWN, "the source changed after this was evidenced"
    base = _overall(items)
    areas: dict[str, Any] = {}
    for a in AREA_WEIGHT:
        mine = [it for it in items if it.area == a]
        b = _pool(mine)
        if not mine or not b:
            continue
        counts: dict[str, int] = {}
        for it in mine:
            counts[label_for(it.criteria)] = counts.get(label_for(it.criteria), 0) + 1
        areas[a] = {"title": AREA_LABEL[a], "low": _pct_low(b[0]), "high": _pct_high(b[1]), "known": _pct_low(b[2]), "labels": counts,
                    "items": sorted((_item_json(it) for it in mine), key=lambda j: (LABELS.index(j["label"]), j["low"] or 0, j["name"].lower()))}
    labels = [label_for(it.criteria) for it in items]
    overall_label = next((lab for lab in LABELS if lab in labels), "unproven")
    return {"project": pid, "ready": True, "created_at": now.isoformat(), "note": EVIDENCE_NOTE, "stale": stale,
            "overall": ({"low": _pct_low(base[0]), "high": _pct_high(base[1]), "known": _pct_low(base[2]), "label": overall_label,
                         "gap": _pct_high(base[1]) - _pct_low(base[0])} if base else None),
            "areas": areas, "absent_areas": [AREA_LABEL[a] for a in AREA_WEIGHT if a not in areas],
            "questions": _questions(items, base) if base else []}


def _applied_connections(store: Any, pid: str) -> dict[str, dict]:
    import yaml

    path = store.project_dir(pid) / "connections" / "apply.yaml"
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return {}
    return {str(i.get("linked_service", "")).lower(): i for i in data.get("items", []) if isinstance(i, dict)}


def render(d: dict[str, Any]) -> str:
    if not d.get("ready"):
        return f"# Migration confidence: {d['project']}\n\n{d['message']}\n"
    o = d["overall"]
    out = [f"# Migration confidence: {d['project']}", "", f"> {d['note']}", ""]
    if d["stale"]:
        out += ["> **The source changed after the conversion.** Nothing proven earlier is assumed to hold: run discovery and convert again.", ""]
    if o:
        out += [f"**Overall: {o['low']}% to {o['high']}%** ({o['label'].replace('_', ' ')}). {o['known']}% of the picture has been checked; "
                f"{o['gap']} points are still assumed.", ""]
    out += ["| Area | Range | Checked | Where its items stand |", "|---|---|---|---|"]
    for a in d["areas"].values():
        out.append(f"| {a['title']} | {a['low']}% to {a['high']}% | {a['known']}% | " + ", ".join(f"{n} {k.replace('_', ' ')}" for k, n in sorted(a["labels"].items(), key=lambda x: LABELS.index(x[0]))) + " |")
    if d["absent_areas"]:
        out += ["", "Not in this estate, so not scored: " + ", ".join(d["absent_areas"]) + "."]
    if d["questions"]:
        out += ["", "## What to find out first", "", "Each question is an open criterion, with the reason it matters. Answering it favourably raises the range by the points shown.", ""]
        for i, q in enumerate(d["questions"], 1):
            out += [f"{i}. **{q['question']}** ({q['count']} item(s): {', '.join(q['items'][:4])}{'...' if q['count'] > 4 else ''})",
                    f"   - Why: {q['why']}", f"   - Do: {q['do'] or 'see the diagnosis'}", f"   - Raises the range: low +{q['raises_low_by']}, high +{q['raises_high_by']} points"]
    for key, a in d["areas"].items():
        out += ["", f"## {a['title']}", ""]
        for it in a["items"]:
            out += [f"### {it['name']}: {it['low']}% to {it['high']}% ({it['label'].replace('_', ' ')})", "", "| Criterion | State | Evidence |", "|---|---|---|"]
            out += [f"| {c['label']} | {c['state']} | {c['evidence'].replace('|', '/') or '-'} |" for c in it["criteria"]]
            out += [""]
    return "\n".join(out) + "\n"
