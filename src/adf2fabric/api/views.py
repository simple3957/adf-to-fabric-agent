"""UI-safe summaries of a project, built on the server from the project's own records.

The browser never receives a raw record. Each function here chooses what an ordinary view may show (names, counts, states, reasons,
what to do next) and leaves out what it may not: source code and generated definitions, queries, connection strings, secrets and
tokens, absolute or internal paths, resource and tenant identifiers that are not needed to act, stack traces. Everything is then
passed through ``sanitize`` as a second line of defence, so a field added to a record later cannot leak by accident.

State vocabulary (one pipeline can have flags, but its ``state`` is the furthest point reached that the evidence supports):

* discovered         read from the source and fully understood
* inferred           read, but something was dynamic, unresolved or filled in (so a person should confirm it)
* unsupported        an activity or connector with no Fabric counterpart
* blocked            cannot proceed: a placeholder stands in for an activity, or a connection / item is unbound
* converted          Fabric pipeline JSON was generated (this is NOT "works in Fabric")
* deployed           a deploy call reported it created or updated the pipeline (this is NOT "runs correctly")
* runtime_validated  a real Fabric run completed and matched its baseline
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.deployment import record as deploy_record
from adf2fabric.agent.deployment import verify as verify_mod
from adf2fabric.api.security.guards import scrub
from adf2fabric.connections.redaction import redact
from adf2fabric.project.discovery import AdfConfidence
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore

STATES = ("discovered", "inferred", "unsupported", "blocked", "converted", "deployed", "runtime_validated")
STATE_HELP = {
    "discovered": "Read from the source and fully understood.",
    "inferred": "Read, but something was dynamic or filled in: confirm it.",
    "unsupported": "No Fabric counterpart for an activity or connector.",
    "blocked": "Cannot proceed until something is supplied or decided.",
    "converted": "Fabric JSON was generated. This is not proof it works.",
    "deployed": "A deploy call reported success. This is not proof it runs correctly.",
    "runtime_validated": "A real Fabric run completed and matched its baseline.",
}
_DENY_KEYS = re.compile(r"(?i)(^|_)(path|file|local|trace|traceback|stack|token|secret|password|pwd|connection_?string|credential|authorization|"
                        r"artifact|pointer|sha|hash|raw|configuration|definition|source_text)($|_)")
_SAFE_KEYS = {"source_path_count", "files_parsed"}
MAX_LIST = 200


def sanitize(obj: Any, roots: tuple[Path, ...] = (), depth: int = 0) -> Any:
    """Recursively drop keys that name paths, secrets, code or definitions; scrub and bound every string."""
    if depth > 12:
        return None
    if isinstance(obj, dict):
        return {k: sanitize(v, roots, depth + 1) for k, v in obj.items() if k in _SAFE_KEYS or not _DENY_KEYS.search(str(k))}
    if isinstance(obj, (list, tuple)):
        return [sanitize(v, roots, depth + 1) for v in list(obj)[:MAX_LIST]]
    if isinstance(obj, str):
        return scrub(redact(obj), roots)[:600]
    return obj


def _try(fn):
    try:
        return fn()
    except (ProjectNotFoundError, ValueError, OSError):
        return None


def _worst(confidences: list[str]) -> str:
    for level in ("unsupported", "low", "medium", "high"):
        if level in confidences:
            return level
    return "high"


def _walk(acts):
    for a in acts or []:
        yield a
        yield from _walk(a.activities)


# =====================================================================================================================
# pipeline states
# =====================================================================================================================


def pipeline_states(store: ProjectStore, pid: str) -> dict[str, dict[str, Any]]:
    """name -> {state, flags, reasons}. Built only from persisted evidence."""
    d = _try(lambda: store.load_adf_semantic_discovery(pid))
    if d is None:
        return {}
    conv = _try(lambda: store.load_conversion(pid)) if store.has_conversion(pid) else None
    conv_by = {p.source_pipeline.lower(): p for p in conv.pipelines} if conv else {}
    rec = deploy_record.load(store.project_dir(pid)).get("pipelines", {})
    verified = verify_mod.load_record(verify_mod.record_path(store.project_dir(pid)))
    out: dict[str, dict[str, Any]] = {}
    for p in d.pipelines:
        confs = [x.confidence.value for x in _walk(p.activities)]
        flags = {"inferred": sum(1 for c in confs if c in ("medium", "low")), "unsupported": sum(1 for c in confs if c == "unsupported"),
                 "blocked": 0}
        reasons: list[str] = []
        state = "discovered"
        if flags["inferred"]:
            state = "inferred"
            reasons.append(f"{flags['inferred']} activity(ies) are dynamic or incomplete in the source")
        if flags["unsupported"]:
            state = "unsupported"
            reasons.append(f"{flags['unsupported']} activity(ies) have an unknown or unsupported type")
        c = conv_by.get(p.name.lower())
        if c is not None:
            blocked = [a for a in c.activities if a.status.value == "blocked"]
            manual = [a for a in c.activities if a.status.value == "manual_required"]
            unbound = [q.name for q in c.prerequisites if not q.bound]
            flags["blocked"] = len(blocked) + len(manual)
            if blocked or manual:
                state = "unsupported" if any(a.runtime_disposition == "redesign" for a in blocked) else "blocked"
                reasons.append(f"{len(blocked)} not converted, {len(manual)} need manual work")
            elif c.status.value in ("converted", "converted_with_differences"):
                state = "converted"
                if unbound:
                    reasons.append(f"{len(unbound)} connection or item binding(s) still open (it will not run until bound)")
            else:
                state = "blocked"
        row = rec.get(p.name) or rec.get(next((k for k in rec if k.lower() == p.name.lower()), ""), None)
        if state == "converted" and row and row.get("ok"):
            state = "deployed"
        v = verified.get(p.name) or next((x for k, x in verified.items() if k.lower() == p.name.lower()), None)
        if state == "deployed" and v and v.get("verdict") == "matches" and v.get("fabric_status") == "Completed":
            state = "runtime_validated"
        out[p.name] = {"state": state, "flags": flags, "reasons": reasons}
    return out


# =====================================================================================================================
# inventory
# =====================================================================================================================


def inventory(store: ProjectStore, pid: str, roots: tuple[Path, ...] = ()) -> dict[str, Any]:
    d = store.load_adf_semantic_discovery(pid)  # ProjectNotFoundError when discovery has not run (the route turns it into guidance)
    states = pipeline_states(store, pid)
    by_type: dict[str, int] = {}
    conf_counts: dict[str, int] = {}
    pipes = []
    for p in d.pipelines:
        confs = [a.confidence.value for a in _walk(p.activities)]
        for c in confs:
            conf_counts[c] = conf_counts.get(c, 0) + 1
        for t, n in p.activity_type_counts.items():
            by_type[t] = by_type.get(t, 0) + n
        pipes.append({"name": p.name, "folder": p.folder, "activities": p.activity_total, "top_level": p.activity_count, "calls": list(p.pipeline_dependencies),
                      "worst_confidence": _worst(confs), "unknown_types": list(p.unknown_activity_types), "issues": len(p.issues),
                      "reconciled": bool(p.inventory is None or p.inventory.matched), **states.get(p.name, {"state": "discovered", "flags": {}, "reasons": []})})
    unprocessed = [{"kind": u.kind, "name": u.name or "", "reason": u.reason} for u in d.unprocessed_artifacts]
    completeness = {
        "status": d.status.value, "pipelines": len(d.pipelines), "datasets": len(d.datasets), "linked_services": len(d.linked_services),
        "triggers": len(d.triggers), "data_flows": len(d.dataflows), "integration_runtimes": len(d.integration_runtimes),
        "activities": d.activity_count, "parsed": d.parsed_count, "skipped": d.skipped_count, "failed": d.failed_count,
        "unprocessed": unprocessed[:50], "unprocessed_total": len(unprocessed), "inventory_mismatches": d.inventory_mismatch_count,
        "unresolved_references": len(d.unresolved_references), "unknown_activity_types": list(d.unknown_activity_types),
        "complete": d.failed_count == 0 and not unprocessed and d.inventory_mismatch_count == 0 and not d.unresolved_references,
    }
    state_counts = {s: 0 for s in STATES}
    for v in states.values():
        state_counts[v["state"]] = state_counts.get(v["state"], 0) + 1
    return sanitize({"completeness": completeness, "pipelines": pipes, "activity_types": dict(sorted(by_type.items())), "confidence": conf_counts,
                     "linked_services": [{"name": x.name, "type": x.type} for x in d.linked_services],
                     "integration_runtimes": [{"name": r.name, "kind": getattr(r.kind, "value", r.kind)} for r in d.integration_runtimes],
                     "triggers": [{"name": t.name, "type": t.type, "state": t.runtime_state} for t in d.triggers],
                     "states": state_counts, "state_help": STATE_HELP,
                     "dependencies": _dependencies(store, pid), "compute": _compute(store, pid)}, roots)


def _dependencies(store: ProjectStore, pid: str) -> dict[str, Any]:
    inv = _try(lambda: store.load_dependency_inventory(pid)) if store.has_dependency_inventory(pid) else None
    if inv is None:
        return {"available": False, "needs": "Run the 'dependencies' stage to resolve references between pipelines, datasets, connections and external systems."}
    return {"available": True, "total": inv.summary.total, "by_type": inv.summary.by_type, "by_resolution": inv.summary.by_resolution,
            "by_category": inv.summary.by_category, "blocking": inv.summary.blocking, "errors": inv.summary.findings_error,
            "warnings": inv.summary.findings_warning, "cycles": len(inv.cycles), "source_complete": inv.source.complete,
            "findings": [{"code": f.code, "severity": f.severity.value, "subject": f.subject, "message": f.message, "action": f.action} for f in inv.findings[:60]],
            "order": [{"pipeline": o.pipeline, "wave": o.wave, "ready": o.ready, "blocked_by": len(o.blocked_by), "in_cycle": o.in_cycle} for o in inv.pipeline_order],
            "waves": [{"number": w.number, "name": w.name, "items": len(w.items)} for w in inv.waves],
            "access_checks": "run" if inv.checks_requested else "not run: nothing external was contacted"}


def waves(store: ProjectStore, pid: str, roots: tuple[Path, ...] = ()) -> dict[str, Any]:
    """The dependency waves and the order the pipelines are deployed in, with what each object needs and what needs it.

    Built from the dependency inventory only: names, types, counts and the order. Nothing here carries a definition, a path or a secret."""
    inv = _try(lambda: store.load_dependency_inventory(pid)) if store.has_dependency_inventory(pid) else None
    if inv is None:
        return {"available": False, "needs": "Run 'Resolve dependencies' on the Review step: the waves and the order are built from the references it resolves."}
    states = pipeline_states(store, pid)
    kind_of = {w.number: w.kind for w in inv.waves}
    wave_of: dict[str, int] = {}
    for w in inv.waves:
        for item in w.items:  # most items are 'type:name'; the pipeline waves list plain pipeline names
            wave_of[(item if ":" in item and w.kind != "pipelines" else f"pipeline:{item}").lower()] = w.number
    order = {o.pipeline.lower(): o for o in inv.pipeline_order}

    def top_level(consumer: str) -> Optional[str]:
        """A consumer such as 'activity:PL/Copy' or 'dataset:DS_X' as the object it belongs to ('pipeline:pl' / 'dataset:ds_x')."""
        kind, _, rest = consumer.partition(":")
        if kind == "activity":
            return f"pipeline:{rest.split('/', 1)[0].lower()}"
        return f"{kind}:{rest.lower()}" if kind in ("pipeline", "dataset", "linked_service", "databricks_notebook") else None

    nodes: dict[str, dict[str, Any]] = {}
    for d in inv.dependencies:
        if d.type.value in ("variable", "pipeline_parameter", "activity_output"):
            continue  # inside one pipeline: not something that is ordered or migrated on its own
        key = d.id.lower()
        short = d.name.rsplit("/", 1)[-1] if d.type.value == "databricks_notebook" else d.name
        nodes[key] = {"id": key, "name": short, "type": d.type.value, "wave": wave_of.get(key, 0), "needs": set(), "needed_by": set(),
                      "category": getattr(d.migration.category, "value", d.migration.category) if d.migration else "", "blocks": bool(d.migration and d.migration.blocks),
                      "state": (states.get(d.name) or {}).get("state") if d.type.value == "pipeline" else None}
    for d in inv.dependencies:
        user = nodes.get(d.id.lower())
        if user is None:
            continue
        for c in d.consumers:
            holder = top_level(c)
            if holder and holder in nodes and holder != d.id.lower():
                nodes[holder]["needs"].add(d.id.lower())  # the consumer needs this dependency
                user["needed_by"].add(holder)
    for key, o in order.items():  # pipeline-to-pipeline calls come from the order, which also breaks ties
        me = nodes.get(f"pipeline:{key}")
        for callee in o.calls:
            other = nodes.get(f"pipeline:{callee.lower()}")
            if me and other:
                me["needs"].add(other["id"]); other["needed_by"].add(me["id"])
    names = {k: v["name"] for k, v in nodes.items()}
    out_nodes = [{"id": n["id"], "name": n["name"], "type": n["type"], "wave": n["wave"], "category": n["category"], "blocks": n["blocks"], "state": n["state"],
                  "needs": sorted(names[x] for x in n["needs"]), "needed_by": sorted(names[x] for x in n["needed_by"])} for n in nodes.values()]
    out_nodes.sort(key=lambda n: (n["wave"], n["type"], n["name"].lower()))
    wave_rows = []
    for w in inv.waves:
        counts: dict[str, int] = {}
        for item in w.items:
            kind = "pipeline" if w.kind == "pipelines" else item.split(":", 1)[0]
            counts[kind] = counts.get(kind, 0) + 1
        # rows, not a dict keyed by type: sanitize() drops any key that looks like a path (e.g. 'storage_path')
        types = [{"type": k, "count": n} for k, n in sorted(counts.items(), key=lambda kv: -kv[1])]
        wave_rows.append({"number": w.number, "name": w.name, "kind": w.kind, "note": w.note, "count": len(w.items), "types": types,
                          "items": [nodes[(i if ":" in i and w.kind != "pipelines" else f"pipeline:{i}").lower()]["name"]
                    if (i if ":" in i and w.kind != "pipelines" else f"pipeline:{i}").lower() in nodes else i.split(":", 1)[-1] for i in w.items][:200]})
    edges = sum(len(n["needs"]) for n in nodes.values())
    return sanitize({
        "available": True,
        "stats": {"objects": len(out_nodes), "dependencies": edges, "waves": len(inv.waves), "with_dependencies": sum(1 for n in nodes.values() if n["needs"] or n["needed_by"]),
                  "cycles": len(inv.cycles), "blocking": inv.summary.blocking},
        "waves": wave_rows,
        "order": [{"position": o.position, "pipeline": o.pipeline, "wave": o.wave, "calls": list(o.calls), "called_by": list(o.called_by), "ready": o.ready,
                   "blocked_by": list(o.blocked_by), "in_cycle": o.in_cycle, "state": (states.get(o.pipeline) or {}).get("state")} for o in inv.pipeline_order],
        "nodes": out_nodes[:600], "truncated": max(0, len(out_nodes) - 600),
        "types": sorted({n["type"] for n in out_nodes}),
        "note": "Nothing is created before what it needs. Pipelines are deployed in this order, children before the pipelines that call them.",
    }, roots)


def _compute(store: ProjectStore, pid: str) -> dict[str, Any]:
    from adf2fabric.agent.provisioning.requirements import estimates, load_compute_plan

    plan = _try(lambda: load_compute_plan(store, pid))
    wm = _try(lambda: store.load_workload_manifest(pid)) if store.has_workload_manifest(pid) else None
    out: dict[str, Any] = {"available": plan is not None or wm is not None}
    if plan is not None:
        out["sources"] = [{"platform": u.platform, "mode": u.mode, "runtime": u.runtime_version, "sized": u.sizing_known, "activities": len(u.activities),
                           "libraries": len(u.libraries)} for u in plan.inventory]
        out["pools"] = [{"name": p.name, "status": p.status, "node_size": p.node_size, "peak_vcores": p.peak_spark_vcores} for p in plan.pools]
        out["gaps"] = [{"code": g.code, "message": g.message, "action": g.action} for g in plan.gaps[:30]]
        out["estimate"] = estimates(type("I", (), {"compute": plan, "workloads": None, "inventory": None})())
    if wm is not None:
        out["workloads"] = [{"name": w.source_item.rsplit("/", 1)[-1], "kind": w.kind.value, "classification": w.classification.value, "code": w.code.status.value,
                             "generation": w.generation.status, "approval": w.approval.status, "execution": w.execution.status,
                             "open_issues": len(w.unresolved)} for w in wm.workloads]
    if not out["available"]:
        out["needs"] = "No Spark compute was found, or the compute plan has not been built yet (run convert)."
    return out


# =====================================================================================================================
# compatibility
# =====================================================================================================================


def compatibility(store: ProjectStore, pid: str, roots: tuple[Path, ...] = ()) -> dict[str, Any]:
    if not store.has_conversion(pid):
        return {"available": False, "needs": "Run the convert stage: compatibility is judged against the generated conversion."}
    conv = store.load_conversion(pid)
    items = []
    for p in conv.pipelines:
        for a in p.activities:
            if a.status.value == "converted" and not a.alternatives:
                continue
            items.append({"pipeline": p.source_pipeline, "activity": a.source_name, "type": a.source_type or "", "status": a.status.value,
                          "strategy": a.strategy.value, "disposition": a.runtime_disposition or "", "summary": a.summary,
                          "limitations": a.limitations[:5], "manual_steps": a.manual_steps[:5], "alternatives": a.alternatives[:5]})
    wm = _try(lambda: store.load_workload_manifest(pid)) if store.has_workload_manifest(pid) else None
    workloads = [] if wm is None else [{"name": w.source_item.rsplit("/", 1)[-1], "kind": w.kind.value, "classification": w.classification.value,
                                        "reasons": w.classification_reasons[:3], "alternatives": [{"summary": x.summary, "limitations": x.limitations} for x in w.alternatives]}
                                       for w in wm.workloads if w.classification.value != "directly_convertible"]
    counts = {"converted": 0, "converted_with_differences": 0, "manual_required": 0, "blocked": 0}
    for p in conv.pipelines:
        for a in p.activities:
            counts[a.status.value] = counts.get(a.status.value, 0) + 1
    return sanitize({
        "available": True, "activity_status": counts,
        "needs_attention": items[:150], "needs_attention_total": len(items),
        "retired_or_preview": [{"subject": f.subject, "kind": f.kind, "adf_type": f.adf_type, "status": f.status, "date": f.date, "summary": f.summary,
                                "alternatives": f.alternatives[:4]} for f in conv.compatibility],
        "connectors": [{"linked_service": c.linked_service, "type": c.adf_type, "level": c.level, "note": c.note} for c in conv.connector_coverage],
        "source_problems": [{"code": f.code, "severity": f.severity, "subject": f.subject, "message": f.message, "fix": f.fix} for f in conv.preflight],
        "workloads": workloads,
        "triggers": [{"name": t.name, "type": t.trigger_type, "schedule": t.schedule_status or "manual", "action": t.action} for p in conv.pipelines for t in p.triggers],
    }, roots)


# =====================================================================================================================
# provisioning plan (redacted)
# =====================================================================================================================


def step_ref(step_id: str) -> str:
    """An opaque handle for a plan step. Step ids can contain resource identifiers; the browser only ever sees and sends this."""
    import hashlib

    return hashlib.sha256(step_id.encode("utf-8")).hexdigest()[:10]


def provisioning(store: ProjectStore, pid: str, roots: tuple[Path, ...] = ()) -> dict[str, Any]:
    from adf2fabric.project.provisioning import ProvisioningApproval, ProvisioningPlan, ProvisioningRun, VerificationReport

    if not store.has_provisioning(pid, "plan"):
        return {"available": False, "needs": "Build the target plan (read-only): it checks what already exists and what the migration needs."}
    p = store.load_provisioning(pid, "plan", ProvisioningPlan)
    approval = store.load_provisioning(pid, "approval", ProvisioningApproval) if store.has_provisioning(pid, "approval") else None
    run = store.load_provisioning(pid, "run", ProvisioningRun) if store.has_provisioning(pid, "run") else None
    ver = store.load_provisioning(pid, "verification", VerificationReport) if store.has_provisioning(pid, "verification") else None
    approved = set(approval.step_ids) if approval and approval.plan_hash == p.plan_hash else set()  # step ids (internal)
    results = {r.step_id: r for r in (run.results if run and run.plan_hash == p.plan_hash else [])}
    verified = {r.step_id: r for r in (ver.results if ver and ver.plan_hash == p.plan_hash else [])}
    steps = []
    for s in p.steps:
        r, v = results.get(s.id), verified.get(s.id)
        steps.append({"ref": step_ref(s.id), "type": s.type.value, "name": s.name, "action": s.action.value, "api": s.api.value, "needs_approval": s.requires_approval,
                      "approved": s.id in approved, "blockers": s.blockers[:3], "notes": s.notes[:3], "estimate": s.estimate, "delegated_to": s.delegated_to,
                      "exists": bool(s.existing), "result": (r.status.value if r else None), "result_detail": (r.detail[:200] if r else ""),
                      "verification": (v.status.value if v else None)})
    return sanitize({
        "available": True, "plan_ref": p.plan_hash[:16], "plan_ref_full": p.plan_hash, "state_read_at": p.state_read_at.isoformat() if p.state_read_at else None,
        "summary": p.summary.model_dump(), "estimates": p.estimates, "blockers": p.blockers, "notes": p.notes[:10],
        "checks": [{"name": c.name, "status": c.status.value, "detail": c.detail, "proved": c.proved, "not_proved": c.not_proved, "blocking": c.blocking, "action": c.action}
                   for c in p.checks],
        "steps": steps, "approval": ({"by": approval.approved_by, "at": approval.approved_at.isoformat(), "steps": len(approved), "current": bool(approved)} if approval else None),
        "run": ({"dry_run": run.dry_run, "finished": run.finished_at is not None, "interrupted": run.interrupted} if run else None),
        "verification": ({"all_verified": ver.all_verified, "at": ver.verified_at.isoformat()} if ver else None),
    }, roots)


# =====================================================================================================================
# the final report
# =====================================================================================================================


def validation_summary(store: ProjectStore, pid: str, roots: tuple[Path, ...] = ()) -> dict[str, Any]:
    rep = _try(lambda: store.load_validation_report(pid))
    if rep is None:
        return {"available": False, "needs": "Run validation: it re-reads the source and the generated files and compares them."}
    checks = [{"id": c.id, "stage": c.stage.value, "result": c.result.value, "summary": c.summary, "component": c.component or "", "remediation": c.remediation,
               "required": c.required, "evidence": c.evidence[:4]} for c in rep.checks if c.result.value != "PASS"]
    return sanitize({"available": True, "overall": rep.overall, "reasons": rep.overall_reasons, "full_success": rep.full_success_claimed,
                     "created_at": rep.created_at.isoformat(), "stages": [{"stage": s.stage.value, "status": s.status.value, "passed": s.passed, "failed": s.failed,
                                                                           "blocked": s.blocked, "not_run": s.not_run} for s in rep.stages],
                     "components": rep.inventory.by_status, "open_checks": checks[:120], "open_checks_total": len(checks),
                     "components_needing_review": [{"id": c.id, "status": c.status.value, "note": c.note, "diff": c.diff[:3]} for c in rep.components
                                                   if c.status.value in ("missing", "extra", "altered", "unverified")][:120]}, roots)


def report(store: ProjectStore, pid: str, roots: tuple[Path, ...] = ()) -> dict[str, Any]:
    """The final migration report. Its headline is the validation engine's honest overall status; with no validation it says so."""
    states = pipeline_states(store, pid)
    counts = {s: 0 for s in STATES}
    for v in states.values():
        counts[v["state"]] += 1
    validation = validation_summary(store, pid, roots)
    unresolved: list[dict[str, str]] = []
    for name, v in sorted(states.items()):
        if v["state"] in ("blocked", "unsupported", "inferred") or v["reasons"]:
            unresolved.append({"scope": name, "state": v["state"], "issue": "; ".join(v["reasons"]) or v["state"]})
    if validation.get("available"):
        for c in validation["open_checks"]:
            if c["required"] and c["result"] in ("FAIL", "BLOCKED"):
                unresolved.append({"scope": c["component"] or c["id"], "state": c["result"].lower(), "issue": c["summary"], "action": c["remediation"]})
    if validation.get("available") and validation["overall"] == "MIGRATION_VERIFIED":
        headline, tone = "Migration verified: every required check passed, including a runtime comparison.", "success"
    elif validation.get("available"):
        headline, tone = {"FAILED": ("Validation failed: the migration is not correct yet.", "error"),
                          "BLOCKED": ("Blocked: required checks cannot be answered until something is supplied.", "warning"),
                          "NOT_VALIDATED": ("Not fully validated: this is not a claim of success.", "warning")}.get(validation["overall"], ("Unknown.", "warning"))
    else:
        headline, tone = "No validation has run, so nothing here is confirmed. Run validation.", "warning"
    return sanitize({"headline": headline, "tone": tone, "overall": validation.get("overall", "NOT_VALIDATED"), "full_success": bool(validation.get("full_success")),
                     "states": counts, "state_help": STATE_HELP, "pipelines": [{"name": n, **v} for n, v in sorted(states.items())],
                     "unresolved": unresolved[:150], "unresolved_total": len(unresolved), "validation": validation,
                     "provisioning": provisioning(store, pid, roots) if store.has_provisioning(pid, "plan") else {"available": False}}, roots)
