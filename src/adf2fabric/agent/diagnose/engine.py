"""Why a pipeline or a service is not yet running perfectly in Fabric, and what to do about it. Read-only.

For every pipeline it finds the first stage that is not satisfied, in this order, from the records each stage already wrote (and, when a workspace is given,
from what is actually deployed and how the latest run behaved):

    not_converted  ->  unbound  ->  not_deployed  ->  deployed_differs  ->  never_run  ->  run_failed  ->  differs_from_baseline  ->  (run_ok_no_baseline)  ->  proven

Each problem carries a cause, the fix and, where one exists, the command. Problems are also ranked across the estate so the fix that unblocks the most
pipelines comes first. Services (linked services and the Fabric connections they need) and pipelines found in the workspace that the project does not
know about are listed separately. Nothing here changes anything: the tool never deletes or modifies a Fabric item.
"""

from __future__ import annotations

from typing import Any, Optional

from adf2fabric.agent.diagnose import remedies as R

STAGE_ORDER = ("not_converted", "unbound", "not_deployed", "deployed_differs", "never_run", "run_failed", "differs_from_baseline", "run_ok_no_baseline",
               "ready_to_deploy", "proven")
HEADLINE = {
    "not_converted": "The converter could not finish it: something needs a person.",
    "unbound": "Converted, but connections or items it needs are not bound yet.",
    "not_deployed": "Converted and bound, but not in the Fabric workspace.",
    "deployed_differs": "Deployed, but what is in Fabric differs from what was generated.",
    "never_run": "Deployed, but never run in Fabric.",
    "run_failed": "Its latest Fabric run did not complete.",
    "differs_from_baseline": "It ran, but the result differs from the ADF baseline.",
    "run_ok_no_baseline": "It ran and completed, but there is nothing to compare it with.",
    "ready_to_deploy": "Converted and bound: ready to deploy (no workspace was read).",
    "proven": "Deployed, run, and matched its ADF baseline.",
}
NEEDS_A_PERSON = ("manual_required", "blocked")


def _problem(kind: str, cause: str, detail: str, fix: str, command: str = "", **extra: Any) -> dict[str, Any]:
    return {"kind": kind, "cause": cause, "detail": detail[:300], "fix": fix, "command": command, **extra}


def _conversion_problems(rec: Any) -> list[dict[str, Any]]:
    out, seen = [], set()
    for a in rec.activities:
        if a.status.value not in NEEDS_A_PERSON:
            continue
        found = False
        for w in a.warnings:
            if w.impact.value not in ("blocker", "manual"):
                continue
            found = True
            key = (w.code, a.source_name)
            if key in seen:
                continue
            seen.add(key)
            r = R.CONVERSION.get(w.code) or R.Remedy(w.message, w.action or "Read the conversion report for this activity.")
            out.append(_problem("conversion", r.cause, w.message, r.fix, r.command, code=w.code, activity=a.source_name, activity_type=a.source_type))
        if not found:
            out.append(_problem("conversion", a.summary or "Not converted.", a.summary, "Read the conversion report for this activity.", "", code="", activity=a.source_name,
                                activity_type=a.source_type))
    for issue in getattr(getattr(rec, "validation", None), "issues", []) or []:
        if getattr(issue.severity, "value", "") == "error":
            out.append(_problem("structure", f"The generated pipeline fails Fabric's schema checks ({issue.code}).", issue.message, getattr(issue, "action", "") or "Fix the cause in the source and reconvert.", "", code=issue.code))
    return out


def _unbound(rec: Any, converted: set[str]) -> list[dict[str, Any]]:
    out = []
    for q in rec.prerequisites:
        if q.bound or q.kind not in R.BINDING:
            continue
        if q.kind == "workspace_item" and q.name.lower().startswith("pipeline:") and q.name.split(":", 1)[1].lower() in converted:
            continue  # a converted child pipeline: `deploy` creates it first and fills its id in
        if q.kind == "workspace_item" and q.name.lower() == "workspace":
            continue  # `deploy --workspace-id` fills in the workspace id itself
        r = R.BINDING[q.kind]
        out.append(_problem("binding", f"{r.cause} ({q.name})", q.detail or q.name, r.fix, r.command, binding=q.name, binding_kind=q.kind))
    return out


def _run_failure_text(run: dict) -> str:
    bits = [str(run.get("failure") or "")] + [str(a.get("error") or "") for a in run.get("activities", []) if a.get("status") not in ("Succeeded", "Skipped")]
    return " | ".join(b for b in bits if b)[:600]


def _after_conversion(row: dict, verdict: Optional[dict]) -> tuple[str, list[dict[str, Any]], list[str]]:
    notes: list[str] = []
    if not row.get("deployed"):
        return "not_deployed", [_problem("deploy", "It is not in the Fabric workspace.", "No pipeline with this name was found in the workspace.",
                                         "Deploy it (a dry run first). Children are created before their callers.",
                                         "adf2fabric deploy --project P --workspace-id W --with-children          # dry run\nadf2fabric deploy --project P --workspace-id W --with-children --apply")], notes
    parity = row.get("parity")
    if row.get("fabric_error"):
        notes.append(f"The deployed definition could not be read: {row['fabric_error']}")
    if parity is not None and parity.verdict == "differs":
        return "deployed_differs", [_problem("deploy", "What is deployed differs from what was generated.", "; ".join(parity.differences[:3]),
                                             "Deploy the current generated file again, then run the pipeline report to confirm the two match.",
                                             "adf2fabric deploy --project P --workspace-id W --apply", differences=parity.differences[:6])], notes
    run = row.get("run")
    if not run:
        return "never_run", [_problem("run", "It has never run in Fabric (or only runs when its caller starts it).", "", "Run it and compare it with the ADF baseline. A pipeline another one calls is run through its caller.",
                                      "adf2fabric verify --project P --workspace-id W --pipeline NAME --apply")], notes
    if run.get("status") != "Completed":
        text = _run_failure_text(run)
        r = R.for_run_failure(text)
        failed = [a["name"] for a in run.get("activities", []) if a.get("status") not in ("Succeeded", "Skipped")]
        return "run_failed", [_problem("run", r.cause if r else "The run ended " + str(run.get("status")) + ".", text or "No message was returned.",
                                       r.fix if r else "Open the run in Fabric Monitor, find the failed activity and read its error.", "", failed_activities=failed[:6],
                                       recognised=bool(r))], notes
    if verdict and verdict.get("verdict") in ("differs", "failed"):
        return "differs_from_baseline", [_problem("run", "Its result differs from the ADF baseline.", "; ".join(f"{d.get('activity')} {d.get('fact')}: ADF {d.get('baseline')}, Fabric {d.get('fabric')}" for d in (verdict.get("differences") or [])[:3]),
                                                  "Find out whether Fabric or ADF is right (a changed source, a different parameter, a data race), then fix the cause and run again.",
                                                  "adf2fabric verify --project P --workspace-id W --pipeline NAME --apply")], notes
    if verdict and verdict.get("verdict") == "matches":
        return "proven", [], notes
    return "run_ok_no_baseline", [_problem("run", "Nothing earlier to compare it with.", "", "Run it in ADF too (or run it twice in Fabric) so there is a baseline.", "adf2fabric verify --project P --workspace-id W --apply")], notes


def diagnose(store: Any, pid: str, report: Optional[dict] = None) -> dict[str, Any]:
    if not store.has_conversion(pid):
        return {"project": pid, "ready": False, "message": "Nothing is converted yet: run `convert` first.", "pipelines": [], "services": [], "extras": [], "fix_first": [], "summary": {}}
    conv = store.load_conversion(pid)
    verification = _load_json(store.project_dir(pid) / "verification" / "verification.json")
    rows = {r["pipeline"].lower(): r for r in (report or {}).get("pipelines", [])}
    converted = {p.source_pipeline.lower() for p in conv.pipelines}
    pipelines: list[dict[str, Any]] = []
    for rec in conv.pipelines:
        problems = _conversion_problems(rec)
        notes: list[str] = []
        stage = ""
        if problems:
            stage = "not_converted"
        else:
            problems = _unbound(rec, converted)
            stage = "unbound" if problems else ""
        row = rows.get(rec.source_pipeline.lower())
        if row is None and report is not None:
            row = {"pipeline": rec.source_pipeline, "deployed": False}  # the workspace was read and it is not there
        if row is not None:
            later, later_problems, notes = _after_conversion(row, (verification or {}).get(rec.source_pipeline))
            if not stage:
                stage, problems = later, later_problems
            elif row.get("deployed"):
                notes.append("It is deployed in the workspace from an earlier conversion; fix the problems above and deploy again.")
        elif not stage:
            stage = "ready_to_deploy"
        pipelines.append({"pipeline": rec.source_pipeline, "stage": stage, "headline": HEADLINE[stage], "problems": problems, "notes": notes,
                          "activities": len(rec.activities)})
    services = _services(conv)
    ws_names = {n.lower(): n for n in (report or {}).get("workspace_pipelines", [])}
    extras = sorted(n for k, n in ws_names.items() if k not in converted)
    summary: dict[str, int] = {}
    for p in pipelines:
        summary[p["stage"]] = summary.get(p["stage"], 0) + 1
    return {"project": pid, "ready": True, "workspace_read": report is not None, "pipelines": sorted(pipelines, key=lambda p: (STAGE_ORDER.index(p["stage"]), p["pipeline"].lower())),
            "services": services, "extras": extras, "fix_first": _fix_first(pipelines), "summary": {k: summary[k] for k in STAGE_ORDER if k in summary}}


def _fix_first(pipelines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for p in pipelines:
        for pr in p["problems"]:
            key = (pr["kind"], pr.get("code") or pr.get("binding") or pr["cause"][:80])
            g = groups.setdefault(key, {"cause": pr["cause"], "fix": pr["fix"], "command": pr["command"], "pipelines": []})
            if p["pipeline"] not in g["pipelines"]:
                g["pipelines"].append(p["pipeline"])
    return sorted(groups.values(), key=lambda g: (-len(g["pipelines"]), g["cause"]))


def _services(conv: Any) -> list[dict[str, Any]]:
    bound = {q.name.lower(): q for q in conv.connections}
    out = []
    for c in conv.connector_coverage:
        q = bound.get(c.linked_service.lower())
        state = "bound" if q is not None and q.bound else "not bound" if q is not None else "not used by a converted pipeline"
        if c.level == "unsupported":
            todo = "Fabric has no connector for it: move the data to a supported store first."
        elif c.level == "unlisted":
            todo = "Not named in Fabric's connector list: check it before relying on it. " + (c.note or "")
        elif state == "bound":
            todo = "Done: test the connection in Fabric (credentials are entered there, never copied from ADF)."
        elif c.level == "created":
            todo = "The tool can create this connection (Connect step, or `connections apply`); then bind its id."
        else:
            todo = "Create the connection by hand in Fabric (Manage connections and gateways), then bind its id in the resolutions file."
        out.append({"service": c.linked_service, "type": c.adf_type, "level": c.level, "state": state, "needed_by": list(q.needed_by) if q is not None else [], "todo": todo.strip()})
    known = {c.linked_service.lower() for c in conv.connector_coverage}
    for q in conv.connections:  # a connection a pipeline needs for a linked service the source does not contain, or one with no linked service (Invoke pipeline)
        if q.kind != "connection" or q.name.lower() in known:
            continue
        free = q.name.lower() == "invoke-pipeline"
        out.append({"service": q.name, "type": "Invoke pipeline (no linked service)" if free else "not in the source", "level": "manual", "state": "bound" if q.bound else "not bound",
                    "needed_by": list(q.needed_by),
                    "todo": ("Create a Fabric Data Pipelines connection (OAuth, in the Fabric portal) and bind it as ItemToId connection:invoke-pipeline." if free else
                             "Its linked service is not in the source, so its type is unknown: complete the source (`linkedService/` or the ARM export), or create the Fabric connection by hand and bind it.")
                    if not q.bound else "Done: test the connection in Fabric."})
    gateways = [q for q in conv.connections if q.kind == "gateway"]
    for q in gateways:
        out.append({"service": q.name, "type": "on-premises data gateway", "level": "manual", "state": "bound" if q.bound else "not bound", "needed_by": list(q.needed_by),
                    "todo": R.BINDING["gateway"].fix})
    return out


def _load_json(path: Any) -> Optional[dict]:
    import json

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def render(d: dict[str, Any]) -> str:
    if not d.get("ready"):
        return f"# Diagnosis: {d['project']}\n\n{d['message']}\n"
    s = d["summary"]
    out = [f"# Diagnosis: {d['project']}", "",
           "Read-only. For each pipeline: the first thing in its way, why, and what to do. " + ("The Fabric workspace was read (what is deployed and the latest runs)."
                                                                                           if d["workspace_read"] else "No workspace was read, so deployment and runs are not covered: add --workspace-id."), "",
           "## Where the pipelines are", "", "| Stage | Pipelines |", "|---|---|"]
    out += [f"| {HEADLINE[k]} | {n} |" for k, n in s.items()]
    if d["fix_first"]:
        out += ["", "## Fix these first (most pipelines unblocked first)", ""]
        for i, g in enumerate(d["fix_first"][:12], 1):
            out += [f"{i}. **{g['cause']}** ({len(g['pipelines'])} pipeline(s): {', '.join(g['pipelines'][:5])}{'...' if len(g['pipelines']) > 5 else ''})", f"   - Do: {g['fix']}"]
            if g["command"]:
                out += ["   - ```", *[f"     {c}" for c in g["command"].split("\n")], "     ```"]
    out += ["", "## Each pipeline", ""]
    for p in d["pipelines"]:
        out += [f"### {p['pipeline']}: {p['headline']}", ""]
        for pr in p["problems"]:
            where = f"`{pr['activity']}` ({pr.get('activity_type')}): " if pr.get("activity") else ""
            out += [f"- {where}**{pr['cause']}**"]
            if pr["detail"] and pr["detail"] != pr["cause"]:
                out += [f"  - Found: {pr['detail']}"]
            out += [f"  - Do: {pr['fix']}"]
            if pr["command"]:
                out += [f"  - Command: `{pr['command'].splitlines()[0]}`"]
        out += [f"- Note: {n}" for n in p["notes"]]
        if not p["problems"] and not p["notes"]:
            out += ["- Nothing to do."]
        out += [""]
    if d["services"]:
        out += ["## Services (linked services and the Fabric connections they need)", "", "| Service | Type | Fabric connection | State | What to do |", "|---|---|---|---|---|"]
        out += [f"| {x['service']} | {x['type']} | {x['level']} | {x['state']} | {x['todo']} |" for x in d["services"]]
        out += [""]
    if d["extras"]:
        out += ["## In the workspace but not in this project", "", "These pipelines are in the Fabric workspace and this project does not convert them (a removed part of the source, or another migration's). "
                "The tool never deletes or changes a Fabric item: remove them in the portal if they are stale.", ""] + [f"- {n}" for n in d["extras"]] + [""]
    return "\n".join(out) + "\n"
