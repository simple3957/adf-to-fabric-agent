"""The handover report: one page per project that says, pipeline by pipeline, how ready it is and what to do next.

It only reads what earlier stages recorded (conversion, its source checks, run-and-compare, the compute assessment), so
it is cheap to regenerate. Readiness is a level, not a score: each level names the one thing that stands in the way, in
the order it has to be dealt with.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from adf2fabric.agent.deployment.verify import load_record, record_path
from adf2fabric.project.conversion import ProjectConversion
from adf2fabric.project.store import ProjectStore

# (level, label, next step) from most to least blocking
LEVELS = [
    ("blocked", "Blocked", "An activity has no Fabric equivalent: replace it in the source, or rebuild it by hand."),
    ("needs_work", "Needs manual work", "Finish the activities the conversion marked manual (see the conversion report)."),
    ("source_fix", "Fix the source first", "Fix the source problem named below, then convert again."),
    ("needs_bindings", "Needs connections", "Bind the connections or items listed, then convert again."),
    ("run_failed", "Last run failed", "Read the failure, fix the cause (often a data source being offline), run it again."),
    ("differs", "Results differ", "Check the differences against the ADF run; the source data may have changed."),
    ("not_run", "Ready, not run yet", "Deploy it and run `adf2fabric verify --apply`."),
    ("ran_no_baseline", "Ran, nothing to compare with", "It ran in Fabric; the next verified run is compared with this one."),
    ("partly_proven", "Partly proven", "Most activities matched; check the ones that could not be compared."),
    ("proven", "Proven", "Its Fabric run matched the ADF run."),
]
_RANK = {lvl: i for i, (lvl, _, _) in enumerate(LEVELS)}
_LABEL = {lvl: label for lvl, label, _ in LEVELS}
_NEXT = {lvl: nxt for lvl, _, nxt in LEVELS}
_FROM_VERDICT = {"matches": "proven", "partial": "partly_proven", "differs": "differs", "no_baseline": "ran_no_baseline",
                 "failed": "run_failed"}


@dataclass
class Readiness:
    pipeline: str
    level: str
    why: str = ""

    @property
    def label(self) -> str:
        return _LABEL[self.level]

    @property
    def next_step(self) -> str:
        return _NEXT[self.level]


def assess(conv: ProjectConversion, verification: dict[str, dict]) -> list[Readiness]:
    """One readiness level per converted pipeline, the most blocking reason winning."""
    errors: dict[str, list[str]] = {}
    for f in conv.preflight:
        if f.severity == "error":
            for p in f.pipelines:
                errors.setdefault(p, []).append(f.subject)
    out = []
    for p in conv.pipelines:
        counts = p.status_counts or {}
        owed = sorted({q.name for q in p.prerequisites if not q.bound and not q.name.startswith("pipeline:")})
        v = verification.get(p.source_pipeline)
        if counts.get("blocked"):
            r = Readiness(p.source_pipeline, "blocked", f"{counts['blocked']} blocked activity(ies)")
        elif counts.get("manual_required"):
            r = Readiness(p.source_pipeline, "needs_work", f"{counts['manual_required']} activity(ies) need manual work")
        elif errors.get(p.source_pipeline):
            r = Readiness(p.source_pipeline, "source_fix", "source problem in " + ", ".join(sorted(set(errors[p.source_pipeline]))))
        elif owed:
            r = Readiness(p.source_pipeline, "needs_bindings", "unbound: " + ", ".join(owed))
        elif v and v.get("verdict") in _FROM_VERDICT:
            lvl = _FROM_VERDICT[v["verdict"]]
            r = Readiness(p.source_pipeline, lvl, v.get("detail") or (f"{v.get('matched', 0)} fact(s) matched the "
                                                                      f"{str(v.get('baseline')).upper()} run" if v.get("baseline") else ""))
        else:
            r = Readiness(p.source_pipeline, "not_run", "")
        out.append(r)
    return sorted(out, key=lambda r: (_RANK[r.level], r.pipeline.lower()))


def _compute_line(store: ProjectStore, pid: str) -> Optional[str]:
    path = store.project_dir(pid) / "assessment" / "compute-report.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    sku, cu = data.get("recommended_sku"), data.get("cu_hours_per_day")
    if not sku and cu is None:
        return None
    return f"Recommended capacity: **{sku or '-'}**" + (f", about {cu} CU-hours a day" if cu is not None else "") + "."


def render(store: ProjectStore, pid: str) -> str:
    conv = store.load_conversion(pid)
    project = store.load(pid)
    verification = load_record(record_path(store.project_dir(pid)))
    ready = assess(conv, verification)
    stale = bool(project.repository and conv.source_commit_sha != project.repository.commit_sha)
    by_level: dict[str, int] = {}
    for r in ready:
        by_level[r.level] = by_level.get(r.level, 0) + 1
    out = [f"# Migration handover: {project.name}", "",
           f"Source commit `{conv.source_commit_sha[:12]}` · {conv.pipeline_count} pipeline(s), {conv.activity_count} "
           f"activities · conversion outcome **{conv.status.value}**", ""]
    if stale:
        out += ["> **The source changed after this conversion.** Run discovery and convert again before relying on this page.", ""]
    out += ["## Where each pipeline stands", "",
            " · ".join(f"{_LABEL[lvl]}: **{n}**" for lvl, n in sorted(by_level.items(), key=lambda x: _RANK[x[0]])), "",
            "| Pipeline | Readiness | Why | Next step |", "| --- | --- | --- | --- |"]
    out += [f"| `{r.pipeline}` | **{r.label}** | {r.why.replace('|', '/') or '-'} | {r.next_step} |" for r in ready]
    if conv.preflight:
        out += ["", "## Problems in the source", ""]
        out += [f"- **{f.subject}** ({f.severity}): {f.message} *Fix:* {f.fix}" for f in conv.preflight]
    owed = sorted({q.name for q in [*conv.connections, *conv.workspace_items] if not q.bound and not q.name.startswith("pipeline:")})
    out += ["", "## Still to supply", ""]
    out += [f"- `{n}`" for n in owed] or ["Nothing: every connection and item the pipelines need is bound."]
    line = _compute_line(store, pid)
    if line:
        out += ["", "## Capacity", "", line]
    out += ["", "## Detail", "",
            "- Conversion report: `conversion/conversion-report.md`",
            "- Run-and-compare: `verification/verification-report.md`" if verification else
            "- Run-and-compare: not run yet (`adf2fabric verify`)",
            "- Compute: `assessment/compute-report.md`" if line else "- Compute: not assessed yet (`adf2fabric assess-compute`)"]
    return "\n".join(out) + "\n"
