"""The final migration report: honest overall status, per-stage results, every check with evidence and remediation, component diffs."""

from __future__ import annotations

from adf2fabric.project.validation import Result, ValidationReport

_HEADLINE = {
    "MIGRATION_VERIFIED": "MIGRATION VERIFIED: every required check passed, including a runtime comparison.",
    "FAILED": "MIGRATION FAILED VALIDATION: at least one required check failed or a component is missing.",
    "BLOCKED": "MIGRATION BLOCKED: required checks could not be answered until something is supplied.",
    "NOT_VALIDATED": "MIGRATION NOT FULLY VALIDATED: this is NOT a claim of success.",
}


def _c(text: str) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ")


def render(r: ValidationReport) -> str:
    out = [f"# Migration validation report: {r.project_id}", "", f"## Overall: {r.overall}", "", f"**{_HEADLINE[r.overall]}**", ""]
    out += [f"- {x}" for x in r.overall_reasons]
    out += ["", "Generating files, a clean schema check or a successful API call is never evidence that a pipeline runs correctly. Only the "
            "runtime stage, from a real run compared with the source, can show that.", "",
            f"Scope: {', '.join(f'{k}={v}' for k, v in r.scope.items())}", "", "## Stages", "", "| Stage | Status | Pass | Fail | Blocked | Not run |", "|---|---|---|---|---|---|"]
    out += [f"| {s.stage.value} | {s.status.value} | {s.passed} | {s.failed} | {s.blocked} | {s.not_run} |" for s in r.stages]
    inv = r.inventory
    out += ["", "## Inventory comparison", "", f"{inv.source_components} source component(s); " + ", ".join(f"{v} {k}" for k, v in inv.by_status.items()) + ".", ""]
    interesting = [c for c in r.components if c.status.value not in ("matched", "not_applicable")]
    if interesting:
        out += ["| Component | Status | Source | Target | Note |", "|---|---|---|---|---|"]
        out += [f"| {_c(c.id)} | {c.status.value} | {_c(c.source or '')} | {_c(c.target or '')} | {_c(c.note)} |" for c in interesting]
        diffs = [c for c in r.components if c.diff]
        if diffs:
            out += ["", "### Normalised differences (secrets redacted)", ""]
            for c in diffs:
                out += [f"**{c.id}**", ""] + [f"    {d}" for d in c.diff] + [""]
    out += ["", "## Checks", ""]
    for stage in ("static", "api", "deployment", "runtime"):
        rows = [x for x in r.checks if x.stage.value == stage]
        if not rows:
            continue
        out += [f"### {stage}", "", "| Result | Check | Component | Summary | Remediation |", "|---|---|---|---|---|"]
        for x in rows:
            out.append(f"| {x.result.value}{'' if x.required else ' (info)'} | {x.id} | {_c(x.component or '')} | {_c(x.summary)} | {_c(x.remediation)} |")
        for x in rows:
            if x.evidence and x.result is not Result.PASS:
                out += ["", f"Evidence for `{x.id}`" + (f" ({x.component})" if x.component else "") + ":"] + [f"- {_c(e)}" for e in x.evidence]
        out.append("")
    return "\n".join(out) + "\n"
