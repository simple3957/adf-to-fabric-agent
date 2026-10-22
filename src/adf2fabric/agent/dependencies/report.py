"""Markdown dependency report: what blocks migration, what to do, and in what order. Names and statuses only."""

from __future__ import annotations

from adf2fabric.project.dependencies import AccessStatus, DependencyInventory, Severity


def _cell(text: str) -> str:
    return (text or "").replace("|", "\|").replace("\n", " ")


def render_report(inv: DependencyInventory) -> str:
    s = inv.summary
    out = [f"# Dependency report: {inv.project_id}", "",
           f"Source: {inv.source.source_type} {inv.source.identifier}".rstrip() +
           (f" @ {inv.source.branch}" if inv.source.branch else "") + f" (version {inv.source.version[:12] or 'unknown'}). "
           f"Source complete: {'yes' if inv.source.complete else 'NO, see source gaps'}.", "",
           f"{s.total} dependencies, {s.blocking} blocking; {s.findings_error} errors, {s.findings_warning} warnings. "
           "No secret value, source data or notebook source was read.", ""]
    if inv.checks_requested:
        out += [f"Access checks run for: {', '.join(inv.checks_requested)}.", ""]
    else:
        out += ["No access checks were requested, so no external system was contacted (status `not_checked`).", ""]
    out += ["## Summary", "", "| Resolution | Count |", "|---|---|", *[f"| {k} | {v} |" for k, v in s.by_resolution.items()], "",
            "| Migration category | Count |", "|---|---|", *[f"| {k} | {v} |" for k, v in s.by_category.items()], ""]
    if inv.findings:
        out += ["## Findings", "", "| Severity | Code | Subject | Message | Action |", "|---|---|---|---|---|"]
        out += [f"| {f.severity.value} | {f.code} | {_cell(f.subject)} | {_cell(f.message)} | {_cell(f.action)} |" for f in inv.findings]
        out.append("")
    if inv.source_gaps:
        out += ["## Source gaps", "", "| Kind | Name | Reason | Permission needed | Action |", "|---|---|---|---|---|"]
        out += [f"| {g.kind} | {_cell(g.name or '')} | {g.reason} | {_cell(g.permission or '')} | {_cell(g.action)} |" for g in inv.source_gaps]
        out.append("")
    if inv.cycles:
        out += ["## Circular references", ""] + [f"- {' -> '.join(c)}" for c in inv.cycles] + [""]
    out += ["## Migration order", "", "| # | Wave | Pipeline | Calls | Ready | Blocked by |", "|---|---|---|---|---|---|"]
    out += [f"| {o.position} | {o.wave} | {_cell(o.pipeline)} | {_cell(', '.join(o.calls))} | {'yes' if o.ready else 'no'} | "
            f"{_cell(', '.join(o.blocked_by))} |" for o in inv.pipeline_order]
    out += ["", "## Waves", ""]
    for w in inv.waves:
        out.append(f"{w.number}. **{w.name}** ({len(w.items)}): {w.note}")
    out += ["", "## Dependencies", "", "| Type | Name | Resolution | Consumers | Auth | Migration | Access |", "|---|---|---|---|---|---|---|"]
    for d in inv.dependencies:
        if d.type.value in ("pipeline_parameter", "variable", "activity_output") and d.resolution.value == "resolved":
            continue  # internal names that resolved: counted in the summary, not listed
        out.append(f"| {d.type.value} | {_cell(d.name)} | {d.resolution.value} | {len(d.consumers)} | "
                   f"{d.authentication.mode if d.authentication else ''} | {d.migration.category.value} | "
                   f"{d.access.status.value if d.access else ''} |")
    out += ["", "Resolved pipeline parameters, variables and activity outputs are not listed; they are in `dependencies.yaml`.", ""]
    return "\n".join(out)
