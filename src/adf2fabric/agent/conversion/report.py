"""Deterministic Markdown report + resolutions template for a conversion.

Written for the person who has to finish the migration: what was converted faithfully, what
differs, what needs manual work, what was blocked (with alternatives), and exactly which
connections and workspace items must be bound before anything can run.
"""

from __future__ import annotations

import json

from adf2fabric.agent.conversion.compute_plan import render_compute_section
from adf2fabric.agent.conversion.fabric_schema import INFERRED, SOURCES
from adf2fabric.project.conversion import (
    ActivityConversion,
    ConversionStatus,
    Prerequisite,
    ProjectConversion,
)

_ICON = {
    ConversionStatus.converted: "converted",
    ConversionStatus.converted_with_differences: "converted (with differences)",
    ConversionStatus.manual_required: "MANUAL WORK REQUIRED",
    ConversionStatus.blocked: "BLOCKED",
}


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def render_report(c: ProjectConversion) -> str:
    out: list[str] = [f"# ADF -> Fabric conversion report: `{c.project_id}`", ""]
    out += [f"Source commit `{c.source_commit_sha[:12]}` - outcome **{c.status.value}** - "
            f"{c.pipeline_count} pipeline(s), {c.activity_count} activities.", ""]
    out += ["**Read this first.** *Converted* means the behavior is preserved by the generated components. It does "
            "**not** mean the pipeline can run: connections, workspace items and credentials must still be bound "
            "(see *Prerequisites*), and validation here is against the documented Fabric schema, not a Fabric service.", ""]
    if c.reconstruction_applied or c.reconstruction_open:
        out += [f"**Reconstruction applied:** {', '.join(c.reconstruction_applied) or 'none'}. Unresolved gaps carried "
                f"from reconstruction: {', '.join(c.reconstruction_open) or 'none'}. Inferred elements below are NOT in "
                "the original ADF source; each is also marked in the generated JSON (`userProperties`).", ""]
    out += ["| Status | Activities |", "| --- | --- |"]
    for k in (s.value for s in ConversionStatus):
        out.append(f"| {k} | {c.status_counts.get(k, 0)} |")
    out += ["", "| Strategy | Activities |", "| --- | --- |"]
    for k, v in sorted(c.strategy_counts.items()):
        out.append(f"| {k} | {v} |")

    for p in c.pipelines:
        out += ["", f"## Pipeline `{p.source_pipeline}` - {_ICON[p.status]}", ""]
        out.append(f"Structurally valid: **{'yes' if p.structural_valid else 'NO'}** | runtime ready: "
                   f"**{'yes' if p.runtime_ready else 'no'}** | artifact: `{p.artifact_path or '-'}`")
        cov = p.coverage
        out.append(f"Coverage: {cov.accounted}/{cov.source_activities} source activities accounted for"
                   + ("" if cov.complete else f" - **INCOMPLETE** (missing {cov.missing}, uncovered operations "
                      f"{cov.uncovered_operations}, unaccounted expressions {cov.unaccounted_expressions})"))
        for n in p.notes:
            out.append(f"- Note: {n}")
        for i in p.validation.issues:
            if i.severity.value == "error":
                out.append(f"- **Validation error** `{i.code}`: {i.message} {i.action}".strip())
        out += ["", "| Source activity | Type | -> Fabric | Strategy | Status |", "| --- | --- | --- | --- | --- |"]
        for a in p.activities:
            targets = ", ".join(f"{x.fabric_type} `{x.name}`" for x in a.components if x.role != "helper") or "-"
            helpers = [x.name for x in a.components if x.role == "helper"]
            if helpers:
                targets += f" (+{len(helpers)} helper)"
            out.append(f"| `{_cell(a.source_activity_id.split('/', 1)[-1])}` | {a.source_type} | {_cell(targets)} | "
                       f"{a.strategy.value} | {_ICON[a.status]} |")
        rewrites: dict[str, list[str]] = {}
        for a in p.activities:
            for e in a.expression_conversions:
                if e.status == "rewritten":
                    for ch in e.changes:
                        rewrites.setdefault(ch, []).append(a.source_name)
        if rewrites:
            out += ["", "### Expression rewrites applied", ""]
            for ch, who in sorted(rewrites.items()):
                shown = ", ".join(f"`{w}`" for w in sorted(set(who))[:4])
                more = f" and {len(set(who)) - 4} more" if len(set(who)) > 4 else ""
                out.append(f"- {ch} - in {shown}{more}")
        inferred = [a for a in p.activities if a.inferred_elements]
        if inferred:
            out += ["", "### Inferred elements (NOT in the original source)", ""]
            for a in inferred:
                for e in a.inferred_elements:
                    review = "review required" if e.review_required else "explicit evidence"
                    out.append(f"- `{a.source_name}`: {e.missing_element} - proposal {e.proposal_id} "
                               f"(rule `{e.rule}`, {e.confidence}, {review})"
                               + (f". Assumptions: {'; '.join(e.assumptions)}" if e.assumptions else ""))
        attention = [a for a in p.activities if a.status is not ConversionStatus.converted]
        if attention:
            out += ["", "### Needs attention", ""]
            for a in attention:
                out += _activity_block(a)
        if p.triggers:
            out += ["", "### Triggers (schedules are generated where they map exactly; events and the rest are recreated by hand)", ""]
            for t in p.triggers:
                out.append(f"- `{t.name}`: {t.summary}. {t.action}")
                if t.schedule_status:
                    where = f" - `{t.schedule_file}`" if t.schedule_file else ""
                    out.append(f"  - Fabric schedule: **{t.schedule_status}** ({len(t.schedules)} schedule(s)){where}")
                    out += [f"  - {n}" for n in t.schedule_notes]
                out += [f"  - {d}" for d in t.details]
        if p.prerequisites:
            out += ["", "### Prerequisites for this pipeline", ""] + _prereq_lines(p.prerequisites)

    if c.assisted:
        out += ["", "## Model-assisted pipelines (converted from a reviewed edit of the source, not the original file)", "",
                "A model drafted a corrected ADF source for each pipeline below, a person reviewed and accepted it, and the deterministic converter then converted "
                "it like any other source. The original file in the repository is unchanged and differs from what was converted.", "",
                "| Pipeline | Accepted | By | Edited by the reviewer | Model |", "| --- | --- | --- | --- | --- |"]
        out += [f"| `{n.pipeline}` | {n.accepted_at or '-'} | {_cell(n.accepted_by or '-')} | {'yes' if n.edited else 'no'} | {_cell(n.model or '-')} |" for n in c.assisted]
    if c.dataset_ledger:
        counts: dict[str, int] = {}
        for e in c.dataset_ledger:
            counts[e.treatment] = counts.get(e.treatment, 0) + 1
        out += ["", "## Datasets (Fabric has no dataset item: each one's settings are copied into the activities that use it)", "",
                f"{len(c.dataset_ledger)} source dataset(s): " + ", ".join(f"{n} {k.replace('_', ' ')}" for k, n in sorted(counts.items())) + ".", "",
                "| Dataset | Type | What became of it | Used by |", "| --- | --- | --- | --- |"]
        for e in c.dataset_ledger:
            used = ", ".join(f"`{u}`" for u in e.used_by[:3]) + (f" +{len(e.used_by) - 3}" if len(e.used_by) > 3 else "")
            out.append(f"| `{e.name}` | {e.type or '-'} | {e.treatment.replace('_', ' ')}: {_cell(e.note)} | {used or '-'} |")
    if c.preflight:
        out += ["", "## Source checks (problems in the authored source that conversion cannot fix)", "",
                "| Severity | Subject | Used in | Finding | Fix |", "| --- | --- | --- | --- | --- |"]
        for f in c.preflight:
            used = ", ".join(f"`{n}`" for n in f.pipelines[:3]) + (f" +{len(f.pipelines) - 3}" if len(f.pipelines) > 3 else "")
            out.append(f"| {f.severity} | `{f.subject}` | {used or '-'} | {f.message} | {f.fix} |")
    if c.compatibility:
        out += ["", "## Compatibility findings (retired, deprecated, preview and unavailable components)", "",
                "Each finding cites Microsoft's page and the date the catalog was verified. A component that is retired "
                "in ADF cannot be fixed by conversion: upgrade or replace it in the source first.", "",
                "| Component | Type | Status | Date | Used in | Effect |", "| --- | --- | --- | --- | --- | --- |"]
        for f in c.compatibility:
            used = ", ".join(f"`{n}`" for n in f.pipelines[:3]) + (f" +{len(f.pipelines) - 3}" if len(f.pipelines) > 3 else "")
            out.append(f"| `{f.subject}` | {f.adf_type} | {f.status} | {f.date or '-'} | {used or '-'} | {f.impact.value} |")
        seen: set[str] = set()
        for f in c.compatibility:
            if f.id in seen:
                continue
            seen.add(f.id)
            out += ["", f"**{f.id}** - {f.summary}"]
            out += [f"- Way forward: {alt}" for alt in f.alternatives]
            out.append(f"- Source: <{f.source_url}>")
    if c.compute_plan is not None:
        out += render_compute_section(c.compute_plan)
    if c.connector_coverage:
        counts = {lvl: sum(1 for x in c.connector_coverage if x.level == lvl) for lvl in ("created", "manual", "unsupported", "unlisted")}
        out += ["", "## Connector coverage", "",
                f"{len(c.connector_coverage)} linked service(s): **{counts['created']}** can be created in Fabric by this tool, "
                f"**{counts['manual']}** have a Fabric connector but are created by hand, **{counts['unsupported']}** have no Fabric "
                f"connector, **{counts['unlisted']}** are not named in the Fabric connector documentation consulted (verify them).", "",
                "| Linked service | ADF type | Level | Note |", "| --- | --- | --- | --- |"]
        out += [f"| `{x.linked_service}` | {_cell(x.adf_type or '?')} | {x.level} | {_cell(x.note)} |" for x in c.connector_coverage]
    out += ["", "## Prerequisites (all pipelines)", ""]
    out += _prereq_lines([*c.connections, *c.workspace_items]) or ["None."]
    out += ["", "## Schema provenance", "",
            "Generated JSON is checked against a table encoded from Microsoft's documentation:"]
    out += [f"- <{s}>" for s in SOURCES]
    out += ["", "Parts emitted that the documentation does not list (verify against a pipeline exported from your "
            "workspace):"]
    out += [f"- {s}" for s in INFERRED]
    return "\n".join(out) + "\n"


def _prereq_lines(items: list[Prerequisite]) -> list[str]:
    out = []
    for q in items:
        state = "bound" if q.bound else f"UNBOUND ({q.binding})" if q.binding else "to configure"
        out.append(f"- **{q.kind}** `{q.name}` - {state}. {q.detail}")
    return out


def _activity_block(a: ActivityConversion) -> list[str]:
    out = [f"#### `{a.source_name}` ({a.source_type}) - {_ICON[a.status]}", "", a.summary]
    if a.runtime_disposition:
        out.append(f"- Runtime disposition: **{a.runtime_disposition}**")
    for w in a.warnings:
        if w.impact.value != "none":
            out.append(f"- [{w.impact.value}] `{w.code}`: {w.message}" + (f" -> {w.action}" if w.action else ""))
    for s in a.manual_steps:
        out.append(f"- Manual step: {s}")
    for s in a.alternatives:
        out.append(f"- Alternative: {s}")
    unconverted = [m for m in a.property_mappings if not m.preserved]
    for m in unconverted:
        out.append(f"- Not carried over: `{m.source}` - {m.note}")
    for e in a.expression_conversions:
        if e.status != "unchanged":
            out.append(f"- Expression `{e.source_path}`: {e.status}" + (f" ({'; '.join(e.changes)})" if e.changes else ""))
    out.append("")
    return out


def render_resolutions_template(c: ProjectConversion, linked_services: set[str]) -> str:
    """A resolutions file listing every UNBOUND binding, ready to fill with real GUIDs."""
    entries = []
    for q in c.connections:
        if q.kind != "connection" or q.bound:
            continue
        if q.name.lower() in linked_services or q.detail.startswith("ADF linked service"):  # also one the source lacks
            entries.append({"type": "LinkedServiceToConnectionId", "key": q.name, "value": "<Fabric connection GUID>"})
        else:
            entries.append({"type": "ItemToId", "key": f"connection:{q.name}", "value": "<Fabric connection GUID>"})
    for q in c.workspace_items:
        if q.bound or q.kind != "workspace_item":
            continue
        if q.name == "workspace":
            entries.append({"type": "WorkspaceId", "key": "*", "value": "<Fabric workspace GUID>"})
        elif ":" in q.name and not q.name.startswith("database:"):
            entries.append({"type": "ItemToId", "key": q.name, "value": "<Fabric item GUID>"})
    return json.dumps(entries, indent=2) + "\n"
