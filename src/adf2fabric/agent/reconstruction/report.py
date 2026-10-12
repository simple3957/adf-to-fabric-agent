"""Deterministic Markdown report for a reconstruction.

Every item is labelled by which of the four representations it belongs to, so a reader can never
mistake an inference for something the author wrote:

    ORIGINAL   what the ADF JSON says (gaps are findings about it; it is never altered)
    PARSED     what the parser extracted (unchanged)
    PROPOSED   what the reconstruction infers - evidence, rule, confidence, assumptions
    GENERATED  the Fabric output (see the conversion report; inferred parts are marked there)
"""

from __future__ import annotations

import json

from adf2fabric.project.reconstruction import Completeness, Confidence, ProjectReconstruction, Proposal

_CONF = {Confidence.high: "HIGH", Confidence.medium: "MEDIUM", Confidence.low: "LOW"}


def _cell(t: str) -> str:
    return str(t).replace("|", "\\|").replace("\n", " ")


def _short(v, n=300) -> str:
    t = json.dumps(v, ensure_ascii=False, sort_keys=True) if not isinstance(v, str) else v
    return t if len(t) <= n else t[: n - 1] + "…"


def render_report(r: ProjectReconstruction) -> str:
    out = [f"# Reconstruction report: `{r.project_id}`", "",
           f"Source commit `{r.source_commit_sha[:12]}` - outcome **{r.status.value}**.", "",
           "| Representation | Where | Status |", "| --- | --- | --- |",
           *[f"| {k.upper()} | {v} | {'never modified' if k in ('original', 'parsed') else 'derived'} |"
             for k, v in r.representations.items()], "",
           "**Inferred content exists only in PROPOSED and in what is generated from it.** Nothing here was written "
           "back to the ADF source. A proposal is generated into Fabric only when its confidence is HIGH or MEDIUM "
           "and nothing is still required; everything else is reported as a blocker or a template.", "",
           "| Gaps found | Count |", "| --- | --- |",
           *[f"| {k} | {v} |" for k, v in sorted(r.finding_counts.items())], "",
           "| Proposals by confidence | Count |", "| --- | --- |",
           *[f"| {k} | {v} |" for k, v in sorted(r.proposal_counts.items())],
           "", f"Safe to generate: **{r.safe_proposal_count}** - open blockers: **{r.blocker_count}**."]

    if r.source_findings:
        out += ["", "## File- and artifact-level findings (ORIGINAL)", ""]
        for f in r.source_findings:
            out.append(f"- `{f.id}` **{f.incompleteness.value}** `{f.code}` - {f.message}")

    if r.source_blockers:
        out += ["", "### Unresolved file-level blockers", ""]
        for b in r.source_blockers:
            out += [f"- **{b.id}** ({', '.join(b.finding_ids)}) {b.reason}.",
                    f"  - Information required: {'; '.join(b.information_required)}",
                    f"  - Minimum to unblock: {b.minimum_information}"]

    for p in r.pipelines:
        out += ["", f"## Pipeline `{p.pipeline}` - {p.completeness.value}", "",
                f"Executable as authored: **{'yes' if p.executable_as_is else 'no'}**"
                + (f" - still unresolved after safe proposals: {', '.join(p.unresolved_finding_ids)}"
                   if p.unresolved_finding_ids else "")]
        if p.patterns:
            out += ["", "### Recognized patterns (PARSED)", ""]
            for pat in p.patterns:
                state = "complete" if pat.complete else f"INCOMPLETE - missing {', '.join(pat.missing)}"
                out.append(f"- **{pat.name}** ({state}): {'; '.join(pat.evidence[:3])}")
        if p.findings:
            out += ["", "### Findings (about the ORIGINAL)", "",
                    "| Id | Class | Code | Severity | Activity | Message |", "| --- | --- | --- | --- | --- | --- |"]
            for f in p.findings:
                act = (f.activity_id or "").split("/", 1)[-1]
                out.append(f"| {f.id} | {f.incompleteness.value} | {f.code} | {f.severity.value} | {_cell(act)} | "
                           f"{_cell(f.message)} |")
        if p.proposals:
            out += ["", "### Proposed reconstruction (PROPOSED - not in the original)", ""]
            for pr in p.proposals:
                out += _proposal(pr)
        if p.blockers:
            out += ["", "### Unresolved - blockers", ""]
            for b in p.blockers:
                out += [f"- **{b.id}** ({', '.join(b.finding_ids)}) {b.reason}.",
                        f"  - Information required: {'; '.join(b.information_required)}",
                        f"  - Minimum to unblock: {b.minimum_information}",
                        f"  - Effect on conversion: {b.conversion_impact}",
                        f"  - Closest safe implementation: {b.closest_safe_implementation}"]
    if any(p.completeness is Completeness.unusable for p in r.pipelines):
        out += ["", "Pipelines marked *unusable* could not be read at all; nothing was inferred for them."]
    return "\n".join(out) + "\n"


def _proposal(p: Proposal) -> list[str]:
    gen = "**generated**" if p.safe_to_generate else f"**not generated** - {p.not_generated_reason}"
    out = [f"#### {p.id} `{p.rule}` - {_CONF[p.confidence]} - {gen}", "",
           f"- Missing element: {p.missing_element}" + (f" (activity `{(p.activity_id or '').split('/', 1)[-1]}`)" if p.activity_id else ""),
           f"- Action: {p.action} at `{p.target_path}`",
           f"- Proposed: `{_short(p.proposed)}`" + (" (SQL assembled from evidence - review it)" if p.derived_sql else ""),
           f"- Confidence basis: {p.confidence_basis}", "- Evidence:"]
    out += [f"  - [{e.kind.value}, priority {e.priority}] {e.source}: {e.detail}" for e in p.evidence]
    if p.assumptions:
        out += ["- Assumptions:"] + [f"  - {a}" for a in p.assumptions]
    if p.alternatives:
        out += ["- Alternative interpretations:"] + [f"  - {a.description}" for a in p.alternatives]
    if p.information_required:
        out += ["- Information still required:"] + [f"  - {i}" for i in p.information_required]
    out.append("")
    return out
