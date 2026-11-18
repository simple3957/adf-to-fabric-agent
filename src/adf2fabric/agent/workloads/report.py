"""Markdown report of the workload manifest: classification, what was generated, what is open, what was proven."""

from __future__ import annotations

from adf2fabric.project.workloads import WorkloadManifest


def _c(text: str) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ")


def render_report(m: WorkloadManifest) -> str:
    s = m.summary
    out = [f"# Workload migration report: {m.project_id}", "",
           f"{s.total} workload(s). Generated: {s.generated}; generated with open issues: {s.generated_with_open_issues}; "
           f"not generated: {s.not_generated}. Approved: {s.approved}. **Verified executing in Fabric: {s.executed_in_fabric}.**", "",
           "Generating an artifact means it was written and passed static checks. It is not evidence that it runs: only the "
           "`Execution in Fabric` column (a recorded Fabric run) is.", "",
           "| Classification | Count |", "|---|---|", *[f"| {k} | {v} |" for k, v in s.by_classification.items()], "",
           "## Workloads", "", "| Workload | Kind | Classification | Code | Generation | Approval | Execution in Fabric |", "|---|---|---|---|---|---|---|"]
    for w in m.workloads:
        out.append(f"| {_c(w.source_item)} | {w.kind.value} | {w.classification.value} | {w.code.status.value} | {w.generation.status} | "
                   f"{w.approval.status} | {w.execution.status} |")
    for w in m.workloads:
        out += ["", f"### {w.source_item}", "", f"- Used by: {', '.join(w.used_by)}",
                f"- Environment: {w.environment.platform} {w.environment.mode}, runtime {w.environment.runtime_version or 'unknown'} "
                f"(Spark {w.environment.spark_version or 'unknown'}) -> suggested Fabric runtime {w.environment.fabric_runtime or 'unknown'}",
                f"- Classification: **{w.classification.value}**. " + " ".join(w.classification_reasons)]
        if w.artifact.kind != "none":
            sha = w.artifact.sha256[:12] if w.artifact.sha256 else ""
            out.append(f"- Artifact: {w.artifact.kind} `{w.artifact.file}` (sha256 {sha})"
                       + (f", Fabric item {w.artifact.fabric_item_id}" if w.artifact.fabric_item_id else ", not deployed"))
        else:
            out.append(f"- Artifact: none. {w.generation.reason}")
        if w.rewrites:
            out.append("- Automatic rewrites: " + "; ".join(f"{r.description} ({r.count})" for r in w.rewrites))
        if w.code.status.value not in ("retrieved", "cached", "in_project", "supplied"):
            out.append(f"- Code: {w.code.status.value}. {w.code.detail} " + (f"Needs: {w.code.permission}." if w.code.permission else ""))
        if w.generation.checks:
            out.append("- Static checks: " + ", ".join(f"{'ok' if c.passed else 'FAILED'} {c.name}" for c in w.generation.checks))
        if w.unresolved:
            out += ["- Open issues:"] + [f"  - {_c(u)}" for u in w.unresolved]
        if w.alternatives:
            out += ["- Alternatives:"] + [f"  - {a.summary} Limits: {' '.join(a.limitations)}" for a in w.alternatives]
        if w.libraries:
            out.append("- Libraries: " + ", ".join(f"{l.name} ({l.decision})" for l in w.libraries[:20]))
        if w.execution.status != "not_run":
            out.append(f"- Fabric run: {w.execution.status} (run {w.execution.run_id})")
    return "\n".join(out) + "\n"
