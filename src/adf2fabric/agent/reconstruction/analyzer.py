"""Orchestrates detection, reconstruction rules, safety checks and completeness per pipeline."""

from __future__ import annotations

from adf2fabric.agent.reconstruction import patching
from adf2fabric.agent.reconstruction import detect, rules_patterns, rules_structure
from adf2fabric.agent.reconstruction.context import Collector, PipelineCtx
from adf2fabric.agent.reconstruction.index import ProjectEvidence
from adf2fabric.agent.reconstruction.patterns import recognize_patterns
from adf2fabric.project.reconstruction import (
    Completeness,
    Finding,
    IncompletenessClass as C,
    PipelineAnalysis,
    Proposal,
    Severity,
)

# The order is part of the contract: structural gaps first (later rules read their results).
_RULES = (
    rules_structure.rule_missing_artifacts,
    rules_structure.rule_copy_endpoints,
    rules_structure.rule_dependencies,
    rules_structure.rule_contracts,
    rules_structure.rule_arguments,
    rules_patterns.rule_watermark_update,
    rules_patterns.rule_staging_load,
    rules_patterns.rule_shape_siblings,
    rules_patterns.rule_error_handling,
    detect.detect_pipeline,
)


def analyze_project(ev: ProjectEvidence) -> tuple[Collector, list[Finding], list[PipelineAnalysis]]:
    out = Collector()
    source_findings = detect.detect_sources(ev, out)
    analyses: list[PipelineAnalysis] = []

    # pipelines whose file could not even be parsed are unusable (never silently absent)
    unusable = [c for c in ev.broken_files() if c.artifact_kind.value == "pipeline"]

    patterns_by_pipeline: dict[str, list] = {}
    for p in sorted(ev.discovery.pipelines, key=lambda x: x.name.lower()):
        doc = ev.doc(p.path)
        pc = PipelineCtx(ev=ev, out=out, pipe=p, doc=doc)
        if doc is None:
            out.finding(C.invalid_json, "original_unavailable",
                        f"The original file {p.path} could not be read, so evidence-backed patches cannot be built.",
                        pipeline=p.name, source_path=p.path, severity=Severity.error)
            detect.detect_pipeline(pc)
            patterns_by_pipeline[p.name] = recognize_patterns(pc)
            continue
        for rule in _RULES:
            rule(pc)
        patterns_by_pipeline[p.name] = recognize_patterns(pc)

    _dry_run_patches(ev, out.proposals)

    for p in sorted(ev.discovery.pipelines, key=lambda x: x.name.lower()):
        analyses.append(_pipeline_analysis(ev, out, p.name, p.path, patterns_by_pipeline.get(p.name, [])))
    for c in unusable:
        name = c.name or c.path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        fids = [f.id for f in source_findings if f.source_path == c.path]
        analyses.append(PipelineAnalysis(
            pipeline=name, source_path=c.path, completeness=Completeness.unusable, executable_as_is=False,
            findings=[f for f in source_findings if f.source_path == c.path],
            blockers=[b for b in out.blockers if b.pipeline is None and set(b.finding_ids) & set(fids)],
            unresolved_finding_ids=fids))
    return out, source_findings, analyses


def _pipeline_analysis(ev: ProjectEvidence, out: Collector, name: str, path: str, patterns) -> PipelineAnalysis:
    findings = [f for f in out.findings if f.pipeline == name]
    proposals = [p for p in out.proposals if p.pipeline == name]
    blockers = [b for b in out.blockers if b.pipeline == name]
    covered = {fid for p in proposals if p.safe_to_generate for fid in p.finding_ids}
    gaps = [f for f in findings if f.severity is not Severity.info
            and f.incompleteness is not C.unsupported_functionality]
    unsupported = [f for f in findings if f.incompleteness is C.unsupported_functionality]
    unresolved = [f.id for f in gaps if f.id not in covered]
    if not gaps:
        completeness = Completeness.complete
    elif not unresolved:
        completeness = Completeness.reconstructable
    elif any(p.safe_to_generate for p in proposals):
        completeness = Completeness.partially_reconstructable
    else:
        completeness = Completeness.blocked
    return PipelineAnalysis(
        pipeline=name, source_path=path, completeness=completeness,
        executable_as_is=not gaps and not unsupported and not any(f.severity is Severity.warning for f in findings),
        findings=findings, proposals=proposals, blockers=blockers, patterns=patterns,
        unresolved_finding_ids=unresolved + [f.id for f in unsupported])


def _dry_run_patches(ev: ProjectEvidence, proposals: list[Proposal]) -> None:
    """A proposal may enter the completion only if its patches apply cleanly and do not collide.

    Proposals are tried in id order against a working copy of each ORIGINAL file; a failing or
    conflicting one is demoted (never silently dropped) with the reason recorded.
    """
    work: dict[str, object] = {}
    claimed: dict[tuple[str, str], str] = {}
    for prop in proposals:
        if not prop.safe_to_generate or not prop.patches:
            continue
        trial = {f: work.get(f, ev.doc(f)) for f in {pt.file for pt in prop.patches}}
        try:
            for pt in prop.patches:
                if trial[pt.file] is None:
                    raise patching.PatchError(f"original file {pt.file} is unavailable")
                for op in pt.ops:
                    if op.op == "replace" or not op.path.endswith("/-"):
                        k = (pt.file, op.path)
                        if k in claimed and claimed[k] != prop.id:
                            raise patching.PatchError(f"{op.path} is already changed by {claimed[k]}")
                trial[pt.file] = patching.apply_patch(trial[pt.file], pt)
        except patching.PatchError as exc:
            prop.safe_to_generate = False
            prop.not_generated_reason = f"its patch cannot be applied safely: {exc}"
            continue
        work.update(trial)
        for pt in prop.patches:
            for op in pt.ops:
                if op.op == "replace" or not op.path.endswith("/-"):
                    claimed[(pt.file, op.path)] = prop.id
