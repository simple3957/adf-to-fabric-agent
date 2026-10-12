"""Analysis state: collects findings, proposals and blockers with stable, deterministic ids."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from adf2fabric.agent.reconstruction import patching
from adf2fabric.agent.reconstruction.evidence import grade, safe_to_generate
from adf2fabric.agent.reconstruction.index import ProjectEvidence
from adf2fabric.project.discovery import AdfActivity, AdfPipeline
from adf2fabric.project.reconstruction import (
    Alternative,
    Blocker,
    Evidence,
    Finding,
    IncompletenessClass,
    Patch,
    PatchOp,
    Proposal,
    Severity,
)


@dataclass
class Collector:
    """Everything found for the whole project, in creation order (which is deterministic)."""

    findings: list[Finding] = field(default_factory=list)
    proposals: list[Proposal] = field(default_factory=list)
    blockers: list[Blocker] = field(default_factory=list)
    _keys: set = field(default_factory=set)

    def finding(
        self, cls: IncompletenessClass, code: str, message: str, *, pipeline: Optional[str] = None,
        activity: Optional[AdfActivity] = None, source_path: Optional[str] = None,
        property_path: Optional[str] = None, severity: Severity = Severity.warning, detail: str = "",
    ) -> Finding:
        """Record a finding once per (pipeline, activity, code, property)."""
        aid = activity.activity_id if activity else None
        key = (pipeline, aid, code, property_path, source_path)
        for f in self.findings:
            if (f.pipeline, f.activity_id, f.code, f.property_path, f.source_path) == key:
                return f
        f = Finding(id=f"F-{len(self.findings) + 1:04d}", incompleteness=cls, code=code, severity=severity,
                    pipeline=pipeline, activity_id=aid, source_path=source_path, property_path=property_path,
                    message=message, detail=detail)
        self.findings.append(f)
        return f

    def proposal(
        self, *, rule: str, finding: Finding, pipe: AdfPipeline, activity: Optional[AdfActivity],
        missing_element: str, action: str, target_path: str, proposed: Any, evidence: list[Evidence],
        patches: list[Patch], candidates: int = 1, assumptions: Optional[list[str]] = None,
        alternatives: Optional[list[Alternative]] = None, information_required: Optional[list[str]] = None,
        determined: bool = False, material_assumptions: bool = False, derived_sql: bool = False,
        affects: Optional[list[str]] = None, not_generated_reason: str = "", generate: bool = True,
        extra_findings: Optional[list[Finding]] = None, creates: Optional[str] = None,
    ) -> Proposal:
        info = list(information_required or [])
        confidence, basis = grade(evidence, candidates=candidates, information_required=info,
                                  determined=determined, material_assumptions=material_assumptions)
        safe = safe_to_generate(confidence, info) and generate
        reason = not_generated_reason
        if not safe and not reason:
            reason = ("information is still required" if info else
                      f"confidence is {confidence.value}; only high and medium proposals are generated")
        p = Proposal(
            id=f"P-{len(self.proposals) + 1:04d}", rule=rule,
            finding_ids=[finding.id, *[f.id for f in (extra_findings or [])]], pipeline=pipe.name,
            activity_id=activity.activity_id if activity else None, missing_element=missing_element,
            action=action, target_path=target_path, proposed=proposed, evidence=evidence,
            assumptions=list(assumptions or []), alternatives=list(alternatives or []),
            information_required=info, confidence=confidence, confidence_basis=basis,
            safe_to_generate=safe, not_generated_reason="" if safe else reason, derived_sql=derived_sql,
            patches=patches, affects_activity_ids=list(affects or ([activity.activity_id] if activity else [])),
            creates_activity_id=creates)
        self.proposals.append(p)
        return p

    def blocker(
        self, findings: list[Finding], *, pipeline: Optional[str], activity: Optional[AdfActivity], reason: str,
        information_required: list[str], conversion_impact: str, minimum_information: str,
        closest_safe_implementation: str,
    ) -> Blocker:
        b = Blocker(id=f"B-{len(self.blockers) + 1:04d}", finding_ids=[f.id for f in findings], pipeline=pipeline,
                    activity_id=activity.activity_id if activity else None, reason=reason,
                    information_required=information_required, conversion_impact=conversion_impact,
                    minimum_information=minimum_information, closest_safe_implementation=closest_safe_implementation)
        self.blockers.append(b)
        return b


@dataclass
class PipelineCtx:
    """What a rule needs while analysing one pipeline."""

    ev: ProjectEvidence
    out: Collector
    pipe: AdfPipeline
    doc: Optional[Any]  # the ORIGINAL JSON of the pipeline file (read-only; never mutate)

    def tokens(self, a: AdfActivity) -> list[patching.Token]:
        return self.ev.tokens(a)

    def at(self, a: AdfActivity, *rest: patching.Token) -> list[patching.Token]:
        return [*self.tokens(a), *rest]

    def patch_set(self, tokens: list[patching.Token], value: Any) -> Patch:
        return Patch(file=self.pipe.path, ops=patching.set_ops(self.doc, tokens, value))

    def patch_append(self, list_tokens: list[patching.Token], value: Any) -> Patch:
        return Patch(file=self.pipe.path, ops=patching.append_ops(self.doc, list_tokens, value))

    def new_activity_id(self, sibling: AdfActivity, name: str) -> str:
        """The activity id a new activity named ``name`` will get next to ``sibling``."""
        return "/".join([self.pipe.name, *sibling.nesting_path, name])

    def pointer(self, tokens: list[patching.Token]) -> str:
        return patching.to_pointer(tokens)

    def op(self, op: str, tokens: list[patching.Token], value: Any) -> PatchOp:
        return PatchOp(op=op, path=patching.to_pointer(tokens), value=value)
