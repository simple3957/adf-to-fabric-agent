"""Evidence and the ONE function that turns evidence into a confidence category.

Confidence is never a score. It is derived by :func:`grade` from four facts only — what evidence
exists (and how strong its source is), how many candidate answers it supports, whether any
information is still missing, and whether the answer rests on material assumptions:

    LOW     information is still required, OR evidence supports zero / several candidates,
            OR the only evidence is weak (a single naming hint or a single peer)
    HIGH    exactly one candidate, nothing missing, no material assumption, and either the
            evidence is explicit (priority 1-2) or the mapping is uniquely determined from a
            referenced artifact (priority <= 4, ``determined=True``)
    MEDIUM  exactly one candidate, nothing missing, and sufficient support: evidence of priority
            <= 6, or a pattern corroborated by at least two independent items

Evidence priority (1 = strongest) follows the order the analysis consults its sources:

    1 explicit property of the activity      2 explicit reference in the same pipeline
    3 pipeline / factory parameters          4 referenced datasets / linked services
    5 parent and child pipelines             6 related pipelines of matching structure
    7 repeated source-to-sink patterns       8 names and descriptive metadata
    9 SQL objects, notebooks, schema info
"""

from __future__ import annotations

from adf2fabric.project.reconstruction import Confidence, Evidence, EvidenceKind

PRIORITY: dict[EvidenceKind, int] = {
    EvidenceKind.explicit_property: 1,
    EvidenceKind.same_pipeline_reference: 2,
    EvidenceKind.parameter: 3,
    EvidenceKind.referenced_artifact: 4,
    EvidenceKind.parent_child: 5,
    EvidenceKind.related_pipeline: 6,
    EvidenceKind.repeated_pattern: 7,
    EvidenceKind.naming_convention: 8,
    EvidenceKind.project_artifact: 9,
}


def ev(kind: EvidenceKind, source: str, detail: str) -> Evidence:
    return Evidence(kind=kind, priority=PRIORITY[kind], source=source, detail=detail)


def grade(
    evidence: list[Evidence],
    *,
    candidates: int,
    information_required: list[str] | None = None,
    determined: bool = False,
    material_assumptions: bool = False,
) -> tuple[Confidence, str]:
    """(confidence, the rule that produced it, in words). Pure and deterministic."""
    if information_required:
        return Confidence.low, f"LOW: information is still required ({'; '.join(information_required)})."
    if candidates != 1:
        return Confidence.low, (
            "LOW: no candidate is supported by the evidence."
            if candidates == 0 else f"LOW: {candidates} plausible candidates; the evidence does not choose between them.")
    if not evidence:
        return Confidence.low, "LOW: there is no supporting evidence."

    best = min(e.priority for e in evidence)
    kinds = {e.kind for e in evidence}
    n = len(evidence)

    if (best <= 2 or (determined and best <= 4)) and not material_assumptions:
        why = "explicit evidence" if best <= 2 else "a uniquely determined mapping from a referenced artifact"
        return Confidence.high, f"HIGH: a single candidate backed by {why} (priority {best}), nothing missing."
    if best <= 6:
        cap = " (capped at MEDIUM by a material assumption)" if material_assumptions else ""
        return Confidence.medium, f"MEDIUM: a single candidate supported by evidence of priority {best}{cap}."
    if best == 7 and n >= 2:
        return Confidence.medium, f"MEDIUM: a repeated pattern confirmed by {n} independent items."
    if best >= 8 and n >= 2 and len(kinds) >= 2:
        return Confidence.medium, f"MEDIUM: naming evidence corroborated by {len(kinds)} different kinds of evidence."
    return Confidence.low, (
        f"LOW: the only support is weak (best priority {best}, {n} item(s), {len(kinds)} kind(s)) - "
        "a single peer, a naming hint or a project artifact is not enough.")


def safe_to_generate(confidence: Confidence, information_required: list[str] | None) -> bool:
    """Only HIGH and MEDIUM proposals with nothing outstanding may enter the completed pipeline."""
    return confidence in (Confidence.high, Confidence.medium) and not information_required
