"""Persisted Reconstruction contracts (project/persistence layer).

Shapes serialized to ``projects/<id>/reconstruction/reconstruction.yaml``. Data contract only —
no analysis logic (that is ``agent/reconstruction/``) — and it imports only pydantic/stdlib.

Four representations are kept strictly apart and never merged:

* ORIGINAL       the ADF JSON under ``source/`` (never read for writing, never modified)
* PARSED         ``discovery/adf-semantic.yaml`` (never modified)
* PROPOSED       this record: findings, evidence-backed proposals (as JSON patches against the
                 original), blockers. Inferred values live ONLY here and in the proposed completion
* GENERATED      ``conversion/`` — Fabric output, with inferred parts identified

Nothing is ever inferred without evidence: a ``Proposal`` carries the evidence it rests on, the
named rule that produced it, the assumptions it makes, the alternatives it rejected, and what
information would still be needed. Confidence is one of three categories derived by a single
explicit function from that evidence — there are no numeric scores.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

RECONSTRUCTION_SCHEMA_VERSION = "adf-reconstruction/1"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IncompletenessClass(str, Enum):
    """The four kinds of 'not usable as-is' are different problems with different fixes."""

    invalid_json = "invalid_json"  # the file cannot be parsed (incl. truncated)
    missing_artifact = "missing_artifact"  # a referenced dataset / linked service / pipeline is not there
    incomplete_configuration = "incomplete_configuration"  # present, but properties/values are missing
    unsupported_functionality = "unsupported_functionality"  # complete, but not convertible to Fabric


class Severity(str, Enum):
    error = "error"
    warning = "warning"
    info = "info"


class Confidence(str, Enum):
    high = "high"  # explicit evidence, or a uniquely determined mapping
    medium = "medium"  # a well-supported pattern with limited ambiguity
    low = "low"  # several plausible implementations, or substantial information missing


class EvidenceKind(str, Enum):
    """Evidence sources, in the priority order the analysis uses (1 = strongest)."""

    explicit_property = "explicit_property"  # 1: a property of the source activity itself
    same_pipeline_reference = "same_pipeline_reference"  # 2: an explicit reference inside the same pipeline
    parameter = "parameter"  # 3: pipeline / factory parameters
    referenced_artifact = "referenced_artifact"  # 4: referenced datasets / linked services
    parent_child = "parent_child"  # 5: parent and child pipeline definitions
    related_pipeline = "related_pipeline"  # 6: related pipelines with matching structure
    repeated_pattern = "repeated_pattern"  # 7: the same source-to-sink pattern repeated in the project
    naming_convention = "naming_convention"  # 8: names and descriptive metadata
    project_artifact = "project_artifact"  # 9: SQL objects, notebooks, schema information


class Evidence(_Strict):
    kind: EvidenceKind
    priority: int  # 1..9 (see EvidenceKind)
    source: str  # the artifact / activity / property the evidence comes from
    detail: str  # what it shows (quoted or summarized, never invented)


class Finding(_Strict):
    id: str  # F-0001 ...
    incompleteness: IncompletenessClass
    code: str
    severity: Severity
    pipeline: Optional[str] = None
    activity_id: Optional[str] = None
    source_path: Optional[str] = None  # repository file
    property_path: Optional[str] = None
    message: str
    detail: str = ""


class Alternative(_Strict):
    description: str
    proposed: Optional[Any] = None


class PatchOp(_Strict):
    op: str  # add | replace
    path: str  # JSON pointer into the file
    value: Any


class Patch(_Strict):
    file: str  # repository-relative path of the ORIGINAL file the patch is against
    ops: list[PatchOp] = Field(default_factory=list)


class Proposal(_Strict):
    id: str  # P-0001 ...
    rule: str  # the named, tested inference rule
    finding_ids: list[str] = Field(default_factory=list)
    pipeline: str
    activity_id: Optional[str] = None  # the activity the missing element belongs to
    missing_element: str  # what is missing, in words
    action: str  # set_property | add_dependency | add_activity | rename_reference | set_argument
    target_path: str  # where, in the original JSON (pointer text)
    proposed: Any = None  # the proposed value (JSON), derived only from the evidence below
    evidence: list[Evidence] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    alternatives: list[Alternative] = Field(default_factory=list)
    information_required: list[str] = Field(default_factory=list)
    confidence: Confidence
    confidence_basis: str  # the grading rule that produced the category, spelled out
    safe_to_generate: bool  # may it enter the completed pipeline / Fabric output?
    not_generated_reason: str = ""  # why not, when safe_to_generate is False
    derived_sql: bool = False  # the proposal contains SQL assembled from evidence (review it)
    patches: list[Patch] = Field(default_factory=list)  # applied to COPIES of the original JSON
    affects_activity_ids: list[str] = Field(default_factory=list)  # activities carrying the inferred element
    creates_activity_id: Optional[str] = None  # for add_activity: the id the new activity will have


class Blocker(_Strict):
    id: str  # B-0001 ...
    finding_ids: list[str] = Field(default_factory=list)
    pipeline: Optional[str] = None
    activity_id: Optional[str] = None
    reason: str
    information_required: list[str] = Field(default_factory=list)
    conversion_impact: str  # how the gap affects the Fabric output
    minimum_information: str  # the least additional information that would unblock it
    closest_safe_implementation: str  # what is generated meanwhile


class RecognizedPattern(_Strict):
    name: str
    pipeline: str
    activity_ids: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    complete: bool = True
    missing: list[str] = Field(default_factory=list)  # parts the pattern normally has but this lacks


class Completeness(str, Enum):
    complete = "complete"  # nothing missing
    reconstructable = "reconstructable"  # every gap has a safe proposal
    partially_reconstructable = "partially_reconstructable"  # some gaps remain
    blocked = "blocked"  # at least one gap cannot be bridged without more information
    unusable = "unusable"  # the pipeline file itself could not be read


class PipelineAnalysis(_Strict):
    pipeline: str
    source_path: str
    completeness: Completeness
    executable_as_is: bool  # no gap of any kind: safe to treat as a finished pipeline
    findings: list[Finding] = Field(default_factory=list)
    proposals: list[Proposal] = Field(default_factory=list)
    blockers: list[Blocker] = Field(default_factory=list)
    patterns: list[RecognizedPattern] = Field(default_factory=list)
    unresolved_finding_ids: list[str] = Field(default_factory=list)  # still open after safe proposals


class ReconstructionOutcome(str, Enum):
    complete = "complete"
    reconstructed = "reconstructed"
    incomplete = "incomplete"
    missing_semantic_discovery = "missing_semantic_discovery"
    failed = "failed"


class ProjectReconstruction(_Strict):
    schema_version: str = RECONSTRUCTION_SCHEMA_VERSION
    project_id: str
    source_commit_sha: str
    status: ReconstructionOutcome
    created_at: datetime
    updated_at: datetime
    pipelines: list[PipelineAnalysis] = Field(default_factory=list)
    source_findings: list[Finding] = Field(default_factory=list)  # file-level: invalid / truncated JSON
    source_blockers: list[Blocker] = Field(default_factory=list)  # what unblocks each file-level finding
    finding_counts: dict[str, int] = Field(default_factory=dict)  # by incompleteness class
    proposal_counts: dict[str, int] = Field(default_factory=dict)  # by confidence
    safe_proposal_count: int = 0
    blocker_count: int = 0
    completed_files: list[str] = Field(default_factory=list)  # proposed-completion files (under reconstruction/)
    representations: dict[str, str] = Field(default_factory=dict)
    detail: str = ""
    provenance: dict[str, str] = Field(default_factory=dict)
