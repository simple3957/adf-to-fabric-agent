"""Persisted Conversion contracts (project/persistence layer).

The shapes serialized to ``projects/<id>/conversion/conversion.yaml``. Data contract only:
no conversion logic lives here (that is ``agent/conversion/``). Self-contained — imports
only pydantic/stdlib — so the project layer never depends on the agent layer.

Principles encoded in the shapes:

* A conversion RESULT is never just "JSON was generated". Every source activity carries a
  ``strategy`` (how it was converted), a ``status`` (how faithfully), ``structural_valid``
  (is the generated JSON well-formed against the encoded Fabric schema) and ``runtime_ready``
  (can it run — connections, items and credentials still to be bound are listed
  separately as ``prerequisites``). These four are independent.
* Nothing is lost silently: dropped properties, rewritten expressions, semantic
  differences, required manual steps and alternatives are all explicit fields.
* No secret value is ever stored: generated configuration is built from the already
  secret-masked semantic representation.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from adf2fabric.project.compute import ComputePlan

CONVERSION_SCHEMA_VERSION = "adf-fabric-conversion/1"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ConversionStrategy(str, Enum):
    direct = "direct"  # A: direct equivalent
    transformed = "transformed"  # B: equivalent after rewriting configuration/expressions
    composite = "composite"  # C: several Fabric activities preserve the behavior
    alternative = "alternative"  # D: a documented alternative (no direct equivalent exists)
    manual = "manual"  # E: equivalence cannot be established safely — explicit blocker


class ConversionStatus(str, Enum):
    converted = "converted"  # behavior preserved by the generated components
    converted_with_differences = "converted_with_differences"  # generated, documented differences
    manual_required = "manual_required"  # scaffold generated; semantics need manual authoring
    blocked = "blocked"  # not converted; fail-fast placeholder + alternatives


class Impact(str, Enum):
    """How an issue affects fidelity (drives ``ConversionStatus``)."""

    none = "none"  # informational / prerequisite only
    difference = "difference"  # behavior differs in a documented way
    manual = "manual"  # a human must complete or verify it
    blocker = "blocker"  # cannot be converted


class IssueSeverity(str, Enum):
    error = "error"
    warning = "warning"
    info = "info"


class ConversionIssue(_Strict):
    code: str
    severity: IssueSeverity = IssueSeverity.warning
    impact: Impact = Impact.none
    message: str
    action: str = ""  # what to do about it
    source_path: Optional[str] = None
    target_path: Optional[str] = None


class PropertyMapping(_Strict):
    source: str  # source property path (activity-relative)
    target: Optional[str] = None  # target property path; None when not carried over
    rule: str  # the named rule that applied
    note: str = ""
    preserved: bool = True  # False = the property was NOT carried over (see ``note``)


class ExpressionConversion(_Strict):
    source_path: str  # where it was authored (activity-relative, or dataset:<name>.<path>)
    target_path: Optional[str] = None
    source: str  # the original text, verbatim
    target: Optional[str] = None  # the generated text; None if literal after substitution
    status: str  # unchanged | rewritten | unsupported
    changes: list[str] = Field(default_factory=list)
    issues: list[ConversionIssue] = Field(default_factory=list)


class InferredElement(_Strict):
    """Something in the output that was NOT authored in the source: it came from a reconstruction proposal."""

    proposal_id: str
    rule: str
    confidence: str  # high | medium | low
    missing_element: str
    target_path: str
    assumptions: list[str] = Field(default_factory=list)
    review_required: bool = True  # False only for HIGH-confidence, assumption-free proposals


class FabricComponent(_Strict):
    name: str
    fabric_type: str
    role: str  # primary | helper | placeholder
    origin: str = "source"  # source | inferred (the whole component was created by a proposal)
    configuration: dict[str, Any] = Field(default_factory=dict)  # generated JSON, nested activities elided
    children: list[str] = Field(default_factory=list)  # names of nested components


class DependencyMapping(_Strict):
    source_activity: str  # the activity depended on
    conditions: list[str] = Field(default_factory=list)
    target_activities: list[str] = Field(default_factory=list)
    note: str = ""


class Prerequisite(_Strict):
    kind: str  # connection | workspace_item | gateway | credential | permission | source_system | capacity
    name: str
    detail: str = ""
    binding: Optional[str] = None  # the placeholder token left in the generated JSON
    bound: bool = False  # True when a resolution supplied the real id
    needed_by: list[str] = Field(default_factory=list)  # component names


class OperationCoverage(_Strict):
    operation: str  # an operation of the source activity (read, write, loop, ...)
    covered_by: list[str] = Field(default_factory=list)  # component names; empty = NOT covered
    note: str = ""


class ValidationResult(_Strict):
    valid: bool = True
    issues: list[ConversionIssue] = Field(default_factory=list)


class ActivityConversion(_Strict):
    source_activity_id: str
    source_name: str
    source_type: Optional[str] = None
    source_pointer: Optional[str] = None
    parent_activity_id: Optional[str] = None
    children: list[str] = Field(default_factory=list)  # child source activity ids
    strategy: ConversionStrategy
    status: ConversionStatus
    runtime_disposition: Optional[str] = None  # replace | reuse_externally | redesign
    summary: str = ""
    components: list[FabricComponent] = Field(default_factory=list)
    entry_components: list[str] = Field(default_factory=list)
    exit_components: list[str] = Field(default_factory=list)
    property_mappings: list[PropertyMapping] = Field(default_factory=list)
    expression_conversions: list[ExpressionConversion] = Field(default_factory=list)
    dependency_mappings: list[DependencyMapping] = Field(default_factory=list)
    operation_coverage: list[OperationCoverage] = Field(default_factory=list)
    warnings: list[ConversionIssue] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)  # semantic differences
    manual_steps: list[str] = Field(default_factory=list)
    alternatives: list[str] = Field(default_factory=list)
    prerequisites: list[Prerequisite] = Field(default_factory=list)
    validation: ValidationResult = Field(default_factory=ValidationResult)
    structural_valid: bool = True
    runtime_ready: bool = False
    inferred_elements: list[InferredElement] = Field(default_factory=list)  # empty unless reconstruction was applied


class CoverageReport(_Strict):
    """Proof that no source activity or operation was lost on the way to the target."""

    source_activities: int = 0
    accounted: int = 0
    missing: list[str] = Field(default_factory=list)  # source ids with no conversion record
    duplicated: list[str] = Field(default_factory=list)
    blocked: list[str] = Field(default_factory=list)  # explicitly not converted
    uncovered_operations: list[str] = Field(default_factory=list)  # "<activity>:<operation>"
    unaccounted_expressions: list[str] = Field(default_factory=list)  # "<activity>:<path>"
    complete: bool = True


class TriggerNote(_Strict):
    name: str
    trigger_type: Optional[str] = None
    runtime_state: Optional[str] = None
    pipelines: list[str] = Field(default_factory=list)
    summary: str = ""
    action: str = ""
    # Fabric schedule definitions generated from the trigger (see agent/conversion/triggers.py).
    schedule_status: Optional[str] = None  # generated | generated_with_differences | manual | not_supported
    schedules: list[dict[str, Any]] = Field(default_factory=list)  # Create Item Schedule bodies
    schedule_notes: list[str] = Field(default_factory=list)
    schedule_file: Optional[str] = None  # relative to conversion/, when a definition file was written
    details: list[str] = Field(default_factory=list)  # what to recreate by hand: event settings, parameter values


class ConnectorCoverage(_Strict):
    """How far the tool goes for one linked service's connector type."""

    linked_service: str
    adf_type: Optional[str] = None
    level: str  # created | manual | unsupported | unlisted
    note: str = ""


class PipelineConversion(_Strict):
    source_pipeline: str
    source_path: str
    fabric_pipeline_name: str
    artifact_path: Optional[str] = None  # relative to projects/<id>/conversion/
    status: ConversionStatus
    strategy_counts: dict[str, int] = Field(default_factory=dict)
    status_counts: dict[str, int] = Field(default_factory=dict)
    parameters_added: list[str] = Field(default_factory=list)  # e.g. gp_<global parameter>
    activities: list[ActivityConversion] = Field(default_factory=list)  # flat, source order
    validation: ValidationResult = Field(default_factory=ValidationResult)
    coverage: CoverageReport = Field(default_factory=CoverageReport)
    prerequisites: list[Prerequisite] = Field(default_factory=list)
    triggers: list[TriggerNote] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    structural_valid: bool = True
    runtime_ready: bool = False


class PreflightFinding(_Strict):
    """A problem in the authored source that conversion cannot fix (``agent/conversion/preflight.py``)."""

    code: str  # reserved_sql_alias | linked_service_parameter_without_value | wait_only_pipeline
    severity: str  # error: the Fabric run will fail | warning: the ADF run fails, Fabric is not affected | info: worth knowing
    subject: str  # the activity, linked service or pipeline it is about
    pipelines: list[str] = Field(default_factory=list)
    message: str
    fix: str
    source_url: Optional[str] = None


class CompatibilityFinding(_Strict):
    """A retired / deprecated / preview / unavailable component found in the source estate.

    Findings come from the compatibility catalog (``agent/conversion/compatibility.py``), whose every entry
    cites the Microsoft page and the date it was verified on. ``emitted`` says whether the finding also became
    an issue on the activity that uses it (components the handlers already block are listed here only).
    """

    id: str  # catalog entry id
    kind: str  # activity | linked_service | artifact
    subject: str  # the activity or linked service name
    adf_type: str
    pipelines: list[str] = Field(default_factory=list)  # pipelines that use it
    status: str  # removed | deprecated | preview | not_available
    date: Optional[str] = None  # ISO date of removal / end of support, when documented
    impact: Impact = Impact.manual
    summary: str
    alternatives: list[str] = Field(default_factory=list)
    source_url: str = ""
    emitted: bool = False


class ConversionOutcome(str, Enum):
    converted = "converted"  # every pipeline fully converted
    partial = "partial"  # some activities need differences/manual work/are blocked
    missing_semantic_discovery = "missing_semantic_discovery"
    no_pipelines = "no_pipelines"
    failed = "failed"


class ProjectConversion(_Strict):
    schema_version: str = CONVERSION_SCHEMA_VERSION
    project_id: str
    source_commit_sha: str
    status: ConversionOutcome
    created_at: datetime
    updated_at: datetime
    pipeline_count: int = 0
    activity_count: int = 0
    status_counts: dict[str, int] = Field(default_factory=dict)
    strategy_counts: dict[str, int] = Field(default_factory=dict)
    pipelines: list[PipelineConversion] = Field(default_factory=list)
    connections: list[Prerequisite] = Field(default_factory=list)  # aggregated, deduplicated
    workspace_items: list[Prerequisite] = Field(default_factory=list)
    schema_notes: list[str] = Field(default_factory=list)  # inferred (not documented) schema parts
    reconstruction_applied: list[str] = Field(default_factory=list)  # proposal ids that entered this output
    reconstruction_open: list[str] = Field(default_factory=list)  # blocker ids still unresolved
    compatibility: list[CompatibilityFinding] = Field(default_factory=list)  # retired/deprecated/preview components
    preflight: list[PreflightFinding] = Field(default_factory=list)  # source problems conversion cannot fix
    connector_coverage: list[ConnectorCoverage] = Field(default_factory=list)  # per linked service, see connectors.coverage
    compute_plan: Optional[ComputePlan] = None  # external Spark compute in use and the Fabric compute that would replace it
    detail: str = ""
    provenance: dict[str, str] = Field(default_factory=dict)
