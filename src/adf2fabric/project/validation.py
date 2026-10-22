"""Persisted migration-validation report (project/persistence layer).

The report is produced by an engine that reads the SOURCE files and the GENERATED artifacts itself; it does not use the
converter's success response. Every applicable check ends in exactly one of:

* PASS     the check ran and the evidence shows the property holds
* FAIL     the check ran and the evidence shows it does not
* BLOCKED  the check could not give an answer because something it needs is missing or unbound (an unbound connection id, an
           unreadable source, a placeholder instead of a converted activity); the remediation says what to supply
* NOT_RUN  the check was not attempted (no credentials, no authorised workspace, a stage that was not requested)

Four stages are kept apart because passing an earlier one proves nothing about a later one:
STATIC (files only), API (the target answers and is readable), DEPLOYMENT (what is deployed is what was generated),
RUNTIME (a real run behaved like the source). No static or API result is ever reported as runtime correctness.

Security: evidence and diffs are redacted (secret-named keys, credential-looking values) before they are stored.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

VALIDATION_SCHEMA_VERSION = "migration-validation/1"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Result(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    BLOCKED = "BLOCKED"
    NOT_RUN = "NOT_RUN"


class Stage(str, Enum):
    static = "static"
    api = "api"
    deployment = "deployment"
    runtime = "runtime"


class Category(str, Enum):
    source_inventory = "source_inventory"  # 1
    reference_integrity = "reference_integrity"  # 2
    target_schema = "target_schema"  # 3
    expression_preservation = "expression_preservation"  # 4
    activity_graph = "activity_graph"  # 5
    connections_secrets = "connections_secrets"  # 6
    compute = "compute"  # 7
    execution_semantics = "execution_semantics"  # 8
    deployment = "deployment"  # 9
    runtime = "runtime"  # 10


class ComponentStatus(str, Enum):
    matched = "matched"  # present in both and equal after normalisation
    transformed = "transformed"  # present in both; differs in a way Fabric requires and the conversion tables document
    altered = "altered"  # present in both; differs in a way nothing documents: needs review
    missing = "missing"  # in the source, absent from the target
    extra = "extra"  # in the target, absent from the source (a helper, a placeholder, or an error)
    unverified = "unverified"  # could not be compared (unbound, inlined, or no Fabric counterpart to read)
    not_applicable = "not_applicable"  # has no Fabric counterpart by design (for example an integration runtime)


class CheckResult(_Strict):
    id: str  # "<category>.<name>", stable
    category: Category
    stage: Stage
    result: Result
    summary: str
    component: Optional[str] = None  # stable component identity, when the check is about one component
    evidence: list[str] = Field(default_factory=list)  # redacted facts the verdict rests on
    remediation: str = ""
    required: bool = True  # a required check that is not PASS prevents an overall PASS


class ComponentComparison(_Strict):
    id: str  # stable identity: pipeline:P, pipeline:P/activity:Outer/Inner, linked_service:L, trigger:T ...
    kind: str
    status: ComponentStatus
    source: Optional[str] = None  # normalised, redacted one-line description of the source component
    target: Optional[str] = None
    diff: list[str] = Field(default_factory=list)  # normalised, redacted differences
    note: str = ""


class StageSummary(_Strict):
    stage: Stage
    status: Result  # FAIL if any check failed; else BLOCKED if any blocked; else NOT_RUN if none passed/any not run; else PASS
    passed: int = 0
    failed: int = 0
    blocked: int = 0
    not_run: int = 0


class InventoryCounts(_Strict):
    source_components: int = 0
    target_components: int = 0
    by_status: dict[str, int] = Field(default_factory=dict)


class ValidationReport(_Strict):
    schema_version: str = VALIDATION_SCHEMA_VERSION
    project_id: str
    created_at: datetime
    scope: dict[str, str] = Field(default_factory=dict)  # what was and was not available (workspace, stages requested)
    stages: list[StageSummary] = Field(default_factory=list)
    checks: list[CheckResult] = Field(default_factory=list)
    components: list[ComponentComparison] = Field(default_factory=list)
    inventory: InventoryCounts = Field(default_factory=InventoryCounts)
    overall: str = "NOT_VALIDATED"  # MIGRATION_VERIFIED | FAILED | BLOCKED | NOT_VALIDATED
    overall_reasons: list[str] = Field(default_factory=list)
    full_success_claimed: bool = False  # True only when overall is MIGRATION_VERIFIED
