"""Persisted Fabric Target Provisioning records (project/persistence layer).

Four separate records, written by four separate stages, so discovery, planning, provisioning and verification never blur:

* ``TargetState``      what exists right now (read-only discovery; nothing is created to find out)
* ``ProvisioningPlan`` what the migration needs, what exists, what can be reused, what must be created, in dependency order,
                       with blockers. Carries a ``plan_hash``: an approval is bound to it.
* ``ProvisioningApproval`` a person's approval of specific steps of one plan hash
* ``ProvisioningRun``  what was attempted and what happened, step by step, journaled after every step
* ``VerificationReport`` actual state re-read after provisioning; the only thing allowed to say a resource exists

Security: no token, secret, connection string or credential is stored. Request previews contain names, ids, SKUs and sizes only.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

PROVISIONING_SCHEMA_VERSION = "fabric-provisioning/1"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ResourceType(str, Enum):
    azure_capacity = "azure_capacity"  # Microsoft.Fabric/capacities (Azure Resource Manager)
    workspace = "workspace"
    capacity_assignment = "capacity_assignment"
    workspace_identity = "workspace_identity"
    role_assignment = "role_assignment"
    spark_pool = "spark_pool"
    environment = "environment"
    lakehouse = "lakehouse"
    warehouse = "warehouse"
    connection = "connection"  # delegated: credentials are needed
    shortcut = "shortcut"  # delegated: needs a connection and a lakehouse
    notebook = "notebook"  # delegated: workloads-deploy (approved workloads only)
    spark_job_definition = "spark_job_definition"  # delegated: workloads-deploy


class Api(str, Enum):
    fabric = "fabric"  # Fabric REST
    arm = "arm"  # Azure Resource Manager
    delegated = "delegated"  # another approval-gated command owns the write


class Action(str, Enum):
    create = "create"
    reuse = "reuse"  # exists and is compatible: nothing to do
    configure = "configure"  # exists; an additive change is needed (assign, add a role, provision identity)
    delegate = "delegate"  # not created here; the named command does it
    conflict = "conflict"  # exists but differs in a way that cannot be reconciled without a person
    blocked = "blocked"  # cannot be done: see blockers
    unknown = "unknown"  # the existing state could not be read, so nothing is assumed


class Existence(str, Enum):
    exists = "exists"
    absent = "absent"
    unknown = "unknown"  # the read failed or was not permitted: NOT treated as absent


class CheckStatus(str, Enum):
    passed = "passed"
    failed = "failed"
    unknown = "unknown"  # could not be established without creating something or without a permission
    skipped = "skipped"


class StepStatus(str, Enum):
    planned = "planned"
    approved = "approved"
    applied = "applied"  # the call succeeded; NOT yet verified
    verified = "verified"
    mismatch = "mismatch"  # applied, but the re-read state differs from what was requested
    failed = "failed"
    skipped = "skipped"  # not attempted (not approved, dependency failed, already satisfied, delegated)


class Requirement(_Strict):
    """Something the migration needs, and why. One requirement is one wanted resource."""

    id: str  # "<type>:<name>" case-folded
    type: ResourceType
    name: str
    reason: str
    needed_by: list[str] = Field(default_factory=list)  # workloads / activities / linked services that need it
    spec: dict[str, Any] = Field(default_factory=dict)  # desired properties (non-secret)
    depends_on: list[str] = Field(default_factory=list)  # requirement ids


class ExistingResource(_Strict):
    id: Optional[str] = None
    name: str
    state: Existence = Existence.exists
    properties: dict[str, Any] = Field(default_factory=dict)  # the non-secret facts read from the service
    read_at: Optional[datetime] = None


class Check(_Strict):
    name: str
    status: CheckStatus
    detail: str = ""
    proved: str = ""  # what a pass actually established
    not_proved: str = ""  # what it did NOT establish
    action: str = ""
    blocking: bool = False  # a failed blocking check stops the whole plan; a failed non-blocking one is a warning


class TargetSpec(_Strict):
    """Where the migration lands, as the user states it. Nothing here is guessed or defaulted to a tenant value."""

    workspace_name: Optional[str] = None
    workspace_id: Optional[str] = None  # an existing workspace to use
    capacity_id: Optional[str] = None  # an existing Fabric capacity (GUID from GET /capacities)
    capacity_name: Optional[str] = None  # an existing capacity by display name
    create_capacity: bool = False  # explicit opt-in: create an Azure Fabric capacity
    azure_subscription_id: Optional[str] = None
    azure_resource_group: Optional[str] = None
    azure_capacity_name: Optional[str] = None
    azure_region: Optional[str] = None
    azure_sku: Optional[str] = None  # F2 .. F2048
    tenant_id: Optional[str] = None  # the tenant the migration must land in; checked against the token actually used
    capacity_admins: list[str] = Field(default_factory=list)  # UPNs for a new capacity
    provision_workspace_identity: bool = False
    role_assignments: list[dict[str, str]] = Field(default_factory=list)  # {principal_id, principal_type, role}
    lakehouse_names: list[str] = Field(default_factory=list)  # lakehouses the migration should create/reuse
    warehouse_names: list[str] = Field(default_factory=list)
    lakehouse_schemas_enabled: bool = False
    pipelines: list[str] = Field(default_factory=list)  # limit requirements to these pipelines (empty = all)


class ProvisionStep(_Strict):
    id: str  # same as the requirement id
    type: ResourceType
    name: str
    action: Action
    api: Api
    depends_on: list[str] = Field(default_factory=list)  # step ids, all earlier in the plan
    request: dict[str, Any] = Field(default_factory=dict)  # the non-secret request preview (what would be sent)
    endpoint: str = ""  # documented operation, for the display and the audit trail
    existing: Optional[ExistingResource] = None
    reason: str = ""
    needed_by: list[str] = Field(default_factory=list)
    estimate: dict[str, Any] = Field(default_factory=dict)  # capacity units, SKU, vCores ...
    blockers: list[str] = Field(default_factory=list)
    requires_approval: bool = False  # True for every create / configure
    delegated_to: str = ""  # the command that owns the write
    notes: list[str] = Field(default_factory=list)
    spec: dict[str, Any] = Field(default_factory=dict)  # the requirement's desired properties, carried so apply needs no re-derivation


class PlanSummary(_Strict):
    total: int = 0
    by_action: dict[str, int] = Field(default_factory=dict)
    by_type: dict[str, int] = Field(default_factory=dict)
    to_create: int = 0
    to_configure: int = 0
    reused: int = 0
    delegated: int = 0
    blocked: int = 0
    conflicts: int = 0
    unknown: int = 0


class ProvisioningPlan(_Strict):
    schema_version: str = PROVISIONING_SCHEMA_VERSION
    project_id: str
    created_at: datetime
    target: TargetSpec
    state_read_at: Optional[datetime] = None
    plan_hash: str = ""  # over the steps' action/type/name/request: an approval is bound to it
    checks: list[Check] = Field(default_factory=list)
    requirements: list[Requirement] = Field(default_factory=list)
    steps: list[ProvisionStep] = Field(default_factory=list)  # dependency order
    blockers: list[str] = Field(default_factory=list)  # plan-level: nothing can be applied until these are cleared
    summary: PlanSummary = Field(default_factory=PlanSummary)
    estimates: dict[str, Any] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


class TargetState(_Strict):
    """What discovery read. Anything it could not read is listed, never assumed absent."""

    read_at: datetime
    workspace: Optional[ExistingResource] = None
    capacities: list[ExistingResource] = Field(default_factory=list)
    items: dict[str, list[ExistingResource]] = Field(default_factory=dict)  # item type -> items in the workspace
    pools: list[ExistingResource] = Field(default_factory=list)
    connections: list[ExistingResource] = Field(default_factory=list)
    role_assignments: list[ExistingResource] = Field(default_factory=list)
    azure_capacities: list[ExistingResource] = Field(default_factory=list)
    unread: dict[str, str] = Field(default_factory=dict)  # what could not be read -> why (status / permission)


class ProvisioningApproval(_Strict):
    plan_hash: str
    step_ids: list[str]  # the steps approved (never implicit "all")
    approved_by: str
    approved_at: datetime
    note: str = ""


class StepResult(_Strict):
    step_id: str
    status: StepStatus
    attempts: int = 0
    resource_id: Optional[str] = None
    detail: str = ""  # redacted
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class ProvisioningRun(_Strict):
    plan_hash: str
    dry_run: bool
    started_at: datetime
    finished_at: Optional[datetime] = None
    results: list[StepResult] = Field(default_factory=list)
    interrupted: bool = False  # a previous run stopped before finishing; a resume continues from the journal


class VerifiedResource(_Strict):
    step_id: str
    status: StepStatus  # verified | mismatch | failed (could not read)
    detail: str = ""
    resource_id: Optional[str] = None


class VerificationReport(_Strict):
    plan_hash: str
    verified_at: datetime
    results: list[VerifiedResource] = Field(default_factory=list)
    all_verified: bool = False
