"""Persisted Dependency Inventory contract (project/persistence layer).

One record per project (``discovery/dependencies.yaml``) answering, for every thing the ADF estate depends on: what it is,
how it is referenced, whether it resolves, who consumes it, how it authenticates, what migrating it means, and (when
checked) whether this machine can reach it. It is built from the semantic discovery record plus the source manifest, so it
is the same whichever input adapter (live Azure, a Git repository, a ZIP) produced the source.

Security: nothing here can hold a secret or source data. Key Vault entries are NAMES, authentication is a MODE plus the
NAMES of secret fields, access checks record a status and a permission category, never a token, and no field carries
rows, file content, notebook source or a secret value.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

DEPENDENCY_SCHEMA_VERSION = "dependency-inventory/1"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DependencyType(str, Enum):
    # ADF artifacts (resolved against the source)
    pipeline = "pipeline"
    dataset = "dataset"
    linked_service = "linked_service"
    integration_runtime = "integration_runtime"
    data_flow = "data_flow"
    trigger = "trigger"
    # names inside ADF
    global_parameter = "global_parameter"
    pipeline_parameter = "pipeline_parameter"
    variable = "variable"
    activity_output = "activity_output"
    # outside ADF: known only by name or address
    key_vault_secret = "key_vault_secret"
    databricks_notebook = "databricks_notebook"
    synapse_item = "synapse_item"
    sql_object = "sql_object"
    storage_path = "storage_path"
    rest_endpoint = "rest_endpoint"
    event_source = "event_source"


# Types that are ADF artifacts with a definition file in the source (a missing one is a missing file).
ARTIFACT_TYPES = frozenset({DependencyType.pipeline, DependencyType.dataset, DependencyType.linked_service,
                            DependencyType.integration_runtime, DependencyType.data_flow, DependencyType.trigger})
EXTERNAL_TYPES = frozenset({DependencyType.key_vault_secret, DependencyType.databricks_notebook, DependencyType.synapse_item,
                            DependencyType.sql_object, DependencyType.storage_path, DependencyType.rest_endpoint,
                            DependencyType.event_source})


class Resolution(str, Enum):
    resolved = "resolved"  # the definition (or declaration) is in the source
    unresolved = "unresolved"  # referenced, and nothing of that name is in the source: a broken reference
    unavailable = "unavailable"  # referenced; the source could not be fully retrieved, so it may exist (see source_gaps)
    dynamic = "dynamic"  # which one it is depends on an expression or parameter: not knowable statically
    external = "external"  # lives outside ADF; known only by name or address
    circular = "circular"  # defined, but part of a reference cycle


class MigrationCategory(str, Enum):
    ready = "ready"  # nothing to do beyond the normal conversion
    needs_connection = "needs_connection"  # a Fabric connection must be created or bound
    needs_gateway = "needs_gateway"  # reached through a gateway in Fabric (self-hosted / managed VNet runtime)
    recreate = "recreate"  # an item to re-create or import in Fabric (data flow, notebook, Spark job)
    unsupported = "unsupported"  # Fabric has no counterpart
    missing = "missing"  # the definition is not available: nothing can be migrated until it is supplied
    needs_access = "needs_access"  # an identity or network permission is missing
    manual = "manual"  # a person has to decide or do something
    informational = "informational"  # a name or a fact; no migration action


class AccessStatus(str, Enum):
    not_checked = "not_checked"  # no check was requested (the default: nothing external is contacted)
    accessible = "accessible"
    denied = "denied"  # authenticated, but not permitted
    unauthenticated = "unauthenticated"  # the credential was rejected
    not_found = "not_found"
    unreachable = "unreachable"  # DNS / network / firewall / private endpoint
    missing_credential = "missing_credential"  # no token could be obtained for THIS provider
    missing_dependency = "missing_dependency"  # a local package or driver the check needs is not installed
    not_configured = "not_configured"  # the target (server, vault, workspace) is not known from the source
    error = "error"


class Severity(str, Enum):
    error = "error"
    warning = "warning"
    info = "info"


class DependencyReference(_Strict):
    """Where a dependency is referenced from: the consuming component and the place in the source."""

    consumer_kind: str  # pipeline | activity | dataset | linked_service | data_flow | trigger | integration_runtime
    consumer: str  # the consumer's name, or an activity's id (<pipeline>/<ancestors>/<name>)
    role: str  # what the reference is: dataset | linkedServiceName | executePipeline | connectVia | expression ...
    file: Optional[str] = None  # source-relative file of the consumer
    pointer: Optional[str] = None  # JSON path inside that file


class AuthRequirement(_Strict):
    """How the dependency authenticates, from what the source states. Modes and names only, never values."""

    mode: str  # managed_identity | service_principal | account_key | sas | password | oauth | key_vault | encrypted | none | unknown
    secret_fields: list[str] = Field(default_factory=list)  # NAMES of fields that hold a secret in the source
    key_vault_secrets: list[str] = Field(default_factory=list)  # "<vault linked service>/<secret name>"
    needs_user_input: bool = False  # a person must supply a credential when creating the Fabric connection
    notes: list[str] = Field(default_factory=list)


class AccessCheck(_Strict):
    """The outcome of one least-privilege probe against one external target."""

    provider: str  # databricks | key_vault | storage | sql | network
    target: str  # workspace host / vault name / account+container / host:port: no secret
    status: AccessStatus
    permission: Optional[str] = None  # the precise permission category the probe exercised
    missing: Optional[str] = None  # what is missing, when the status is not accessible
    detail: str = ""  # redacted
    probe: str = ""  # what the probe did, and what it deliberately did not do
    checked_at: Optional[datetime] = None


class MigrationImplication(_Strict):
    category: MigrationCategory
    summary: str
    actions: list[str] = Field(default_factory=list)
    blocks: bool = False  # True: migration of what consumes it cannot complete until this is dealt with


class DependencyIssue(_Strict):
    code: str
    severity: Severity
    message: str


class Dependency(_Strict):
    id: str  # "<type>:<name>", case-folded, stable across runs
    type: DependencyType
    name: str  # as authored
    resolution: Resolution
    defined_in: Optional[str] = None  # source file of its definition, when there is one
    references: list[DependencyReference] = Field(default_factory=list)  # who references it, and where
    consumers: list[str] = Field(default_factory=list)  # unique consumers ("<kind>:<name>"), sorted
    authentication: Optional[AuthRequirement] = None
    migration: MigrationImplication
    access: Optional[AccessCheck] = None
    issues: list[DependencyIssue] = Field(default_factory=list)
    cycle: list[str] = Field(default_factory=list)  # ids of the dependencies in the cycle this one belongs to
    attributes: dict[str, str] = Field(default_factory=dict)  # type-specific, non-secret facts


class Finding(_Strict):
    """One thing that needs attention, flat so an assessment can filter it."""

    code: str  # broken_reference | missing_file | source_unavailable | unresolved_expression | unsupported_connection_type
    #            | inaccessible_external | circular_dependency | source_gap
    severity: Severity
    subject: str  # what it is about (a dependency name, a consumer)
    message: str
    dependency: Optional[str] = None  # the dependency id, when there is one
    action: str = ""


class SourceProvenance(_Strict):
    """Where the source came from. Whatever the adapter, the inventory below is built the same way."""

    source_type: str  # github | azure_adf | git | zip_export
    identifier: str = ""  # token-free repository URL or azure:// resource id
    branch: Optional[str] = None
    version: str = ""  # commit SHA of the source that was analysed
    retrieved_at: Optional[datetime] = None
    complete: bool = True  # the source manifest says everything asked for was retrieved
    selection: int = 0  # number of artifacts selected at intake (0 = all)


class SourceGap(_Strict):
    """Something the source could not provide, with the precise permission category when retrieval was denied."""

    kind: str  # an artifact kind, or "reference"
    name: Optional[str] = None
    reason: str  # denied | not_found | error | missing_reference | unprocessed
    permission: Optional[str] = None  # e.g. Microsoft.DataFactory/factories/datasets/read
    detail: str = ""
    action: str = ""


class PipelineOrder(_Strict):
    """One pipeline's place in the migration order. Children come before the pipelines that call them."""

    pipeline: str
    position: int  # 1-based
    wave: int  # the migration wave it belongs to
    calls: list[str] = Field(default_factory=list)  # pipelines it executes
    called_by: list[str] = Field(default_factory=list)
    blocked_by: list[str] = Field(default_factory=list)  # ids of blocking dependencies it consumes directly
    ready: bool = True  # no blocking dependency here or in any pipeline it calls (transitively)
    in_cycle: bool = False


class MigrationWave(_Strict):
    number: int
    name: str
    kind: str  # foundation | connections | definitions | items | pipelines | triggers | manual
    items: list[str] = Field(default_factory=list)  # dependency ids or pipeline names
    note: str = ""


class DependencySummary(_Strict):
    total: int = 0
    by_type: dict[str, int] = Field(default_factory=dict)
    by_resolution: dict[str, int] = Field(default_factory=dict)
    by_category: dict[str, int] = Field(default_factory=dict)
    by_access: dict[str, int] = Field(default_factory=dict)
    blocking: int = 0
    findings_error: int = 0
    findings_warning: int = 0


class DependencyInventory(_Strict):
    schema_version: str = DEPENDENCY_SCHEMA_VERSION
    project_id: str
    source_commit_sha: str
    created_at: datetime
    updated_at: datetime
    source: SourceProvenance
    summary: DependencySummary = Field(default_factory=DependencySummary)
    dependencies: list[Dependency] = Field(default_factory=list)  # sorted by id
    findings: list[Finding] = Field(default_factory=list)  # most severe first
    source_gaps: list[SourceGap] = Field(default_factory=list)
    cycles: list[list[str]] = Field(default_factory=list)  # each a list of dependency ids
    pipeline_order: list[PipelineOrder] = Field(default_factory=list)
    waves: list[MigrationWave] = Field(default_factory=list)
    checks: list[AccessCheck] = Field(default_factory=list)  # every probe that was run
    checks_requested: list[str] = Field(default_factory=list)  # providers the caller asked to check
    detail: str = ""
