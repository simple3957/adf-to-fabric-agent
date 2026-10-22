"""Persisted Repository Discovery contracts (project/persistence layer).

This module owns the *data contract* only — the shapes that are serialized to
``projects/<id>/discovery/repository.yaml``. It contains NO implementation
logic: no filesystem traversal, JSON inspection, hashing, candidate detection,
orchestration, or CLI behavior (those live in ``agent/discovery/``). It is
self-contained and imports only pydantic/stdlib, so the project layer never
depends on the agent layer.

The first Discovery domain records WHAT EXISTS — files, sizes, content hashes,
directory structure, and ADF artifact candidates identified from explicit
structural evidence. It records NOTHING about what that means for migration:
there is deliberately no compatibility, supported, risk, complexity, effort,
recommendation, or source->target mapping field here. Those belong to a later
Assessment phase.

Determinism: for a given source commit SHA the inventory is reproducible.
Hashes are content-derived, paths are repository-relative POSIX, and every
list/dict is emitted in sorted order. Timestamps are run metadata only.

Security: no secret value is ever stored. When a candidate artifact contains a
credential-bearing field, only ``credential_present=True`` is recorded, and only
credential-bearing key *names* (never values) inform it.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    """Base model that rejects unknown keys (deterministic, no silent drops)."""

    model_config = ConfigDict(extra="forbid")


class DiscoveryStatus(str, Enum):
    """Per-item discovery outcome, so incomplete discovery stays visible."""

    discovered = "discovered"
    skipped = "skipped"
    failed = "failed"


class AdfArtifactKind(str, Enum):
    """ADF artifact kinds identifiable from explicit structural evidence.

    ``unknown`` marks a file that is a candidate (e.g. JSON under a known
    artifact directory) whose kind could not be deterministically established.
    """

    pipeline = "pipeline"
    dataset = "dataset"
    linked_service = "linkedService"
    trigger = "trigger"
    integration_runtime = "integrationRuntime"
    dataflow = "dataflow"
    factory = "factory"
    managed_virtual_network = "managedVirtualNetwork"
    credential = "credential"
    change_data_capture = "changeDataCapture"
    arm_template = "armTemplate"
    arm_template_parameters = "armTemplateParameters"
    unknown = "unknown"


class KindEvidence(str, Enum):
    """How an artifact kind was determined (deterministic provenance)."""

    top_level_type_field = "top_level_type_field"
    directory_name = "directory_name"
    file_name = "file_name"
    not_determined = "not_determined"


class FileRecord(_Strict):
    """One inventoried repository path. No file contents are ever stored."""

    path: str  # repository-relative, POSIX separators
    name: str
    extension: str  # lowercased, includes leading dot; "" if none
    size_bytes: int
    sha256: Optional[str] = None  # None only when status != discovered
    file_type: str  # extension-derived category, e.g. "json", "yaml", "image"
    status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""  # deterministic, secret-free reason when skipped/failed


class AdfCandidate(_Strict):
    """A repository file identified as a likely ADF artifact by structural evidence.

    Records observed structure ONLY (identity + directly-observable structural
    facts). It makes no claim about migratability, Fabric compatibility,
    complexity, risk, or effort. It is NOT a relationship graph: references are
    recorded as mere presence, never as edges to other artifacts.
    """

    path: str  # repository-relative, POSIX
    name: Optional[str] = None  # explicit JSON "name" field; null if not deterministic
    artifact_kind: AdfArtifactKind
    kind_evidence: KindEvidence
    resource_type: Optional[str] = None  # raw top-level "type" string when present,
    # e.g. "Microsoft.DataFactory/factories/linkedservices"
    structural_type: Optional[str] = None  # properties.type subtype when present,
    # e.g. "AzureSqlTable" / "AzureSqlDatabase" / "ScheduleTrigger"
    activity_count: Optional[int] = None  # pipelines only; deterministic count
    has_parameters: bool = False
    has_variables: bool = False
    has_annotations: bool = False
    has_schema: bool = False  # datasets: properties.schema present
    parameter_names: list[str] = Field(default_factory=list)  # sorted key names only
    connection_field_names: list[str] = Field(default_factory=list)  # linked services:
    # properties.typeProperties key NAMES only (never values), sorted
    linked_service_ref_present: bool = False  # datasets: presence only, NOT the target
    credential_present: bool = False  # a credential-bearing field exists; value NOT stored
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered  # structural read outcome
    note: str = ""


class RepositoryStructure(_Strict):
    total_files: int
    total_directories: int
    total_size_bytes: int
    extension_counts: dict[str, int] = Field(default_factory=dict)  # sorted by key
    directories: list[str] = Field(default_factory=list)  # sorted, relative POSIX
    excluded_paths: list[str] = Field(default_factory=list)  # e.g. [".git/"]


class RepositorySource(_Strict):
    provider: str
    repository_url: str
    branch: str
    commit_sha: str
    local_source_path: str


class RepositoryDiscovery(_Strict):
    """Persisted Repository Discovery result — a factual source inventory.

    Bound to a specific source commit SHA and reproducible for that commit.
    Contains no secret values and no migration interpretation.
    """

    schema_version: str = "repository-discovery/1"
    project_id: str
    source_commit_sha: str
    created_at: datetime
    updated_at: datetime
    source: RepositorySource
    structure: RepositoryStructure
    files: list[FileRecord] = Field(default_factory=list)  # sorted by path
    adf_candidates: list[AdfCandidate] = Field(default_factory=list)  # sorted by path
    artifact_kind_counts: dict[str, int] = Field(default_factory=dict)  # sorted by key
    complete: bool = True  # False if any path was skipped or failed
    skipped_count: int = 0
    failed_count: int = 0
    provenance: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# ADLS Gen2 Discovery contracts (persisted)
#
# Phase 1 records only the top-level storage structure — the filesystems /
# containers visible to the authenticated identity in an explicitly named
# storage account. It records identity + safe container metadata only; it does
# NOT list paths/directories/files, read data, infer schemas, or interpret
# anything for migration. Deterministic: filesystems are sorted by name and
# timestamps are run metadata only. No secret value is ever stored.
# ---------------------------------------------------------------------------


class AdlsDiscoveryStatus(str, Enum):
    """Outcome of an ADLS discovery run.

    ``discovered`` covers a successful listing, INCLUDING an empty one (zero
    filesystems returned). Failures are kept distinct and are never collapsed
    into an empty result.
    """

    discovered = "discovered"
    authentication_failed = "authentication_failed"
    permission_denied = "permission_denied"
    connectivity_failed = "connectivity_failed"
    dependency_unavailable = "dependency_unavailable"
    missing_config = "missing_config"


class AdlsFilesystem(_Strict):
    """One filesystem/container. Safe metadata only — no contents, no paths."""

    name: str
    last_modified: Optional[str] = None  # ISO-8601 string when the SDK provides it


class AdlsDiscovery(_Strict):
    """Persisted ADLS Gen2 Discovery result (Phase 1: filesystem inventory).

    Bound to an explicitly supplied storage account. Contains no secret values
    and no migration interpretation.
    """

    schema_version: str = "adls-discovery/1"
    project_id: str
    storage_account: str
    account_url: str
    status: AdlsDiscoveryStatus
    created_at: datetime
    updated_at: datetime
    filesystem_count: int = 0
    filesystems: list[AdlsFilesystem] = Field(default_factory=list)  # sorted by name
    detail: str = ""  # redacted, secret-free
    provenance: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# SQL Server Discovery contracts (persisted)
#
# Phase 1 records only the STRUCTURAL database estate — schemas, tables, views,
# and stored procedures — obtained from read-only SQL catalog metadata queries
# (sys.schemas / sys.tables / sys.views / sys.procedures). It records identity +
# object structure only; it does NOT read business data, column definitions, row
# values, or counts, and performs no writes/DDL/EXEC. No migration
# interpretation. Deterministic: every list is sorted by (schema, name);
# timestamps are run metadata only. No secret value (password / connection
# string / credential material) is ever stored — only the authentication MODE.
# ---------------------------------------------------------------------------


class SqlDiscoveryStatus(str, Enum):
    """Outcome of a SQL discovery run.

    ``discovered`` covers a successful metadata listing, INCLUDING an empty one.
    Failures are kept distinct and never collapsed into an empty result.
    """

    discovered = "discovered"
    authentication_failed = "authentication_failed"
    permission_denied = "permission_denied"
    connectivity_failed = "connectivity_failed"
    dependency_unavailable = "dependency_unavailable"
    missing_config = "missing_config"
    missing_credential = "missing_credential"
    metadata_query_failed = "metadata_query_failed"


class SqlObject(_Strict):
    """A schema-qualified database object (table / view / stored procedure)."""

    schema: str
    name: str


class SqlDiscovery(_Strict):
    """Persisted SQL Server Discovery result (Phase 1: structural inventory).

    Bound to an explicitly supplied server + database. Contains no secret values
    (only the authentication mode) and no migration interpretation.
    """

    schema_version: str = "sql-discovery/1"
    project_id: str
    server: str
    database: str
    authentication: str  # "integrated" | "sql_login" — the MODE only, never secrets
    status: SqlDiscoveryStatus
    created_at: datetime
    updated_at: datetime
    schema_count: int = 0
    schemas: list[str] = Field(default_factory=list)  # sorted
    table_count: int = 0
    tables: list[SqlObject] = Field(default_factory=list)  # sorted by (schema, name)
    view_count: int = 0
    views: list[SqlObject] = Field(default_factory=list)  # sorted by (schema, name)
    procedure_count: int = 0
    procedures: list[SqlObject] = Field(default_factory=list)  # sorted by (schema, name)
    metadata_queries: list[str] = Field(default_factory=list)  # purpose labels performed
    detail: str = ""  # redacted, secret-free
    provenance: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# ADF Live Discovery contracts (persisted)
#
# Phase 1 records only the top-level artifact INVENTORY of the deployed factory
# — the pipelines / datasets / linked services / triggers / data flows /
# integration runtimes that exist — obtained from read-only Azure Resource
# Manager (ARM) child-collection GETs. It records identity (name + kind) ONLY.
# ARM list responses return each artifact's full properties (and linked-service
# properties can carry secret material); the discoverer reads them but persists
# nothing beyond the name. It does NOT store properties, typeProperties,
# connections, parameters, expressions, activity definitions, or references, and
# performs no writes/activation/runs. No migration interpretation. Deterministic:
# every list is sorted by name; timestamps are run metadata only. No secret value
# (token / connection string / credential material) is ever stored — only the
# authentication MODE.
#
# Referenced data assets (SQL dbs/tables, ADLS paths, REST endpoints) and Azure
# Monitor / Log Analytics linkage are OUT OF SCOPE here — later, separately
# contracted discovery phases own those.
# ---------------------------------------------------------------------------


class AdfDiscoveryStatus(str, Enum):
    """Outcome of an ADF live discovery run.

    ``discovered`` covers a successful enumeration, INCLUDING an empty factory
    (zero artifacts). Failures are kept distinct and never collapsed into an
    empty result.
    """

    discovered = "discovered"
    authentication_failed = "authentication_failed"
    permission_denied = "permission_denied"
    factory_not_found = "factory_not_found"
    connectivity_failed = "connectivity_failed"
    dependency_unavailable = "dependency_unavailable"
    missing_config = "missing_config"
    missing_credential = "missing_credential"


class AdfArtifact(_Strict):
    """One factory artifact. Identity only — NO properties/config/secret material.

    ``kind`` reuses :class:`AdfArtifactKind` (defined above for Repository
    Discovery); Phase 1 populates only its ``pipeline`` / ``dataset`` /
    ``linked_service`` / ``trigger`` / ``dataflow`` / ``integration_runtime``
    members, one per enumerated ARM collection.
    """

    name: str
    kind: AdfArtifactKind


class AdfDiscovery(_Strict):
    """Persisted ADF Live Discovery result (Phase 1: factory artifact inventory).

    Bound to an explicitly supplied subscription + resource group + factory.
    Contains no secret values (only the authentication mode) and no migration
    interpretation.
    """

    schema_version: str = "adf-discovery/1"
    project_id: str
    subscription_id: str
    resource_group: str
    factory_name: str
    authentication: str  # "azure_cli" | "client_secret" | "azure_identity" — MODE only
    status: AdfDiscoveryStatus
    created_at: datetime
    updated_at: datetime
    pipeline_count: int = 0
    pipelines: list[AdfArtifact] = Field(default_factory=list)  # sorted by name
    dataset_count: int = 0
    datasets: list[AdfArtifact] = Field(default_factory=list)  # sorted by name
    linked_service_count: int = 0
    linked_services: list[AdfArtifact] = Field(default_factory=list)  # sorted by name
    trigger_count: int = 0
    triggers: list[AdfArtifact] = Field(default_factory=list)  # sorted by name
    dataflow_count: int = 0
    dataflows: list[AdfArtifact] = Field(default_factory=list)  # sorted by name
    integration_runtime_count: int = 0
    integration_runtimes: list[AdfArtifact] = Field(default_factory=list)  # sorted by name
    artifact_count: int = 0  # total across all kinds
    collections_queried: list[str] = Field(default_factory=list)  # ARM collection labels
    detail: str = ""  # redacted, secret-free
    provenance: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# ADF Semantic / Deep Discovery contracts (persisted)
#
# Phase 1 moves from "what ADF artifacts exist?" to "what is defined inside them
# and what resources do they reference?" — extracted deterministically from the
# IMMUTABLE repository source (the ADF JSON definitions), never from Azure/live.
#
# It records only what the definitions EXPLICITLY contain: pipeline activities +
# ordering + references, dataset asset identity + declared columns, linked-service
# SAFE metadata, triggers, factory global parameters, safe external references,
# and typed artifact relationship edges. It performs NO migration interpretation.
#
# Security: a positive allowlist is authoritative — connection strings and any
# credential/Key-Vault-secret VALUES are never read, parsed, or stored (only a
# ``has_connection_string`` boolean and structural Key Vault *references*). SQL
# server/database are deliberately ``not_extracted`` because they live only inside
# connection strings. A defensive secret-value blocklist is a secondary safeguard.
# Reuses :class:`AdfArtifactKind` (above); no duplicate kind enum.
# ---------------------------------------------------------------------------

# Sentinel recorded where a value exists in the source but is intentionally not
# extracted (e.g. SQL server/database inside a connection string).
NOT_EXTRACTED = "not_extracted"


class AdfSemanticStatus(str, Enum):
    """Outcome of an ADF semantic discovery run."""

    discovered = "discovered"           # all candidate artifacts parsed
    partial = "partial"                 # some artifacts failed to parse
    no_artifacts = "no_artifacts"       # repository discovery found no ADF artifacts
    missing_repository_discovery = "missing_repository_discovery"  # prerequisite not run
    failed = "failed"


class ExternalResourceKind(str, Enum):
    """Kinds of external resource an ADF artifact may explicitly reference."""

    sql_object = "sql_object"
    adls_path = "adls_path"
    rest_endpoint = "rest_endpoint"
    key_vault = "key_vault"
    unknown = "unknown"


class AdfRelationshipKind(str, Enum):
    """Typed, explicitly-represented artifact relationship edges."""

    pipeline_executes_pipeline = "pipeline_executes_pipeline"
    trigger_triggers_pipeline = "trigger_triggers_pipeline"
    pipeline_uses_dataset = "pipeline_uses_dataset"
    pipeline_uses_linked_service = "pipeline_uses_linked_service"
    dataset_uses_linked_service = "dataset_uses_linked_service"
    pipeline_executes_dataflow = "pipeline_executes_dataflow"
    dataflow_uses_dataset = "dataflow_uses_dataset"
    dataflow_uses_linked_service = "dataflow_uses_linked_service"
    dataflow_uses_flowlet = "dataflow_uses_flowlet"
    artifact_references_external = "artifact_references_external"


class AdfParameter(_Strict):
    """A pipeline/dataset/factory parameter or a factory global parameter.

    ``default_value`` is emitted only via the secret-value guard; an expression
    default is captured as its structural text with ``is_expression=True``.
    """

    name: str
    type: Optional[str] = None
    is_expression: bool = False
    default_value: Optional[str] = None  # None when absent OR dropped by secret guard
    # The default exactly as authored (arrays/objects/numbers/booleans/Expression objects),
    # secret-masked. ``has_default`` distinguishes "no default" from an explicit null.
    has_default: bool = False
    default_raw: Optional[Any] = None


class AdfVariable(_Strict):
    name: str
    type: Optional[str] = None
    is_expression: bool = False
    default_value: Optional[str] = None
    has_default: bool = False
    default_raw: Optional[Any] = None


class AdfColumn(_Strict):
    """A dataset column DECLARED in the source (never inferred from live data)."""

    name: str
    type: Optional[str] = None


class AdfDependency(_Strict):
    """An activity ordering edge (``dependsOn``).

    ``conditions`` are normalized to ``Succeeded`` / ``Failed`` / ``Completed`` /
    ``Skipped``; anything else is kept verbatim in ``unknown_conditions``.
    """

    activity: str
    conditions: list[str] = Field(default_factory=list)
    unknown_conditions: list[str] = Field(default_factory=list)
    resolved: bool = True  # the named activity exists in the same scope


class AdfArtifactRef(_Strict):
    """A reference to another ADF artifact, with deterministic resolution."""

    name: str
    kind: AdfArtifactKind
    resolved: bool = False  # True when a discovered artifact of ``kind`` has this name


class AdfKeyVaultRef(_Strict):
    """A structural Key Vault reference. Names only — never the secret value."""

    store_linked_service: str  # referenceName of the AKV linked service
    secret_name: str           # the secret NAME (a reference), never its value


class AdfRecurrence(_Strict):
    frequency: Optional[str] = None
    interval: Optional[int] = None
    time_zone: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    # The trigger's ``recurrence.schedule`` block, flattened (empty = not stated).
    minutes: list[int] = Field(default_factory=list)
    hours: list[int] = Field(default_factory=list)
    week_days: list[str] = Field(default_factory=list)
    month_days: list[int] = Field(default_factory=list)  # negative values count from the end of the month
    monthly_occurrences: list[dict[str, Any]] = Field(default_factory=list)  # [{day, occurrence}]


class ExternalReference(_Strict):
    """A safe, explicitly-represented reference to an external resource.

    Only kind-relevant fields are populated. No secret material: SQL host/db are
    never populated from connection strings (they stay ``None``/not_extracted);
    endpoints come only from allowlisted URL fields.
    """

    resource_kind: ExternalResourceKind
    source_artifact: str
    source_kind: AdfArtifactKind
    is_expression_driven: bool = False
    sql_schema: Optional[str] = None
    sql_table: Optional[str] = None
    sql_stored_procedure: Optional[str] = None
    adls_account: Optional[str] = None
    adls_filesystem: Optional[str] = None
    adls_path: Optional[str] = None
    rest_base_url: Optional[str] = None
    rest_relative_url: Optional[str] = None
    key_vault_url: Optional[str] = None
    key_vault_secret_name: Optional[str] = None
    note: str = ""


class AdfRelationship(_Strict):
    relationship: AdfRelationshipKind
    source_kind: AdfArtifactKind
    source_name: str
    target_kind: AdfArtifactKind
    target_name: str
    resolved: bool = False


# ---------------------------------------------------------------------------
# Activity-level semantic representation (deterministic, no inference engine).
#
# These models extend ``AdfActivity`` / ``AdfPipeline`` in place — they are the
# intermediate representation a later converter consumes. Original property
# values and expressions are preserved next to their interpretation; nothing in
# here is Fabric-specific.
# ---------------------------------------------------------------------------


class AdfSemanticCategory(str, Enum):
    data_movement = "data_movement"
    data_transformation = "data_transformation"
    data_retrieval = "data_retrieval"
    control_flow = "control_flow"
    orchestration = "orchestration"
    external_execution = "external_execution"
    compute = "compute"
    validation = "validation"
    variable_management = "variable_management"
    unknown = "unknown"


class AdfOperationKind(str, Enum):
    read = "read"
    write = "write"
    transform = "transform"
    orchestrate = "orchestrate"
    validate = "validate"
    external_execution = "external_execution"
    branch = "branch"
    loop = "loop"
    filter = "filter"
    wait = "wait"
    fail = "fail"
    delete = "delete"
    set_variable = "set_variable"
    append_variable = "append_variable"
    unknown = "unknown"


class AdfConfidence(str, Enum):
    high = "high"  # known type, required properties present, fully understood
    medium = "medium"  # understood, but something is dynamic/unresolved/contradictory
    low = "low"  # known type but required properties are missing
    unsupported = "unsupported"  # unknown activity type: described structurally only


class AdfIssueSeverity(str, Enum):
    error = "error"
    warning = "warning"
    info = "info"


class AdfParseIssue(_Strict):
    """Something the parser could not fully understand or found inconsistent."""

    code: str
    severity: AdfIssueSeverity = AdfIssueSeverity.warning
    message: str
    property_path: Optional[str] = None


class AdfSourceLocation(_Strict):
    file: str  # repository-relative path of the pipeline JSON
    pointer: str  # JSON pointer-style path, e.g. properties.activities[2].typeProperties.ifTrueActivities[0]


class AdfExpressionRef(_Strict):
    """A reference found inside an expression (never resolved to a value)."""

    kind: str  # pipeline_parameter | global_parameter | variable | activity_output | loop_item | ...
    name: Optional[str] = None
    path: Optional[str] = None  # accessor path after the name, e.g. ".output.firstRow.maxdate"
    raw: str = ""


class AdfExpression(_Strict):
    """One dynamic-content value: the original text AND its analysis."""

    property_path: str
    role: str = "general"  # e.g. loop_collection, source_query, destination_path, condition
    raw: str  # exactly as authored; never rewritten
    form: str  # expression | interpolated | literal | escaped_literal
    is_dynamic: bool = True
    parse_status: str = "parsed"  # parsed | unparsed
    error: Optional[str] = None
    references: list[AdfExpressionRef] = Field(default_factory=list)
    functions: list[str] = Field(default_factory=list)  # as written, sorted, unique
    unknown_functions: list[str] = Field(default_factory=list)  # not in the documented set
    function_groups: list[str] = Field(default_factory=list)  # datetime, string, collection, ...
    literal_text: str = ""  # static text of an interpolated string (for query/comparison analysis)


class AdfIO(_Strict):
    """A data endpoint an activity reads from or writes to."""

    role: str  # input | output | source | sink | target
    kind: str = "dataset"  # dataset | inline | dataflow
    name: Optional[str] = None  # dataset name
    connector_type: Optional[str] = None  # e.g. AzureSqlSource / ParquetSink
    store: Optional[str] = None  # e.g. AzureBlobFS (from store settings)
    format: Optional[str] = None  # e.g. DelimitedText / Parquet
    dataset_type: Optional[str] = None  # filled when the dataset resolves
    linked_service: Optional[str] = None
    connection_type: Optional[str] = None  # linked service type, when resolved
    resolved: Optional[bool] = None  # None = not checked yet
    parameters: dict[str, Any] = Field(default_factory=dict)  # arguments passed to the dataset
    location: dict[str, Any] = Field(default_factory=dict)  # table/schema/path/query/stored_procedure
    declared_columns: Optional[int] = None
    settings: dict[str, Any] = Field(default_factory=dict)  # source/sink options, preserved


class AdfActivityReference(_Strict):
    kind: str  # pipeline | dataset | linkedService | dataflow | integrationRuntime | notebook | ...
    name: str
    role: str  # property path it was found at
    resolved: Optional[bool] = None  # None = this kind is not inventoried


class AdfPolicy(_Strict):
    timeout: Optional[str] = None
    retry: Optional[int] = None
    retry_interval_in_seconds: Optional[int] = None
    secure_input: Optional[bool] = None
    secure_output: Optional[bool] = None
    is_dynamic: bool = False  # some policy value is an expression
    raw: dict[str, Any] = Field(default_factory=dict)  # every policy key, as authored


class AdfActivityUsage(_Strict):
    """Where an activity's output (or a variable it sets) is consumed."""

    activity_id: str
    property_path: str
    via: str  # activity_output | variable
    detail: str = ""


class AdfPattern(_Strict):
    name: str
    description: str
    activities: list[str] = Field(default_factory=list)  # activity ids
    evidence: list[str] = Field(default_factory=list)


class AdfActivity(_Strict):
    """A pipeline activity. Nested container activities recurse via ``activities``.

    The first block of fields is the original structural record (unchanged). The
    rest is the semantic representation; every field has a default so records
    written before it existed still load.
    """

    name: str
    type: Optional[str] = None
    depends_on: list[AdfDependency] = Field(default_factory=list)
    dataset_refs: list[str] = Field(default_factory=list)
    linked_service_refs: list[str] = Field(default_factory=list)
    pipeline_refs: list[str] = Field(default_factory=list)
    source_type: Optional[str] = None
    sink_type: Optional[str] = None
    stored_procedures: list[str] = Field(default_factory=list)
    activities: list["AdfActivity"] = Field(default_factory=list)  # nested (ForEach/If/Until/Switch)
    note: str = ""

    # --- semantic representation ---
    activity_id: str = ""  # <pipeline>/<ancestor>/.../<name>, unique within the pipeline
    semantic_category: AdfSemanticCategory = AdfSemanticCategory.unknown
    operation: AdfOperationKind = AdfOperationKind.unknown  # primary operation
    operations: list[AdfOperationKind] = Field(default_factory=list)
    semantic_tags: list[str] = Field(default_factory=list)  # evidence-backed, e.g. watermark_lookup
    parent_activity_id: Optional[str] = None
    nesting_path: list[str] = Field(default_factory=list)  # ancestor activity names, outermost first
    branch: Optional[str] = None  # which container slot: activities | ifTrue | ifFalse | default | case:<v>
    execution_order: Optional[int] = None  # topological rank within its scope (None if cyclic)
    stage: Optional[int] = None  # parallel stage within its scope (0 = no dependencies)
    dependents: list[str] = Field(default_factory=list)  # sibling activities that depend on this one
    inputs: list[AdfIO] = Field(default_factory=list)
    outputs: list[AdfIO] = Field(default_factory=list)
    expressions: list[AdfExpression] = Field(default_factory=list)
    parameters: list[str] = Field(default_factory=list)  # pipeline parameters referenced
    global_parameters: list[str] = Field(default_factory=list)
    variables_read: list[str] = Field(default_factory=list)
    variables_written: list[str] = Field(default_factory=list)
    configuration: dict[str, Any] = Field(default_factory=dict)  # interpreted, activity-specific
    raw_properties: dict[str, Any] = Field(default_factory=dict)  # typeProperties as authored (secret-masked)
    unknown_properties: dict[str, Any] = Field(default_factory=dict)  # preserved, not understood
    missing_properties: list[str] = Field(default_factory=list)  # required but absent
    empty_properties: list[str] = Field(default_factory=list)  # present but explicitly empty
    policy: Optional[AdfPolicy] = None
    state: Optional[str] = None  # Active | Inactive (deactivated activities)
    description: Optional[str] = None
    references: list[AdfActivityReference] = Field(default_factory=list)
    output_consumers: list[AdfActivityUsage] = Field(default_factory=list)
    semantic_summary: str = ""
    confidence: AdfConfidence = AdfConfidence.unsupported
    warnings: list[AdfParseIssue] = Field(default_factory=list)
    source_location: Optional[AdfSourceLocation] = None


class AdfDependencyEdge(_Strict):
    """One ``dependsOn`` entry as a graph edge: ``from`` must finish (per ``conditions``) before ``to`` runs."""

    scope: str  # "<parent activity id or pipeline name>|<branch>": dependencies never cross a scope
    from_id: Optional[str] = None  # None when the named activity is not in this scope
    from_name: str
    to_id: str
    conditions: list[str] = Field(default_factory=list)  # Succeeded | Failed | Skipped | Completed
    unknown_conditions: list[str] = Field(default_factory=list)
    resolved: bool = True
    problem: Optional[str] = None  # unresolved | outside_scope | self


class AdfDependencyGraph(_Strict):
    """The explicit activity dependency graph of one pipeline (every scope, nested activities included)."""

    nodes: list[str] = Field(default_factory=list)  # activity ids, pre-order
    edges: list[AdfDependencyEdge] = Field(default_factory=list)
    cycles: list[list[str]] = Field(default_factory=list)  # activity ids of each cycle, sorted
    scope_count: int = 0


class AdfInventoryCheck(_Strict):
    """Reconciles the activities the parser reported with an independent scan of the raw pipeline JSON.

    ``matched`` is False when the source holds an activity the record does not (a component that would have been
    silently dropped), or the record holds one the scan did not find. Pointers use the same form as
    ``AdfSourceLocation.pointer``.
    """

    source_activity_count: int = 0
    parsed_activity_count: int = 0
    matched: bool = True
    missing_pointers: list[str] = Field(default_factory=list)  # in the source, not in the record
    unexpected_pointers: list[str] = Field(default_factory=list)  # in the record, not found in the source scan
    malformed_pointers: list[str] = Field(default_factory=list)  # list entries that are not objects


class AdfPipeline(_Strict):
    name: str
    path: str
    folder: Optional[str] = None
    parameters: list[AdfParameter] = Field(default_factory=list)
    variables: list[AdfVariable] = Field(default_factory=list)
    activities: list[AdfActivity] = Field(default_factory=list)  # source order preserved
    activity_count: int = 0  # top-level activity count
    pipeline_dependencies: list[str] = Field(default_factory=list)  # ExecutePipeline targets, sorted
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""

    # --- semantic representation ---
    description: Optional[str] = None
    annotations: list[str] = Field(default_factory=list)
    concurrency: Optional[int] = None
    activity_total: int = 0  # every activity, nested included
    activity_type_counts: dict[str, int] = Field(default_factory=dict)  # sorted by type
    unknown_activity_types: list[str] = Field(default_factory=list)
    patterns: list[AdfPattern] = Field(default_factory=list)
    issues: list[AdfParseIssue] = Field(default_factory=list)  # pipeline-level problems
    unknown_properties: dict[str, Any] = Field(default_factory=dict)
    raw_properties: dict[str, Any] = Field(default_factory=dict)  # pipeline settings (policy, runDimensions...)
    dependency_graph: Optional[AdfDependencyGraph] = None
    inventory: Optional[AdfInventoryCheck] = None


class AdfDataset(_Strict):
    name: str
    path: str
    folder: Optional[str] = None
    type: Optional[str] = None
    linked_service_ref: Optional[AdfArtifactRef] = None
    parameters: list[AdfParameter] = Field(default_factory=list)
    columns: list[AdfColumn] = Field(default_factory=list)  # declared columns only
    asset: Optional[ExternalReference] = None  # source/sink asset identity
    # The dataset's typeProperties as authored (secret-masked; dynamic content kept as
    # Expression objects). Needed to inline the dataset into a target pipeline.
    type_properties: dict[str, Any] = Field(default_factory=dict)
    annotations: list[str] = Field(default_factory=list)
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""
    # Arguments this dataset passes to its linked service's parameters (linkedServiceName.parameters), as authored.
    linked_service_arguments: dict[str, Any] = Field(default_factory=dict)


class AdfLinkedConnection(_Strict):
    """What a linked service states outright (``server``, ``database``, ``userName`` properties). Never a secret."""

    server: Optional[str] = None
    database: Optional[str] = None
    user_name: Optional[str] = None  # a login NAME, never a password
    parameterized_fields: list[str] = Field(default_factory=list)  # fields that use linkedService().X


class AdfSparkCompute(_Strict):
    """The cluster a Databricks linked service describes (explicit, non-secret settings only).

    Used to size the Fabric Spark compute that would replace it. A value authored as an expression or a linked
    service parameter is not read; its field name is listed in ``unresolved_fields`` instead of being guessed.
    """

    platform: str = "databricks"
    mode: str = "unspecified"  # existing_cluster | new_cluster | instance_pool | unspecified
    workspace: Optional[str] = None  # the workspace domain (a URL, not a credential)
    workspace_resource_id: Optional[str] = None  # ARM id of the workspace (region and pricing tier are read from it)
    existing_cluster_id: Optional[str] = None
    instance_pool_id: Optional[str] = None
    policy_id: Optional[str] = None
    node_type: Optional[str] = None
    driver_node_type: Optional[str] = None
    min_workers: Optional[int] = None
    max_workers: Optional[int] = None  # equal to min_workers when the cluster is a fixed size
    runtime_version: Optional[str] = None  # as authored, e.g. 14.3.x-scala2.12
    spark_conf: dict[str, str] = Field(default_factory=dict)  # values that look like secrets are masked
    spark_env_var_names: list[str] = Field(default_factory=list)  # names only
    init_script_count: int = 0
    custom_tag_names: list[str] = Field(default_factory=list)
    unresolved_fields: list[str] = Field(default_factory=list)


class AdfLinkedService(_Strict):
    name: str
    path: str
    type: Optional[str] = None
    compute: Optional[AdfSparkCompute] = None  # cluster settings, for Databricks linked services
    endpoint: Optional[str] = None  # non-secret url/baseUrl only; never a connection string
    has_connection_string: bool = False  # presence only; value NEVER read
    key_vault_refs: list[AdfKeyVaultRef] = Field(default_factory=list)
    connection_field_names: list[str] = Field(default_factory=list)  # key NAMES only
    credential_present: bool = False
    authentication_type: Optional[str] = None
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""
    integration_runtime: Optional[str] = None  # connectVia reference name, if any
    # azure | self_hosted | managed_vnet, from the runtime's own definition; None = not classified (runtime
    # artifact missing or of a type we do not classify), which the converter treats cautiously.
    integration_runtime_kind: Optional[str] = None
    # typeProperties.version as written ('1.0', '2.0', ...); None when the linked service does not state one.
    # Several ADF connectors have a removed V1 and a current V2 under the same type name.
    connector_version: Optional[str] = None
    # Explicitly authored, non-secret connection properties. Connection strings are NEVER read or parsed, so a linked
    # service that uses one has an empty ``connection`` (the UI asks the user for the values instead of guessing them).
    connection: "AdfLinkedConnection" = Field(default_factory=lambda: AdfLinkedConnection())
    parameters: list["AdfParameter"] = Field(default_factory=list)  # the linked service's own parameters


class AdfEventTrigger(_Strict):
    """What an event trigger listens to, so someone can recreate it as a Fabric Activator rule. Descriptive only."""

    scope: Optional[str] = None  # storage account or Event Grid topic resource id
    events: list[str] = Field(default_factory=list)  # e.g. Microsoft.Storage.BlobCreated
    blob_path_begins_with: Optional[str] = None
    blob_path_ends_with: Optional[str] = None
    ignore_empty_blobs: Optional[bool] = None
    subject_begins_with: Optional[str] = None  # custom events
    subject_ends_with: Optional[str] = None


class AdfTrigger(_Strict):
    name: str
    path: str
    type: Optional[str] = None
    runtime_state: Optional[str] = None
    pipeline_refs: list[str] = Field(default_factory=list)
    # pipeline name -> {parameter: value or expression text}, as the trigger passes them (secret-shaped values dropped).
    pipeline_parameters: dict[str, dict[str, str]] = Field(default_factory=dict)
    event: Optional[AdfEventTrigger] = None
    recurrence: Optional[AdfRecurrence] = None
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""


class AdfIntegrationRuntime(_Strict):
    """An integration runtime definition: what kind it is and the non-secret facts that decide how Fabric can reach data."""

    name: str
    path: str
    type: Optional[str] = None  # Managed | SelfHosted
    kind: Optional[str] = None  # azure | self_hosted | managed_vnet (None: unclassified, e.g. Azure-SSIS)
    is_ssis: bool = False
    managed_virtual_network: Optional[str] = None
    compute_location: Optional[str] = None
    shared_from: Optional[str] = None  # a self-hosted runtime shared from another factory (resource id, no key)
    description: Optional[str] = None
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""


class AdfChangeDataCapture(_Strict):
    """An ADF change data capture resource (``Microsoft.DataFactory/factories/adfcdcs``), described, not converted.

    Fabric has no CDC artifact: Microsoft's comparison says incremental data movement moves to Copy jobs.
    """

    name: str
    path: str
    status: Optional[str] = None  # running / stopped, as stored
    mode: Optional[str] = None  # batch | continuous
    frequency: Optional[str] = None  # batch mode only
    interval: Optional[int] = None
    source_linked_services: list[str] = Field(default_factory=list)
    target_linked_services: list[str] = Field(default_factory=list)
    source_tables: list[str] = Field(default_factory=list)
    target_tables: list[str] = Field(default_factory=list)
    mapping_count: int = 0
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""


class AdfFactoryConfig(_Strict):
    name: str
    path: str
    global_parameters: list[AdfParameter] = Field(default_factory=list)


class AdfDataFlowNode(_Strict):
    """A source, sink or transformation of a data flow, as authored (``typeProperties.sources|sinks|transformations``)."""

    role: str  # source | sink | transformation
    name: str = ""
    description: Optional[str] = None
    dataset: Optional[str] = None
    linked_service: Optional[str] = None
    flowlet: Optional[str] = None  # a flowlet this node embeds
    schema_linked_service: Optional[str] = None
    rejected_data_linked_service: Optional[str] = None  # sinks only
    has_script: bool = False  # a Power Query source carries a script (its text is never stored)
    pointer: str = ""


class AdfDataFlow(_Strict):
    """A data flow definition (``MappingDataFlow``, ``Flowlet`` or ``WranglingDataFlow``), described, not converted.

    The transformation logic lives in ``script`` / ``scriptLines`` (data flow script). Only its size is recorded
    here, never its content: it is parsed by the converter's own stage, and may embed expressions and literals.
    """

    name: str
    path: str
    flow_type: Optional[str] = None  # MappingDataFlow | Flowlet | WranglingDataFlow | <as authored>
    folder: Optional[str] = None
    description: Optional[str] = None
    annotations: list[str] = Field(default_factory=list)
    sources: list[AdfDataFlowNode] = Field(default_factory=list)
    sinks: list[AdfDataFlowNode] = Field(default_factory=list)
    transformations: list[AdfDataFlowNode] = Field(default_factory=list)
    has_script: bool = False
    script_line_count: int = 0
    document_locale: Optional[str] = None  # wrangling flows
    dataset_refs: list[str] = Field(default_factory=list)
    linked_service_refs: list[str] = Field(default_factory=list)
    flowlet_refs: list[str] = Field(default_factory=list)
    unknown_properties: dict[str, Any] = Field(default_factory=dict)  # preserved, not understood
    issues: list[AdfParseIssue] = Field(default_factory=list)
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""
    source_location: Optional[AdfSourceLocation] = None


class AdfUnprocessedArtifact(_Strict):
    """A source artifact that was seen but not turned into a record, and why.

    Nothing seen in the source disappears: it is either a record in this discovery or an entry here.
    """

    path: str
    kind: str  # an AdfArtifactKind value, or "unknown"
    name: Optional[str] = None
    reason: str  # malformed_json | unreadable | not_a_json_object | parse_error | skipped | kind_not_parsed
    detail: str = ""  # secret-free


# Reasons that mean something the source holds is missing from the record (the run is then ``partial`` and a
# conversion made from it is not complete). ``kind_not_parsed`` is informational: the artifact is seen and listed, but
# this phase does not interpret that kind.
UNPROCESSED_LOSS_REASONS = frozenset({"malformed_json", "unreadable", "not_a_json_object", "parse_error", "skipped"})


class AdfSemanticDiscovery(_Strict):
    """Persisted ADF Semantic Discovery result (Phase 1: repository-derived).

    Bound to the source commit; reproducible for that commit. Secret-free.
    """

    schema_version: str = "adf-semantic-discovery/1"
    project_id: str
    source_commit_sha: str
    status: AdfSemanticStatus
    created_at: datetime
    updated_at: datetime
    pipeline_count: int = 0
    dataset_count: int = 0
    linked_service_count: int = 0
    trigger_count: int = 0
    factory_count: int = 0
    activity_count: int = 0  # every pipeline activity, nested included
    activity_type_counts: dict[str, int] = Field(default_factory=dict)
    unknown_activity_types: list[str] = Field(default_factory=list)
    parse_issue_count: int = 0  # pipeline + activity issues across all pipelines
    pipelines: list[AdfPipeline] = Field(default_factory=list)  # sorted by name
    datasets: list[AdfDataset] = Field(default_factory=list)  # sorted by name
    linked_services: list[AdfLinkedService] = Field(default_factory=list)  # sorted by name
    triggers: list[AdfTrigger] = Field(default_factory=list)  # sorted by name
    factory: list[AdfFactoryConfig] = Field(default_factory=list)  # sorted by name
    change_data_captures: list[AdfChangeDataCapture] = Field(default_factory=list)  # sorted by name
    integration_runtimes: list[AdfIntegrationRuntime] = Field(default_factory=list)  # sorted by name
    dataflows: list[AdfDataFlow] = Field(default_factory=list)  # sorted by name
    dataflow_count: int = 0
    unprocessed_artifacts: list[AdfUnprocessedArtifact] = Field(default_factory=list)  # sorted by path
    inventory_mismatch_count: int = 0  # pipelines whose activity inventory did not reconcile with the source
    external_references: list[ExternalReference] = Field(default_factory=list)  # sorted, deduped
    relationships: list[AdfRelationship] = Field(default_factory=list)  # sorted, deduped
    unresolved_references: list[AdfArtifactRef] = Field(default_factory=list)  # sorted
    parsed_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    detail: str = ""  # redacted, secret-free
    provenance: dict[str, str] = Field(default_factory=dict)


# Self-referential AdfActivity (nested activities) — finalize the forward ref.
AdfActivity.model_rebuild()


# ---------------------------------------------------------------------------
# SQL Server Discovery — Phase 2 contracts (structural metadata, persisted)
#
# Phase 2 moves from the Phase-1 catalog INVENTORY (schemas/tables/views/procs)
# to STRUCTURAL metadata: columns + data types + nullability + identity/computed,
# primary keys, foreign keys, and the structural relationships between tables.
# All of it is obtained from read-only SQL catalog (sys.*) queries — NO business
# data, no row reads, no profiling, no writes/DDL/EXEC, no stored-proc bodies.
#
# It is a SEPARATE contract persisted to discovery/sql-structural.yaml; the
# Phase-1 ``SqlDiscovery`` contract is never mutated. Reuses ``SqlDiscoveryStatus``
# (above) — no duplicate status enum. Deterministic: every list is ordered by
# (schema, table, ordinal). No secret value is ever stored (only the auth MODE).
# ---------------------------------------------------------------------------


class SqlColumn(_Strict):
    """One column of a user table (structural metadata only — never data)."""

    schema: str
    table: str
    ordinal: int  # column_id (1-based source order)
    name: str
    data_type: str
    max_length: Optional[int] = None  # bytes as reported by sys.columns (-1 = max)
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = False
    is_identity: bool = False
    is_computed: bool = False


class SqlKeyColumn(_Strict):
    """A column participating in a primary key, with its key ordinal."""

    column: str
    ordinal: int  # key_ordinal


class SqlPrimaryKey(_Strict):
    schema: str
    table: str
    name: str
    columns: list[SqlKeyColumn] = Field(default_factory=list)  # ordered by key_ordinal


class SqlForeignKeyColumn(_Strict):
    parent_column: str
    referenced_column: str
    ordinal: int  # constraint_column_id


class SqlForeignKey(_Strict):
    name: str
    parent_schema: str
    parent_table: str
    referenced_schema: str
    referenced_table: str
    columns: list[SqlForeignKeyColumn] = Field(default_factory=list)  # ordered


class SqlTableStructure(_Strict):
    """A user table's structure: columns + primary key + outgoing foreign keys."""

    schema: str
    table: str
    columns: list[SqlColumn] = Field(default_factory=list)  # ordered by ordinal
    primary_key: Optional[SqlPrimaryKey] = None
    foreign_keys: list[SqlForeignKey] = Field(default_factory=list)  # ordered by name


class SqlRelationship(_Strict):
    """A directed foreign-key relationship edge: parent table -> referenced table."""

    parent_schema: str
    parent_table: str
    referenced_schema: str
    referenced_table: str
    foreign_key_name: str
    columns: list[SqlForeignKeyColumn] = Field(default_factory=list)  # ordered


class SqlStructuralDiscovery(_Strict):
    """Persisted SQL Server Discovery Phase 2 result (structural metadata).

    Bound to an explicit server + database. Secret-free (only the auth mode).
    Reproducible: deterministic ordering; timestamps are run metadata only.
    """

    schema_version: str = "sql-structural-discovery/1"
    project_id: str
    server: str
    database: str
    authentication: str  # "integrated" | "sql_login" — MODE only, never secrets
    status: SqlDiscoveryStatus
    created_at: datetime
    updated_at: datetime
    table_count: int = 0
    column_count: int = 0
    primary_key_count: int = 0
    foreign_key_count: int = 0
    tables: list[SqlTableStructure] = Field(default_factory=list)  # sorted (schema, table)
    relationships: list[SqlRelationship] = Field(default_factory=list)  # sorted, deduped
    metadata_queries: list[str] = Field(default_factory=list)  # purpose labels performed
    detail: str = ""  # redacted, secret-free
    provenance: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# ADLS Gen2 Discovery — Phase 2 contracts (directory / path / file inventory)
#
# Phase 2 records the directories and files INSIDE the filesystem(s) that Phase 1
# discovered, obtained from the ADLS Gen2 data-plane path listing
# (``get_paths(recursive=True)``, fully paginated). It captures only SAFE
# structural metadata — path / name / directory / is_directory / size /
# last_modified. It NEVER reads file contents, downloads, parses data, infers
# schemas, profiles, counts rows, or writes/deletes/renames. No migration
# interpretation and no ADF<->ADLS reconciliation.
#
# It is a SEPARATE contract persisted to discovery/adls-paths.yaml; the Phase-1
# ``AdlsDiscovery`` contract is never mutated. Reuses ``AdlsDiscoveryStatus``
# (above) — no duplicate status enum. Deterministic: paths are sorted by
# (filesystem, path); timestamps are run metadata only. No secret value (token /
# SAS / account key / connection string / auth header) is ever stored.
# ---------------------------------------------------------------------------


class AdlsPath(_Strict):
    """One directory or file inside a filesystem. Safe metadata only — no contents."""

    filesystem: str
    path: str  # full path within the filesystem (POSIX, no leading slash)
    name: str  # basename (last path segment)
    directory: str = ""  # parent path within the filesystem; "" at the root
    is_directory: bool = False
    size_bytes: Optional[int] = None  # files only (content length); None for directories
    last_modified: Optional[str] = None  # ISO-8601 string when the SDK provides it


class AdlsFilesystemPaths(_Strict):
    """The enumerated paths of a single filesystem, with summary counts."""

    name: str  # filesystem/container name
    path_count: int = 0
    directory_count: int = 0
    file_count: int = 0
    total_size_bytes: int = 0
    paths: list[AdlsPath] = Field(default_factory=list)  # sorted by path
    complete: bool = True  # False if the pagination backstop truncated the listing
    truncated: bool = False


class AdlsPathsDiscovery(_Strict):
    """Persisted ADLS Gen2 Discovery Phase 2 result (path/file inventory).

    Bound to the storage account + filesystem(s) recorded by Phase 1. Secret-free
    and reproducible (deterministic ordering; timestamps are run metadata only).
    """

    schema_version: str = "adls-paths-discovery/1"
    project_id: str
    storage_account: str
    account_url: str
    status: AdlsDiscoveryStatus
    created_at: datetime
    updated_at: datetime
    filesystem_count: int = 0
    filesystems: list[AdlsFilesystemPaths] = Field(default_factory=list)  # sorted by name
    total_path_count: int = 0
    total_directory_count: int = 0
    total_file_count: int = 0
    total_size_bytes: int = 0
    detail: str = ""  # redacted, secret-free
    provenance: dict[str, str] = Field(default_factory=dict)
