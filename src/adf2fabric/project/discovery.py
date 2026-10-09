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
from typing import Optional

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


class AdfVariable(_Strict):
    name: str
    type: Optional[str] = None
    is_expression: bool = False
    default_value: Optional[str] = None


class AdfColumn(_Strict):
    """A dataset column DECLARED in the source (never inferred from live data)."""

    name: str
    type: Optional[str] = None


class AdfDependency(_Strict):
    """An activity ordering edge (``dependsOn``)."""

    activity: str
    conditions: list[str] = Field(default_factory=list)


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


class AdfActivity(_Strict):
    """A pipeline activity. Nested container activities recurse via ``activities``."""

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


class AdfDataset(_Strict):
    name: str
    path: str
    folder: Optional[str] = None
    type: Optional[str] = None
    linked_service_ref: Optional[AdfArtifactRef] = None
    parameters: list[AdfParameter] = Field(default_factory=list)
    columns: list[AdfColumn] = Field(default_factory=list)  # declared columns only
    asset: Optional[ExternalReference] = None  # source/sink asset identity
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""


class AdfLinkedService(_Strict):
    name: str
    path: str
    type: Optional[str] = None
    endpoint: Optional[str] = None  # non-secret url/baseUrl only; never a connection string
    has_connection_string: bool = False  # presence only; value NEVER read
    key_vault_refs: list[AdfKeyVaultRef] = Field(default_factory=list)
    connection_field_names: list[str] = Field(default_factory=list)  # key NAMES only
    credential_present: bool = False
    authentication_type: Optional[str] = None
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""


class AdfTrigger(_Strict):
    name: str
    path: str
    type: Optional[str] = None
    runtime_state: Optional[str] = None
    pipeline_refs: list[str] = Field(default_factory=list)
    recurrence: Optional[AdfRecurrence] = None
    parse_status: DiscoveryStatus = DiscoveryStatus.discovered
    note: str = ""


class AdfFactoryConfig(_Strict):
    name: str
    path: str
    global_parameters: list[AdfParameter] = Field(default_factory=list)


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
    pipelines: list[AdfPipeline] = Field(default_factory=list)  # sorted by name
    datasets: list[AdfDataset] = Field(default_factory=list)  # sorted by name
    linked_services: list[AdfLinkedService] = Field(default_factory=list)  # sorted by name
    triggers: list[AdfTrigger] = Field(default_factory=list)  # sorted by name
    factory: list[AdfFactoryConfig] = Field(default_factory=list)  # sorted by name
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
