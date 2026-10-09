"""Minimal data models recorded by the Intake Agent.

Intentionally small. Intake answers only "where is the source and can we
obtain it?" - so a project is just an identity plus the metadata describing
the source repository we fetched. Nothing here describes the *contents* of the
repository; that is Discovery's job in a later stage.

Security: no credential/token field exists on any model here, by design. The
GitHub token is used only for the git operation and never persisted.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class RepositoryMetadata(BaseModel):
    """What we recorded about the fetched source repository.

    ``repository_url`` is always the token-free (scrubbed) URL. ``local_source_path``
    is the canonical local copy that future stages (Discovery onward) consume
    directly instead of re-cloning.
    """

    provider: str = Field(description="Source provider, e.g. 'github'.")
    repository_url: str = Field(
        description="Token-free clone URL of the source repository."
    )
    branch: str = Field(description="Branch that was fetched.")
    commit_sha: str = Field(description="HEAD commit SHA of the fetched branch.")
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the source was fetched (or last confirmed).",
    )
    local_source_path: str = Field(
        description="Canonical local path of the fetched repository "
        "(projects/<id>/source/). Source of truth for later stages."
    )


class Project(BaseModel):
    """A migration engagement's identity plus the source it points at."""

    id: str = Field(description="Slugified project id, used as the on-disk folder name.")
    name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    repository: RepositoryMetadata


# ---------------------------------------------------------------------------
# Consultation models
#
# The Consultation phase is a deterministic readiness gate for the current
# engagement: Azure Data Factory + SQL Server + ADLS Gen2 -> Microsoft Fabric,
# like-for-like. It captures an explicit questionnaire, validates access across
# five connectivity domains, and records a READY/BLOCKED decision plus a
# Discovery handoff contract.
#
# Security: NONE of these models carry a secret. Credentials are described only
# by *references* (env-var names, auth mechanism). Actual passwords/tokens/
# secrets are resolved from the environment at validation time and never
# persisted. All models forbid unknown keys so a typo fails loudly rather than
# being silently dropped.
# ---------------------------------------------------------------------------


class _Strict(BaseModel):
    """Base model that rejects unknown keys (deterministic, no silent drops)."""

    model_config = ConfigDict(extra="forbid")


# --- fixed current-scope enums ---------------------------------------------

class MigrationSourceTool(str, Enum):
    azure_data_factory = "azure_data_factory"


class MigrationWarehouse(str, Enum):
    sql_server = "sql_server"


class MigrationTarget(str, Enum):
    microsoft_fabric = "microsoft_fabric"


class MigrationMode(str, Enum):
    """Current + known future modes. Only ``like_for_like`` is supported today.

    Future modes are enumerated so an out-of-capability request can be *parsed*
    and explicitly flagged as unsupported rather than rejected or silently
    reinterpreted as supported.
    """

    like_for_like = "like_for_like"
    redesign = "redesign"
    functional_transformation = "functional_transformation"
    modernization = "modernization"
    optimization = "optimization"
    restructuring = "restructuring"
    naming_transformation = "naming_transformation"
    alternative_architecture = "alternative_architecture"


# The only migration mode the current migrator supports.
SUPPORTED_MODES: frozenset[MigrationMode] = frozenset({MigrationMode.like_for_like})


class AccessDomain(str, Enum):
    repository = "repository"
    adf = "adf"
    sql_server = "sql_server"
    adls_gen2 = "adls_gen2"
    fabric = "fabric"


class ConnectivityStatus(str, Enum):
    validated = "validated"
    failed = "failed"
    not_required = "not_required"
    missing_config = "missing_config"
    missing_credential = "missing_credential"
    dependency_unavailable = "dependency_unavailable"


class ReadinessStatus(str, Enum):
    ready = "ready"
    blocked = "blocked"


# --- credential references (never actual secrets) --------------------------

class AzureCredentialRef(_Strict):
    """Reference to an Azure credential. Holds env-var *names*, never values."""

    auth: str = Field(description="'client_secret' or 'azure_identity'.")
    tenant_id_env: Optional[str] = None
    client_id_env: Optional[str] = None
    client_secret_env: Optional[str] = None


class SqlCredentialRef(_Strict):
    """Reference to a SQL Server credential. ``password_env`` names an env var."""

    auth: str = Field(description="'sql_login' or 'integrated'.")
    username: Optional[str] = None
    password_env: Optional[str] = None


# --- per-domain access configuration (from the questionnaire) --------------

class RepositoryAccess(_Strict):
    required: bool
    mandatory: bool = True
    owner: Optional[str] = None
    token_env: str = "GITHUB_TOKEN"
    additional_repositories: list[str] = Field(default_factory=list)
    submodules_expected: bool = False


class AdfAccess(_Strict):
    required: bool
    mandatory: bool = True
    owner: Optional[str] = None
    subscription_id: Optional[str] = None
    resource_group: Optional[str] = None
    factory_name: Optional[str] = None
    environment: Optional[str] = None
    credential: Optional[AzureCredentialRef] = None


class SqlTlsMode(str, Enum):
    """Explicit TLS policy for a SQL Server connection.

    The connector translates the chosen mode to ODBC options deterministically
    and never infers, weakens, or falls back between modes:

    - ``strict``                    -> Encrypt=yes, TrustServerCertificate=no
    - ``trust_server_certificate``  -> Encrypt=yes, TrustServerCertificate=yes
    - ``disabled``                  -> Encrypt=no
    """

    strict = "strict"
    trust_server_certificate = "trust_server_certificate"
    disabled = "disabled"


class SqlTlsPolicy(_Strict):
    """Explicit, user-selected TLS policy. Never chosen automatically."""

    mode: SqlTlsMode


class SqlServerAccess(_Strict):
    required: bool
    mandatory: bool = True
    owner: Optional[str] = None
    server: Optional[str] = None
    database: Optional[str] = None
    # Explicit connection details. None means "not supplied" — the connector
    # never assumes a value (no default port or TLS setting). ``driver`` names
    # the local ODBC driver (a client-side detail, not server topology); it has
    # a documented default the user can override.
    port: Optional[int] = None
    driver: Optional[str] = None
    # TLS is an EXPLICIT policy. When omitted, the connector emits no encryption
    # options and the ODBC driver's own defaults apply — a security-sensitive
    # option is never silently selected on the user's behalf.
    tls: Optional[SqlTlsPolicy] = None
    connect_timeout: Optional[int] = None
    environment: Optional[str] = None
    credential: Optional[SqlCredentialRef] = None


class AdlsGen2Access(_Strict):
    required: bool
    mandatory: bool = True
    owner: Optional[str] = None
    storage_account: Optional[str] = None
    filesystem: Optional[str] = None
    environment: Optional[str] = None
    credential: Optional[AzureCredentialRef] = None


class FabricAccess(_Strict):
    required: bool
    mandatory: bool = True
    owner: Optional[str] = None
    tenant: Optional[str] = None
    workspace_id: Optional[str] = None
    environment: Optional[str] = None
    credential: Optional[AzureCredentialRef] = None


class AccessConfig(_Strict):
    """All five current connectivity domains. Each must be declared explicitly."""

    repository: RepositoryAccess
    adf: AdfAccess
    sql_server: SqlServerAccess
    adls_gen2: AdlsGen2Access
    fabric: FabricAccess


# --- questionnaire (authoritative input) -----------------------------------

class ArtifactRef(_Strict):
    """A user-named artifact requested for migration. Recorded verbatim; NOT
    resolved against source/ (that is Discovery's job)."""

    name: str
    type: Optional[str] = None


class RequestedScope(_Strict):
    whole_repository: bool
    artifacts: list[ArtifactRef] = Field(default_factory=list)


class MigrationDefinition(_Strict):
    source_tool: MigrationSourceTool
    source_warehouse: MigrationWarehouse
    target_platform: MigrationTarget
    mode: MigrationMode
    objective: str


class AdditionalRequirement(_Strict):
    """A requirement the user explicitly wants recorded that goes beyond
    like-for-like. Captured verbatim and flagged unsupported — never executed."""

    description: str
    kind: MigrationMode


class Questionnaire(_Strict):
    migration: MigrationDefinition
    requested_scope: RequestedScope
    success_criteria: list[str]
    constraints: list[str] = Field(default_factory=list)
    exclusions: list[str] = Field(default_factory=list)
    access: AccessConfig
    additional_requirements: list[AdditionalRequirement] = Field(default_factory=list)


# --- connectivity + readiness outcomes (computed) --------------------------

class ConnectivityOutcome(_Strict):
    """Result of a single connector's genuine auth/reachability check.

    Secret-free: ``detail`` is redacted of any credential before it is stored.
    """

    domain: AccessDomain
    status: ConnectivityStatus
    reachable: bool = False
    authenticated: bool = False
    detail: str = ""
    validated_at: Optional[datetime] = None


class AccessRequirement(_Strict):
    """Per-domain readiness state. ``required``/``provided``/``validated`` are
    kept distinct and never collapsed.

    ``config`` echoes the domain's non-secret connection metadata + credential
    *references* (env-var names) from the questionnaire so Discovery can consume
    the handoff directly. It never contains an actual secret value.
    """

    domain: AccessDomain
    required: bool
    mandatory: bool
    provided: bool
    validated: bool
    owner: Optional[str] = None
    config: dict[str, Any] = Field(default_factory=dict)
    connectivity: Optional[ConnectivityOutcome] = None


class Blocker(_Strict):
    requirement: str
    reason: str


class Readiness(_Strict):
    status: ReadinessStatus
    blockers: list[Blocker] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class UnsupportedRequirement(_Strict):
    description: str
    kind: str
    reason: str


class CapabilityFit(_Strict):
    """Deterministic classification of the *request* against current capability.

    Not an assessment of the discovered estate — that is the Assessment phase.
    """

    requested_mode: MigrationMode
    mode_supported: bool
    currently_supported: bool
    unsupported_requirements: list[UnsupportedRequirement] = Field(default_factory=list)


class Consultation(_Strict):
    """Persisted Consultation record and the Discovery handoff contract.

    Everything here is traceable to explicit questionnaire input or deterministic
    project metadata (see ``provenance``). No secrets are ever stored.
    """

    project_id: str
    source_commit_sha: str
    created_at: datetime
    updated_at: datetime
    migration: MigrationDefinition
    requested_scope: RequestedScope
    success_criteria: list[str]
    constraints: list[str] = Field(default_factory=list)
    exclusions: list[str] = Field(default_factory=list)
    capability_fit: CapabilityFit
    access: list[AccessRequirement]
    connectivity: list[ConnectivityOutcome]
    readiness: Readiness
    provenance: dict[str, str] = Field(default_factory=dict)
