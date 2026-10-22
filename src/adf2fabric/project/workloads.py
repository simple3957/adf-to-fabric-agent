"""Persisted Workload Migration Manifest (project/persistence layer).

One record per project (``workloads/manifest.yaml``) connecting every source Spark workload (a Databricks notebook,
Python file or JAR; a Synapse notebook or Spark job definition) to: the environment it ran in, the code that was (or
could not be) retrieved, what an analysis of that code found, how it classifies for Fabric, the Fabric artifact that was
generated, what that artifact still needs, and the evidence that it is valid.

Two things are kept apart on purpose:

* ``generation``: an artifact was written and passed STATIC checks (it parses, nothing Databricks-only is left, the
  parameters are declared). This says nothing about whether it runs.
* ``execution``: a Fabric run of the deployed artifact, recorded from the run itself. It stays ``not_run`` until then.

Security: no secret or source data. A finding stores a construct name and a line number, never the line (a line can hold
a literal credential). Source code lives in the generated artifact files, not in this record.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

WORKLOAD_SCHEMA_VERSION = "workload-manifest/1"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WorkloadKind(str, Enum):
    databricks_notebook = "databricks_notebook"
    databricks_python = "databricks_python"
    databricks_jar = "databricks_jar"
    synapse_notebook = "synapse_notebook"
    synapse_spark_job = "synapse_spark_job"


class Classification(str, Enum):
    directly_convertible = "directly_convertible"  # no code change is needed
    convertible_with_changes = "convertible_with_changes"  # code changes (automatic and/or manual) are needed
    requires_alternative = "requires_alternative"  # a construct needs a different Fabric approach
    blocked = "blocked"  # cannot be assessed or generated until an input is supplied (code, path, mapping)
    unsupported = "unsupported"  # Fabric has no equivalent


class CodeStatus(str, Enum):
    retrieved = "retrieved"  # fetched from the source with an authorised call during this run
    cached = "cached"  # an earlier authorised export already in the project
    in_project = "in_project"  # part of the Git/ZIP source that was imported (a Synapse notebook definition)
    supplied = "supplied"  # a local file the user pointed at
    not_requested = "not_requested"  # retrieval needs credentials and none were offered
    missing_credential = "missing_credential"
    denied = "denied"
    not_found = "not_found"
    unreachable = "unreachable"
    unsupported_location = "unsupported_location"  # a location this tool cannot read (for example a cloud store URI)
    dynamic = "dynamic"  # the path is computed at run time
    binary = "binary"  # compiled code (a JAR): there is no source to read
    incomplete_definition = "incomplete_definition"  # the source definition does not say what to run
    error = "error"


class Severity(str, Enum):
    info = "info"  # worth knowing; nothing to do
    review = "review"  # works as written if the stated resource/permission exists
    change = "change"  # the code or configuration must change
    alternative = "alternative"  # needs a different approach in Fabric
    blocker = "blocker"  # nothing can be generated until it is resolved


class FindingCategory(str, Enum):
    language = "language"
    runtime = "runtime"
    library = "library"
    spark_config = "spark_config"
    data_access = "data_access"
    authentication = "authentication"
    networking = "networking"
    external_dependency = "external_dependency"
    filesystem = "filesystem"
    api = "api"
    contract = "contract"  # parameters / outputs / error behaviour that downstream pipeline steps rely on
    source = "source"


class ExecutionEnvironment(_Strict):
    """Where the workload ran, resolved from the ADF linked service / Synapse definition and the compute inventory."""

    platform: str  # databricks | synapse
    source_ref: str = ""  # linked service name, or Synapse pool name
    workspace: Optional[str] = None  # Databricks workspace host
    mode: str = "unspecified"  # existing_cluster | new_cluster | instance_pool | spark_pool | unspecified
    runtime_version: Optional[str] = None  # as the source states it
    spark_version: Optional[str] = None
    node_type: Optional[str] = None
    max_workers: Optional[int] = None
    sizing_known: bool = False
    spark_conf: list[str] = Field(default_factory=list)  # NAMES of Spark settings the environment sets (values are not copied)
    libraries: list[str] = Field(default_factory=list)
    fabric_runtime: Optional[str] = None  # the Fabric runtime the plan suggests
    gaps: list[str] = Field(default_factory=list)  # what could not be resolved, and why


class CodeSource(_Strict):
    status: CodeStatus
    location: str = ""  # workspace path / dbfs path / project file: no credential
    language: Optional[str] = None
    sha256: Optional[str] = None
    lines: int = 0
    cells: int = 0
    permission: Optional[str] = None  # the permission category a failed retrieval needs
    detail: str = ""  # redacted
    action: str = ""


class Finding(_Strict):
    code: str
    category: FindingCategory
    severity: Severity
    message: str
    line: Optional[int] = None  # 1-based line in the source (cell-relative lines are made absolute)
    construct: str = ""  # the name that matched (dbutils.secrets.get, abfss://acct@..., import dlt): never a whole line
    rewritten: bool = False  # True: the generator changed the code for this
    action: str = ""  # what a person still has to do (empty when fully handled)


class Rewrite(_Strict):
    code: str
    count: int
    description: str


class LibraryResolution(_Strict):
    name: str  # as a pip requirement or a Maven coordinate
    origin: str  # activity | cluster | pip_magic | import | job_definition
    kind: str  # pypi | maven | jar | whl | egg | cran | stdlib | databricks_only | local_file
    decision: str  # environment_public_library | environment_custom_library | stdlib | not_available_in_fabric | needs_review
    note: str = ""


class ParameterContract(_Strict):
    """What the pipeline passes in and how the generated artifact receives it. Values are never copied."""

    name: str
    passed_by_pipeline: bool
    declared_in_code: bool = False
    has_default: bool = False
    mapped: bool = False  # True: the generated artifact reads it the way the pipeline now passes it
    note: str = ""


class OutputContract(_Strict):
    """A downstream pipeline step reads the result of this workload."""

    consumer: str  # <pipeline>/<activity>
    source_property: str  # what it reads today (output.runOutput)
    fabric_property: Optional[str] = None  # where the same value is in Fabric (when verified)
    action: str = ""


class GeneratedArtifact(_Strict):
    kind: str  # fabric_notebook | fabric_spark_job_definition | fabric_environment_libraries | none
    file: Optional[str] = None  # project-relative path of what was generated
    definition_file: Optional[str] = None  # the REST definition payload, for items that have one
    sha256: Optional[str] = None
    display_name: Optional[str] = None
    fabric_item_id: Optional[str] = None  # filled when deployed
    configuration: dict[str, str] = Field(default_factory=dict)  # lakehouse, environment, pool ... NAMES / ids, no secrets


class RequiredResource(_Strict):
    kind: str  # lakehouse | environment | spark_pool | connection | shortcut | key_vault | identity_access | file_upload | notebook
    name: str
    reason: str
    status: str = "needed"  # needed | provided | unknown


class StaticCheck(_Strict):
    name: str
    passed: bool
    detail: str = ""


class GenerationEvidence(_Strict):
    status: str = "not_generated"  # generated | generated_with_open_issues | not_generated
    checks: list[StaticCheck] = Field(default_factory=list)
    reason: str = ""  # why nothing was generated, or what is still open
    generated_at: Optional[datetime] = None


class ExecutionEvidence(_Strict):
    """Proof from a Fabric run. Only this block may say a workload ran in Fabric."""

    status: str = "not_run"  # not_run | succeeded | failed
    fabric_item_id: Optional[str] = None
    run_id: Optional[str] = None
    finished_at: Optional[datetime] = None
    detail: str = ""


class Approval(_Strict):
    status: str = "pending"  # pending | approved | rejected
    by: Optional[str] = None
    at: Optional[datetime] = None
    note: str = ""


class AlternativeOption(_Strict):
    summary: str
    limitations: list[str] = Field(default_factory=list)


class WorkloadEntry(_Strict):
    id: str  # "<kind>:<identity>", stable
    kind: WorkloadKind
    name: str  # display name
    source_item: str  # notebook path / python file / main class / Synapse item name
    used_by: list[str] = Field(default_factory=list)  # <pipeline>/<activity>
    environment: ExecutionEnvironment
    code: CodeSource
    classification: Classification
    classification_reasons: list[str] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)
    rewrites: list[Rewrite] = Field(default_factory=list)
    libraries: list[LibraryResolution] = Field(default_factory=list)
    parameters: list[ParameterContract] = Field(default_factory=list)
    outputs: list[OutputContract] = Field(default_factory=list)
    artifact: GeneratedArtifact = Field(default_factory=lambda: GeneratedArtifact(kind="none"))
    required_resources: list[RequiredResource] = Field(default_factory=list)
    unresolved: list[str] = Field(default_factory=list)  # open issues, each one a sentence a person can act on
    alternatives: list[AlternativeOption] = Field(default_factory=list)
    generation: GenerationEvidence = Field(default_factory=GenerationEvidence)
    execution: ExecutionEvidence = Field(default_factory=ExecutionEvidence)
    approval: Approval = Field(default_factory=Approval)


class WorkloadSummary(_Strict):
    total: int = 0
    by_classification: dict[str, int] = Field(default_factory=dict)
    by_kind: dict[str, int] = Field(default_factory=dict)
    generated: int = 0
    generated_with_open_issues: int = 0
    not_generated: int = 0
    executed_in_fabric: int = 0  # entries whose execution evidence is "succeeded"
    approved: int = 0


class WorkloadManifest(_Strict):
    schema_version: str = WORKLOAD_SCHEMA_VERSION
    project_id: str
    source_commit_sha: str
    created_at: datetime
    updated_at: datetime
    summary: WorkloadSummary = Field(default_factory=WorkloadSummary)
    workloads: list[WorkloadEntry] = Field(default_factory=list)  # sorted by id
    notes: list[str] = Field(default_factory=list)
