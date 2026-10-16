"""The Spark compute plan: what external compute the ADF estate uses, and the Fabric compute that would replace it.

Read-only analysis. Nothing here creates anything: each pool proposal carries the exact request body the documented
Fabric ``Create Workspace Custom Pool`` API would take, so a later, explicit apply step can send it unchanged.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ComputeUse(_Strict):
    """One distinct external Spark compute the estate runs on, with the activities that use it."""

    id: str  # stable slug, e.g. databricks:LS_DBX:new:Standard_DS3_v2:2-8
    platform: str  # databricks | synapse
    source_ref: str  # linked service name, or Synapse Spark pool name
    mode: str  # new_cluster | existing_cluster | instance_pool | spark_pool | session_override | unspecified
    sizing_known: bool
    sizing_source: str  # adf_linked_service | activity_override | compute_sources_file | not_available
    node_type: Optional[str] = None
    driver_node_type: Optional[str] = None
    vcores_per_node: Optional[int] = None
    driver_vcores: Optional[int] = None
    min_workers: Optional[int] = None
    max_workers: Optional[int] = None
    autoscale: bool = False
    source_peak_vcores: Optional[int] = None  # driver + max workers
    runtime_version: Optional[str] = None  # as the source states it
    spark_version: Optional[str] = None  # the Apache Spark version that runtime carries, when known
    spark_conf: dict[str, str] = Field(default_factory=dict)
    libraries: list[str] = Field(default_factory=list)
    init_script_count: int = 0
    gpu: bool = False
    photon: bool = False
    activities: list[str] = Field(default_factory=list)  # <pipeline>/<activity>
    pipelines: list[str] = Field(default_factory=list)
    triggers: list[str] = Field(default_factory=list)  # triggers that start those pipelines
    notes: list[str] = Field(default_factory=list)


class FabricPoolProposal(_Strict):
    """A Fabric custom Spark pool that matches one or more source computes."""

    name: str
    status: str  # proposed | needs_input | not_possible
    node_family: str = "MemoryOptimized"
    node_size: Optional[str] = None  # the API enum: Small | Medium | Large | XLarge | XXLarge
    vcores_per_node: Optional[int] = None
    min_nodes: Optional[int] = None
    max_nodes: Optional[int] = None
    peak_spark_vcores: Optional[int] = None
    peak_capacity_units: Optional[float] = None  # peak vCores / 2, before any burst
    replaces: list[str] = Field(default_factory=list)  # ComputeUse ids
    request_body: Optional[dict[str, Any]] = None  # POST /v1/workspaces/{workspaceId}/spark/pools
    fabric_id: Optional[str] = None  # set after `compute-deploy --apply` created or reused it
    rationale: list[str] = Field(default_factory=list)


class FabricEnvironmentProposal(_Strict):
    """The Fabric Environment (runtime, Spark properties, libraries) that goes with a pool."""

    name: str
    runtime_version: str  # Fabric runtime, e.g. 2.0
    runtime_spark_version: str
    pool: Optional[str] = None
    fabric_id: Optional[str] = None  # set after `compute-deploy --apply` created or found it
    spark_properties: dict[str, str] = Field(default_factory=dict)
    libraries: list[str] = Field(default_factory=list)
    replaces: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class ComputeGap(_Strict):
    """Something the tool cannot decide or carry over; a person has to."""

    code: str
    compute_id: str
    message: str
    action: str


class ActivityTarget(_Strict):
    """Where one Spark activity would run in Fabric, and what is still needed to get it there.

    ``conversion_today`` is what ``convert`` does with the activity right now; the target is the plan for moving the
    compute, which is opt-in and not applied by the pipeline conversion.
    """

    pipeline: str
    activity: str
    activity_type: str
    source_compute_id: str
    source_item: Optional[str] = None  # notebook path / notebook name / Spark job name
    parameters: list[str] = Field(default_factory=list)  # parameter names the activity passes (values are not copied)
    conversion_today: str  # reuse_externally | replace (scaffold) | redesign
    target_execution: str  # FabricNotebook | FabricSparkJobDefinition
    spark_pool: Optional[str] = None
    environment: Optional[str] = None
    requires_notebook_migration: bool = False
    requires_job_definition: bool = False
    requires_compatibility_review: bool = True
    review_reasons: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    provisioning_status: str = "pending_approval"  # pending_approval | needs_input | not_possible | exists | created


class ComputePlan(_Strict):
    targets: list[ActivityTarget] = Field(default_factory=list)
    inventory: list[ComputeUse] = Field(default_factory=list)
    pools: list[FabricPoolProposal] = Field(default_factory=list)
    environments: list[FabricEnvironmentProposal] = Field(default_factory=list)
    gaps: list[ComputeGap] = Field(default_factory=list)
    total_peak_spark_vcores: int = 0  # sum over the proposed pools (an upper bound: pools rarely peak together)
    max_single_pool_vcores: int = 0  # the largest pool: the least capacity that can run any one of them at peak
    sources_used: list[str] = Field(default_factory=list)
    sources_problems: list[str] = Field(default_factory=list)
    basis: list[str] = Field(default_factory=list)  # the rules and documents the proposals rest on
