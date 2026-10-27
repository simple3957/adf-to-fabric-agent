"""Runtime request/result models for the Conversion stage (no secrets, no file contents)."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.conversion import ProjectConversion


class ConversionRequest(BaseModel):
    project_name: str = Field(description="Existing project (Intake + Semantic Discovery must have run).")
    pipelines: list[str] = Field(
        default_factory=list,
        description="Convert only these pipelines (default: all). Child pipelines are referenced, not converted.")
    resolutions_path: Optional[str] = Field(
        default=None,
        description="JSON file mapping linked services / items to real Fabric ids (see docs/conversion.md).")
    compute_sources_path: Optional[str] = Field(
        default=None,
        description="JSON file describing external Spark compute that ADF only references (Databricks clusters by id, "
                    "Synapse Spark pools by name), so it can be sized. See docs/compute-plan.md.")
    assisted: bool = Field(
        default=False,
        description="Opt-in: convert the model-assisted drafts a person accepted (and that are still current) in place of the original source of "
                    "those pipelines. The converter itself stays deterministic; each such pipeline is marked as assisted.")
    lakehouse: Optional[str] = Field(
        default=None,
        description="Repoint storage datasets to this lakehouse through the OneLake shortcuts of the storage map (opt-in; "
                    "`storage-map` must have run). Bind its id with ItemToId 'lakehouse:<name>' and WorkspaceId.")
    apply_reconstruction: bool = Field(
        default=False,
        description="Convert the completed pipelines (original + SAFE reconstruction proposals) and mark every "
                    "inferred element. Off by default: inferred content never enters the output implicitly.")


class ConversionResult(BaseModel):
    success: bool
    conversion: Optional[ProjectConversion] = None
    message: str = ""
    error: Optional[str] = None
    problems: list[str] = Field(default_factory=list)  # e.g. unusable resolution entries
