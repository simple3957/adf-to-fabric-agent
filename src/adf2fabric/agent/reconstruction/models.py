"""Runtime request/result models for the Reconstruction stage (no secrets, no file contents)."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.reconstruction import ProjectReconstruction


class ReconstructionRequest(BaseModel):
    project_name: str = Field(
        description="Existing project (Intake, Repository Discovery and ADF Semantic Discovery must have run).")


class ReconstructionResult(BaseModel):
    success: bool
    reconstruction: Optional[ProjectReconstruction] = None
    message: str = ""
    error: Optional[str] = None
