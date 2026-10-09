"""Runtime input/output models for ADF Semantic Discovery.

Runtime contracts only — the persisted source-of-truth is
``project.discovery.AdfSemanticDiscovery``. No secrets: the request names a
project; the phase reads only the immutable repository source.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.discovery import AdfSemanticDiscovery


class AdfSemanticDiscoveryRequest(BaseModel):
    """What the user asks ADF Semantic Discovery to do."""

    project_name: str = Field(
        description="Existing project name (Intake + Repository Discovery must have run)."
    )


class AdfSemanticDiscoveryResult(BaseModel):
    """The outcome of an ADF Semantic Discovery run. Contains no secrets."""

    success: bool
    discovery: Optional[AdfSemanticDiscovery] = None
    message: str = ""
    error: Optional[str] = None
