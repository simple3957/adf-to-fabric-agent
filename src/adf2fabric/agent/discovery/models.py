"""Input/output models for the Repository Discovery Agent.

Neither model carries a credential or any file content. The result references
the persisted :class:`RepositoryDiscovery` inventory, which is secret-free by
design.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.discovery import RepositoryDiscovery


class DiscoveryRequest(BaseModel):
    """What the user asks Repository Discovery to do."""

    project_name: str = Field(
        description="Existing project name (Intake must have run)."
    )


class DiscoveryResult(BaseModel):
    """The outcome of a Repository Discovery run. Contains no secrets."""

    success: bool
    discovery: Optional[RepositoryDiscovery] = None
    message: str = ""
    error: Optional[str] = None
