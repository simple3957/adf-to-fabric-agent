"""Input/output models for the Intake Agent.

Neither model carries a credential. The token is supplied to the agent
out-of-band (from the environment) and never round-trips through these.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.models import Project


class IntakeRequest(BaseModel):
    """What the user asks Intake to do."""

    project_name: str = Field(description="Human-readable name for the engagement.")
    repository_url: str = Field(description="Source repository URL.")
    branch: str = Field(default="main", description="Branch to fetch.")
    provider: str = Field(default="github", description="Source provider.")
    refresh: bool = Field(
        default=False,
        description="Force a fresh fetch even if a matching local source already exists.",
    )


class IntakeResult(BaseModel):
    """The outcome of an intake run. Contains no secrets."""

    success: bool
    project: Optional[Project] = None
    reused: bool = Field(
        default=False,
        description="True when an existing local source was reused instead of re-cloned.",
    )
    message: str = ""
    error: Optional[str] = None
