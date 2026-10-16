"""Input/output models for the Intake Agent.

Neither model carries a credential. The token is supplied to the agent
out-of-band (from the environment) and never round-trips through these.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.models import AzureCredentialRef, Project
from adf2fabric.project.source import SourceManifest


class IntakeRequest(BaseModel):
    """What the user asks Intake to do."""

    project_name: str = Field(description="Human-readable name for the engagement.")
    repository_url: str = Field(description="Source repository URL.")
    branch: Optional[str] = Field(
        default=None, description="Branch to fetch. Empty means the repository's own default branch."
    )
    provider: str = Field(default="github", description="Source provider.")
    refresh: bool = Field(
        default=False,
        description="Force a fresh fetch even if a matching local source already exists.",
    )
    subdirectory: Optional[str] = Field(
        default=None,
        description="Repository subdirectory holding the ADF folders (source root).",
    )
    pipelines: list[str] = Field(
        default_factory=list,
        description="Optional pipeline selection; empty means every pipeline.",
    )


class AzureIntakeRequest(BaseModel):
    """Retrieve ADF JSON from a live factory. Carries no secret."""

    project_name: str = Field(description="Human-readable name for the engagement.")
    resource_group: str
    factory_name: str
    subscription_id: Optional[str] = Field(
        default=None,
        description="Defaults to the active Azure CLI subscription when omitted.",
    )
    pipelines: list[str] = Field(
        default_factory=list,
        description="Optional pipeline selection; its referenced artifacts come along.",
    )
    artifact_kinds: list[str] = Field(
        default_factory=list,
        description="Optional restriction of artifact kinds; empty means all kinds.",
    )
    credential: AzureCredentialRef = Field(
        default_factory=lambda: AzureCredentialRef(auth="azure_cli"),
        description="Credential REFERENCE (auth mechanism + env-var names).",
    )
    allow_partial: bool = Field(
        default=False,
        description="Persist the source even if some artifact retrievals failed.",
    )


class IntakeResult(BaseModel):
    """The outcome of an intake run. Contains no secrets."""

    success: bool
    project: Optional[Project] = None
    reused: bool = Field(
        default=False,
        description="True when an existing local source was reused instead of re-cloned.",
    )
    manifest: Optional[SourceManifest] = Field(
        default=None, description="How the source was obtained (secret-free)."
    )
    message: str = ""
    error: Optional[str] = None
