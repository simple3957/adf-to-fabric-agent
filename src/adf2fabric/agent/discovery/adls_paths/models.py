"""Runtime input/output models for ADLS Gen2 Discovery Phase 2 (path inventory).

Runtime contracts only — the persisted source-of-truth is
``project.discovery.AdlsPathsDiscovery``. No secrets: the credential is described
only by an ``AzureCredentialRef`` and resolved from the Azure CLI session at run
time. The storage account + filesystem list come from the Phase-1 record.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.discovery import AdlsPathsDiscovery
from adf2fabric.project.models import AzureCredentialRef


def _default_credential() -> AzureCredentialRef:
    # Local development path: identity of the current `az login` session.
    return AzureCredentialRef(auth="azure_cli")


class AdlsPathsDiscoveryRequest(BaseModel):
    """What the user asks ADLS Phase-2 discovery to do."""

    project_name: str = Field(
        description="Existing project name (ADLS Phase-1 discovery must have run)."
    )
    credential: AzureCredentialRef = Field(default_factory=_default_credential)


class AdlsPathsDiscoveryResult(BaseModel):
    """The outcome of an ADLS Phase-2 discovery run. Contains no secrets."""

    success: bool
    discovery: Optional[AdlsPathsDiscovery] = None
    message: str = ""
    error: Optional[str] = None
