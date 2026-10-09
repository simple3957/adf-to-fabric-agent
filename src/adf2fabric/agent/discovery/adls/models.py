"""Runtime input/output models for ADLS Gen2 Discovery.

These are runtime contracts only — the persisted source-of-truth contract is
``project.discovery.AdlsDiscovery``. Neither model carries a secret; the
credential is described only by an ``AzureCredentialRef`` (auth mechanism), and
the actual credential is resolved from the Azure CLI session at run time.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.discovery import AdlsDiscovery
from adf2fabric.project.models import AzureCredentialRef


def _default_credential() -> AzureCredentialRef:
    # Local development path: identity of the current `az login` session.
    return AzureCredentialRef(auth="azure_cli")


class AdlsDiscoveryRequest(BaseModel):
    """What the user asks ADLS Discovery to do."""

    project_name: str = Field(description="Existing project name (Intake must have run).")
    storage_account: str = Field(
        description="Explicit target storage account (e.g. 'adfstorage299'); never inferred."
    )
    credential: AzureCredentialRef = Field(default_factory=_default_credential)


class AdlsDiscoveryResult(BaseModel):
    """The outcome of an ADLS Discovery run. Contains no secrets."""

    success: bool
    discovery: Optional[AdlsDiscovery] = None
    message: str = ""
    error: Optional[str] = None
