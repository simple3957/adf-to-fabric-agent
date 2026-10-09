"""Runtime input/output models for ADF Live Discovery.

These are runtime contracts only — the persisted source-of-truth contract is
``project.discovery.AdfDiscovery``. Neither model carries a secret: the request
carries an :class:`AdfAccess`, whose credential is described only by an
``AzureCredentialRef`` (auth mechanism + env-var NAMES), and the actual token is
resolved from the credential abstraction at run time and never stored.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.discovery import AdfDiscovery
from adf2fabric.project.models import AdfAccess


class AdfDiscoveryRequest(BaseModel):
    """What the user asks ADF Live Discovery to do."""

    project_name: str = Field(
        description="Existing project name (Intake must have run)."
    )
    access: AdfAccess = Field(
        description="Explicit ADF target (subscription/resource group/factory + "
        "credential reference); never inferred."
    )


class AdfDiscoveryResult(BaseModel):
    """The outcome of an ADF Live Discovery run. Contains no secrets."""

    success: bool
    discovery: Optional[AdfDiscovery] = None
    message: str = ""
    error: Optional[str] = None
