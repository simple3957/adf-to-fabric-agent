"""Runtime input/output models for SQL Server Discovery.

These are runtime contracts only — the persisted source-of-truth contract is
``project.discovery.SqlDiscovery``. Neither model carries a secret: the request
carries a :class:`SqlServerAccess`, whose credential is described only by a
reference (auth mode + the NAME of a password env var), and the password value
is resolved from the environment at run time and never stored on the model.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.discovery import SqlDiscovery
from adf2fabric.project.models import SqlServerAccess


class SqlDiscoveryRequest(BaseModel):
    """What the user asks SQL Discovery to do."""

    project_name: str = Field(
        description="Existing project name (Intake must have run)."
    )
    access: SqlServerAccess = Field(
        description="Explicit SQL target (server/database/auth/TLS); never inferred."
    )


class SqlDiscoveryResult(BaseModel):
    """The outcome of a SQL Discovery run. Contains no secrets."""

    success: bool
    discovery: Optional[SqlDiscovery] = None
    message: str = ""
    error: Optional[str] = None
