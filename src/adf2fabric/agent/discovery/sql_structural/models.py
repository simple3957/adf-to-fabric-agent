"""Runtime input/output models for SQL Server Discovery Phase 2 (structural).

Runtime contracts only — the persisted source-of-truth is
``project.discovery.SqlStructuralDiscovery``. No secrets: the request carries a
:class:`SqlServerAccess` whose credential is a reference only; any password is
resolved from the named env var at run time and never stored.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.discovery import SqlStructuralDiscovery
from adf2fabric.project.models import SqlServerAccess


class SqlStructuralDiscoveryRequest(BaseModel):
    """What the user asks SQL structural discovery to do."""

    project_name: str = Field(
        description="Existing project name (Intake must have run)."
    )
    access: SqlServerAccess = Field(
        description="Explicit SQL target (server/database/auth/TLS); never inferred."
    )


class SqlStructuralDiscoveryResult(BaseModel):
    """The outcome of a SQL structural discovery run. Contains no secrets."""

    success: bool
    discovery: Optional[SqlStructuralDiscovery] = None
    message: str = ""
    error: Optional[str] = None
