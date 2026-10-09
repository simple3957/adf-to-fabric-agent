"""SQL Server Discovery domain (behavior layer).

Phase 1 inventories the structural database estate — schemas, tables, views,
and stored procedures — via read-only SQL catalog metadata queries on a single
connection. Orchestration lives in :mod:`.discoverer`; the fixed catalog queries
and the pyodbc connection seam live in :mod:`.catalog`; the persisted data
contract lives in ``project.discovery`` (project layer). This package holds
behavior + runtime request/result models only.
"""

from __future__ import annotations

from adf2fabric.agent.discovery.sql.discoverer import SqlDiscoverer
from adf2fabric.agent.discovery.sql.models import (
    SqlDiscoveryRequest,
    SqlDiscoveryResult,
)

__all__ = ["SqlDiscoverer", "SqlDiscoveryRequest", "SqlDiscoveryResult"]
