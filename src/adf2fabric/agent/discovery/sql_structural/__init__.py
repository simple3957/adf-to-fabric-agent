"""SQL Server Discovery — Phase 2 domain (structural metadata, behavior layer).

Phase 2 extracts columns / data types / nullability / identity / primary keys /
foreign keys / relationships from read-only ``sys.*`` catalog metadata on a single
connection. Orchestration lives in :mod:`.discoverer`; the three fixed queries in
:mod:`.catalog`; the persisted contract in ``project.discovery`` (project layer).
It reuses the authoritative DSN builder and the Phase-1 pyodbc connect seam — no
second connection builder — and never mutates the Phase-1 ``sql.yaml`` contract.
"""

from __future__ import annotations

from adf2fabric.agent.discovery.sql_structural.discoverer import SqlStructuralDiscoverer
from adf2fabric.agent.discovery.sql_structural.models import (
    SqlStructuralDiscoveryRequest,
    SqlStructuralDiscoveryResult,
)

__all__ = [
    "SqlStructuralDiscoverer",
    "SqlStructuralDiscoveryRequest",
    "SqlStructuralDiscoveryResult",
]
