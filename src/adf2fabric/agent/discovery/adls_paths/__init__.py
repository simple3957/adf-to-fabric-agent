"""ADLS Gen2 Discovery — Phase 2 domain (directory / path / file inventory).

Phase 2 enumerates the directories and files inside the filesystem(s) that Phase 1
discovered, via the ADLS Gen2 data-plane path listing (fully paginated), recording
safe structural metadata only. Orchestration lives in :mod:`.discoverer`; the SDK
seam + path enumeration in :mod:`.paths`; the persisted contract in
``project.discovery`` (project layer). It reuses the shared Azure credential
abstraction and never mutates the Phase-1 ``adls.yaml`` contract.
"""

from __future__ import annotations

from adf2fabric.agent.discovery.adls_paths.discoverer import AdlsPathsDiscoverer
from adf2fabric.agent.discovery.adls_paths.models import (
    AdlsPathsDiscoveryRequest,
    AdlsPathsDiscoveryResult,
)

__all__ = [
    "AdlsPathsDiscoverer",
    "AdlsPathsDiscoveryRequest",
    "AdlsPathsDiscoveryResult",
]
