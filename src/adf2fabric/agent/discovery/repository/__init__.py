"""Repository Discovery domain (behavior layer).

Read-only, deterministic inspection of the source working tree:

    RepositoryDiscoverer  -> domain orchestration (invoked by DiscoveryAgent)
    scan_repository()     -> RepositoryStructure + file inventory
    extract_candidates()  -> ADF artifact candidates (structural classification)
    hashing.sha256_file() -> streamed content hashing

The phase-level orchestrator lives in ``agent/discovery/agent.py``; the persisted
data contract lives in ``project/discovery.py``. This package holds behavior only.
"""

from __future__ import annotations

from adf2fabric.agent.discovery.repository.artifacts import extract_candidates
from adf2fabric.agent.discovery.repository.scanner import scan_repository
from adf2fabric.agent.discovery.repository.discoverer import RepositoryDiscoverer

__all__ = ["RepositoryDiscoverer", "scan_repository", "extract_candidates"]
