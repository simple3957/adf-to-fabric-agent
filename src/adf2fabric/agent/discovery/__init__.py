"""Repository Discovery — the first Discovery domain.

Produces a deterministic, read-only, factual inventory of the source repository
Intake acquired. Reports WHAT EXISTS; performs no migration interpretation.
"""

from __future__ import annotations

from adf2fabric.agent.discovery.agent import DiscoveryAgent
from adf2fabric.agent.discovery.models import DiscoveryRequest, DiscoveryResult

__all__ = ["DiscoveryAgent", "DiscoveryRequest", "DiscoveryResult"]
