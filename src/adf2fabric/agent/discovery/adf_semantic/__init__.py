"""ADF Semantic / Deep Discovery domain (behavior layer).

Phase 1 extracts the structural definition of ADF artifacts and the resources
they reference from the IMMUTABLE repository source — pipelines + nested
activities + ordering + references, dataset asset identity + declared columns,
linked-service SAFE metadata, triggers, factory global parameters, safe external
references, and typed relationship edges. Orchestration lives in :mod:`.discoverer`;
secret-safe extraction in :mod:`.parser`; reference/relationship derivation in
:mod:`.references`; the persisted contract in ``project.discovery`` (project
layer). This package holds behavior + runtime request/result models only.
"""

from __future__ import annotations

from adf2fabric.agent.discovery.adf_semantic.discoverer import AdfSemanticDiscoverer
from adf2fabric.agent.discovery.adf_semantic.models import (
    AdfSemanticDiscoveryRequest,
    AdfSemanticDiscoveryResult,
)

__all__ = [
    "AdfSemanticDiscoverer",
    "AdfSemanticDiscoveryRequest",
    "AdfSemanticDiscoveryResult",
]
