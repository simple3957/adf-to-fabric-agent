"""ADF Live Discovery domain (behavior layer).

Phase 1 inventories the deployed factory's top-level artifacts — pipelines,
datasets, linked services, triggers, data flows, integration runtimes — via
read-only Azure Resource Manager child-collection GETs, recording name + kind
only. Orchestration lives in :mod:`.discoverer`; the ARM transport seam and the
fixed collection enumeration live in :mod:`.arm`; the persisted data contract
lives in ``project.discovery`` (project layer). This package holds behavior +
runtime request/result models only.
"""

from __future__ import annotations

from adf2fabric.agent.discovery.adf.discoverer import AdfDiscoverer
from adf2fabric.agent.discovery.adf.models import (
    AdfDiscoveryRequest,
    AdfDiscoveryResult,
)

__all__ = ["AdfDiscoverer", "AdfDiscoveryRequest", "AdfDiscoveryResult"]
