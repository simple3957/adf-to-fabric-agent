"""ADLS Gen2 Discovery domain (behavior layer).

Phase 1 enumerates the filesystems/containers in an explicitly named storage
account via the ADLS Gen2 data-plane SDK. Orchestration lives in
:mod:`.discoverer`; the persisted data contract lives in
``project.discovery`` (project layer). This package holds behavior + runtime
request/result models only.
"""

from __future__ import annotations

from adf2fabric.agent.discovery.adls.discoverer import AdlsDiscoverer
from adf2fabric.agent.discovery.adls.models import (
    AdlsDiscoveryRequest,
    AdlsDiscoveryResult,
)

__all__ = ["AdlsDiscoverer", "AdlsDiscoveryRequest", "AdlsDiscoveryResult"]
