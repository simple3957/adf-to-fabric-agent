"""The Discovery PHASE orchestrator.

``DiscoveryAgent`` is the phase-level boundary. It owns NO discovery behavior —
no filesystem traversal, JSON inspection, hashing, artifact extraction, or Azure/
SQL SDK calls. It simply wires and delegates to the domain discoverers that
actually exist:

    DiscoveryAgent
        ├── RepositoryDiscoverer   (repository domain)
        ├── AdlsDiscoverer         (ADLS Gen2 domain)
        ├── SqlDiscoverer          (SQL Server domain)
        ├── AdfDiscoverer          (ADF live domain)
        └── AdfSemanticDiscoverer  (ADF semantic / deep, repository-based)

The repository/ADLS/SQL/ADF CLI commands remain independent operations; each
routes through the matching delegation method below.
"""

from __future__ import annotations

from typing import Optional

from adf2fabric.agent.discovery.adf.discoverer import AdfDiscoverer
from adf2fabric.agent.discovery.adf.models import (
    AdfDiscoveryRequest,
    AdfDiscoveryResult,
)
from adf2fabric.agent.discovery.adf_semantic.discoverer import AdfSemanticDiscoverer
from adf2fabric.agent.discovery.adf_semantic.models import (
    AdfSemanticDiscoveryRequest,
    AdfSemanticDiscoveryResult,
)
from adf2fabric.agent.discovery.adls.discoverer import AdlsDiscoverer
from adf2fabric.agent.discovery.adls.models import (
    AdlsDiscoveryRequest,
    AdlsDiscoveryResult,
)
from adf2fabric.agent.discovery.adls_paths.discoverer import AdlsPathsDiscoverer
from adf2fabric.agent.discovery.adls_paths.models import (
    AdlsPathsDiscoveryRequest,
    AdlsPathsDiscoveryResult,
)
from adf2fabric.agent.discovery.models import DiscoveryRequest, DiscoveryResult
from adf2fabric.agent.discovery.repository.discoverer import RepositoryDiscoverer
from adf2fabric.agent.discovery.sql.discoverer import SqlDiscoverer
from adf2fabric.agent.discovery.sql.models import (
    SqlDiscoveryRequest,
    SqlDiscoveryResult,
)
from adf2fabric.agent.discovery.sql_structural.discoverer import SqlStructuralDiscoverer
from adf2fabric.agent.discovery.sql_structural.models import (
    SqlStructuralDiscoveryRequest,
    SqlStructuralDiscoveryResult,
)
from adf2fabric.project.store import ProjectStore


class DiscoveryAgent:
    """Phase-level orchestration: delegate to the currently implemented domains."""

    def __init__(
        self,
        store: ProjectStore,
        repository_discoverer: Optional[RepositoryDiscoverer] = None,
        adls_discoverer: Optional[AdlsDiscoverer] = None,
        sql_discoverer: Optional[SqlDiscoverer] = None,
        adf_discoverer: Optional[AdfDiscoverer] = None,
        adf_semantic_discoverer: Optional[AdfSemanticDiscoverer] = None,
        sql_structural_discoverer: Optional[SqlStructuralDiscoverer] = None,
        adls_paths_discoverer: Optional[AdlsPathsDiscoverer] = None,
    ):
        self.store = store
        # Domain discoverers are injectable for testing; production builds defaults.
        self._repository = repository_discoverer or RepositoryDiscoverer(store)
        self._adls = adls_discoverer or AdlsDiscoverer(store)
        self._sql = sql_discoverer or SqlDiscoverer(store)
        self._adf = adf_discoverer or AdfDiscoverer(store)
        self._adf_semantic = adf_semantic_discoverer or AdfSemanticDiscoverer(store)
        self._sql_structural = sql_structural_discoverer or SqlStructuralDiscoverer(store)
        self._adls_paths = adls_paths_discoverer or AdlsPathsDiscoverer(store)

    def discover_repository(self, request: DiscoveryRequest) -> DiscoveryResult:
        return self._repository.run(request)

    def discover_adls(self, request: AdlsDiscoveryRequest) -> AdlsDiscoveryResult:
        return self._adls.run(request)

    def discover_sql(self, request: SqlDiscoveryRequest) -> SqlDiscoveryResult:
        return self._sql.run(request)

    def discover_adf(self, request: AdfDiscoveryRequest) -> AdfDiscoveryResult:
        return self._adf.run(request)

    def discover_adf_semantic(
        self, request: AdfSemanticDiscoveryRequest
    ) -> AdfSemanticDiscoveryResult:
        return self._adf_semantic.run(request)

    def discover_sql_structural(
        self, request: SqlStructuralDiscoveryRequest
    ) -> SqlStructuralDiscoveryResult:
        return self._sql_structural.run(request)

    def discover_adls_paths(
        self, request: AdlsPathsDiscoveryRequest
    ) -> AdlsPathsDiscoveryResult:
        return self._adls_paths.run(request)
