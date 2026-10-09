"""Connector registry — the extension point for connectivity capabilities.

Registers the five connectors of the current engagement (Repository, ADF, SQL
Server, ADLS Gen2, Fabric). Future ETL tools, warehouses, storage technologies,
source/target platforms, and connectivity providers register here without any
change to the readiness engine or the Consultation agent.
"""

from __future__ import annotations

from typing import Callable

from adf2fabric.connectivity.adls_gen2 import AdlsGen2Connector
from adf2fabric.connectivity.azure_data_factory import AdfConnector
from adf2fabric.connectivity.base import Connector
from adf2fabric.connectivity.fabric import FabricConnector
from adf2fabric.connectivity.repository import RepositoryConnector
from adf2fabric.connectivity.sql_server import SqlServerConnector
from adf2fabric.project.models import AccessDomain

_REGISTRY: dict[str, Callable[[], Connector]] = {}


def register(domain: str, factory: Callable[[], Connector]) -> None:
    """Register a connector factory for ``domain`` (overwrites any existing)."""
    _REGISTRY[domain] = factory


def get_connector(domain: str) -> Connector:
    """Return a connector instance for ``domain`` from the registry."""
    try:
        return _REGISTRY[domain]()
    except KeyError:
        raise KeyError(
            f"No connector registered for domain '{domain}'. "
            f"Registered: {sorted(_REGISTRY)}."
        ) from None


def registered_domains() -> list[str]:
    return sorted(_REGISTRY)


# --- current engagement: five connectors -----------------------------------
register(AccessDomain.repository.value, RepositoryConnector)
register(AccessDomain.adf.value, AdfConnector)
register(AccessDomain.sql_server.value, SqlServerConnector)
register(AccessDomain.adls_gen2.value, AdlsGen2Connector)
register(AccessDomain.fabric.value, FabricConnector)
