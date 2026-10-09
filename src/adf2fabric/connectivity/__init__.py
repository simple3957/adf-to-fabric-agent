"""Connectivity layer for the Consultation readiness gate.

Injectable, extensible connectors that perform genuine — but narrow —
auth/reachability/permission validation for each access domain of the current
engagement (Repository, ADF, SQL Server, ADLS Gen2, Fabric). Connectors never
perform discovery: no enumeration, no reads, no schema/artifact inspection.

- ``base``     - ``Connector`` protocol + ``ConnectivityOutcome`` contract
- ``registry`` - capability registration + lookup (the extension point)
- one module per domain connector
"""

from adf2fabric.connectivity.base import (
    AccessDomain,
    ConnectivityOutcome,
    ConnectivityStatus,
    Connector,
)

__all__ = [
    "AccessDomain",
    "ConnectivityOutcome",
    "ConnectivityStatus",
    "Connector",
]
