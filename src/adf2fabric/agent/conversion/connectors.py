"""Which ADF linked-service types have a documented Fabric counterpart.

Read from the master compatibility catalog (``adf2fabric/catalog/compatibility.json``), which was derived from Microsoft's Fabric
Data Factory documentation (the data-store tables of the
Lookup, Script and Stored procedure activity articles, and the ADF-vs-Fabric comparison), and
checked on 2026-10-16 against two more pages and Fabric's own list of connection types:

* ADF connector overview:   https://learn.microsoft.com/en-us/azure/data-factory/connector-overview
* ADF vs Fabric connectors: https://learn.microsoft.com/en-us/fabric/data-factory/connector-parity (dated 2026-07-08)
* the ADF linked-service ``type`` strings: the Microsoft.DataFactory/factories/linkedservices template reference
* Fabric ``GET /v1/connections/supportedConnectionTypes`` (287 types on that date)

The verdicts are deliberately three-valued — a connector the documentation does not mention is
"unlisted", not "unsupported": absence from this table is a reason to VERIFY, not to refuse.
"""

from __future__ import annotations

from typing import Optional

from adf2fabric.catalog import master

_LS = master()["connectors"]["adf_linked_services"]  # the master compatibility catalog (adf2fabric/catalog/compatibility.json)

# ADF linked-service ``type`` (lowercase) -> Fabric connector name, for connectors the Fabric documentation lists as data stores for
# pipeline activities.
SUPPORTED: dict[str, str] = {k.lower(): v["fabric_connector"] for k, v in _LS.items() if v["verdict"] == "supported"}
# Not available in Fabric pipelines per Microsoft's ADF-vs-Fabric connector comparison: "unlisted" with that reason, not "unsupported".
NOT_IN_PIPELINES: dict[str, str] = {k.lower(): v["reason"] for k, v in _LS.items() if v["verdict"] == "not_in_pipelines"}
# Known to have no Fabric counterpart (retired or not offered).
UNSUPPORTED: dict[str, str] = {k.lower(): v["reason"] for k, v in _LS.items() if v["verdict"] == "unsupported"}
# Stores the Stored procedure and Script activities support (activity articles).
STORED_PROCEDURE_STORES = frozenset(k.lower() for k, v in _LS.items() if v.get("stored_procedure"))
SCRIPT_STORES = frozenset(k.lower() for k, v in _LS.items() if v.get("script"))


def verdict(ls_type: Optional[str]) -> tuple[str, str]:
    """('supported'|'unlisted'|'unsupported', explanation)."""
    key = (ls_type or "").lower()
    if not key:
        return "unlisted", "the linked service type is unknown"
    if key in UNSUPPORTED:
        return "unsupported", UNSUPPORTED[key]
    if key in SUPPORTED:
        return "supported", SUPPORTED[key]
    if key in NOT_IN_PIPELINES:
        return "unlisted", NOT_IN_PIPELINES[key]
    return "unlisted", f"'{ls_type}' is not named in the Fabric connector documentation consulted"


# Levels, best first. ``created``: this tool can create the Fabric connection. ``manual``: Fabric has a connector, but the
# connection is created by hand. ``unsupported``: no Fabric connector (retired source). ``unlisted``: not named in the Fabric
# connector documentation consulted, so check it; it is not a refusal.
LEVELS = ("created", "manual", "unsupported", "unlisted")


def coverage(linked_services: list) -> list:
    """One :class:`ConnectorCoverage` per linked service, from its ADF type. Sorted by name."""
    from adf2fabric.connections.catalog import connector_for
    from adf2fabric.project.conversion import ConnectorCoverage

    out = []
    for ls in sorted(linked_services, key=lambda x: x.name.lower()):
        kind, why = verdict(ls.type)
        if connector_for((ls.type or "").lower()) is not None:
            level, note = "created", "the tool can create this Fabric connection"
        elif kind == "supported":
            level, note = "manual", "Fabric has a connector for it; create the connection in Fabric and bind it"
        elif (ls.type or "").lower() == "azuresynapseartifacts":
            level, note = "unlisted", ("used by Synapse notebook and Spark job activities, which become Fabric items, so no "
                                       "connection is created for it")
        else:
            level, note = kind, why
        out.append(ConnectorCoverage(linked_service=ls.name, adf_type=ls.type, level=level, note=note))
    return out
