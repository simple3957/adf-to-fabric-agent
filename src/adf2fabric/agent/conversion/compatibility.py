"""Compatibility catalog: components that are retired, deprecated, in preview, or missing in Fabric.

The converter's per-type handlers know *how* to convert; this catalog knows *what Microsoft says about the
component's lifecycle*, with the source page and the date it was verified, so a retired connector or a deprecated
activity is reported with its date and a concrete way forward instead of passing as "unverified".

Rules that keep it honest:

* Every entry cites a Microsoft page (``source_url``) and ``VERIFIED_ON``. Nothing here is from memory: when a
  page changes, update the entry and the date together (``tests/test_compatibility.py`` enforces the shape).
* Absence from the catalog means "not known to be a problem", never "supported".
* A linked service whose connector exists in several versions is matched on ``typeProperties.version``. When the
  version is not stated, the finding says so; it does not assume V1.
* Entries with ``emit=False`` are components whose handlers already block or redesign them; they are listed in the
  report's compatibility section but do not produce a second issue on the activity.

Each entry id must be documented in ``skills/conversion/compatibility-playbook.md`` (also enforced by a test).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.project.conversion import CompatibilityFinding, Impact
from adf2fabric.project.discovery import AdfSemanticDiscovery

VERIFIED_ON = "2026-10-13"

COMPARE_URL = ("https://learn.microsoft.com/en-us/fabric/data-factory/"
               "compare-fabric-data-factory-and-azure-data-factory")
CONNECTORS_URL = ("https://learn.microsoft.com/en-us/azure/data-factory/"
                  "connector-release-stages-and-timelines")
MDF_URL = "https://learn.microsoft.com/en-us/fabric/data-factory/dataflow-gen2-mapping-data-flows-transforms"

REMOVED, DEPRECATED, PREVIEW, NOT_AVAILABLE = "removed", "deprecated", "preview", "not_available"
REPLACED = "replaced"  # exists in Fabric, but as a different kind of item


@dataclass(frozen=True)
class CompatEntry:
    id: str
    kind: str  # activity | linked_service | artifact
    types: tuple[str, ...]  # ADF type names, lower case
    status: str
    summary: str
    impact: Impact
    alternatives: tuple[str, ...]
    source_url: str
    date: Optional[str] = None  # ISO removal / end-of-support date, when the page gives one
    versions: Optional[tuple[str, ...]] = None  # linked services: affected versions; "" = not stated; None = any
    emit: bool = True  # also raise an issue on the using activity


# --- activities (Fabric activity comparison) -----------------------------------------------------------------

_ACTIVITIES: tuple[CompatEntry, ...] = (
    CompatEntry(
        id="activity.aml_classic", kind="activity", types=("azuremlbatchexecution", "azuremlupdateresource"),
        status=DEPRECATED, impact=Impact.blocker, emit=False, source_url=COMPARE_URL,
        summary="Azure Machine Learning Batch Execution and Update Resource are deprecated in ADF and not available "
                "in Fabric Data Factory.",
        alternatives=("Publish the model as an Azure Machine Learning pipeline or batch endpoint and call it with the "
                      "Fabric Azure Machine Learning activity.",)),
    CompatEntry(
        id="activity.wrangling_dataflow", kind="activity", types=("executewranglingdataflow",),
        status=DEPRECATED, impact=Impact.difference, source_url=COMPARE_URL,
        summary="Power Query / Wrangling Data Flow is deprecated in ADF. Fabric has no equivalent activity; its "
                "comparison points to Dataflow Gen2 (Power Query).",
        alternatives=("Re-author the Power Query in a Dataflow Gen2 item and run it from a pipeline Dataflow activity.",)),
    CompatEntry(
        id="activity.mapping_dataflow", kind="activity", types=("executedataflow",),
        status=PREVIEW, impact=Impact.difference, source_url=MDF_URL,
        summary="Mapping Data Flows run in Fabric as Mapping Data Flow transforms in Dataflow Gen2, which is in public "
                "preview. Not supported there: flowlets, the Data Flow library, user-defined functions and managed "
                "virtual networks; execution is only through a pipeline Dataflow activity; not all capabilities have "
                "parity. The data flow itself is not part of the pipeline JSON.",
        alternatives=("Use the built-in Mapping Data Flow upgrade (Dataflow Gen2 with MDF transforms) and check the "
                      "documented limitations first.",
                      "Re-implement the transformation as a Fabric notebook (Spark) or Power Query in Dataflow Gen2.")),
    CompatEntry(
        id="activity.ssis", kind="activity", types=("executessispackage",),
        status=NOT_AVAILABLE, impact=Impact.blocker, emit=False, source_url=COMPARE_URL,
        summary="SSIS package execution is not available in Fabric Data Factory, and Azure-SSIS integration runtimes "
                "have no Fabric equivalent yet.",
        alternatives=("Keep the package on the Azure-SSIS runtime in ADF and call that pipeline from Fabric.",
                      "Re-implement the package logic with Copy, Dataflow Gen2 or a notebook.")),
    CompatEntry(
        id="activity.synapse_spark", kind="activity", types=("synapsenotebook", "sparkjob"),
        status=NOT_AVAILABLE, impact=Impact.manual, emit=False, source_url=COMPARE_URL,
        summary="Synapse Notebook and Spark job definition activities are not available in Fabric Data Factory; Fabric "
                "has its own Notebook activity.",
        alternatives=("Move the notebook or Spark job definition into the Fabric workspace and run it with the Fabric "
                      "Notebook activity or a Spark job definition.",)),
    CompatEntry(
        id="activity.usql", kind="activity", types=("datalakeanalyticsusql",),
        status=REMOVED, impact=Impact.blocker, emit=False, source_url=COMPARE_URL,
        summary="Azure Data Lake Analytics (U-SQL) is retired and is not in Fabric Data Factory's activity list.",
        alternatives=("Re-implement the U-SQL job as a Fabric notebook (Spark) or Dataflow Gen2.",)),
)

# --- factory-level artifacts that are not pipeline activities ----------------------------------------------------
# Detected from the source estate (an ADF CDC resource is ``Microsoft.DataFactory/factories/adfcdcs``). Azure Synapse Link
# is NOT detectable this way: it is configured in Synapse / the source database, not in an ADF export, so it has no
# entry here; Microsoft's guidance (use Fabric Mirroring instead) is in the playbook.

_ARTIFACTS: tuple[CompatEntry, ...] = (
    CompatEntry(
        id="artifact.adf_cdc", kind="artifact", types=("changedatacapture",), status=REPLACED, impact=Impact.manual,
        emit=False, source_url=COMPARE_URL,
        summary="An ADF change data capture resource has no Fabric artifact: Microsoft's comparison says Fabric manages "
                "incremental data movement through Copy jobs instead of CDC artifacts. Its table mappings are not converted.",
        alternatives=("Create a Fabric Copy job with the same source and destination connections and the same tables, in "
                      "the incremental mode that fits; recreate any column mappings there.",
                      "Keep the CDC resource running in ADF until the Copy job is verified.")),
)

# --- linked services / connectors (ADF connector release stages and timelines) -------------------------------

# Connectors Microsoft lists as Removed outright (no replacement version): type -> removal date.
_REMOVED_OUTRIGHT: dict[str, Optional[str]] = {
    "azuremariadb": "2024-12-31", "concur": "2024-12-31", "couchbase": "2024-12-31", "drill": "2024-12-31",
    "hbase": "2024-12-31", "magento": "2024-12-31", "marketo": "2024-12-31", "eloqua": "2024-12-31",
    "responsys": "2024-12-31", "oracleservicecloud": "2024-12-31", "paypal": "2024-12-31", "phoenix": "2024-12-31",
    "salesforcemarketingcloud": "2024-12-31", "zoho": "2024-12-31", "amazonmws": None,
}

# V1 connectors that have a separate V2 type name; the V1 type is Removed: type -> (V2 type, removal date).
_V1_TYPE_REMOVED: dict[str, tuple[str, str]] = {
    "googlebigquery": ("GoogleBigQueryV2", "2025-09-30"),
    "postgresql": ("PostgreSqlV2", "2025-09-30"),
    "salesforce": ("SalesforceV2", "2025-09-30"),
    "salesforceservicecloud": ("SalesforceServiceCloudV2", "2025-09-30"),
    "servicenow": ("ServiceNowV2", "2025-09-30"),
    "snowflake": ("SnowflakeV2", "2025-09-30"),
}

# Connectors keeping one type name across versions where version 1.0 is Removed: type -> removal date.
_V1_VERSION_REMOVED: dict[str, str] = {
    "amazonrdsfororacle": "2026-03-31", "amazonredshift": "2026-04-30", "cassandra": "2025-09-30",
    "greenplum": "2025-09-30", "hive": "2025-10-31", "hubspot": "2025-11-22", "impala": "2025-10-31",
    "jira": "2026-04-30", "mariadb": "2025-09-30", "mysql": "2025-09-30", "netezza": "2025-10-31",
    "oracle": "2026-03-31", "presto": "2025-09-30", "quickbooks": "2025-09-30", "shopify": "2025-11-22",
    "spark": "2025-10-31", "square": "2025-11-15", "teradata": "2025-10-31", "vertica": "2025-09-30",
    "xero": "2026-04-30",
}


def _connector_entries() -> list[CompatEntry]:
    out: list[CompatEntry] = []
    for t, date in _REMOVED_OUTRIGHT.items():
        when = f"on {date}" if date else "(no date given)"
        out.append(CompatEntry(
            id=f"connector.{t}.removed", kind="linked_service", types=(t,), status=REMOVED, date=date,
            impact=Impact.blocker, source_url=CONNECTORS_URL,
            summary=f"Microsoft lists the ADF '{t}' connector as Removed {when}; there is no current version to "
                    "upgrade to, so pipelines that use it cannot run in ADF either.",
            alternatives=("Move the data with a different supported connector (for example a generic REST, ODBC or "
                          "file-based export) or keep this step outside Fabric.",
                          "Check Fabric's connector overview for a replacement before redesigning.")))
    for t, (v2, date) in _V1_TYPE_REMOVED.items():
        out.append(CompatEntry(
            id=f"connector.{t}.v1", kind="linked_service", types=(t,), status=REMOVED, date=date,
            impact=Impact.manual, source_url=CONNECTORS_URL,
            summary=f"The V1 '{t}' linked service type was Removed on {date} (the current type is {v2}). The linked "
                    "service is out of date, so its settings are the V1 shape.",
            alternatives=(f"Upgrade the ADF linked service to {v2} first, or create the Fabric connection directly from "
                          "the current connector's settings; do not copy V1 settings across.",)))
    for t, date in _V1_VERSION_REMOVED.items():
        out.append(CompatEntry(
            id=f"connector.{t}.v1", kind="linked_service", types=(t,), status=REMOVED, date=date,
            impact=Impact.manual, source_url=CONNECTORS_URL, versions=("1.0", ""),
            summary=f"Version 1.0 of the ADF '{t}' connector was Removed on {date}; version 2.0 is current.",
            alternatives=("Upgrade the linked service to version 2.0 (set typeProperties.version) or create the Fabric "
                          "connection from the current connector's settings; V1 settings differ.",)))
    return out


CATALOG: tuple[CompatEntry, ...] = _ACTIVITIES + _ARTIFACTS + tuple(_connector_entries())
_BY_ID = {e.id: e for e in CATALOG}
if len(_BY_ID) != len(CATALOG):  # a duplicated id would silently shadow an entry
    raise RuntimeError("duplicate compatibility catalog ids")


def entry(entry_id: str) -> CompatEntry:
    return _BY_ID[entry_id]


def for_activity(adf_type: Optional[str]) -> list[CompatEntry]:
    key = (adf_type or "").lower()
    return [e for e in CATALOG if e.kind == "activity" and key in e.types]


def for_artifact(kind_value: Optional[str]) -> list[CompatEntry]:
    key = (kind_value or "").lower()
    return [e for e in CATALOG if e.kind == "artifact" and key in e.types]


def for_linked_service(ls_type: Optional[str], version: Optional[str] = None) -> list[CompatEntry]:
    key, v = (ls_type or "").lower(), (version or "").strip()
    out = []
    for e in CATALOG:
        if e.kind != "linked_service" or key not in e.types:
            continue
        if e.versions is None or v in e.versions:
            out.append(e)
    return out


def describe(e: CompatEntry, version: Optional[str] = None) -> str:
    """The entry's summary, with the 'version not stated' caveat where it applies."""
    if e.versions is not None and not (version or "").strip():
        return (e.summary + " The linked service does not state a version, so it may be the removed 1.0 - "
                "set or confirm typeProperties.version.")
    return e.summary


def issue_code(e: CompatEntry) -> str:
    """The ConversionIssue code for an entry, e.g. ``compat_activity_wrangling_dataflow``."""
    return "compat_" + e.id.replace(".", "_")


def issue_message(e: CompatEntry, version: Optional[str] = None, prefix: str = "") -> str:
    when = f" Microsoft's date: {e.date}." if e.date else ""
    return f"{prefix}{describe(e, version)}{when} Source: {e.source_url}"


def _finding(e: CompatEntry, subject: str, adf_type: str, pipelines: Iterable[str],
             version: Optional[str] = None) -> CompatibilityFinding:
    return CompatibilityFinding(
        id=e.id, kind=e.kind, subject=subject, adf_type=adf_type, pipelines=sorted(set(pipelines)), status=e.status,
        date=e.date, impact=e.impact, summary=describe(e, version), alternatives=list(e.alternatives),
        source_url=e.source_url, emitted=e.emit)


def scan(d: AdfSemanticDiscovery, pipeline_names: Optional[set[str]] = None) -> list[CompatibilityFinding]:
    """Every catalog hit in the source estate (activities and linked services), sorted and de-duplicated."""
    wanted = {n.lower() for n in pipeline_names} if pipeline_names is not None else None
    findings: list[CompatibilityFinding] = []
    ls_by_name = {ls.name.lower(): ls for ls in d.linked_services}
    ls_users: dict[str, set[str]] = {}
    for p in d.pipelines:
        if wanted is not None and p.name.lower() not in wanted:
            continue
        for a in walk(p.activities):
            for e in for_activity(a.type):
                findings.append(_finding(e, a.name, a.type or "", [p.name]))
            names = set(a.linked_service_refs) | {io.linked_service for io in [*a.inputs, *a.outputs] if io.linked_service}
            for n in names:
                ls_users.setdefault(n.lower(), set()).add(p.name)
    for low, users in ls_users.items():
        ls = ls_by_name.get(low)
        if ls is None:
            continue
        for e in for_linked_service(ls.type, ls.connector_version):
            findings.append(_finding(e, ls.name, ls.type or "", users, ls.connector_version))
    for c in d.change_data_captures:  # factory-level: reported whichever pipelines were selected
        for e in for_artifact("changeDataCapture"):
            detail = (f" This resource ({c.mode or 'mode not stated'}"
                      f"{f', every {c.interval} {c.frequency}' if c.interval and c.frequency else ''}) reads "
                      f"{len(c.source_tables)} table(s) from {', '.join(c.source_linked_services) or '?'} into "
                      f"{', '.join(c.target_linked_services) or '?'} with {c.mapping_count} mapping(s)"
                      + (f"; tables: {', '.join(c.source_tables[:8])}" + (" ..." if len(c.source_tables) > 8 else "")
                         if c.source_tables else "") + ".")
            f = _finding(e, c.name, "ADF change data capture", [])
            findings.append(f.model_copy(update={"summary": f.summary + detail}))
    return sorted(findings, key=lambda f: (f.kind, f.id, f.subject.lower()))
