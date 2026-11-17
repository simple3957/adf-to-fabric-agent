"""Compatibility catalog: components that are retired, deprecated, in preview, or missing in Fabric.

The converter's per-type handlers know *how* to convert; this catalog knows *what Microsoft says about the
component's lifecycle*, with the source page and the date it was verified, so a retired connector or a deprecated
activity is reported with its date and a concrete way forward instead of passing as "unverified".

Rules that keep it honest:

* Every entry cites a Microsoft page (``source_url``) and its date. The entries live in the master compatibility catalog
  (``adf2fabric/catalog/compatibility.json``): when a page changes, update the entry there with the source's date.
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

from adf2fabric.catalog import master, source

VERIFIED_ON = source("adf_connector_release_stages")["read_on"]
COMPARE_URL = source("fabric_vs_adf")["url"]
CONNECTORS_URL = source("adf_connector_release_stages")["url"]
MDF_URL = source("fabric_mdf_transforms")["url"]

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


# --- read from the master compatibility catalog (adf2fabric/catalog/compatibility.json) ----------------------------------


def _entry(d: dict, kind: str, types: tuple[str, ...]) -> CompatEntry:
    return CompatEntry(id=d["id"], kind=kind, types=types, status=d["status"], summary=d["summary"], impact=Impact(d["impact"]),
                       alternatives=tuple(d["alternatives"]), source_url=d["source_url"], emit=d.get("emit", True))


def _activity_entries() -> tuple[CompatEntry, ...]:
    """Each lifecycle entry once, with every ADF activity type that carries it (one entry may cover several types)."""
    seen: dict[str, tuple[dict, list[str]]] = {}
    for cap in master()["capabilities"].values():
        for adf_type, e in cap["by_adf_type"].items():
            for d in e.get("lifecycle") or []:
                seen.setdefault(d["id"], (d, []))[1].append(adf_type.lower())
    return tuple(_entry(d, "activity", tuple(types)) for d, types in seen.values())


_ACTIVITIES: tuple[CompatEntry, ...] = _activity_entries()
_ARTIFACTS: tuple[CompatEntry, ...] = tuple(_entry(d, "artifact", tuple(d["types"])) for d in master()["artifacts"])
_LIFECYCLE = master()["connectors"]["lifecycle"]
# Connectors Microsoft lists as Removed outright (no replacement version): type -> removal date.
_REMOVED_OUTRIGHT: dict[str, Optional[str]] = dict(_LIFECYCLE["removed"])
# V1 connectors that have a separate V2 type name; the V1 type is Removed: type -> (V2 type, removal date).
_V1_TYPE_REMOVED: dict[str, tuple[str, str]] = {k: (v["v2_type"], v["date"]) for k, v in _LIFECYCLE["v1_type_removed"].items()}
# Connectors keeping one type name across versions where version 1.0 is Removed: type -> removal date.
_V1_VERSION_REMOVED: dict[str, str] = dict(_LIFECYCLE["v1_version_removed"])


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
