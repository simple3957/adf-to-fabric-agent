"""Helpers shared by the conversion handlers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

from adf2fabric.agent.conversion import connectors
from adf2fabric.agent.conversion.context import ActivityBuilder
from adf2fabric.agent.conversion.expr_convert import is_expression
from adf2fabric.project.conversion import ConversionStrategy, Impact, IssueSeverity
from adf2fabric.project.discovery import AdfIO

REDACTED = "[REDACTED]"


def carry(cx: ActivityBuilder, documented: Iterable[str], *, rename: Optional[dict[str, str]] = None,
          skip: Iterable[str] = (), note_skipped: bool = True) -> dict:
    """Carry the activity's typeProperties to Fabric: documented keys are converted and kept,
    every other key is reported as not carried over (never silently dropped)."""
    documented = set(documented)
    skip = set(skip)
    rename = rename or {}
    out: dict[str, Any] = {}
    for key, value in cx.act.raw_properties.items():
        if key in skip:
            continue
        if key in documented:
            target = rename.get(key, key)
            out[target] = cx.tree(value, f"typeProperties.{key}", f"typeProperties.{target}")
            cx.mapping(f"typeProperties.{key}", f"typeProperties.{target}",
                       "renamed" if target != key else "passthrough")
        elif note_skipped:
            cx.dropped(f"typeProperties.{key}", "not a documented Fabric property for this activity")
    return out


def check_connector(cx: ActivityBuilder, ls_name: str, *, what: str) -> str:
    """Record the documented-connector verdict for a linked service; returns the verdict."""
    ls = cx.pc.project.linked_services.get(ls_name.lower())
    kind, why = connectors.verdict(ls.type if ls else None)
    if kind == "unsupported":
        cx.blocker("connector_unsupported",
                   f"{what}: linked service '{ls_name}' ({ls.type if ls else '?'}) has no Fabric counterpart: {why}.",
                   "Move the data to a supported store first, or keep this step outside Fabric.")
    elif kind == "unlisted":
        cx.difference("connector_unverified",
                      f"{what}: linked service '{ls_name}' ({ls.type if ls else '?'}) - {why}; support could not be "
                      "confirmed.", "Check the Fabric connector overview before relying on this activity.")
    return kind


# --- opt-in Lakehouse repointing ----------------------------------------------------------------------------------
# Live-verified 2026-10-22 (Copy CSV->CSV and Parquet->Parquet, Lookup, GetMetadata childItems/exists; runs Completed with identical
# output): datasetSettings.linkedService {type: Lakehouse, typeProperties {workspaceId, artifactId, rootFolder: Files}},
# location {type: LakehouseLocation, folderPath, fileName}, storeSettings LakehouseReadSettings / LakehouseWriteSettings.
# Microsoft's connector page documents only the portal fields, so nothing beyond what ran is emitted.
LAKEHOUSE_FORMATS = ("DelimitedText", "Parquet")
# Dataset types whose converted form has been RUN in Fabric and matched ADF (Azure Synapse tables; ADLS Gen2 CSV and Parquet; the Lakehouse
# Files repointing above). Every other type is converted from its ADF shape and says so.
LIVE_VERIFIED_DATASET_TYPES = frozenset({"AzureSqlDWTable", "DelimitedText", "Parquet"})
_LAKEHOUSE_STORE = {"AzureBlobFSReadSettings": "LakehouseReadSettings", "AzureBlobFSWriteSettings": "LakehouseWriteSettings"}
# storeSettings keys the Lakehouse connector documents with the same meaning (none of them is a path relative to the container)
_LAKEHOUSE_STORE_KEYS = {"type", "recursive", "wildcardFileName", "modifiedDatetimeStart", "modifiedDatetimeEnd",
                         "enablePartitionDiscovery", "maxConcurrentConnections", "copyBehavior", "blockSizeInMB", "metadata"}
_ADLS_URL = re.compile(r"^https://([a-z0-9]{3,24})\.dfs\.core\.windows\.net/?$", re.I)


@dataclass
class LakehouseRoute:
    shortcut: str
    folder: str  # folder under the container, literal ("" when none)
    dynamic_folder: bool = False  # the folder is an expression: the shortcut is prefixed to it, the expression itself is untouched


def _literal(v: Any) -> tuple[Optional[str], bool]:
    """(text, is_expression) of a dataset location field as authored."""
    if v is None:
        return None, False
    if isinstance(v, dict):
        return None, True
    if hasattr(v, "value") and not isinstance(v, str):  # an Expression object from discovery
        return None, True
    s = str(v)
    if s.startswith("@@"):  # ADF's escape for a literal leading '@'
        return s[1:], False
    return s, s.startswith("@") or "@{" in s


def lakehouse_route(cx: ActivityBuilder, io: Optional[AdfIO], store_settings: Any, role: str) -> Optional[LakehouseRoute]:
    """The shortcut a dataset can be read or written through, or None (with an info issue saying why) when it stays on its connection."""
    pc = cx.pc.project
    if not pc.storage_routes or io is None or io.kind != "dataset" or not io.name:
        return None
    ds = pc.datasets.get(io.name.lower())
    if ds is None:
        return None

    def keep(why: str) -> None:
        cx.info("storage_not_repointed", f"{role} dataset '{ds.name}' stays on its storage connection: {why}.",
                "See the storage map (storage-map) for what to decide.")
        return None

    loc = (ds.type_properties or {}).get("location")
    if not isinstance(loc, dict) or str(loc.get("type")) != "AzureBlobFSLocation":
        return None  # not an ADLS Gen2 file dataset: nothing to repoint, nothing to report
    if ds.type not in LAKEHOUSE_FORMATS:
        return keep(f"format {ds.type} has not been verified against the Fabric Lakehouse connector (verified: {', '.join(LAKEHOUSE_FORMATS)})")
    ls = pc.linked_services.get(ds.linked_service_ref.name.lower()) if ds.linked_service_ref else None
    m = _ADLS_URL.match((ls.endpoint or "").strip()) if ls else None
    if not m:
        return keep("its linked service does not state a literal ADLS Gen2 account URL")
    container, c_dyn = _literal(loc.get("fileSystem"))
    folder, f_dyn = _literal(loc.get("folderPath"))
    if not container or c_dyn:
        return keep("its container is empty or an expression")
    if folder and ".." in folder:
        return keep("its folder path contains '..'")
    shortcut = pc.storage_routes.get((m.group(1).lower(), container.lower()))
    if not shortcut:
        return keep(f"no shortcut is planned for {m.group(1).lower()}/{container}")
    if store_settings is not None:
        if not isinstance(store_settings, dict) or str(store_settings.get("type")) not in _LAKEHOUSE_STORE:
            return keep(f"its storeSettings type {store_settings.get('type') if isinstance(store_settings, dict) else '?'} is not an ADLS Gen2 read/write setting")
        extra = sorted(set(store_settings) - _LAKEHOUSE_STORE_KEYS)
        if extra:
            return keep(f"storeSettings uses {', '.join(extra)}, which is a path under the container or has no verified Lakehouse meaning")
    if "'" in shortcut:
        return keep("the shortcut name contains a quote")
    return LakehouseRoute(shortcut, "" if f_dyn else "/".join(p for p in (folder or "").replace("\\", "/").split("/") if p), f_dyn)


def _prefixed_folder(shortcut: str, converted: Any) -> Optional[Any]:
    """The folder expression with the shortcut in front, or None when its form is not one this handles.

    ``@expr`` -> ``@concat('<shortcut>/', expr)``; an interpolated string (``a/@{x}``) -> ``<shortcut>/a/@{x}``."""
    v = converted.get("value") if isinstance(converted, dict) and converted.get("type") == "Expression" else converted
    if not isinstance(v, str):
        return None
    if v.startswith("@@") or ("@" not in v):  # the argument substituted to a literal (or an escaped '@'): prefix it as text
        lit = v[1:] if v.startswith("@@") else v
        if ".." in lit:
            return None
        return "/".join(p for p in (shortcut, *lit.replace("\\", "/").split("/")) if p)
    if v.startswith("@") and not v.startswith("@{"):
        new = f"@concat('{shortcut}/', {v[1:]})"
    elif "@{" in v and isinstance(converted, dict):  # an interpolated expression: the shortcut is plain text in front
        new = f"{shortcut}/{v}"
    else:
        return None  # a plain string holding '@{...}' was never converted: prefixing it would ship unconverted syntax
    return {"value": new, "type": "Expression"} if isinstance(converted, dict) else new


def lakehouse_store_settings(cx: ActivityBuilder, store: dict, target_path: str) -> dict:
    out = dict(store)
    out["type"] = _LAKEHOUSE_STORE[str(store["type"])]
    cx.mapping(target_path, target_path, "storage_repointed", f"{store['type']} -> {out['type']}")
    return out


def dataset_settings(cx: ActivityBuilder, io: Optional[AdfIO], source_path: str, target_path: str,
                     route: Optional[LakehouseRoute] = None) -> Optional[dict]:
    """Inline an ADF dataset as Fabric ``datasetSettings`` (Fabric has no datasets).

    Dataset parameters are substituted with the arguments the activity passed (or the dataset's
    declared defaults), at the syntax-tree level; the linked service becomes a connection reference.
    """
    if io is None:
        cx.blocker("dataset_missing", "The activity references no dataset, so there is nothing to inline.",
                   "Reference a dataset in the source pipeline.")
        return None
    if io.kind != "dataset" or not io.name:
        cx.blocker("inline_dataset_unsupported", "The activity uses an inline (non-dataset) endpoint.")
        return None
    ds = cx.pc.project.datasets.get(io.name.lower())
    if ds is None:
        cx.blocker("dataset_not_found",
                   f"Dataset '{io.name}' is not among the discovered artifacts, so it cannot be inlined.",
                   "Run Repository + ADF Semantic Discovery on the complete source (all dataset folders).")
        return None
    if not ds.type:
        cx.blocker("dataset_type_missing", f"Dataset '{ds.name}' declares no type.")
        return None

    if ds.type not in LIVE_VERIFIED_DATASET_TYPES:
        cx.info("dataset_type_not_live_verified",
                f"Dataset '{ds.name}' ({ds.type}) is carried with its ADF type and properties; this type has not been run in Fabric by this tool.",
                "Run the converted pipeline once in a test workspace and compare it with ADF (`verify`).")
    args = {k.lower(): cx.tree(v, f"{source_path}.parameters.{k}", f"{target_path}.parameters.{k}")
            for k, v in io.parameters.items()}
    defaults = {p.name.lower(): p.default_raw for p in ds.parameters if p.has_default}
    env = cx.env(dataset_name=ds.name, dataset_args=args, dataset_defaults=defaults)
    tp = cx.tree(ds.type_properties, f"dataset:{ds.name}.typeProperties", f"{target_path}.typeProperties", env)

    settings: dict[str, Any] = {
        "annotations": list(ds.annotations),
        "type": ds.type,
        "schema": [{"name": c.name, **({"type": c.type} if c.type else {})} for c in ds.columns],
        "typeProperties": tp,
    }
    cx.mapping(f"dataset:{ds.name}", target_path, "dataset_inlined",
               f"Fabric has no datasets; '{ds.name}' ({ds.type}) became inline datasetSettings"
               + (f" with {len(args)} argument(s) substituted" if args else ""))
    folder_value: Any = "/".join(p for p in (route.shortcut, route.folder) if p) if route is not None else None
    if route is not None and route.dynamic_folder:
        folder_value = _prefixed_folder(route.shortcut, (tp.get("location") or {}).get("folderPath"))
        if folder_value is None:
            cx.info("storage_not_repointed", f"Dataset '{ds.name}' stays on its storage connection: its folder expression has a form "
                    "the shortcut cannot be prefixed to safely.", "Repoint it by hand.")
            route = None
    if route is not None:
        lh = cx.pc.project.route_lakehouse or ""
        loc = dict(tp.get("location") or {})
        new_loc: dict[str, Any] = {"type": "LakehouseLocation", "folderPath": folder_value}
        if "fileName" in loc:
            new_loc["fileName"] = loc["fileName"]
        settings["typeProperties"] = {**tp, "location": new_loc}
        settings["linkedService"] = {"name": lh, "properties": {"annotations": [], "type": "Lakehouse", "typeProperties": {
            "workspaceId": cx.workspace(), "artifactId": cx.item("lakehouse", lh, f"The lakehouse '{lh}' that holds the OneLake shortcuts."),
            "rootFolder": "Files"}}}
        cx.mapping(f"dataset:{ds.name}.location", f"{target_path}.typeProperties.location", "storage_repointed",
                   f"ADLS container {loc.get('fileSystem')} -> lakehouse '{lh}' Files/{route.shortcut} (OneLake shortcut)"
                   + ("; the folder expression is kept, with the shortcut in front" if route.dynamic_folder else ""))
        return settings
    if ds.linked_service_ref is not None:
        ls = ds.linked_service_ref.name
        check_connector(cx, ls, what=f"dataset '{ds.name}'")
        settings["externalReferences"] = {"connection": cx.connection(ls, args=ds.linked_service_arguments, subject=f"Dataset '{ds.name}'")}
        cx.mapping(f"dataset:{ds.name}.linkedServiceName", f"{target_path}.externalReferences.connection",
                   "linked_service_to_connection", f"linked service '{ls}' -> Fabric connection")
    else:
        cx.issue("dataset_without_linked_service", f"Dataset '{ds.name}' has no linked service.",
                 impact=Impact.manual, action="Specify a Fabric connection for this dataset.")
    return settings


def split_url(url: str) -> tuple[Optional[str], str]:
    """(base URL, relative URL) of a literal URL, for Fabric's connection + relativeUrl model."""
    p = urlparse(url)
    if not p.scheme or not p.netloc:
        return None, url
    base = f"{p.scheme}://{p.netloc}"
    rel = p.path + (f"?{p.query}" if p.query else "") + (f"#{p.fragment}" if p.fragment else "")
    return base, rel


def has_masked_secret(value: Any) -> bool:
    if isinstance(value, str):
        return value == REDACTED
    if isinstance(value, dict):
        return any(has_masked_secret(v) for v in value.values())
    if isinstance(value, list):
        return any(has_masked_secret(v) for v in value)
    return False


def downgrade_if_not_direct(cx: ActivityBuilder) -> None:
    """A conversion that rewrote anything is 'transformed', not 'direct'."""
    if cx.strategy is ConversionStrategy.direct and (
            any(c.status == "rewritten" for c in cx.expression_conversions)
            or any(not m.preserved for m in cx.property_mappings)):
        cx.strategy = ConversionStrategy.transformed


def literal_text(v: Any) -> Optional[str]:
    """A plain string value (not dynamic content), else None."""
    return v if isinstance(v, str) else None


def expr_or_literal(v: Any) -> Any:
    return v["value"] if is_expression(v) else v


def block(cx: ActivityBuilder, code: str, reason: str, *alternatives: str, disposition: Optional[str] = None) -> None:
    """Explicitly NOT convert the activity.

    Strategy E: a fail-fast placeholder keeps the dependency graph intact (a run stops here loudly
    instead of silently skipping the operation), every source operation is recorded as uncovered, and
    a blocker issue with the documented alternatives is attached.
    """
    cx.strategy = ConversionStrategy.manual
    if disposition:
        cx.disposition = disposition
    for a in alternatives:
        cx.alternative(a)
    if not any(i.impact is Impact.blocker for i in cx.issues):
        cx.issue(code, f"{cx.act.type} was not converted: {reason}", impact=Impact.blocker,
                 severity=IssueSeverity.error, action=cx.alternatives[0] if cx.alternatives else "")
    cx.placeholder_component(code, reason)
    for op in cx.act.operations:
        cx.cover(op, note="not converted: " + reason)
