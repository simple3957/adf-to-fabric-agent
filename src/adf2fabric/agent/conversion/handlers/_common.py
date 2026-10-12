"""Helpers shared by the conversion handlers."""

from __future__ import annotations

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


def dataset_settings(cx: ActivityBuilder, io: Optional[AdfIO], source_path: str, target_path: str) -> Optional[dict]:
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
    if ds.linked_service_ref is not None:
        ls = ds.linked_service_ref.name
        check_connector(cx, ls, what=f"dataset '{ds.name}'")
        settings["externalReferences"] = {"connection": cx.connection(ls)}
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
