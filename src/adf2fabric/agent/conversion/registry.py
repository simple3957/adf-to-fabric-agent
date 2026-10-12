"""The conversion registry: one :class:`ConversionRule` per ADF activity family.

A rule couples the handler that performs the conversion with the DOCUMENTATION of that
conversion: which ADF types and variants it covers, the Fabric target, the property /
input-output / expression / dependency / parameter-variable rules it applies, what it cannot
carry over, what must be configured by hand, and how the result is validated. The metadata is
data, not prose in a comment, so tests can require it to be complete and reports can print it.

Parsing stays in ``agent/discovery``; the registry only consumes the semantic representation.
An ADF activity type with no rule is NEVER dropped — the engine converts it with
into an explicit blocker with a fail-fast placeholder (see ``ConversionEngine._unregistered``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional

from adf2fabric.project.conversion import ConversionStrategy

if TYPE_CHECKING:  # pragma: no cover
    from adf2fabric.agent.conversion.context import ActivityBuilder

Handler = Callable[["ActivityBuilder"], None]


@dataclass(frozen=True)
class ConversionRule:
    name: str
    adf_types: tuple[str, ...]  # canonical ADF type names this rule converts
    variants: tuple[str, ...]  # configuration variants it distinguishes
    fabric_types: tuple[str, ...]  # Fabric components it may generate
    strategy: ConversionStrategy  # the typical outcome (a handler may downgrade it)
    handler: Handler
    property_rules: tuple[str, ...]
    io_rules: tuple[str, ...] = ()
    expression_rules: tuple[str, ...] = ()
    dependency_rules: tuple[str, ...] = ()
    parameter_variable_rules: tuple[str, ...] = ()
    unsupported: tuple[str, ...] = ()  # source properties/behaviors that are not carried over
    manual_configuration: tuple[str, ...] = ()  # what a person must configure
    validation_rules: tuple[str, ...] = ()
    runtime_disposition: Optional[str] = None  # replace | reuse_externally | redesign (compute activities)


REGISTRY: dict[str, ConversionRule] = {}


def register(rule: ConversionRule) -> ConversionRule:
    for t in rule.adf_types:
        key = t.lower()
        if key in REGISTRY:
            raise ValueError(f"ADF activity type '{t}' is registered twice")
        REGISTRY[key] = rule
    return rule


def rule_for(adf_type: Optional[str]) -> Optional[ConversionRule]:
    return REGISTRY.get((adf_type or "").strip().lower())


def registered_rules() -> list[ConversionRule]:
    """Each rule once, in a stable order."""
    seen: dict[str, ConversionRule] = {}
    for rule in REGISTRY.values():
        seen.setdefault(rule.name, rule)
    return sorted(seen.values(), key=lambda r: r.name)
