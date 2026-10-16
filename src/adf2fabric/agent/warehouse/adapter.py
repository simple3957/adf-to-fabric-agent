"""The source-adapter contract. Each source warehouse platform implements it; the rest of the framework only sees
``SourceCatalog``. Adapters are registered by name; only registered (built and tested) adapters exist.
"""

from __future__ import annotations

from typing import Any, Callable, Protocol, runtime_checkable

from adf2fabric.agent.warehouse.models import SourceCatalog


@runtime_checkable
class SourceAdapter(Protocol):
    source_type: str  # e.g. "synapse_dedicated"
    mode: str  # "offline" | "live"

    def capabilities(self) -> dict[str, bool]:
        """What this adapter can provide: columns, definitions, row_counts, dependencies, distribution ..."""
        ...

    def discover(self) -> SourceCatalog:
        """Read-only discovery of the source warehouse into the neutral catalog."""
        ...


_REGISTRY: dict[str, Callable[[dict[str, Any]], SourceAdapter]] = {}


def register(name: str, factory: Callable[[dict[str, Any]], SourceAdapter]) -> None:
    _REGISTRY[name] = factory


def create(name: str, options: dict[str, Any]) -> SourceAdapter:
    if name not in _REGISTRY:
        raise ValueError(f"No source adapter '{name}'. Available: {', '.join(sorted(_REGISTRY)) or 'none'}.")
    return _REGISTRY[name](options)


def available() -> list[str]:
    return sorted(_REGISTRY)
