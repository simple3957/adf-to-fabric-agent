"""Run dependency discovery for a project: semantic record + source manifest (+ optional access checks) -> inventory.

Input adapters (live Azure, Git, ZIP) have already normalised the source into the project by intake, so this reads the same
two records whichever adapter was used.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Optional

from adf2fabric.agent.dependencies import access
from adf2fabric.agent.dependencies.builder import build_inventory
from adf2fabric.agent.dependencies.report import render_report
from adf2fabric.project.dependencies import DependencyInventory
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore


class DependencyDiscoveryError(Exception):
    pass


def discover_dependencies(store: ProjectStore, project_id: str, check_providers: Iterable[str] = (),
                          tokens: Optional[access.TokenSource] = None, transport: access.Transport = access._requests_transport,
                          connector: access.Connector = access._tcp, now: Optional[datetime] = None) -> DependencyInventory:
    """Build and save the inventory. Without ``check_providers`` nothing external is contacted."""
    if not store.has_adf_semantic_discovery(project_id):
        raise DependencyDiscoveryError("Run `discovery-adf-semantic` first: the dependency inventory is built from its result.")
    d = store.load_adf_semantic_discovery(project_id)
    try:
        manifest = store.load_source_manifest(project_id)
    except ProjectNotFoundError:
        manifest = None
    unknown = sorted(set(check_providers) - set(access.PROVIDERS))
    if unknown:
        raise DependencyDiscoveryError(f"Unknown provider(s) {', '.join(unknown)}; choose from {', '.join(access.PROVIDERS)}.")
    now = now or datetime.now(timezone.utc)
    previous = store.load_dependency_inventory(project_id) if store.has_dependency_inventory(project_id) else None
    requested = sorted(set(check_providers))
    checks: dict = {}
    if requested:
        first = build_inventory(d, manifest, now=now)
        checks = access.run_checks(first.dependencies, requested, tokens, transport, connector, now)
    inv = build_inventory(d, manifest, checks, requested, now, previous)
    store.save_dependency_inventory(project_id, inv, render_report(inv))
    return inv
