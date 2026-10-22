"""Dependency inventory: what the ADF estate depends on, whether it resolves, and what migrating it means."""

from adf2fabric.agent.dependencies.builder import build_inventory
from adf2fabric.agent.dependencies.discoverer import DependencyDiscoveryError, discover_dependencies

__all__ = ["build_inventory", "discover_dependencies", "DependencyDiscoveryError"]
