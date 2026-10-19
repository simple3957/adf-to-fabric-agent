"""Shared result models for connection providers. Every field is safe to send to a browser: no secret ever lands here."""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ConnectionState(str, Enum):
    """Distinct states a source, destination or linked-service connection can be in. Authenticated is NOT tested."""

    not_configured = "not_configured"
    authentication_required = "authentication_required"
    authenticated = "authenticated"
    resource_discovered = "resource_discovered"
    connection_configured = "connection_configured"
    connection_tested = "connection_tested"
    validation_failed = "validation_failed"
    manual_action_required = "manual_action_required"
    skipped = "skipped"  # the user chose to leave this linked service unbound; nothing is created for it


class CheckStatus(str, Enum):
    passed = "passed"
    failed = "failed"
    skipped = "skipped"  # not applicable to this configuration
    unverified = "unverified"  # applicable but cannot be checked from here (stated, never faked)


class Check(_Model):
    name: str
    status: CheckStatus
    detail: str = ""


class ProviderStatus(_Model):
    provider: str
    available: bool = False  # the CLI / library exists on this machine
    version: Optional[str] = None
    authenticated: bool = False
    account: Optional[str] = None
    tenant_id: Optional[str] = None
    subscription_id: Optional[str] = None
    state: ConnectionState = ConnectionState.not_configured
    checks: list[Check] = Field(default_factory=list)
    actions: list[str] = Field(default_factory=list)  # what the user should do next
    error: Optional[str] = None


class TestReport(_Model):
    """Exactly what a test verified, and what it could not."""

    target: str
    state: ConnectionState
    verified: list[str] = Field(default_factory=list)
    not_verified: list[str] = Field(default_factory=list)  # limitations, stated plainly
    checks: list[Check] = Field(default_factory=list)
    error: Optional[str] = None
    actions: list[str] = Field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.state == ConnectionState.connection_tested


class Subscription(_Model):
    id: str
    name: str
    tenant_id: Optional[str] = None
    state: Optional[str] = None
    is_default: bool = False
    user: Optional[str] = None


class AzureAccount(_Model):
    """One person signed in to the Azure CLI on this machine (a username can appear in several tenants)."""

    username: str
    tenants: list[str] = Field(default_factory=list)
    is_default: bool = False
    subscription_count: int = 0  # real subscriptions; the tenant-level entry of a sign-in without any is not counted


class ArmResource(_Model):
    id: str
    name: str
    type: str
    location: Optional[str] = None
    resource_group: Optional[str] = None
    properties: dict[str, Any] = Field(default_factory=dict)  # only the non-secret fields we asked for


class FabricWorkspace(_Model):
    id: str
    display_name: str
    type: Optional[str] = None
    capacity_id: Optional[str] = None
    capacity_region: Optional[str] = None
    description: Optional[str] = None
