"""Request bodies. Secrets are ``SecretStr`` so they never show in a repr, a validation error or a log line."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class _In(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LoginBody(_In):
    tenant_id: Optional[str] = None
    device_code: bool = False
    method: str = "interactive"  # fabric_cli only: interactive | managed_identity | service_principal_certificate
    client_id: Optional[str] = None
    certificate_path: Optional[str] = None


class LogoutBody(_In):
    account: Optional[str] = None  # azure_cli only: the account to sign out (default: the current one)


class SourceSelection(_In):
    kind: str  # azure | git | zip
    subscription_id: Optional[str] = None
    resource_group: Optional[str] = None
    factory_name: Optional[str] = None
    repository_url: Optional[str] = None
    revision: str = "HEAD"
    subdirectory: Optional[str] = None
    upload_id: Optional[str] = None
    username: Optional[str] = None
    token: Optional[SecretStr] = None  # git access token: used for this request only, never stored


class DestinationSelection(_In):
    provider: str = "azure_cli"  # azure_cli | fabric_cli
    account: Optional[str] = None  # azure_cli: which signed-in account Fabric is used as (default: the CLI's default)
    tenant_id: Optional[str] = None
    workspace_id: Optional[str] = None


class ImportBody(_In):
    project_name: str
    source: SourceSelection
    pipelines: list[str] = Field(default_factory=list)


class ChoiceBody(_In):
    method: str
    endpoint: dict[str, str] = Field(default_factory=dict)
    values: dict[str, str] = Field(default_factory=dict)
    display_name: Optional[str] = None
    privacy_level: str = "Organizational"
    variants: list[dict[str, str]] = Field(default_factory=list)


class SecretBody(_In):
    field: str
    value: SecretStr


class ApplyBody(_In):
    names: list[str] = Field(default_factory=list)
    auto_match: bool = False
    dry_run: bool = False
    workspace_id: Optional[str] = None


class StageBody(_In):
    pipelines: list[str] = Field(default_factory=list)
    workspace_id: Optional[str] = None
    with_children: bool = False
    allow_unbound: bool = False


class WorkspaceBody(_In):
    display_name: str
    capacity_id: Optional[str] = None
    tenant_id: Optional[str] = None


class IdentityBody(_In):
    workspace_id: Optional[str] = None


class ModeBody(_In):
    mode: str


class GrantsBody(_In):
    subscriptions: list[str] = Field(default_factory=list, max_length=200)
    tenants: list[str] = Field(default_factory=list, max_length=200)
    accounts: list[str] = Field(default_factory=list, max_length=200)
    workspaces: list[str] = Field(default_factory=list, max_length=200)


class UserCreate(_In):
    id: str = Field(max_length=64)
    roles: list[str] = Field(min_length=1, max_length=5)
    grants: Optional[GrantsBody] = None


class UserUpdate(_In):
    roles: Optional[list[str]] = Field(default=None, min_length=1, max_length=5)
    disabled: Optional[bool] = None
    grants: Optional[GrantsBody] = None


def dump(model: Any) -> Any:
    return model.model_dump(mode="json") if hasattr(model, "model_dump") else model
