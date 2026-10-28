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


class BindingBody(_In):
    key: str = Field(max_length=200)
    value: str = Field(max_length=64)


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
    confirm: bool = False  # a real (not dry-run) apply must say it is meant


class AssistGenerateBody(_In):
    names: list[str] = Field(min_length=1, max_length=25)  # pipelines to draft a fix for


class AssistAcceptBody(_In):
    code: Optional[str] = Field(default=None, max_length=1_048_576)  # an edited version of the draft; None accepts the draft as it is


class AssistDiscardBody(_In):
    scope: str = Field(default="draft", pattern="^(draft|all)$")


class StageBody(_In):
    pipelines: list[str] = Field(default_factory=list)
    workspace_id: Optional[str] = None
    with_children: bool = False
    allow_unbound: bool = False
    confirm: bool = False  # required by stages that change the target (deploy_apply)
    validate_stages: list[str] = Field(default_factory=list, max_length=4)  # validate stage: static | api | deployment | runtime
    lakehouse: Optional[str] = Field(default=None, max_length=256)  # convert: repoint ADLS datasets to this lakehouse; "" = keep connections; None = follow the brief
    apply_reconstruction: Optional[bool] = None  # convert: apply the safe reconstruction proposals; None = follow the brief
    assisted: bool = False  # convert: use the model-assisted drafts a person accepted (and that are still current) in place of the original source
    run: bool = False  # verify stage: START a real Fabric run of each named pipeline, then compare (needs migration:execute + confirm)
    run_timeout: int = Field(default=1800, ge=60, le=3000)  # seconds to wait for each started run; kept within one Fabric token's lifetime


class WorkspaceBody(_In):
    display_name: str
    capacity_id: Optional[str] = None
    tenant_id: Optional[str] = None


class IdentityBody(_In):
    workspace_id: Optional[str] = None
    confirm: bool = False


class ProvisionPlanBody(_In):
    workspace_id: Optional[str] = None  # an existing workspace (must be granted)
    workspace_name: Optional[str] = Field(default=None, max_length=256)
    capacity_id: Optional[str] = None
    capacity_name: Optional[str] = Field(default=None, max_length=256)
    tenant_id: Optional[str] = None
    lakehouses: list[str] = Field(default_factory=list, max_length=20)
    warehouses: list[str] = Field(default_factory=list, max_length=20)
    provision_identity: bool = False


class JourneyBody(_In):
    steps: list[str] = Field(max_length=20)  # step keys to confirm (PUT) or to forget (DELETE-style reset)


class BriefAnswersBody(_In):
    answers: dict[str, Any] = Field(max_length=100)  # question id -> answer (None clears it)


class BriefApproveBody(_In):
    fingerprint: str = Field(max_length=64)  # the answers the approver read: a brief that changed since is refused


class BriefImportBody(_In):
    questionnaire: dict[str, Any]


class BriefNoteBody(_In):
    text: str = Field(max_length=2000)


class ProvisionApproveBody(_In):
    plan_ref: str = Field(max_length=64)  # the plan the person reviewed: an approval of a different plan is refused
    steps: Optional[list[str]] = Field(default=None, max_length=200)  # None = every step that can be approved
    note: str = Field(default="", max_length=300)


class ProvisionApplyBody(_In):
    plan_ref: str = Field(max_length=64)
    apply: bool = False  # False = dry run: nothing is written
    confirm: bool = False  # required when apply is true


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
