"""Persisted Source Manifest contract (project/persistence layer).

Intake can obtain ADF source from more than one place (a GitHub repository or a
live Azure Data Factory). Whatever the origin, Intake normalizes it into
``projects/<id>/source/`` and records HOW it was obtained in
``projects/<id>/source-manifest.yaml``. This module owns that data contract only;
the behavior lives in ``agent/intake/``.

Security: nothing here can hold a secret. Redactions record WHERE a value was
masked (artifact path + field path + reason), never the value. Azure identity is
limited to subscription / resource group / factory name. Tokens are never a field.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

SOURCE_MANIFEST_SCHEMA_VERSION = 1


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceType(str, Enum):
    github = "github"
    azure_adf = "azure_adf"
    git = "git"  # any HTTPS Git repository (GitHub, Azure DevOps, GitLab, Bitbucket, ...)
    zip_export = "zip_export"  # an uploaded workspace / repository export


class FindingSeverity(str, Enum):
    error = "error"
    warning = "warning"


class AzureFactoryIdentity(_Strict):
    subscription_id: str
    resource_group: str
    factory_name: str


class SourceArtifact(_Strict):
    """One normalized ADF artifact file under the source root."""

    kind: str
    name: str
    path: str  # POSIX path relative to the source root
    sha256: str  # hash of the file bytes as stored
    etag: Optional[str] = None  # ARM etag when the artifact came from Azure


class RetrievalFailure(_Strict):
    """A retrieval that did not succeed. Never silently dropped."""

    kind: str
    name: Optional[str] = None
    status_code: Optional[int] = None
    error_code: Optional[str] = None
    error: str
    action: str  # what the user should do about it


class RedactionRecord(_Strict):
    artifact_path: str
    field_path: str
    reason: str


class SourceFinding(_Strict):
    severity: FindingSeverity
    code: str
    message: str
    artifact: Optional[str] = None
    action: str = ""


class SourceSelection(_Strict):
    """What the user asked for, and what that resolved to."""

    pipelines: list[str] = Field(default_factory=list)
    artifact_kinds: list[str] = Field(default_factory=list)
    # Source-root-relative paths of the selected pipelines and everything they
    # reference. Empty means "no selection: everything".
    resolved_paths: list[str] = Field(default_factory=list)


class SourceManifest(_Strict):
    schema_version: int = SOURCE_MANIFEST_SCHEMA_VERSION
    project_id: str
    source_type: SourceType
    source_identifier: str  # token-free repository URL or azure:// resource id
    branch: Optional[str] = None  # git branch; for Azure the logical export label
    subdirectory: Optional[str] = None  # source root within source/ (GitHub only)
    version: str  # commit SHA (GitHub) or local export commit SHA (Azure)
    content_sha256: str  # digest over the normalized ADF artifacts
    retrieved_at: datetime
    azure: Optional[AzureFactoryIdentity] = None
    selection: SourceSelection = Field(default_factory=SourceSelection)
    artifacts: list[SourceArtifact] = Field(default_factory=list)
    failures: list[RetrievalFailure] = Field(default_factory=list)
    redactions: list[RedactionRecord] = Field(default_factory=list)
    findings: list[SourceFinding] = Field(default_factory=list)
    complete: bool = True
