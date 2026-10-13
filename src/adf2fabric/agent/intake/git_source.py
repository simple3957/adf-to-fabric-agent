"""Intake from any HTTPS Git repository (public or private): fetch a branch, tag or commit and record its verified SHA.

Same destination and manifest as the GitHub and Azure intakes, so Discovery, semantic parsing and conversion run unchanged.
The access token is held only for the fetch: it is never written into the project, the manifest, the clone's git config, a log
or an error message.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, SecretStr

from adf2fabric.agent.intake import github_source
from adf2fabric.agent.intake.agent import _remove_tree
from adf2fabric.agent.intake.models import IntakeResult
from adf2fabric.connections.providers.git_source import GitAuth, GitProvider, GitSourceError, clean_url
from adf2fabric.connections.redaction import redact
from adf2fabric.connections.source_analysis import analyze_source
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.source import SourceType
from adf2fabric.project.store import ProjectStore, slugify

PROVIDER_NAME = "git"


class GitIntakeRequest(BaseModel):
    project_name: str
    repository_url: str
    revision: str = Field(default="HEAD", description="Branch, tag or commit SHA; HEAD = the default branch.")
    subdirectory: Optional[str] = None
    pipelines: list[str] = Field(default_factory=list)
    token: Optional[SecretStr] = Field(default=None, description="Access token for a private repository (never persisted).")
    username: Optional[str] = Field(default=None, description="Overrides the host's default token user name.")


class GitSourceIntake:
    def __init__(self, store: ProjectStore, provider: Optional[GitProvider] = None):
        self.store = store
        self.provider = provider or GitProvider()

    def run(self, request: GitIntakeRequest) -> IntakeResult:
        secret = request.token.get_secret_value() if request.token else ""
        staging: Optional[Path] = None
        try:
            project_id = slugify(request.project_name)
            cleaned, _host = clean_url(request.repository_url)
            dest = self.store.source_dir(project_id)
            dest.parent.mkdir(parents=True, exist_ok=True)
            staging = dest.parent / f"source.staging-{uuid.uuid4().hex[:8]}"
            sha = self.provider.fetch(request.repository_url, request.revision, staging,
                                      GitAuth(token=secret or None, username=request.username))
            summary = analyze_source(staging, request.subdirectory)
            sub = request.subdirectory or summary.root or None
            if dest.exists():
                _remove_tree(dest)
            staging.rename(dest)
            staging = None
            metadata = RepositoryMetadata(provider=PROVIDER_NAME, repository_url=cleaned, branch=request.revision,
                                          commit_sha=sha, fetched_at=datetime.now(timezone.utc), local_source_path=str(dest))
            manifest = github_source.build_manifest(project_id, metadata, dest, subdirectory=sub, pipelines=request.pipelines)
            manifest = manifest.model_copy(update={"source_type": SourceType.git})
            project = Project(id=project_id, name=request.project_name, repository=metadata)
            self.store.save(project)
            self.store.save_source_manifest(project_id, manifest)
            return IntakeResult(
                success=True, project=project, manifest=manifest,
                message=f"Fetched '{cleaned}' ({request.revision} @ {sha[:8]}): {summary.total} artifact(s), "
                        f"{len(summary.pipelines)} pipeline(s)" + ("" if summary.complete else "; the source has errors to review")
                        + ".")
        except GitSourceError as exc:
            return IntakeResult(success=False, error=redact(str(exc), secret), message="Intake failed.")
        except Exception as exc:  # noqa: BLE001 - never echo a message that could carry the token
            return IntakeResult(success=False, error=redact(f"{type(exc).__name__}: {exc}", secret), message="Intake failed.")
        finally:
            if staging is not None and staging.exists():
                _remove_tree(staging)
