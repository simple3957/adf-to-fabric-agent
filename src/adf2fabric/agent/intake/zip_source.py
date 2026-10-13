"""Intake from an uploaded workspace / repository export (ZIP), in two steps so the user can review before importing:

1. ``inspect``  - validate the archive, extract its JSON into a scratch folder, describe it (artifact counts, missing
                  definitions, unresolved references), then delete the scratch folder. Creates no project.
2. ``run``      - the same extraction, then store the result as the project's source with a manifest.

Nothing in the archive is executed; only ``.json`` entries are copied (see ``connections.providers.zip_source``).
"""

from __future__ import annotations

import hashlib
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.agent.intake import github_source
from adf2fabric.agent.intake.agent import _remove_tree
from adf2fabric.agent.intake.models import IntakeResult
from adf2fabric.connections.providers.zip_source import ExtractReport, ZipLimits, ZipRejected, extract_json
from adf2fabric.connections.source_analysis import SourceSummary, analyze_source
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.source import SourceType
from adf2fabric.project.store import ProjectStore, slugify
from adf2fabric.repository import git

PROVIDER_NAME = "zip-export"
EXPORT_BRANCH = "upload"


class ZipIntakeRequest(BaseModel):
    project_name: str
    zip_path: str  # a file on this machine (the API stores uploads in its own scratch folder)
    original_name: str = "export.zip"
    subdirectory: Optional[str] = None
    pipelines: list[str] = Field(default_factory=list)


def _safe_label(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", Path(name).name)[:80] or "export.zip"


def inspect_zip(zip_path: Path, subdirectory: Optional[str] = None,
                limits: ZipLimits = ZipLimits()) -> tuple[SourceSummary, ExtractReport]:
    """Describe an archive without importing it. Raises :class:`ZipRejected` for an unusable archive."""
    with tempfile.TemporaryDirectory(prefix="a2f") as tmp:
        out = Path(tmp) / "x"
        report = extract_json(zip_path, out, limits)
        return analyze_source(out, subdirectory), report


class ZipSourceIntake:
    def __init__(self, store: ProjectStore, limits: ZipLimits = ZipLimits()):
        self.store = store
        self.limits = limits

    def run(self, request: ZipIntakeRequest) -> IntakeResult:
        staging: Optional[Path] = None
        try:
            project_id = slugify(request.project_name)
            zip_path = Path(request.zip_path)
            digest = hashlib.sha256(zip_path.read_bytes()).hexdigest()
            dest = self.store.source_dir(project_id)
            dest.parent.mkdir(parents=True, exist_ok=True)
            staging = dest.parent / f"source.staging-{uuid.uuid4().hex[:6]}"
            report = extract_json(zip_path, staging, self.limits)
            summary = analyze_source(staging, request.subdirectory)
            if not summary.layout_ok:
                msg = next((f.message for f in summary.findings if f.code == "no_artifacts"), "The archive holds no artifacts.")
                return IntakeResult(success=False, error=msg, message="Intake failed.")
            sub = request.subdirectory or summary.root or None
            git.init_repo(staging, EXPORT_BRANCH)
            label = _safe_label(request.original_name)
            sha = git.commit_all(staging, f"Import of {label} ({datetime.now(timezone.utc).isoformat()})")
            if dest.exists():
                _remove_tree(dest)
            staging.rename(dest)
            staging = None
            identifier = f"zip://{digest[:16]}/{label}"
            metadata = RepositoryMetadata(provider=PROVIDER_NAME, repository_url=identifier, branch=EXPORT_BRANCH,
                                          commit_sha=sha, fetched_at=datetime.now(timezone.utc), local_source_path=str(dest))
            manifest = github_source.build_manifest(project_id, metadata, dest, subdirectory=sub, pipelines=request.pipelines)
            manifest = manifest.model_copy(update={"source_type": SourceType.zip_export, "source_identifier": identifier})
            project = Project(id=project_id, name=request.project_name, repository=metadata)
            self.store.save(project)
            self.store.save_source_manifest(project_id, manifest)
            return IntakeResult(
                success=True, project=project, manifest=manifest,
                message=f"Imported '{label}': {summary.total} artifact(s), {len(summary.pipelines)} pipeline(s)"
                        f" ({report.ignored} non-JSON file(s) ignored)"
                        + ("" if summary.complete else "; the source has errors to review") + ".")
        except ZipRejected as exc:
            return IntakeResult(success=False, error=str(exc), message="Intake failed.")
        except github_source.SourceSelectionError as exc:
            return IntakeResult(success=False, error=str(exc), message="Intake failed.")
        except Exception as exc:  # noqa: BLE001
            return IntakeResult(success=False, error=f"{type(exc).__name__}: {str(exc)[:200]}", message="Intake failed.")
        finally:
            if staging is not None and staging.exists():
                _remove_tree(staging)
