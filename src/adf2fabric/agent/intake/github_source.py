"""Intake Mode B bookkeeping: describe a fetched GitHub source.

The clone itself is done by the existing :class:`IntakeAgent` / ``RepositoryClient``
and is never modified. This module only INSPECTS the fetched tree (read-only) to
validate the optional subdirectory, resolve the optional pipeline selection, run
the shared integrity checks, and build the :class:`SourceManifest`.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Optional

from adf2fabric.agent.intake import source_validation as sv
from adf2fabric.project.discovery import AdfArtifactKind
from adf2fabric.project.models import RepositoryMetadata
from adf2fabric.project.source import (
    FindingSeverity,
    SourceManifest,
    SourceSelection,
    SourceType,
)


class SourceSelectionError(Exception):
    """The requested subdirectory/pipelines cannot be satisfied (user-facing)."""


def normalize_subdirectory(source_dir: Path, subdirectory: Optional[str]) -> Optional[str]:
    """Validated POSIX subdirectory relative to ``source_dir`` (None for the root)."""
    if not subdirectory or not subdirectory.strip().strip("/\\"):
        return None
    raw = subdirectory.strip().replace("\\", "/").strip("/")
    pure = PurePosixPath(raw)
    if pure.is_absolute() or ".." in pure.parts or ":" in pure.parts[0]:
        raise SourceSelectionError(
            f"Subdirectory '{subdirectory}' must be a relative path inside the repository."
        )
    root = source_dir.resolve()
    target = (source_dir / raw).resolve()
    if root != target and root not in target.parents:
        raise SourceSelectionError(f"Subdirectory '{subdirectory}' escapes the repository.")
    if not target.is_dir():
        raise SourceSelectionError(
            f"Subdirectory '{subdirectory}' does not exist in the fetched branch. "
            "Check the path (it is relative to the repository root) and the branch."
        )
    return pure.as_posix()


def build_manifest(
    project_id: str,
    metadata: RepositoryMetadata,
    source_dir: Path,
    *,
    subdirectory: Optional[str] = None,
    pipelines: Optional[list[str]] = None,
) -> SourceManifest:
    """Inspect the fetched tree and describe it. Raises :class:`SourceSelectionError`."""
    sub = normalize_subdirectory(source_dir, subdirectory)
    root = source_dir / sub if sub else source_dir
    root, loaded, extraction = sv.with_arm_extraction(root, sv.load_source(root))
    if extraction is not None:  # an adf_publish source: from here on the generated Git layout is the source root
        sub = root.relative_to(source_dir).as_posix()

    resolved: list[str] = []
    scope = None
    if pipelines:
        closure = sv.resolve_selection(loaded, pipelines)
        if closure.missing_pipelines:
            available = sorted(
                a.name for a in loaded.artifacts if a.kind == AdfArtifactKind.pipeline
            )
            shown = ", ".join(available[:20]) + (" ..." if len(available) > 20 else "")
            where = f" under '{sub}'" if sub else ""
            raise SourceSelectionError(
                f"Pipeline(s) not found{where} on branch '{metadata.branch}': "
                f"{', '.join(closure.missing_pipelines)}. "
                f"Available pipelines: {shown or '(none)'}."
            )
        resolved = closure.paths
        scope = closure.keys

    findings = sv.extraction_findings(extraction) + sv.validate_source(loaded, scope=scope)
    artifacts = sv.to_source_artifacts(loaded)
    return SourceManifest(
        project_id=project_id,
        source_type=SourceType.github,
        source_identifier=metadata.repository_url,
        branch=metadata.branch,
        subdirectory=sub,
        version=metadata.commit_sha,
        content_sha256=sv.content_digest(artifacts),
        retrieved_at=metadata.fetched_at,
        selection=SourceSelection(pipelines=sorted(pipelines or []), resolved_paths=resolved),
        artifacts=artifacts,
        findings=findings,
        complete=not any(f.severity == FindingSeverity.error for f in findings),
    )
