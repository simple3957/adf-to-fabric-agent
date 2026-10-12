"""The Repository Discoverer — the Repository domain of the Discovery phase.

Responsibility (and hard boundary):

    load project -> verify source commit integrity -> scan source read-only
    -> extract ADF artifact candidates -> assemble + persist -> STOP

This is a DOMAIN discoverer (invoked by the phase-level ``DiscoveryAgent``), not
the phase orchestrator. Filesystem traversal lives in :mod:`.scanner`, hashing in
:mod:`.hashing`, ADF candidate detection + structural extraction in
:mod:`.artifacts`; the persisted data contract lives in ``project.discovery``.

Discovery reports WHAT EXISTS. It performs NO migration interpretation, is
deterministic and fully offline — no Azure / SQL / ADLS / Fabric access, no
network — and it NEVER executes any repository file. The source repository is
treated as immutable; nothing under ``source/`` is modified. The only subprocess
used is ``git rev-parse HEAD`` (read-only) to bind the inventory to the Intake
commit SHA.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from adf2fabric.agent.discovery.models import DiscoveryRequest, DiscoveryResult
from adf2fabric.agent.discovery.repository.artifacts import extract_candidates
from adf2fabric.agent.discovery.repository.scanner import scan_repository
from adf2fabric.project.discovery import (
    DiscoveryStatus,
    RepositoryDiscovery,
    RepositorySource,
)
from adf2fabric.project.models import Project
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify
from adf2fabric.repository import git

_PROVENANCE = {
    "source": "intake_metadata",
    "source_commit_sha": "verified_git_head",
    "structure": "read_only_filesystem_scan",
    "files": "read_only_filesystem_scan",
    "adf_candidates": "deterministic_structural_evidence",
}


class DiscoveryError(Exception):
    """A clear, user-facing hard-stop condition for Repository Discovery."""


class RepositoryDiscoverer:
    def __init__(self, store: ProjectStore):
        self.store = store

    def run(self, request: DiscoveryRequest) -> DiscoveryResult:
        try:
            project_id = slugify(request.project_name)
            if not self.store.exists(project_id):
                return DiscoveryResult(
                    success=False,
                    message="Repository Discovery failed.",
                    error=(
                        f"No project '{project_id}' found. Run `adf2fabric intake` "
                        "first to establish the source repository."
                    ),
                )
            project = self.store.load(project_id)
            source_dir = self.store.source_dir(project_id)

            self._verify_source(source_dir, project)

            # Git integrity is checked on source/; the scan honors the source root
            # Intake recorded (a repository subdirectory), otherwise source/ itself.
            scan_root = self.store.source_root(project_id)
            structure, files = scan_repository(scan_root)
            candidates = extract_candidates(scan_root, files)
            discovery = self._assemble(project_id, project, structure, files, candidates)
            self.store.save_repository_discovery(project_id, discovery)

            state = "COMPLETE" if discovery.complete else "INCOMPLETE"
            return DiscoveryResult(
                success=True,
                discovery=discovery,
                message=(
                    f"Repository Discovery {state} for '{project_id}' "
                    f"@ {discovery.source_commit_sha[:8]}."
                ),
            )
        except (DiscoveryError, ProjectNotFoundError) as exc:
            return DiscoveryResult(
                success=False, message="Repository Discovery failed.", error=str(exc)
            )

    # --- source integrity (hard stop) --------------------------------------

    def _verify_source(self, source_dir: Path, project: Project) -> None:
        if not source_dir.exists() or not source_dir.is_dir():
            raise DiscoveryError(
                f"Source directory not found: {source_dir}. "
                "Run `adf2fabric intake` to acquire the repository."
            )
        if not git.is_git_repo(source_dir):
            raise DiscoveryError(
                f"Source at {source_dir} is not a git repository; cannot verify the "
                "commit SHA recorded by Intake. Discovery requires a verifiable "
                "source commit and will not proceed."
            )
        persisted = project.repository.commit_sha
        actual = git.head_sha(source_dir)
        if actual != persisted:
            raise DiscoveryError(
                "Source commit mismatch — the working tree HEAD does not match the "
                "commit recorded by Intake. Discovery will not silently refresh, "
                "fetch, or checkout.\n"
                f"  persisted (Intake) commit_sha: {persisted}\n"
                f"  actual working-tree HEAD:      {actual}"
            )

    # --- assembly + persistence --------------------------------------------

    def _assemble(
        self, project_id, project, structure, files, candidates
    ) -> RepositoryDiscovery:
        now = datetime.now(timezone.utc)
        skipped = sum(1 for f in files if f.status == DiscoveryStatus.skipped)
        failed = sum(1 for f in files if f.status == DiscoveryStatus.failed)

        kind_counts: dict[str, int] = {}
        for c in candidates:
            kind_counts[c.artifact_kind.value] = (
                kind_counts.get(c.artifact_kind.value, 0) + 1
            )

        meta = project.repository
        discovery = RepositoryDiscovery(
            project_id=project_id,
            source_commit_sha=meta.commit_sha,
            created_at=now,
            updated_at=now,
            source=RepositorySource(
                provider=meta.provider,
                repository_url=meta.repository_url,
                branch=meta.branch,
                commit_sha=meta.commit_sha,
                local_source_path=meta.local_source_path,
            ),
            structure=structure,
            files=files,
            adf_candidates=candidates,
            artifact_kind_counts={k: kind_counts[k] for k in sorted(kind_counts)},
            complete=(skipped == 0 and failed == 0),
            skipped_count=skipped,
            failed_count=failed,
            provenance=dict(_PROVENANCE),
        )

        # Timestamp semantics: created_at is set once; updated_at bumps only when
        # the semantic inventory changes (timestamps excluded from comparison).
        if self.store.has_repository_discovery(project_id):
            try:
                existing = self.store.load_repository_discovery(project_id)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                discovery.created_at = existing.created_at
                if _semantic(existing) == _semantic(discovery):
                    discovery.updated_at = existing.updated_at
                else:
                    discovery.updated_at = now
        return discovery


def _semantic(discovery: RepositoryDiscovery) -> dict:
    """Inventory content used to detect meaningful change (excludes timestamps)."""
    data = json.loads(discovery.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data
