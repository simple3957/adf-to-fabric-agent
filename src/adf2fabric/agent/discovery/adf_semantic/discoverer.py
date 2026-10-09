"""The ADF Semantic Discoverer — deep, repository-based ADF discovery (Phase 1).

Responsibility (and hard boundary):

    load project -> require Repository Discovery -> for each ADF artifact candidate
    read its immutable source JSON -> secret-safe structural parse -> derive safe
    external references + typed relationship edges + deterministic resolution
    -> persist discovery/adf-semantic.yaml -> STOP

It reads ONLY from ``projects/<id>/source/`` and never mutates it. No Azure/live
calls, no SQL/ADLS data access. It reuses the Repository Discovery candidate list
(agent -> project) rather than re-detecting artifacts, and reuses the shared
``AdfArtifactKind``. Connection strings and credential values are never read (see
:mod:`.parser`). Repository/ADLS/SQL/ADF-live records are never modified.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from adf2fabric.agent.discovery.adf_semantic import references
from adf2fabric.agent.discovery.adf_semantic.models import (
    AdfSemanticDiscoveryRequest,
    AdfSemanticDiscoveryResult,
)
from adf2fabric.agent.discovery.adf_semantic import parser
from adf2fabric.project.discovery import (
    AdfArtifactKind,
    AdfSemanticDiscovery,
    AdfSemanticStatus,
)
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify

_PROVENANCE = {
    "source": "repository_source_immutable",
    "candidates": "repository_discovery",
    "extraction": "positive_allowlist",
    "secrets": "never_extracted",
}

# The artifact kinds Semantic Discovery Phase 1 parses (from the shared enum).
_RELEVANT_KINDS = {
    AdfArtifactKind.pipeline,
    AdfArtifactKind.dataset,
    AdfArtifactKind.linked_service,
    AdfArtifactKind.trigger,
    AdfArtifactKind.factory,
}


class AdfSemanticDiscoverer:
    """ADF semantic domain discoverer, invoked by the phase-level ``DiscoveryAgent``."""

    def __init__(self, store: ProjectStore):
        self.store = store

    def run(self, request: AdfSemanticDiscoveryRequest) -> AdfSemanticDiscoveryResult:
        try:
            project_id = slugify(request.project_name)
            if not self.store.exists(project_id):
                return AdfSemanticDiscoveryResult(
                    success=False,
                    message="ADF Semantic Discovery failed.",
                    error=(
                        f"No project '{project_id}' found. Run `adf2fabric intake` "
                        "first to establish the project."
                    ),
                )

            if not self.store.has_repository_discovery(project_id):
                return self._persist(
                    project_id, "", AdfSemanticStatus.missing_repository_discovery,
                    detail=(
                        "Repository Discovery has not run. Run `adf2fabric discovery` "
                        "first to inventory the source artifacts."
                    ),
                )

            repo = self.store.load_repository_discovery(project_id)
            source_dir = self.store.source_dir(project_id)

            candidates = [
                c for c in repo.adf_candidates if c.artifact_kind in _RELEVANT_KINDS
            ]
            if not candidates:
                return self._persist(
                    project_id, repo.source_commit_sha, AdfSemanticStatus.no_artifacts,
                    detail="Repository Discovery found no ADF artifacts to parse.",
                )

            pipelines, datasets, linked_services, triggers, factory = [], [], [], [], []
            parsed = skipped = failed = 0
            failures: list[str] = []

            for cand in sorted(candidates, key=lambda c: c.path):
                # Only parse candidates the repository read cleanly.
                if getattr(cand.parse_status, "value", cand.parse_status) != "discovered":
                    skipped += 1
                    continue
                data = self._load_json(source_dir / cand.path)
                if data is None or not isinstance(data, dict):
                    failed += 1
                    failures.append(cand.path)
                    continue
                try:
                    self._dispatch(
                        cand.artifact_kind, data, cand.path,
                        pipelines, datasets, linked_services, triggers, factory,
                    )
                    parsed += 1
                except Exception:  # noqa: BLE001 - a single bad artifact never aborts the run
                    failed += 1
                    failures.append(cand.path)

            pipelines.sort(key=lambda a: a.name)
            datasets.sort(key=lambda a: a.name)
            linked_services.sort(key=lambda a: a.name)
            triggers.sort(key=lambda a: a.name)
            factory.sort(key=lambda a: a.name)

            external_references = references.build_external_references(
                pipelines, datasets, linked_services
            )
            relationships, unresolved = references.build_relationships(
                pipelines, datasets, linked_services, triggers, external_references
            )

            status = AdfSemanticStatus.partial if failed else AdfSemanticStatus.discovered
            detail = (
                f"{failed} artifact(s) could not be parsed: {', '.join(sorted(failures))}"
                if failed else ""
            )
            return self._persist(
                project_id, repo.source_commit_sha, status, detail=detail,
                pipelines=pipelines, datasets=datasets, linked_services=linked_services,
                triggers=triggers, factory=factory,
                external_references=external_references, relationships=relationships,
                unresolved=unresolved, parsed=parsed, skipped=skipped, failed=failed,
            )
        except ProjectNotFoundError as exc:
            return AdfSemanticDiscoveryResult(
                success=False, message="ADF Semantic Discovery failed.", error=str(exc)
            )

    # --- helpers ------------------------------------------------------------

    @staticmethod
    def _load_json(path: Path):
        try:
            with path.open(encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            return None

    @staticmethod
    def _dispatch(kind, data, path, pipelines, datasets, linked_services, triggers, factory):
        if kind == AdfArtifactKind.pipeline:
            pipelines.append(parser.parse_pipeline(data, path))
        elif kind == AdfArtifactKind.dataset:
            datasets.append(parser.parse_dataset(data, path))
        elif kind == AdfArtifactKind.linked_service:
            linked_services.append(parser.parse_linked_service(data, path))
        elif kind == AdfArtifactKind.trigger:
            triggers.append(parser.parse_trigger(data, path))
        elif kind == AdfArtifactKind.factory:
            factory.append(parser.parse_factory(data, path))

    # --- assembly + persistence --------------------------------------------

    def _persist(
        self, project_id, source_commit_sha, status, *, detail,
        pipelines=None, datasets=None, linked_services=None, triggers=None, factory=None,
        external_references=None, relationships=None, unresolved=None,
        parsed=0, skipped=0, failed=0,
    ) -> AdfSemanticDiscoveryResult:
        discovery = self._assemble(
            project_id, source_commit_sha, status, detail,
            pipelines or [], datasets or [], linked_services or [], triggers or [],
            factory or [], external_references or [], relationships or [], unresolved or [],
            parsed, skipped, failed,
        )
        self.store.save_adf_semantic_discovery(project_id, discovery)

        # discovered / no_artifacts / partial are valid outcomes (partial = some
        # artifacts unparsed but the run completed). Only the prerequisite/hard
        # failures are surfaced as success=False.
        if status in (
            AdfSemanticStatus.discovered,
            AdfSemanticStatus.no_artifacts,
            AdfSemanticStatus.partial,
        ):
            msg = (
                f"ADF Semantic Discovery complete for '{project_id}': "
                f"{discovery.pipeline_count} pipeline(s), {discovery.dataset_count} dataset(s), "
                f"{discovery.linked_service_count} linked service(s), "
                f"{discovery.trigger_count} trigger(s)."
            )
            if status == AdfSemanticStatus.partial:
                msg += f" ({failed} unparsed)"
            return AdfSemanticDiscoveryResult(success=True, discovery=discovery, message=msg)
        return AdfSemanticDiscoveryResult(
            success=False, discovery=discovery,
            message="ADF Semantic Discovery failed.",
            error=f"{status.value}: {detail}" if detail else status.value,
        )

    def _assemble(
        self, project_id, source_commit_sha, status, detail,
        pipelines, datasets, linked_services, triggers, factory,
        external_references, relationships, unresolved, parsed, skipped, failed,
    ) -> AdfSemanticDiscovery:
        now = datetime.now(timezone.utc)
        discovery = AdfSemanticDiscovery(
            project_id=project_id,
            source_commit_sha=source_commit_sha,
            status=status,
            created_at=now,
            updated_at=now,
            pipeline_count=len(pipelines),
            dataset_count=len(datasets),
            linked_service_count=len(linked_services),
            trigger_count=len(triggers),
            factory_count=len(factory),
            pipelines=pipelines,
            datasets=datasets,
            linked_services=linked_services,
            triggers=triggers,
            factory=factory,
            external_references=external_references,
            relationships=relationships,
            unresolved_references=unresolved,
            parsed_count=parsed,
            skipped_count=skipped,
            failed_count=failed,
            detail=detail,
            provenance=dict(_PROVENANCE),
        )
        if self.store.has_adf_semantic_discovery(project_id):
            try:
                existing = self.store.load_adf_semantic_discovery(project_id)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                discovery.created_at = existing.created_at
                if _semantic(existing) == _semantic(discovery):
                    discovery.updated_at = existing.updated_at
                else:
                    discovery.updated_at = now
        return discovery


def _semantic(discovery: AdfSemanticDiscovery) -> dict:
    """Discovery content used to detect meaningful change (excludes timestamps)."""
    data = json.loads(discovery.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data
