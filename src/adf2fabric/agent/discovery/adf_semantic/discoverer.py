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
from typing import Optional

from adf2fabric.agent.discovery.adf_semantic import references
from adf2fabric.agent.discovery.adf_semantic.models import (
    AdfSemanticDiscoveryRequest,
    AdfSemanticDiscoveryResult,
)
from adf2fabric.agent.discovery.adf_semantic import parser, parser_dataflows, parser_pipelines
from adf2fabric.project.discovery import (
    AdfArtifactKind,
    AdfSemanticDiscovery,
    AdfSemanticStatus,
    AdfUnprocessedArtifact,
    UNPROCESSED_LOSS_REASONS as _LOSS_REASONS,
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
    AdfArtifactKind.integration_runtime,
    AdfArtifactKind.change_data_capture,
    AdfArtifactKind.dataflow,
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
            source_dir = self.store.source_root(project_id)

            # Honor the pipeline selection Intake recorded (pipelines + what they
            # reference). No selection recorded means every artifact.
            selected = self._selected_paths(project_id)
            candidates, ignored = self._split_candidates(repo, selected)
            if not candidates:
                return self._persist(
                    project_id, repo.source_commit_sha, AdfSemanticStatus.no_artifacts,
                    detail="Repository Discovery found no ADF artifacts to parse.",
                    unprocessed=[self._ignored_entry(c) for c in ignored],
                )

            parsed_set = self._parse_all(candidates, source_dir, ignored=ignored)
            pipelines, datasets, linked_services = (
                parsed_set["pipelines"], parsed_set["datasets"], parsed_set["linked_services"])
            triggers, factory = parsed_set["triggers"], parsed_set["factory"]
            parsed, skipped, failed = parsed_set["parsed"], parsed_set["skipped"], parsed_set["failed"]
            external_references = parsed_set["external_references"]
            relationships, unresolved = parsed_set["relationships"], parsed_set["unresolved"]
            unprocessed = parsed_set["unprocessed"]

            lost = [u for u in unprocessed if u.reason in _LOSS_REASONS]
            status = AdfSemanticStatus.partial if (failed or lost) else AdfSemanticStatus.discovered
            detail = self._detail(lost)
            return self._persist(
                project_id, repo.source_commit_sha, status, detail=detail,
                pipelines=pipelines, datasets=datasets, linked_services=linked_services,
                triggers=triggers, factory=factory,
                external_references=external_references, relationships=relationships,
                unresolved=unresolved, parsed=parsed, skipped=skipped, failed=failed,
                cdcs=parsed_set["cdcs"], runtimes=parsed_set["runtimes"],
                dataflows=parsed_set["dataflows"], unprocessed=unprocessed,
            )
        except ProjectNotFoundError as exc:
            return AdfSemanticDiscoveryResult(
                success=False, message="ADF Semantic Discovery failed.", error=str(exc)
            )

    def discover_in_memory(
        self, project_id: str, overrides: "dict[str, dict] | None" = None
    ) -> "AdfSemanticDiscovery | None":
        """The same parse as :meth:`run`, WITHOUT persisting anything.

        ``overrides`` maps a repository-relative artifact path to JSON that replaces that file's
        content for this parse only (used to parse a proposed completion). The source on disk is
        read-only and never modified. Returns ``None`` when there is nothing to parse.
        """
        if not self.store.has_repository_discovery(project_id):
            return None
        repo = self.store.load_repository_discovery(project_id)
        source_dir = self.store.source_root(project_id)
        selected = self._selected_paths(project_id)
        candidates, ignored = self._split_candidates(repo, selected)
        if not candidates:
            return None
        ps = self._parse_all(candidates, source_dir, overrides, ignored=ignored)
        lost = [u for u in ps["unprocessed"] if u.reason in _LOSS_REASONS]
        status = AdfSemanticStatus.partial if (ps["failed"] or lost) else AdfSemanticStatus.discovered
        return self._assemble(
            project_id, repo.source_commit_sha, status, self._detail(lost),
            ps["pipelines"], ps["datasets"], ps["linked_services"], ps["triggers"], ps["factory"],
            ps["external_references"], ps["relationships"], ps["unresolved"],
            ps["parsed"], ps["skipped"], ps["failed"], ps["cdcs"], ps["runtimes"],
            dataflows=ps["dataflows"], unprocessed=ps["unprocessed"],
        )

    @staticmethod
    def _split_candidates(repo, selected: "set[str] | None"):
        """(candidates this phase parses, candidates of other kinds). A selection recorded at intake narrows both."""
        everything = list(repo.adf_candidates)
        if selected is not None:
            everything = [c for c in everything if c.path in selected]
        return ([c for c in everything if c.artifact_kind in _RELEVANT_KINDS],
                [c for c in everything if c.artifact_kind not in _RELEVANT_KINDS])

    @staticmethod
    def _ignored_entry(cand) -> AdfUnprocessedArtifact:
        kind = getattr(cand.artifact_kind, "value", cand.artifact_kind)
        return AdfUnprocessedArtifact(
            path=cand.path, kind=str(kind), name=cand.name, reason="kind_not_parsed",
            detail=f"Seen in the source; '{kind}' artifacts are not interpreted by semantic discovery.")

    @staticmethod
    def _detail(lost: "list[AdfUnprocessedArtifact]") -> str:
        if not lost:
            return ""
        shown = ", ".join(f"{u.path} ({u.reason})" for u in sorted(lost, key=lambda u: u.path)[:10])
        more = f" and {len(lost) - 10} more" if len(lost) > 10 else ""
        return f"{len(lost)} artifact(s) could not be processed and are not in the record: {shown}{more}"

    def _parse_all(self, candidates, source_dir: Path, overrides: "dict[str, dict] | None" = None,
                   ignored=()) -> dict:
        pipelines, datasets, linked_services, triggers, factory = [], [], [], [], []
        dataflows: list = []
        unprocessed: list[AdfUnprocessedArtifact] = [self._ignored_entry(c) for c in ignored]

        def lose(cand, reason: str, detail: str) -> None:
            kind = getattr(cand.artifact_kind, "value", cand.artifact_kind)
            unprocessed.append(AdfUnprocessedArtifact(path=cand.path, kind=str(kind), name=cand.name,
                                                      reason=reason, detail=detail))

        runtime_kinds: dict[str, Optional[str]] = {}  # runtime name (lower) -> kind
        cdcs: list = []
        runtimes: list = []
        parsed = skipped = failed = 0
        failures: list[str] = []
        for cand in sorted(candidates, key=lambda c: c.path):
            # Only parse candidates the repository read cleanly; the others are recorded with the reason.
            status = getattr(cand.parse_status, "value", cand.parse_status)
            if status != "discovered":
                skipped += 1
                note = cand.note or f"Repository discovery status: {status}."
                reason = ("malformed_json" if "JSONDecodeError" in note or "UnicodeDecodeError" in note
                          else "unreadable" if "unparseable JSON (" in note else "skipped")
                lose(cand, reason, note)
                continue
            if overrides and cand.path in overrides:
                data, reason, detail = overrides[cand.path], "", ""
            else:
                data, reason, detail = self._read(source_dir / cand.path)
            if data is None or not isinstance(data, dict):
                failed += 1
                failures.append(cand.path)
                lose(cand, reason or "not_a_json_object", detail or "Top-level JSON is not an object.")
                continue
            if cand.artifact_kind == AdfArtifactKind.change_data_capture:
                try:
                    cdcs.append(parser.parse_change_data_capture(data, cand.path))
                    parsed += 1
                except Exception as exc:  # noqa: BLE001 - one bad artifact never aborts the run
                    failed += 1
                    failures.append(cand.path)
                    lose(cand, "parse_error", f"The parser raised {type(exc).__name__}.")
                continue
            if cand.artifact_kind == AdfArtifactKind.dataflow:
                try:
                    dataflows.append(parser_dataflows.parse_dataflow(data, cand.path))
                    parsed += 1
                except Exception as exc:  # noqa: BLE001 - parse_dataflow never raises; this is the last guard
                    failed += 1
                    failures.append(cand.path)
                    lose(cand, "parse_error", f"The parser raised {type(exc).__name__}.")
                continue
            if cand.artifact_kind == AdfArtifactKind.integration_runtime:
                name = str(data.get("name") or "").lower()
                if name:
                    runtime_kinds[name] = parser.classify_integration_runtime(data)
                try:
                    runtimes.append(parser.parse_integration_runtime(data, cand.path))
                except Exception as exc:  # noqa: BLE001 - classification above still stands
                    lose(cand, "parse_error", f"The parser raised {type(exc).__name__}; only its kind was recorded.")
                parsed += 1
                continue
            try:
                self._dispatch(
                    cand.artifact_kind, data, cand.path,
                    pipelines, datasets, linked_services, triggers, factory,
                )
                parsed += 1
            except Exception as exc:  # noqa: BLE001 - a single bad artifact never aborts the run
                failed += 1
                failures.append(cand.path)
                lose(cand, "parse_error", f"The parser raised {type(exc).__name__}.")

        dataflows.sort(key=lambda a: a.name)
        unprocessed.sort(key=lambda u: (u.path, u.reason))
        pipelines.sort(key=lambda a: a.name)
        datasets.sort(key=lambda a: a.name)
        linked_services.sort(key=lambda a: a.name)
        triggers.sort(key=lambda a: a.name)
        factory.sort(key=lambda a: a.name)
        cdcs.sort(key=lambda a: a.name)
        runtimes.sort(key=lambda a: a.name)

        for ls in linked_services:
            if ls.integration_runtime:
                ls.integration_runtime_kind = runtime_kinds.get(ls.integration_runtime.lower())

        # Stage 7 of the pipeline parser: resolve datasets / linked services /
        # child pipelines now that every artifact has been parsed.
        parser_pipelines.resolve_pipelines(pipelines, datasets, linked_services, dataflows)

        external_references = references.build_external_references(pipelines, datasets, linked_services)
        relationships, unresolved = references.build_relationships(
            pipelines, datasets, linked_services, triggers, external_references, dataflows
        )
        return dict(
            pipelines=pipelines, datasets=datasets, linked_services=linked_services, triggers=triggers,
            factory=factory, external_references=external_references, relationships=relationships,
            unresolved=unresolved, parsed=parsed, skipped=skipped, failed=failed, failures=failures,
            cdcs=cdcs, runtimes=runtimes, dataflows=dataflows, unprocessed=unprocessed,
        )

    # --- helpers ------------------------------------------------------------

    def _selected_paths(self, project_id: str) -> "set[str] | None":
        if not self.store.has_source_manifest(project_id):
            return None
        try:
            paths = self.store.load_source_manifest(project_id).selection.resolved_paths
        except Exception:  # noqa: BLE001 - an unreadable manifest never hides artifacts
            return None
        return set(paths) if paths else None

    @staticmethod
    def _read(path: Path):
        """(data, reason, detail): the JSON object, or why there is none. Detail never carries file content."""
        try:
            with path.open(encoding="utf-8") as fh:
                data = json.load(fh)
        except json.JSONDecodeError:
            return None, "malformed_json", "Not valid JSON (JSONDecodeError)."
        except UnicodeDecodeError:
            return None, "malformed_json", "Not valid UTF-8 text (UnicodeDecodeError)."
        except OSError as exc:
            return None, "unreadable", f"The file could not be read ({type(exc).__name__})."
        if not isinstance(data, dict):
            return None, "not_a_json_object", "Top-level JSON is not an object."
        return data, "", ""

    @staticmethod
    def _dispatch(kind, data, path, pipelines, datasets, linked_services, triggers, factory):
        if kind == AdfArtifactKind.pipeline:
            pipelines.append(parser_pipelines.parse_pipeline(data, path))
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
        parsed=0, skipped=0, failed=0, cdcs=None, runtimes=None, dataflows=None, unprocessed=None,
    ) -> AdfSemanticDiscoveryResult:
        discovery = self._assemble(
            project_id, source_commit_sha, status, detail,
            pipelines or [], datasets or [], linked_services or [], triggers or [],
            factory or [], external_references or [], relationships or [], unresolved or [],
            parsed, skipped, failed, cdcs or [], runtimes or [],
            dataflows=dataflows or [], unprocessed=unprocessed or [],
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
        external_references, relationships, unresolved, parsed, skipped, failed, cdcs=(), runtimes=(),
        dataflows=(), unprocessed=(),
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
            activity_count=sum(p.activity_total for p in pipelines),
            activity_type_counts=_sum_counts(pipelines),
            unknown_activity_types=sorted({t for p in pipelines for t in p.unknown_activity_types}),
            parse_issue_count=sum(
                len(p.issues) + sum(len(a.warnings) for a in _all_activities(p.activities))
                for p in pipelines),
            pipelines=pipelines,
            datasets=datasets,
            linked_services=linked_services,
            triggers=triggers,
            factory=factory,
            change_data_captures=list(cdcs),
            integration_runtimes=list(runtimes),
            dataflows=list(dataflows),
            dataflow_count=len(dataflows),
            unprocessed_artifacts=list(unprocessed),
            inventory_mismatch_count=sum(1 for p in pipelines if p.inventory is not None and not p.inventory.matched),
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


def _all_activities(activities):
    for a in activities:
        yield a
        yield from _all_activities(a.activities)


def _sum_counts(pipelines) -> dict:
    total: dict = {}
    for p in pipelines:
        for k, v in p.activity_type_counts.items():
            total[k] = total.get(k, 0) + v
    return {k: total[k] for k in sorted(total)}


def _semantic(discovery: AdfSemanticDiscovery) -> dict:
    """Discovery content used to detect meaningful change (excludes timestamps)."""
    data = json.loads(discovery.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data
