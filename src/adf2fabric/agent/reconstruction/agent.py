"""The Reconstruction agent.

    load project -> require current ADF Semantic Discovery -> analyse (read-only evidence) ->
    propose (evidence-backed JSON patches) -> write the record, the proposed ADF completion and a report
    -> STOP

It reads ``source/`` only to quote the original JSON; it never writes there. Everything it produces lives
under ``projects/<id>/reconstruction/``:

    reconstruction.yaml              findings, proposals (evidence, rule, confidence...), blockers, patterns
    completed/<same layout>          the proposed ADF completion: original JSON + SAFE proposals only
    adf-semantic.completed.yaml      that completion parsed by the existing semantic parser
    reconstruction-report.md         original / parsed / proposed / generated, kept apart
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone

import yaml

from adf2fabric.agent.discovery.adf_semantic.discoverer import AdfSemanticDiscoverer
from adf2fabric.agent.reconstruction.analyzer import analyze_project
from adf2fabric.agent.reconstruction.completion import applied_proposal_ids, completed_documents
from adf2fabric.agent.reconstruction.index import ProjectEvidence
from adf2fabric.agent.reconstruction.models import ReconstructionRequest, ReconstructionResult
from adf2fabric.agent.reconstruction.report import render_report
from adf2fabric.project.discovery import AdfSemanticStatus
from adf2fabric.project.reconstruction import (
    Completeness,
    ProjectReconstruction,
    ReconstructionOutcome,
    Severity,
)
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify

COMPLETED_SEMANTIC = "adf-semantic.completed.yaml"
_PROVENANCE = {
    "input": "adf_semantic_discovery + repository_discovery + original_json_read_only",
    "inference": "deterministic_evidence_rules",
    "confidence": "explicit_grading_function_no_scores",
    "source": "never_modified",
}


class ReconstructionAgent:
    def __init__(self, store: ProjectStore):
        self.store = store

    def run(self, request: ReconstructionRequest) -> ReconstructionResult:
        try:
            return self._run(request)
        except ProjectNotFoundError as exc:
            return ReconstructionResult(success=False, message="Reconstruction failed.", error=str(exc))

    def _run(self, request: ReconstructionRequest) -> ReconstructionResult:
        pid = slugify(request.project_name)
        fail = lambda msg: ReconstructionResult(success=False, message="Reconstruction failed.", error=msg)  # noqa: E731
        if not self.store.exists(pid):
            return fail(f"No project '{pid}' found. Run `adf2fabric intake` first.")
        if not self.store.has_adf_semantic_discovery(pid):
            return fail("ADF Semantic Discovery has not run. Run `adf2fabric discovery` then "
                        "`adf2fabric discovery-adf-semantic` first; reconstruction analyses that record.")
        d = self.store.load_adf_semantic_discovery(pid)
        project = self.store.load(pid)
        if d.status not in (AdfSemanticStatus.discovered, AdfSemanticStatus.partial):
            return fail(f"The semantic discovery status is '{d.status.value}'; there is nothing to analyse.")
        if d.source_commit_sha != project.repository.commit_sha:
            return fail("The semantic discovery record is stale: it was produced from commit "
                        f"{d.source_commit_sha[:8]} but the project source is at {project.repository.commit_sha[:8]}. "
                        "Re-run `adf2fabric discovery` and `adf2fabric discovery-adf-semantic`.")

        repo = self.store.load_repository_discovery(pid) if self.store.has_repository_discovery(pid) else None
        sql = None
        if self.store.has_sql_discovery(pid):
            try:
                sql = self.store.load_sql_discovery(pid)
            except Exception:  # noqa: BLE001 - optional corroboration only
                sql = None
        ev = ProjectEvidence(d, self.store.source_root(pid), repo, sql)
        out, source_findings, analyses = analyze_project(ev)

        # --- the proposed ADF completion (separate files; the original is untouched) ---------------------------
        self._clear_previous(pid)
        docs = completed_documents(ev, out.proposals)
        written = []
        for path, doc in sorted(docs.items()):
            self.store.write_reconstruction_file(pid, f"completed/{path}", json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
            written.append(f"completed/{path}")
        if docs:
            completed = AdfSemanticDiscoverer(self.store).discover_in_memory(pid, docs)
            if completed is not None:
                self.store.write_reconstruction_file(
                    pid, COMPLETED_SEMANTIC,
                    yaml.safe_dump(json.loads(completed.model_dump_json()), sort_keys=False))
                written.append(COMPLETED_SEMANTIC)

        record = self._record(pid, d.source_commit_sha, out, source_findings, analyses, written)
        self.store.save_reconstruction(pid, record)
        self.store.write_reconstruction_file(pid, "reconstruction-report.md", render_report(record))
        msg = (f"Analysed {len(analyses)} pipeline(s): "
               + ", ".join(f"{sum(1 for a in analyses if a.completeness is c)} {c.value}" for c in Completeness
                           if any(a.completeness is c for a in analyses))
               + f"; {len(out.proposals)} proposal(s) ({record.safe_proposal_count} safe to generate), "
               f"{len(out.blockers)} blocker(s).")
        return ReconstructionResult(success=True, reconstruction=record, message=msg)

    def _clear_previous(self, pid: str) -> None:
        root = self.store.reconstruction_dir(pid)
        done = root / "completed"
        if done.exists():
            shutil.rmtree(done)
        stale = root / COMPLETED_SEMANTIC
        if stale.exists():
            stale.unlink()

    def _record(self, pid, sha, out, source_findings, analyses, written) -> ProjectReconstruction:
        now = datetime.now(timezone.utc)
        counts: dict[str, int] = {}
        for f in [*out.findings]:
            counts[f.incompleteness.value] = counts.get(f.incompleteness.value, 0) + 1
        pcounts: dict[str, int] = {}
        for p in out.proposals:
            pcounts[p.confidence.value] = pcounts.get(p.confidence.value, 0) + 1
        gaps = [a for a in analyses if a.completeness is not Completeness.complete]
        if not gaps and not any(f.severity is not Severity.info for f in source_findings):
            outcome = ReconstructionOutcome.complete
        elif all(a.completeness in (Completeness.complete, Completeness.reconstructable) for a in analyses) \
                and not any(f.severity is Severity.error for f in source_findings):
            outcome = ReconstructionOutcome.reconstructed
        else:
            outcome = ReconstructionOutcome.incomplete
        rec = ProjectReconstruction(
            project_id=pid, source_commit_sha=sha, status=outcome, created_at=now, updated_at=now,
            pipelines=analyses, source_findings=source_findings,
            source_blockers=[b for b in out.blockers if b.pipeline is None], finding_counts=dict(sorted(counts.items())),
            proposal_counts=dict(sorted(pcounts.items())), safe_proposal_count=len(applied_proposal_ids(out.proposals)),
            blocker_count=len(out.blockers), completed_files=written,
            representations={
                "original": "source/ - the ADF JSON as authored",
                "parsed": "discovery/adf-semantic.yaml - the parsed source",
                "proposed": "reconstruction/reconstruction.yaml and reconstruction/completed/ - inferred, evidence-backed",
                "generated": "conversion/ - Fabric output (run `adf2fabric convert --apply-reconstruction`)",
            },
            provenance=dict(_PROVENANCE))
        if self.store.has_reconstruction(pid):
            try:
                existing = self.store.load_reconstruction(pid)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                rec.created_at = existing.created_at
                if _semantic(existing) == _semantic(rec):
                    rec.updated_at = existing.updated_at
        return rec


def _semantic(r: ProjectReconstruction) -> dict:
    data = json.loads(r.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data
