"""The Conversion agent.

Responsibility (and hard boundary):

    load project -> require current ADF Semantic Discovery -> build the conversion context
    -> convert each pipeline through the registry -> validate -> write Fabric pipeline content,
    the conversion record, a report and a resolutions template -> STOP

It consumes ONLY the persisted semantic representation (``discovery/adf-semantic.yaml``). It never
reads or modifies ``source/``, never calls Azure or Fabric, and uses no model or inference — every
output is a fixed function of the semantic record and the optional resolutions file. Generated
files live under ``projects/<id>/conversion/``.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from adf2fabric.agent.conversion import compatibility as compat
from adf2fabric.agent.conversion import preflight
from adf2fabric.agent.conversion.compute_plan import ComputeSources, build_compute_plan, load_sources
from adf2fabric.agent.conversion.connectors import coverage as connector_coverage
from adf2fabric.agent.conversion.context import ProjectContext, Resolutions
from adf2fabric.agent.conversion.engine import ConversionEngine
from adf2fabric.agent.conversion.fabric_schema import INFERRED
from adf2fabric.agent.conversion.models import ConversionRequest, ConversionResult
from adf2fabric.agent.conversion.report import render_report, render_resolutions_template
from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.project.conversion import (
    ConversionOutcome,
    ConversionStatus,
    Prerequisite,
    ProjectConversion,
)
from adf2fabric.project.discovery import UNPROCESSED_LOSS_REASONS, AdfSemanticDiscovery, AdfSemanticStatus
from adf2fabric.project.reconstruction import Proposal
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify

_PROVENANCE = {
    "input": "adf_semantic_discovery",
    "conversion": "deterministic_registry_rules",
    "schema": "encoded_from_microsoft_fabric_documentation",
    "source": "never_read_or_modified",
}


def build_project_context(d: AdfSemanticDiscovery, resolutions: Resolutions) -> ProjectContext:
    pipelines = {p.name.lower(): p for p in d.pipelines}
    direct: dict[str, dict[str, str]] = {}
    for p in d.pipelines:
        names: dict[str, str] = {}
        for a in walk(p.activities):
            for g in a.global_parameters:
                names.setdefault(g.lower(), g)
        direct[p.name.lower()] = names
    changed = True
    while changed:  # a pipeline needs every global parameter any pipeline it invokes needs
        changed = False
        for p in d.pipelines:
            mine = direct[p.name.lower()]
            for child in p.pipeline_dependencies:
                for low, orig in direct.get(child.lower(), {}).items():
                    if low not in mine:
                        mine[low] = orig
                        changed = True
    globals_needed = {k: sorted(v.values(), key=str.lower) for k, v in direct.items()}
    gp = {g.name.lower(): g for f in d.factory for g in f.global_parameters}
    return ProjectContext(
        datasets={x.name.lower(): x for x in d.datasets},
        linked_services={x.name.lower(): x for x in d.linked_services},
        pipelines=pipelines, global_params=gp, triggers=list(d.triggers), resolutions=resolutions,
        ir_kinds={x.integration_runtime.lower(): x.integration_runtime_kind
                  for x in d.linked_services if x.integration_runtime},
        globals_needed=globals_needed)


class ConversionAgent:
    def __init__(self, store: ProjectStore):
        self.store = store

    def run(self, request: ConversionRequest) -> ConversionResult:
        try:
            return self._run(request)
        except ProjectNotFoundError as exc:
            return ConversionResult(success=False, message="Conversion failed.", error=str(exc))

    def _run(self, request: ConversionRequest) -> ConversionResult:
        pid = slugify(request.project_name)
        if not self.store.exists(pid):
            return ConversionResult(success=False, message="Conversion failed.",
                                    error=f"No project '{pid}' found. Run `adf2fabric intake` first.")
        if not self.store.has_adf_semantic_discovery(pid):
            return ConversionResult(
                success=False, message="Conversion failed.",
                error="ADF Semantic Discovery has not run. Run `adf2fabric discovery` then "
                      "`adf2fabric discovery-adf-semantic` first; conversion reads only that record.")
        d = self.store.load_adf_semantic_discovery(pid)
        project = self.store.load(pid)
        if d.status not in (AdfSemanticStatus.discovered, AdfSemanticStatus.partial):
            return ConversionResult(success=False, message="Conversion failed.",
                                    error=f"The semantic discovery status is '{d.status.value}'; nothing to convert.")
        if d.source_commit_sha != project.repository.commit_sha:
            return ConversionResult(
                success=False, message="Conversion failed.",
                error="The semantic discovery record is stale: it was produced from commit "
                      f"{d.source_commit_sha[:8]} but the project source is at {project.repository.commit_sha[:8]}. "
                      "Re-run `adf2fabric discovery` and `adf2fabric discovery-adf-semantic`.")

        applied: list[Proposal] = []
        open_blockers: list[str] = []
        if request.apply_reconstruction:
            prepared = self._with_reconstruction(pid, d, project.repository.commit_sha)
            if isinstance(prepared, str):
                return ConversionResult(success=False, message="Conversion failed.", error=prepared)
            d, applied, open_blockers = prepared

        resolutions = Resolutions.load(Path(request.resolutions_path) if request.resolutions_path else None)
        ctx = build_project_context(d, resolutions)
        if self.store.has_workload_manifest(project.id):
            try:
                wm = self.store.load_workload_manifest(project.id)
                ctx.workloads = {w.source_item.lower(): w for w in wm.workloads
                                 if w.approval.status == 'approved' and w.artifact.fabric_item_id}
            except Exception:  # noqa: BLE001 - a damaged manifest must not stop conversion; the default path is unaffected
                ctx.workloads = {}
        compute_sources = load_sources(request.compute_sources_path)
        wanted = {n.lower() for n in request.pipelines}
        selected = [p for p in sorted(d.pipelines, key=lambda p: p.name.lower())
                    if not wanted or p.name.lower() in wanted]
        if wanted and len(selected) != len(wanted):
            known = {p.name.lower() for p in selected}
            unknown = sorted(n for n in request.pipelines if n.lower() not in known)
            return ConversionResult(success=False, message="Conversion failed.",
                                    error=f"Pipeline(s) not found in the semantic record: {', '.join(unknown)}. "
                                          f"Available: {', '.join(sorted(p.name for p in d.pipelines)) or '(none)'}.")
        if not selected:
            conv = self._assemble(pid, d, [], ctx, ConversionOutcome.no_pipelines, "No pipelines to convert.",
                                  compute_sources)
            self.store.save_conversion(pid, conv)
            return ConversionResult(success=True, conversion=conv, message="No pipelines found to convert.",
                                    problems=resolutions.problems)

        inferences: dict[str, list[Proposal]] = {}
        for pr in applied:
            for aid in pr.affects_activity_ids:
                inferences.setdefault(aid, []).append(pr)
        engine = ConversionEngine(ctx, inferences)
        converted = []
        contents: dict[str, dict] = {}
        for p in selected:
            record, content = engine.convert_pipeline(p)
            rel = f"pipelines/{p.name}.DataPipeline/pipeline-content.json"
            self.store.write_conversion_file(pid, rel, json.dumps(content, indent=2, ensure_ascii=False) + "\n")
            record.artifact_path = rel
            converted.append(record)
            contents[p.name] = content

        written: dict[str, str] = {}  # trigger name -> relative file (one file per trigger, shared by its pipelines)
        for record in converted:
            for t in record.triggers:
                if not t.schedules:
                    continue
                if t.name not in written:
                    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", t.name).strip("._") or "trigger"
                    rel = f"schedules/{safe}.json"
                    body = {"trigger": t.name, "triggerType": t.trigger_type, "sourceState": t.runtime_state,
                            "status": t.schedule_status, "pipelines": t.pipelines, "jobType": "Pipeline",
                            "schedules": t.schedules, "notes": t.schedule_notes,
                            "api": "POST /v1/workspaces/{workspaceId}/items/{itemId}/jobs/Pipeline/schedules"}
                    self.store.write_conversion_file(pid, rel, json.dumps(body, indent=2, ensure_ascii=False) + "\n")
                    written[t.name] = rel
                t.schedule_file = written[t.name]

        # Source artifacts discovery could not process are missing from this conversion: it cannot be "converted".
        lost = [u for u in d.unprocessed_artifacts if u.reason in UNPROCESSED_LOSS_REASONS]
        all_fine = all(r.status is ConversionStatus.converted for r in converted) and not lost
        outcome = ConversionOutcome.converted if all_fine else ConversionOutcome.partial
        conv = self._assemble(pid, d, converted, ctx, outcome, "", compute_sources, contents)
        conv.reconstruction_applied = sorted(p.id for p in applied)
        conv.reconstruction_open = sorted(open_blockers)
        self.store.save_conversion(pid, conv)
        if conv.compute_plan is not None:
            self.store.write_conversion_file(pid, "compute-plan.json", conv.compute_plan.model_dump_json(indent=2) + "\n")
        self.store.write_conversion_file(pid, "conversion-report.md", render_report(conv))
        self.store.write_conversion_file(
            pid, "resolutions.template.json", render_resolutions_template(conv, set(ctx.linked_services)))
        msg = (f"Converted {conv.pipeline_count} pipeline(s), {conv.activity_count} activities: "
               + ", ".join(f"{v} {k}" for k, v in sorted(conv.status_counts.items()) if v) + ".")
        return ConversionResult(success=True, conversion=conv, message=msg, problems=resolutions.problems)

    # ------------------------------------------------------------------------------------------------------
    def _with_reconstruction(self, pid: str, d: AdfSemanticDiscovery, commit: str):
        """(completed discovery, applied proposals, open blocker ids) - or an error message."""
        if not self.store.has_reconstruction(pid):
            return ("No reconstruction record. Run `adf2fabric reconstruct --project ...` first, or convert "
                    "without --apply-reconstruction.")
        rec = self.store.load_reconstruction(pid)
        if rec.source_commit_sha != commit:
            return ("The reconstruction record is stale (different source commit). Re-run `adf2fabric reconstruct`.")
        applied = [p for a in rec.pipelines for p in a.proposals if p.safe_to_generate and p.patches]
        open_blockers = [b.id for a in rec.pipelines for b in a.blockers]
        completed_file = self.store.reconstruction_dir(pid) / "adf-semantic.completed.yaml"
        if applied:
            if not completed_file.exists():
                return "The completed semantic record is missing; re-run `adf2fabric reconstruct`."
            import yaml
            with completed_file.open(encoding="utf-8") as fh:
                d = AdfSemanticDiscovery.model_validate(yaml.safe_load(fh))
        return d, applied, open_blockers

    def _assemble(self, pid: str, d: AdfSemanticDiscovery, pipelines, ctx: ProjectContext,
                  outcome: ConversionOutcome, detail: str,
                  compute_sources: ComputeSources | None = None,
                  contents: dict[str, dict] | None = None) -> ProjectConversion:
        status_counts: dict[str, int] = {s.value: 0 for s in ConversionStatus}
        strategy_counts: dict[str, int] = {}
        for p in pipelines:
            for a in p.activities:
                status_counts[a.status.value] += 1
                strategy_counts[a.strategy.value] = strategy_counts.get(a.strategy.value, 0) + 1
        prereqs: dict[tuple[str, str], Prerequisite] = {}
        for p in pipelines:
            for q in p.prerequisites:
                existing = prereqs.get((q.kind, q.name.lower()))
                if existing is None:
                    prereqs[(q.kind, q.name.lower())] = q.model_copy(deep=True)
                else:
                    existing.needed_by = sorted({*existing.needed_by, *q.needed_by})
        ordered = sorted(prereqs.values(), key=lambda q: (q.kind, q.name.lower()))
        now = datetime.now(timezone.utc)
        notes = [f"Not documented by Microsoft, so not independently verified: {s}" for s in INFERRED]
        notes += [f"Resolution file: {m}" for m in ctx.resolutions.problems]
        notes += [f"Source artifact NOT converted (discovery could not process it): {u.path} ({u.reason})"
                  for u in d.unprocessed_artifacts if u.reason in UNPROCESSED_LOSS_REASONS]
        conv = ProjectConversion(
            project_id=pid, source_commit_sha=d.source_commit_sha, status=outcome, created_at=now, updated_at=now,
            pipeline_count=len(pipelines), activity_count=sum(len(p.activities) for p in pipelines),
            status_counts=status_counts, strategy_counts=dict(sorted(strategy_counts.items())),
            pipelines=pipelines,
            connections=[q for q in ordered if q.kind in ("connection", "gateway", "credential")],
            workspace_items=[q for q in ordered if q.kind not in ("connection", "gateway", "credential")],
            schema_notes=notes, detail=detail, provenance=dict(_PROVENANCE),
            compatibility=compat.scan(d, {p.source_pipeline for p in pipelines}),
            preflight=preflight.scan(d, contents or {}),
            connector_coverage=connector_coverage(d.linked_services),
            compute_plan=build_compute_plan(d, {p.source_pipeline.lower() for p in pipelines}, compute_sources))
        if self.store.has_conversion(pid):
            try:
                existing = self.store.load_conversion(pid)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                conv.created_at = existing.created_at
                if _semantic(existing) == _semantic(conv):
                    conv.updated_at = existing.updated_at
        return conv


def _semantic(c: ProjectConversion) -> dict:
    data = json.loads(c.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data
