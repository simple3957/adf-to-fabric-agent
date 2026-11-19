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
from adf2fabric.agent.conversion import dataset_ledger, preflight
from adf2fabric.agent.conversion.compute_plan import ComputeSources, build_compute_plan, load_sources
from adf2fabric.agent.conversion.connectors import coverage as connector_coverage
from adf2fabric.agent.conversion.context import ProjectContext, Resolutions
from adf2fabric.agent.conversion.engine import ConversionEngine
from adf2fabric.agent.conversion.fabric_schema import INFERRED
from adf2fabric.agent.conversion.models import ConversionRequest, ConversionResult
from adf2fabric.agent.conversion.report import render_report, render_resolutions_template
from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.project.conversion import (
    AssistedNote,
    ConversionOutcome,
    ConversionStatus,
    Prerequisite,
    ProjectConversion,
)
from adf2fabric.project.discovery import UNPROCESSED_LOSS_REASONS, AdfSemanticDiscovery, AdfSemanticStatus
from adf2fabric.project.reconstruction import Proposal
from adf2fabric.project.store import ProjectNotFoundError, ProjectPathError, ProjectStore, slugify
from adf2fabric.project.semantic import semantic as _semantic

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
    gp = {g.name.lower(): g for f in d.factory for g in f.global_parameters}
    from adf2fabric.agent.conversion import variable_library

    library = {k: kinds[1] for k, g in gp.items() if (kinds := variable_library.eligible(g))}
    library_decode = {k: fn for k, g in gp.items() if (fn := variable_library.decoder(g))}  # Float/Array/Object held as JSON text
    # a global the Variable Library holds is read by each pipeline itself; only the others are passed down as gp_ parameters
    globals_needed = {k: sorted((o for low, o in v.items() if low not in library), key=str.lower) for k, v in direct.items()}
    factory = next((f.name for f in d.factory if f.name), "factory")
    return ProjectContext(
        library_name=variable_library.library_name(factory) if library else None, library=library, library_decode=library_decode,
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
        except (ProjectNotFoundError, ProjectPathError) as exc:
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

        assisted: list[dict] = []
        assist_log: list[str] = []
        if request.assisted:  # opt-in: accepted, still-current model-assisted drafts stand in for their pipelines' source (agent/assist)
            from adf2fabric.agent.assist import service as assist_service

            d, assisted, assist_log = assist_service.apply(self.store, pid, d)

        resolutions = Resolutions.load(Path(request.resolutions_path) if request.resolutions_path else None)
        ctx = build_project_context(d, resolutions)
        self._attach_workloads(ctx, pid)
        if request.lakehouse:
            smap = self.store.project_dir(pid) / "storage" / "storage-map.json"
            if not smap.exists():
                return ConversionResult(success=False, message="Conversion failed.",
                                        error=f"--lakehouse needs the storage map. Run `adf2fabric storage-map --project {pid}` first.")
            shortcuts = json.loads(smap.read_text(encoding="utf-8")).get("shortcuts", [])
            ctx.storage_routes = {(s["account"].lower(), s["container"].lower()): s["shortcut"] for s in shortcuts}
            ctx.route_lakehouse = request.lakehouse
        # The project's activity mapping (conversion/activity-mapping.json): written again from discovery (choices kept), then followed.
        from adf2fabric.agent.conversion import project_mapping as PM

        try:
            mapping = PM.build(self.store, pid)
            ctx.choices = PM.choices(mapping)
        except PM.MappingError as exc:
            return ConversionResult(success=False, message="Conversion failed.", error=str(exc))
        if not request.lakehouse and any(v == ("storage", "lakehouse") for v in ctx.choices.values()):
            routes, lh = self._lakehouse_for_choices(pid)  # only the activities that chose it are repointed
            if routes:
                ctx.storage_routes, ctx.route_lakehouse, ctx.route_default = routes, lh, False
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
            for ch in record.split_children:  # over the activity limit: the child pipelines split off it (agent/conversion/split.py)
                ch.artifact_path = f"pipelines/{ch.name}.DataPipeline/pipeline-content.json"
                self.store.write_conversion_file(pid, ch.artifact_path,
                                                 json.dumps(engine.split_contents[p.name][ch.name], indent=2, ensure_ascii=False) + "\n")
            converted.append(record)
            contents[p.name] = content

        from adf2fabric.agent.conversion import variable_library

        lib_dir = self.store.project_dir(pid) / "conversion" / variable_library.FOLDER
        if lib_dir.exists():  # a library from an earlier conversion must not be deployed for pipelines that no longer read it
            import shutil

            shutil.rmtree(lib_dir)
        if ctx.library_name and variable_library.referenced(contents.values()):
            for part, text in variable_library.definition(ctx.global_params.values()).items():
                self.store.write_conversion_file(pid, f"{variable_library.FOLDER}/{ctx.library_name}.VariableLibrary/{part}", text)

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

        # Storage / custom event triggers -> the event bridge plan (deploy --triggers turns it into an ARM template with the workspace's ids).
        from adf2fabric.agent.conversion import event_triggers

        by_name = {t.name.lower(): t for t in d.triggers}
        planned: dict[str, str] = {}
        for record in converted:
            for t in record.triggers:
                if t.event_status not in ("generated", "generated_with_differences") or t.name.lower() not in by_name:
                    continue
                if t.name not in planned:
                    ev = event_triggers.plan(by_name[t.name.lower()])
                    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", t.name).strip("._") or "trigger"
                    rel = f"event-triggers/{safe}.json"
                    body = {**ev.to_dict(), "sourceState": t.runtime_state,
                            "fabricApi": "POST /v1/workspaces/{workspaceId}/items/{pipelineId}/jobs/instances?jobType=Pipeline"}
                    self.store.write_conversion_file(pid, rel, json.dumps(body, indent=2, ensure_ascii=False) + "\n")
                    planned[t.name] = rel
                t.event_file = planned[t.name]

        # Source artifacts discovery could not process are missing from this conversion: it cannot be "converted".
        lost = [u for u in d.unprocessed_artifacts if u.reason in UNPROCESSED_LOSS_REASONS]
        all_fine = all(r.status is ConversionStatus.converted for r in converted) and not lost
        outcome = ConversionOutcome.converted if all_fine else ConversionOutcome.partial
        conv = self._assemble(pid, d, converted, ctx, outcome, "", compute_sources, contents)
        conv.assisted = [AssistedNote(**n) for n in assisted if n["pipeline"].lower() in {p.name.lower() for p in selected}]
        conv.schema_notes += [f"Converted from a reviewed model-assisted edit of the source (not the original file): {n.pipeline}, accepted "
                              f"{n.accepted_at or '?'} by {n.accepted_by or '?'}{' and edited' if n.edited else ''}." for n in conv.assisted] + assist_log
        conv.schema_notes += [f"Activity mapping: {m}" for m in PM.check(mapping, conv)]  # the converter and the master catalog must agree
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
    def forecast(self, pid: str) -> list:
        """What a convert would produce for every pipeline, computed in memory and never written (the Connect step reads it to know
        which linked services the converted pipelines still use, before Convert has run). Follows what a convert follows by itself: the
        project's resolutions, approved workloads already in the workspace, Databricks job records and the activity mapping's choices;
        not a run's own options (a run with --lakehouse can need fewer connections). Returns the pipeline records ([] when discovery
        is missing or stale)."""
        if not self.store.has_adf_semantic_discovery(pid):
            return []
        d = self.store.load_adf_semantic_discovery(pid)
        project = self.store.load(pid)
        if d.status not in (AdfSemanticStatus.discovered, AdfSemanticStatus.partial) or d.source_commit_sha != project.repository.commit_sha:
            return []
        res = self.store.conversion_dir(pid) / "resolutions.json"
        ctx = build_project_context(d, Resolutions.load(res if res.exists() else None))
        self._attach_workloads(ctx, pid)
        from adf2fabric.agent.conversion import project_mapping as PM

        try:
            mapping = PM.load(self.store, pid)
            ctx.choices = PM.choices(mapping) if mapping else {}
        except PM.MappingError:
            ctx.choices = {}
        if any(v == ("storage", "lakehouse") for v in ctx.choices.values()):
            routes, lh = self._lakehouse_for_choices(pid)
            if routes:
                ctx.storage_routes, ctx.route_lakehouse, ctx.route_default = routes, lh, False
        engine = ConversionEngine(ctx, {})
        return [engine.convert_pipeline(p)[0] for p in sorted(d.pipelines, key=lambda p: p.name.lower())]

    def _attach_workloads(self, ctx: ProjectContext, pid: str) -> None:
        """Approved workloads already in the workspace, and the Databricks job records: what conversion binds activities to."""
        if self.store.has_workload_manifest(pid):
            try:
                wm = self.store.load_workload_manifest(pid)
                ctx.workloads = {w.source_item.lower(): w for w in wm.workloads
                                 if w.approval.status == 'approved' and w.artifact.fabric_item_id}
            except Exception:  # noqa: BLE001 - a damaged manifest must not stop conversion; the default path is unaffected
                ctx.workloads = {}
        from adf2fabric.agent.workloads import databricks_jobs

        ctx.databricks_jobs = databricks_jobs.load_all(self.store.project_dir(pid))

    def _lakehouse_for_choices(self, pid: str) -> tuple[dict, Optional[str]]:
        """Shortcut routes and the lakehouse name for activities whose mapping chose the Lakehouse when the run itself did not: the
        storage map's shortcuts, and the lakehouse a person named (the storage map's, else the last provisioning plan's). Never invents one."""
        smap = self.store.project_dir(pid) / "storage" / "storage-map.json"
        if not smap.exists():
            return {}, None
        data = json.loads(smap.read_text(encoding="utf-8"))
        name = data.get("lakehouse")
        if not name and self.store.has_provisioning(pid, "target"):
            from adf2fabric.agent.provisioning import service as prov

            spec = prov.load_spec(self.store, pid)
            name = spec.lakehouse_names[0] if spec and len(spec.lakehouse_names) == 1 else None
        if not name:
            return {}, None
        return {(s["account"].lower(), s["container"].lower()): s["shortcut"] for s in data.get("shortcuts", [])}, name

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
            dataset_ledger=dataset_ledger.build(d, pipelines),
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


