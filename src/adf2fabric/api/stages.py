"""Migration stages for the API, calling the existing agents unchanged. Each returns a small, secret-free summary.

``deploy`` re-uses the single-sourced pieces of ``agent/deployment/fabric_deploy`` (the same ordering, placeholder checks and
children-first logic the CLI uses); item assembly mirrors ``cli.py``'s deploy command, and a parity test keeps them aligned.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Optional

from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.deployment import record as deploy_record
from adf2fabric.agent.deployment.fabric_deploy import DeployError, DeployItem, FabricClient, child_refs, deploy
from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoveryRequest
from adf2fabric.agent.reconstruction.agent import ReconstructionAgent
from adf2fabric.agent.reconstruction.models import ReconstructionRequest
from adf2fabric.api.container import FABRIC_RESOURCE, Container, Session
from adf2fabric.project.store import ProjectStore

STAGES = ("discovery", "semantic", "dependencies", "workloads", "reconstruct", "convert", "compute", "validate", "deploy_dry_run", "deploy_apply", "verify")
Log = Callable[[str], None]


class StageError(Exception):
    def __init__(self, message: str, actions: Optional[list[str]] = None):
        super().__init__(message)
        self.actions = actions or []


def _ok(result: Any, **extra: Any) -> dict[str, Any]:
    if not getattr(result, "success", False):
        raise StageError(str(getattr(result, "error", None) or getattr(result, "message", "The stage failed.")))
    return {"message": getattr(result, "message", ""), **extra}


def assemble_items(store: ProjectStore, pid: str, names: Optional[list[str]] = None, with_children: bool = False) -> list[DeployItem]:
    """Deploy items from the conversion output (the same assembly as the CLI's deploy command)."""
    conversion = store.load_conversion(pid)
    base = store.conversion_dir(pid)
    available: dict[str, DeployItem] = {}
    for p in conversion.pipelines:
        rel = p.artifact_path
        if rel and (base / rel).exists():
            path = base / rel
            available[p.source_pipeline.lower()] = DeployItem(
                p.fabric_pipeline_name, json.loads(path.read_text(encoding="utf-8")), str(path), p.structural_valid)
    wanted = [n.lower() for n in names] if names else list(available)
    unknown = [n for n in (names or []) if n.lower() not in available]
    if unknown:
        raise StageError(f"No converted pipeline named {', '.join(unknown)}.", [f"Available: {', '.join(sorted(available))}."])
    if with_children:
        seen: list[str] = []
        queue = [n for n in wanted if n in available]
        while queue:
            n = queue.pop(0)
            if n not in seen:
                seen.append(n)
                queue += [c.lower() for c in child_refs(available[n].content) if c.lower() in available]
        wanted = seen
    return [available[n] for n in wanted if n in available]


def conversion_summary(store: ProjectStore, pid: str, conv=None) -> dict[str, Any]:
    """What the UI shows for a conversion. ``stale`` = made from an older commit than the project's source."""
    conv = conv or store.load_conversion(pid)
    project = store.load(pid)
    res = store.conversion_dir(pid) / "resolutions.json"
    return dict(
        status=conv.status.value, status_counts=conv.status_counts,
        pipelines=[{"name": p.source_pipeline, "status": p.status.value, "structurally_valid": p.structural_valid,
                    "runtime_ready": p.runtime_ready} for p in conv.pipelines],
        unbound=[q.name for q in [*conv.connections, *conv.workspace_items] if not q.bound and not q.name.startswith("pipeline:")],
        filled_at_deploy=[q.name for q in conv.workspace_items if not q.bound and q.name.startswith("pipeline:")],
        preflight=[f.model_dump(mode="json") for f in conv.preflight],
        compatibility_findings=len(conv.compatibility), resolutions_used=res.exists(),
        stale=bool(project.repository and conv.source_commit_sha != project.repository.commit_sha))


def _verify(c: Container, session: Session, pid: str, options: dict, log: Log) -> dict[str, Any]:
    """Run-and-compare on the latest finished Fabric runs. Read-only: from the UI it never starts a run."""
    from adf2fabric.agent.deployment.verify import (AdfRuns, Factory, callers, load_record, record_path, render_report,
                                                    save_record, verify_all)

    dest = session.destination
    workspace = options.get("workspace_id") or dest.get("workspace_id")
    if not workspace:
        raise StageError("No destination workspace is selected.", ["Choose a workspace on the Connections screen."])
    if not c.store.has_conversion(pid):
        raise StageError("Nothing converted yet.", ["Run the convert stage first."])
    conv = c.store.load_conversion(pid)
    names = options.get("pipelines") or [p.source_pipeline for p in conv.pipelines]
    called = callers(c.store.conversion_dir(pid), conv.pipelines)
    path = record_path(c.store.project_dir(pid))
    previous = load_record(path)
    project = c.store.load(pid)
    factory = Factory.from_source(project.repository.repository_url if project.repository else None)
    adf = None
    if factory:
        try:  # the factory's subscription selects the account that owns it
            adf = AdfRuns(factory, c.azure.token(subscription=factory.subscription))
        except Exception as exc:  # noqa: BLE001 - no ADF history is a weaker baseline, not a failure
            log(f"ADF run history not available: {type(exc).__name__}")
    from adf2fabric.connections.providers.azure_cli import ProviderError

    try:
        token = c.fabric_token(session)
    except ProviderError as exc:
        raise StageError(str(exc), getattr(exc, "actions", None) or ["Sign in to Fabric on the Connections page."]) from None
    try:
        results = verify_all(lambda: FabricClient(token, session=c.fabric_http), workspace, names, start=False, adf=adf, previous=previous,
                             called=called, log=log)
    except DeployError as exc:
        raise StageError(str(exc)) from None
    record = save_record(path, results)
    path.with_name("verification-report.md").write_text(render_report(record), encoding="utf-8")
    keep = ("pipeline", "verdict", "fabric_status", "ran_at", "baseline", "baseline_at", "matched", "differences",
            "baseline_only", "fabric_only", "detail")
    return {"results": [{k: v.to_dict()[k] for k in keep} for v in results]}


def _workloads(c: Container, pid: str, log: Log) -> dict[str, Any]:
    """Assess Spark workloads (read-only: nothing is generated or sent to Fabric; no code is fetched without a credential)."""
    from adf2fabric.agent.workloads.engine import Options, build_manifest
    from adf2fabric.agent.workloads.report import render_report

    if not c.store.has_adf_semantic_discovery(pid):
        raise StageError("Run the 'semantic' stage first.")
    m = build_manifest(c.store, pid, Options(generate=False))
    c.store.save_workload_manifest(pid, m, render_report(m))
    log(f"{m.summary.total} workload(s) assessed")
    return {"message": "Workloads assessed.", "workloads": m.summary.total, "by_classification": m.summary.by_classification}


def _validate(c: Container, session: Session, pid: str, options: dict, log: Log) -> dict[str, Any]:
    """Independent validation. Static always; api / deployment need an authorised workspace; runtime reads recorded real runs only."""
    from adf2fabric.agent.validation import engine
    from adf2fabric.agent.validation.report import render

    wanted = set(options.get("validate_stages") or ["static"]) & set(engine.ALL_STAGES) or {"static"}
    workspace = options.get("workspace_id") or session.destination.get("workspace_id")
    fab = None
    if wanted & {"api", "deployment"}:
        if not workspace:
            raise StageError("Choose a Fabric workspace before validating the API and deployment stages.", ["Connect a workspace on the Connections page."])
        fab, _ = c.gateways(session)
    log("validating: " + ", ".join(sorted(wanted)))
    report = engine.validate(c.store, pid, stages=wanted, fab=fab, workspace=workspace)
    c.store.save_validation_report(pid, report)
    d = c.store.project_dir(pid) / "validation"
    d.mkdir(parents=True, exist_ok=True)
    (d / "report.md").write_text(render(report), encoding="utf-8")
    return {"message": "Validation finished.", "overall": report.overall, "stages": {s.stage.value: s.status.value for s in report.stages},
            "checks": len(report.checks)}


def run_stage(c: Container, session: Session, pid: str, stage: str, log: Log, options: Optional[dict] = None) -> dict[str, Any]:
    options = options or {}
    if stage not in STAGES:
        raise StageError(f"Unknown stage '{stage}'.", [f"Use one of: {', '.join(STAGES)}."])
    if not c.store.exists(pid):
        raise StageError(f"Project '{pid}' does not exist.", ["Import a source first."])
    log(f"starting {stage}")
    if stage == "discovery":
        r = DiscoveryAgent(c.store).discover_repository(DiscoveryRequest(project_name=pid))
        return _ok(r)
    if stage == "semantic":
        from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer

        r = AdfSemanticDiscoverer(c.store).run(AdfSemanticDiscoveryRequest(project_name=pid))
        d = r.discovery
        return _ok(r, pipelines=getattr(d, "pipeline_count", 0), linked_services=getattr(d, "linked_service_count", 0),
                   integration_runtimes=len(getattr(d, "integration_runtimes", []) or []))
    if stage == "reconstruct":
        r = ReconstructionAgent(c.store).run(ReconstructionRequest(project_name=pid))
        return _ok(r)
    if stage == "convert":
        res = c.store.conversion_dir(pid) / "resolutions.json"
        r = ConversionAgent(c.store).run(ConversionRequest(
            project_name=pid, pipelines=options.get("pipelines") or [], resolutions_path=str(res) if res.exists() else None))
        _ok(r)  # a failed conversion has no record: say why before reading it
        return _ok(r, **conversion_summary(c.store, pid, r.conversion))
    if stage == "compute":
        from adf2fabric.agent.assessment.agent import ComputeAgent, ComputeRequest

        src = session.source or {}
        fetcher = None
        if src.get("kind") == "azure" and src.get("subscription_id") and src.get("resource_group") and src.get("factory_name"):
            from adf2fabric.agent.assessment import adf_history
            from adf2fabric.agent.discovery.adf.arm import default_transport

            def fetcher(names):  # run history from the live factory this project came from (read-only)
                token = c.azure.token(subscription=src["subscription_id"])
                return adf_history.fetch(default_transport(), token, src["subscription_id"], src["resource_group"],
                                         src["factory_name"], names, log=log)
        r = ComputeAgent(c.store).run(ComputeRequest(project_name=pid), history_fetcher=fetcher, log=log)
        a = r.assessment
        return _ok(r, recommended_sku=a.recommended_sku if a else None, cu_hours_per_day=a.cu_hours_per_day if a else None,
                   history_used=a.history_used if a else False, report=r.report_markdown)
    if stage == "verify":
        return _verify(c, session, pid, options, log)
    if stage == "dependencies":
        from adf2fabric.agent.dependencies import DependencyDiscoveryError, discover_dependencies

        try:
            inv = discover_dependencies(c.store, pid)
        except DependencyDiscoveryError as exc:
            raise StageError(str(exc), ["Run the 'semantic' stage first."]) from None
        return {"message": "Dependencies resolved.", "dependencies": inv.summary.total, "blocking": inv.summary.blocking, "errors": inv.summary.findings_error,
                "cycles": len(inv.cycles)}
    if stage == "workloads":
        return _workloads(c, pid, log)
    if stage == "validate":
        return _validate(c, session, pid, options, log)
    # deploy
    dest = session.destination
    if dest.get("provider", "azure_cli") != "azure_cli":
        raise StageError("Deployment uses the Azure CLI sign-in; the Fabric CLI session cannot deploy pipelines in this version.",
                         ["Switch the destination to the Azure CLI sign-in for the deploy step."])
    workspace = options.get("workspace_id") or dest.get("workspace_id")
    if not workspace:
        raise StageError("No destination workspace is selected.", ["Choose a workspace on the Connections screen."])
    items = assemble_items(c.store, pid, options.get("pipelines"), bool(options.get("with_children")))
    if not items:
        raise StageError("Nothing to deploy.", ["Run the convert stage first."])
    try:
        token = c.fabric_token(session)
        results = deploy(FabricClient(token, session=c.fabric_http), workspace, items, apply=stage == "deploy_apply",
                         allow_unbound=bool(options.get("allow_unbound")))
    except DeployError as exc:
        raise StageError(str(exc)) from None
    if stage == "deploy_apply":
        deploy_record.note_apply(c.store.project_dir(pid), workspace, results)
    else:
        deploy_record.note_dry_run(c.store.project_dir(pid), workspace, [i.name for i in items], sum(1 for r in results if r.action in ("failed", "refused")))
    return {"applied": stage == "deploy_apply",
            "results": [{"name": r.name, "action": r.action, "detail": r.detail, "children": r.children}
                        for r in results]}
