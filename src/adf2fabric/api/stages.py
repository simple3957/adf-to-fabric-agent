"""Migration stages for the API, calling the existing agents unchanged. Each returns a small, secret-free summary.

``deploy`` re-uses the single-sourced pieces of ``agent/deployment/fabric_deploy`` (the same ordering, placeholder checks and
children-first logic the CLI uses); item assembly mirrors ``cli.py``'s deploy command, and a parity test keeps them aligned.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Optional

from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.deployment.fabric_deploy import DeployError, DeployItem, FabricClient, child_refs, deploy
from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoveryRequest
from adf2fabric.agent.reconstruction.agent import ReconstructionAgent
from adf2fabric.agent.reconstruction.models import ReconstructionRequest
from adf2fabric.api.container import FABRIC_RESOURCE, Container, Session
from adf2fabric.project.store import ProjectStore

STAGES = ("discovery", "semantic", "reconstruct", "convert", "compute", "deploy_dry_run", "deploy_apply")
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
        conv = r.conversion
        return _ok(r, status=conv.status.value, status_counts=conv.status_counts,
                   pipelines=[{"name": p.source_pipeline, "status": p.status.value, "structurally_valid": p.structural_valid,
                               "runtime_ready": p.runtime_ready} for p in conv.pipelines],
                   unbound=[q.name for q in [*conv.connections, *conv.workspace_items] if not q.bound],
                   compatibility_findings=len(conv.compatibility), resolutions_used=res.exists())
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
        token = c.azure.token(tenant=dest.get("tenant_id"), account=dest.get("account"), resource=FABRIC_RESOURCE)
        results = deploy(FabricClient(token), workspace, items, apply=stage == "deploy_apply",
                         allow_unbound=bool(options.get("allow_unbound")))
    except DeployError as exc:
        raise StageError(str(exc)) from None
    return {"applied": stage == "deploy_apply",
            "results": [{"name": r.name, "action": r.action, "item_id": r.item_id, "detail": r.detail, "children": r.children}
                        for r in results]}
