"""Migration stages for the API, calling the existing agents unchanged. Each returns a small, secret-free summary.

``deploy`` re-uses the single-sourced pieces of ``agent/deployment/fabric_deploy`` (the same ordering, placeholder checks and
children-first logic the CLI uses); item assembly mirrors ``cli.py``'s deploy command, and a parity test keeps them aligned.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.deployment import record as deploy_record, sjd_deploy
from adf2fabric.agent.deployment.fabric_deploy import DeployError, DeployItem, FabricClient, child_refs, deploy
from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoveryRequest
from adf2fabric.agent.reconstruction.agent import ReconstructionAgent
from adf2fabric.agent.reconstruction.models import ReconstructionRequest
from adf2fabric.api.container import Container, Session
from adf2fabric.connections.redaction import redact
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
            for ch in p.split_children:  # split off over the activity limit: deployed with their parent, children first
                cpath = base / (ch.artifact_path or "")
                if ch.artifact_path and cpath.exists():
                    available[ch.name.lower()] = DeployItem(ch.name, json.loads(cpath.read_text(encoding="utf-8")), str(cpath), p.structural_valid)
    wanted = [n.lower() for n in names] if names else list(available)
    split_of = {p.source_pipeline.lower(): [ch.name.lower() for ch in p.split_children] for p in conversion.pipelines}
    wanted += [ch for n in list(wanted) for ch in split_of.get(n, []) if ch not in wanted]  # a split-off child always goes with its parent
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
    from adf2fabric.connections.needs import _walk, is_waiting

    res = store.conversion_dir(pid) / "resolutions.json"
    waiting = {p.source_pipeline: sum(1 for a in _walk(p.activities) if is_waiting(a)) for p in conv.pipelines}
    return dict(
        status=conv.status.value, status_counts=conv.status_counts, waiting=sum(waiting.values()),
        pipelines=[{"name": p.source_pipeline, "status": p.status.value, "structurally_valid": p.structural_valid,
                    "runtime_ready": p.runtime_ready, "waiting": waiting[p.source_pipeline]} for p in conv.pipelines],
        unbound=[q.name for q in [*conv.connections, *conv.workspace_items] if not q.bound and not q.name.startswith("pipeline:")],
        filled_at_deploy=[q.name for q in conv.workspace_items if not q.bound and q.name.startswith("pipeline:")],
        preflight=[f.model_dump(mode="json") for f in conv.preflight],
        compatibility_findings=len(conv.compatibility), resolutions_used=res.exists(),
        stale=bool(project.repository and conv.source_commit_sha != project.repository.commit_sha))


def _approved_brief(c: Container, session: Session, pid: str, log: Log) -> Optional[dict[str, Any]]:
    """The approved consultation brief's settings (with its version), or None when there is no approved, current brief."""
    from adf2fabric.agent.consultation import brief_record

    v = brief_record.view(c.store, pid, session.destination)
    if not v["settings"]:
        if v["state"] != "draft" or v["versions"]:
            log(f"The consultation brief is {v['state']}{' and changed since approval' if v['changed_since_approval'] else ''}: its settings are not used.")
        return None
    log(f"Following the approved consultation brief v{v['current_version']['version']}.")
    return {**v["settings"], "version": v["current_version"]["version"]}


def _write_storage_map(c: Container, pid: str, lakehouse: str) -> None:
    import json as _json

    from adf2fabric.agent.storage.mapping import build_storage_map

    out = c.store.project_dir(pid) / "storage"
    existing = out / "storage-map.json"
    names = {}
    if existing.exists():  # keep shortcut names a person chose (storage-map --shortcut)
        try:
            names = {(s["account"], s["container"]): s["shortcut"] for s in _json.loads(existing.read_text(encoding="utf-8")).get("shortcuts", [])}
        except (ValueError, KeyError, OSError):
            names = {}
    from adf2fabric.agent.storage.mapping import StorageMapError

    disc = c.store.load_adf_semantic_discovery(pid)
    try:
        m = build_storage_map(disc, lakehouse, names)
    except StorageMapError:  # a chosen name no longer fits what discovery found: plan the default names
        m = build_storage_map(disc, lakehouse)
    out.mkdir(parents=True, exist_ok=True)
    from adf2fabric.project.locks import write_atomic

    write_atomic(existing, _json.dumps(m.to_dict(), indent=2, default=str))


def _verify(c: Container, session: Session, pid: str, options: dict, log: Log) -> dict[str, Any]:
    """Compare Fabric runs with ADF. By default read-only (the latest finished runs); with ``run`` (authorised, confirmed and audited by the
    route) it STARTS a real run of each named pipeline and waits for it."""
    from adf2fabric.agent.deployment.verify import splits as verify_splits
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
            adf = AdfRuns(factory, lambda: c.azure.token(subscription=factory.subscription))  # a fresh token per read: a job can outlast one
        except Exception as exc:  # noqa: BLE001 - no ADF history is a weaker baseline, not a failure
            log(f"ADF run history not available: {type(exc).__name__}")
    from adf2fabric.connections.providers.azure_cli import ProviderError

    try:
        c.fabric_token(session)  # signed in to Fabric? (raises when not); the run helper reads its own token
    except ProviderError as exc:
        raise StageError(str(exc), getattr(exc, "actions", None) or ["Sign in to Fabric on the Connections page."]) from None
    try:
        start = bool(options.get("run"))
        if start:
            log(f"Starting real runs of {len(names)} pipeline(s) in Fabric")
        # a fresh token for every pipeline: started runs are waited for one after another and can outlast a single token
        results = verify_all(lambda: FabricClient(c.fabric_token(session)), workspace, names, start=start, adf=adf, previous=previous,
                             called=called, timeout_seconds=int(options.get("run_timeout") or 1800), log=log,
                             splits=verify_splits(conv.pipelines))
    except DeployError as exc:
        raise StageError(str(exc)) from None
    record = save_record(path, results)
    path.with_name("verification-report.md").write_text(render_report(record), encoding="utf-8")
    from adf2fabric.agent.validation import engine

    engine.record(c.store, pid, workspace=workspace)  # the runs are recorded: the runtime stage is worked out again from them, now
    keep = ("pipeline", "verdict", "fabric_status", "ran_at", "baseline", "baseline_at", "matched", "differences",
            "baseline_only", "fabric_only", "detail")
    return {"results": [{k: v.to_dict()[k] for k in keep} for v in results]}


def _workloads(c: Container, session: Session, pid: str, options: dict, log: Log) -> dict[str, Any]:
    """Assess Spark workloads; with ``generate``, also generate the Fabric notebooks, Spark jobs and Environments locally (the CLI's
    workloads-generate). Nothing is sent to Fabric. Code is read from Databricks only with ``fetch_code`` (the Azure CLI sign-in's Entra
    token); generated code reads storage through the OneLake shortcuts of the lakehouse verified in the chosen workspace, when there is one."""
    from adf2fabric.agent.dependencies.access import TokenSource
    from adf2fabric.agent.workloads.engine import Options, build_manifest
    from adf2fabric.agent.workloads.report import render_report

    if not c.store.has_adf_semantic_discovery(pid):
        raise StageError("Run the 'semantic' stage first.")
    generate, fetch = bool(options.get("generate")), bool(options.get("fetch_code"))
    opts = Options(generate=generate, fetch=fetch, tokens=TokenSource(env_names={}, allow_az=fetch))
    notes: list[str] = []
    if generate:
        workspace = options.get("workspace_id") or session.destination.get("workspace_id")
        lakehouse = _verified_lakehouse(c, pid, workspace)
        if lakehouse:
            opts.lakehouse_id = lakehouse
            smap = c.store.project_dir(pid) / "storage" / "storage-map.json"
            if not smap.exists():  # generated code reads storage through the map's shortcuts: write it (read-only), named after the plan's lakehouse
                from adf2fabric.agent.provisioning import service as prov

                spec = prov.load_spec(c.store, pid)
                if spec and len(spec.lakehouse_names) == 1:
                    _write_storage_map(c, pid, spec.lakehouse_names[0])
                    log("wrote the storage map (the shortcuts generated code reads through)")
            if smap.exists():
                import json

                from adf2fabric.agent.storage.routes import Routes, Target

                opts.storage_routes = Routes.from_storage_map(json.loads(smap.read_text(encoding="utf-8")), Target(workspace, lakehouse)) or None
        else:
            notes.append("No lakehouse is verified in the chosen workspace (Plan target), so generated code keeps its original storage paths.")
    m = build_manifest(c.store, pid, opts)
    c.store.save_workload_manifest(pid, m, render_report(m))
    log(f"{m.summary.total} workload(s) {'generated' if generate else 'assessed'}")
    return {"message": "Workloads generated (approve them on the Deploy step)." if generate else "Workloads assessed.",
            "workloads": m.summary.total, "generated": m.summary.generated, "with_open_issues": m.summary.generated_with_open_issues,
            "by_classification": m.summary.by_classification, "notes": notes}


def _verified_lakehouse(c: Container, pid: str, workspace: Optional[str]) -> Optional[str]:
    """The id of the one lakehouse provisioning verified for ``workspace``, or None (none, several, or another workspace's)."""
    from adf2fabric.agent.provisioning import service as prov

    if not workspace or not c.store.has_provisioning(pid, "verification"):
        return None
    target = prov.load_spec(c.store, pid)
    if target is None or not target.workspace_id or target.workspace_id.lower() != workspace.lower():
        return None
    import yaml

    rows = yaml.safe_load((c.store.project_dir(pid) / "provisioning" / "verification.yaml").read_text(encoding="utf-8")) or {}
    ids = [r.get("resource_id") for r in (rows.get("results") or rows.get("steps") or []) if isinstance(r, dict)
           and str(r.get("step_id", "")).startswith("lakehouse:") and r.get("status") == "verified" and r.get("resource_id")]
    return ids[0] if len(ids) == 1 else None


def _validate(c: Container, session: Session, pid: str, options: dict, log: Log) -> dict[str, Any]:
    """Validation: the static stage, worked out again from the source and the generated files. The runtime stage is worked out again from the
    recorded runs, and what Deploy read back from the workspace is kept while it is current (``engine.record``). Asking for the api or
    deployment stage reads the workspace now, as the Deploy step does after a deploy."""
    from adf2fabric.agent.validation import engine

    wanted = set(options.get("validate_stages") or ["static"]) & set(engine.ALL_STAGES) or {"static"}
    workspace = options.get("workspace_id") or session.destination.get("workspace_id")
    fab = None
    readback = bool(wanted & set(engine.READBACK))
    if readback:
        if not workspace:
            raise StageError("Choose a Fabric workspace before reading it back.", ["Connect a workspace on the Connections page."])
        fab, _ = c.gateways(session)
    log("validating: static" + (" and reading the workspace back" if readback else ""))
    report = engine.record(c.store, pid, readback=readback, fab=fab, workspace=workspace)
    return {"message": "Validation finished.", "overall": report.overall, "stages": {s.stage.value: s.status.value for s in report.stages},
            "checks": len(report.checks)}


def _read_back(c: Container, session: Session, pid: str, workspace: str, log: Log) -> dict[str, Any]:
    """Right after a real deploy: read the workspace back (api + deployment stages, read-only) and record it. A read that fails is said as
    such; nothing is assumed to be in Fabric that was not read."""
    from adf2fabric.agent.validation import engine

    log("Reading the workspace back from Fabric")
    try:
        fab, _ = c.gateways(session)
        rep = engine.record(c.store, pid, readback=True, fab=fab, workspace=workspace)
    except Exception as exc:  # noqa: BLE001  the deploy happened; the read-back is reported, never raised over it
        return {"ok": False, "error": redact(str(exc))[:300]}
    status = {s.stage.value: s.status.value for s in rep.stages}
    return {"ok": True, "api": status.get("api"), "deployment": status.get("deployment")}


AFTER = ("convert", "deploy_apply", "verify", "validate")  # the stages after which each pipeline's "why and what to do" is written again


def refresh_diagnosis(c: Container, pid: str, workspace: str = "") -> None:
    """Write the diagnosis again from the project's own records (no Fabric read), so every pipeline that has a problem at any step has its
    explanation (why, what to do) on record without anyone asking for it."""
    from adf2fabric.agent.diagnose import engine

    engine.refresh(c.store, pid, workspace)


def _notebooks_for_deploy(c: Container, session: Session, pid: str, workspace: str, stage: str, options: dict,
                          log: Log) -> tuple[dict[str, str], dict[str, str]]:
    """Deploy creates the generated notebooks the pipelines run that are missing from ``workspace`` (approved ones only; see
    ``lifecycle.notebooks_to_create``), then converts again so the pipelines use this workspace's ids. A dry run changes nothing in Fabric:
    it asks Fabric what would happen and converts again (project files only), and the pipelines that wait for those notebooks are reported
    as pending, not refused. A notebook with that name already in the workspace is never replaced: its pipelines are refused until it is
    bound. Returns (pending, refused): Fabric pipeline name -> why."""
    from adf2fabric.agent.workloads import lifecycle as wl
    from adf2fabric.project import bindings as bindings_mod

    need = wl.notebooks_to_create(c.store, pid, workspace, options.get("pipelines"))
    jobs = wl.spark_jobs_to_create(c.store, pid, workspace, options.get("pipelines"))
    flows = wl.dataflows_to_create(c.store, pid, workspace, options.get("pipelines"))
    if not need and not jobs and not flows:
        return {}, {}
    client = FabricClient(c.fabric_token(session))
    bindings_mod.use_workspace(c.store, pid, workspace)  # the ids saved for this workspace; the other workspace's are kept for it
    res = c.store.conversion_dir(pid) / "resolutions.json"
    names = {p.source_pipeline.lower(): p.fabric_pipeline_name for p in c.store.load_conversion(pid).pipelines} if c.store.has_conversion(pid) else {}
    pending: dict[str, list[str]] = {}
    clash: dict[str, list[str]] = {}
    for w in need:
        try:
            _w, _detail, existing = wl.create_notebook(c.store, pid, client, workspace, w.id, by="", apply=False)
            if existing:  # same name already there: never replaced here
                target = clash
            elif stage == "deploy_apply":
                wl.create_notebook(c.store, pid, client, workspace, w.id, by=str(options.get("by") or ""), apply=True, resolutions=res)
                log(f"created notebook {w.name} in the workspace")
                continue
            else:
                target = pending
        except (wl.LifecycleError, DeployError) as exc:
            raise StageError(f"Notebook '{w.name}' could not be prepared for this workspace: {getattr(exc, 'detail', None) or exc}") from None
        for u in w.used_by:
            pipe = names.get(u.split("/", 1)[0].lower())
            if pipe:
                target.setdefault(pipe, []).append(w.name)
    for w in jobs:  # approved Spark job definitions (an HDInsight Spark job's), created the same way: never replacing one with that name
        label = w.artifact.display_name or w.name
        try:
            existing = sjd_deploy.list_spark_job_definitions(client, workspace).get(label.lower())
            if existing:
                target = clash
            elif stage == "deploy_apply":
                done = wl.deploy(c.store, pid, client, workspace, [w.id], apply=True, resolutions=res)
                outcome = done[0][1] if done else "nothing happened"
                if not outcome.startswith("create"):
                    raise StageError(f"Spark job definition '{label}' could not be created: {outcome}")
                log(f"created Spark job definition {label} in the workspace")
                continue
            else:
                target = pending
        except DeployError as exc:
            raise StageError(f"Spark job definition '{label}' could not be prepared for this workspace: {getattr(exc, 'detail', None) or exc}") from None
        for u in w.used_by:
            pipe = names.get(u.split("/", 1)[0].lower())
            if pipe:
                target.setdefault(pipe, []).append(label)
    for w in flows:  # approved Dataflow Gen2 items generated from wrangling data flows, created the same way
        label = w.artifact.display_name or w.name
        try:
            _w, what = wl.create_dataflow(c.store, pid, client, workspace, w.id, apply=False)
            if what == "exists":
                target = clash
            elif stage == "deploy_apply":
                wl.create_dataflow(c.store, pid, client, workspace, w.id, apply=True, resolutions=res)
                log(f"created Dataflow Gen2 {label} in the workspace (set its sign-in and output destination in Fabric)")
                continue
            else:
                target = pending
        except (wl.LifecycleError, DeployError) as exc:
            raise StageError(f"Dataflow Gen2 '{label}' could not be prepared for this workspace: {getattr(exc, 'detail', None) or exc}") from None
        for u in w.used_by:
            pipe = names.get(u.split("/", 1)[0].lower())
            if pipe:
                target.setdefault(pipe, []).append(label)
    log("converting again with this workspace's notebook and Spark job definition ids")
    _run_stage(c, session, pid, "convert", log, {"workspace_id": workspace})
    return ({n: f"deploys once its notebook(s) / Spark job definition(s) {', '.join(sorted(set(v)))} are created: Deploy creates them first"
             for n, v in pending.items()},
            {n: f"an item named {', '.join(sorted(set(v)))} is already in this workspace and is not replaced: bind it (Deploy step), then "
                "deploy again" for n, v in clash.items()})


def run_stage(c: Container, session: Session, pid: str, stage: str, log: Log, options: Optional[dict] = None) -> dict[str, Any]:
    try:
        return _run_stage(c, session, pid, stage, log, options)
    finally:  # a stage that failed part-way (a refused deploy) has the most to explain
        if stage in AFTER and c.store.exists(pid):
            try:
                refresh_diagnosis(c, pid, (options or {}).get("workspace_id") or session.destination.get("workspace_id") or "")
            except Exception as exc:  # noqa: BLE001  a diagnosis that cannot be written is said, never raised over the stage's own outcome
                log(f"the per-pipeline explanations could not be updated ({type(exc).__name__})")


def _run_stage(c: Container, session: Session, pid: str, stage: str, log: Log, options: Optional[dict] = None) -> dict[str, Any]:
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
        from adf2fabric.project import bindings as bindings_mod

        target = options.get("workspace_id") or session.destination.get("workspace_id")
        if bindings_mod.use_workspace(c.store, pid, target):  # never another workspace's notebook, child-pipeline or lakehouse ids
            log("using the ids saved for the chosen workspace; the other workspace's ids are kept for it")
        res = c.store.conversion_dir(pid) / "resolutions.json"
        brief = _approved_brief(c, session, pid, log)
        pipelines = options.get("pipelines") or (brief or {}).get("pipelines") or []
        if brief and not options.get("pipelines") and brief.get("pipelines") is not None and not brief["pipelines"]:
            # an empty list means "every pipeline" to the converter: a brief that holds them all back must never turn into that
            raise StageError("The approved brief leaves no pipeline to convert.", ["Change the scope or the held-back pipelines in the consultation."])
        # what a person chose for this run beats the brief's default; "" means "keep connections" and None means "follow the brief"
        lakehouse = options["lakehouse"] or None if options.get("lakehouse") is not None else (brief or {}).get("lakehouse")
        if lakehouse:  # repointing reads the storage map; the brief's choice brings it up to date first (read-only)
            _write_storage_map(c, pid, lakehouse)
        r = ConversionAgent(c.store).run(ConversionRequest(
            project_name=pid, pipelines=pipelines, resolutions_path=str(res) if res.exists() else None, lakehouse=lakehouse,
            apply_reconstruction=bool(options["apply_reconstruction"]) if options.get("apply_reconstruction") is not None
            else bool((brief or {}).get("apply_reconstruction")),
            assisted=bool(options.get("assisted"))))
        _ok(r)  # a failed conversion has no record: say why before reading it
        return _ok(r, **conversion_summary(c.store, pid, r.conversion), brief=(brief or {}).get("version"), lakehouse=lakehouse,
                   assisted=[n.pipeline for n in r.conversion.assisted] if r.conversion else [])
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
        return _workloads(c, session, pid, options, log)
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
    from adf2fabric.project import bindings as bindings_mod

    waiting, blocked = _notebooks_for_deploy(c, session, pid, workspace, stage, options, log)
    other = bindings_mod.workspace_mismatch(c.store, pid, workspace)
    if other:  # the converted pipelines name items of another workspace: deploying them here would run that workspace's notebooks
        raise StageError(other, ["Choose this workspace, create or bind its notebooks on Convert, and Convert again."])
    items = assemble_items(c.store, pid, options.get("pipelines"), bool(options.get("with_children")))
    if not items:
        raise StageError("Nothing to deploy.", ["Run the convert stage first."])
    held = {**waiting, **blocked}  # fabric pipeline name -> why it is not pushed in this call
    try:
        token = c.fabric_token(session)
        from adf2fabric.agent.conversion import variable_library

        lib_results: list = []
        results = deploy(FabricClient(token), workspace, [i for i in items if i.name not in held],
                         apply=stage == "deploy_apply", allow_unbound=bool(options.get("allow_unbound")),
                         libraries=variable_library.load(c.store.conversion_dir(pid)), library_results=lib_results)
    except DeployError as exc:
        raise StageError(str(exc)) from None
    from adf2fabric.agent.deployment.fabric_deploy import DeployResult

    results += [DeployResult(n, "pending", detail=why) for n, why in sorted(waiting.items()) if any(i.name == n for i in items)]
    results += [DeployResult(n, "refused", detail=why) for n, why in sorted(blocked.items()) if any(i.name == n for i in items)]
    readback = None
    if stage == "deploy_apply":
        deploy_record.note_apply(c.store.project_dir(pid), workspace, results)
        readback = _read_back(c, session, pid, workspace, log)
    else:
        deploy_record.note_dry_run(c.store.project_dir(pid), workspace, [i.name for i in items], sum(1 for r in results if r.action in ("failed", "refused")),
                                   allow_unbound=bool(options.get("allow_unbound")))
    from adf2fabric.project import bindings as bindings_mod

    from adf2fabric.agent.consultation import brief_record

    note = None  # another workspace's ids are refused before deploying
    refused = brief_record.workspace_mismatch(c.store, pid, workspace) if stage == "deploy_dry_run" else None
    warnings = ([f"The real deploy to this workspace will be refused: {refused}"] if refused else []) + ([note] if note else [])
    return {"applied": stage == "deploy_apply",
            "results": [{"name": r.name, "action": r.action, "detail": r.detail, "children": r.children}
                        for r in results],
            "libraries": [{"name": lr.name, "action": lr.action, "detail": lr.detail} for lr in lib_results],
            "warnings": warnings, "readback": readback}
