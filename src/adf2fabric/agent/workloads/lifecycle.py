"""Approval, deployment and run evidence for generated workloads.

* ``approve`` records a person's decision about one workload's CURRENT artifact (the approval is tied to its hash, so changed
  output needs a new approval). Nothing outward happens.
* ``deploy`` creates/updates the Fabric item for approved workloads only, dry-run unless ``apply``. It never deletes.
* ``record_run`` stores evidence of execution, and only from a real Fabric pipeline run whose activity ran the deployed item.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.deployment import notebook_deploy, sjd_deploy
from adf2fabric.agent.deployment.fabric_deploy import DeployError, FabricClient
from adf2fabric.agent.workloads.engine import summarize
from adf2fabric.project.store import ProjectStore
from adf2fabric.project.workloads import Approval, ExecutionEvidence, WorkloadEntry, WorkloadManifest


class LifecycleError(Exception):
    pass


def _find(m: WorkloadManifest, ident: str) -> WorkloadEntry:
    low = ident.lower()
    hits = [w for w in m.workloads if w.id.lower() == low or w.source_item.lower() == low or w.name.lower() == low]
    if not hits:
        raise LifecycleError(f"No workload '{ident}'. Known: {', '.join(w.id for w in m.workloads) or '(none)'}.")
    if len(hits) > 1:
        raise LifecycleError(f"'{ident}' is ambiguous ({', '.join(w.id for w in hits)}); use the full id.")
    return hits[0]


def approve(store: ProjectStore, project_id: str, ident: str, *, by: str, approve: bool = True, note: str = "",
            now: Optional[datetime] = None) -> WorkloadEntry:
    m = store.load_workload_manifest(project_id)
    w = _find(m, ident)
    if approve and w.artifact.kind == "none":
        raise LifecycleError(f"'{w.id}' has no generated artifact ({w.generation.reason}); there is nothing to approve.")
    if not by.strip():
        raise LifecycleError("Say who is approving (--by).")
    w.approval = Approval(status="approved" if approve else "rejected", by=by.strip(), at=now or datetime.now(timezone.utc), note=note[:300])
    m.summary = summarize(m)
    m.updated_at = now or datetime.now(timezone.utc)
    store.save_workload_manifest(project_id, m)
    return w


def deploy(store: ProjectStore, project_id: str, client: FabricClient, workspace: str, idents: list[str], *, apply: bool,
           resolutions: Optional[Path] = None) -> list[tuple[WorkloadEntry, str]]:
    m = store.load_workload_manifest(project_id)
    chosen = [_find(m, i) for i in idents] if idents else [w for w in m.workloads if w.artifact.kind != "none"]
    results: list[tuple[WorkloadEntry, str]] = []
    project_dir = store.project_dir(project_id)
    for w in chosen:
        if w.artifact.kind == "none":
            results.append((w, "skipped: no artifact was generated"))
            continue
        if w.approval.status != "approved":
            results.append((w, f"skipped: not approved (status {w.approval.status}); run workloads-approve"))
            continue
        tracked = project_dir / str(w.artifact.definition_file or w.artifact.file)
        if not tracked.is_file() or hashlib.sha256(tracked.read_bytes()).hexdigest() != w.artifact.sha256:
            results.append((w, "skipped: the generated file changed since it was generated; run workloads-generate again"))
            continue
        try:
            if w.artifact.kind == "fabric_notebook":
                r = notebook_deploy.deploy_notebook(client, workspace, w.artifact.display_name or w.name,
                                                    notebook_deploy.build_definition(project_dir / str(w.artifact.file)), apply=apply)
                if apply and r.item_id:
                    w.artifact.fabric_item_id = r.item_id
                    if resolutions is not None:
                        notebook_deploy.bind_resolutions(resolutions, w.name, r.item_id, workspace)
            elif w.artifact.kind == "fabric_spark_job_definition":
                if w.artifact.configuration.get("lakehouse") != "set":
                    results.append((w, "skipped: no default lakehouse was set when this definition was generated (--lakehouse-id)"))
                    continue
                definition = json.loads((project_dir / str(w.artifact.definition_file)).read_text(encoding="utf-8"))
                r = sjd_deploy.deploy_spark_job_definition(client, workspace, w.artifact.display_name or w.name, definition, apply=apply)
                if apply and r.item_id:
                    w.artifact.fabric_item_id = r.item_id
            else:
                results.append((w, f"skipped: unsupported artifact kind {w.artifact.kind}"))
                continue
            results.append((w, f"{r.action}: {r.detail}" + (f" [{r.item_id}]" if r.item_id else "")))
        except DeployError as exc:
            results.append((w, f"failed: {getattr(exc, 'detail', None) or exc}"))
    if apply:
        m.updated_at = datetime.now(timezone.utc)
        store.save_workload_manifest(project_id, m)
    return results


def _bound_item(store: ProjectStore, project_id: str, pipeline: str, activity: str) -> Optional[str]:
    """The item id the converted pipeline's activity is bound to (notebookId / sparkJobDefinitionId)."""
    path = store.conversion_dir(project_id) / "pipelines" / f"{pipeline}.DataPipeline" / "pipeline-content.json"
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    def walk(items: Any):
        for a in items if isinstance(items, list) else []:
            yield a
            tp = a.get("typeProperties", {}) if isinstance(a, dict) else {}
            for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
                yield from walk(tp.get(key))
            for case in tp.get("cases", []) if isinstance(tp.get("cases"), list) else []:
                yield from walk(case.get("activities") if isinstance(case, dict) else None)

    for a in walk((data.get("properties") or {}).get("activities")):
        if isinstance(a, dict) and str(a.get("name", "")).lower() == activity.lower():
            tp = a.get("typeProperties", {})
            return tp.get("notebookId") or tp.get("sparkJobDefinitionId")
    return None


def record_run(store: ProjectStore, project_id: str, ident: str, run: Any, *, now: Optional[datetime] = None) -> WorkloadEntry:
    """Record execution evidence from a ``fabric_run.RunResult`` of the pipeline that runs this workload's activity."""
    m = store.load_workload_manifest(project_id)
    w = _find(m, ident)
    item_id = w.artifact.fabric_item_id
    if not item_id:
        raise LifecycleError(f"'{w.id}' has not been deployed (no Fabric item id), so a run cannot be evidence for it.")
    candidates = [u for u in w.used_by if u.split("/", 1)[0].lower() == str(run.pipeline).lower()]
    if not candidates:
        raise LifecycleError(f"The run is of pipeline '{run.pipeline}', which does not run this workload (used by {', '.join(w.used_by)}).")
    activity = candidates[0].split("/", 1)[1]
    bound = _bound_item(store, project_id, run.pipeline, activity)
    if bound != item_id:
        raise LifecycleError(f"The converted pipeline's activity '{activity}' is not bound to this workload's Fabric item "
                             f"({'unbound' if not bound else 'bound to a different item'}): run `convert --resolutions` and deploy again.")
    act = next((a for a in run.activities if a.name.lower() == activity.lower()), None)
    if act is None:
        raise LifecycleError(f"The run has no record of activity '{activity}'.")
    ok = act.status.lower() == "succeeded"
    w.execution = ExecutionEvidence(status="succeeded" if ok else "failed", fabric_item_id=item_id, run_id=run.job_id or None,
                                    finished_at=now or datetime.now(timezone.utc),
                                    detail=("" if ok else act.error[:300]) or f"activity status {act.status}")
    m.summary = summarize(m)
    store.save_workload_manifest(project_id, m)
    return w
