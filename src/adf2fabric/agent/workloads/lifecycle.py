"""Approval, deployment and run evidence for generated workloads.

* ``approve`` records a person's decision about one workload's CURRENT artifact (the approval is tied to its hash, so changed
  output needs a new approval). Nothing outward happens.
* ``deploy`` creates/updates the Fabric item for approved workloads only, dry-run unless ``apply``. It never deletes.
* ``record_run`` stores evidence of execution, and only from a real Fabric pipeline run whose activity ran the deployed item.
* ``bind_deployed`` records a notebook already in the workspace (deployed by any route) as this workload's item, only when its code
  equals the generated notebook; the person who binds it is recorded as accepting it.
"""

from __future__ import annotations

import hashlib
import json
import re
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
                    _record_item(w, workspace, r.item_id)
                    if resolutions is not None:
                        notebook_deploy.bind_resolutions(resolutions, w.name, r.item_id, workspace)
            elif w.artifact.kind == "fabric_spark_job_definition":
                if w.artifact.configuration.get("lakehouse") != "set":
                    results.append((w, "skipped: no default lakehouse was set when this definition was generated (--lakehouse-id)"))
                    continue
                definition = json.loads((project_dir / str(w.artifact.definition_file)).read_text(encoding="utf-8"))
                r = sjd_deploy.deploy_spark_job_definition(client, workspace, w.artifact.display_name or w.name, definition, apply=apply)
                if apply and r.item_id:
                    _record_item(w, workspace, r.item_id)
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


def create_notebook(store: ProjectStore, project_id: str, client: FabricClient, workspace: str, ident: str, *, by: str, apply: bool,
                    resolutions: Optional[Path] = None, now: Optional[datetime] = None) -> tuple[WorkloadEntry, str, Optional[str]]:
    """Put this workload's generated notebook into ``workspace`` and take its id: (workload, what happened, item id).

    Dry run unless ``apply``: it says whether Fabric would create the notebook or replace the content of the one with its name. With ``apply`` the
    person who asks accepts the generated notebook (the approval, with their name), the id is recorded on the workload and, in ``resolutions``,
    for the next Convert. A notebook whose content was replaced keeps no earlier run as evidence: that run ran other code."""
    m = store.load_workload_manifest(project_id)
    w = _find(m, ident)
    if w.artifact.kind != "fabric_notebook" or w.generation.status != "generated":
        raise LifecycleError(f"'{w.name}' has no generated notebook to create: generate it first (workloads-generate).")
    if apply and not by.strip():
        raise LifecycleError("Say who is creating the notebook.")
    path = store.project_dir(project_id) / str(w.artifact.file)
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != w.artifact.sha256:
        raise LifecycleError("The generated notebook changed since it was generated; run workloads-generate again.")
    cells, moved = workspace_cells(store, project_id, client, workspace, w)
    if moved:  # this workspace's copy: the same code, its OneLake paths pointed at this workspace's lakehouse
        nb = json.loads(path.read_text(encoding="utf-8"))
        path = path.parent / "workspaces" / f"{workspace.lower()}.ipynb"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({**nb, "cells": cells}, indent=1), encoding="utf-8")
    r = notebook_deploy.deploy_notebook(client, workspace, w.artifact.display_name or w.name, notebook_deploy.build_definition(path), apply=apply)
    if not apply:
        return w, ("Fabric would create a new notebook" if r.action == "create" else
                   "A notebook with this name is already in the workspace: its content would be replaced by the generated notebook"), r.item_id
    item_id = str(r.item_id).lower()
    at = now or datetime.now(timezone.utc)
    _record_item(w, workspace, item_id)
    w.approval = Approval(status="approved", by=by.strip(), at=at, note=f"Accepted by creating the generated notebook in Fabric as {item_id} ({r.action})"[:300])
    if r.action == "update" or (w.execution.fabric_item_id or "").lower() != item_id:
        w.execution = ExecutionEvidence()
    m.summary = summarize(m)
    m.updated_at = at
    store.save_workload_manifest(project_id, m)
    if resolutions is not None:
        notebook_deploy.bind_resolutions(resolutions, w.name, item_id, workspace)
    return w, (("Created in Fabric" if r.action == "create" else "Content replaced in Fabric")
               + (", reading this workspace's lakehouse" if moved else "") + ": its id is saved. Convert again to use it."), item_id


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


def recorded_run(store: ProjectStore, project_id: str, w: WorkloadEntry, item_id: str, workspace: str = "") -> Optional[ExecutionEvidence]:
    """The run evidence already in the run-and-compare record: the latest completed Fabric run of a pipeline that runs this workload, newer
    than that pipeline's latest deploy, whose activity is bound to ``item_id`` and succeeded. None when there is no such run."""
    from adf2fabric.agent.deployment import verify

    record = {k.lower(): v for k, v in verify.current_record(store.project_dir(project_id), workspace).items()}
    for u in w.used_by:
        pipeline, _, activity = u.partition("/")
        entry = record.get(pipeline.lower()) or {}
        if entry.get("stale_run") or entry.get("fabric_status") != "Completed" or _bound_item(store, project_id, pipeline, activity) != item_id:
            continue
        act = next((a for n, a in (entry.get("activities") or {}).items() if n.lower() == activity.lower()), None)
        if act and act.get("status") == "Succeeded":
            try:
                finished = datetime.fromisoformat(str(entry.get("ran_at")).replace("Z", "+00:00"))
                finished = finished if finished.tzinfo else finished.replace(tzinfo=timezone.utc)
            except ValueError:
                finished = None
            return ExecutionEvidence(status="succeeded", fabric_item_id=item_id, run_id=entry.get("job_id") or None, finished_at=finished,
                                     detail=f"'{activity}' succeeded in the recorded Fabric run of {pipeline} ({entry.get('verdict')} against its baseline)")
    return None


def pipeline_item(store: ProjectStore, project_id: str, w: WorkloadEntry) -> Optional[str]:
    """The Fabric item id the converted pipelines run for this workload (the first activity that uses it and is bound), or None."""
    for u in w.used_by:
        pipeline, _, activity = u.partition("/")
        if activity and (item := _bound_item(store, project_id, pipeline, activity)) and not str(item).startswith("<"):
            return str(item)
    return None


def pipeline_item_here(store: ProjectStore, project_id: str, w: WorkloadEntry, workspace: str) -> Optional[str]:
    """``pipeline_item``, but only when the last Convert used ``workspace``'s ids: another workspace's notebook id, offered for binding
    here, could only be refused (found 2026-11-10: Bind in a new workspace was pre-filled with the previous workspace's notebook)."""
    from adf2fabric.project import bindings as B

    converted_for = (B.bound_workspace(store, project_id) or "").lower()
    if workspace and converted_for and converted_for != workspace.lower():
        return None
    return pipeline_item(store, project_id, w)


_G = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_ONELAKE_ABFSS = re.compile(r"abfss://(" + _G + r")@onelake\.dfs\.fabric\.microsoft\.com/(" + _G + r")/", re.I)
_ONELAKE_HTTPS = re.compile(r"https://onelake\.dfs\.fabric\.microsoft\.com/(" + _G + r")/(" + _G + r")/", re.I)


def onelake_workspaces(cells: list[dict]) -> set[str]:
    """The workspaces the code's OneLake paths (by id) name."""
    code = "\n".join(_code(cells))
    return {m.group(1).lower() for rx in (_ONELAKE_ABFSS, _ONELAKE_HTTPS) for m in rx.finditer(code)}


def retarget(cells: list[dict], workspace: str, lakehouse_id: str) -> list[dict]:
    """The cells with every OneLake path (by id) of another workspace pointed at ``workspace``'s lakehouse ``lakehouse_id``. Paths below the
    lakehouse (Files/<shortcut>/...) stay the same: the plan makes the shortcuts with the same names in every workspace."""
    ws, lh = workspace.lower(), lakehouse_id.lower()

    def one(text: str) -> str:
        text = _ONELAKE_ABFSS.sub(lambda m: m.group(0) if m.group(1).lower() == ws else f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/", text)
        return _ONELAKE_HTTPS.sub(lambda m: m.group(0) if m.group(1).lower() == ws else f"https://onelake.dfs.fabric.microsoft.com/{ws}/{lh}/", text)

    out = []
    for c in cells:
        c = dict(c)
        if c.get("cell_type") == "code":
            src = c.get("source", "")
            c["source"] = [one(x) for x in src] if isinstance(src, list) else one(str(src))
        out.append(c)
    return out


def workspace_cells(store: ProjectStore, project_id: str, client: FabricClient, workspace: str, w: WorkloadEntry) -> tuple[list[dict], bool]:
    """(cells, retargeted): the generated notebook as it must be in ``workspace``. When its OneLake paths name another workspace, they are
    pointed at this workspace's lakehouse of the name the storage map chose; that lakehouse must exist here."""
    from adf2fabric.agent.deployment.shortcut_deploy import list_lakehouses

    cells = json.loads((store.project_dir(project_id) / str(w.artifact.file)).read_text(encoding="utf-8")).get("cells", [])
    if not (onelake_workspaces(cells) - {workspace.lower()}):
        return cells, False
    try:
        name = str(json.loads((store.project_dir(project_id) / "storage" / "storage-map.json").read_text(encoding="utf-8")).get("lakehouse") or "")
    except (OSError, ValueError):
        name = ""
    if not name:
        raise LifecycleError(f"'{w.name}' reads its data through a lakehouse in another workspace, and no lakehouse name is chosen for this "
                             "project (the storage map). Run the storage map, then plan the lakehouse on Plan target.")
    lh = list_lakehouses(client, workspace).get(name.lower())
    if not lh:
        raise LifecycleError(f"'{w.name}' reads its data through lakehouse '{name}' and its OneLake shortcuts, which this workspace does not "
                             "have yet. Create them first (Plan target: approve and apply the lakehouse and shortcut steps), then create the notebook.")
    return retarget(cells, workspace, lh), True


def notebooks_to_create(store: ProjectStore, project_id: str, workspace: str, pipelines: Optional[list[str]] = None) -> list[WorkloadEntry]:
    """The generated notebooks Deploy creates in ``workspace`` before deploying: generated, approved by a person at least once (an approval is
    reset when the code changes), used by a pipeline in scope (``pipelines``: source names; empty = all), and with no id in that workspace yet."""
    if not workspace or not store.has_workload_manifest(project_id):
        return []
    scope = {p.lower() for p in pipelines} if pipelines else None
    out = []
    for w in store.load_workload_manifest(project_id).workloads:
        if w.artifact.kind != "fabric_notebook" or w.generation.status != "generated" or w.approval.status != "approved":
            continue
        if (w.artifact.fabric_item_ids or {}).get(workspace.lower()):
            continue
        users = {u.split("/", 1)[0].lower() for u in w.used_by}
        if scope is None or users & scope:
            out.append(w)
    return out


def _record_item(w: WorkloadEntry, workspace: str, item_id: str) -> None:
    w.artifact.fabric_item_id = item_id
    w.artifact.fabric_item_ids[workspace.lower()] = item_id


def _code(cells: list[dict]) -> list[str]:
    return [("".join(c.get("source", "")) if isinstance(c.get("source"), list) else str(c.get("source", ""))).strip()
            for c in cells if c.get("cell_type") == "code"]


def check_deployed(store: ProjectStore, project_id: str, client: FabricClient, workspace: str, ident: str, item_id: str) -> tuple[WorkloadEntry, bool, str]:
    """(workload, ok, why): is ``item_id`` a notebook in ``workspace`` whose code cells equal this workload's generated notebook? Read-only."""
    m = store.load_workload_manifest(project_id)
    w = _find(m, ident)
    if w.artifact.kind != "fabric_notebook" or w.generation.status != "generated":
        return w, False, "This workload has no generated notebook to compare with: generate it first (workloads-generate)."
    item_id = item_id.strip().strip("{}").lower()
    if item_id not in {i.lower() for i in notebook_deploy.list_notebooks(client, workspace).values()}:
        return w, False, "That id is not a notebook in the chosen Fabric workspace."
    deployed = _code(notebook_deploy.read_cells(client, workspace, item_id))
    generated = _code(workspace_cells(store, project_id, client, workspace, w)[0])
    if deployed != generated:
        return w, False, (f"The deployed notebook's code differs from the generated one ({len(deployed)} vs {len(generated)} code cell(s)). "
                          "Deploy the generated notebook (or regenerate from the code that is deployed), then bind it.")
    return w, True, f"The deployed notebook's {len(generated)} code cell(s) equal the generated notebook."


def bind_deployed(store: ProjectStore, project_id: str, client: FabricClient, workspace: str, ident: str, item_id: str, *, by: str,
                  resolutions: Optional[Path] = None, now: Optional[datetime] = None) -> WorkloadEntry:
    """Record that this workload is deployed as ``item_id``. Only when ``check_deployed`` passes; the person who binds it accepts the
    generated notebook, so the approval is recorded with their name (there is no separate approval step in this path)."""
    if not by.strip():
        raise LifecycleError("Say who is binding the notebook.")
    w, ok, why = check_deployed(store, project_id, client, workspace, ident, item_id)
    if not ok:
        raise LifecycleError(why)
    m = store.load_workload_manifest(project_id)
    w = _find(m, w.id)
    item_id = item_id.strip().strip("{}").lower()
    at = now or datetime.now(timezone.utc)
    _record_item(w, workspace, item_id)
    w.approval = Approval(status="approved", by=by.strip(), at=at, note=f"Accepted by binding the deployed notebook {item_id}: {why}"[:300])
    if w.execution.status == "not_run" and (ran := recorded_run(store, project_id, w, item_id, workspace)) is not None:
        w.execution = ran
    m.summary = summarize(m)
    m.updated_at = at
    store.save_workload_manifest(project_id, m)
    if resolutions is not None:
        notebook_deploy.bind_resolutions(resolutions, w.name, item_id, workspace)
    return w
