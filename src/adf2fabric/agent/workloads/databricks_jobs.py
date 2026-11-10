"""The Databricks job behind an ADF ``DatabricksJob`` activity: its structure, and whether Fabric notebooks can run it.

An ADF ``DatabricksJob`` activity holds only a ``jobId``; what the job runs is defined in Databricks. With an offered credential,
``workloads-assess`` reads the job once (Jobs API ``GET /api/2.2/jobs/get``) and keeps only its STRUCTURE in
``projects/<p>/databricks/jobs/<id>.json``: task keys and types, notebook paths, dependencies, run conditions, the NAMES of
parameters, and library names. No parameter values, no cluster or compute settings, no secrets. Conversion reads that file,
never Databricks.

Fabric can run the job as notebooks only when every task is a notebook task from the workspace, nothing passes parameters, no task
installs libraries and every run condition maps to a Fabric dependency condition. Anything else is a reason, never a guess.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# Databricks `run_if` -> the Fabric dependency condition that means the same thing for each upstream task.
# ALL_SUCCESS: run when every dependency succeeded; ALL_DONE: run when every dependency finished, whatever the result.
RUN_IF_CONDITION = {"ALL_SUCCESS": "Succeeded", "ALL_DONE": "Completed"}
_LIBRARY_KEYS = {"pypi": "package", "cran": "package", "maven": "coordinates"}
_LIBRARY_FILES = ("jar", "whl", "egg", "requirements")


def job_id_of(raw: Any) -> Optional[str]:
    """The literal job id of an ADF DatabricksJob activity's typeProperties, or None when it is missing or an expression."""
    v = raw.get("jobId") if isinstance(raw, dict) else None
    if isinstance(v, int) and not isinstance(v, bool):
        v = str(v)
    return v if isinstance(v, str) and v.isdigit() else None


def normalize_path(p: Any) -> Optional[str]:
    """A workspace notebook path as the workspace export API takes it: ``/Workspace/Shared/x`` and ``/Shared/x`` are one notebook."""
    if not isinstance(p, str) or not p:
        return None
    return p[len("/Workspace"):] if p.startswith("/Workspace/") else p


def _kind(task: dict) -> str:
    return next((k[: -len("_task")] for k, v in task.items() if k.endswith("_task") and isinstance(v, dict)), "unknown")


def _libraries(raw: Any) -> list[dict]:
    """Library names only, in the shape ADF uses for an activity's libraries (so the existing library resolution reads them)."""
    out: list[dict] = []
    for lib in raw if isinstance(raw, list) else []:
        if not isinstance(lib, dict):
            continue
        for k, v in lib.items():
            if k in _LIBRARY_KEYS and isinstance(v, dict) and isinstance(v.get(_LIBRARY_KEYS[k]), str):
                out.append({k: {_LIBRARY_KEYS[k]: v[_LIBRARY_KEYS[k]]}})
            elif k in _LIBRARY_FILES and isinstance(v, str):
                out.append({k: v})
    return out


def structure(job: dict) -> dict:
    """What is kept of a Jobs API ``get`` response: names and shape, no values."""
    s = job.get("settings") if isinstance(job.get("settings"), dict) else {}
    tasks = []
    for t in s.get("tasks") or []:
        if not isinstance(t, dict):
            continue
        kind = _kind(t)
        nb = t.get("notebook_task") if kind == "notebook" else {}
        tasks.append({
            "key": str(t.get("task_key") or ""),
            "kind": kind,
            "notebook_path": normalize_path(nb.get("notebook_path")) if kind == "notebook" else None,
            "notebook_source": str(nb.get("source") or "WORKSPACE").upper() if kind == "notebook" else None,
            "depends_on": [{"task_key": str(d.get("task_key") or ""), **({"outcome": str(d["outcome"])} if "outcome" in d else {})}
                           for d in t.get("depends_on") or [] if isinstance(d, dict)],
            "run_if": str(t.get("run_if") or "ALL_SUCCESS").upper(),
            "parameter_names": sorted(str(k) for k in (nb.get("base_parameters") or {})) if kind == "notebook" else [],
            "libraries": _libraries(t.get("libraries")),
        })
    return {"job_id": str(job.get("job_id") or ""), "name": str(s.get("name") or ""),
            "parameter_names": sorted(str(p.get("name")) for p in s.get("parameters") or [] if isinstance(p, dict) and p.get("name")),
            "tasks": tasks}


# --- the project's copy ---------------------------------------------------------------------------------------------------------


def job_file(project_dir: Path, job_id: str) -> Path:
    return project_dir / "databricks" / "jobs" / f"{job_id}.json"


def save(project_dir: Path, job_id: str, struct: dict) -> Path:
    f = job_file(project_dir, job_id)
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(struct, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return f


def load(project_dir: Path, job_id: str) -> Optional[dict]:
    f = job_file(project_dir, job_id)
    if not f.is_file():
        return None
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) and isinstance(data.get("tasks"), list) else None


def load_all(project_dir: Path) -> dict[str, dict]:
    """Every job structure kept in the project: job id -> structure."""
    folder = project_dir / "databricks" / "jobs"
    out: dict[str, dict] = {}
    for f in sorted(folder.glob("*.json")) if folder.is_dir() else []:
        data = load(project_dir, f.stem)
        if data is not None:
            out[f.stem] = data
    return out


# --- what Fabric can run -------------------------------------------------------------------------------------------------------


@dataclass
class FabricShape:
    tasks: list[dict] = field(default_factory=list)  # every task, upstream before downstream
    reasons: list[str] = field(default_factory=list)  # why Fabric notebooks cannot run the job as it is; empty = they can


def fabric_shape(struct: dict, activity_passes_parameters: bool = False) -> FabricShape:
    tasks = [t for t in struct.get("tasks") or [] if isinstance(t, dict)]
    shape = FabricShape()
    if not tasks:
        shape.reasons.append("the job has no tasks")
        return shape
    if activity_passes_parameters:
        shape.reasons.append("the activity passes job parameters, and how a Fabric notebook would receive Databricks job parameters is not mapped")
    if struct.get("parameter_names"):
        shape.reasons.append(f"the job defines job parameters ({', '.join(struct['parameter_names'])}), which are not mapped")
    keys = [t.get("key") for t in tasks]
    if len(set(keys)) != len(keys) or not all(keys):
        shape.reasons.append("the job's task keys are missing or repeated")
        return shape
    for t in tasks:
        k = t["key"]
        if t.get("kind") != "notebook":
            shape.reasons.append(f"task '{k}' is a {t.get('kind')} task; only notebook tasks become Fabric notebooks")
            continue
        if not t.get("notebook_path"):
            shape.reasons.append(f"task '{k}' names no notebook")
        if t.get("notebook_source") != "WORKSPACE":
            shape.reasons.append(f"task '{k}' runs its notebook from a Git repository, not the workspace")
        if t.get("parameter_names"):
            shape.reasons.append(f"task '{k}' passes base parameters ({', '.join(t['parameter_names'])}), which are not mapped")
        if t.get("libraries"):
            shape.reasons.append(f"task '{k}' installs libraries, which a Fabric notebook activity cannot")
        if t.get("run_if") not in RUN_IF_CONDITION:
            shape.reasons.append(f"task '{k}' runs on {t.get('run_if')}, which has no Fabric dependency condition")
        for d in t.get("depends_on") or []:
            if d.get("task_key") not in keys:
                shape.reasons.append(f"task '{k}' depends on '{d.get('task_key')}', which is not in the job")
            if "outcome" in d:
                shape.reasons.append(f"task '{k}' depends on an outcome of '{d.get('task_key')}' (a condition task)")
    # upstream before downstream, keeping the job's own order where it is free
    done: list[str] = []
    pending = list(tasks)
    while pending:
        ready = [t for t in pending if all(d.get("task_key") in done or d.get("task_key") not in keys for d in t.get("depends_on") or [])]
        if not ready:
            shape.reasons.append("the job's task dependencies form a cycle")
            shape.tasks = tasks
            return shape
        for t in ready:
            done.append(t["key"])
            pending.remove(t)
            shape.tasks.append(t)
    return shape


def task_activity_name(activity: str, task_key: str, single: bool) -> str:
    """The Fabric activity that runs one task: the ADF activity's own name for a one-task job, else ``<activity>_<task key>``."""
    return activity if single else f"{activity}_{re.sub(r'[^A-Za-z0-9_-]+', '_', task_key)}"


def terminal_tasks(shape: FabricShape) -> list[str]:
    """Task keys no other task depends on: the job has finished when these have."""
    needed = {d.get("task_key") for t in shape.tasks for d in t.get("depends_on") or []}
    return [t["key"] for t in shape.tasks if t["key"] not in needed]
