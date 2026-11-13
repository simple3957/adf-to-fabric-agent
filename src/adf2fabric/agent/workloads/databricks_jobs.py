"""The Databricks job behind an ADF ``DatabricksJob`` activity: its structure, and whether Fabric notebooks can run it.

An ADF ``DatabricksJob`` activity holds only a ``jobId``; what the job runs is defined in Databricks. With an offered credential,
``workloads-assess`` reads the job once (Jobs API ``GET /api/2.2/jobs/get``) and keeps its structure in
``projects/<p>/databricks/jobs/<id>.json``: task keys and types, notebook and Python file paths, dependencies, run conditions,
library names, and (since 2026-11-13, by the user's decision) the VALUES of job parameters, notebook base parameters and Python
task arguments, because they cannot be mapped without them. A value that references a Databricks secret or looks like a key,
token or password is never stored: its name is listed under ``withheld`` and the task that needs it is not migrated. No cluster
or compute settings are kept. Conversion reads that file, never Databricks.

Fabric runs the job as notebooks when every task is a notebook task or a Python file task from the workspace (the Python file
becomes a notebook whose ``sys.argv[N]`` are parameter-cell variables ``argv_N``), no task installs libraries, every run condition
maps to a Fabric dependency condition, and every parameter value is a literal or a whole ``{{job.parameters.<name>}}`` reference.
Job parameters reach every notebook task by name, as Databricks pushes them to tasks that take key-value parameters (the live run
of 2026-11-13 showed the value the ADF activity passed reaching the notebook); a Python file task gets only its own arguments.
Anything else is a reason, never a guess.
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
_TASK_KINDS = ("notebook", "spark_python")  # the task types Fabric notebooks can run
# what makes a parameter value a secret that is never written to the project
_SECRET_VALUE = re.compile(r"\{\{\s*secrets/|dapi[0-9a-f]{32}|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}|AccountKey=|SharedAccessSignature=|"
                           r"(?:^|[?&])sig=|(?:password|pwd)\s*=", re.I)
_SECRET_NAME = re.compile(r"(?i)(password|passwd|pwd|secret|token|apikey|api_key|accountkey|account_key|\bsas\b|credential)")
_JOB_REF = re.compile(r"^\{\{\s*job\.parameters\.([A-Za-z0-9_.-]+)\s*\}\}$")
_ANY_REF = re.compile(r"\{\{.*?\}\}")


def is_secret(name: str, value: Any) -> bool:
    """A value that must never be stored: it references a Databricks secret, or looks like a key, token or password."""
    return isinstance(value, str) and bool(_SECRET_VALUE.search(value) or (_SECRET_NAME.search(name or "") and value and not _JOB_REF.match(value)))


def notebook_name(path: Any) -> str:
    """The Fabric notebook a task's code becomes: the last part of its workspace path, without a ``.py`` extension."""
    last = path.rstrip("/").rsplit("/", 1)[-1] if isinstance(path, str) else ""
    return last[:-3] if last.lower().endswith(".py") else last


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


def _keep(name: str, value: Any, withheld: list[str], where: str) -> Optional[str]:
    """A parameter value as stored: a string, or None (and its name in ``withheld``) when it is a secret or not a string."""
    if value is None:
        return ""
    if not isinstance(value, (str, int, float, bool)) or is_secret(name, str(value)):
        withheld.append(where)
        return None
    return str(value).lower() if isinstance(value, bool) else str(value)


def structure(job: dict) -> dict:
    """What is kept of a Jobs API ``get`` response: names, shape and non-secret parameter values; no cluster settings."""
    s = job.get("settings") if isinstance(job.get("settings"), dict) else {}
    withheld: list[str] = []
    tasks = []
    for t in s.get("tasks") or []:
        if not isinstance(t, dict):
            continue
        kind = _kind(t)
        key = str(t.get("task_key") or "")
        nb = t.get("notebook_task") if kind == "notebook" else {}
        py = t.get("spark_python_task") if kind == "spark_python" else {}
        base = nb.get("base_parameters") if isinstance(nb.get("base_parameters"), dict) else {}
        args = py.get("parameters") if isinstance(py.get("parameters"), list) else []
        task = {
            "key": key,
            "kind": kind,
            "notebook_path": normalize_path(nb.get("notebook_path")) if kind == "notebook" else None,
            "notebook_source": str(nb.get("source") or "WORKSPACE").upper() if kind == "notebook" else None,
            "depends_on": [{"task_key": str(d.get("task_key") or ""), **({"outcome": str(d["outcome"])} if "outcome" in d else {})}
                           for d in t.get("depends_on") or [] if isinstance(d, dict)],
            "run_if": str(t.get("run_if") or "ALL_SUCCESS").upper(),
            "parameter_names": sorted(str(k) for k in base),
            "base_parameters": {str(k): kept for k in sorted(base)
                                if (kept := _keep(str(k), base[k], withheld, f"task '{key}' base parameter '{k}'")) is not None},
            "libraries": _libraries(t.get("libraries")),
        }
        if kind == "spark_python":
            task["python_file"] = normalize_path(py.get("python_file")) if isinstance(py.get("python_file"), str) else None
            task["python_source"] = str(py.get("source") or ("WORKSPACE" if str(py.get("python_file") or "").startswith("/") else "")).upper()
            kept = [_keep(f"argument {i}", a, withheld, f"task '{key}' argument {i}") for i, a in enumerate(args, 1)]
            task["arguments"] = [a for a in kept if a is not None]
            task["argument_count"] = len(args)
        tasks.append(task)
    params = [p for p in s.get("parameters") or [] if isinstance(p, dict) and p.get("name")]
    job_params = {str(p["name"]): kept for p in params
                  if (kept := _keep(str(p["name"]), p.get("default"), withheld, f"job parameter '{p['name']}'")) is not None}
    return {"job_id": str(job.get("job_id") or ""), "name": str(s.get("name") or ""),
            "parameter_names": sorted(str(p["name"]) for p in params), "parameters": dict(sorted(job_params.items())),
            "withheld": sorted(withheld), "tasks": tasks}


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
    # per task key: the Fabric notebook parameter name -> ("literal", text) or ("job", job parameter name)
    parameters: dict[str, dict[str, tuple[str, str]]] = field(default_factory=dict)
    job_parameters: dict[str, str] = field(default_factory=dict)  # job parameter -> its default in the job


def code_path(task: dict) -> Optional[str]:
    """The workspace path of the code a task runs: its notebook, or its Python file."""
    return task.get("notebook_path") if task.get("kind") == "notebook" else task.get("python_file")


def _value(v: str, where: str, job_params: dict[str, str], reasons: list[str]) -> Optional[tuple[str, str]]:
    m = _JOB_REF.match(v.strip())
    if m:
        if m.group(1) not in job_params:
            reasons.append(f"{where} refers to job parameter '{m.group(1)}', which the job does not define")
            return None
        return ("job", m.group(1))
    if _ANY_REF.search(v):
        reasons.append(f"{where} uses the dynamic value reference {_ANY_REF.search(v).group(0)}, which is not mapped")
        return None
    return ("literal", v)


def fabric_shape(struct: dict, activity_parameters: Any = None) -> FabricShape:
    """Whether Fabric notebooks can run the job, and what each task's notebook receives. ``activity_parameters`` is the ADF
    activity's ``jobParameters`` (a dict; their values may be ADF expressions and are converted by the caller)."""
    tasks = [t for t in struct.get("tasks") or [] if isinstance(t, dict)]
    shape = FabricShape()
    if not tasks:
        shape.reasons.append("the job has no tasks")
        return shape
    legacy = ("parameters" not in struct and struct.get("parameter_names")) or any(
        t.get("parameter_names") and "base_parameters" not in t for t in tasks)
    if legacy:
        shape.reasons.append("the job was read before parameter values were kept, so its parameters cannot be mapped: read it again "
                             "(workloads-assess with a Databricks credential)")
    for w in struct.get("withheld") or []:
        shape.reasons.append(f"{w} references a secret or looks like one, so its value is not stored and cannot be passed to Fabric")
    job_params = dict(struct.get("parameters") or {})
    shape.job_parameters = job_params
    whole_expression = isinstance(activity_parameters, dict) and activity_parameters.get("type") == "Expression" \
        and set(activity_parameters) == {"value", "type"}
    passed = activity_parameters if isinstance(activity_parameters, dict) and not whole_expression else {}
    if activity_parameters and (whole_expression or not isinstance(activity_parameters, dict)):
        shape.reasons.append("the activity's job parameters are an expression, so the names it passes are not known")
    for name in passed:
        if name not in job_params and not legacy:
            shape.reasons.append(f"the activity passes job parameter '{name}', which the job does not define")
    keys = [t.get("key") for t in tasks]
    if len(set(keys)) != len(keys) or not all(keys):
        shape.reasons.append("the job's task keys are missing or repeated")
        return shape
    for t in tasks:
        k = t["key"]
        if t.get("kind") not in _TASK_KINDS:
            shape.reasons.append(f"task '{k}' is a {t.get('kind')} task; only notebook and Python file tasks become Fabric notebooks")
            continue
        params: dict[str, tuple[str, str]] = {}
        if t.get("kind") == "notebook":
            if not t.get("notebook_path"):
                shape.reasons.append(f"task '{k}' names no notebook")
            if t.get("notebook_source") != "WORKSPACE":
                shape.reasons.append(f"task '{k}' runs its notebook from a Git repository, not the workspace")
            params = {jp: ("job", jp) for jp in job_params}  # Databricks pushes job parameters to notebook tasks by name
            for name, v in (t.get("base_parameters") or {}).items():
                if name in job_params:
                    shape.reasons.append(f"task '{k}' base parameter '{name}' has the name of a job parameter; which value wins is not mapped")
                    continue
                got = _value(v, f"task '{k}' base parameter '{name}'", job_params, shape.reasons)
                if got is not None:
                    params[name] = got
        else:
            if not t.get("python_file"):
                shape.reasons.append(f"task '{k}' names no Python file")
            elif t.get("python_source") != "WORKSPACE":
                shape.reasons.append(f"task '{k}' runs a Python file that is not in the workspace ({t.get('python_file')}); only workspace files are read")
            for i, v in enumerate(t.get("arguments") or [], 1):
                got = _value(v, f"task '{k}' argument {i}", job_params, shape.reasons)
                if got is not None:
                    params[f"argv_{i}"] = got
        shape.parameters[k] = params
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


def fabric_value(source: tuple[str, str], shape: FabricShape, activity_parameters: Any) -> Any:
    """The value a task parameter gets in Fabric, still in ADF form: the literal text, or the job parameter's value as the ADF
    activity passes it (which may be an expression), else the job's default."""
    kind, v = source
    if kind == "literal":
        return v
    passed = activity_parameters if isinstance(activity_parameters, dict) else {}
    return passed[v] if v in passed else shape.job_parameters.get(v, "")


def task_activity_name(activity: str, task_key: str, single: bool) -> str:
    """The Fabric activity that runs one task: the ADF activity's own name for a one-task job, else ``<activity>_<task key>``."""
    return activity if single else f"{activity}_{re.sub(r'[^A-Za-z0-9_-]+', '_', task_key)}"


def terminal_tasks(shape: FabricShape) -> list[str]:
    """Task keys no other task depends on: the job has finished when these have."""
    needed = {d.get("task_key") for t in shape.tasks for d in t.get("depends_on") or []}
    return [t["key"] for t in shape.tasks if t["key"] not in needed]
