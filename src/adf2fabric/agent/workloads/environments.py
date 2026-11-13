"""Fabric Environments for the libraries a Databricks job environment installs (since 2026-11-13).

A serverless Databricks job declares its packages in a job ENVIRONMENT (``environments[].spec.dependencies``): PyPI requirements and
``.whl`` files in the workspace. Each environment a migrated task runs in becomes ONE Fabric Environment (the user's decision:
one per job environment), named ``<job name>_<environment key>``:

* PyPI requirements go to ``Libraries/PublicLibraries/environment.yml`` (``dependencies: - pip: [...]``);
* each workspace ``.whl`` is read from Databricks (only when authorised, like notebook code) and goes, as is, to
  ``Libraries/CustomLibraries/<file>``.

Anything else (Volumes, DBFS, URLs, requirements files, pip options) is a reason the job is not migrated, never a guess.

It is approved on its own (the user's decision), tied to the hash of its definition. Deploy creates it in a workspace and publishes it
before the notebooks that use it, never changes an Environment that is already there under that name (it must be bound, the same rule as
notebooks), and attaches it to each notebook through ``metadata.dependencies.environment {environmentId, workspaceId}``.

Checked live in fabric-demo on 2026-11-13 (probe items zz_env_probe*): an Environment created with these definition parts staged both
libraries, ``publish?beta=false`` succeeded in about 5 minutes, the notebook definition kept ``dependencies.environment`` (it comes back
in the ``# META`` block of the .py format too), and a PIPELINE run of that notebook imported the wheel and the PyPI package
(exit value ``high|low|1,234,567``). The key is not in Microsoft's docs; that run is the evidence.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Callable, Optional

from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, DeployError, FabricClient
from adf2fabric.agent.workloads import databricks_jobs
from adf2fabric.project.workloads import Approval, EnvironmentLibrary, FabricEnvironment

PUBLISH_WAIT_SECONDS = 1200  # a full publish took about 5 minutes live; Microsoft says 3-6 minutes typically
_NAME_BAD = re.compile(r"[^A-Za-z0-9_-]+")


def env_id(job_id: str, key: str) -> str:
    return f"{job_id}:{key}"


def display_name(job_name: str, key: str) -> str:
    return (_NAME_BAD.sub("_", f"{job_name}_{key}").strip("_") or "environment")[:256]


def from_jobs(jobs: dict[str, dict], users: dict[str, list[str]]) -> list[FabricEnvironment]:
    """One entry per job environment some migrated task runs in. ``users``: environment id -> workload ids of those tasks."""
    out = []
    for job_id, struct in sorted(jobs.items()):
        for key, deps in sorted((struct.get("environments") or {}).items()):
            eid = env_id(job_id, key)
            if eid not in users:
                continue
            libs, problems = [], []
            for d in deps:
                kind = databricks_jobs.dependency_kind(d)
                if kind is None:
                    problems.append(f"'{d}' is neither a PyPI requirement nor a .whl file in the workspace")
                else:
                    libs.append(EnvironmentLibrary(kind=kind, spec=d.strip()))
            if not libs and not problems:
                continue  # an environment with nothing to install needs no Fabric Environment
            out.append(FabricEnvironment(id=eid, name=display_name(struct.get("name") or job_id, key), job_id=job_id, environment_key=key,
                                         used_by=sorted(users[eid]), libraries=libs, problems=problems))
    return out


def environment_yml(libs: list[EnvironmentLibrary]) -> Optional[str]:
    public = [lib.spec for lib in libs if lib.kind == "public"]
    if not public:
        return None
    return "dependencies:\n  - pip:\n" + "".join(f"      - {p}\n" for p in public)


def generate(env: FabricEnvironment, project_dir: Path, read_file: Optional[Callable[[str], bytes]]) -> None:
    """Read the wheel files (``read_file``: workspace path -> bytes, None when not authorised), write the definition, hash it."""
    env.definition_file, env.sha256 = None, None
    if env.problems:
        return
    parts = []
    for lib in env.libraries:
        if lib.kind != "custom":
            continue
        cache = project_dir / "databricks" / "files" / lib.spec.lstrip("/")
        if read_file is not None:
            try:
                raw = read_file(lib.spec)
            except Exception as exc:  # noqa: BLE001 - said on the environment, never raised over the whole assessment
                env.problems.append(f"'{lib.spec}' could not be read from Databricks: {exc}")
                continue
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_bytes(raw)
        if not cache.is_file():
            env.problems.append(f"'{lib.spec}' has not been read from Databricks (run workloads-generate with a Databricks credential)")
            continue
        raw = cache.read_bytes()
        lib.file = str(cache.relative_to(project_dir)).replace("\\", "/")
        lib.sha256 = hashlib.sha256(raw).hexdigest()
        parts.append({"path": f"Libraries/CustomLibraries/{Path(lib.spec).name}", "payload": base64.b64encode(raw).decode("ascii"),
                      "payloadType": "InlineBase64"})
    if env.problems:
        return
    yml = environment_yml(env.libraries)
    if yml:
        parts.append({"path": "Libraries/PublicLibraries/environment.yml", "payload": base64.b64encode(yml.encode()).decode("ascii"),
                      "payloadType": "InlineBase64"})
    rel = f"workloads/environments/{env.name}/definition.json"
    target = project_dir / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps({"parts": parts}, indent=1, sort_keys=True).encode("utf-8")
    target.write_bytes(data)
    env.definition_file, env.sha256 = rel, hashlib.sha256(data).hexdigest()


def keep_decisions(new: list[FabricEnvironment], old: list[FabricEnvironment]) -> list[FabricEnvironment]:
    """An approval holds only for the same definition; ids in workspaces are kept (the item is there whatever its approval)."""
    prior = {e.id: e for e in old}
    for e in new:
        o = prior.get(e.id)
        if o is None:
            continue
        e.fabric_item_ids, e.published = dict(o.fabric_item_ids), dict(o.published)
        if e.sha256 is None and o.sha256 and o.definition_file:
            # not generated this run (assessment only): keep what was generated, and with it its approval
            e.definition_file, e.sha256 = o.definition_file, o.sha256
            for lib in e.libraries:
                was = next((x for x in o.libraries if x.spec == lib.spec), None)
                if was is not None:
                    lib.file, lib.sha256 = was.file, was.sha256
        if e.sha256 and e.sha256 == o.sha256:
            e.approval = o.approval
    return new


def current(env: FabricEnvironment, project_dir: Path) -> bool:
    """The definition on disk is the one generated (and approved): it was not changed since."""
    f = project_dir / str(env.definition_file or "")
    return bool(env.definition_file) and f.is_file() and hashlib.sha256(f.read_bytes()).hexdigest() == env.sha256


def approve(env: FabricEnvironment, by: str, approve_: bool, note: str, at: Any) -> None:
    env.approval = Approval(status="approved" if approve_ else "rejected", by=by, at=at, note=note[:300])


# --- in a workspace ---------------------------------------------------------------------------------------------------------------


def list_environments(client: FabricClient, workspace: str) -> dict[str, str]:
    """Display name (lowercased) -> id of every Environment in the workspace."""
    found: dict[str, str] = {}
    url, token = f"{FABRIC_API}/workspaces/{workspace}/environments", None
    while True:
        resp = client._call("GET", url + (f"?continuationToken={token}" if token else ""))
        if resp.status_code != 200:
            raise DeployError(f"Listing environments failed (HTTP {resp.status_code}).")
        data = resp.json()
        for it in data.get("value", []):
            found[str(it.get("displayName", "")).lower()] = it["id"]
        token = data.get("continuationToken")
        if not token:
            return found


def publish_state(client: FabricClient, workspace: str, item_id: str) -> str:
    resp = client._call("GET", f"{FABRIC_API}/workspaces/{workspace}/environments/{item_id}")
    if resp.status_code != 200:
        raise DeployError(f"Reading environment {item_id} failed (HTTP {resp.status_code}).")
    return str(((resp.json().get("properties") or {}).get("publishDetails") or {}).get("state") or "")


def ensure(client: FabricClient, workspace: str, env: FabricEnvironment, project_dir: Path, *, apply: bool,
           log: Callable[[str], None] = lambda _m: None, sleep: Callable[[float], None] = time.sleep) -> tuple[str, Optional[str]]:
    """(what happened, environment id) for ``env`` in ``workspace``. Dry run unless ``apply``. Refuses when it is not approved or its
    definition changed since, or when an Environment of its name is already there but not this project's (it is never changed)."""
    ws = workspace.lower()
    known = env.fabric_item_ids.get(ws)
    if env.approval.status != "approved":
        raise DeployError(f"Environment '{env.name}' is not approved (status {env.approval.status}): approve it first "
                          f"(adf2fabric workloads-approve --workload {env.name}).")
    if not current(env, project_dir):
        raise DeployError(f"The definition of environment '{env.name}' changed since it was generated: run workloads-generate again.")
    there = list_environments(client, workspace).get(env.name.lower())
    if known and there == known:
        state = env.published.get(ws, "")
        return (f"already in the workspace ({state or 'publish state not read'})", known)
    if there:
        raise DeployError(f"An environment named '{env.name}' is already in this workspace and is not the one this project created; it is "
                          "never changed here. Bind it, or rename or remove it in Fabric, then deploy again.")
    if not apply:
        return ("Fabric would create and publish this environment", None)
    definition = json.loads((project_dir / str(env.definition_file)).read_text(encoding="utf-8"))
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/environments",
                        {"displayName": env.name, "description": f"Libraries of Databricks job {env.job_id} environment '{env.environment_key}' "
                         "(generated by adf2fabric).", "definition": definition})
    body = client._finish(resp, f"Creating environment '{env.name}'") or {}
    item_id = body.get("id") if isinstance(body.get("id"), str) else list_environments(client, workspace).get(env.name.lower())
    if not item_id:
        raise DeployError(f"Environment '{env.name}' was created but its id could not be read; check the workspace.")
    env.fabric_item_ids[ws] = item_id
    log(f"created environment {env.name}; publishing (Microsoft: typically 3-6 minutes)")
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/environments/{item_id}/staging/publish?beta=false")
    if resp.status_code not in (200, 202):
        raise DeployError(f"Publishing environment '{env.name}' failed (HTTP {resp.status_code}).")
    waited, state = 0.0, ""
    while waited <= PUBLISH_WAIT_SECONDS:
        state = publish_state(client, workspace, item_id)
        if state in ("Success", "Failed", "Cancelled"):
            break
        sleep(20)
        waited += 20
    env.published[ws] = state or "unknown"
    if state != "Success":
        raise DeployError(f"Environment '{env.name}' was created but its publish ended '{state or 'unfinished'}': the notebooks that use it "
                          "are not created. Check it in Fabric and deploy again.")
    return ("created and published", item_id)


def attach(nb: dict, environment_id: str, workspace: str) -> dict:
    """The notebook (ipynb dict) with its environment set the way the live check of 2026-11-13 showed Fabric keeps and uses it."""
    meta = dict(nb.get("metadata") or {})
    deps = dict(meta.get("dependencies") or {})
    deps["environment"] = {"environmentId": environment_id, "workspaceId": workspace.lower()}
    meta["dependencies"] = deps
    return {**nb, "metadata": meta}
