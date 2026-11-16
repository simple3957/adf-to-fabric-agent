"""A deployable package of the migration in Fabric's source-control (Git) format: what a client deploys without this tool and without the
Fabric CLI (since 2026-11-15, the user's request: "a working ARM template for Fabric").

Fabric items are not ARM resources (ARM covers only the capacity). Their equivalent is the item-definition format Fabric's Git integration
reads and writes (learn.microsoft.com/fabric/cicd/git-integration/source-code-format): one folder per item named
``<display name>.<type>`` with a ``.platform`` file (``$schema``, ``metadata.type``/``displayName``, ``config.version`` "2.0",
``config.logicalId``) and the item's definition files. Such a folder is deployed by syncing it into a Git-connected workspace, by
Microsoft's ``fabric-cicd`` library (``publish_all_items``), or through the REST API.

What goes in (``projects/<p>/package/``):

* every converted pipeline that is complete (no ``<item:...>`` / ``<connection:...>`` placeholder and no fail-fast stand-in):
  ``<name>.DataPipeline/pipeline-content.json``;
* every generated notebook a packaged pipeline runs and a person approved: ``<name>.Notebook/notebook-content.py`` in the format
  Fabric itself returns (read back live on 2026-11-15: ``# Fabric notebook source``, ``# PARAMETERS CELL`` / ``# CELL`` markers, a
  ``# META`` block holding ``dependencies.environment``);
* every approved Fabric Environment those notebooks use: ``<name>.Environment/Libraries/...`` (fabric-cicd publishes it after
  creating it; a Git sync only stages it, so it must be published in Fabric afterwards);
* the Variable Library holding the factory's global parameters, when a packaged pipeline reads it:
  ``<name>.VariableLibrary/variables.json`` + ``settings.json`` (pipelines refer to it by name; fabric-cicd publishes it first).

References between them are written the way Fabric's own export writes them: the referenced item's logicalId in place of its id (a
pipeline's ``notebookId``, an Invoke pipeline's ``pipelineId``, a notebook's ``environmentId``), and the all-zero GUID for the workspace;
Git sync and fabric-cicd resolve both in the target workspace (Microsoft, cross-workspace-dependency-binding). Connections never travel
(credentials stay where they are): every connection id is listed in ``parameter.yml`` to be replaced per target, as fabric-cicd documents,
and so is any OneLake path that names this project's workspace or lakehouse. Nothing that is not complete is put in: what was left out
is listed, with why, in the package's README.md and in the result.
"""

from __future__ import annotations

import base64
import json
import re
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from adf2fabric.project.store import ProjectStore

PLATFORM_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
ZERO = "00000000-0000-0000-0000-000000000000"
_NS = uuid.UUID("5d0c7a9e-3f43-4c7b-9a3e-0adf2fab1c00")  # logicalIds are derived from project + item, so a rebuild keeps them
_GUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_PLACEHOLDER = re.compile(r"<(item|connection|workspace)[:>][^>]*>?")
_ONELAKE_NAME = re.compile(r"abfss://([^@/\s\"']+)@onelake\.dfs\.fabric\.microsoft\.com/([^/\s\"']+)\.Lakehouse", re.I)
_BAD_NAME = re.compile(r'[\\/:*?"<>|]')


@dataclass
class PackageItem:
    kind: str  # DataPipeline | Notebook | Environment | VariableLibrary
    name: str
    folder: str
    logical_id: str


@dataclass
class PackageResult:
    root: Path
    items: list[PackageItem] = field(default_factory=list)
    left_out: list[str] = field(default_factory=list)  # "<kind> <name>: why"
    parameters: list[str] = field(default_factory=list)  # what parameter.yml asks the deployer to set


def logical_id(project_id: str, kind: str, name: str) -> str:
    return str(uuid.uuid5(_NS, f"{project_id}/{kind}/{name.lower()}"))


def platform(kind: str, name: str, lid: str, description: str = "") -> str:
    meta: dict[str, Any] = {"type": kind, "displayName": name}
    if description:
        meta["description"] = description[:256]
    return json.dumps({"$schema": PLATFORM_SCHEMA, "metadata": meta, "config": {"version": "2.0", "logicalId": lid}}, indent=2) + "\n"


def notebook_source(nb: dict, environment_lid: Optional[str]) -> str:
    """An ipynb notebook in Fabric's .py source format (as Fabric returns it from getDefinition)."""
    out = ["# Fabric notebook source\n"]
    if environment_lid:
        meta = {"dependencies": {"environment": {"environmentId": environment_lid, "workspaceId": ZERO}}}
        out.append("\n# METADATA ********************\n\n" + "".join(f"# META {line}\n" for line in json.dumps(meta, indent=2).splitlines()))
    for cell in nb.get("cells") or []:
        src = cell.get("source", "")
        text = "".join(src) if isinstance(src, list) else str(src)
        if cell.get("cell_type") == "markdown":
            out.append("\n# MARKDOWN ********************\n\n" + "".join(f"# {line}\n" if line else "#\n" for line in text.splitlines()))
            continue
        tags = (cell.get("metadata") or {}).get("tags") or []
        marker = "PARAMETERS CELL" if "parameters" in tags else "CELL"
        out.append(f"\n# {marker} ********************\n\n{text.rstrip()}\n")
    return "\n".join(out).replace("\n\n\n\n", "\n\n\n")


def _safe(name: str) -> str:
    return _BAD_NAME.sub(lambda m: f"&#{ord(m.group(0))};", name)


def _walk(acts: Any):
    for a in acts if isinstance(acts, list) else []:
        if isinstance(a, dict):
            yield a
            tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
            for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
                yield from _walk(tp.get(key))
            for case in tp.get("cases") or []:
                yield from _walk(case.get("activities") if isinstance(case, dict) else None)


def _bound_ids(project_dir: Path):
    """(kind, name, id) of every notebook / pipeline id saved for any workspace: the current resolutions and the kept ones."""
    files = [project_dir / "conversion" / "resolutions.json"]
    kept = project_dir / "conversion" / "bindings-by-workspace"
    files += sorted(kept.glob("*.json")) if kept.is_dir() else []
    for f in files:
        try:
            rows = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for b in rows if isinstance(rows, list) else []:
            key = str(b.get("key", "")) if isinstance(b, dict) else ""
            if ":" in key and b.get("value") and key.split(":", 1)[0].lower() in ("notebook", "pipeline"):
                yield key.split(":", 1)[0].lower(), key.split(":", 1)[1], str(b["value"]).lower()


def build(store: ProjectStore, project_id: str, out_dir: Optional[Path] = None) -> PackageResult:
    """Write the package (replacing an earlier one in the same folder) and say what is in it and what was left out."""
    project_dir = store.project_dir(project_id)
    root = out_dir or project_dir / "package"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    res = PackageResult(root=root)
    conv = store.load_conversion(project_id)
    wm = store.load_workload_manifest(project_id) if store.has_workload_manifest(project_id) else None

    # notebooks and environments a person approved, and every id each notebook has had in any workspace (so a pipeline bound in any of
    # them is recognised)
    notebooks: dict[str, Any] = {}  # name (lower) -> workload
    nb_by_id: dict[str, str] = {}
    for w in (wm.workloads if wm else []):
        if w.artifact.kind == "fabric_notebook" and w.approval.status == "approved" and w.artifact.file:
            name = w.artifact.display_name or w.name
            notebooks[name.lower()] = w
            for i in list((w.artifact.fabric_item_ids or {}).values()) + [w.artifact.fabric_item_id or ""]:
                if i:
                    nb_by_id[i.lower()] = name
    pipe_by_id: dict[str, str] = {}  # a child pipeline's item id in any workspace -> its source name
    for kind, name, value in _bound_ids(project_dir):
        if kind == "notebook":
            nb_by_id.setdefault(value, name)
        elif kind == "pipeline":
            pipe_by_id.setdefault(value, name)
    envs = {e.id: e for e in (wm.environments if wm else [])}
    pipe_lid = {p.fabric_pipeline_name.lower(): logical_id(project_id, "DataPipeline", p.fabric_pipeline_name) for p in conv.pipelines}
    pipe_by_source = {p.source_pipeline.lower(): p.fabric_pipeline_name for p in conv.pipelines}

    used_notebooks: set[str] = set()
    connections: dict[str, set[str]] = {}
    for p in sorted(conv.pipelines, key=lambda x: x.fabric_pipeline_name.lower()):
        name = p.fabric_pipeline_name
        path = project_dir / "conversion" / str(p.artifact_path or "")
        if not p.artifact_path or not path.is_file():
            res.left_out.append(f"DataPipeline {name}: it has no generated definition ({p.status.value})")
            continue
        content = json.loads(path.read_text(encoding="utf-8"))
        text = json.dumps(content)
        acts = list(_walk((content.get("properties") or {}).get("activities")))
        fail = [a["name"] for a in acts if a.get("type") == "Fail" and "not converted" in json.dumps(a.get("typeProperties", {})).lower()]
        unbound = sorted(set(m.group(0) for m in _PLACEHOLDER.finditer(text) if not m.group(0).startswith("<item:pipeline:")))
        if fail or unbound:
            res.left_out.append(f"DataPipeline {name}: " + ("activities not converted: " + ", ".join(fail) if fail else "")
                                + ("; " if fail and unbound else "") + ("unbound: " + ", ".join(unbound[:4]) if unbound else ""))
            continue
        missing: list[str] = []
        for a in acts:
            tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
            if a.get("type") == "TridentNotebook":
                nb = nb_by_id.get(str(tp.get("notebookId", "")).lower())
                if nb is None or nb.lower() not in notebooks:
                    missing.append(f"notebook {tp.get('notebookId')} of '{a.get('name')}' is not an approved generated notebook of this project")
                    continue
                tp["notebookId"] = logical_id(project_id, "Notebook", nb)
                used_notebooks.add(nb.lower())
            if a.get("type") in ("InvokePipeline", "ExecutePipeline"):
                ref = str(tp.get("pipelineId", ""))
                m = re.match(r"^<item:pipeline:(.+)>$", ref)
                source = m.group(1) if m else pipe_by_id.get(ref.lower())  # a placeholder, or an id bound in some workspace
                child = pipe_by_source.get(source.lower(), source) if source else None
                if child is None or child.lower() not in pipe_lid:
                    missing.append(f"child pipeline of '{a.get('name')}' is not one of this project's pipelines")
                    continue
                tp["pipelineId"] = pipe_lid[child.lower()]
            for key in ("workspaceId",):
                if isinstance(tp.get(key), str) and _GUID.fullmatch(tp[key]):
                    tp[key] = ZERO
            for ref in re.findall(r'"connection":\s*"([0-9a-fA-F-]{36})"', json.dumps(a)):
                connections.setdefault(ref.lower(), set()).add(f"{name}/{a.get('name')}")
        if missing:
            res.left_out.append(f"DataPipeline {name}: " + "; ".join(missing))
            continue
        folder = f"{_safe(name)}.DataPipeline"
        (root / folder).mkdir()
        (root / folder / "pipeline-content.json").write_text(json.dumps(content, indent=2) + "\n", encoding="utf-8")
        lid = pipe_lid[name.lower()]
        (root / folder / ".platform").write_text(platform("DataPipeline", name, lid, f"Converted from ADF pipeline {p.source_pipeline}."),
                                                 encoding="utf-8")
        res.items.append(PackageItem("DataPipeline", name, folder, lid))

    # notebooks the packaged pipelines run, and the environments they use
    onelake_names: dict[str, set[str]] = {}
    used_envs: set[str] = set()
    for low in sorted(used_notebooks):
        w = notebooks[low]
        name = w.artifact.display_name or w.name
        env = envs.get(w.fabric_environment) if w.fabric_environment else None
        if w.fabric_environment and (env is None or env.approval.status != "approved" or not env.definition_file):
            res.left_out.append(f"Notebook {name}: its Environment {w.fabric_environment} is not generated and approved")
            continue
        nb = json.loads((project_dir / str(w.artifact.file)).read_text(encoding="utf-8"))
        env_lid = logical_id(project_id, "Environment", env.name) if env else None
        src = notebook_source(nb, env_lid)
        for m in _ONELAKE_NAME.finditer(src):
            onelake_names.setdefault(f"{m.group(1)}@onelake.dfs.fabric.microsoft.com/{m.group(2)}.Lakehouse", set()).add(name)
        folder = f"{_safe(name)}.Notebook"
        (root / folder).mkdir()
        (root / folder / "notebook-content.py").write_text(src, encoding="utf-8")
        lid = logical_id(project_id, "Notebook", name)
        (root / folder / ".platform").write_text(platform("Notebook", name, lid, f"Generated from {w.source_item}."), encoding="utf-8")
        res.items.append(PackageItem("Notebook", name, folder, lid))
        if env:
            used_envs.add(env.id)
    for eid in sorted(used_envs):
        env = envs[eid]
        definition = json.loads((project_dir / str(env.definition_file)).read_text(encoding="utf-8"))
        folder = f"{_safe(env.name)}.Environment"
        for part in definition.get("parts") or []:
            target = root / folder / part["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(base64.b64decode(part["payload"]))
        lid = logical_id(project_id, "Environment", env.name)
        (root / folder / ".platform").write_text(platform("Environment", env.name, lid, f"Libraries of Databricks job {env.job_id} "
                                                          f"environment '{env.environment_key}'."), encoding="utf-8")
        res.items.append(PackageItem("Environment", env.name, folder, lid))

    # the Variable Library the packaged pipelines read (the factory's global parameters)
    from adf2fabric.agent.conversion import variable_library

    packaged = [json.loads((root / i.folder / "pipeline-content.json").read_text(encoding="utf-8")) for i in res.items if i.kind == "DataPipeline"]
    generated = {k.lower(): (k, v) for k, v in variable_library.load(project_dir / "conversion").items()}
    for lib in sorted(variable_library.referenced(packaged)):
        name, parts = generated.get(lib.lower(), (lib, None))
        if parts is None:
            res.left_out.append(f"VariableLibrary {lib}: not in the conversion output (run convert again); the pipelines that read it fail without it")
            continue
        folder = f"{_safe(name)}.VariableLibrary"
        (root / folder).mkdir()
        for part, text in parts.items():
            (root / folder / part).write_text(text, encoding="utf-8")
        lid = logical_id(project_id, "VariableLibrary", name)
        (root / folder / ".platform").write_text(platform("VariableLibrary", name, lid, "Global parameters of the ADF factory."), encoding="utf-8")
        res.items.append(PackageItem("VariableLibrary", name, folder, lid))

    _parameters(root, res, connections, onelake_names)
    _readme(root, res, project_id)
    return res


def _parameters(root: Path, res: PackageResult, connections: dict[str, set[str]], onelake: dict[str, set[str]]) -> None:
    """parameter.yml (fabric-cicd's format): what differs per target workspace. The values are this project's, to be replaced."""
    lines = ["# What differs per target. fabric-cicd replaces find_value with replace_value[<environment>] when it publishes",
             "# (FabricWorkspace(..., environment=\"TARGET\")). Replace each TARGET value before deploying; the values below are this",
             "# project's own, so leaving them deploys against the source project's connections and lakehouse.", "find_replace:"]
    for cid, users in sorted(connections.items()):
        lines += [f"  # Fabric connection used by: {', '.join(sorted(users)[:4])}" + (" ..." if len(users) > 4 else ""),
                  f'  - find_value: "{cid}"', '    replace_value:', f'      TARGET: "{cid}"', '    item_type: "DataPipeline"']
        res.parameters.append(f"connection {cid} (used by {len(users)} activity(ies))")
    for path, users in sorted(onelake.items()):
        lines += [f"  # OneLake path (workspace and lakehouse by name) in: {', '.join(sorted(users))}",
                  f'  - find_value: "{path}"', '    replace_value:', f'      TARGET: "{path}"', '    item_type: "Notebook"']
        res.parameters.append(f"OneLake path {path} (in {len(users)} notebook(s))")
    if len(lines) == 4:
        lines.append("  []")
    (root / "parameter.yml").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _readme(root: Path, res: PackageResult, project_id: str) -> None:
    by = {}
    for i in res.items:
        by.setdefault(i.kind, []).append(i.name)
    text = [f"# Fabric package: {project_id}", "",
            "Generated by adf2fabric in Fabric's source-control (Git) format: one folder per item, each with a `.platform` file.",
            "Items refer to each other by logicalId; Git sync and fabric-cicd resolve them in the target workspace. Nothing here holds a",
            "credential: connections are referenced by id and listed in `parameter.yml`.", "", "## Items", ""]
    for kind in ("DataPipeline", "Notebook", "Environment", "VariableLibrary"):
        if by.get(kind):
            text.append(f"* {kind}: " + ", ".join(sorted(by[kind])))
    text += ["", "## Left out (not complete, so not packaged)", ""] + ([f"* {x}" for x in res.left_out] or ["* nothing"])
    text += ["", "## Before deploying", "",
             "1. The Fabric connections in `parameter.yml` must exist in the target tenant (credentials are entered there, never shipped):",
             "   set each `TARGET` value to the target's connection id.",
             "2. Notebooks that read data through OneLake name a workspace and lakehouse: set those `TARGET` values to the target's, and",
             "   create that lakehouse (with the same shortcuts) first.",
             "3. A Variable Library holds the factory's global parameters with the factory's values; set the target's values in it (or add a",
             "   value set) after deploying. fabric-cicd overwrites a library of the same name in the target.", "",
             "## Deploy with fabric-cicd (no Fabric CLI)", "", "```python",
             "from azure.identity import AzureCliCredential",
             "from fabric_cicd import FabricWorkspace, publish_all_items",
             'ws = FabricWorkspace(workspace_id="<target workspace id>", repository_directory="<this folder>", environment="TARGET",',
             '                     item_type_in_scope=["VariableLibrary", "Environment", "Notebook", "DataPipeline"],',
             '                     token_credential=AzureCliCredential())',
             "publish_all_items(ws)  # Variable Libraries, then Environments (published), notebooks, pipelines", "```", "",
             "Or commit this folder to the branch of a Git-connected workspace and update it from Git; then publish each Environment in",
             "Fabric (a Git sync only stages its libraries)."]
    (root / "README.md").write_text("\n".join(text) + "\n", encoding="utf-8")
