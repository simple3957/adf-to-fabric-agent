"""API and DEPLOYMENT validation (reads only) and RUNTIME validation (from recorded real runs).

* API stage: does the target answer, is the workspace readable and on a capacity, do the connections and items the generated
  pipelines point at exist? Every check is a GET; the stage never writes and never needs anything but read access.
* DEPLOYMENT stage: is each generated pipeline deployed, and is what is deployed the same as what was generated (the pipeline
  definition is read back with ``getDefinition`` and compared after normalisation), are its schedules present and disabled?
* RUNTIME stage: a pipeline's behaviour is only ever established by a RUN. This stage never starts one. It reads the run-and-compare
  record that ``adf2fabric verify`` / ``run --apply`` write, and judges each pipeline by what that run did, refusing a run that
  is older than the artifact it is supposed to vouch for.

A success status from any API proves only what that call proves (the item exists, the definition equals the file); it is never
reported as runtime correctness.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.deployment import verify as verify_mod
from adf2fabric.agent.provisioning.transport import FabricGateway, ProvisionError
from adf2fabric.agent.validation import views
from adf2fabric.agent.validation.diffing import diff
from adf2fabric.agent.validation.static import Collector, Inputs, _TOKEN, _connection_values, _lakehouse_artifacts
from adf2fabric.project.validation import Category as K, ComponentStatus as S, Result as R, Stage

_GUID_LEN = 36


def _gid(x: str) -> bool:
    return len(x) == _GUID_LEN and x.count("-") == 4


def api_and_deployment(inp: Inputs, c: Collector, fab: Optional[FabricGateway], workspace: Optional[str], want_api: bool, want_deploy: bool) -> None:
    if not (want_api or want_deploy):
        return
    why = None
    if fab is None or not workspace:
        why = "No authorised Fabric workspace was given (--workspace-id and a signed-in account with read access)."
    stages = [s for s, w in ((Stage.api, want_api), (Stage.deployment, want_deploy)) if w]
    if why:
        for st in stages:
            if st is Stage.api:
                c.add("deployment.api_access", K.deployment, R.NOT_RUN, why, stage=st, remediation="Run again with --workspace-id and Fabric credentials.")
            else:
                c.add("deployment.pipelines_deployed", K.deployment, R.NOT_RUN, why, stage=st, remediation="Deploy (`adf2fabric deploy --apply`), then run again with --workspace-id.")
        return
    try:
        ws = fab.request("GET", f"workspaces/{workspace}")
    except ProvisionError as exc:
        for st in stages:
            c.add("deployment.api_access", K.deployment, R.FAIL, f"Fabric could not be reached: {exc}", stage=st)
        return
    if ws.status != 200:
        msg = {401: "the sign-in was rejected", 403: "this account cannot read the workspace", 404: "the workspace does not exist or is not visible"}.get(ws.status, f"HTTP {ws.status}")
        for st in stages:
            c.add("deployment.api_access", K.deployment, R.FAIL if ws.status in (403, 404) else R.BLOCKED, f"Workspace {workspace}: {msg}.", stage=st,
                  remediation="Use an account with at least Viewer on the workspace.")
        return
    body = ws.body or {}
    if want_api:
        c.add("deployment.api_access", K.deployment, R.PASS, f"Workspace '{body.get('displayName')}' is readable.", stage=Stage.api,
              evidence=["proved: the token is accepted and the workspace can be read", "not proved: that anything in it works"])
        c.add("deployment.capacity", K.deployment, R.PASS if body.get("capacityId") else R.FAIL,
              "The workspace is on a capacity." if body.get("capacityId") else "The workspace is not assigned to a capacity, so Fabric items cannot run.", stage=Stage.api,
              remediation="" if body.get("capacityId") else "Assign a capacity (provision-plan / provision-apply).")
    try:
        pipes = {p["displayName"].lower(): p["id"] for p in fab.paged(f"workspaces/{workspace}/dataPipelines", "listing pipelines")}
        items = {i["id"].lower(): i for i in fab.paged(f"workspaces/{workspace}/items", "listing items")}
        conns = {x["id"].lower(): x for x in fab.paged("connections", "listing connections")}
    except ProvisionError as exc:
        for st in stages:
            c.add("deployment.listing", K.deployment, R.BLOCKED, f"The workspace could not be listed: {exc}", stage=st, remediation="Grant read access.")
        return
    if want_api:
        _api_references(inp, c, items, conns, fab)
    if want_deploy:
        _deployed(inp, c, fab, workspace, pipes)


def _api_references(inp: Inputs, c: Collector, items: dict, conns: dict, fab: FabricGateway) -> None:
    ids_conn: dict[str, list[str]] = {}
    ids_item: dict[str, list[str]] = {}
    for tp in inp.tgt.pipelines:
        for a in views.pipeline_activities(tp.content or {}):
            lakehouses = {v.lower() for v in _lakehouse_artifacts(a.raw)}
            for v in _connection_values(a.raw):
                if _gid(v):  # a repointed dataset's "connection" is its lakehouse, a workspace item, not a Fabric connection
                    (ids_item if v.lower() in lakehouses else ids_conn).setdefault(v.lower(), []).append(f"{tp.name}/{a.name}")
            for key in ("notebookId", "sparkJobDefinitionId", "pipelineId", "dataflowId"):
                v = a.type_properties.get(key)
                if isinstance(v, str) and _gid(v):
                    ids_item.setdefault(v.lower(), []).append(f"{tp.name}/{a.name}")
    miss = [f"connection {i} used by {', '.join(u[:3])}" for i, u in ids_conn.items() if i not in conns]
    c.add("deployment.connections_exist", K.deployment, R.FAIL if miss else (R.PASS if ids_conn else R.NOT_RUN),
          (f"{len(miss)} connection id(s) in the generated pipelines do not exist (or this account cannot see them)." if miss else
           f"All {len(ids_conn)} bound connection id(s) exist." if ids_conn else "No generated activity is bound to a connection id."),
          stage=Stage.api, evidence=miss, remediation="Create the connection or correct the id in the resolutions file." if miss else "", required=bool(ids_conn))
    if ids_conn and not miss:
        kinds = []
        for i in list(ids_conn)[:5]:
            try:
                r = fab.request("GET", f"connections/{i}")
                ctype = ((r.body or {}).get("credentialDetails") or {}).get("credentialType") if r.status == 200 else None
                kinds.append(f"{i[:8]}...: credential type {ctype or 'not shown'}")
            except ProvisionError:
                pass
        c.add("connections_secrets.credential_types", K.connections_secrets, R.PASS, "Credential TYPES of the bound connections were read (no secret is ever returned).",
              stage=Stage.api, evidence=kinds, required=False)
    missing_items = [f"item {i} used by {', '.join(u[:3])}" for i, u in ids_item.items() if i not in items]
    c.add("deployment.items_exist", K.deployment, R.FAIL if missing_items else (R.PASS if ids_item else R.NOT_RUN),
          (f"{len(missing_items)} notebook / job / pipeline / lakehouse id(s) the generated pipelines use do not exist." if missing_items else
           f"All {len(ids_item)} referenced item id(s) exist." if ids_item else "No generated activity references a Fabric item id."),
          stage=Stage.api, evidence=missing_items, remediation="Deploy the item (workloads-deploy / deploy) and convert again with --resolutions." if missing_items else "",
          required=bool(ids_item))


def _norm(content: dict) -> dict:
    return json.loads(json.dumps(content, sort_keys=True))


def _resolve(node: Any, workspace: str, pipes: dict[str, str]) -> Any:
    """What deployment itself substitutes: <item:pipeline:NAME> -> the deployed pipeline's id, <workspace-id> -> the workspace id. Connection and
    notebook tokens are NOT resolved: deployment refuses unbound ones, so a token still present in a deployed file is a real difference."""
    if isinstance(node, dict):
        return {k: _resolve(v, workspace, pipes) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve(v, workspace, pipes) for v in node]
    if isinstance(node, str):
        if node == "<workspace-id>":
            return workspace
        if node.startswith("<item:pipeline:") and node.endswith(">"):
            return pipes.get(node[len("<item:pipeline:"):-1].lower(), node)
    return node


def _get_definition(fab: FabricGateway, workspace: str, item_id: str) -> Optional[dict]:
    r = fab.request("POST", f"workspaces/{workspace}/dataPipelines/{item_id}/getDefinition", None, ambiguous_retry=False)
    if r.status == 202:
        fab.wait_operation(r, "Reading the pipeline definition")
        op = r.headers.get("x-ms-operation-id") or r.headers.get("location", "").rstrip("/").rsplit("/", 1)[-1]
        r = fab.request("GET", f"operations/{op}/result")
    if r.status != 200:
        raise ProvisionError(f"Reading the definition failed (HTTP {r.status}).", r.status)
    for part in ((r.body or {}).get("definition") or {}).get("parts", []):
        if part.get("path") == "pipeline-content.json":
            return json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
    raise ProvisionError("The definition has no pipeline-content.json part.")


def _deployed(inp: Inputs, c: Collector, fab: FabricGateway, workspace: str, pipes: dict[str, str]) -> None:
    for low, tp in sorted(inp.paired.items()):
        comp = f"pipeline:{tp.name}"
        if tp.content is None:
            continue
        item = pipes.get(tp.name.lower())
        if item is None:
            c.add("deployment.pipeline_deployed", K.deployment, R.FAIL, f"Pipeline '{tp.name}' is not in the workspace.", stage=Stage.deployment, component=comp,
                  remediation="Deploy it: `adf2fabric deploy --apply` (dry run first).")
            c.comp(comp, "pipeline", S.missing, note="not deployed")
            continue
        try:
            live = _get_definition(fab, workspace, item)
        except ProvisionError as exc:
            c.add("deployment.definition_matches", K.deployment, R.BLOCKED, f"The deployed definition of '{tp.name}' could not be read: {exc}", stage=Stage.deployment,
                  component=comp, remediation="Grant read access to the pipeline definition.")
            continue
        d = diff(_norm(_resolve(tp.content, workspace, pipes)), _norm(live or {}))
        c.add("deployment.definition_matches", K.deployment, R.FAIL if d else R.PASS,
              f"The deployed definition of '{tp.name}' differs from the generated file." if d else f"The deployed definition of '{tp.name}' equals the generated file (read back from Fabric).",
              stage=Stage.deployment, component=comp, evidence=d, remediation="Deploy again, or regenerate if the portal copy was edited deliberately." if d else "",)
        if d:
            c.comp(comp, "pipeline", S.altered, diff_=d, note="deployed definition differs from the generated file")
    sched_by_pipe: dict[str, dict] = {}
    for s in inp.tgt.schedules:
        for p in s.get("pipelines", []):
            sched_by_pipe[str(p).lower()] = s
    for p, s in sorted(sched_by_pipe.items()):
        item = pipes.get(p)
        if item is None:
            continue
        try:
            r = fab.request("GET", f"workspaces/{workspace}/items/{item}/jobs/Pipeline/schedules")
        except ProvisionError as exc:
            c.add("deployment.schedules", K.deployment, R.BLOCKED, f"Schedules of '{p}' could not be read: {exc}", stage=Stage.deployment, component=f"pipeline:{p}")
            continue
        live = (r.body or {}).get("value", []) if r.status == 200 else None
        want = len(s.get("schedules", []))
        if live is None:
            c.add("deployment.schedules", K.deployment, R.BLOCKED, f"Schedules of '{p}' could not be read (HTTP {r.status}).", stage=Stage.deployment, component=f"pipeline:{p}")
        else:
            enabled = [x for x in live if x.get("enabled")]
            running_in_source = any(str(((f.data or {}).get("properties") or {}).get("runtimeState", "")).lower() == "started"
                                    for n, f in inp.src.names("trigger").items() if n in {str(t.get("trigger", "")).lower() for t in inp.tgt.schedules if p in [str(x).lower() for x in t.get("pipelines", [])]})
            bad = ([f"{want} schedule(s) expected, {len(live)} deployed"] if len(live) != want else []) + (
                [f"{len(enabled)} schedule(s) are enabled although the source trigger was not running (the tool creates them disabled)"] if enabled and not running_in_source else [])
            c.add("deployment.schedules", K.deployment, R.FAIL if bad else R.PASS,
                  f"Schedules of '{p}' do not match." if bad else f"Schedules of '{p}' are deployed as generated.", stage=Stage.deployment, component=f"pipeline:{p}", evidence=bad)


# =====================================================================================================================
# runtime
# =====================================================================================================================


def runtime(inp: Inputs, c: Collector, project_dir: Path, requested: bool) -> None:
    if not requested:
        c.add("runtime.execution", K.runtime, R.NOT_RUN, "Runtime validation was not requested. Static, API and deployment results say nothing about whether the pipelines run correctly.",
              stage=Stage.runtime, remediation="Run the pipelines (`adf2fabric run --apply`), compare them with ADF (`adf2fabric verify`), then validate with --runtime.")
        return
    rec = verify_mod.load_record(verify_mod.record_path(project_dir))
    by = {k.lower(): v for k, v in rec.items()}
    if not rec:
        c.add("runtime.execution", K.runtime, R.NOT_RUN, "There is no run-and-compare record: no pipeline has been run and compared.", stage=Stage.runtime,
              remediation="Run `adf2fabric verify --apply` (it runs the pipelines and uses capacity) against an authorised workspace.")
        return
    for low, tp in sorted(inp.paired.items()):
        comp = f"pipeline:{tp.name}"
        e = by.get(tp.name.lower())
        if e is None:
            c.add("runtime.execution", K.runtime, R.NOT_RUN, f"Pipeline '{tp.name}' has never been run and compared.", stage=Stage.runtime, component=comp,
                  remediation=f"Run `adf2fabric verify --pipeline {tp.name} --apply`.")
            continue
        art = project_dir / "conversion" / tp.path
        stale = False
        try:
            ran = datetime.fromisoformat(str(e.get("ran_at", "")).replace("Z", "+00:00"))
            ran = ran if ran.tzinfo else ran.replace(tzinfo=timezone.utc)
            stale = art.is_file() and datetime.fromtimestamp(art.stat().st_mtime, timezone.utc) > ran
        except ValueError:
            ran = None
        verdict, status = str(e.get("verdict", "")), str(e.get("fabric_status", ""))
        if stale:
            c.add("runtime.execution", K.runtime, R.BLOCKED, f"The recorded run of '{tp.name}' is older than its generated file, so it does not vouch for the current artifact.",
                  stage=Stage.runtime, component=comp, remediation="Deploy the current file and run the pipeline again.")
        elif status != "Completed":
            c.add("runtime.execution", K.runtime, R.FAIL, f"The last run of '{tp.name}' ended '{status or 'unknown'}'.", stage=Stage.runtime, component=comp,
                  evidence=[str(e.get("detail", ""))[:300]], remediation="Fix the failing activity and run again.")
        elif verdict == "matches":
            c.add("runtime.execution", K.runtime, R.PASS, f"'{tp.name}' ran to completion in Fabric and matched its baseline ({e.get('baseline') or 'baseline'}) on {e.get('matched', 0)} activity(ies): status, copy counts and lookup results.",
                  stage=Stage.runtime, component=comp, evidence=[f"run {e.get('job_id')} at {e.get('ran_at')}"])
        elif verdict == "differs":
            c.add("runtime.execution", K.runtime, R.FAIL, f"'{tp.name}' ran, but its results differ from the baseline.", stage=Stage.runtime, component=comp,
                  evidence=[json.dumps(x)[:200] for x in e.get("differences", [])[:6]], remediation="Investigate the differing activities; the migrated pipeline does not behave like the source.")
        else:
            c.add("runtime.execution", K.runtime, R.BLOCKED, f"'{tp.name}' ran, but could not be fully compared ({verdict}).", stage=Stage.runtime, component=comp,
                  evidence=[str(e.get("detail", ""))[:300]], remediation="Provide an ADF baseline run (or a previous Fabric run) covering every activity, then verify again.")
