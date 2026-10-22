"""Commit what the migration created in a Fabric workspace to the workspace's connected Git branch.

Fabric REST (Microsoft Learn, checked 2026-10-22):

* ``GET  /v1/workspaces/{ws}/git/connection`` - ``gitConnectionState`` NotConnected | Connected | ConnectedAndInitialized, provider
  details (GitHub ``ownerName`` / AzureDevOps ``organizationName``, ``projectName``, ``repositoryName``, ``branchName``).
* ``GET  /v1/workspaces/{ws}/git/status`` - 200, or 202 (long-running; the result is at ``/operations/{id}/result``):
  ``workspaceHead``, ``remoteCommitHash``, ``changes[]`` of ``{itemMetadata {itemIdentifier {objectId, logicalId}, itemType,
  displayName}, workspaceChange, remoteChange, conflictType None | Conflict | SameChanges}``.
* ``POST /v1/workspaces/{ws}/git/commitToGit`` ``{mode: All | Selective, workspaceHead, comment (<= 300), items: [{objectId,
  logicalId}]}`` - 200 or 202. Needs a contributor role and the caller's Git credentials configured in Fabric.

What this module will and will not do:

* Without ``apply`` it only reads (connection and status) and says what a commit would contain.
* It commits Selectively: only the workspace changes of the items the migration deployed (by display name), unless ``all_changes``
  is asked for. An item with a conflict is never committed; it is listed for a person.
* When the remote branch has changes the workspace does not have, it refuses: the workspace must be updated from Git first
  (in Fabric), so a commit can never be made on top of a state nobody reviewed.
* An item deleted in the workspace is never committed (that would delete it from the branch); it is listed for a person.
* Names from the conversion match only DataPipeline items; other item types are matched only when named explicitly.
* It never connects a workspace to Git, never updates the workspace from Git, never deletes anything.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Iterable, Optional

from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, _GUID, DeployError, FabricClient, _error_text

MAX_COMMENT = 300


@dataclass
class GitConnection:
    state: str  # NotConnected | Connected | ConnectedAndInitialized | unknown
    provider: str = ""
    repository: str = ""  # owner/repo or organization/project/repo
    branch: str = ""
    directory: str = ""

    @property
    def connected(self) -> bool:
        return self.state == "ConnectedAndInitialized"


@dataclass
class Change:
    name: str
    item_type: str
    object_id: Optional[str]
    logical_id: Optional[str]
    workspace_change: Optional[str]
    remote_change: Optional[str]
    conflict: str


@dataclass
class CommitPlan:
    connection: GitConnection
    workspace_head: str = ""
    to_commit: list[Change] = field(default_factory=list)
    conflicts: list[Change] = field(default_factory=list)
    remote_only: list[Change] = field(default_factory=list)  # changes in Git the workspace does not have
    not_ours: list[Change] = field(default_factory=list)  # workspace changes outside the migration's items (left alone)
    deletions: list[Change] = field(default_factory=list)  # items deleted in the workspace: never committed (it would delete them from Git)
    blockers: list[str] = field(default_factory=list)
    committed: bool = False
    detail: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def get_connection(client: FabricClient, workspace: str) -> GitConnection:
    if not _GUID.match(workspace or ""):
        raise DeployError("The workspace id must be a GUID.")
    resp = client._call("GET", f"{FABRIC_API}/workspaces/{workspace}/git/connection")
    if resp.status_code != 200:
        raise DeployError(f"Reading the workspace's Git connection failed (HTTP {resp.status_code}): {_error_text(resp)}")
    body = resp.json() or {}
    det = body.get("gitProviderDetails") or {}
    repo = "/".join(x for x in (det.get("ownerName") or det.get("organizationName"), det.get("projectName"), det.get("repositoryName")) if x)
    return GitConnection(state=str(body.get("gitConnectionState") or "unknown"), provider=str(det.get("gitProviderType") or ""),
                         repository=repo, branch=str(det.get("branchName") or ""), directory=str(det.get("directoryName") or ""))


def get_status(client: FabricClient, workspace: str) -> dict:
    resp = client._call("GET", f"{FABRIC_API}/workspaces/{workspace}/git/status")
    if resp.status_code == 200:
        return resp.json() or {}
    if resp.status_code != 202:
        raise DeployError(f"Reading the Git status failed (HTTP {resp.status_code}): {_error_text(resp)}")
    done = client._finish(resp, "Reading the Git status")
    result = client._call("GET", f"{done['operation']}/result")
    if result.status_code != 200:
        raise DeployError(f"Reading the Git status result failed (HTTP {result.status_code}): {_error_text(result)}")
    return result.json() or {}


def _change(c: dict) -> Change:
    meta = c.get("itemMetadata") or {}
    ident = meta.get("itemIdentifier") or {}
    return Change(name=str(meta.get("displayName") or ""), item_type=str(meta.get("itemType") or ""), object_id=ident.get("objectId"),
                  logical_id=ident.get("logicalId"), workspace_change=c.get("workspaceChange"), remote_change=c.get("remoteChange"),
                  conflict=str(c.get("conflictType") or "None"))


def plan_commit(client: FabricClient, workspace: str, pipelines: Iterable[str], *, extra: Iterable[str] = (),
                all_changes: bool = False) -> CommitPlan:
    """Read the connection and status (read-only) and decide what a commit would contain.

    ``pipelines`` match DataPipeline items only; ``extra`` names match an item of any type."""
    conn = get_connection(client, workspace)
    plan = CommitPlan(connection=conn)
    if not conn.connected:
        plan.blockers.append(f"The workspace is not connected to Git (state: {conn.state}). Connect it to a repository and branch in "
                             "Fabric (workspace settings > Git integration) first; this tool never connects it.")
        return plan
    status = get_status(client, workspace)
    plan.workspace_head = str(status.get("workspaceHead") or "")
    wanted_pipes = {n.lower() for n in pipelines}
    wanted_any = {n.lower() for n in extra}
    for raw in status.get("changes") or []:
        ch = _change(raw)
        if ch.conflict == "Conflict":
            plan.conflicts.append(ch)
        elif ch.remote_change and not ch.workspace_change:
            plan.remote_only.append(ch)
        elif ch.workspace_change == "Deleted":
            plan.deletions.append(ch)
        elif ch.workspace_change and ch.conflict != "SameChanges":
            ours = ch.name.lower() in wanted_any or (ch.item_type == "DataPipeline" and ch.name.lower() in wanted_pipes)
            (plan.to_commit if all_changes or ours else plan.not_ours).append(ch)
    if plan.remote_only:
        plan.blockers.append(f"{len(plan.remote_only)} change(s) in the Git branch are not in the workspace. Update the workspace from Git "
                             "in Fabric first, review it, then commit.")
    if not plan.to_commit and not plan.blockers:
        plan.detail = "Nothing to commit: the migration's items have no uncommitted workspace changes."
    return plan


def commit(client: FabricClient, workspace: str, plan: CommitPlan, comment: str, *, apply: bool) -> CommitPlan:
    """Commit the planned items (Selective). Without ``apply`` nothing is sent."""
    if plan.blockers or not plan.to_commit:
        return plan
    comment = (comment or "").strip()
    if len(comment) > MAX_COMMENT:
        raise DeployError(f"The commit comment is longer than {MAX_COMMENT} characters (Fabric's limit).")
    if not apply:
        plan.detail = f"Dry run: would commit {len(plan.to_commit)} item(s) to {plan.connection.branch}."
        return plan
    body: dict = {"mode": "Selective", "workspaceHead": plan.workspace_head,
                  "items": [{k: v for k, v in (("objectId", c.object_id), ("logicalId", c.logical_id)) if v} for c in plan.to_commit]}
    if comment:
        body["comment"] = comment
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/git/commitToGit", body)
    client._finish(resp, "Committing to Git")
    plan.committed = True
    plan.detail = f"Committed {len(plan.to_commit)} item(s) to {plan.connection.branch}."
    return plan
