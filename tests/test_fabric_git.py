"""Committing the migration's items to the workspace's Git branch: read-only by default, selective, refuses conflicts and a stale workspace."""

from __future__ import annotations

import pytest

from adf2fabric.agent.deployment.fabric_deploy import DeployError, FabricClient
from adf2fabric.agent.deployment.fabric_git import commit, get_connection, plan_commit

WS = "11111111-1111-1111-1111-111111111111"
API = "https://api.fabric.microsoft.com/v1"
OP = "22222222-2222-2222-2222-222222222222"


class R:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._b, self.headers = status, body, headers or {}
        self.content, self.text = (b"x" if body is not None else b""), ""

    def json(self):
        return self._b


def change(name, oid, ws=None, remote=None, conflict="None", type_="DataPipeline"):
    c = {"itemMetadata": {"itemIdentifier": {"objectId": oid}, "itemType": type_, "displayName": name}, "conflictType": conflict}
    if ws:
        c["workspaceChange"] = ws
    if remote:
        c["remoteChange"] = remote
    return c


CONNECTED = {"gitConnectionState": "ConnectedAndInitialized", "gitConnectionType": "Full",
             "gitProviderDetails": {"gitProviderType": "GitHub", "ownerName": "org", "repositoryName": "fabric", "branchName": "dev", "directoryName": ""},
             "gitSyncDetails": {"head": "abc", "lastSyncTime": "2026-10-22T00:00:00"}}


class Fake:
    def __init__(self, connection=CONNECTED, changes=(), status_lro=False, commit_status=202):
        self.connection, self.changes, self.status_lro, self.commit_status = connection, list(changes), status_lro, commit_status
        self.calls: list[tuple[str, str, object]] = []

    def request(self, method, url, headers=None, json=None, timeout=None):
        self.calls.append((method, url, json))
        if url.endswith("/git/connection"):
            return R(200, self.connection)
        status = {"workspaceHead": "head123", "remoteCommitHash": "remote9", "changes": self.changes}
        if url.endswith("/git/status"):
            return R(202, None, {"x-ms-operation-id": OP, "Retry-After": "0"}) if self.status_lro else R(200, status)
        if url == f"{API}/operations/{OP}":
            return R(200, {"status": "Succeeded"})
        if url == f"{API}/operations/{OP}/result":
            return R(200, status)
        if url.endswith("/git/commitToGit"):
            return R(self.commit_status, None if self.commit_status == 202 else {"errorCode": "WorkspaceHeadMismatch", "message": "x"},
                     {"x-ms-operation-id": OP, "Retry-After": "0"})
        return R(404, {"errorCode": "NotFound"})


def client(fake):
    return FabricClient("t", session=fake, sleep=lambda s: None)


def commits(fake):
    return [c for c in fake.calls if c[1].endswith("/commitToGit")]


def test_a_workspace_not_connected_to_git_is_blocked_and_nothing_is_sent():
    fake = Fake(connection={"gitProviderDetails": None, "gitSyncDetails": None, "gitConnectionState": "NotConnected"})
    plan = commit(client(fake), WS, plan_commit(client(fake), WS, ["PL_A"]), "c", apply=True)
    assert plan.blockers and "not connected" in plan.blockers[0] and not plan.committed and not commits(fake)


def test_only_the_migrations_items_are_committed_selectively():
    fake = Fake(changes=[change("PL_A", "a" * 8 + "-0000-0000-0000-000000000001", ws="Added"),
                         change("Report_X", "b" * 8 + "-0000-0000-0000-000000000002", ws="Modified", type_="Report")])
    c = client(fake)
    plan = plan_commit(c, WS, ["pl_a"])
    assert [x.name for x in plan.to_commit] == ["PL_A"] and [x.name for x in plan.not_ours] == ["Report_X"]
    dry = commit(c, WS, plan, "migration", apply=False)
    assert not commits(fake) and "Dry run" in dry.detail and not dry.committed
    done = commit(c, WS, plan, "migration", apply=True)
    (_, _, body), = commits(fake)
    assert body == {"mode": "Selective", "workspaceHead": "head123", "comment": "migration",
                    "items": [{"objectId": "aaaaaaaa-0000-0000-0000-000000000001"}]}
    assert done.committed and "dev" in done.detail


def test_all_changes_includes_other_items_but_never_conflicts():
    fake = Fake(changes=[change("PL_A", "1", ws="Modified"), change("Other", "2", ws="Added"),
                         change("Both", "3", ws="Modified", remote="Modified", conflict="Conflict")])
    plan = plan_commit(client(fake), WS, ["PL_A"], all_changes=True)
    assert sorted(x.name for x in plan.to_commit) == ["Other", "PL_A"] and [x.name for x in plan.conflicts] == ["Both"]


def test_remote_changes_the_workspace_lacks_block_the_commit():
    fake = Fake(changes=[change("PL_A", "1", ws="Modified"), change("Upstream", "2", remote="Modified")])
    c = client(fake)
    plan = commit(c, WS, plan_commit(c, WS, ["PL_A"]), "c", apply=True)
    assert plan.blockers and "Update the workspace from Git" in plan.blockers[0] and not commits(fake)


def test_status_is_read_through_the_long_running_operation():
    fake = Fake(changes=[change("PL_A", "1", ws="Added")], status_lro=True)
    plan = plan_commit(client(fake), WS, ["PL_A"])
    assert [x.name for x in plan.to_commit] == ["PL_A"] and plan.workspace_head == "head123"


def test_nothing_to_commit_says_so():
    plan = plan_commit(client(Fake(changes=[change("PL_A", "1", ws="Modified", remote="Modified", conflict="SameChanges")])), WS, ["PL_A"])
    assert not plan.to_commit and not plan.blockers and "Nothing to commit" in plan.detail


def test_a_rejected_commit_and_a_long_comment_are_errors():
    fake = Fake(changes=[change("PL_A", "1", ws="Added")], commit_status=409)
    c = client(fake)
    with pytest.raises(DeployError):
        commit(c, WS, plan_commit(c, WS, ["PL_A"]), "c", apply=True)
    with pytest.raises(DeployError, match="300"):
        commit(c, WS, plan_commit(c, WS, ["PL_A"]), "x" * 301, apply=False)


def test_the_connection_is_summarised_without_ids():
    conn = get_connection(client(Fake()), WS)
    assert conn.connected and conn.repository == "org/fabric" and conn.branch == "dev" and conn.provider == "GitHub"
    with pytest.raises(DeployError):
        get_connection(client(Fake()), "not-a-guid")


def test_a_deletion_is_never_committed_even_with_all_changes():
    fake = Fake(changes=[{"itemMetadata": {"itemIdentifier": {"logicalId": "L1"}, "itemType": "DataPipeline", "displayName": "PL_A"},
                          "workspaceChange": "Deleted", "conflictType": "None"}])
    c = client(fake)
    for all_changes in (False, True):
        plan = commit(c, WS, plan_commit(c, WS, ["PL_A"], all_changes=all_changes), "c", apply=True)
        assert [x.name for x in plan.deletions] == ["PL_A"] and not plan.to_commit and not commits(fake)


def test_converted_names_match_pipelines_only_and_extra_names_match_any_type():
    fake = Fake(changes=[change("PL_Load", "1", ws="Modified"), change("pl_load", "2", ws="Added", type_="Notebook"),
                         change("NB_X", "3", ws="Added", type_="Notebook")])
    plan = plan_commit(client(fake), WS, ["PL_Load"], extra=["NB_X"])
    assert sorted((x.name, x.item_type) for x in plan.to_commit) == [("NB_X", "Notebook"), ("PL_Load", "DataPipeline")]
    assert [x.name for x in plan.not_ours] == ["pl_load"]
