"""Robustness fixes found in the architecture review: root resolution, atomic records, job eviction, user-store cache, UI client errors."""

from __future__ import annotations


import yaml

from adf2fabric import paths
from adf2fabric.api import jobs as jobs_mod
from adf2fabric.api.security.principals import UserStore
from adf2fabric.project.models import Project  # noqa: F401  (import check only)
from adf2fabric.project.store import _write_yaml


def test_repo_root_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv(paths.ENV_HOME, str(tmp_path))
    assert paths.repo_root() == tmp_path.resolve()


def test_repo_root_is_the_checkout_here(monkeypatch):
    monkeypatch.delenv(paths.ENV_HOME, raising=False)
    assert (paths.repo_root() / "pyproject.toml").exists()


def test_write_yaml_replaces_whole_file_and_leaves_no_temp(tmp_path):
    target = tmp_path / "p" / "record.yaml"
    _write_yaml(target, {"a": 1})
    _write_yaml(target, {"a": 2, "b": [1, 2]})
    assert yaml.safe_load(target.read_text(encoding="utf-8")) == {"a": 2, "b": [1, 2]}
    assert [p.name for p in target.parent.iterdir()] == ["record.yaml"]


def test_user_store_cache_sees_changes(tmp_path):
    store = UserStore(tmp_path)
    rec, token = store.create("alice", "org", ["admin"])
    assert store.authenticate(token).id == "alice"
    store.update("alice", disabled=True)  # same size or not, an in-process write is never hidden by the cache
    assert store.authenticate(token) is None
    rec2, token2 = store.rotate("alice")
    store.update("alice", disabled=False)
    assert store.authenticate(token) is None
    assert store.authenticate(token2).id == "alice"


def test_a_databricks_workspace_url_is_read_from_domain():
    """The Connect step asked for a Workspace URL that the linked service already held (authored as `domain`)."""
    from adf2fabric.agent.discovery.adf_semantic.parser import parse_linked_service

    ls = parse_linked_service({"name": "LS_Databricks", "properties": {"type": "AzureDatabricks", "typeProperties": {
        "domain": "https://adb-1.1.azuredatabricks.net", "authentication": "MSI"}}}, "linkedService/LS_Databricks.json")
    assert ls.endpoint == "https://adb-1.1.azuredatabricks.net"


def test_a_run_older_than_the_latest_deploy_does_not_prove_the_new_definition(tmp_path):
    import json, time
    from adf2fabric.agent.deployment import record as deploy_record, verify

    old = "2026-10-23T06:53:50.8566667"  # Fabric reports 7 fractional digits and no zone
    p = verify.record_path(tmp_path)
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps({"PL_A": {"verdict": "matches", "fabric_status": "Completed", "ran_at": old}}), encoding="utf-8")
    assert verify.current_record(tmp_path)["PL_A"]["verdict"] == "matches"  # nothing was deployed since: the run stands
    deploy_record.note_apply(tmp_path, "ws", [type("R", (), {"name": "PL_A", "action": "update"})()], now=time.time())
    got = verify.current_record(tmp_path)["PL_A"]
    assert got["verdict"] == "not_run" and got["stale_run"] and "deploy" in got["detail"]


# ---- jobs survive a restart ---------------------------------------------------------------------------------------------------


def test_finished_jobs_are_still_listed_after_a_restart(tmp_path):
    first = jobs_mod.JobRegistry(workers=2, store=tmp_path)
    ok = first.submit("convert", "proj", lambda log: (log("converting"), {"pipelines": 10})[1], org="acme", owner="u1")
    first.wait(ok.id)
    bad = first.submit("deploy", "other", lambda log: (_ for _ in ()).throw(RuntimeError("boom")), org="acme")
    first.wait(bad.id)
    second = jobs_mod.JobRegistry(workers=1, store=tmp_path)  # a new server over the same folder
    got = {j.id: j for j in second.list(org="acme")}
    assert got[ok.id].state == jobs_mod.SUCCEEDED and got[ok.id].result == {"pipelines": 10} and got[ok.id].owner == "u1"
    assert [e["message"] for e in got[ok.id].events] == ["converting"]
    assert got[bad.id].state == jobs_mod.FAILED and "boom" in got[bad.id].error
    assert second.list(org="someone-else") == []  # still scoped to the organisation that started it


def test_a_job_that_was_running_when_the_server_stopped_is_shown_as_interrupted(tmp_path):
    import json

    (tmp_path / "abcdef012345.json").write_text(json.dumps({"id": "abcdef012345", "kind": "deploy_apply", "project_id": "p", "state": "running", "events": [],
                                                           "created": 1.0, "org": "acme"}), encoding="utf-8")
    reg = jobs_mod.JobRegistry(store=tmp_path)
    job = reg.get("abcdef012345")
    assert job.state == jobs_mod.FAILED and "Interrupted" in job.error and job.finished
    assert json.loads((tmp_path / "abcdef012345.json").read_text(encoding="utf-8"))["state"] == "failed"  # said once, kept
    assert reg.submit("convert", "p", lambda log: {}).id  # and it does not block the project: nothing is "already running"


def test_scrubbing_server_paths_leaves_web_addresses_whole():
    from adf2fabric.api.security.guards import scrub

    assert scrub("endpoint https://acct1.dfs.core.windows.net/c") == "endpoint https://acct1.dfs.core.windows.net/c"
    assert scrub(r"failed reading C:\Users\me\projects\p\x.json") == "failed reading <path>"
    assert scrub("see c:/data/run.log and /home/me/x") == "see <path> and <path>"
