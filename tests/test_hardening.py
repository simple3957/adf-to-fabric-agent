"""Robustness fixes found in the architecture review: root resolution, atomic records, job eviction, user-store cache, UI client errors."""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest
import requests
import yaml

from adf2fabric import paths
from adf2fabric.api import jobs as jobs_mod
from adf2fabric.api.security.principals import UserStore
from adf2fabric.project.models import Project  # noqa: F401  (import check only)
from adf2fabric.project.store import ProjectStore, _write_yaml


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


def test_failed_write_keeps_the_old_record(tmp_path, monkeypatch):
    target = tmp_path / "record.yaml"
    _write_yaml(target, {"ok": True})

    def boom(self, *a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(Path, "replace", boom)
    with pytest.raises(OSError):
        _write_yaml(target, {"ok": False})
    monkeypatch.undo()
    assert yaml.safe_load(target.read_text(encoding="utf-8")) == {"ok": True}
    assert [p.name for p in tmp_path.iterdir()] == ["record.yaml"]


def test_finished_jobs_are_evicted_but_running_ones_stay(monkeypatch):
    monkeypatch.setattr(jobs_mod, "KEEP_FINISHED", 3)
    reg = jobs_mod.JobRegistry(workers=2)
    for _ in range(6):
        reg.wait(reg.submit("x", None, lambda log: {"ok": True}).id)
    release = threading.Event()
    running = reg.submit("slow", "proj", lambda log: (release.wait(5), {})[1])
    reg.submit("x", None, lambda log: {})  # triggers eviction
    finished = [j for j in reg.list() if j.state in (jobs_mod.SUCCEEDED, jobs_mod.FAILED)]
    assert len(finished) <= 4
    assert reg.get(running.id) is not None
    release.set()
    reg.wait(running.id)


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


def test_ui_client_turns_timeouts_into_api_errors():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ui"))
    try:
        from api_client import ApiClient, ApiError
    finally:
        sys.path.pop(0)

    class Http:
        def request(self, *a, **k):
            raise requests.ReadTimeout("slow")

    with pytest.raises(ApiError) as exc:
        ApiClient("s", base_url="http://x", http=Http())._get("/health")
    assert exc.value.status == 0 and "did not answer" in exc.value.message


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
