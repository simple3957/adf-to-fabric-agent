"""POST /projects/{id}/workloads/code (2026-11-18): the person supplies the file of an SSIS package or a U-SQL script the tool could not read.
The real app runs on a real (temporary) projects folder with a real discovery and workload manifest; local single-user mode (no sign-in)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from adf2fabric.api import views  # noqa: E402
from adf2fabric.api.app import create_app  # noqa: E402
from adf2fabric.api.container import Container, Settings  # noqa: E402
from adf2fabric.agent.workloads.engine import Options, build_manifest  # noqa: E402
from adf2fabric.project.workloads import WorkloadKind as W  # noqa: E402
from tests.test_discovery_activity_inventory import _discover, _project  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "ssis" / "orders_etl.dtsx"
PATH = "\\\\fileshare\\packages\\orders_etl.dtsx"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def api(tmp_path):
    act = {"name": "RunPkg", "type": "ExecuteSSISPackage", "typeProperties": {"packageLocation": {"type": "File", "typeProperties": {"packagePath": PATH}}}}
    store, pid = _project(tmp_path, {"pipeline/PL_Ssis.json": {"name": "PL_Ssis", "properties": {"activities": [act]}}})
    _discover(store)
    store.save_workload_manifest(pid, build_manifest(store, pid, Options(), NOW))
    app = create_app(Settings(projects_dir=store.projects_root), live=Container(Settings(projects_dir=store.projects_root), store=store))
    return TestClient(app, base_url="http://127.0.0.1"), store, pid


def _ref(store, pid):
    w = next(x for x in store.load_workload_manifest(pid).workloads if x.kind is W.adf_ssis)
    return views.code_ref(w.id)


def test_the_compute_view_lists_the_package_that_needs_a_file_without_a_path(api):
    client, store, pid = api
    got = client.get(f"/projects/{pid}/inventory").json()["compute"]
    [n] = got["needs_code"]
    assert n["kind"] == "adf_ssis" and n["name"] == "orders_etl" and n["accept"] == ".dtsx" and n["ref"] == _ref(store, pid)
    assert "fileshare" not in str(got) and "\\\\" not in n["ref"]  # views never carry a path


def test_a_supplied_package_is_checked_stored_and_used_by_the_next_generate(api):
    client, store, pid = api
    body = {"ref": _ref(store, pid), "text": FIXTURE.read_text(encoding="utf-8"), "confirm": True}
    r = client.post(f"/projects/{pid}/workloads/code", json=body)
    assert r.status_code == 200 and r.json()["saved"] and r.json()["kind"] == "adf_ssis"
    stored = list((store.project_dir(pid) / "workloads" / "code" / "ssis").rglob("orders_etl.dtsx"))
    assert len(stored) == 1 and stored[0].read_text(encoding="utf-8") == FIXTURE.read_text(encoding="utf-8")
    m = build_manifest(store, pid, Options(generate=True), NOW)
    store.save_workload_manifest(pid, m)
    w = next(x for x in m.workloads if x.kind is W.adf_ssis)
    assert w.code.status.value == "cached" and w.artifact.kind == "fabric_notebook"
    assert client.get(f"/projects/{pid}/inventory").json()["compute"].get("needs_code") in ([], None)  # nothing left to supply


def test_it_refuses_a_file_that_is_not_a_package_without_confirmation_or_with_a_wrong_handle(api):
    client, store, pid = api
    ref = _ref(store, pid)
    assert client.post(f"/projects/{pid}/workloads/code", json={"ref": ref, "text": "<root/>", "confirm": True}).status_code == 422
    assert client.post(f"/projects/{pid}/workloads/code", json={"ref": ref, "text": FIXTURE.read_text(encoding="utf-8")}).status_code == 422  # not confirmed
    assert client.post(f"/projects/{pid}/workloads/code", json={"ref": "0" * 12, "text": "x", "confirm": True}).status_code == 404
    assert client.post(f"/projects/{pid}/workloads/code", json={"ref": "../etc/pass", "text": "x", "confirm": True}).status_code == 422
    assert client.post(f"/projects/{pid}/workloads/code", json={"ref": ref, "text": "a\0b", "confirm": True}).status_code in (404, 422)
    assert not (store.project_dir(pid) / "workloads" / "code" / "ssis").exists()  # nothing was stored by a refused request
