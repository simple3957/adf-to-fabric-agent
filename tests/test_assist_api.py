"""The model-assisted draft routes against a real (non-demo) project, with a fake model."""

from __future__ import annotations

import time

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from adf2fabric.agent.assist import model as M  # noqa: E402
from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest  # noqa: E402
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest  # noqa: E402
from adf2fabric.api.app import create_app  # noqa: E402
from adf2fabric.api.container import Container, Settings  # noqa: E402
from tests.test_adf_semantic_discovery import _make_project  # noqa: E402
from tests.test_assist import CLEAN, SOURCE, FakeModel  # noqa: E402


@pytest.fixture()
def api(tmp_path, monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_LLM", "on")
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    store, pid = _make_project(tmp_path, {"pipeline/PL_T.json": SOURCE, "pipeline/PL_Clean.json": CLEAN})
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    fake = FakeModel()
    monkeypatch.setattr(M, "ClaudeCli", lambda *a, **k: fake)
    live = Container(Settings(projects_dir=store.projects_root), store=store)
    tc = TestClient(create_app(Settings(), live=live, demo=None), base_url="http://localhost", raise_server_exceptions=False)
    tc.fake, tc.store = fake, store
    return tc


def _wait(api, state="draft"):
    for _ in range(100):
        v = api.get("/projects/test-proj/assist/PL_T").json()
        if v["state"] == state:
            return v
        time.sleep(0.05)
    raise AssertionError(v)


def test_overview_lists_candidates_and_settings(api):
    r = api.get("/projects/test-proj/assist").json()
    assert r["settings"]["enabled"] is True and [c["pipeline"] for c in r["candidates"]["items"]] == ["PL_T"]


def _job(api, response):
    assert response.status_code == 202, response.text
    for _ in range(200):
        j = api.get(f"/jobs/{response.json()['id']}").json()
        if j["state"] in ("succeeded", "failed"):
            return j
        time.sleep(0.05)
    raise AssertionError("job did not finish")


def test_generate_poll_accept_and_convert_with_the_assisted_option(api):
    assert api.post("/projects/test-proj/assist/generate", json={"names": ["PL_T"]}).status_code == 202
    d = _wait(api)
    assert d["draft"]["checks"][0]["level"] == "ok" and len(d["draft"]["code"]) > 600  # the code is returned whole, not cut like a summary field
    assert d["source"]["problems"] and d["source"]["summary"]
    prompt = api.get("/projects/test-proj/assist/PL_T/prompt").json()
    assert "Set_Start" in prompt["prompt"] and prompt["characters"] == len(prompt["prompt"]) and len(api.fake.prompts) == 1  # previewing sent nothing
    acc = api.post("/projects/test-proj/assist/PL_T/accept", json={}).json()
    assert acc["state"] == "accepted" and acc["accepted"]["accepted_by"]
    plain = _job(api, api.post("/projects/test-proj/stages/convert", json={}))
    assert plain["state"] == "succeeded" and plain["result"]["assisted"] == []  # accepted, but not asked for
    used = _job(api, api.post("/projects/test-proj/stages/convert", json={"assisted": True}))
    assert used["state"] == "succeeded" and used["result"]["assisted"] == ["PL_T"]
    after = {p["name"]: p["status"] for p in used["result"]["pipelines"]}
    assert after["PL_T"] == "converted"


def test_discard_and_an_unknown_pipeline(api):
    api.post("/projects/test-proj/assist/generate", json={"names": ["PL_T"]})
    _wait(api)
    assert api.post("/projects/test-proj/assist/PL_T/discard", json={"scope": "all"}).json()["state"] == "none"
    assert api.get("/projects/test-proj/assist/PL_Nope").status_code == 400
    assert api.post("/projects/test-proj/assist/PL_T/discard", json={"scope": "everything"}).status_code == 422


def test_off_means_off(api, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_LLM")
    r = api.post("/projects/test-proj/assist/generate", json={"names": ["PL_T"]})
    assert r.status_code == 409 and "off" in r.json()["error"].lower() and not api.fake.prompts


def test_the_actions_are_audited(api):
    api.post("/projects/test-proj/assist/generate", json={"names": ["PL_T"]})
    _wait(api)
    api.post("/projects/test-proj/assist/PL_T/accept", json={})
    actions = [e["action"] for e in api.app.state.audit.read()]
    assert "assist.generate" in actions and "assist.accept" in actions
