"""The Convert step's model-assisted drafts panel, driven through Streamlit's test harness against a real project and a fake model."""

from __future__ import annotations

import time

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("streamlit")
from fastapi.testclient import TestClient  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

from adf2fabric.agent.assist import model as M  # noqa: E402
from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest  # noqa: E402
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest  # noqa: E402
from adf2fabric.api.app import create_app  # noqa: E402
from adf2fabric.api.container import Container, Settings  # noqa: E402
from tests.test_adf_semantic_discovery import _make_project  # noqa: E402
from tests.test_assist import CLEAN, SOURCE, FakeModel  # noqa: E402
from tests.test_ui import UI, ApiClient, _Http, journey  # noqa: E402


@pytest.fixture()
def ui(tmp_path, monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_LLM", "on")
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    store, pid = _make_project(tmp_path, {"pipeline/PL_T.json": SOURCE, "pipeline/PL_Clean.json": CLEAN})
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    conv = ConversionAgent(store).run(ConversionRequest(project_name="test-proj")).conversion
    fake = FakeModel()
    monkeypatch.setattr(M, "ClaudeCli", lambda *a, **k: fake)
    c = Container(Settings(projects_dir=store.projects_root), store=store)
    tc = TestClient(create_app(Settings(), live=c, demo=c), base_url="http://localhost", raise_server_exceptions=False)
    api = ApiClient(session_id="ui-assist", base_url="http://localhost", token="", http=_Http(tc))
    api.set_mode("demo")
    result = {"status": conv.status.value, "pipelines": [{"name": p.source_pipeline, "status": p.status.value, "structurally_valid": p.structural_valid,
                                                           "runtime_ready": p.runtime_ready} for p in conv.pipelines], "status_counts": conv.status_counts}
    at = AppTest.from_function(_script, args=(str(UI), api, result), default_timeout=90)
    at.fake = fake
    return at, api


def _script(ui_dir, api, result):
    import sys

    sys.path.insert(0, ui_dir)
    import panels

    panels.convert(api, "test-proj", result)


def _button(at, key):
    return next(b for b in at.button if b.key == key)


def test_the_panel_walks_a_draft_from_generation_to_acceptance_and_offers_the_drafts_to_convert(ui):
    at, api = ui
    at.run()
    assert not at.exception, at.exception
    assert any(str(e.label).startswith("Model-assisted drafts (optional)") for e in at.expander)
    assert not [c for c in at.checkbox if c.key and c.key.startswith("conv-assisted")]  # nothing accepted yet: nothing to offer
    assert "Preview what is sent" in [b.label for b in at.button]
    _button(at, "assist-gen-test-proj-PL_T").click().run()
    for _ in range(100):
        if api.assist_detail("test-proj", "PL_T")["state"] == "draft":
            break
        time.sleep(0.05)
    at.run()
    assert not at.exception, at.exception
    assert any("Passed the automatic checks" in s.value for s in at.success)
    assert at.fake.prompts and "Set_Start" in at.fake.prompts[0]
    _button(at, "assist-acc-test-proj-PL_T").click().run()
    assert not at.exception, at.exception
    assert api.assist_detail("test-proj", "PL_T")["state"] == "accepted"
    at.run()
    offer = [c for c in at.checkbox if c.key == "conv-assisted-test-proj"]
    assert offer and "PL_T" in offer[0].label


def test_the_panel_is_silent_when_the_converter_left_nothing_and_explains_when_the_feature_is_off(ui, monkeypatch):
    at, api = ui
    monkeypatch.delenv("ADF2FABRIC_LLM")
    at.run()
    assert not at.exception, at.exception
    assert any("ADF2FABRIC_LLM=on" in i.value for i in at.info)
    assert _button(at, "assist-gen-test-proj-PL_T").disabled
