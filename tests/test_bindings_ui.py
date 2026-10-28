"""The Convert step asks for the ids a person must set by hand, saves them, and points linked-service connections at the Connect step."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("streamlit")
from fastapi.testclient import TestClient  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

from adf2fabric.api.app import create_app  # noqa: E402
from adf2fabric.api.container import Container, Settings  # noqa: E402
from tests.test_conversion_agent import FILES  # noqa: E402
from tests.test_partial_source import _convert  # noqa: E402
from tests.test_ui import UI, ApiClient, _Http  # noqa: E402

GUID = "55555555-5555-5555-5555-555555555555"


def _script(ui_dir, api, result):
    import sys

    sys.path.insert(0, ui_dir)
    import panels

    panels.convert(api, "test-proj", result)


@pytest.fixture()
def ui(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    store, pid, conv = _convert(tmp_path, FILES)
    c = Container(Settings(projects_dir=store.projects_root), store=store)
    tc = TestClient(create_app(Settings(), live=c, demo=c), base_url="http://localhost", raise_server_exceptions=False)
    api = ApiClient(session_id="ui-bind", base_url="http://localhost", token="", http=_Http(tc))
    api.set_mode("demo")
    unbound = [q.name for q in [*conv.connections, *conv.workspace_items] if not q.bound and not q.name.startswith("pipeline:")]
    result = {"status": conv.status.value, "pipelines": [], "status_counts": {}, "unbound": unbound}
    return AppTest.from_function(_script, args=(str(UI), api, result), default_timeout=90), store, unbound


def test_the_open_ids_are_asked_for_with_a_field_each_and_linked_service_ones_point_at_connect(ui):
    at, _store, unbound = ui
    at.run()
    assert not at.exception, at.exception
    assert any("Things you need to set" in m.value for m in at.markdown)
    assert at.text_input(key="bind-id-connection:invoke-pipeline") is not None  # the Invoke pipeline connection has no linked service: asked for here
    assert any("Set on the **Connect** step" in i.value for i in at.info)  # LS_* connections are not asked for twice
    assert any("still open" in w.value for w in at.warning) and len(unbound) >= 2


def test_saving_an_id_writes_it_for_the_next_convert(ui):
    at, store, _ = ui
    at.run()
    at.text_input(key="bind-id-connection:invoke-pipeline").set_value(GUID)
    save = next(b for b in at.button if getattr(b, "label", "") == "Save")
    save.click().run()
    assert not at.exception, at.exception
    rows = json.loads((store.conversion_dir("test-proj") / "resolutions.json").read_text(encoding="utf-8"))
    assert {"type": "ItemToId", "key": "connection:invoke-pipeline", "value": GUID} in rows
    assert any("Saved. Convert again" in i.value for i in at.info)


def test_an_empty_save_asks_for_an_item_and_writes_nothing(ui):
    at, store, _ = ui
    at.run()
    next(b for b in at.button if getattr(b, "label", "") == "Save").click().run()
    assert any("Choose an item or paste its id" in w.value for w in at.warning)
    assert not (store.conversion_dir("test-proj") / "resolutions.json").exists()
