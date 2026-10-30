"""The ids a person supplies by hand after conversion: listed from the conversion, written to the resolutions file, checked against Fabric."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from adf2fabric.api.app import create_app  # noqa: E402
from adf2fabric.api.container import Container, Settings  # noqa: E402
from adf2fabric.project import bindings as B  # noqa: E402
from adf2fabric.project.conversion import Prerequisite  # noqa: E402
from tests.test_conversion_agent import FILES  # noqa: E402
from tests.test_partial_source import _convert  # noqa: E402

NB = "11111111-1111-1111-1111-111111111111"
CONN = "22222222-2222-2222-2222-222222222222"


def _with_notebook(store, pid):
    conv = store.load_conversion(pid)
    conv.workspace_items.append(Prerequisite(kind="workspace_item", name="notebook:NB_X", detail="runs NB_X", binding="<item:notebook:NB_X>", needed_by=["Run_X"]))
    store.save_conversion(pid, conv)


def test_open_items_are_the_ids_a_person_must_supply_and_nothing_else(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)
    _with_notebook(store, pid)
    items = {i["key"]: i for i in B.open_items(store, pid)}
    assert "notebook:NB_X" in items and items["notebook:NB_X"]["item_type"] == "Notebook" and items["notebook:NB_X"]["bound"] is False
    assert "connection:invoke-pipeline" in items  # no linked service behind it: not the Connect step's
    assert not any(k.startswith(("pipeline:", "connection:LS_")) or k == "workspace" for k in items)  # filled at deploy / set in Connect


def test_a_binding_is_written_to_the_resolutions_file_kept_with_the_rest_and_shown_as_bound(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)
    _with_notebook(store, pid)
    res = store.conversion_dir(pid) / "resolutions.json"
    res.write_text(json.dumps([{"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": CONN}]), encoding="utf-8")
    assert B.set_binding(store, pid, "notebook:nb_x", NB) == {"key": "notebook:NB_X", "changed": True}
    assert B.set_binding(store, pid, "notebook:NB_X", NB)["changed"] is False
    rows = json.loads(res.read_text(encoding="utf-8"))
    assert {"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": CONN} in rows
    assert {"type": "ItemToId", "key": "notebook:NB_X", "value": NB} in rows and len(rows) == 2
    assert next(i for i in B.open_items(store, pid) if i["key"] == "notebook:NB_X")["value"] == NB


def test_a_binding_for_something_the_conversion_does_not_need_or_not_an_id_is_refused(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)
    with pytest.raises(B.BindingError, match="not in the list"):
        B.set_binding(store, pid, "notebook:Nobody", NB)
    with pytest.raises(B.BindingError, match="not a Fabric item id"):
        B.set_binding(store, pid, "connection:invoke-pipeline", "NB_X")
    assert not (store.conversion_dir(pid) / "resolutions.json").exists()


class _Fab:
    def __init__(self, fail=False):
        self.fail = fail

    def items(self, ws, t):
        if self.fail:
            raise RuntimeError("401 Unauthorized")
        return [{"id": NB, "displayName": "NB_X"}] if t == "Notebook" else []

    def connections(self):
        return [{"id": CONN, "displayName": "Invoke", "connectionDetails": {"type": "FabricDataPipelines"}},
                {"id": NB, "displayName": "Other", "connectionDetails": {"type": "SQL"}}]


def test_candidates_come_from_live_fabric_and_say_why_when_there_are_none(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)
    _with_notebook(store, pid)
    items = B.open_items(store, pid)
    found, note = B.candidates(_Fab(), "ws", items)
    assert not note and found["notebook:NB_X"] == [{"id": NB, "name": "NB_X"}]
    assert [c["id"] for c in found["connection:invoke-pipeline"]] == [CONN]  # only the Fabric pipeline connection type
    assert "not connected" in B.candidates(None, "ws", items)[1]
    assert "Choose a Fabric workspace" in B.candidates(_Fab(), None, items)[1]
    none, note = B.candidates(_Fab(fail=True), "ws", items)
    assert none == {} and "could not be read" in note and "401" in note


@pytest.fixture()
def api(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)
    _with_notebook(store, pid)
    live = Container(Settings(projects_dir=store.projects_root), store=store, fabric_factory=lambda s, p: _Fab())
    tc = TestClient(create_app(Settings(), live=live, demo=None), base_url="http://localhost", raise_server_exceptions=False)
    tc.store = store
    return tc


def test_the_api_lists_open_items_and_records_a_verified_binding(api):
    api.put("/session/destination", json={"provider": "azure_cli", "workspace_id": "33333333-3333-3333-3333-333333333333"})
    got = api.get("/projects/test-proj/bindings").json()
    nb = next(i for i in got["items"] if i["key"] == "notebook:NB_X")
    assert nb["bound"] is False and nb["candidates"] == [{"id": NB, "name": "NB_X"}]
    ok = api.put("/projects/test-proj/bindings", json={"key": "notebook:NB_X", "value": NB})
    assert ok.status_code == 200 and ok.json()["verified"] is True
    again = next(i for i in api.get("/projects/test-proj/bindings").json()["items"] if i["key"] == "notebook:NB_X")
    assert again["value"] == NB and again["bound"] is False  # saved; "bound" turns true when the next convert has used it
    assert json.loads((api.store.conversion_dir("test-proj") / "resolutions.json").read_text(encoding="utf-8"))[0]["value"] == NB


def test_the_api_refuses_a_notebook_id_that_is_not_in_the_workspace(api):
    api.put("/session/destination", json={"provider": "azure_cli", "workspace_id": "33333333-3333-3333-3333-333333333333"})
    r = api.put("/projects/test-proj/bindings", json={"key": "notebook:NB_X", "value": "44444444-4444-4444-4444-444444444444"})
    assert r.status_code in (400, 422) and "not a fabric notebook" in r.text.lower()
    assert not (api.store.conversion_dir("test-proj") / "resolutions.json").exists()


def test_a_lakehouse_the_convert_step_pointed_at_is_offered_to_bind(tmp_path):
    """Choosing the lakehouse option leaves <item:lakehouse:NAME>; without an entry here the five refused pipelines had nothing to click to fix them."""
    store, pid, _ = _convert(tmp_path, FILES)
    conv = store.load_conversion(pid)
    conv.workspace_items.append(Prerequisite(kind="workspace_item", name="lakehouse:LH_Migration", detail="the lakehouse the datasets point at",
                                             binding="<item:lakehouse:LH_Migration>", needed_by=["Copy_X"]))
    store.save_conversion(pid, conv)
    item = next(i for i in B.open_items(store, pid) if i["key"] == "lakehouse:LH_Migration")
    assert item["item_type"] == "Lakehouse" and item["label"] == "Fabric lakehouse" and item["bound"] is False
    assert B.set_binding(store, pid, "lakehouse:LH_Migration", NB)["changed"] is True


def test_a_pasted_binding_id_is_normalised(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)
    _with_notebook(store, pid)
    assert B.set_binding(store, pid, "notebook:NB_X", "{" + NB.upper() + "}") == {"key": "notebook:NB_X", "changed": True}
    assert next(i for i in B.open_items(store, pid) if i["key"] == "notebook:NB_X")["value"] == NB
