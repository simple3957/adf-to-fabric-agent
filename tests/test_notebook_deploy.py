"""Deploying a notebook from a local file and binding its id for conversion (fake Fabric, no network)."""

from __future__ import annotations

import base64
import json

import pytest

from adf2fabric.agent.deployment import fabric_deploy as fd
from adf2fabric.agent.deployment import notebook_deploy as nd
from tests.test_fabric_deploy import WS, Resp, client

NB = "44444444-4444-4444-4444-444444444444"
LIST = ("GET", f"/workspaces/{WS}/notebooks")
CREATE = ("POST", f"/workspaces/{WS}/notebooks")
UPDATE = ("POST", f"/workspaces/{WS}/notebooks/{NB}/updateDefinition")


def notebooks(*pairs):
    return Resp(200, {"value": [{"id": i, "displayName": n} for i, n in pairs]})


def payload(definition):
    part = definition["parts"][0]
    assert part["path"] == "notebook-content.ipynb" and part["payloadType"] == "InlineBase64"
    return json.loads(base64.b64decode(part["payload"]))


def test_a_py_file_becomes_one_code_cell_in_ipynb_format(tmp_path):
    f = tmp_path / "nb.py"
    f.write_text('storage = "x"\nprint(storage)\n', encoding="utf-8")
    d = nd.build_definition(f)
    nb = payload(d)
    assert d["format"] == "ipynb" and nb["nbformat"] == 4 and nb["metadata"]["language_info"]["name"] == "python"
    assert len(nb["cells"]) == 1 and "".join(nb["cells"][0]["source"]) == 'storage = "x"\nprint(storage)\n'


def test_an_ipynb_file_is_sent_as_it_is(tmp_path):
    f = tmp_path / "nb.ipynb"
    original = {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [
        {"cell_type": "code", "source": ["1+1"], "metadata": {}, "outputs": [], "execution_count": None}]}
    f.write_text(json.dumps(original), encoding="utf-8")
    assert payload(nd.build_definition(f)) == original


@pytest.mark.parametrize("name,content,why", [
    ("a.txt", "x", "Only .py and .ipynb"),
    ("a.ipynb", "not json", "not valid JSON"),
    ("a.ipynb", '{"nope": 1}', "no 'cells'"),
])
def test_bad_notebook_files_are_refused(tmp_path, name, content, why):
    f = tmp_path / name
    f.write_text(content, encoding="utf-8")
    with pytest.raises(fd.DeployError, match=why):
        nd.build_definition(f)
    with pytest.raises(fd.DeployError, match="not found"):
        nd.build_definition(tmp_path / "missing.py")


DEF = {"format": "ipynb", "parts": [{"path": "notebook-content.ipynb", "payload": "e30=", "payloadType": "InlineBase64"}]}


def test_dry_run_only_reads():
    c, s = client({LIST: [notebooks((NB, "NB_Read_Raw_Data"))]})
    r = nd.deploy_notebook(c, WS, "nb_read_raw_data", DEF, apply=False)
    assert (r.action, r.item_id) == ("update", NB)
    r = nd.deploy_notebook(c, WS, "Brand_New", DEF, apply=False)
    assert (r.action, r.item_id) == ("create", None)
    assert {m for m, *_ in s.calls} == {"GET"}


def test_create_uses_the_returned_id_and_sends_the_definition():
    c, s = client({LIST: [notebooks()], CREATE: [Resp(201, {"id": NB, "displayName": "NB_X"})]})
    r = nd.deploy_notebook(c, WS, "NB_X", DEF, apply=True)
    assert (r.action, r.item_id) == ("create", NB)
    post = next(x for x in s.calls if x[0] == "POST")
    assert post[2] == {"displayName": "NB_X", "definition": DEF}


def test_create_via_long_running_operation_looks_the_id_up_afterwards():
    op = f"{fd.FABRIC_API}/operations/op1"
    c, s = client({("GET", f"/workspaces/{WS}/notebooks"): [notebooks(), notebooks((NB, "NB_X"))],
                   CREATE: [Resp(202, {}, {"Location": op, "Retry-After": "1"})],
                   ("GET", "/operations/op1"): [Resp(200, {"status": "Succeeded"})]})
    r = nd.deploy_notebook(c, WS, "NB_X", DEF, apply=True)
    assert r.item_id == NB


def test_an_existing_notebook_has_its_content_replaced_never_deleted():
    c, s = client({LIST: [notebooks((NB, "NB_X"))], UPDATE: [Resp(200, {})]})
    r = nd.deploy_notebook(c, WS, "NB_X", DEF, apply=True)
    assert (r.action, r.item_id) == ("update", NB)
    assert [m for m, *_ in s.calls] == ["GET", "POST"] and "DELETE" not in {m for m, *_ in s.calls}


def test_a_refused_create_is_an_error_and_bad_input_is_caught_early():
    c, _ = client({LIST: [notebooks()], CREATE: [Resp(403, {"errorCode": "FeatureNotAvailable", "message": "no capacity"})]})
    with pytest.raises(fd.DeployError, match="HTTP 403"):
        nd.deploy_notebook(c, WS, "NB_X", DEF, apply=True)
    with pytest.raises(fd.DeployError, match="GUID"):
        nd.deploy_notebook(c, "nope", "NB_X", DEF, apply=False)
    with pytest.raises(fd.DeployError, match="1..256"):
        nd.deploy_notebook(c, WS, "", DEF, apply=False)


def test_listing_follows_continuation_tokens():
    c, _ = client({LIST: [Resp(200, {"value": [{"id": "a", "displayName": "A"}], "continuationToken": "t"}),
                          Resp(200, {"value": [{"id": "b", "displayName": "B"}]})]})
    assert nd.list_notebooks(c, WS) == {"a": "a", "b": "b"}


# --- recording the id for convert --resolutions -------------------------------------------------------------


def test_binding_adds_the_item_and_workspace_and_keeps_other_entries(tmp_path):
    f = tmp_path / "resolutions.json"
    f.write_text(json.dumps([{"type": "LinkedServiceToConnectionId", "key": "LS", "value": NB}]), encoding="utf-8")
    changes = nd.bind_resolutions(f, "NB_Read_Raw_Data", NB, WS)
    data = json.loads(f.read_text(encoding="utf-8"))
    assert {"type": "ItemToId", "key": "notebook:NB_Read_Raw_Data", "value": NB} in data
    assert {"type": "WorkspaceId", "key": "*", "value": WS} in data
    assert data[0]["key"] == "LS" and len(changes) == 2
    assert nd.bind_resolutions(f, "nb_read_raw_data", NB, WS) == []  # same again: nothing changes


def test_binding_updates_a_changed_id_and_creates_a_missing_file(tmp_path):
    f = tmp_path / "new.json"
    nd.bind_resolutions(f, "NB", NB, WS)
    other = "55555555-5555-5555-5555-555555555555"
    assert nd.bind_resolutions(f, "NB", other, WS) == ["ItemToId notebook:NB: updated"]
    assert json.loads(f.read_text(encoding="utf-8"))[0]["value"] == other


def test_binding_refuses_a_corrupt_file_and_leaves_it_alone(tmp_path):
    f = tmp_path / "r.json"
    f.write_text("{not json", encoding="utf-8")
    with pytest.raises(fd.DeployError, match="not changed"):
        nd.bind_resolutions(f, "NB", NB, WS)
    assert f.read_text(encoding="utf-8") == "{not json"


def test_a_regional_operation_address_is_not_followed_but_polled_on_the_fabric_host():
    op = "6d899865-241b-4694-8232-74ec47124d24"
    foreign = f"https://wabi-india-central-a-primary-redirect.analysis.windows.net/v1/operations/{op}"
    c, s = client({LIST: [notebooks((NB, "NB_X"))],
                   UPDATE: [Resp(202, {}, {"Location": foreign, "Retry-After": "1", "x-ms-operation-id": op})],
                   ("GET", f"/operations/{op}"): [Resp(200, {"status": "Succeeded"})]})
    assert nd.deploy_notebook(c, WS, "NB_X", DEF, apply=True).action == "update"
    assert all(call[1].startswith(fd.FABRIC_API + "/") for call in s.calls)
    assert any(call[1] == f"{fd.FABRIC_API}/operations/{op}" for call in s.calls)


def test_an_accepted_request_with_no_operation_address_is_an_error():
    c, _ = client({LIST: [notebooks((NB, "NB_X"))], UPDATE: [Resp(202, {}, {})]})
    with pytest.raises(fd.DeployError, match="no usable operation address"):
        nd.deploy_notebook(c, WS, "NB_X", DEF, apply=True)
