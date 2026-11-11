"""Deploying a notebook from a local file and binding its id for conversion.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import base64
import json

import pytest

from adf2fabric.agent.deployment import fabric_deploy as fd
from adf2fabric.agent.deployment import notebook_deploy as nd
from tests.test_fabric_deploy import WS

NB = "44444444-4444-4444-4444-444444444444"
LIST = ("GET", f"/workspaces/{WS}/notebooks")
CREATE = ("POST", f"/workspaces/{WS}/notebooks")
UPDATE = ("POST", f"/workspaces/{WS}/notebooks/{NB}/updateDefinition")


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


