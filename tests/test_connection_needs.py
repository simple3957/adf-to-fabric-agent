"""Only linked services a converted activity still uses need a Fabric connection; the tool decides the rest by itself (2026-11-17).

The user's rule: "for wherever we are using the fabric notebooks or the next workaround is present it should only show that way and not
asking the user to create things manually". A real discovery and the real converter run here (connections/needs.py forecasts with it).
"""

from __future__ import annotations

import json

from adf2fabric.agent.conversion import project_mapping as PM
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from adf2fabric.connections import needs as N
from adf2fabric.connections.models import ConnectionState
from adf2fabric.connections.resolution import ConnectionChoice, build_cards
from adf2fabric.connections.service import ConnectionService
from tests.test_adf_semantic_discovery import _make_project
from tests.test_connections_resolution import conn, copy, ds, ls, ref


def _estate() -> dict:
    files = dict([
        ls("LS_LAKE", "AzureBlobFS", {"url": "https://lake1.dfs.core.windows.net"}),
        ls("LS_DBX", "AzureDatabricks", {"domain": "https://adb-1.2.azuredatabricks.net", "authentication": "MSI"}),
        ls("LS_UNUSED", "RestService", {"url": "https://api.example.com"}),
        ds("DS_IN", "Parquet", "LS_LAKE", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "raw"}}),
        ds("DS_OUT", "Parquet", "LS_LAKE", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "raw", "folderPath": "out"}}),
    ])
    files["pipeline/PL_Main.json"] = {"name": "PL_Main", "properties": {"activities": [
        copy("CopyRaw", "DS_IN", "DS_OUT"),
        {"name": "RunNotebook", "type": "DatabricksNotebook", "dependsOn": [], "linkedServiceName": ref("LinkedService", "LS_DBX"),
         "typeProperties": {"notebookPath": "/Shared/job"}}]}}
    return files


LAKE1, LAKE2 = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"


def _project(tmp_path, spark: str | None = "fabric"):
    """``spark``: the activity mapping's choice for the notebook activity (None leaves the default: it stays on Databricks)."""
    store, pid = _make_project(tmp_path, _estate())
    d = AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj")).discovery
    if spark:
        data = PM.build(store, pid)
        for row in data["activities"]:
            if row["name"] == "RunNotebook":
                row["chosen"] = spark
        (store.conversion_dir(pid) / PM.FILE).write_text(json.dumps(data), encoding="utf-8")
    return store, pid, d


def test_only_a_linked_service_a_converted_activity_uses_needs_a_connection(tmp_path):
    store, pid, _ = _project(tmp_path)
    told = N.needs(store, pid)  # before any convert: the converter runs in memory and writes nothing
    assert not store.has_conversion(pid)
    assert told["ls_lake"].needed and told["ls_lake"].source == "forecast" and "CopyRaw" in told["ls_lake"].needed_by
    dbx = told["ls_dbx"]
    assert not dbx.needed and "RunNotebook (DatabricksNotebook) became a Fabric notebook" in dbx.reason
    assert not told["ls_unused"].needed and "no activity uses it" in told["ls_unused"].reason


def test_a_notebook_that_stays_on_databricks_still_needs_its_connection(tmp_path):
    store, pid, _ = _project(tmp_path, spark=None)  # the default: the activity keeps running on Azure Databricks
    assert N.needs(store, pid)["ls_dbx"].needed


def test_a_not_needed_linked_service_is_shown_so_even_after_an_earlier_choice(tmp_path):
    store, pid, _ = _project(tmp_path)
    svc = ConnectionService(store)
    svc.set_choice(pid, ConnectionChoice(linked_service="LS_DBX", method="existing_connection",
                                         values={"connection_id": "11111111-1111-1111-1111-111111111111"}))
    cards = {c.name: c for c in svc.cards(pid, "s")}
    assert cards["LS_DBX"].state is ConnectionState.not_needed and cards["LS_DBX"].choice is None
    assert "Fabric notebook" in cards["LS_DBX"].next_steps[0]
    assert svc.load_choices(pid)["ls_dbx"].method == "existing_connection"  # what the person saved is kept on disk
    assert cards["LS_LAKE"].needed is True and cards["LS_LAKE"].state is not ConnectionState.not_needed


def test_a_needed_linked_service_reuses_the_one_connection_with_its_address_by_itself(tmp_path):
    store, pid, d = _project(tmp_path)
    conns = [conn(LAKE1, "lake-raw", "AzureDataLakeStorage", "https://lake1.dfs.core.windows.net/raw", "Key")]
    cards = {c.name: c for c in build_cards(d, connections=conns, needs=N.needs(store, pid))}
    lake = cards["LS_LAKE"]
    assert lake.planned.startswith("Will reuse the Fabric connection 'lake-raw'")
    assert lake.choice.origin == "auto_matched" and lake.choice.values == {"connection_id": LAKE1}
    assert lake.state is ConnectionState.connection_configured
    two = conns + [conn(LAKE2, "lake-raw-2", "AzureDataLakeStorage", "https://lake1.dfs.core.windows.net/raw", "Key")]
    again = {c.name: c for c in build_cards(d, connections=two, needs=N.needs(store, pid))}["LS_LAKE"]
    assert again.planned == "" and again.choice is None  # two with the same address: never picked between, the person chooses
    assert again.next_steps[0].startswith("Needs you:")
