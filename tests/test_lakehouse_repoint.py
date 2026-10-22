"""Opt-in Lakehouse repointing in conversion: ADLS datasets read/written through the storage map's OneLake shortcuts.

The emitted shape is the one that ran in Fabric (2026-10-22: Copy CSV and Parquet, Lookup, GetMetadata). Everything the
converter cannot repoint safely stays on its storage connection, with the reason, never guessed."""

from __future__ import annotations

import json

from adf2fabric.agent.conversion.agent import build_project_context
from adf2fabric.agent.conversion.context import Resolutions
from adf2fabric.agent.conversion.engine import ConversionEngine
from tests.conversion_support import act, dataset, expr, linked_service, pipe, ref, semantic

WS, LH = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
ADLS = linked_service("LS_ADLS", "AzureBlobFS", {"url": "https://acctone.dfs.core.windows.net/"})
OTHER = linked_service("LS_OTHER", "AzureBlobFS", {"url": "https://accttwo.dfs.core.windows.net/"})
BLOB = linked_service("LS_BLOB", "AzureBlobStorage", {"serviceEndpoint": "https://acctone.blob.core.windows.net/"})


def loc(container="raw", folder=None, file=None, type_="AzureBlobFSLocation"):
    out = {"type": type_, "fileSystem": container}
    if folder is not None:
        out["folderPath"] = folder
    if file is not None:
        out["fileName"] = file
    return out


CSV = dataset("DS_CSV", "DelimitedText", "LS_ADLS", {"location": loc("raw", "in/sales", "s.csv"), "columnDelimiter": ","})
PQ = dataset("DS_PQ", "Parquet", "LS_ADLS", {"location": loc("raw", "out"), "compressionCodec": "snappy"},
             params={"folder": {"type": "String"}})
PQ_DYN = dataset("DS_PQ_DYN", "Parquet", "LS_ADLS", {"location": {**loc("raw"), "folderPath": expr("@dataset().folder")}},
                 params={"folder": {"type": "String"}})
JSON = dataset("DS_JSON", "Json", "LS_ADLS", {"location": loc("raw", "j", "x.json")})
DYN_C = dataset("DS_DYNC", "Parquet", "LS_ADLS", {"location": {"type": "AzureBlobFSLocation", "fileSystem": expr("@dataset().c")}},
                params={"c": {"type": "String"}})
UNPLANNED = dataset("DS_UNPLANNED", "Parquet", "LS_OTHER", {"location": loc("raw", "x")})
BLOB_DS = dataset("DS_BLOB", "DelimitedText", "LS_BLOB", {"location": {"type": "AzureBlobStorageLocation", "container": "raw"}})
SQL_LS = linked_service("LS_SQL", "AzureSqlDatabase", {"server": "s.database.windows.net"})
SQL = dataset("DS_SQL", "AzureSqlTable", "LS_SQL", {"schema": "dbo", "table": "T"})
ALL = dict(datasets=[CSV, PQ, PQ_DYN, JSON, DYN_C, UNPLANNED, BLOB_DS, SQL], linked_services=[ADLS, OTHER, BLOB, SQL_LS])
RESOLUTIONS = [{"type": "WorkspaceId", "key": "*", "value": WS}, {"type": "ItemToId", "key": "lakehouse:lh_main", "value": LH}]
SRC = {"type": "DelimitedTextSource", "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": True}}
SNK = {"type": "ParquetSink", "storeSettings": {"type": "AzureBlobFSWriteSettings"}}


def run(*activities, routes=True, resolutions=RESOLUTIONS, parameters=None):
    d = semantic([pipe(*activities, parameters=parameters)], **ALL)
    ctx = build_project_context(d, Resolutions(resolutions))
    if routes:
        ctx.storage_routes, ctx.route_lakehouse = {("acctone", "raw"): "raw"}, "lh_main"
    rec, content = ConversionEngine(ctx).convert_pipeline(d.pipelines[0])
    return rec, content


def find(content, name):
    return next(a for a in content["properties"]["activities"] if a["name"] == name)


def copy(name="Copy", src="DS_CSV", snk="DS_PQ", source=SRC, sink=SNK, **snk_params):
    return act(name, "Copy", {"source": source, "sink": sink}, inputs=[ref("Dataset", src)], outputs=[ref("Dataset", snk, **snk_params)])


def issues(rec, code="storage_not_repointed"):
    return [i.message for a in rec.activities for i in a.warnings if i.code == code]


def test_copy_reads_and_writes_the_lakehouse_with_the_verified_shape():
    rec, content = run(copy())
    tp = find(content, "Copy")["typeProperties"]
    s, k = tp["source"], tp["sink"]
    link = {"name": "lh_main", "properties": {"annotations": [], "type": "Lakehouse",
                                              "typeProperties": {"workspaceId": WS, "artifactId": LH, "rootFolder": "Files"}}}
    assert s["datasetSettings"]["linkedService"] == link and k["datasetSettings"]["linkedService"] == link
    assert s["datasetSettings"]["typeProperties"]["location"] == {"type": "LakehouseLocation", "folderPath": "raw/in/sales", "fileName": "s.csv"}
    assert s["datasetSettings"]["typeProperties"]["columnDelimiter"] == ","  # format settings carried
    assert k["datasetSettings"]["typeProperties"]["location"] == {"type": "LakehouseLocation", "folderPath": "raw/out"}
    assert s["storeSettings"] == {"type": "LakehouseReadSettings", "recursive": True} and k["storeSettings"] == {"type": "LakehouseWriteSettings"}
    assert "externalReferences" not in json.dumps(tp)  # no storage connection is needed any more
    a = rec.activities[0]
    assert not any(p.kind == "connection" for p in a.prerequisites)
    assert any(m.rule == "storage_repointed" for m in a.property_mappings)


def test_without_the_option_nothing_changes():
    _, content = run(copy(), routes=False)
    tp = find(content, "Copy")["typeProperties"]
    assert tp["source"]["datasetSettings"]["externalReferences"] == {"connection": "<connection:LS_ADLS>"}
    assert tp["source"]["storeSettings"]["type"] == "AzureBlobFSReadSettings" and "Lakehouse" not in json.dumps(tp)


def test_an_unbound_lakehouse_is_a_placeholder_that_deploy_refuses():
    from adf2fabric.agent.deployment.fabric_deploy import unbound_placeholders

    _, content = run(copy(), resolutions=[])
    props = find(content, "Copy")["typeProperties"]["source"]["datasetSettings"]["linkedService"]["properties"]["typeProperties"]
    assert props["artifactId"] == "<item:lakehouse:lh_main>" and props["workspaceId"] == "<workspace-id>"
    assert "<item:lakehouse:lh_main>" in unbound_placeholders(content)


def test_a_folder_expression_gets_the_shortcut_in_front_and_is_otherwise_untouched():
    _, content = run(copy(snk="DS_PQ_DYN", folder=expr("@pipeline().parameters.day")), parameters=["day"])
    fp = find(content, "Copy")["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["location"]["folderPath"]
    assert fp == {"value": "@concat('raw/', pipeline().parameters.day)", "type": "Expression"}


def test_an_interpolated_folder_is_prefixed_too():
    # the expression converter normalises the interpolation to concat() first; the shortcut goes in front of the whole value
    _, content = run(copy(snk="DS_PQ_DYN", folder=expr("out/@{pipeline().parameters.day}")), parameters=["day"])
    fp = find(content, "Copy")["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["location"]["folderPath"]
    assert fp["type"] == "Expression" and fp["value"].startswith("@concat('raw/', ") and "pipeline().parameters.day" in fp["value"]


def test_lookup_and_getmetadata_are_repointed_and_delete_is_not():
    lookup = act("Lookup", "Lookup", {"source": SRC, "firstRowOnly": False, "dataset": ref("Dataset", "DS_CSV")})
    meta = act("Meta", "GetMetadata", {"dataset": ref("Dataset", "DS_CSV"), "fieldList": ["childItems"],
                                       "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": True}})
    delete = act("Del", "Delete", {"dataset": ref("Dataset", "DS_CSV"), "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": True}})
    rec, content = run(lookup, meta, delete)
    lk, md, dl = find(content, "Lookup")["typeProperties"], find(content, "Meta")["typeProperties"], find(content, "Del")["typeProperties"]
    assert lk["datasetSettings"]["linkedService"]["properties"]["type"] == "Lakehouse" and lk["source"]["storeSettings"]["type"] == "LakehouseReadSettings"
    assert md["datasetSettings"]["linkedService"]["properties"]["type"] == "Lakehouse" and md["storeSettings"]["type"] == "LakehouseReadSettings"
    assert dl["datasetSettings"]["externalReferences"] == {"connection": "<connection:LS_ADLS>"}  # Delete stays: never proven live
    assert dl["storeSettings"]["type"] == "AzureBlobFSReadSettings"


def test_what_cannot_be_repointed_stays_on_its_connection_with_the_reason():
    cases = [("unverified format", copy("C1", src="DS_JSON", source={"type": "JsonSource", "storeSettings": {"type": "AzureBlobFSReadSettings"}}),
              "format Json"),
             ("dynamic container", copy("C2", snk="DS_DYNC", c="x"), "container is empty or an expression"),
             ("no shortcut", copy("C3", snk="DS_UNPLANNED"), "no shortcut is planned for accttwo/raw"),
             ("path in storeSettings", copy("C4", source={"type": "DelimitedTextSource", "storeSettings": {
                 "type": "AzureBlobFSReadSettings", "wildcardFolderPath": "in/*"}}), "wildcardFolderPath")]
    for label, activity, why in cases:
        rec, content = run(activity)
        msgs = issues(rec)
        assert any(why in m for m in msgs), (label, msgs)
        assert "externalReferences" in json.dumps(find(content, activity["name"])), label


def test_non_storage_and_blob_datasets_are_left_alone_silently():
    sql_copy = act("Sql", "Copy", {"source": {"type": "AzureSqlSource"}, "sink": SNK}, inputs=[ref("Dataset", "DS_SQL")], outputs=[ref("Dataset", "DS_PQ")])
    blob_copy = copy("Blob", src="DS_BLOB", source={"type": "DelimitedTextSource", "storeSettings": {"type": "AzureBlobStorageReadSettings"}})
    rec, content = run(sql_copy, blob_copy)
    assert find(content, "Sql")["typeProperties"]["source"]["datasetSettings"]["externalReferences"]  # SQL is not storage
    assert find(content, "Sql")["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["properties"]["type"] == "Lakehouse"
    assert find(content, "Blob")["typeProperties"]["source"]["datasetSettings"]["externalReferences"]  # Blob location: not ADLS Gen2
    assert issues(rec) == []


def test_convert_needs_the_storage_map(tmp_path):
    from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
    from adf2fabric.project.store import ProjectStore

    store = ProjectStore(tmp_path)
    agent = ConversionAgent(store)
    agent.store.exists = lambda pid: True
    agent.store.has_adf_semantic_discovery = lambda pid: True
    d = semantic([pipe(copy())], **ALL)
    agent.store.load_adf_semantic_discovery = lambda pid: d
    agent.store.load = lambda pid: type("P", (), {"id": pid, "repository": type("R", (), {"commit_sha": d.source_commit_sha})()})()
    r = agent.run(ConversionRequest(project_name="p", lakehouse="lh_main"))
    assert not r.success and "storage-map" in r.error
