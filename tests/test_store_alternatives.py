"""Datasets on stores other than ADLS Gen2 (cloud buckets, SFTP/FTP, file shares, ...): captured, explained from Microsoft's pages, with alternatives."""

from __future__ import annotations

import pytest

from adf2fabric.agent.dependencies.builder import build_inventory
from adf2fabric.agent.storage import stores
from adf2fabric.agent.storage.mapping import build_storage_map, render_markdown
from tests.conversion_support import act, convert_one, dataset, linked_service, pipe, ref, semantic

CASES = [  # (ADF location type, linked service type, extra location properties, store kind)
    ("AzureBlobStorageLocation", "AzureBlobStorage", {"container": "c"}, "blob"),
    ("AmazonS3Location", "AmazonS3", {"bucketName": "b"}, "s3"),
    ("AmazonS3CompatibleLocation", "AmazonS3Compatible", {"bucketName": "b"}, "s3_compatible"),
    ("GoogleCloudStorageLocation", "GoogleCloudStorage", {"bucketName": "b"}, "gcs"),
    ("AzureFileStorageLocation", "AzureFileStorage", {}, "azure_files"),
    ("SftpLocation", "Sftp", {}, "sftp"),
    ("FtpServerLocation", "FtpServer", {}, "ftp"),
    ("FileServerLocation", "FileServer", {}, "file_server"),
    ("HdfsLocation", "Hdfs", {}, "hdfs"),
    ("HttpServerLocation", "HttpServer", {}, "http"),
    ("OracleCloudStorageLocation", "OracleCloudStorage", {"bucketName": "b"}, "oracle_cloud_storage"),
    ("AzureDataLakeStoreLocation", "AzureDataLakeStore", {}, "adls_gen1"),
]


def _estate(loc, ls_type, extra):
    ds = dataset("DS_X", "DelimitedText", "LS_X", {"location": {"type": loc, "folderPath": "in", "fileName": "a.csv", **extra}})
    ls = linked_service("LS_X", ls_type, {"url": "https://x.example"})
    cp = act("Copy", "Copy", {"source": {"type": "DelimitedTextSource"}, "sink": {"type": "ParquetSink"}}, inputs=[ref("Dataset", "DS_X")], outputs=[ref("Dataset", "DS_X")])
    return semantic([pipe(cp)], datasets=[ds], linked_services=[ls])


@pytest.mark.parametrize("loc,ls_type,extra,kind", CASES)
def test_every_file_store_is_recognised_and_offers_alternatives(loc, ls_type, extra, kind):
    m = build_storage_map(_estate(loc, ls_type, extra))
    row = m.datasets[0]
    assert row.store == kind and row.status == "needs_decision"
    assert row.alternatives and "not a file store this tool knows" not in row.reason  # the S3-compatible, HDFS and Oracle ones used to be 'not recognised'
    assert stores.STORES[kind].label in row.reason
    assert m.to_decide[0]["alternatives"] == row.alternatives


def test_the_advice_matches_what_microsoft_documents():
    s = stores.STORES
    assert [s[k].shortcut for k in ("blob", "s3", "s3_compatible", "gcs")] == ["yes", "gateway", "gateway", "gateway"]
    assert [s[k].shortcut for k in ("sftp", "ftp", "file_server", "azure_files", "hdfs", "http", "oracle_cloud_storage")] == ["no"] * 7
    assert [s[k].copy for k in ("ftp", "hdfs", "http", "oracle_cloud_storage")] == ["read_only"] * 4 and s["adls_gen1"].copy == "none"
    assert "on-premises data gateway" in s["file_server"].gateway and "Windows" in s["file_server"].gateway


def test_the_markdown_lists_the_alternatives_and_cites_the_sources():
    text = render_markdown(build_storage_map(_estate("SftpLocation", "Sftp", {})))
    assert "Alternatives, per dataset" in text and "SFTP connection" in text and "learn.microsoft.com/fabric/onelake/onelake-shortcuts" in text


@pytest.mark.parametrize("loc,ls_type,extra,kind", CASES)
def test_the_dependency_inventory_records_the_external_store(loc, ls_type, extra, kind):
    inv = build_inventory(_estate(loc, ls_type, extra))
    deps = [x for x in inv.dependencies if x.type.value == "storage_path"]
    assert len(deps) == 1 and deps[0].attributes["store"] == kind
    assert deps[0].migration.summary.startswith(stores.STORES[kind].label) and deps[0].migration.actions


def test_adls_gen2_is_still_mapped_and_not_listed_as_an_other_store():
    ds = dataset("DS_L", "Parquet", "LS_L", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "raw", "folderPath": "f"}})
    ls = linked_service("LS_L", "AzureBlobFS", {"url": "https://acct.dfs.core.windows.net"})
    d = semantic([pipe(act("W", "Wait", {"waitTimeInSeconds": 1}))], datasets=[ds], linked_services=[ls])
    assert build_storage_map(d).datasets[0].status == "mapped"
    assert not [x for x in build_inventory(d).dependencies if x.attributes.get("store")]


def test_conversion_still_carries_these_datasets_unchanged():
    ds = dataset("DS_X", "DelimitedText", "LS_X", {"location": {"type": "SftpLocation", "folderPath": "in", "fileName": "a.csv"}})
    ls = linked_service("LS_X", "Sftp", {"host": "h"})
    cp = act("Copy", "Copy", {"source": {"type": "DelimitedTextSource"}, "sink": {"type": "ParquetSink"}}, inputs=[ref("Dataset", "DS_X")], outputs=[ref("Dataset", "DS_X")])
    rec, _ = convert_one(pipe(cp), datasets=[ds], linked_services=[ls])
    assert rec.activities[0].status.value in ("converted", "converted_with_differences")
