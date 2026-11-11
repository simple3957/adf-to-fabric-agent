"""Retrieving Databricks notebooks over the workspace REST API.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import json

import pytest

from adf2fabric.agent.discovery import databricks_notebooks as dn

HOST = "https://adb-1.2.azuredatabricks.net"
TOKEN = "dapi-SECRET-TOKEN-123"


# --- the linked service ------------------------------------------------------------------------------------------


def ls(**tp):
    return {"name": "LS_Dbx", "properties": {"type": "AzureDatabricks", "typeProperties": tp}}


def test_the_workspace_url_and_auth_are_read_without_exposing_the_token():
    info = dn.inspect_linked_service(ls(domain="https://adb-1.2.azuredatabricks.net/", existingClusterId="0101-abc",
                                        accessToken={"type": "SecureString", "value": TOKEN}))
    assert info.workspace_url == HOST and info.authentication == "access_token"
    assert (info.cluster, info.existing_cluster_id) == ("existing_cluster", "0101-abc")
    assert TOKEN not in json.dumps(info.__dict__)


@pytest.mark.parametrize("tp,auth,cluster", [
    ({"domain": "https://x", "authentication": "MSI", "newClusterVersion": "13.3.x", "newClusterNodeType": "n"},
     "managed_identity", "new_job_cluster"),
    ({"domain": "https://x", "accessToken": {"type": "AzureKeyVaultSecret", "secretName": "s"}, "instancePoolId": "p"},
     "key_vault_token", "instance_pool"),
    ({"domain": "https://x"}, "unknown", "unspecified"),
])
def test_other_authentication_and_cluster_shapes(tp, auth, cluster):
    info = dn.inspect_linked_service(ls(**tp))
    assert (info.authentication, info.cluster) == (auth, cluster)


def test_a_linked_service_of_another_type_is_noted():
    info = dn.inspect_linked_service({"name": "X", "properties": {"type": "AzureSqlDatabase", "typeProperties": {}}})
    assert any("not AzureDatabricks" in n for n in info.notes) and info.workspace_url is None


# --- the workspace API -------------------------------------------------------------------------------------------


@pytest.mark.parametrize("url", ["http://adb-1.azuredatabricks.net", "https://adb-1.azuredatabricks.net/api",
                                 "https://u:p@adb-1.azuredatabricks.net", "adb-1.azuredatabricks.net", ""])
def test_a_workspace_url_must_be_plain_https(url):
    with pytest.raises(dn.DatabricksError) as e:
        dn.DatabricksClient(url, TOKEN, session=object())
    assert e.value.code == "bad_workspace_url"


def test_a_missing_token_is_refused_before_any_request():
    with pytest.raises(dn.DatabricksError) as e:
        dn.DatabricksClient(HOST, "  ", session=object())
    assert e.value.code == "no_token"


# --- writing files -----------------------------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["/a/../../etc/passwd", "/a/b:c", "/"])
def test_paths_that_cannot_be_files_are_refused(tmp_path, bad):
    with pytest.raises(dn.DatabricksError) as e:
        dn.local_path(tmp_path, bad, "PYTHON")
    assert e.value.code == "unsafe_path"


# --- compatibility scan ------------------------------------------------------------------------------------------

SRC = """# Databricks notebook source
token = dbutils.secrets.get("scope", "key")
dbutils.widgets.text("day", "")
df = spark.read.parquet("dbfs:/mnt/raw/sales")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
dbutils.fs.ls("/mnt/raw")
# MAGIC %run ./helpers
df.write.format("delta").save("abfss://c@a.dfs.core.windows.net/out")
"""


def test_databricks_specific_constructs_are_found_with_line_numbers():
    found = {(f.code, f.line) for f in dn.analyze_source(SRC)}
    assert ("dbutils_secrets", 2) in found and ("dbutils_widgets", 3) in found
    assert ("dbfs_path", 4) in found and ("databricks_conf", 5) in found and ("dbutils_fs", 6) in found
    assert ("magic_run", 7) in found
    assert ("adls_direct_path", 8) in found  # a direct ADLS path is flagged: Fabric authenticates differently


def test_clean_code_has_no_findings():
    assert dn.analyze_source("df = spark.read.format('delta').load('Files/raw/x')\n") == []


def test_a_direct_adls_path_is_flagged_for_review():
    f = dn.analyze_source('storage = "abfss://migrationcontainer@datamigrationadftofabric.dfs.core.windows.net/a"\n')
    assert [(x.code, x.line, x.severity) for x in f] == [("adls_direct_path", 1, "review")]


# --- the command -------------------------------------------------------------------------------------------------


