"""Retrieving Databricks notebooks over the workspace REST API (fake workspace, no network)."""

from __future__ import annotations

import base64
import json
from types import SimpleNamespace as NS

import pytest
from click.testing import CliRunner

from adf2fabric.agent.discovery import databricks_notebooks as dn

HOST = "https://adb-1.2.azuredatabricks.net"
TOKEN = "dapi-SECRET-TOKEN-123"


class Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body or {}, headers or {}

    def json(self):
        return self._body


class FakeWorkspace:
    """Serves get-status and export from a dict of path -> (object_type, language, source)."""

    def __init__(self, objects, fail=None):
        self.objects, self.calls, self.fail = objects, [], fail or {}

    def request(self, method, url, headers=None, timeout=None):
        self.calls.append((method, url, headers))
        from urllib.parse import parse_qs, urlsplit
        parts = urlsplit(url)
        path = parse_qs(parts.query)["path"][0]
        if path in self.fail:
            return self.fail[path]
        if path not in self.objects:
            return Resp(404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": "nope"})
        kind, lang, src = self.objects[path]
        if parts.path.endswith("get-status"):
            return Resp(200, {"object_type": kind, "path": path, **({"language": lang} if lang else {})})
        assert parse_qs(parts.query)["format"] == ["SOURCE"]
        return Resp(200, {"content": base64.b64encode(src.encode()).decode(), "file_type": "py"})


def client(objects, **kw):
    ws = FakeWorkspace(objects, **kw)
    return dn.DatabricksClient(HOST, TOKEN, session=ws, sleep=lambda _: None), ws


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


def activity(name, path, type_="DatabricksNotebook", children=()):
    return NS(name=name, type=type_, raw_properties={"notebookPath": path} if path is not None else {},
              activities=list(children))


def test_notebook_paths_are_taken_from_activities_including_nested_ones_and_expressions_are_reported():
    inner = activity("Inner", "/Shared/b")
    pls = [NS(name="P1", activities=[activity("A", "/Shared/a"), activity("W", None, "Wait"),
                                     activity("Loop", None, "ForEach", [inner]), activity("Dyn", "@{pipeline().x}"),
                                     activity("Rel", "relative/path")])]
    found, skipped = dn.notebook_paths_in_pipelines(pls)
    assert found == [("P1", "A", "/Shared/a"), ("P1", "Inner", "/Shared/b")]
    assert len(skipped) == 2 and all("P1/" in s for s in skipped)


# --- the workspace API -------------------------------------------------------------------------------------------


def test_export_returns_source_and_language_and_sends_the_token_only_to_the_workspace():
    c, ws = client({"/Shared/nb": ("NOTEBOOK", "PYTHON", "print(1)\n")})
    raw, lang = c.export_source("/Shared/nb")
    assert raw == b"print(1)\n" and lang == "PYTHON"
    assert all(call[1].startswith(HOST + "/api/2.0/workspace/") for call in ws.calls)
    assert all(call[2]["Authorization"] == f"Bearer {TOKEN}" for call in ws.calls)


@pytest.mark.parametrize("status,body,code", [
    (404, {"error_code": "RESOURCE_DOES_NOT_EXIST"}, "not_found"),
    (401, {}, "unauthorized"),
    (403, {}, "forbidden"),
    (400, {"error_code": "MAX_NOTEBOOK_SIZE_EXCEEDED"}, "too_large"),
    (500, {"error_code": "INTERNAL_ERROR", "message": "boom"}, "api_error"),
])
def test_failures_are_structured_with_actions_and_never_carry_the_token(status, body, code):
    c, _ = client({"/p": ("NOTEBOOK", "PYTHON", "x")}, fail={"/p": Resp(status, body)})
    with pytest.raises(dn.DatabricksError) as e:
        c.export_source("/p")
    assert e.value.code == code and TOKEN not in json.dumps(e.value.as_dict())
    if code != "api_error":
        assert e.value.actions


def test_a_folder_is_not_a_notebook():
    c, _ = client({"/Shared": ("DIRECTORY", None, "")})
    with pytest.raises(dn.DatabricksError) as e:
        c.export_source("/Shared")
    assert e.value.code == "not_a_notebook"


def test_rate_limiting_is_retried_then_reported():
    waits = []
    ws = FakeWorkspace({"/p": ("NOTEBOOK", "PYTHON", "x")}, fail={"/p": Resp(429, {}, {"Retry-After": "3"})})
    c = dn.DatabricksClient(HOST, TOKEN, session=ws, sleep=waits.append)
    with pytest.raises(dn.DatabricksError) as e:
        c.get_status("/p")
    assert e.value.code == "rate_limited" and e.value.retriable and waits == [3.0] * 5


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


def test_notebooks_are_written_under_their_workspace_path_with_the_language_extension(tmp_path):
    c, _ = client({"/Shared/etl/clean": ("NOTEBOOK", "PYTHON", "a = 1\n"), "/Users/me@x.com/q": ("NOTEBOOK", "SQL", "select 1"),
                   "/Shared/s": ("NOTEBOOK", "SCALA", "val a = 1")})
    res = dn.fetch_notebooks(c, ["/Shared/etl/clean", "/Users/me@x.com/q", "/Shared/s"], tmp_path,
                             {"/Shared/etl/clean": ["P/A"]})
    assert {r.local_file for r in res} == {"Shared/etl/clean.py", "Users/me@x.com/q.sql", "Shared/s.scala"}
    assert (tmp_path / "Shared" / "etl" / "clean.py").read_text() == "a = 1\n"
    assert next(r for r in res if r.workspace_path == "/Shared/etl/clean").used_by == ["P/A"]


def test_one_failure_does_not_stop_the_others_and_is_recorded(tmp_path):
    c, _ = client({"/ok": ("NOTEBOOK", "PYTHON", "x")})
    res = dn.fetch_notebooks(c, ["/missing", "/ok"], tmp_path)
    assert {r.workspace_path: r.status for r in res} == {"/missing": "error", "/ok": "ok"}
    assert next(r for r in res if r.status == "error").error["code"] == "not_found"


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


def test_cli_reports_the_linked_service_and_notebooks_without_a_token_and_fetches_with_one(tmp_path, monkeypatch):
    from adf2fabric import cli as cli_mod
    from adf2fabric.project.store import ProjectStore

    store = ProjectStore(tmp_path / "projects")
    pid = "demo"
    (store.source_root(pid) / "linkedService").mkdir(parents=True)
    (store.source_root(pid) / "linkedService" / "LS_Dbx.json").write_text(json.dumps(ls(
        domain=HOST, accessToken={"type": "SecureString", "value": TOKEN})), encoding="utf-8")
    monkeypatch.setattr(cli_mod, "projects_root", lambda cfg: tmp_path / "projects")
    monkeypatch.setattr(ProjectStore, "load_adf_semantic_discovery",
                        lambda self, p: NS(pipelines=[NS(name="PL", activities=[activity("N", "/Shared/nb")])]))
    ws = FakeWorkspace({"/Shared/nb": ("NOTEBOOK", "PYTHON", "dbutils.fs.ls('/')\n")})
    real = dn.DatabricksClient
    monkeypatch.setattr(dn, "DatabricksClient", lambda url, tok: real(url, tok, session=ws, sleep=lambda _: None))
    monkeypatch.setenv("DBX_TOKEN", TOKEN)

    out = CliRunner().invoke(cli_mod.cli, ["fetch-databricks-notebooks", "--project", pid, "--linked-service", "LS_Dbx"])
    assert out.exit_code == 0, out.output
    assert HOST in out.output and "access_token" in out.output and "/Shared/nb" in out.output and "nothing was fetched" in out.output
    assert TOKEN not in out.output and not ws.calls

    out = CliRunner().invoke(cli_mod.cli, ["fetch-databricks-notebooks", "--project", pid, "--linked-service", "LS_Dbx",
                                           "--token-env", "DBX_TOKEN"])
    assert out.exit_code == 0, out.output
    assert "ok     /Shared/nb" in out.output and "1 rewrite" in out.output and TOKEN not in out.output
    base = store.project_dir(pid) / "databricks"
    assert (base / "notebooks" / "Shared" / "nb.py").is_file()
    record = json.loads((base / "retrieval.json").read_text(encoding="utf-8"))
    assert record["notebooks"][0]["used_by"] == ["PL/N"] and TOKEN not in json.dumps(record)


def test_cli_explains_a_missing_token_variable_and_a_missing_linked_service(tmp_path, monkeypatch):
    from adf2fabric import cli as cli_mod

    monkeypatch.setattr(cli_mod, "projects_root", lambda cfg: tmp_path / "projects")
    monkeypatch.delenv("NOPE", raising=False)
    out = CliRunner().invoke(cli_mod.cli, ["fetch-databricks-notebooks", "--project", "x", "--linked-service", "Missing"])
    assert out.exit_code == 1 and "linked_service_not_found" in out.output
    out = CliRunner().invoke(cli_mod.cli, ["fetch-databricks-notebooks", "--project", "x", "--workspace-url", HOST,
                                           "--path", "/p", "--token-env", "NOPE"])
    assert out.exit_code == 1 and "no_token" in out.output
