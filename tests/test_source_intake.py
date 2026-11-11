"""Unified ADF source intake: Mode A (live Azure) and Mode B (GitHub).

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations


import pytest
from click.testing import CliRunner

from adf2fabric import cli as cli_mod
from adf2fabric.agent.discovery.adf_semantic import parser_pipelines
from adf2fabric.agent.intake.azure_source import (
    redact_artifact,
)
from adf2fabric.project.store import ProjectStore

SUB, RG, FACTORY = "sub-1111", "rg-demo", "adf-demo"
BASE = (
    f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}"
    f"/providers/Microsoft.DataFactory/factories/{FACTORY}"
)
TOKEN = "TOKEN_ABC_999"
SECRET_CONN = "SECRET_PW_123"
SECRET_SECURE = "SECRET_SECURE_456"


# --- ADF fixtures --------------------------------------------------------------


def ref(kind: str, name: str) -> dict:
    return {"referenceName": name, "type": f"{kind}Reference"}


def pipeline(name: str, *, dataset_in=None, dataset_out=None, child=None) -> dict:
    acts = []
    if dataset_in:
        acts.append({
            "name": f"Copy_{name}", "type": "Copy", "dependsOn": [],
            "typeProperties": {"source": {"type": "AzureSqlSource"}, "sink": {"type": "ParquetSink"}},
            "inputs": [ref("Dataset", dataset_in)], "outputs": [ref("Dataset", dataset_out)],
        })
    if child:
        acts.append({
            "name": f"Run_{child}", "type": "ExecutePipeline",
            "dependsOn": [{"activity": acts[0]["name"], "dependencyConditions": ["Succeeded"]}] if acts else [],
            "typeProperties": {"pipeline": ref("Pipeline", child), "waitOnCompletion": True},
        })
    return {"name": name, "properties": {"activities": acts, "parameters": {"p": {"type": "String"}}}}


def dataset(name: str, ls: str) -> dict:
    return {"name": name, "properties": {
        "type": "AzureSqlTable", "linkedServiceName": ref("LinkedService", ls),
        "typeProperties": {"schema": "dbo", "table": name}}}


def linked_service(name: str, secret: bool = False) -> dict:
    tp: dict = {"url": f"https://{name.lower()}.dfs.core.windows.net"}
    if secret:
        tp = {
            "connectionString": f"Server=tcp:x;Password={SECRET_CONN};",
            "password": {"type": "SecureString", "value": SECRET_SECURE},
        }
    return {"name": name, "properties": {"type": "AzureSqlDatabase" if secret else "AzureBlobFS",
                                         "typeProperties": tp}}


def trigger(name: str, pipeline_name: str) -> dict:
    return {"name": name, "properties": {
        "type": "ScheduleTrigger", "runtimeState": "Started",
        "pipelines": [{"pipelineReference": ref("Pipeline", pipeline_name)}],
        "typeProperties": {"recurrence": {"frequency": "Day", "interval": 1}}}}


def estate() -> dict[str, list[dict]]:
    """A small factory: PL_Main -> PL_Child, plus an unrelated pipeline."""
    return {
        "pipelines": [
            pipeline("PL_Main", dataset_in="DS_In", dataset_out="DS_Out", child="PL_Child"),
            pipeline("PL_Child"),
            pipeline("PL_Unrelated", dataset_in="DS_Other", dataset_out="DS_Other"),
        ],
        "datasets": [dataset("DS_In", "LS_Sql"), dataset("DS_Out", "LS_Lake"),
                     dataset("DS_Other", "LS_Other")],
        "linkedservices": [linked_service("LS_Sql", secret=True), linked_service("LS_Lake"),
                           linked_service("LS_Other")],
        "triggers": [trigger("TR_Main", "PL_Main"), trigger("TR_Other", "PL_Unrelated")],
        "dataflows": [],
        "integrationruntimes": [],
    }


def arm_item(segment: str, body: dict) -> dict:
    return {
        "id": f"/subscriptions/{SUB}/.../{body['name']}",
        "name": body["name"],
        "type": f"Microsoft.DataFactory/factories/{segment}",
        "etag": f"etag-{body['name']}",
        "properties": body["properties"],
    }


# --- harness -------------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ProjectStore(tmp_path / "projects")


# =============================================================================
# Mode A: live Azure Data Factory
# =============================================================================


def test_redact_artifact_leaves_key_vault_refs_and_masks_alone():
    body = {"properties": {"typeProperties": {
        "password": {"type": "AzureKeyVaultSecret", "store": ref("LinkedService", "KV"), "secretName": "pw"},
        "accountKey": "**********", "sasToken": "sv=2020&sig=abc"}}}
    out, recs = redact_artifact(body, "linkedService/X.json")
    tp = out["properties"]["typeProperties"]
    assert tp["password"]["secretName"] == "pw"  # reference, not a secret
    assert tp["accountKey"] == "**********"       # already masked: untouched
    assert tp["sasToken"] == "[REDACTED]"
    assert [r.field_path for r in recs] == ["properties.typeProperties.sasToken"]


# =============================================================================
# Mode B: GitHub repository
# =============================================================================


@pytest.fixture
def token_env(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_TESTTOKEN123")


# =============================================================================
# Both modes -> one parser interface
# =============================================================================


def test_azure_arm_item_and_git_item_both_accepted_by_parser():
    body = pipeline("PL_Main", dataset_in="DS_In", dataset_out="DS_Out", child="PL_Child")
    from_git = parser_pipelines.parse_pipeline(body, "pipeline/PL_Main.json")
    from_arm = parser_pipelines.parse_pipeline(arm_item("pipelines", body), "pipeline/PL_Main.json")
    assert from_git == from_arm and from_git.activity_count == 2


# =============================================================================
# CLI wiring
# =============================================================================


def test_cli_requires_exactly_one_source():
    runner = CliRunner()
    neither = runner.invoke(cli_mod.cli, ["intake", "--project", "x"])
    both = runner.invoke(cli_mod.cli, ["intake", "--project", "x", "--repo", "r", "--factory", "f"])
    assert neither.exit_code != 0 and "exactly one source" in neither.output
    assert both.exit_code != 0 and "exactly one source" in both.output


def test_cli_rejects_cross_mode_options():
    runner = CliRunner()
    a = runner.invoke(cli_mod.cli, ["intake", "--project", "x", "--factory", "f"])
    b = runner.invoke(cli_mod.cli, ["intake", "--project", "x", "--factory", "f", "--resource-group", "g",
                                    "--branch", "main"])
    c = runner.invoke(cli_mod.cli, ["intake", "--project", "x", "--repo", "r", "--resource-group", "g"])
    assert "--resource-group is required" in a.output
    assert "--repo only" in b.output
    assert "--factory only" in c.output


# --- change data capture (adfcdcs): a tolerant extra collection -----------------------------------------------


