"""adf_publish (ARM template) sources and default-branch detection, offline."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import adf2fabric.repository.git as git_mod
from adf2fabric.agent.intake import IntakeAgent, IntakeRequest
from adf2fabric.agent.intake import agent as agent_mod
from adf2fabric.agent.intake import arm_template, github_source
from adf2fabric.agent.intake import source_validation as sv
from adf2fabric.connections.source_analysis import analyze_source
from adf2fabric.project.models import RepositoryMetadata
from adf2fabric.project.store import ProjectStore
from adf2fabric.repository.providers import RepositoryProvider
from tests.conftest import make_repo

FACTORY = "[concat(parameters('factoryName'), '/{}')]"


def _res(subtype: str, name: str, props: dict) -> dict:
    return {"name": FACTORY.format(name), "type": f"Microsoft.DataFactory/factories/{subtype}",
            "apiVersion": "2018-06-01", "properties": props, "dependsOn": []}


def _template() -> dict:
    return {
        "$schema": "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
        "parameters": {
            "factoryName": {"type": "string", "defaultValue": "adf-prod"},
            "Sql_connectionString": {"type": "secureString"},
            "StorageUrl": {"type": "string", "defaultValue": "https://acct.dfs.core.windows.net/"},
        },
        "resources": [
            _res("pipelines", "PL_Copy", {"activities": [{"name": "Wait1", "type": "Wait",
                                                          "typeProperties": {"waitTimeInSeconds": 1}}]}),
            _res("datasets", "DS_Lake", {"type": "Parquet", "linkedServiceName": {
                "referenceName": "LS_Lake", "type": "LinkedServiceReference"}}),
            _res("linkedServices", "LS_Lake", {"type": "AzureBlobFS",
                                               "typeProperties": {"url": "[parameters('StorageUrl')]"}}),
            _res("linkedServices", "LS_Sql", {"type": "AzureSqlDatabase", "typeProperties": {
                "connectionString": "[parameters('Sql_connectionString')]", "note": "[[literal"}}),
            _res("triggers", "TR_Daily", {"type": "ScheduleTrigger", "runtimeState": "Started"}),
            _res("managedVirtualNetworks/managedPrivateEndpoints", "default/pe1", {"fqdns": []}),
            {"name": "[concat(parameters('factoryName'), '/PL_Dup')]", "type": "Microsoft.Resources/deployments"},
        ],
    }


def _publish_layout(root: Path, with_linked: bool = False) -> Path:
    folder = root / "adf-prod"
    folder.mkdir(parents=True)
    (folder / "ARMTemplateForFactory.json").write_text(json.dumps(_template()), encoding="utf-8")
    if with_linked:
        linked = folder / "linkedTemplates"
        linked.mkdir()
        (linked / "ArmTemplate_0.json").write_text(json.dumps({"resources": [
            _res("pipelines", "PL_Linked", {"activities": []})]}), encoding="utf-8")
    return folder


def _meta(source: Path, branch: str = "adf_publish") -> RepositoryMetadata:
    return RepositoryMetadata(provider="local", repository_url="x", branch=branch, commit_sha="a" * 40,
                              fetched_at=datetime.now(timezone.utc), local_source_path=str(source))


# --- ARM template extraction ------------------------------------------------

def test_extract_writes_git_layout(tmp_path):
    folder = _publish_layout(tmp_path)
    ex = arm_template.extract(tmp_path, ["adf-prod/ARMTemplateForFactory.json"])

    assert ex is not None and ex.root == folder / arm_template.EXTRACT_DIR
    assert ex.counts == {"dataset": 1, "linkedService": 2, "pipeline": 1, "trigger": 1}
    assert (ex.root / "pipeline" / "PL_Copy.json").is_file()
    body = json.loads((ex.root / "linkedService" / "LS_Lake.json").read_text(encoding="utf-8"))
    assert body["name"] == "LS_Lake"
    assert body["properties"]["typeProperties"]["url"] == "https://acct.dfs.core.windows.net/"  # default applied


def test_secure_parameters_and_expressions_stay_unresolved(tmp_path):
    _publish_layout(tmp_path)
    ex = arm_template.extract(tmp_path, ["adf-prod/ARMTemplateForFactory.json"])

    sql = json.loads((ex.root / "linkedService" / "LS_Sql.json").read_text(encoding="utf-8"))["properties"]
    assert sql["typeProperties"]["connectionString"] == "[parameters('Sql_connectionString')]"  # never invented
    assert sql["typeProperties"]["note"] == "[literal"  # "[[" is ARM's escape
    assert ex.unresolved_expressions == 1
    assert any("managedPrivateEndpoints" in s.lower() or "managedvirtualnetworks" in s.lower() for s in ex.skipped)


def test_extract_reads_linked_templates(tmp_path):
    _publish_layout(tmp_path, with_linked=True)
    ex = arm_template.extract(tmp_path, ["adf-prod/ARMTemplateForFactory.json"])
    assert ex.counts["pipeline"] == 2
    assert (ex.root / "pipeline" / "PL_Linked.json").is_file()


def test_extract_is_repeatable_and_ignores_unrelated_templates(tmp_path):
    _publish_layout(tmp_path)
    (tmp_path / "other.json").write_text("{}", encoding="utf-8")
    first = arm_template.extract(tmp_path, ["adf-prod/ARMTemplateForFactory.json"])
    second = arm_template.extract(tmp_path, ["adf-prod/ARMTemplateForFactory.json"])
    assert first.counts == second.counts


def test_template_without_factory_resources_extracts_nothing(tmp_path):
    (tmp_path / "ARMTemplateForFactory.json").write_text(json.dumps({"resources": []}), encoding="utf-8")
    assert arm_template.extract(tmp_path, ["ARMTemplateForFactory.json"]) is None


def test_unsafe_resource_names_are_skipped(tmp_path):
    t = {"resources": [_res("pipelines", "../evil", {"activities": []}), _res("pipelines", "ok", {})]}
    (tmp_path / "ARMTemplateForFactory.json").write_text(json.dumps(t), encoding="utf-8")
    ex = arm_template.extract(tmp_path, ["ARMTemplateForFactory.json"])
    assert ex.counts == {"pipeline": 1}
    assert not (tmp_path / "evil.json").exists()
    assert any("evil" in s for s in ex.skipped)


# --- through intake ---------------------------------------------------------

def test_build_manifest_uses_generated_layout_as_source_root(tmp_path):
    _publish_layout(tmp_path)
    manifest = github_source.build_manifest("p", _meta(tmp_path), tmp_path)

    assert manifest.subdirectory == f"adf-prod/{arm_template.EXTRACT_DIR}"
    assert {a.kind for a in manifest.artifacts} == {"pipeline", "dataset", "linkedService", "trigger"}
    assert any(f.code == "arm_template_extracted" for f in manifest.findings)
    assert not any(f.code == "arm_template_only" for f in manifest.findings)


def test_second_manifest_build_does_not_double_count(tmp_path):
    _publish_layout(tmp_path)
    first = github_source.build_manifest("p", _meta(tmp_path), tmp_path)
    second = github_source.build_manifest("p", _meta(tmp_path), tmp_path)
    assert len(first.artifacts) == len(second.artifacts)
    assert first.content_sha256 == second.content_sha256


def test_pipeline_selection_works_on_extracted_source(tmp_path):
    _publish_layout(tmp_path)
    manifest = github_source.build_manifest("p", _meta(tmp_path), tmp_path, pipelines=["PL_Copy"])
    assert manifest.selection.pipelines == ["PL_Copy"]


def test_analyze_source_extracts_for_git_and_zip_paths(tmp_path):
    _publish_layout(tmp_path)
    summary = analyze_source(tmp_path)
    assert summary.pipelines == ["PL_Copy"]
    assert summary.root.endswith(arm_template.EXTRACT_DIR)
    assert summary.arm_templates == ["adf-prod/ARMTemplateForFactory.json"]
    assert summary.layout_ok


def test_git_layout_source_is_untouched(tmp_path):
    (tmp_path / "pipeline").mkdir()
    (tmp_path / "pipeline" / "PL_A.json").write_text('{"name": "PL_A", "properties": {"activities": []}}',
                                                     encoding="utf-8")
    (tmp_path / "ARMTemplateForFactory.json").write_text(json.dumps(_template()), encoding="utf-8")
    loaded = sv.load_source(tmp_path)
    root, again, ex = sv.with_arm_extraction(tmp_path, loaded)
    assert ex is None and root == tmp_path and again is loaded
    assert not (tmp_path / arm_template.EXTRACT_DIR).exists()


# --- default branch ---------------------------------------------------------

class _LocalProvider(RepositoryProvider):
    name = "local"

    def normalize_url(self, url: str) -> str:
        return url

    def authenticated_url(self, url: str, token: str) -> str:
        return url


def test_default_branch_reads_remote_head(tmp_path):
    repo = make_repo(tmp_path / "origin", branch="master")
    assert git_mod.default_branch(str(repo)) == "master"


def test_default_branch_is_none_for_empty_remote(tmp_path):
    import subprocess
    empty = tmp_path / "empty"
    subprocess.run(["git", "init", "-b", "main", str(empty)], check=True, capture_output=True)
    assert git_mod.default_branch(str(empty)) is None


@pytest.fixture
def local_intake(monkeypatch, tmp_path):
    monkeypatch.setattr(agent_mod, "get_provider", lambda name: _LocalProvider())
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_TESTTOKEN123")
    return IntakeAgent(ProjectStore(tmp_path / "projects"))


def test_intake_without_a_branch_follows_the_repos_default(local_intake, tmp_path):
    repo = make_repo(tmp_path / "origin", branch="master")
    result = local_intake.run(IntakeRequest(project_name="demo", repository_url=str(repo), provider="local"))

    assert result.success, result.error
    assert result.project.repository.branch == "master"
    assert "master" in result.message


def test_named_branch_still_wins(local_intake, tmp_path):
    repo = make_repo(tmp_path / "origin", branch="master", extra_branches=("adf_publish",))
    result = local_intake.run(IntakeRequest(project_name="demo", repository_url=str(repo), branch="adf_publish",
                                            provider="local"))
    assert result.success, result.error
    assert result.project.repository.branch == "adf_publish"


def test_rerun_without_a_branch_reuses_the_existing_clone(local_intake, tmp_path):
    repo = make_repo(tmp_path / "origin", branch="master")
    request = IntakeRequest(project_name="demo", repository_url=str(repo), provider="local")
    assert local_intake.run(request).success
    again = local_intake.run(request)
    assert again.success and again.reused and again.project.repository.branch == "master"
