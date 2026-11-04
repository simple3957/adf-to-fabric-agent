"""The CLI commands that read or change a project, driven through click against a temporary projects folder."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from adf2fabric import cli as cli_mod
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from tests.test_adf_semantic_discovery import _make_project
from tests.test_conversion_agent import FILES


@pytest.fixture
def project(tmp_path, monkeypatch):
    store, pid = _make_project(tmp_path, FILES)
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    return store, pid


def run(*args):
    return CliRunner().invoke(cli_mod.cli, list(args))


def test_commands_that_need_a_conversion_say_so_instead_of_failing_silently(project):
    for cmd in ("handover", "confidence"):
        r = run(cmd, "--project", "test-proj")
        assert r.exit_code == 1, (cmd, r.output)


def test_convert_then_the_read_only_reports(project):
    store, pid = project
    r = run("convert", "--project", "test-proj")
    assert r.exit_code == 0, r.output
    assert store.has_conversion(pid)

    handover = run("handover", "--project", "test-proj")
    assert handover.exit_code == 0 and "HANDOVER" in handover.output
    assert (store.project_dir(pid) / "handover.md").is_file()

    conf = run("confidence", "--project", "test-proj")
    assert conf.exit_code == 0 and "CONFIDENCE" in conf.output
    data = json.loads((store.project_dir(pid) / "confidence" / "confidence.json").read_text(encoding="utf-8"))
    assert data["overall"]["low"] <= data["overall"]["high"]

    diag = run("diagnose", "--project", "test-proj")
    assert diag.exit_code == 0 and "DIAGNOSIS" in diag.output
    assert (store.project_dir(pid) / "diagnosis" / "diagnosis.json").is_file()


def test_diagnose_names_a_missing_project(project):
    r = run("diagnose", "--project", "no-such-project")
    assert r.exit_code == 1


def test_the_brief_is_shown_answered_and_exported(project, tmp_path):
    shown = run("brief", "--project", "test-proj")
    assert shown.exit_code == 0, shown.output
    md, qs = tmp_path / "brief.md", tmp_path / "questions.json"
    out = run("brief", "--project", "test-proj", "--markdown", str(md), "--questionnaire", str(qs))
    assert out.exit_code == 0 and md.is_file()
    assert isinstance(json.loads(qs.read_text(encoding="utf-8")), (list, dict))

    accepted = run("brief-accept", "--project", "test-proj")
    assert accepted.exit_code == 0 and "Accepted" in accepted.output


def test_a_malformed_or_unknown_answer_is_refused_and_nothing_is_saved(project):
    assert run("brief-answer", "--project", "test-proj", "--set", "no-equals-sign").exit_code != 0
    assert run("brief-answer", "--project", "test-proj", "--set", "scope.pipelines=[not json").exit_code != 0
    r = run("brief-answer", "--project", "test-proj", "--set", "no.such.question=x")
    assert r.exit_code == 1 and "Not saved" in r.output


def test_deploy_records_where_it_deployed_like_the_api_does(project, monkeypatch):
    """The Report and the UI read the deploy record: a CLI deploy that did not write it left them showing the previous workspace."""
    from types import SimpleNamespace

    from adf2fabric.agent.deployment import fabric_deploy, record as deploy_record
    from adf2fabric.connectivity import _azure

    store, pid = project
    assert run("convert", "--project", "test-proj").exit_code == 0
    token = SimpleNamespace(get_token=lambda scope: SimpleNamespace(token="t"))
    monkeypatch.setattr(_azure, "build_credential", lambda cred, **kw: (token, ""))
    monkeypatch.setattr(fabric_deploy, "FabricClient", lambda tok, **kw: object())
    monkeypatch.setattr(fabric_deploy, "deploy", lambda client, ws, items, apply, allow_unbound=False:
                        [SimpleNamespace(name=i.name, action="update", detail="" if apply else "dry run: nothing was changed", item_id="x", children=[]) for i in items])
    dry = run("deploy", "--project", "test-proj", "--workspace-id", "ws-dry", "--allow-unbound")
    assert dry.exit_code == 0, dry.output
    rec = deploy_record.load(store.project_dir(pid))
    assert rec["last_dry_run"]["workspace"] == "ws-dry" and "pipelines" not in rec  # a dry run deploys nothing
    applied = run("deploy", "--project", "test-proj", "--workspace-id", "ws-real", "--allow-unbound", "--apply")
    assert applied.exit_code == 0, applied.output
    rec = deploy_record.load(store.project_dir(pid))
    assert rec["last_apply"]["workspace"] == "ws-real" and rec["pipelines"]
    assert {v["workspace"] for v in rec["pipelines"].values()} == {"ws-real"} and all(v["ok"] for v in rec["pipelines"].values())
