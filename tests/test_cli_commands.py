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
