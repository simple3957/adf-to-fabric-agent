"""Model-assisted drafts: optional and off by default, never authoritative, pinned to the source, checked by the project's own converter."""

from __future__ import annotations

import copy
import json

import pytest

from adf2fabric.agent.assist import checks, context, templates
from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from tests.conversion_support import act, dep, expr, pipe
from tests.test_adf_semantic_discovery import _make_project

# A schedule trigger in Fabric supplies no trigger() outputs, so the converter leaves this activity for a person.
SOURCE = pipe(
    act("Set_Start", "SetVariable", {"variableName": "start", "value": expr("@trigger().startTime")}),
    act("Pause", "Wait", {"waitTimeInSeconds": 1}, deps=[dep("Set_Start")]),
    name="PL_T", variables={"start": "String"}, parameters=["p1"])
CLEAN = pipe(act("Only", "Wait", {"waitTimeInSeconds": 1}), name="PL_Clean")


def _fixed(source=SOURCE):
    """The corrected source a good model would give: the trigger value comes from a pipeline parameter with a default."""
    d = copy.deepcopy(source)
    d["properties"]["parameters"]["runStart"] = {"type": "String", "defaultValue": "2026-01-01T00:00:00Z"}
    for a in d["properties"]["activities"]:
        if a["name"] == "Set_Start":
            a["typeProperties"]["value"] = {"value": "@pipeline().parameters.runStart", "type": "Expression"}
            a["description"] = "Fabric: the trigger value is now a parameter."
    return d


@pytest.fixture()
def project(tmp_path, monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_LLM", "on")
    store, pid = _make_project(tmp_path, {"pipeline/PL_T.json": SOURCE, "pipeline/PL_Clean.json": CLEAN})
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    assert ConversionAgent(store).run(ConversionRequest(project_name="test-proj")).success
    return store, pid


def convert(project, **kw):
    store, _ = project
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj", **kw))
    assert r.success, r.error
    return r.conversion


def status(conv, name):
    return next(p for p in conv.pipelines if p.source_pipeline == name).status.value


# --- optional, off by default ---------------------------------------------------------------------------------------------------------


# --- the flow: draft, review, accept, convert ------------------------------------------------------------------------------------------


# --- the checks ------------------------------------------------------------------------------------------------------------------------


def _check(project, mutate):
    store, pid = project
    subject = context.load_subject(store, pid, "PL_T")
    draft = _fixed()
    mutate(draft)
    return checks.run(store, pid, subject, json.dumps(draft))


def _errors(found):
    return " ".join(c["message"] for c in found if c["level"] == "error")


def test_a_good_draft_passes_and_a_converter_improvement_is_stated(project):
    found = _check(project, lambda d: None)
    assert found[0]["level"] == "ok" and not checks.blocking(found)


@pytest.mark.parametrize("mutate,expect", [
    (lambda d: d["properties"]["activities"].pop(1), "removed"),
    (lambda d: d.update(name="Other"), "cannot rename"),
    (lambda d: d["properties"]["activities"][1].update(type="Fail"), "changed type"),
    (lambda d: d["properties"]["activities"][1].pop("dependsOn"), "dependencies"),
    (lambda d: d["properties"]["activities"][1].update(dependsOn=[{"activity": "Set_Start", "dependencyConditions": ["Failed"]}]), "dependencies"),
    (lambda d: d["properties"]["parameters"].pop("p1"), "removed or renamed"),
    (lambda d: d["properties"]["activities"].append(act("Copy_X", "Copy", {"source": {"type": "S"}, "sink": {"type": "K"}}, inputs=[{"referenceName": "DS_Invented", "type": "DatasetReference"}])), "not in the source"),
    (lambda d: d["properties"]["activities"][1]["typeProperties"].update(password="hunter2-real-secret"), "secret"),
])
def test_every_kind_of_bad_draft_is_caught(project, mutate, expect):
    found = _check(project, mutate)
    assert checks.blocking(found) and expect in _errors(found)


def test_invalid_json_and_a_non_pipeline_are_refused(project):
    store, pid = project
    subject = context.load_subject(store, pid, "PL_T")
    assert "not valid JSON" in _errors(checks.run(store, pid, subject, "{ nope"))
    assert "no properties.activities" in _errors(checks.run(store, pid, subject, json.dumps({"name": "PL_T"})))


def test_a_draft_that_does_not_help_is_flagged_and_one_that_makes_it_worse_is_refused(project):
    store, pid = project
    subject = context.load_subject(store, pid, "PL_T")
    same = checks.run(store, pid, subject, json.dumps(SOURCE))
    assert not checks.blocking(same) and any("does not help" in c["message"] for c in same)
    worse = copy.deepcopy(SOURCE)
    worse["properties"]["activities"][1]["typeProperties"]["waitTimeInSeconds"] = expr("@trigger().scheduledTime")
    found = checks.run(store, pid, subject, json.dumps(worse))
    assert checks.blocking(found) and "makes it worse" in _errors(found)


# --- what is sent ----------------------------------------------------------------------------------------------------------------------


# --- the templates and the CLI call ----------------------------------------------------------------------------------------------------


def test_templates_support_values_blocks_comments_and_leave_braces_in_source_alone():
    ctx = {"a": {"b": "X"}, "items": ["one", "two"], "code": "{{not a placeholder}}", "empty": ""}
    out = templates.render("<!-- c -->\nA={{a.b}}\n{{#if empty}}no{{else}}yes{{/if}}\n{{#if a.b}}{{#if items}}{{items}}{{/if}}{{/if}}\n{{code}}", ctx)
    assert "A=X" in out and "yes" in out and "- one\n- two" in out and "{{not a placeholder}}" in out and "<!--" not in out
    with pytest.raises(templates.TemplateError):
        templates.render("{{#if a}} open", ctx)


# --- the rest of the project knows about it ----------------------------------------------------------------------------------------------


# --- the CLI ---------------------------------------------------------------------------------------------------------------------------


