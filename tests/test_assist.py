"""Model-assisted drafts: optional and off by default, never authoritative, pinned to the source, checked by the project's own converter."""

from __future__ import annotations

import copy
import json
import subprocess

import pytest

from adf2fabric.agent.assist import checks, context, templates
from adf2fabric.agent.assist import model as M
from adf2fabric.agent.assist.service import AssistError, AssistService, apply
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


class FakeModel:
    def __init__(self, draft=None, confidence="high", unconverted=()):
        self.draft, self.confidence, self.unconverted, self.prompts = draft, confidence, list(unconverted), []

    def available(self):
        return {"available": True, "model": "fake", "timeout_seconds": 1, "message": None}

    def complete(self, prompt):
        self.prompts.append(prompt)
        return M.Answer(code=json.dumps(self.draft if self.draft is not None else _fixed(), indent=2), notes=["Replaced trigger().startTime with a parameter."],
                        unconverted=self.unconverted, confidence=self.confidence, model="fake-model")


@pytest.fixture()
def project(tmp_path, monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_LLM", "on")
    store, pid = _make_project(tmp_path, {"pipeline/PL_T.json": SOURCE, "pipeline/PL_Clean.json": CLEAN})
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    assert ConversionAgent(store).run(ConversionRequest(project_name="test-proj")).success
    return store, pid


def svc(project, model=None):
    store, pid = project
    return AssistService(store, pid, model=model or FakeModel(), synchronous=True)


def convert(project, **kw):
    store, _ = project
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj", **kw))
    assert r.success, r.error
    return r.conversion


def status(conv, name):
    return next(p for p in conv.pipelines if p.source_pipeline == name).status.value


# --- optional, off by default ---------------------------------------------------------------------------------------------------------


def test_it_is_off_unless_switched_on_and_nothing_is_sent(project, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_LLM", raising=False)
    model = FakeModel()
    s = svc(project, model)
    assert s.settings()["enabled"] is False and "ADF2FABRIC_LLM=on" in s.settings()["disabled_reason"]
    with pytest.raises(AssistError) as e:
        s.generate(["PL_T"])
    assert e.value.status == 409 and not model.prompts


def test_only_pipelines_the_converter_left_unfinished_are_candidates(project):
    found = svc(project).candidates()
    assert [c["pipeline"] for c in found["items"]] == ["PL_T"] and found["summary"]["total"] == 1
    with pytest.raises(AssistError, match="nothing for a model to fix"):
        svc(project).prompt("PL_Clean")


# --- the flow: draft, review, accept, convert ------------------------------------------------------------------------------------------


def test_generate_accept_and_convert_assisted_finishes_what_the_converter_could_not_and_marks_it(project):
    s = svc(project)
    assert status(convert(project), "PL_T") == "manual_required"
    s.generate(["PL_T"])
    d = s.detail("PL_T")
    assert d["state"] == "draft" and d["draft"]["confidence"] == "high" and d["draft"]["stale"] is False
    assert d["draft"]["checks"][0]["level"] == "ok" and any("now needs a person for 0" in c["message"] for c in d["draft"]["checks"])
    assert status(convert(project), "PL_T") == "manual_required"  # a draft alone changes nothing
    s.accept("PL_T", by="sam")
    assert status(convert(project), "PL_T") == "manual_required"  # accepted, but not asked for
    conv = convert(project, assisted=True)
    assert status(conv, "PL_T") == "converted" and status(conv, "PL_Clean") == "converted"
    assert [n.pipeline for n in conv.assisted] == ["PL_T"] and conv.assisted[0].accepted_by == "sam" and not conv.assisted[0].edited
    assert any("model-assisted edit of the source" in n for n in conv.schema_notes)


def test_the_original_source_file_is_never_modified(project):
    store, pid = project
    f = next(store.source_dir(pid).rglob("PL_T.json"))
    before = f.read_bytes()
    s = svc(project)
    s.generate(["PL_T"])
    s.accept("PL_T")
    convert(project, assisted=True)
    assert f.read_bytes() == before


def test_an_accepted_draft_is_pinned_to_its_source_and_ignored_when_the_source_changes(project):
    store, pid = project
    s = svc(project)
    s.generate(["PL_T"])
    s.accept("PL_T")
    f = next(store.source_dir(pid).rglob("PL_T.json"))
    data = json.loads(f.read_text("utf-8"))
    data["properties"]["activities"][1]["typeProperties"]["waitTimeInSeconds"] = 99
    f.write_text(json.dumps(data), encoding="utf-8")
    _, notes, log = apply(store, pid, store.load_adf_semantic_discovery(pid))
    assert notes == [] and any("out of date" in x for x in log)
    assert s.detail("PL_T")["accepted"]["stale"] is True
    with pytest.raises(AssistError, match="source changed"):  # and a stale draft cannot be accepted
        s2 = svc(project)
        s2.generate(["PL_T"])  # a fresh draft is made from the new source...
        record = s2.drafts.get("PL_T")
        record["draft"]["source_hash"] = "old"
        s2.drafts.put(record)
        s2.accept("PL_T")


def test_discarding_goes_back_to_the_original(project):
    s = svc(project)
    s.generate(["PL_T"])
    s.accept("PL_T")
    s.discard("PL_T", "all")
    assert s.detail("PL_T")["state"] == "none"
    assert status(convert(project, assisted=True), "PL_T") == "manual_required"


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


def test_accepting_an_edited_draft_re_checks_it_and_a_failing_edit_is_refused(project):
    s = svc(project)
    s.generate(["PL_T"])
    bad = _fixed()
    bad["properties"]["activities"].pop(1)
    with pytest.raises(AssistError, match="cannot be accepted yet"):
        s.accept("PL_T", code=json.dumps(bad))
    good = _fixed()
    good["properties"]["activities"][1]["description"] = "kept"
    assert s.accept("PL_T", code=json.dumps(good), by="sam")["accepted"]["edited"] is True


# --- what is sent ----------------------------------------------------------------------------------------------------------------------


def test_the_prompt_holds_the_source_the_problems_and_the_rules_and_no_secret(tmp_path, monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_LLM", "on")
    leaky = copy.deepcopy(SOURCE)
    leaky["properties"]["activities"].append(act("Call", "WebActivity", {"url": "https://x.example/a", "method": "POST", "headers": {"x": "y"},
                                                                          "authentication": {"type": "Basic", "username": "u", "password": "hunter2-real-secret"}}))
    store, pid = _make_project(tmp_path, {"pipeline/PL_T.json": leaky})
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    s = AssistService(store, pid, model=FakeModel(), synchronous=True)
    text = s.prompt("PL_T")["prompt"]
    assert "hunter2-real-secret" not in text and "Set_Start" in text and "trigger().startTime" in text
    assert "unsupported_trigger" in text or "manual required" in text and "Never write a secret" in text
    assert "Do not invent" in text and "SetVariable" in text  # the supported activity types come from the converter's registry
    assert s.detail("PL_T")["source"]["removed"]  # what was taken out before sending is listed
    s.generate(["PL_T"])
    assert "hunter2-real-secret" not in s._model.prompts[0]


def test_the_project_requirements_file_is_part_of_every_prompt(project, tmp_path):
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    for f in ("base.md", "pipeline.md"):
        (prompts / f).write_text((M.__file__ and __import__("pathlib").Path(__import__("adf2fabric.agent.assist.service", fromlist=["x"]).prompts_dir()) / f).read_text("utf-8"), encoding="utf-8")
    (prompts / "project.md").write_text("<!-- note -->\n- Our rule: {{object.name}} keeps names.\n", encoding="utf-8")
    store, pid = project
    text = AssistService(store, pid, model=FakeModel(), prompts=prompts).prompt("PL_T")["prompt"]
    assert "Our rule: PL_T keeps names." in text and "<!--" not in text


# --- the templates and the CLI call ----------------------------------------------------------------------------------------------------


def test_templates_support_values_blocks_comments_and_leave_braces_in_source_alone():
    ctx = {"a": {"b": "X"}, "items": ["one", "two"], "code": "{{not a placeholder}}", "empty": ""}
    out = templates.render("<!-- c -->\nA={{a.b}}\n{{#if empty}}no{{else}}yes{{/if}}\n{{#if a.b}}{{#if items}}{{items}}{{/if}}{{/if}}\n{{code}}", ctx)
    assert "A=X" in out and "yes" in out and "- one\n- two" in out and "{{not a placeholder}}" in out and "<!--" not in out
    with pytest.raises(templates.TemplateError):
        templates.render("{{#if a}} open", ctx)


def _envelope(code='{"name": "x"}', **extra):
    return json.dumps({"subtype": "success", "is_error": False, "structured_output": {"code": code, "notes": ["n"], "unconverted": [], "confidence": "medium"},
                       "modelUsage": {"claude-x": {}}, "total_cost_usd": 0.01, **extra})


def test_the_cli_runs_locked_down_from_an_empty_folder_and_reads_the_structured_answer(monkeypatch, tmp_path):
    seen = {}

    def run(cmd, **kw):
        seen.update(cmd=cmd, kw=kw, cwd_empty=not any(__import__("pathlib").Path(kw["cwd"]).iterdir()))
        return subprocess.CompletedProcess(cmd, 0, stdout=_envelope(), stderr="")

    monkeypatch.setattr(M, "executable", lambda: "claude")
    a = M.ClaudeCli(run=run).complete("the prompt")
    cmd = seen["cmd"]
    assert cmd[:2] == ["claude", "-p"] and cmd[cmd.index("--tools") + 1] == "" and "--strict-mcp-config" in cmd and "--no-session-persistence" in cmd
    assert "--json-schema" in cmd and seen["kw"]["input"] == "the prompt" and seen["cwd_empty"]
    assert a.code == '{"name": "x"}' and a.confidence == "medium" and a.model == "claude-x" and a.cost_usd == 0.01


@pytest.mark.parametrize("stdout,rc,expect", [("not json", 0, "did not return JSON"), (json.dumps({"is_error": True, "result": "boom"}), 0, "reported an error"),
                                              (json.dumps({"subtype": "success", "structured_output": {"code": ""}}), 0, "no code"), ("", 1, "failed")])
def test_model_failures_are_plain_messages(monkeypatch, stdout, rc, expect):
    monkeypatch.setattr(M, "executable", lambda: "claude")
    run = lambda cmd, **kw: subprocess.CompletedProcess(cmd, rc, stdout=stdout, stderr="oops")  # noqa: E731
    with pytest.raises(M.ModelError, match=expect):
        M.ClaudeCli(run=run).complete("p")


def test_a_timeout_and_a_missing_cli_are_plain_messages(monkeypatch):
    monkeypatch.setattr(M, "executable", lambda: "claude")
    def slow(cmd, **kw):
        raise subprocess.TimeoutExpired(cmd, 1)
    with pytest.raises(M.ModelError, match="did not answer within"):
        M.ClaudeCli(run=slow, timeout=1).complete("p")
    monkeypatch.setattr(M, "executable", lambda: None)
    with pytest.raises(M.ModelError, match="not on this machine"):
        M.ClaudeCli().complete("p")


# --- the rest of the project knows about it ----------------------------------------------------------------------------------------------


def _assisted_project(project):
    s = svc(project)
    s.generate(["PL_T"])
    s.accept("PL_T", by="sam")
    return convert(project, assisted=True)


def test_the_conversion_report_has_a_section_that_marks_the_assisted_pipelines(project):
    store, pid = project
    _assisted_project(project)
    text = (store.conversion_dir(pid) / "conversion-report.md").read_text("utf-8")
    assert "## Model-assisted pipelines" in text and "`PL_T`" in text and "sam" in text and "original file in the repository is unchanged" in text
    convert(project)  # converting again without --assisted leaves no mark: nothing assisted was used
    assert "## Model-assisted pipelines" not in (store.conversion_dir(pid) / "conversion-report.md").read_text("utf-8")


def test_validation_compares_with_the_reviewed_source_and_lists_it_as_an_informational_check(project):
    from adf2fabric.agent.validation import engine

    store, pid = project
    _assisted_project(project)
    r = engine.validate(store, pid, stages={"static"})
    info = [c for c in r.checks if c.id == "source_inventory.assisted_source"]
    assert [(c.component, c.required) for c in info] == [("pipeline:PL_T", False)]
    assert not [x for x in r.components if x.id.startswith("pipeline:PL_T") and x.status.value in ("altered", "missing")]  # the reviewed edit is not an unexplained difference
    assert r.overall != "MIGRATION_VERIFIED"  # and it is never a success claim
    convert(project)
    plain = engine.validate(store, pid, stages={"static"})
    assert not [c for c in plain.checks if c.id == "source_inventory.assisted_source"]


def test_confidence_for_an_assisted_pipeline_is_capped_and_says_why():
    from types import SimpleNamespace as NS

    from adf2fabric.agent.structure import confidence as conf

    entry = {"pipeline": "P", "deployed": False, "adf": None, "fabric": None}
    conv = NS(activities=[], triggers=[])
    plain, assisted = conf.assess(entry, conv, [], None, None), conf.assess(entry, conv, [], None, None, assisted=True)
    assert any("model-assisted edit" in x for x in assisted.limits) and not any("model-assisted edit" in x for x in plain.limits)


# --- the CLI ---------------------------------------------------------------------------------------------------------------------------


def test_cli_flow_candidates_prompt_draft_show_accept_convert_assisted_discard(project, monkeypatch):
    from click.testing import CliRunner

    from adf2fabric import cli as cli_mod

    store, pid = project
    fake = FakeModel()
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    monkeypatch.setattr(M, "ClaudeCli", lambda *a, **k: fake)
    run = lambda *args: CliRunner().invoke(cli_mod.cli, [*args, "--project", "test-proj"], catch_exceptions=False)  # noqa: E731
    out = run("assist-candidates").output
    assert "MODEL-ASSISTED DRAFTS  on" in out and "PL_T" in out and "1 candidate(s)" in out
    assert "Set_Start" in run("assist-prompt", "--pipeline", "PL_T").output and not fake.prompts  # preview sends nothing
    out = run("assist-draft", "--pipeline", "PL_T").output
    assert "draft ready (high confidence" in out and "ok: Passed the automatic checks" in out and len(fake.prompts) == 1
    assert "note: Replaced trigger().startTime" in run("assist-show", "--pipeline", "PL_T").output
    assert "accepted by" in run("assist-accept", "--pipeline", "PL_T").output
    out = run("convert", "--assisted").output
    assert "PL_T: converted" in out and "model-assisted: PL_T was converted from the draft accepted" in out
    assert "PL_T: manual_required" in run("convert").output
    assert "discarded" in run("assist-discard", "--pipeline", "PL_T", "--all").output
    assert "PL_T: manual_required" in run("convert", "--assisted").output


def test_cli_draft_is_refused_while_the_feature_is_off(project, monkeypatch):
    from click.testing import CliRunner

    from adf2fabric import cli as cli_mod

    store, _ = project
    monkeypatch.delenv("ADF2FABRIC_LLM", raising=False)
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    out = CliRunner().invoke(cli_mod.cli, ["assist-draft", "--project", "test-proj", "--pipeline", "PL_T"])
    assert out.exit_code == 1 and "off on this server" in out.output
    shown = CliRunner().invoke(cli_mod.cli, ["assist-candidates", "--project", "test-proj"]).output
    assert "OFF" in shown and "ADF2FABRIC_LLM=on" in shown


def test_the_arm_envelope_and_azure_resource_ids_are_never_sent(tmp_path, monkeypatch):
    """A pipeline exported from Azure carries an id holding the subscription, resource group and factory: none of that may reach the model."""
    monkeypatch.setenv("ADF2FABRIC_LLM", "on")
    exported = copy.deepcopy(SOURCE)
    sub = "dbd817b4-9d82-42c5-85b5-8ec49b01907d"
    exported.update({"id": f"/subscriptions/{sub}/resourceGroups/secret-rg/providers/Microsoft.DataFactory/factories/secret-factory/pipelines/PL_T",
                     "type": "Microsoft.DataFactory/factories/pipelines", "etag": "45025cee-0000-2000-0000-6ac8f06c0000"})
    exported["properties"]["lastPublishTime"] = "2026-10-09T13:47:24Z"
    exported["properties"]["activities"][1]["description"] = f"see /subscriptions/{sub}/resourceGroups/secret-rg/x"
    store, pid = _make_project(tmp_path, {"pipeline/PL_T.json": exported})
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    s = AssistService(store, pid, model=FakeModel(), synchronous=True)
    text = s.prompt("PL_T")["prompt"]
    for leak in (sub, "secret-rg", "secret-factory", "45025cee", "2026-10-09", "Microsoft.DataFactory"):
        assert leak not in text, leak
    assert "<azure-resource-id>" in text and "Set_Start" in text
    s.generate(["PL_T"])
    assert sub not in s._model.prompts[0]
    s.accept("PL_T")  # a draft without the envelope is still a valid draft of this pipeline
    assert s.detail("PL_T")["state"] == "accepted"
