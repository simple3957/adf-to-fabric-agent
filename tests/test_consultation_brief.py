"""The migration consultation: questions grounded in what discovery found, recommendations with reasons, a versioned,
approved brief whose settings drive the run, and nothing guessed."""

from __future__ import annotations

import json

import pytest

from adf2fabric.agent.consultation import brief as B
from adf2fabric.agent.consultation import brief_record as BR

PIPES = ["PL_A", "PL_B", "PL_C"]


def facts(**kw) -> B.Facts:
    base = dict(discovered=True, pipelines=list(PIPES), activities={"Copy": 3}, linked_services=2, file_datasets=4, shortcuts_planned=1,
                pipeline_waves=[["PL_A", "PL_B"], ["PL_C"]])
    base.update(kw)
    return B.Facts(**base)


def ids(f: B.Facts, answers=None) -> set[str]:
    return {r.q.id for r in B.resolve(f, answers or {}) if r.applies}


def get(rs, qid):
    return next(r for r in rs if r.q.id == qid)


# ---- grounded in discovery --------------------------------------------------------------------------------------------


def test_questions_are_asked_only_about_what_discovery_found():
    plain = ids(facts(file_datasets=0, linked_services=0))
    assert not {q for q in plain if q.startswith(("compute.", "storage.", "connections."))}
    assert "pipelines.events" not in plain and "pipelines.dataflows" not in plain and "pipelines.unsupported" not in plain
    rich = ids(facts(activities={"Copy": 1, "DatabricksNotebook": 2, "ExecutePipeline": 1, "ExecuteDataFlow": 1}, dataflows=1,
                     triggers={"BlobEventsTrigger": 1, "ScheduleTrigger": 2}, self_hosted=1, key_vault=1, ls_params_no_default=2,
                     storage_undecided=3, blocked_activities=2, blocked_pipelines=["PL_C"], reconstruction_proposals=1))
    for q in ("compute.engine", "pipelines.events", "pipelines.dataflows", "pipelines.triggers", "pipelines.invoke", "connections.gateway",
              "connections.keyvault", "connections.parameters", "storage.undecided", "pipelines.unsupported", "pipelines.reconstruct"):
        assert q in rich, q


def test_a_question_that_depends_on_an_earlier_answer_appears_only_then():
    f = facts(activities={"DatabricksNotebook": 1}, notebooks_with_storage_paths=1)
    assert "scope.pipelines" not in ids(f) and "scope.pipelines" in ids(f, {"scope.mode": "selected"})
    assert "compute.code" not in ids(f) and {"compute.code", "compute.paths"} <= ids(f, {"compute.engine": "fabric"})
    assert "storage.lakehouse" in ids(f, {"storage.route": "lakehouse"}) and "storage.lakehouse" not in ids(f, {"storage.route": "connections"})


def test_what_the_configuration_says_is_read_not_asked():
    rs = B.resolve(facts(fabric_workspace="ws-prod", lakehouse_bound="LH_Main", activities={"ExecutePipeline": 1}, invoke_connection_bound=True),
                   {"target.workspace": "new", "storage.route": "lakehouse"})
    assert get(rs, "target.workspace").source == "config" and get(rs, "target.workspace").value == "ws-prod"  # a typed answer never overrides it
    assert get(rs, "storage.lakehouse").value == "LH_Main" and get(rs, "pipelines.invoke").source == "config"


def test_recommendations_carry_a_reason_from_the_counts_and_are_not_answers():
    small, big = B.resolve(facts(), {}), B.resolve(facts(pipelines=[f"P{i}" for i in range(80)], pipeline_waves=[["P0"]] * 6), {})
    assert get(small, "scope.mode").rec.value == "all" and "3 pipelines" in get(small, "scope.mode").rec.reason
    assert get(big, "scope.mode").rec.value == "pilot" and "80 pipelines" in get(big, "scope.mode").rec.reason
    assert get(small, "scope.mode").source == "recommendation"
    assert B.counts(facts(), small)["answered"] == 0  # nothing counts as answered until a person (or the configuration) says so


def test_answers_are_checked_against_the_questions_own_options():
    f = facts()
    q = B.QUESTION_BY_ID
    assert B.validate_answer(q["scope.mode"], f, "pilot") == "pilot"
    for qid, bad in (("scope.mode", "everything"), ("scope.mode", ["all"]), ("scope.pipelines", ["PL_A", "PL_Z"]), ("scope.pipelines", "PL_A"),
                     ("context.owner", "x" * 2001), ("context.owner", 3)):
        with pytest.raises(ValueError):
            B.validate_answer(q[qid], f, bad)
    assert B.validate_answer(q["scope.pipelines"], f, ["PL_B", "PL_B", "PL_A"]) == ["PL_B", "PL_A"]


# ---- settings the run follows ------------------------------------------------------------------------------------------


def test_scope_answers_set_the_pipelines_the_run_takes():
    f = facts()
    assert B.settings_from(f, B.resolve(f, {"scope.mode": "all"})).pipelines is None
    assert B.settings_from(f, B.resolve(f, {"scope.mode": "pilot"})).pipelines == ["PL_A", "PL_B"]  # the first wave
    assert B.settings_from(f, B.resolve(f, {"scope.mode": "selected", "scope.pipelines": ["PL_C"]})).pipelines == ["PL_C"]
    held = facts(blocked_activities=1, blocked_pipelines=["PL_B"])
    s = B.settings_from(held, B.resolve(held, {"pipelines.unsupported": "hold"}))
    assert s.pipelines == ["PL_A", "PL_C"] and s.hold_blocked == ["PL_B"]


def test_storage_compute_and_validation_answers_map_to_real_options():
    f = facts(activities={"DatabricksNotebook": 1}, notebooks_with_storage_paths=1)
    s = B.settings_from(f, B.resolve(f, {"storage.route": "lakehouse", "storage.lakehouse": " LH_X ", "compute.engine": "fabric",
                                         "compute.paths": "shortcuts", "validation.depth": "run"}))
    assert s.lakehouse == "LH_X" and s.workloads_to_fabric and s.route_notebook_paths and s.allow_real_runs
    assert s.validate_stages == ["static", "api", "deployment", "runtime"]
    s2 = B.settings_from(f, B.resolve(f, {"storage.route": "connections", "validation.depth": "run", "security.sensitive": "blocked"}))
    assert s2.lakehouse is None and not s2.allow_real_runs  # blocked data overrides the depth
    s3 = B.settings_from(f, B.resolve(f, {"validation.depth": "static"}))
    assert s3.validate_stages == ["static"] and not s3.allow_real_runs


def test_prerequisites_name_what_an_administrator_must_do_first():
    f = facts(self_hosted=1, activities={"ExecutePipeline": 1})
    s = B.settings_from(f, B.resolve(f, {"connections.gateway": "gateway", "security.network": "unknown", "target.capacity": "provision",
                                         "pipelines.invoke": "create"}))
    text = " ".join(s.prerequisites)
    assert "gateway" in text and "reach the source" in text and "capacity" in text and "Invoke pipeline" in text


def test_conflicting_answers_are_named():
    f = facts()
    assert B.conflicts(f, B.resolve(f, {"validation.depth": "run", "security.sensitive": "blocked"}))
    assert B.conflicts(f, B.resolve(f, {"scope.mode": "selected", "scope.pipelines": []}))
    nowaves = facts(pipeline_waves=[])
    assert any("dependency waves" in c for c in B.conflicts(nowaves, B.resolve(nowaves, {"scope.mode": "pilot"})))
    assert not B.conflicts(f, B.resolve(f, {}))


def test_the_fingerprint_follows_the_answers():
    f = facts()
    a, b = B.fingerprint(B.resolve(f, {})), B.fingerprint(B.resolve(f, {"scope.approach": "parallel"}))
    assert a != b and a == B.fingerprint(B.resolve(f, {}))


def test_the_brief_and_the_questionnaire_say_where_each_answer_came_from():
    f = facts(fabric_workspace="ws-prod")
    rs = B.resolve(f, {"scope.mode": "pilot"})
    md = B.brief_markdown(f, rs, "proj", None, "draft")
    assert "# Migration brief: proj" in md and "Draft, not yet submitted" in md and "_(from the configuration)_" in md and "_(recommendation)_" in md
    assert "Not visible to discovery" in md and "Pilot" in md
    q = B.questionnaire(f, rs)
    assert q["format"] == "adf2fabric-consultation/1" and "target.workspace" not in {x["id"] for x in q["questions"]}  # config is not asked
    assert next(x for x in q["questions"] if x["id"] == "scope.mode")["answer"] == "pilot"


# ---- the record: answer, submit, approve, change ------------------------------------------------------------------------


class _Store:
    """A project store over a temp folder, with the facts injected (the record logic is what is tested here)."""

    def __init__(self, root, f):
        self.root, self.f = root, f

    def project_dir(self, pid):
        return self.root / pid


@pytest.fixture()
def rec(tmp_path, monkeypatch):
    f = facts()
    monkeypatch.setattr(B, "read_facts", lambda store, pid, destination=None: store.f)
    return _Store(tmp_path, f)


def _answer_required(store):
    BR.accept_recommendations(store, "p")
    BR.set_answers(store, "p", {"target.workspace": "existing", "target.capacity": "assigned", "storage.lakehouse": "LH",
                                "context.owner": "Ann, lead"})


def test_inapplicable_and_unknown_answers_are_refused(rec):
    for bad in ({"nope": "x"}, {"scope.pipelines": ["PL_A"]}, {"compute.engine": "fabric"}, {"scope.mode": "everything"}):
        with pytest.raises(BR.BriefError):
            BR.set_answers(rec, "p", bad)
    assert BR.load(rec, "p")["answers"] == {}  # nothing half-saved


def test_submit_needs_every_required_answer_and_no_conflict(rec):
    with pytest.raises(BR.BriefError, match="required"):
        BR.submit(rec, "p")
    _answer_required(rec)
    BR.set_answers(rec, "p", {"validation.depth": "run", "security.sensitive": "blocked"})
    with pytest.raises(BR.BriefError, match="conflict"):
        BR.submit(rec, "p")
    BR.set_answers(rec, "p", {"security.sensitive": "none"})
    v = BR.submit(rec, "p", by="ann")
    assert v["version"] == 1 and BR.view(rec, "p")["state"] == "submitted"
    with pytest.raises(BR.BriefError, match="already submitted"):
        BR.submit(rec, "p")


def test_approval_is_for_the_answers_the_approver_read(rec):
    _answer_required(rec)
    BR.submit(rec, "p")
    fp = BR.view(rec, "p")["fingerprint"]
    with pytest.raises(BR.BriefError, match="changed"):
        BR.approve(rec, "p", "0" * 16, "abe")
    with pytest.raises(BR.BriefError, match="Name"):
        BR.approve(rec, "p", fp, "  ")
    BR.approve(rec, "p", fp, "abe")
    v = BR.view(rec, "p")
    assert v["state"] == "approved" and v["settings"]["lakehouse"] == "LH" and v["current_version"]["approved_by"] == "abe"
    assert BR.approved_settings(rec, "p") == v["settings"]


def test_a_changed_answer_takes_the_approval_away_until_approved_again(rec):
    _answer_required(rec)
    BR.submit(rec, "p")
    BR.approve(rec, "p", BR.view(rec, "p")["fingerprint"], "abe")
    BR.set_answers(rec, "p", {"scope.mode": "pilot"})
    v = BR.view(rec, "p")
    assert v["state"] == "draft" and v["changed_since_approval"] and v["settings"] is None and BR.approved_settings(rec, "p") is None
    BR.set_answers(rec, "p", {"scope.mode": "all"})  # back to exactly what was approved
    assert BR.view(rec, "p")["state"] == "approved"
    rec.f.blocked_activities, rec.f.blocked_pipelines = 1, ["PL_C"]  # discovery / conversion changed what applies
    assert BR.view(rec, "p")["state"] == "draft"


def test_importing_a_questionnaire_skips_what_does_not_fit(rec):
    q = B.questionnaire(rec.f, B.resolve(rec.f, {}))
    for item in q["questions"]:
        item["answer"] = {"scope.mode": "selected", "scope.approach": "parallel", "context.owner": "Client owner"}.get(item["id"])
    q["questions"].append({"id": "made.up", "answer": "x"})
    q["questions"].append({"id": "validation.depth", "answer": "everything"})
    assert BR.import_answers(rec, "p", q) == 3
    a = BR.load(rec, "p")["answers"]
    assert a == {"scope.mode": "selected", "scope.approach": "parallel", "context.owner": "Client owner"}
    assert BR.import_answers(rec, "p", {"scope.approach": "phased"}) == 1  # a plain map works too
    with pytest.raises(BR.BriefError):
        BR.import_answers(rec, "p", ["not", "a", "questionnaire"])


def test_the_record_keeps_notes_and_versions_and_holds_no_secrets(rec):
    _answer_required(rec)
    BR.add_note(rec, "p", "Cut over on a Sunday", by="ann")
    BR.submit(rec, "p", by="ann")
    data = json.loads(BR.path(rec, "p").read_text(encoding="utf-8"))
    assert set(data) == {"answers", "notes", "versions", "config"} and any(n["text"] == "Cut over on a Sunday" for n in data["notes"])
    with pytest.raises(BR.BriefError):
        BR.add_note(rec, "p", "   ")
    BR.reset(rec, "p")
    assert BR.load(rec, "p")["answers"] == {} and BR.load(rec, "p")["versions"]  # history kept
