"""The migration consultation: questions grounded in what discovery found, recommendations with reasons, a versioned,
approved brief whose settings drive the run, and nothing guessed."""

from __future__ import annotations


import pytest

from adf2fabric.agent.consultation import brief as B

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
              "connections.vault_url", "connections.parameters", "storage.undecided", "pipelines.unsupported", "pipelines.reconstruct"):
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


WS_A, WS_B = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"


@pytest.mark.parametrize("bad", ["Dana Lee, platform lead", "LH Migration", "a/b", "x" * 257, "lh;drop", "../x", 'q"uote'])
def test_a_lakehouse_name_must_be_a_name_not_a_sentence(bad):
    with pytest.raises(ValueError, match="letters, digits"):
        B.validate_answer(B.QUESTION_BY_ID["storage.lakehouse"], facts(), bad)


def test_a_valid_lakehouse_name_is_kept_and_other_free_text_is_not_restricted():
    f = facts()
    assert B.validate_answer(B.QUESTION_BY_ID["storage.lakehouse"], f, "  LH_Migration-2  ") == "LH_Migration-2"
    assert B.validate_answer(B.QUESTION_BY_ID["context.owner"], f, "Dana Lee, platform lead") == "Dana Lee, platform lead"


def test_a_stored_answer_that_no_longer_passes_its_rules_is_ignored_and_says_why():
    f = facts()
    rs = B.resolve(f, {"storage.route": "lakehouse", "storage.lakehouse": "Dana Lee, platform lead", "scope.mode": "everything"})
    lh, mode = get(rs, "storage.lakehouse"), get(rs, "scope.mode")
    assert lh.source == "open" and lh.value is None and "letters, digits" in lh.problem
    assert mode.source == "recommendation" and "takes one of" in mode.problem  # falls back to the recommendation, not to the bad value
    assert B.settings_from(f, rs).lakehouse is None  # and the run never sees it


def test_holding_back_every_pipeline_is_a_conflict_not_an_empty_list_that_means_all():
    f = facts(blocked_activities=3, blocked_pipelines=["PL_A", "PL_B", "PL_C"])
    rs = B.resolve(f, {"pipelines.unsupported": "hold"})
    assert B.settings_from(f, rs).pipelines == []  # kept as 'none', never silently 'all'
    assert any("nothing to convert" in c for c in B.conflicts(f, rs))


def test_a_scope_that_leaves_out_a_pipeline_the_chosen_one_calls_is_a_conflict():
    f = facts(calls={"PL_C": ["PL_A"]})
    rs = B.resolve(f, {"scope.mode": "selected", "scope.pipelines": ["PL_C"]})
    msgs = B.conflicts(f, rs)
    assert any("PL_C invokes PL_A" in m for m in msgs)
    assert not B.conflicts(f, B.resolve(f, {"scope.mode": "selected", "scope.pipelines": ["PL_C", "PL_A"]}))
    assert not B.conflicts(f, B.resolve(f, {"scope.mode": "pilot"}))  # the first wave holds the children first


# ---- the main points, tenancy and Key Vault ---------------------------------------------------------------------------------


def test_the_default_consultation_asks_the_main_points_only():
    assert B.ESSENTIAL == {"scope.mode", "scope.pipelines", "connections.tenancy", "connections.credentials", "connections.vault_url",
                           "storage.route", "storage.lakehouse", "compute.engine", "compute.paths", "pipelines.unsupported", "validation.depth"}
    assert B.ESSENTIAL <= {q.id for q in B.QUESTIONS}
    # these have a safe default and are asked only in the full consultation
    assert not B.ESSENTIAL & {"pipelines.reconstruct", "pipelines.triggers", "scope.approach", "context.owner", "target.git"}


def test_tenancy_is_asked_when_there_are_connections_and_defaults_to_one_tenant():
    assert "connections.tenancy" not in ids(facts(linked_services=0))
    r = get(B.resolve(facts(), {}), "connections.tenancy")
    assert r.applies and r.value == "same" and {o.value for o in r.q.options(facts())} == {"same", "different"}


def test_secrets_can_come_from_key_vault_and_the_vault_is_named_once():
    f = facts()
    assert "keyvault" in {o.value for o in get(B.resolve(f, {}), "connections.credentials").q.options(f)}
    assert "connections.vault_url" not in ids(f)  # asked only when the person chose Key Vault
    assert "connections.vault_url" in ids(f, {"connections.credentials": "keyvault"})
    # ADF already reads secrets from a vault: Key Vault is the recommendation, and ADF's own vault is the answer
    g = facts(key_vault=2, vault_url_hint="https://kv-prod.vault.azure.net")
    rs = B.resolve(g, {})
    assert get(rs, "connections.credentials").value == "keyvault" and get(rs, "connections.vault_url").value == "https://kv-prod.vault.azure.net"
    # no vault in ADF: typing each secret once is the recommendation
    assert get(B.resolve(f, {}), "connections.credentials").value == "person"


def test_a_typed_vault_address_must_be_an_azure_key_vault():
    q = next(q for q in B.QUESTIONS if q.id == "connections.vault_url")
    assert B.validate_answer(q, facts(), " https://kv1.vault.azure.net ") == "https://kv1.vault.azure.net"
    with pytest.raises(ValueError):
        B.validate_answer(q, facts(), "https://evil.example.com")  # a token must never be sent to another server


def test_the_settings_carry_where_secrets_come_from_and_the_tenancy():
    f = facts()
    given = {"connections.credentials": "keyvault", "connections.vault_url": "https://kv1.vault.azure.net", "connections.tenancy": "different"}
    s = B.settings_from(f, B.resolve(f, given))
    assert (s.secrets_from, s.key_vault_url, s.cross_tenant) == ("keyvault", "https://kv1.vault.azure.net", True)
    assert any("Key Vault" in p and "kv1.vault.azure.net" in p for p in s.prerequisites) and any("other tenant" in p for p in s.prerequisites)
    plain = B.settings_from(f, B.resolve(f, {}))
    assert (plain.secrets_from, plain.key_vault_url, plain.cross_tenant) == ("person", None, False)


