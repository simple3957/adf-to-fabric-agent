"""Confidence grading, evidence rules, the patch engine and JSON-failure classification (pure, offline)."""

from __future__ import annotations

import copy

import pytest

from adf2fabric.agent.reconstruction import patching
from adf2fabric.agent.reconstruction.detect import classify_json_failure
from adf2fabric.agent.reconstruction.evidence import PRIORITY, ev, grade, safe_to_generate
from adf2fabric.project.reconstruction import Confidence as C
from adf2fabric.project.reconstruction import EvidenceKind as K
from adf2fabric.project.reconstruction import PatchOp

E = lambda kind, n=1: [ev(kind, f"s{i}", "d") for i in range(n)]  # noqa: E731


# --- grading: explicit rules, no scores -----------------------------------------------------------------------


def test_priorities_follow_the_documented_order():
    order = [K.explicit_property, K.same_pipeline_reference, K.parameter, K.referenced_artifact, K.parent_child,
             K.related_pipeline, K.repeated_pattern, K.naming_convention, K.project_artifact]
    assert [PRIORITY[k] for k in order] == list(range(1, 10))


@pytest.mark.parametrize("evidence,kw,expected", [
    # HIGH: single candidate, nothing missing, explicit evidence or a uniquely determined mapping
    (E(K.explicit_property), dict(candidates=1), C.high),
    (E(K.same_pipeline_reference), dict(candidates=1), C.high),
    (E(K.referenced_artifact), dict(candidates=1, determined=True), C.high),
    # determined mappings stop being HIGH when the source is further away than a referenced artifact
    (E(K.parent_child), dict(candidates=1, determined=True), C.medium),
    (E(K.referenced_artifact), dict(candidates=1), C.medium),  # not determined -> a supported pattern, not a certainty
    # a material assumption caps even explicit evidence at MEDIUM
    (E(K.explicit_property), dict(candidates=1, material_assumptions=True), C.medium),
    # MEDIUM: supported pattern
    (E(K.parameter), dict(candidates=1), C.medium),
    (E(K.related_pipeline, 2), dict(candidates=1), C.medium),
    (E(K.repeated_pattern, 2), dict(candidates=1), C.medium),
    (E(K.naming_convention) + E(K.referenced_artifact), dict(candidates=1), C.medium),  # corroborated naming
    # LOW: weak or ambiguous
    (E(K.repeated_pattern, 1), dict(candidates=1), C.low),  # a single peer
    (E(K.naming_convention, 1), dict(candidates=1), C.low),  # a naming hint alone
    (E(K.naming_convention, 3), dict(candidates=1), C.low),  # three hints of ONE kind are still one kind
    (E(K.explicit_property), dict(candidates=2), C.low),  # several plausible implementations
    (E(K.explicit_property), dict(candidates=0), C.low),
    ([], dict(candidates=1), C.low),
    (E(K.explicit_property), dict(candidates=1, information_required=["the table name"]), C.low),
])
def test_grade_table(evidence, kw, expected):
    conf, basis = grade(evidence, **kw)
    assert conf is expected and basis.startswith(expected.value.upper())


def test_information_required_always_wins():
    assert grade(E(K.explicit_property), candidates=1, information_required=["x"], determined=True)[0] is C.low


def test_grade_is_deterministic_and_explains_itself():
    a = grade(E(K.same_pipeline_reference, 2), candidates=1)
    assert a == grade(E(K.same_pipeline_reference, 2), candidates=1) and "priority 2" in a[1]


@pytest.mark.parametrize("conf,info,ok", [(C.high, [], True), (C.medium, [], True), (C.low, [], False),
                                          (C.high, ["x"], False), (C.medium, None, True)])
def test_only_high_and_medium_without_open_questions_may_be_generated(conf, info, ok):
    assert safe_to_generate(conf, info) is ok


# --- patching -----------------------------------------------------------------------------------------------------


DOC = {"properties": {"activities": [{"name": "A", "typeProperties": {"x": 1}}, {"name": "B"}]}}


def test_ir_pointers_round_trip_to_json_pointers():
    toks = patching.ir_pointer_tokens("properties.activities[2].typeProperties.cases[1].activities[0]")
    assert toks == ["properties", "activities", 2, "typeProperties", "cases", 1, "activities", 0]
    assert patching.to_pointer(toks) == "/properties/activities/2/typeProperties/cases/1/activities/0"
    assert patching.from_pointer(patching.to_pointer(toks)) == toks
    assert patching.from_pointer("/a~1b/c~0d") == ["a/b", "c~d"]


def test_set_ops_creates_only_what_is_missing():
    t = ["properties", "activities", 0, "typeProperties", "x"]
    assert [(o.op, o.path) for o in patching.set_ops(DOC, t, 2)] == [("replace", "/properties/activities/0/typeProperties/x")]
    t2 = ["properties", "activities", 1, "typeProperties", "a", "b"]
    ops = patching.set_ops(DOC, t2, 9)
    assert len(ops) == 1 and ops[0].path == "/properties/activities/1/typeProperties" and ops[0].value == {"a": {"b": 9}}
    assert patching.apply_ops(DOC, ops)["properties"]["activities"][1]["typeProperties"]["a"]["b"] == 9


def test_append_ops_create_or_extend_a_list():
    lst = ["properties", "activities"]
    assert patching.append_ops(DOC, lst, {"n": 1})[0].path == "/properties/activities/-"
    created = patching.append_ops(DOC, ["properties", "activities", 1, "dependsOn"], {"d": 1})
    assert created[0].value == [{"d": 1}]
    patched = patching.apply_ops(DOC, patching.append_ops(DOC, lst, {"name": "C"}))
    assert [a["name"] for a in patched["properties"]["activities"]] == ["A", "B", "C"]


def test_patching_never_mutates_the_original():
    before = copy.deepcopy(DOC)
    patching.apply_ops(DOC, [PatchOp(op="replace", path="/properties/activities/0/name", value="Z")])
    assert DOC == before


@pytest.mark.parametrize("op", [
    PatchOp(op="replace", path="/properties/nope", value=1),
    PatchOp(op="add", path="/properties/activities/9/x", value=1),
    PatchOp(op="add", path="/properties/activities/0/name/x", value=1),
    PatchOp(op="add", path="", value=1),
])
def test_bad_patches_fail_loudly(op):
    with pytest.raises(patching.PatchError):
        patching.apply_ops(DOC, [op])


def test_list_insert_and_replace():
    out = patching.apply_ops(DOC, [PatchOp(op="add", path="/properties/activities/0", value={"name": "First"}),
                                   PatchOp(op="replace", path="/properties/activities/2/name", value="B2")])
    assert [a["name"] for a in out["properties"]["activities"]] == ["First", "A", "B2"]


# --- distinguishing kinds of unreadable JSON -------------------------------------------------------------------------


@pytest.mark.parametrize("text,code", [
    ("", "empty_json_file"),
    ("   \n ", "empty_json_file"),
    ('{"name": "P", "properties": {"activities": [', "truncated_json"),
    ('{"name": "P", "properties": {"activities": [{"name": "A"', "truncated_json"),
    ('{"name": "unterminated str', "truncated_json"),
    ('{"name": "P",}', "invalid_json"),  # balanced, but malformed
    ('{"a": 1}}', "invalid_json"),  # an extra closer is not truncation
    ('{"a" 1}', "invalid_json"),
    ("not json at all", "invalid_json"),
])
def test_json_failures_are_classified(text, code):
    got, why = classify_json_failure(text)
    assert got == code and why


def test_brackets_inside_strings_do_not_fool_truncation_detection():
    assert classify_json_failure('{"q": "select [x] from {t}", "n":}')[0] == "invalid_json"
    assert classify_json_failure('{"q": "a ] } b", "n": [1, ')[0] == "truncated_json"
