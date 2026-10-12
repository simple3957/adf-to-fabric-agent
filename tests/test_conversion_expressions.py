"""Expression conversion, registry completeness and architecture guards (offline, deterministic)."""

from __future__ import annotations

import inspect
import pathlib

import pytest

import adf2fabric.agent.conversion as conversion_pkg
from adf2fabric.agent.conversion import handlers  # noqa: F401
from adf2fabric.agent.conversion.expr_convert import ExprEnv, convert_expression, convert_tree, expr_obj
from adf2fabric.agent.conversion.fabric_schema import FABRIC_ACTIVITIES
from adf2fabric.agent.conversion.registry import REGISTRY, registered_rules, rule_for
from adf2fabric.agent.discovery.adf_semantic.activity_registry import ACTIVITY_REGISTRY
from adf2fabric.project.conversion import Impact
from tests.test_parser_pipelines import expr


def conv(raw, **env):
    return convert_expression(raw, ExprEnv(**env))


# --- pass-through: the common case must not be touched ----------------------------------------------------------


@pytest.mark.parametrize("raw", [
    "@pipeline().parameters.env",
    "@concat('a',   variables('v'),  formatDateTime(utcnow(),'yyyy'))",  # odd spacing must survive
    "SELECT * FROM t WHERE d > '@{activity('L').output.firstRow.w}'",
    "@activity('Lookup1').output.value",
    "@item().name",
    "@pipeline().RunId",
    "@pipeline().TriggerTime",
    "@if(equals(length(item().rows), 0), 'a', 'b')",
])
def test_ordinary_expressions_are_unchanged_verbatim(raw):
    r = conv(raw)
    assert r.status == "unchanged" and r.value == expr_obj(raw) and r.issues == []


def test_plain_text_and_escaped_text_are_left_alone():
    assert conv("plain").status == "unchanged"
    assert conv("@@literal").value == expr_obj("@@literal")


# --- named rewrites ------------------------------------------------------------------------------------------------


def test_pipeline_name_variable_is_rewritten_with_the_reason():
    r = conv("@concat('run ', pipeline().Pipeline)")
    assert r.status == "rewritten" and r.value["value"] == "@concat('run ', pipeline().PipelineName)"
    assert "name" in r.changes[0] and "ID" in r.changes[0]


def test_data_factory_variable_is_kept_but_flagged_as_different():
    r = conv("@pipeline().DataFactory")
    assert r.status == "unchanged" and r.value["value"] == "@pipeline().DataFactory"
    assert [i.impact for i in r.issues] == [Impact.difference]
    assert "WORKSPACE" in r.issues[0].message


def test_trigger_type_is_unsupported_and_not_replaced():
    r = conv("@pipeline().TriggerType")
    assert r.status == "unsupported" and r.value["value"] == "@pipeline().TriggerType"
    assert r.issues[0].impact is Impact.manual


def test_global_parameter_becomes_a_pipeline_parameter_and_is_recorded():
    used: set[str] = set()
    r = conv("@concat(pipeline().globalParameters.Env, '-x')", used_globals=used)
    assert r.value["value"] == "@concat(pipeline().parameters.gp_Env, '-x')"
    assert used == {"Env"} and any(i.code == "global_parameter_converted" for i in r.issues)


def test_global_parameter_name_never_collides_with_a_real_parameter():
    r = conv("@pipeline().globalParameters.Env", existing_params={"gp_env"})
    assert "gp_Env_" in r.value["value"]


def test_activity_rename_applies_only_where_requested():
    r = conv("@activity('Old').output.x", activity_rename={"old": "New"})
    assert r.value["value"] == "@activity('New').output.x" and r.status == "rewritten"
    assert conv("@activity('Other').output.x", activity_rename={"old": "New"}).status == "unchanged"


# --- dataset() inlining ----------------------------------------------------------------------------------------------


def test_dataset_reference_is_replaced_by_a_literal_argument():
    r = conv("@dataset().folder", dataset_name="DS", dataset_args={"folder": "customer"})
    assert r.value == "customer" and r.status == "rewritten"  # a plain literal, not an Expression


def test_dataset_reference_inside_a_function_gets_the_argument_expression():
    r = conv("@concat(dataset().folder, '/x')", dataset_args={"folder": expr("@variables('f')")})
    assert r.value["value"] == "@concat(variables('f'), '/x')"


def test_dataset_reference_with_an_interpolated_argument_becomes_concat():
    r = conv("@dataset().p", dataset_args={"p": expr("raw/@{variables('d')}/x")})
    assert r.value["value"] == "@concat('raw/', variables('d'), '/x')"


def test_dataset_interpolation_with_literal_argument_collapses_to_a_plain_string():
    r = conv("data/@{dataset().folder}/file", dataset_args={"folder": "a"})
    assert r.value == "data/a/file"


def test_dataset_default_is_used_when_no_argument_is_passed():
    r = conv("@dataset().folder", dataset_args={}, dataset_defaults={"folder": "dflt"})
    assert r.value == "dflt"


def test_unresolved_dataset_parameter_is_reported_not_guessed():
    r = conv("@dataset().folder", dataset_name="DS", dataset_args={})
    assert r.status == "unsupported" and r.value["value"] == "@dataset().folder"
    assert r.issues[0].code == "dataset_parameter_unresolved" and "DS" in r.issues[0].message


def test_dataset_reference_outside_a_dataset_is_unsupported():
    assert conv("@dataset().x").status == "unsupported"


def test_array_argument_is_embedded_as_json_not_dropped():
    r = conv("@dataset().cols", dataset_args={"cols": ["a", "b"]})
    assert r.value["value"] == "@json('[\"a\", \"b\"]')"


# --- unsupported constructs: kept verbatim, never substituted ---------------------------------------------------------


@pytest.mark.parametrize("raw,code", [
    ("@decimal('1.5')", "unsupported_function"),
    ("@formatNumber(1, 'N2')", "unsupported_function"),
    ("@triggerBody().fileName", "unsupported_triggerbody"),
    ("@trigger().outputs.windowStartTime", "unsupported_trigger"),
    ("@linkedService().db", "unsupported_linkedservice"),
    ("@pipeline().nonsense", "unsupported_system_variable"),
])
def test_unsupported_constructs_are_flagged_and_kept(raw, code):
    r = conv(raw)
    assert r.status == "unsupported" and r.value["value"] == raw
    assert code in {i.code for i in r.issues} and all(i.impact is Impact.manual for i in r.issues)


def test_unparsable_expression_is_kept_verbatim_and_flagged():
    r = conv("@concat('a'")
    assert r.status == "unsupported" and r.value["value"] == "@concat('a'"
    assert r.issues[0].code == "expression_unparsed"


def test_no_similar_function_substitution_ever_happens():
    r = conv("@decimal('1.5')")
    assert "float" not in r.value["value"] and "int" not in r.value["value"]


# --- trees ----------------------------------------------------------------------------------------------------------------


def test_convert_tree_records_every_expression_with_its_paths():
    sink, issues = [], []
    tree = {"a": expr("@pipeline().Pipeline"), "b": [expr("@variables('v')"), "plain"], "c": {"d": 3}}
    out = convert_tree(tree, ExprEnv(), "src", "dst", sink, issues)
    assert out["a"]["value"] == "@pipeline().PipelineName" and out["b"][1] == "plain" and out["c"] == {"d": 3}
    assert [(e.source_path, e.status) for e in sink] == [("src.a", "rewritten"), ("src.b[0]", "unchanged")]


# --- registry completeness -----------------------------------------------------------------------------------------------------


def test_every_discoverable_adf_activity_type_has_a_conversion_rule():
    missing = [s.type for s in ACTIVITY_REGISTRY.values() if rule_for(s.type) is None]
    assert missing == []


def test_every_rule_documents_itself():
    for r in registered_rules():
        assert r.adf_types and r.variants and callable(r.handler), r.name
        assert r.property_rules, f"{r.name}: property rules"
        assert r.expression_rules and r.dependency_rules and r.parameter_variable_rules, r.name
        assert r.validation_rules, f"{r.name}: validation rules"
        assert r.io_rules, f"{r.name}: input/output rules"
        assert r.runtime_disposition in ("replace", "reuse_externally", "redesign"), r.name
        assert isinstance(r.unsupported, tuple) and isinstance(r.manual_configuration, tuple), r.name


def test_rules_only_target_fabric_activity_types_the_schema_knows():
    for r in registered_rules():
        for t in r.fabric_types:
            assert t in FABRIC_ACTIVITIES, f"{r.name} targets undocumented Fabric type {t}"


def test_a_type_cannot_be_registered_twice():
    from adf2fabric.agent.conversion.registry import register

    with pytest.raises(ValueError):
        register(next(iter(REGISTRY.values())))


# --- architecture guards ---------------------------------------------------------------------------------------------------------------


def _modules():
    root = pathlib.Path(inspect.getfile(conversion_pkg)).parent
    return [p for p in root.rglob("*.py")]


def test_conversion_does_not_depend_on_the_parsing_stage():
    """Parsing and conversion stay separate: conversion reads the persisted IR only."""
    forbidden = ("adf_semantic.parser", "adf_semantic import parser", "parser_pipelines", "activity_semantics",
                 "discovery.repository", "connectivity")
    for p in _modules():
        text = p.read_text(encoding="utf-8")
        for f in forbidden:
            assert f not in text, f"{p.name} imports from the parsing/IO stage ({f})"


def test_discovery_does_not_import_conversion():
    root = pathlib.Path(inspect.getfile(conversion_pkg)).parents[1] / "discovery"
    assert not any("agent.conversion" in p.read_text(encoding="utf-8") for p in root.rglob("*.py"))


def test_project_layer_does_not_import_the_agent_layer():
    import adf2fabric.project.conversion as c

    assert "adf2fabric.agent" not in inspect.getsource(c)


def test_conversion_uses_no_model_or_network_library():
    banned = ("openai", "anthropic", "langchain", "requests", "urllib.request", "httpx", "socket", "subprocess")
    for p in _modules():
        text = p.read_text(encoding="utf-8")
        for b in banned:
            assert f"import {b}" not in text and f"from {b}" not in text, f"{p.name} uses {b}"


def test_conversion_never_writes_outside_the_conversion_folder():
    from adf2fabric.project.store import ProjectStore

    store = ProjectStore(pathlib.Path("x"))
    with pytest.raises(ValueError):
        store.write_conversion_file("p", "../source/evil.json", "{}")
