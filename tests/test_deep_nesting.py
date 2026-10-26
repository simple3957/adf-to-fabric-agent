"""Activities nested deeper than discovery reads, and absurdly nested expressions: never silently dropped, never a crash."""

from __future__ import annotations

import pytest

from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from tests.conversion_support import act, by_name, convert_one, expr, pipe
from tests.test_adf_semantic_discovery import _make_project


def _nest(n, kind="IfCondition"):
    inner = act("Leaf", "Wait", {"waitTimeInSeconds": 1})
    for i in range(n):
        inner = (act(f"If{i}", "IfCondition", {"expression": expr("@equals(1,1)"), "ifTrueActivities": [inner]}) if kind == "IfCondition"
                 else act(f"FE{i}", "ForEach", {"items": expr("@createArray(1)"), "activities": [inner]}))
    return pipe(inner, name="PL_Deep")


def _end_to_end(tmp_path, pipeline):
    store, pid = _make_project(tmp_path, {"pipeline/PL_Deep.json": pipeline})
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    return ConversionAgent(store).run(ConversionRequest(project_name="test-proj")).conversion


@pytest.mark.parametrize("kind", ["IfCondition", "ForEach"])
def test_nesting_within_the_limit_converts_completely(tmp_path, kind):
    conv = _end_to_end(tmp_path, _nest(25, kind))
    p = conv.pipelines[0]
    assert p.status.value in ("converted", "converted_with_differences") and not [f for f in conv.preflight if f.code == "source_activities_unaccounted"]
    assert len(p.activities) == 26  # every level and the leaf


@pytest.mark.parametrize("kind", ["IfCondition", "ForEach"])
def test_nesting_beyond_the_limit_is_blocked_and_reported_never_converted_silently(tmp_path, kind):
    conv = _end_to_end(tmp_path, _nest(60, kind))
    p = conv.pipelines[0]
    assert p.status.value == "blocked" and not p.runtime_ready
    assert any(w.code == "nesting_too_deep" for a in p.activities for w in a.warnings)
    f = next(f for f in conv.preflight if f.code == "source_activities_unaccounted")
    assert f.severity == "error" and "61" in f.message


def test_an_absurdly_nested_expression_is_flagged_not_a_crash():
    e = "'x'"
    for _ in range(1500):
        e = f"concat('a',{e})"
    rec, _ = convert_one(pipe(act("W", "Wait", {"waitTimeInSeconds": expr("@" + e)})))
    a = by_name(rec)["W"]
    assert "expression_unparsed" in {w.code for w in a.warnings} and a.status.value == "manual_required"
