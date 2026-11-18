"""A pipeline over Fabric's activity limit is split into child pipelines (agent/conversion/split.py, 2026-11-17).

The catalog's workaround for ForEach / If Condition / Switch / Until, done by the converter at the user's request. The pipelines below are
built in ADF's JSON shape; the real converter runs. No real estate reaches the limit (the largest real pipeline has 19 activities), so
this is the only evidence until one does."""

from __future__ import annotations

import json

from adf2fabric.agent.conversion import split as SP
from adf2fabric.agent.conversion.engine import ConversionEngine
from adf2fabric.agent.conversion.agent import build_project_context
from adf2fabric.agent.conversion.fabric_schema import ACTIVITY_LIMIT
from adf2fabric.agent.conversion.context import Resolutions
from tests.conversion_support import DS_LAKE, DS_SQL, LAKE_LS, SQL_LS, act, dep, expr, pipe, semantic

ESTATE = dict(datasets=[DS_SQL, DS_LAKE], linked_services=[SQL_LS, LAKE_LS])


def _waits(n, prefix="W", seconds="@item().delay"):
    return [act(f"{prefix}{i}", "Wait", {"waitTimeInSeconds": expr(seconds) if seconds.startswith("@") else 1}) for i in range(n)]


def _convert(p):
    d = semantic([p], **ESTATE)
    engine = ConversionEngine(build_project_context(d, Resolutions(None)))
    record, content = engine.convert_pipeline(d.pipelines[0])
    return engine, record, content


def _big(inner, outside=25):
    """A ForEach with ``inner`` activities after a first activity, and ``outside`` more at the top: over the limit together."""
    lk = act("Lookup_Files", "Wait", {"waitTimeInSeconds": 1})
    fe = act("Each", "ForEach", {"items": expr("@pipeline().parameters.files"), "isSequential": False, "activities": inner})
    fe["dependsOn"] = [dep("Lookup_Files")]
    p = pipe(lk, fe, *_waits(outside, "Top", "1"), name="PL_Big")
    p["properties"]["parameters"] = {"files": {"type": "array"}, "root": {"type": "string", "defaultValue": "in"}}
    return p


def test_a_foreach_over_the_limit_moves_to_a_child_that_gets_what_it_reads_as_parameters():
    inner = _waits(100)
    inner[0]["typeProperties"]["waitTimeInSeconds"] = expr("@length(activity('Lookup_Files').output)")
    inner[1]["typeProperties"]["waitTimeInSeconds"] = expr("@length(pipeline().parameters.root)")
    engine, record, content = _convert(_big(inner))
    assert SP.count(content["properties"]["activities"]) <= ACTIVITY_LIMIT
    [ch] = record.split_children
    assert ch.container == "Each" and ch.branch == "activities" and ch.activities == 100
    call = content["properties"]["activities"][1]["typeProperties"]["activities"]
    assert len(call) == 1 and call[0]["type"] == "InvokePipeline" and call[0]["typeProperties"]["waitOnCompletion"] is True
    assert call[0]["typeProperties"]["pipelineId"] == f"<item:pipeline:{ch.name}>"
    passed = {k: v["value"] for k, v in call[0]["typeProperties"]["parameters"].items()}
    assert passed == {"item": "@item()", "out_Lookup_Files": "@activity('Lookup_Files').output", "root": "@pipeline().parameters.root"}
    child = engine.split_contents["PL_Big"][ch.name]
    text = json.dumps(child)
    assert "item()" not in text and "activity('Lookup_Files')" not in text and "pipeline().parameters.item.delay" in text
    assert child["properties"]["parameters"]["root"] == {"type": "string", "defaultValue": "in"}
    assert child["properties"]["annotations"] == [f"{SP.ANNOTATION}PL_Big"]
    assert record.structural_valid and not any(i.code == "activity_limit_exceeded" for i in record.validation.issues)
    assert any(q.name == f"pipeline:{ch.name}" for q in record.prerequisites)
    # validation compares the source with the pipeline as authored: the child put back, its parameters replaced by what was passed
    merged = SP.merge(content, {ch.name.lower(): child})
    back = merged["properties"]["activities"][1]["typeProperties"]["activities"]
    assert len(back) == 100 and back[0]["typeProperties"]["waitTimeInSeconds"]["value"] == "@length(activity('Lookup_Files').output)"
    assert back[5]["typeProperties"]["waitTimeInSeconds"]["value"] == "@item().delay"


def test_a_block_that_sets_a_pipeline_variable_is_never_moved_and_the_limit_error_stays():
    inner = _waits(99) + [act("Remember", "SetVariable", {"variableName": "last", "value": expr("@item().name")})]
    p = _big(inner)
    p["properties"]["variables"] = {"last": {"type": "String"}}
    _, record, content = _convert(p)
    assert record.split_children == [] and not record.structural_valid
    assert any(i.code == "activity_limit_exceeded" for i in record.validation.issues)
    assert any("sets a variable of the pipeline" in n for n in record.notes)


def test_a_block_that_alone_is_over_the_limit_stays_with_the_error():
    _, record, _ = _convert(_big(_waits(125), outside=0))
    assert record.split_children == [] and any("the block alone is over" in n for n in record.notes)


def test_a_pipeline_under_the_limit_is_left_alone():
    _, record, content = _convert(_big(_waits(10)))
    assert record.split_children == [] and len(content["properties"]["activities"][1]["typeProperties"]["activities"]) == 10


def test_validation_reads_a_split_pipeline_as_one_and_keeps_the_file_for_the_deployment_check(tmp_path):
    from adf2fabric.agent.validation import views

    engine, record, content = _convert(_big(_waits(100)))
    [ch] = record.split_children
    for name, body in (("PL_Big", content), (ch.name, engine.split_contents["PL_Big"][ch.name])):
        d = tmp_path / "pipelines" / f"{name}.DataPipeline"
        d.mkdir(parents=True)
        (d / "pipeline-content.json").write_text(json.dumps(body), encoding="utf-8")
    scan = views.scan_target(tmp_path)
    parent = next(tp for tp in scan.pipelines if tp.name == "PL_Big")
    assert parent.raw == content and parent.split_children == [ch.name.lower()]
    inner = parent.content["properties"]["activities"][1]["typeProperties"]["activities"]
    assert len(inner) == 100 and inner[0]["typeProperties"]["waitTimeInSeconds"]["value"] == "@item().delay"
