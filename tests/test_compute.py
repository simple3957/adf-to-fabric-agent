"""Compute assessment: ADF resources -> Fabric CU, runs per day, SKU, run history. Offline and deterministic."""

from __future__ import annotations

import pytest

from adf2fabric.agent.assessment import adf_history, compute_rates as R
from adf2fabric.agent.assessment.compute import (
    ActivityHistory,
    AdfMeter,
    FabricMeter,
    PipelineHistory,
    Source,
    estimate,
    render_markdown,
)
from tests.conversion_support import act, dep, pipe, ref, semantic, trigger

ORCH = R.ORCHESTRATION_CU_HOURS_PER_RUN * 3600  # 20.16 CU-seconds per non-Copy activity run


def copy(name="Copy1", diu=None, parallel=None, deps=()):
    tp = {"source": {"type": "DelimitedTextSource"}, "sink": {"type": "ParquetSink"}}
    if diu is not None:
        tp["dataIntegrationUnits"] = diu
    if parallel is not None:
        tp["parallelCopies"] = parallel
    a = act(name, "Copy", tp, deps=list(deps))
    a["inputs"], a["outputs"] = [ref("Dataset", "In")], [ref("Dataset", "Out")]
    return a


def by(a, pipeline):
    return next(p for p in a.pipelines if p.pipeline == pipeline)


def test_copy_with_fixed_diu_becomes_the_same_throughput():
    a = estimate(semantic([pipe(copy(diu=8, parallel=2), name="P")]))
    x = by(a, "P").activities[0]
    assert x.adf_meter is AdfMeter.data_movement and x.fabric_meters == [FabricMeter.data_movement]
    assert x.adf_resources == {"dataIntegrationUnits": 8, "parallelCopies": 2}
    assert "8 (custom)" in x.fabric_setting
    assert x.duration_source is Source.assumed and x.duration_s == 60
    assert x.cu_seconds_per_pipeline_run == pytest.approx(1.5 * 8 * 60)


def test_auto_and_too_small_diu_are_estimated_at_the_fabric_minimum_and_flagged():
    a = estimate(semantic([pipe(copy("A"), copy("B", diu=2, deps=[dep("A")]), name="P")]))
    auto, small = by(a, "P").activities
    assert auto.adf_resources["dataIntegrationUnits"] == "Auto" and "(Auto)" in auto.fabric_setting
    assert any("Auto" in n for n in auto.notes)
    assert small.cu_seconds_per_pipeline_run == pytest.approx(1.5 * R.ITO_MINIMUM * 60)
    assert any("below Fabric's documented minimum" in n for n in small.notes)


def test_non_copy_activities_cost_one_orchestration_run_each():
    p = pipe(act("Set", "SetVariable", {"variableName": "v", "value": "x"}),
             act("Wait", "Wait", {"waitTimeInSeconds": 5}, deps=[dep("Set")]),
             act("Look", "Lookup", {"source": {"type": "DelimitedTextSource"}, "dataset": ref("Dataset", "In")},
                 deps=[dep("Wait")]), name="P", variables={"v": "String"})
    pc = by(estimate(semantic([p])), "P")
    assert [x.adf_meter for x in pc.activities] == [AdfMeter.orchestration, AdfMeter.orchestration, AdfMeter.pipeline_activity]
    assert pc.cu_seconds_per_run == pytest.approx(3 * ORCH) and pc.cu_seconds_by_meter == {"orchestration": pytest.approx(3 * ORCH)}


def test_data_flow_uses_its_cores_as_spark_and_says_it_is_an_upper_estimate():
    df = act("DF", "ExecuteDataFlow", {"dataflow": ref("DataFlow", "Flow"), "compute": {"coreCount": 16, "computeType": "General"}})
    x = by(estimate(semantic([pipe(df, name="P")])), "P").activities[0]
    assert x.adf_resources == {"coreCount": 16, "computeType": "General"}
    assert x.fabric_meters == [FabricMeter.orchestration, FabricMeter.spark]
    assert x.cu_seconds_by_meter["spark"] == pytest.approx(16 / 2 * 300)
    assert any("not billed yet" in n for n in x.notes)


def test_runs_per_day_from_trigger_flow_down_to_invoked_pipelines():
    child = pipe(copy(), name="Child")
    parent = pipe(act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "Child"), "waitOnCompletion": True}), name="Parent")
    lonely = pipe(act("W", "Wait", {"waitTimeInSeconds": 1}), name="Lonely")
    a = estimate(semantic([parent, child, lonely], triggers=[trigger("Daily", "Parent")]), default_runs_per_day=2)
    assert by(a, "Parent").runs_per_day == 1 and "trigger Daily" in by(a, "Parent").runs_per_day_source
    assert by(a, "Child").runs_per_day == 1 and "invoked by Parent" in by(a, "Child").runs_per_day_source
    assert by(a, "Lonely").runs_per_day == 2 and "assumed" in by(a, "Lonely").runs_per_day_source
    assert a.cu_hours_per_day == pytest.approx(sum(p.cu_seconds_per_day for p in a.pipelines) / 3600, rel=1e-3)


def test_sku_is_daily_cu_hours_over_24_with_headroom():
    # 40 copies of 1 hour at ITO 4 per day: 40 * 1.5 * 4 = 240 CU-hours/day -> 10 CU average -> 12.5 with headroom -> F16
    h = {"p": PipelineHistory(pipeline="P", runs_sampled=1,
                              activities={"Copy1": ActivityHistory(activity_type="Copy", median_duration_s=3600, used_diu=4)})}
    a = estimate(semantic([pipe(copy(), name="P")]), h, default_runs_per_day=40)
    assert a.cu_hours_per_day == pytest.approx(240) and a.average_cu_needed == pytest.approx(10)
    assert a.recommended_sku == "F16" and "÷ 24" in a.sku_reason
    tiny = estimate(semantic([pipe(act("W", "Wait", {"waitTimeInSeconds": 1}), name="Q")]))
    assert tiny.recommended_sku == "F2" and any("other Fabric workloads" in n for n in tiny.notes)


def test_history_replaces_assumptions_with_measured_values():
    h = {"p": PipelineHistory(pipeline="P", runs_sampled=3, adf_billed_per_run={"DataMovement DIUHours": 0.0667},
                              activities={"Copy1": ActivityHistory(activity_type="Copy", runs_per_pipeline_run=12,
                                                                   median_duration_s=18.4, used_diu=6)})}
    pc = by(estimate(semantic([pipe(copy(), name="P")]), h), "P")
    x = pc.activities[0]
    assert (x.runs_source, x.duration_source) == (Source.history, Source.history)
    assert x.adf_resources["usedDataIntegrationUnits"] == 6
    assert x.cu_seconds_per_pipeline_run == pytest.approx(1.5 * 6 * 60 * 12)  # billed per started minute
    assert any("per started minute" in n for n in x.notes)
    assert pc.adf_billed_per_run == {"DataMovement DIUHours": 0.0667} and pc.history_runs_sampled == 3


def test_report_names_sources_and_assumptions():
    md = render_markdown(estimate(semantic([pipe(copy(), name="P")])))
    assert "Recommended capacity" in md and R.PIPELINES_PRICING in md and "assumed" in md


class _Resp:
    def __init__(self, data, status=200):
        self.status_code, self._d = status, data

    def json(self):
        return self._d


class _Transport:
    """Fake ARM: two runs of P; the second runs the Copy twice (a loop)."""

    def __init__(self):
        self.calls = []

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append(url)
        if url.split("?")[0].endswith("/queryPipelineRuns"):
            return _Resp({"value": [{"runId": "r1", "durationInMs": 40000}, {"runId": "r2", "durationInMs": 60000}]})
        copies = 1 if "/r1/" in url else 2
        bill = {"activityType": "DataMovement", "billableDuration": [{"meterType": "AzureIR", "duration": 0.0667, "unit": "DIUHours"}]}
        return _Resp({"value": [{"activityName": "Copy1", "activityType": "Copy", "durationInMs": 20000,
                                 "output": {"usedDataIntegrationUnits": 4, "usedParallelCopies": 1, "billingReference": bill}}
                                for _ in range(copies)]})


def test_history_fetch_aggregates_runs_and_only_uses_query_endpoints():
    t = _Transport()
    h = adf_history.fetch(t, "tok", "sub", "rg", "fac", ["P"])["p"]
    assert h.runs_sampled == 2 and h.median_duration_s == 50
    c = h.activities["Copy1"]
    assert c.runs_per_pipeline_run == 1.5 and c.median_duration_s == 20 and c.used_diu == 4
    assert h.adf_billed_per_run == {"DataMovement DIUHours": pytest.approx(0.0667 * 3 / 2)}
    assert all("/query" in u for u in t.calls)  # read-only: nothing created or run


def test_history_http_error_is_reported():
    class Bad:
        def post(self, *a, **k):
            return _Resp({}, status=403)

    with pytest.raises(adf_history.HistoryError):
        adf_history.fetch(Bad(), "tok", "sub", "rg", "fac", ["P"])


def test_copy_is_billed_per_started_minute_like_the_measured_runs():
    # PL_Compute_Test: a 4-DIU copy of 10-18 s was billed 0.0667 DIU-hours (4 x 1 minute) in ADF and in Fabric.
    h = {"p": PipelineHistory(pipeline="P", runs_sampled=1,
                              activities={"Copy1": ActivityHistory(activity_type="Copy", median_duration_s=17.3, used_diu=4)})}
    x = by(estimate(semantic([pipe(copy(diu=4), name="P")]), h), "P").activities[0]
    assert x.cu_seconds_per_pipeline_run == pytest.approx(1.5 * 4 * 60)  # 0.1 CU-hours = 1.5 x 0.0667 DIU-hours
    long = {"p": PipelineHistory(pipeline="P", runs_sampled=1,
                                 activities={"Copy1": ActivityHistory(activity_type="Copy", median_duration_s=61, used_diu=4)})}
    assert by(estimate(semantic([pipe(copy(diu=4), name="P")]), long), "P").activities[0].cu_seconds_per_pipeline_run == pytest.approx(1.5 * 4 * 120)
