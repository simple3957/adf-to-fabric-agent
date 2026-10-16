"""Compute assessment: what each ADF pipeline uses today and how many Fabric capacity units (CU) it will need.

Deterministic, no LLM. Input is the ADF semantic discovery record (activities with their configured resources and the
triggers), plus optional ADF run history (``adf_history``) that replaces assumed durations and iteration counts with
measured ones. Every number in the result says where it came from: ``history`` (measured in ADF) or ``assumed``
(a stated default), and every Fabric rate comes from ``compute_rates`` (Microsoft Learn, with the date it was read).

Sizing follows Fabric's own rule: pipelines are background operations, smoothed over 24 hours, so the capacity needed is
the daily CU-hours divided by 24 (an F<n> provides n CUs).
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Optional

from pydantic import BaseModel, ConfigDict, Field

from adf2fabric.agent.assessment import compute_rates as R
from adf2fabric.project.discovery import AdfActivity, AdfPipeline, AdfSemanticDiscovery, AdfTrigger


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AdfMeter(str, Enum):
    """How ADF bills the activity today."""

    data_movement = "data_movement"  # Copy: DIU-hours
    pipeline_activity = "pipeline_activity"  # Lookup, Get Metadata, Script ...: hours on the integration runtime
    external = "external"  # Web, Azure Function, Databricks ...: external activity hours
    data_flow = "data_flow"  # Mapping data flow: vCore-hours
    ssis = "ssis"  # Azure-SSIS integration runtime hours
    orchestration = "orchestration"  # control activities: activity runs only


class FabricMeter(str, Enum):
    data_movement = "data_movement"
    orchestration = "orchestration"
    spark = "spark"
    ssis = "ssis"


class Source(str, Enum):
    history = "history"  # measured from ADF runs
    assumed = "assumed"  # a stated default


# ADF activity type -> how ADF bills it. Anything not listed is treated as orchestration and flagged.
_ADF_METER: dict[str, AdfMeter] = {
    "Copy": AdfMeter.data_movement,
    **{t: AdfMeter.pipeline_activity for t in ("Lookup", "GetMetadata", "Delete", "Script", "SqlServerStoredProcedure",
                                                "Validation")},
    **{t: AdfMeter.external for t in ("WebActivity", "WebHook", "AzureFunctionActivity", "DatabricksNotebook",
                                       "DatabricksSparkJar", "DatabricksSparkPython", "HDInsightHive", "HDInsightPig",
                                       "HDInsightSpark", "HDInsightStreaming", "HDInsightMapReduce", "Custom",
                                       "AzureMLExecutePipeline", "AzureMLBatchExecution", "SynapseNotebook", "SparkJob")},
    "ExecuteDataFlow": AdfMeter.data_flow,
    "ExecuteWranglingDataflow": AdfMeter.data_flow,
    "ExecuteSSISPackage": AdfMeter.ssis,
    **{t: AdfMeter.orchestration for t in ("SetVariable", "AppendVariable", "Wait", "IfCondition", "ForEach", "Until",
                                            "Switch", "Filter", "ExecutePipeline", "Fail")},
}
_SPARK_TYPES = {"SynapseNotebook", "SparkJob", "DatabricksNotebook", "DatabricksSparkJar", "DatabricksSparkPython"}

# Stated defaults, used only when there is no run history for the activity.
DEFAULT_DURATION_S: dict[AdfMeter, float] = {
    AdfMeter.data_movement: 60.0,
    AdfMeter.pipeline_activity: 15.0,
    AdfMeter.external: 30.0,
    AdfMeter.data_flow: 300.0,
    AdfMeter.ssis: 600.0,
    AdfMeter.orchestration: 1.0,
}
DEFAULT_DATA_FLOW_CORES = 8  # ADF's default Azure IR data flow compute (General, 8 cores)
DEFAULT_COPY_DIU = 4  # "Auto" DIUs: the smallest documented Fabric throughput value, stated as an assumption
HEADROOM = 0.25  # recommended SKU keeps 25% free for growth and other Fabric workloads


# ---- run history (optional) -------------------------------------------------------------------------------------------
class ActivityHistory(_M):
    activity_type: Optional[str] = None
    runs_per_pipeline_run: float = 1.0  # e.g. a Copy inside a ForEach of 12 items -> 12
    median_duration_s: float = 0.0
    used_diu: Optional[float] = None
    used_parallel_copies: Optional[float] = None


class PipelineHistory(_M):
    pipeline: str
    runs_sampled: int = 0
    median_duration_s: float = 0.0
    activities: dict[str, ActivityHistory] = Field(default_factory=dict)  # by activity name
    adf_billed_per_run: dict[str, float] = Field(default_factory=dict)  # e.g. {"DataMovement DIUHours": 0.0667}


# ---- result -------------------------------------------------------------------------------------------------------------
class ActivityCompute(_M):
    pipeline: str
    activity: str
    activity_type: Optional[str] = None
    nesting: list[str] = Field(default_factory=list)
    adf_meter: AdfMeter
    adf_resources: dict[str, Any] = Field(default_factory=dict)  # as configured: diu, parallel_copies, core_count ...
    fabric_meters: list[FabricMeter] = Field(default_factory=list)
    fabric_setting: str = ""  # the Fabric equivalent of the ADF resource setting
    runs_per_pipeline_run: float = 1.0
    runs_source: Source = Source.assumed
    duration_s: float = 0.0
    duration_source: Source = Source.assumed
    cu_seconds_per_pipeline_run: float = 0.0
    cu_seconds_by_meter: dict[str, float] = Field(default_factory=dict)  # per pipeline run
    notes: list[str] = Field(default_factory=list)


class PipelineCompute(_M):
    pipeline: str
    activities: list[ActivityCompute] = Field(default_factory=list)
    cu_seconds_per_run: float = 0.0
    cu_seconds_by_meter: dict[str, float] = Field(default_factory=dict)
    runs_per_day: float = 0.0
    runs_per_day_source: str = ""  # trigger name(s), "invoked by <parent>", or "assumed"
    cu_seconds_per_day: float = 0.0
    adf_billed_per_run: dict[str, float] = Field(default_factory=dict)  # measured in ADF (history only)
    history_runs_sampled: int = 0
    notes: list[str] = Field(default_factory=list)


class ComputeAssessment(_M):
    schema_version: str = "compute-assessment/1"
    project_id: str
    created_at: datetime
    source_commit_sha: Optional[str] = None
    history_used: bool = False
    assumptions: dict[str, Any] = Field(default_factory=dict)
    rates: dict[str, Any] = Field(default_factory=dict)
    sources: list[dict[str, str]] = Field(default_factory=list)
    pipelines: list[PipelineCompute] = Field(default_factory=list)
    cu_hours_per_day: float = 0.0
    average_cu_needed: float = 0.0  # CU-hours per day / 24 (background smoothing)
    recommended_sku: str = ""
    sku_reason: str = ""
    notes: list[str] = Field(default_factory=list)


# ---- estimate ---------------------------------------------------------------------------------------------------------
def _walk(acts: Iterable[AdfActivity], nesting: tuple[str, ...] = ()):
    for a in acts:
        yield a, nesting
        if a.activities:
            yield from _walk(a.activities, nesting + (f"{a.type}:{a.name}",))


def _int(v: Any) -> Optional[int]:
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _activity(p: AdfPipeline, a: AdfActivity, nesting: tuple[str, ...], hist: Optional[PipelineHistory]) -> ActivityCompute:
    meter = _ADF_METER.get(a.type or "", AdfMeter.orchestration)
    ac = ActivityCompute(pipeline=p.name, activity=a.name, activity_type=a.type, nesting=list(nesting), adf_meter=meter)
    if (a.type or "") not in _ADF_METER:
        ac.notes.append(f"Activity type '{a.type}' has no compute rule; counted as one orchestration run.")
    if any(n.split(":", 1)[0] in ("ForEach", "Until") for n in nesting):
        ac.notes.append("Runs inside a loop: assumes 1 iteration unless run history says otherwise.")

    h = hist.activities.get(a.name) if hist else None
    if h:
        ac.runs_per_pipeline_run, ac.runs_source = h.runs_per_pipeline_run, Source.history
        ac.duration_s, ac.duration_source = h.median_duration_s, Source.history
    else:
        ac.duration_s = DEFAULT_DURATION_S[meter]

    cfg = a.configuration or {}
    parts: dict[FabricMeter, float] = {}
    if meter is AdfMeter.data_movement:
        diu = _int(cfg.get("data_integration_units"))
        ac.adf_resources = {"dataIntegrationUnits": diu if diu is not None else "Auto",
                            "parallelCopies": _int(cfg.get("parallel_copies")) or "Auto"}
        used = h.used_diu if h and h.used_diu else None
        ito = max(R.ITO_MINIMUM, int(math.ceil(used if used else (diu or DEFAULT_COPY_DIU))))
        if used:
            ac.adf_resources["usedDataIntegrationUnits"] = used
        if diu is None and not used:
            ac.notes.append(f"DIUs are Auto: estimated at {DEFAULT_COPY_DIU} (Fabric's smallest documented throughput).")
        if diu is not None and diu < R.ITO_MINIMUM:
            ac.notes.append(f"{diu} DIUs is below Fabric's documented minimum throughput of {R.ITO_MINIMUM}; estimated at {R.ITO_MINIMUM}.")
        ac.fabric_meters = [FabricMeter.data_movement]
        ac.fabric_setting = f"Intelligent throughput optimization {ito}" + (" (custom)" if diu else " (Auto)")
        billed_s = math.ceil(max(ac.duration_s, 1) / R.COPY_BILLING_GRANULARITY_SECONDS) * R.COPY_BILLING_GRANULARITY_SECONDS
        if billed_s != ac.duration_s:
            ac.notes.append(f"Copy is billed per started minute: {ac.duration_s:g} s counts as {billed_s} s.")
        parts[FabricMeter.data_movement] = R.DATA_MOVEMENT_CU_PER_ITO * ito * billed_s
    else:
        ac.fabric_meters = [FabricMeter.orchestration]
        parts[FabricMeter.orchestration] = R.ORCHESTRATION_CU_HOURS_PER_RUN * 3600  # per run, not per second
        if meter is AdfMeter.data_flow:
            comp = cfg.get("compute") if isinstance(cfg.get("compute"), dict) else {}
            cores = _int(comp.get("core_count")) or DEFAULT_DATA_FLOW_CORES
            ac.adf_resources = {"coreCount": cores, "computeType": comp.get("compute_type") or "General"}
            if not _int(comp.get("core_count")):
                ac.notes.append(f"Core count not set on the activity: ADF's default {DEFAULT_DATA_FLOW_CORES} cores assumed.")
            ac.fabric_meters.append(FabricMeter.spark)
            ac.fabric_setting = f"Spark, {cores} vCores ({cores / R.SPARK_VCORES_PER_CU:g} CU)"
            ac.notes.append("Estimated as Spark compute. Mapping data flow transforms in Dataflow Gen2 are not billed yet "
                            "(preview), so this is an upper estimate until that pricing is published.")
            parts[FabricMeter.spark] = cores / R.SPARK_VCORES_PER_CU * ac.duration_s
        elif (a.type or "") in _SPARK_TYPES:
            v = R.SPARK_MEDIUM_NODE_VCORES
            ac.adf_resources = {"engine": a.type}
            ac.fabric_meters.append(FabricMeter.spark)
            ac.fabric_setting = f"Fabric notebook on a Medium node ({v} vCores, {v / R.SPARK_VCORES_PER_CU:g} CU)"
            ac.notes.append("Becomes a Fabric notebook; one Medium Spark node assumed. Size it from the real job.")
            parts[FabricMeter.spark] = v / R.SPARK_VCORES_PER_CU * ac.duration_s
        elif meter is AdfMeter.ssis:
            ac.fabric_meters.append(FabricMeter.ssis)
            ac.fabric_setting = "Invoke SSIS package (1.35 CU-hours per vCore-hour)"
            ac.notes.append("SSIS vCores depend on the Azure-SSIS runtime size, which the pipeline doesn't state: "
                            "only the orchestration run is estimated.")
        elif meter is AdfMeter.external:
            ac.fabric_setting = "Orchestration run; the external service is billed by that service, not Fabric CU"
        elif meter is AdfMeter.pipeline_activity:
            ac.fabric_setting = "Orchestration run (Fabric has no separate per-hour meter for this activity)"
        else:
            ac.fabric_setting = "Orchestration run"
    ac.cu_seconds_by_meter = {m.value: round(v * ac.runs_per_pipeline_run, 3) for m, v in parts.items()}
    ac.cu_seconds_per_pipeline_run = round(sum(ac.cu_seconds_by_meter.values()), 3)
    return ac


def _runs_per_day(t: AdfTrigger) -> Optional[float]:
    r = t.recurrence
    if not r or not r.frequency:
        return None
    every = max(1, r.interval or 1)
    per_slot = max(1, len(r.hours) or 1) * max(1, len(r.minutes) or 1)
    f = r.frequency.lower()
    if f == "minute":
        return 1440 / every
    if f == "hour":
        return 24 / every
    if f == "day":
        return per_slot / every
    if f == "week":
        return per_slot * max(1, len(r.week_days)) / 7 / every
    if f == "month":
        days = len(r.month_days) or len(r.monthly_occurrences) or 1
        return per_slot * days / 30.4 / every
    return None


def estimate(d: AdfSemanticDiscovery, history: Optional[dict[str, PipelineHistory]] = None,
             default_runs_per_day: float = 1.0, include_stopped_triggers: bool = True) -> ComputeAssessment:
    history = history or {}
    pipes = {p.name.lower(): p for p in d.pipelines}
    out: dict[str, PipelineCompute] = {}
    for p in d.pipelines:
        hist = history.get(p.name.lower())
        pc = PipelineCompute(pipeline=p.name)
        for a, nesting in _walk(p.activities):
            pc.activities.append(_activity(p, a, nesting, hist))
        pc.cu_seconds_per_run = round(sum(x.cu_seconds_per_pipeline_run for x in pc.activities), 3)
        for x in pc.activities:
            for m, v in x.cu_seconds_by_meter.items():
                pc.cu_seconds_by_meter[m] = round(pc.cu_seconds_by_meter.get(m, 0) + v, 3)
        if hist:
            pc.adf_billed_per_run, pc.history_runs_sampled = dict(hist.adf_billed_per_run), hist.runs_sampled
        out[p.name.lower()] = pc

    # Runs per day: from triggers, then pushed down to the pipelines they invoke (Execute pipeline).
    rpd: dict[str, float] = {}
    why: dict[str, list[str]] = {}
    for t in d.triggers:
        stopped = (t.runtime_state or "").lower() == "stopped"
        if stopped and not include_stopped_triggers:
            continue
        n = _runs_per_day(t)
        for ref in t.pipeline_refs:
            k = ref.lower()
            if n is None:
                why.setdefault(k, []).append(f"trigger {t.name} ({t.type}, frequency unknown)")
                continue
            rpd[k] = rpd.get(k, 0) + n
            why.setdefault(k, []).append(f"trigger {t.name}: {n:g}/day" + (" (currently Stopped)" if stopped else ""))
    invoked_by: dict[str, set[str]] = {}
    for p in d.pipelines:
        for a, _ in _walk(p.activities):
            if a.type == "ExecutePipeline":
                for c in a.pipeline_refs:
                    invoked_by.setdefault(c.lower(), set()).add(p.name.lower())

    def daily(k: str, seen: frozenset = frozenset()) -> float:
        if k in seen:
            return 0.0
        total = rpd.get(k, 0.0)
        for parent in invoked_by.get(k, ()):
            total += daily(parent, seen | {k})
        return total

    for k, pc in out.items():
        n = daily(k)
        if n:
            parents = sorted(pipes[x].name for x in invoked_by.get(k, ()) if x in pipes and daily(x) > 0)
            pc.runs_per_day = round(n, 4)
            pc.runs_per_day_source = "; ".join(why.get(k, []) + [f"invoked by {', '.join(parents)}"] if parents else why.get(k, []))
        elif k in invoked_by:
            pc.runs_per_day, pc.runs_per_day_source = 0.0, f"only invoked by {', '.join(sorted(pipes[x].name for x in invoked_by[k] if x in pipes))}, which is not scheduled"
        else:
            pc.runs_per_day, pc.runs_per_day_source = default_runs_per_day, f"assumed {default_runs_per_day:g}/day (no trigger)"
        pc.cu_seconds_per_day = round(pc.cu_seconds_per_run * pc.runs_per_day, 3)

    a = ComputeAssessment(project_id=d.project_id, created_at=datetime.now(timezone.utc),
                          source_commit_sha=d.source_commit_sha, history_used=bool(history),
                          pipelines=sorted(out.values(), key=lambda x: x.pipeline.lower()), sources=R.SOURCES)
    a.assumptions = {"default_runs_per_day_without_trigger": default_runs_per_day,
                     "stopped_triggers_counted": include_stopped_triggers,
                     "default_durations_seconds": {k.value: v for k, v in DEFAULT_DURATION_S.items()},
                     "copy_auto_diu_estimated_as": DEFAULT_COPY_DIU, "data_flow_default_cores": DEFAULT_DATA_FLOW_CORES,
                     "loop_iterations_without_history": 1, "sku_headroom": HEADROOM}
    a.rates = {"data_movement_cu_per_ito_hour": R.DATA_MOVEMENT_CU_PER_ITO,
               "orchestration_cu_hours_per_activity_run": R.ORCHESTRATION_CU_HOURS_PER_RUN,
               "spark_vcores_per_cu": R.SPARK_VCORES_PER_CU, "ssis_cu_hours_per_vcore_hour": R.SSIS_CU_PER_VCORE,
               "background_smoothing_hours": R.SMOOTHING_HOURS_BACKGROUND, "verified_on": R.VERIFIED_ON}
    a.cu_hours_per_day = round(sum(p.cu_seconds_per_day for p in a.pipelines) / 3600, 4)
    a.average_cu_needed = round(a.cu_hours_per_day / R.SMOOTHING_HOURS_BACKGROUND, 4)
    need = a.average_cu_needed * (1 + HEADROOM)
    sku = next((s for s in R.F_SKUS if s >= need), R.F_SKUS[-1])
    a.recommended_sku = f"F{sku}"
    a.sku_reason = (f"{a.cu_hours_per_day:g} CU-hours/day ÷ 24 h = {a.average_cu_needed:g} CU on average "
                    f"(+{int(HEADROOM * 100)}% headroom = {need:.3g} CU); F{sku} provides {sku} CU. "
                    "Pipelines are background operations, smoothed over 24 hours.")
    if sku == R.F_SKUS[0] and need < sku / 10:
        a.notes.append("Pipeline compute uses under 10% of the smallest capacity: the SKU will be driven by the other "
                       "Fabric workloads (Spark notebooks, warehouses, Power BI) sharing it, not by these pipelines.")
    if not history:
        a.notes.append("No ADF run history used: durations, loop iterations and Auto DIUs are stated assumptions. "
                       "Run with run history for measured numbers.")
    return a


# ---- report -----------------------------------------------------------------------------------------------------------
def render_markdown(a: ComputeAssessment) -> str:
    L = [f"# Compute assessment: {a.project_id}", "",
         f"Generated {a.created_at:%Y-%m-%d %H:%M} UTC · source commit `{(a.source_commit_sha or '-')[:12]}` · "
         f"run history: {'yes' if a.history_used else 'no (assumed durations)'}", "",
         "## Summary", "",
         f"- **Fabric compute for these pipelines:** {a.cu_hours_per_day:g} CU-hours per day "
         f"(≈ {a.average_cu_needed:g} CU on average).",
         f"- **Recommended capacity:** **{a.recommended_sku}**. {a.sku_reason}"]
    L += [f"- {n}" for n in a.notes]
    L += ["", "## Per pipeline", "",
          "| Pipeline | Runs/day | Why | Fabric CU-s per run | CU-s per day | ADF billed per run (measured) |",
          "|---|---|---|---|---|---|"]
    for p in a.pipelines:
        adf = ", ".join(f"{k}: {v:g}" for k, v in sorted(p.adf_billed_per_run.items())) or "-"
        L.append(f"| {p.pipeline} | {p.runs_per_day:g} | {p.runs_per_day_source} | {p.cu_seconds_per_run:g} | "
                 f"{p.cu_seconds_per_day:g} | {adf} |")
    L += ["", "## Per activity", "",
          "| Pipeline | Activity | Type | ADF meter | ADF resources | Fabric equivalent | Runs | Duration (s) | CU-s/run | Notes |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for p in a.pipelines:
        for x in p.activities:
            res = ", ".join(f"{k}={v}" for k, v in x.adf_resources.items()) or "-"
            L.append(f"| {p.pipeline} | {' / '.join(x.nesting + [x.activity])} | {x.activity_type} | {x.adf_meter.value} | "
                     f"{res} | {x.fabric_setting} | {x.runs_per_pipeline_run:g} ({x.runs_source.value}) | "
                     f"{x.duration_s:g} ({x.duration_source.value}) | {x.cu_seconds_per_pipeline_run:g} | "
                     f"{' '.join(x.notes) or '-'} |")
    L += ["", "## Rates and assumptions", "",
          f"- Data movement: {R.DATA_MOVEMENT_CU_PER_ITO} CU-hours per throughput (ITO) hour of Copy run time, "
          "billed per started minute.",
          f"- Orchestration: {R.ORCHESTRATION_CU_HOURS_PER_RUN} CU-hours per non-Copy activity run.",
          f"- Spark: {R.SPARK_VCORES_PER_CU} vCores = 1 CU. SSIS: {R.SSIS_CU_PER_VCORE} CU-hours per vCore-hour.",
          "- Background operations (pipelines) are smoothed over 24 hours; an F<n> capacity provides n CU.",
          f"- Defaults when there is no run history: {a.assumptions.get('default_durations_seconds')} seconds; "
          f"{a.assumptions.get('default_runs_per_day_without_trigger')} run/day for pipelines without a trigger; "
          "1 iteration per loop.", "", "Sources (verified " + R.VERIFIED_ON + "):"]
    L += [f"- {s['what']}: {s['url']}" for s in a.sources]
    return "\n".join(L) + "\n"
