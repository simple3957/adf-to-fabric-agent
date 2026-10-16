"""Measured ADF usage from run history, for the compute assessment (read-only ARM calls).

For each pipeline: the most recent successful runs in the look-back window (``queryPipelineRuns``), and for each run its
activity runs (``queryActivityruns``). From those: per activity the median duration, how many times it ran per pipeline
run (loops), the DIUs and parallel copies a Copy really used, and per pipeline what ADF billed (``billingReference``).

Only POST query endpoints are called; nothing is created, changed or run. The transport is injectable for tests.
"""

from __future__ import annotations

import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from adf2fabric.agent.assessment.compute import ActivityHistory, PipelineHistory
from adf2fabric.agent.discovery.adf.arm import API_VERSION, factory_base_url


class HistoryError(Exception):
    pass


def _post(transport: Any, url: str, token: str, body: dict) -> dict:
    r = transport.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json=body,
                       timeout=60)
    if getattr(r, "status_code", None) != 200:
        raise HistoryError(f"ADF run history request failed (HTTP {getattr(r, 'status_code', '?')}).")
    return r.json() or {}


def _window(days: int) -> dict:
    now = datetime.now(timezone.utc)
    return {"lastUpdatedAfter": (now - timedelta(days=days)).isoformat(), "lastUpdatedBefore": (now + timedelta(hours=1)).isoformat()}


def fetch(transport: Any, token: str, subscription_id: str, resource_group: str, factory: str, pipelines: list[str],
          days: int = 30, runs_per_pipeline: int = 10, log: Optional[Callable[[str], None]] = None) -> dict[str, PipelineHistory]:
    """Run history keyed by lower-case pipeline name. Pipelines with no successful run in the window are left out.

    Runs are read newest first. An activity whose type changed (e.g. a Wait placeholder later replaced by a Databricks
    notebook) only counts the runs with its newest type, so old runs of the placeholder never skew the numbers.
    """
    base = factory_base_url(subscription_id, resource_group, factory)
    out: dict[str, PipelineHistory] = {}
    for name in pipelines:
        body = {**_window(days), "filters": [
            {"operand": "PipelineName", "operator": "Equals", "values": [name]},
            {"operand": "Status", "operator": "Equals", "values": ["Succeeded"]}],
            "orderBy": [{"orderBy": "RunStart", "order": "DESC"}]}
        runs = _post(transport, f"{base}/queryPipelineRuns?api-version={API_VERSION}", token, body).get("value", [])
        runs = runs[:runs_per_pipeline]
        if not runs:
            continue
        if log:
            log(f"{name}: {len(runs)} run(s)")
        durations: dict[str, list[float]] = {}
        counts: dict[str, list[int]] = {}
        types: dict[str, str] = {}
        dius: dict[str, list[float]] = {}
        copies: dict[str, list[float]] = {}
        billed: dict[str, float] = {}
        appeared: dict[str, int] = {}  # runs in which the activity ran with its newest type
        for run in runs:
            acts = _post(transport, f"{base}/pipelineruns/{run['runId']}/queryActivityruns?api-version={API_VERSION}", token,
                         _window(days)).get("value", [])
            per_run: dict[str, int] = {}
            for a in acts:
                n = a.get("activityName") or "?"
                types.setdefault(n, a.get("activityType"))  # the newest run's type
                if a.get("activityType") != types[n]:
                    continue
                per_run[n] = per_run.get(n, 0) + 1
                durations.setdefault(n, []).append((a.get("durationInMs") or 0) / 1000)
                o = a.get("output") or {}
                if isinstance(o.get("usedDataIntegrationUnits"), (int, float)):
                    dius.setdefault(n, []).append(o["usedDataIntegrationUnits"])
                if isinstance(o.get("usedParallelCopies"), (int, float)):
                    copies.setdefault(n, []).append(o["usedParallelCopies"])
                ref = o.get("billingReference") or {}
                for b in ref.get("billableDuration") or []:
                    key = f"{ref.get('activityType', '?')} {b.get('unit', '?')}"
                    billed[key] = billed.get(key, 0.0) + float(b.get("duration") or 0)
            for n, c in per_run.items():
                counts.setdefault(n, []).append(c)
                appeared[n] = appeared.get(n, 0) + 1
        k = len(runs)
        out[name.lower()] = PipelineHistory(
            pipeline=name, runs_sampled=k,
            median_duration_s=round(statistics.median((r.get("durationInMs") or 0) / 1000 for r in runs), 1),
            activities={n: ActivityHistory(
                activity_type=types.get(n), runs_per_pipeline_run=round(sum(counts.get(n, [1])) / max(1, appeared.get(n, k)), 2),
                median_duration_s=round(statistics.median(durations[n]), 1),
                used_diu=statistics.median(dius[n]) if dius.get(n) else None,
                used_parallel_copies=statistics.median(copies[n]) if copies.get(n) else None) for n in durations},
            adf_billed_per_run={key: round(v / k, 6) for key, v in sorted(billed.items())})
    return out
