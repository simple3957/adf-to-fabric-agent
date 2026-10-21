"""Run a deployed Fabric pipeline on demand and report how it ended.

Uses the Job Scheduler APIs (verified against Microsoft Learn, 2026-10-16, and live on 2026-10-12):

* ``POST /v1/workspaces/{ws}/items/{id}/jobs/Pipeline/instances`` - 202 with a ``Location`` header for the job instance
  and a ``Retry-After`` hint. Pipeline parameters go in ``executionData.parameters`` (the generic top-level
  ``parameters`` list is documented as not broadly supported, so it is not used).
* ``GET /v1/workspaces/{ws}/items/{id}/jobs/instances/{jobId}`` - ``status`` is NotStarted | InProgress | Completed |
  Failed | Cancelled | Deduped; ``failureReason`` carries an error response when it failed.
* ``POST /v1/workspaces/{ws}/datapipelines/pipelineruns/{jobId}/queryactivityruns`` - per-activity results; the body needs
  ``lastUpdatedAfter`` and ``lastUpdatedBefore`` or nothing comes back.

Running a pipeline executes it, so the caller must ask for it explicitly; this module never runs anything by itself.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, DeployError, FabricClient, _GUID, _error_text

TERMINAL = frozenset({"completed", "failed", "cancelled", "deduped"})
DEFAULT_TIMEOUT_SECONDS = 1800


@dataclass
class ActivityRun:
    name: str
    type: str
    status: str
    error: str = ""
    output: dict = field(default_factory=dict)  # the activity's output as the service reports it (run-and-compare reads it)
    iteration: str = ""  # iterationHash: tells the items of a ForEach apart (retries of one item share it)


@dataclass
class RunResult:
    pipeline: str
    item_id: str
    job_id: str = ""
    status: str = ""  # a documented job status, or "Timeout" / "NotRun"
    started: str = ""
    ended: str = ""
    failure: str = ""
    activities: list[ActivityRun] = field(default_factory=list)
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "Completed"


def _failure_text(body: Any) -> str:
    if not isinstance(body, dict):
        return ""
    return f"{body.get('errorCode', '')}: {body.get('message', '')}".strip(": ").strip()


def _query_activities(client: FabricClient, workspace: str, job_id: str, started: str) -> list[ActivityRun]:
    """Per-activity outcome; best effort, since the run itself already has its verdict."""
    try:
        start = datetime.fromisoformat(started.replace("Z", "+00:00")) if started else None
    except ValueError:
        start = None
    if start is not None and start.tzinfo is None:  # Fabric's times are UTC, often without a zone suffix
        start = start.replace(tzinfo=timezone.utc)
    after = (start or datetime.now(timezone.utc) - timedelta(days=1)) - timedelta(minutes=5)
    before = datetime.now(timezone.utc) + timedelta(minutes=5)
    stamp = lambda d: d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    try:
        resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/datapipelines/pipelineruns/{job_id}/queryactivityruns",
                            {"lastUpdatedAfter": stamp(after), "lastUpdatedBefore": stamp(before)})
    except DeployError:
        return []
    if resp.status_code != 200:
        return []
    body = resp.json() or {}
    rows = body if isinstance(body, list) else body.get("value", [])  # Microsoft's sample shows a bare list
    out: list[ActivityRun] = []
    for a in rows:
        err = a.get("error") if isinstance(a.get("error"), dict) else {}
        out.append(ActivityRun(name=str(a.get("activityName", "")), type=str(a.get("activityType", "")),
                               status=str(a.get("status", "")), error=str(err.get("message", ""))[:300],
                               output=a.get("output") if isinstance(a.get("output"), dict) else {},
                               iteration=str(a.get("iterationHash") or "")))
    return sorted(out, key=lambda r: r.name.lower())


def latest_run(client: FabricClient, workspace: str, name: str) -> Optional[RunResult]:
    """The most recent finished run of a pipeline, read-only (``GET .../jobs/instances``). None when it never ran."""
    runs = recent_runs(client, workspace, name, window_minutes=0)
    return runs[0] if runs else None


def recent_runs(client: FabricClient, workspace: str, name: str, window_minutes: int = 10) -> list[RunResult]:
    """The most recent finished run and every other finished run that started within ``window_minutes`` of it, newest
    first, read-only. A child pipeline that a parent calls once per item runs as such a batch."""
    ids = client.list_pipelines(workspace)
    item_id = ids.get(name.lower())
    if item_id is None:
        raise DeployError(f"No pipeline named '{name}' in the workspace.")
    jobs: list[dict] = []
    url: Optional[str] = f"{FABRIC_API}/workspaces/{workspace}/items/{item_id}/jobs/instances"
    for _ in range(20):  # pages; _call refuses any URL that is not Fabric's
        resp = client._call("GET", url)
        if resp.status_code != 200:
            raise DeployError(f"Reading the runs of '{name}' failed (HTTP {resp.status_code}): {_error_text(resp)}")
        body = resp.json() or {}
        jobs += body.get("value", [])
        url = body.get("continuationUri")
        if not url:
            break
    # Completed or Failed only: a Deduped or Cancelled job says nothing about what the pipeline does
    done = [j for j in jobs if str(j.get("status", "")).lower() in ("completed", "failed")]
    if not done:
        return []
    done.sort(key=lambda j: str(j.get("startTimeUtc") or j.get("endTimeUtc") or ""), reverse=True)
    newest = _utc(done[0].get("startTimeUtc"))
    if newest is not None:
        done = [j for j in done if (t := _utc(j.get("startTimeUtc"))) is not None
                and abs((newest - t).total_seconds()) <= window_minutes * 60]
    else:
        done = done[:1]
    out = []
    for job in done:
        result = RunResult(pipeline=name, item_id=item_id, job_id=str(job.get("id", "")), status=str(job.get("status", "")),
                           started=str(job.get("startTimeUtc") or ""), ended=str(job.get("endTimeUtc") or ""),
                           failure=_failure_text(job.get("failureReason")))
        result.activities = _query_activities(client, workspace, result.job_id, result.started)
        out.append(result)
    return out


def _utc(s: Any) -> Optional[datetime]:
    try:
        d = datetime.fromisoformat(str(s).replace("Z", "+00:00")) if s else None
    except ValueError:
        return None
    return d.replace(tzinfo=timezone.utc) if d is not None and d.tzinfo is None else d


def run_pipeline(client: FabricClient, workspace: str, name: str, *, parameters: Optional[dict[str, Any]] = None,
                 wait: bool = True, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> RunResult:
    """Start one pipeline by name and (by default) wait for its verdict."""
    if not _GUID.match(workspace or ""):
        raise DeployError("The workspace id must be a GUID (Fabric workspace settings > About).")
    ids = client.list_pipelines(workspace)
    item_id = ids.get(name.lower())
    if item_id is None:
        raise DeployError(f"No pipeline named '{name}' in the workspace. Present: {', '.join(sorted(ids)) or '(none)'}.")
    result = RunResult(pipeline=name, item_id=item_id)
    body = {"executionData": {"parameters": parameters}} if parameters else None
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/items/{item_id}/jobs/Pipeline/instances", body)
    if resp.status_code != 202:
        raise DeployError(f"Starting '{name}' failed (HTTP {resp.status_code}): {_error_text(resp)}")
    location = resp.headers.get("Location", "")
    result.job_id = location.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    if not _GUID.match(result.job_id):
        raise DeployError("Fabric accepted the run but returned no usable job instance address.")
    # Built here, never taken from Location: a regional host in that header must not receive the bearer token.
    location = f"{FABRIC_API}/workspaces/{workspace}/items/{item_id}/jobs/instances/{result.job_id}"
    result.status = "InProgress"
    if not wait:
        result.detail = "started; not waited for"
        return result
    wait_for = float(resp.headers.get("Retry-After", 10))
    deadline = clock() + timeout_seconds
    while True:
        sleep(wait_for)
        state = client._call("GET", location)
        if state.status_code != 200:
            raise DeployError(f"Reading the run of '{name}' failed (HTTP {state.status_code}): {_error_text(state)}")
        data = state.json()
        status = str(data.get("status", ""))
        result.started = str(data.get("startTimeUtc") or "")
        result.ended = str(data.get("endTimeUtc") or "")
        if status.lower() in TERMINAL:
            result.status = status
            result.failure = _failure_text(data.get("failureReason"))
            result.activities = _query_activities(client, workspace, result.job_id, result.started)
            return result
        result.status = status or result.status
        if clock() >= deadline:
            result.status = "Timeout"
            result.detail = f"still {status or 'running'} after {timeout_seconds}s; it keeps running in Fabric"
            return result
        wait_for = float(state.headers.get("Retry-After", wait_for))
