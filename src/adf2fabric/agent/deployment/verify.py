"""Run-and-compare: prove a converted pipeline does what the ADF one did, from the activity results of both runs.

For each pipeline the Fabric run (a new one, or the latest finished one when nothing should be started) is compared with
a baseline, activity by activity and matched by name (conversion keeps activity names):

* the last successful run of the same pipeline in ADF, when the source is a live factory and such a run exists;
* otherwise the previous verified Fabric run of the pipeline, so a re-deploy can be re-proved;
* otherwise nothing: the run is recorded as the baseline for next time and the verdict says there was none.

What is compared is what both services report about an activity run: its status, Copy row and file counts, and a Lookup's
row count and first row. A Lookup's numbers are kept for display; its other values are only kept as a hash, so no data
is stored. A difference is reported, never "fixed": the source data may have changed between the two runs.

ADF calls (``queryPipelineRuns`` / ``queryActivityruns``, api-version 2018-06-01) go only to the ARM host; Fabric calls go
through ``FabricClient`` (Fabric host only). Nothing here starts a run unless the caller passes ``start=True``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlsplit

from adf2fabric.agent.deployment.fabric_deploy import DeployError, FabricClient
from adf2fabric.agent.deployment.fabric_run import ActivityRun, RunResult, latest_run, recent_runs, run_pipeline
from adf2fabric.cloud import ARM_ENDPOINT

ADF_API_VERSION = "2018-06-01"
COPY_KEYS = ("rowsRead", "rowsCopied", "filesRead", "filesWritten")
VERDICTS = ("matches", "partial", "differs", "no_baseline", "failed", "not_run")
_FAILURE_CHARS = 400


# --- what an activity run says, in comparable form --------------------------------------------------------------------
def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:16]


def metrics(activity_type: str, output: Any) -> dict[str, Any]:
    """The comparable facts in one activity's output. Numbers are kept; any other value only as a short hash."""
    out: dict[str, Any] = {}
    if not isinstance(output, dict):
        return out
    kind = (activity_type or "").lower()
    if kind == "copy":
        for k in COPY_KEYS:
            if isinstance(output.get(k), (int, float)):
                out[k] = output[k]
    elif kind == "lookup":
        if isinstance(output.get("count"), int):
            out["count"] = output["count"]
        row = output.get("firstRow")
        if isinstance(row, dict):
            for k, v in row.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    out[f"firstRow.{k}"] = v
            out["firstRow#"] = _digest(row)
        elif isinstance(output.get("value"), list):
            out["count"] = len(output["value"])
            out["value#"] = _digest(output["value"])
    return out


def summarize(runs: list[ActivityRun]) -> dict[str, dict]:
    """Activity name -> {type, status, runs, metrics}. A ForEach runs an inner activity once per item: numbers are summed
    and hashes are kept as a sorted list, so the order of the iterations does not matter.

    Both services list each retry attempt as its own row. When an activity (in the same iteration) has a successful
    row, its failed or cancelled rows are earlier attempts and are left out: a retry that recovered is not a failure."""
    recovered = {(r.name, r.iteration) for r in runs if r.status == "Succeeded"}
    runs = [r for r in runs if r.status == "Succeeded" or (r.name, r.iteration) not in recovered]
    out: dict[str, dict] = {}
    for r in runs:
        s = out.setdefault(r.name, {"type": r.type, "statuses": [], "runs": 0, "metrics": {}})
        s["runs"] += 1
        s["statuses"].append(r.status)
        for k, v in metrics(r.type, r.output).items():
            if k.endswith("#"):
                s["metrics"].setdefault(k, []).append(v)
            else:
                s["metrics"][k] = s["metrics"].get(k, 0) + v
    for s in out.values():
        s["status"] = "Succeeded" if s["statuses"] and all(x == "Succeeded" for x in s["statuses"]) else \
            next((x for x in s["statuses"] if x != "Succeeded"), "")
        del s["statuses"]
        for k, v in list(s["metrics"].items()):
            if k.endswith("#"):
                s["metrics"][k] = sorted(v)
    return out


def compare(fabric: dict[str, dict], base: dict[str, dict]) -> tuple[list[dict], int]:
    """(differences, number of facts that matched). Only activities and facts present on both sides are compared."""
    diffs, matched = [], 0
    for name in sorted(set(fabric) & set(base), key=str.lower):
        f, b = fabric[name], base[name]
        if f.get("runs") != b.get("runs"):
            diffs.append({"activity": name, "fact": "runs", "baseline": b.get("runs"), "fabric": f.get("runs")})
        else:
            matched += 1
        if f["status"] != b["status"]:
            diffs.append({"activity": name, "fact": "status", "baseline": b["status"], "fabric": f["status"]})
        else:
            matched += 1
        for k in sorted(set(f["metrics"]) & set(b["metrics"])):
            if f["metrics"][k] == b["metrics"][k]:
                matched += 1
            else:
                shown = (lambda v: "(different values)" if k.endswith("#") else v)  # noqa: E731
                diffs.append({"activity": name, "fact": k, "baseline": shown(b["metrics"][k]), "fabric": shown(f["metrics"][k])})
    return diffs, matched


# --- the ADF side -----------------------------------------------------------------------------------------------------
@dataclass
class Factory:
    subscription: str
    resource_group: str
    name: str

    @classmethod
    def from_source(cls, url: Optional[str]) -> Optional["Factory"]:
        """The factory behind an ``azure://subscriptions/../resourceGroups/../factories/..`` source, else None."""
        parts = (url or "").removeprefix("azure://").strip("/").split("/")
        low = [p.lower() for p in parts]
        try:
            return cls(parts[low.index("subscriptions") + 1], parts[low.index("resourcegroups") + 1],
                       parts[low.index("factories") + 1])
        except (ValueError, IndexError):
            return None

    @property
    def base(self) -> str:
        return (f"{ARM_ENDPOINT}/subscriptions/{self.subscription}/resourceGroups/{self.resource_group}"
                f"/providers/Microsoft.DataFactory/factories/{self.name}")


class AdfRuns:
    """Read-only access to an ADF factory's run history."""

    def __init__(self, factory: Factory, token: str, session: Any = None):
        if session is None:
            import requests

            session = requests.Session()
        self.factory, self._s, self._token = factory, session, token
        self._h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self._latest: Optional[dict[str, dict]] = None  # pipeline (lower) -> latest Succeeded run, once prefetched

    def clone(self) -> "AdfRuns":
        """The same factory and token on its own HTTP session (one per worker thread), sharing what was prefetched."""
        other = AdfRuns(self.factory, self._token)
        other._latest = self._latest
        return other

    def prefetch(self, days: int = 45) -> None:
        """Every pipeline's latest Succeeded run in one query (paged), instead of one query per pipeline."""
        now = datetime.now(timezone.utc)
        body: dict[str, Any] = {"lastUpdatedAfter": _stamp(now - timedelta(days=days)), "lastUpdatedBefore": _stamp(now),
                                "filters": [{"operand": "Status", "operator": "Equals", "values": ["Succeeded"]}]}
        latest: dict[str, dict] = {}
        for _ in range(50):
            data = self._post(f"{self.factory.base}/queryPipelineRuns?api-version={ADF_API_VERSION}", body)
            for r in data.get("value", []):
                key = str(r.get("pipelineName", "")).lower()
                if key and str(r.get("runEnd") or "") > str(latest.get(key, {}).get("runEnd") or ""):
                    latest[key] = r
            if not data.get("continuationToken"):
                break
            body["continuationToken"] = data["continuationToken"]
        self._latest = latest

    def _post(self, url: str, body: dict) -> dict:
        if urlsplit(url).netloc != urlsplit(ARM_ENDPOINT).netloc:  # the ARM token only ever goes to ARM
            raise DeployError("Refusing to send an ARM token to a non-ARM URL.")
        try:
            resp = self._s.post(url, headers=self._h, json=body, timeout=60)
        except Exception as exc:  # noqa: BLE001 - connection errors, timeouts: reported, never a traceback
            raise DeployError(f"ADF could not be reached ({type(exc).__name__}).") from None
        if resp.status_code != 200:
            raise DeployError(f"ADF returned HTTP {resp.status_code} for its run history.")
        try:
            data = resp.json()
        except ValueError:
            raise DeployError("ADF returned something that is not JSON for its run history.") from None
        return data if isinstance(data, dict) else {}

    def last_success(self, pipeline: str, days: int = 45) -> Optional[dict]:
        """The latest Succeeded run of ``pipeline`` in the last ``days`` days (ADF keeps 45), or None."""
        if self._latest is not None:
            return self._latest.get(pipeline.lower())
        now = datetime.now(timezone.utc)
        body = {"lastUpdatedAfter": _stamp(now - timedelta(days=days)), "lastUpdatedBefore": _stamp(now),
                "filters": [{"operand": "PipelineName", "operator": "Equals", "values": [pipeline]},
                            {"operand": "Status", "operator": "Equals", "values": ["Succeeded"]}],
                "orderBy": [{"orderBy": "RunEnd", "order": "DESC"}]}
        runs = self._post(f"{self.factory.base}/queryPipelineRuns?api-version={ADF_API_VERSION}", body).get("value", [])
        return runs[0] if runs else None

    def successes_near(self, run: dict, window_minutes: int = 10) -> list[dict]:
        """The batch ``run`` belongs to: when a parent pipeline started it, every Succeeded run of the same pipeline that
        the same parent run started; when it was started by hand or by a trigger, ``run`` alone."""
        parent = (run.get("invokedBy") or {})
        if parent.get("invokedByType") != "PipelineActivity" or not parent.get("pipelineRunId"):
            return [run]
        start = _parse(run.get("runStart"))
        if start is None:
            return [run]
        body = {"lastUpdatedAfter": _stamp(start - timedelta(minutes=window_minutes)),
                "lastUpdatedBefore": _stamp(min(start + timedelta(days=1), datetime.now(timezone.utc))),
                "filters": [{"operand": "PipelineName", "operator": "Equals", "values": [run.get("pipelineName", "")]},
                            {"operand": "Status", "operator": "Equals", "values": ["Succeeded"]}]}
        rows = self._post(f"{self.factory.base}/queryPipelineRuns?api-version={ADF_API_VERSION}", body).get("value", [])
        same = [r for r in rows if (r.get("invokedBy") or {}).get("pipelineRunId") == parent["pipelineRunId"]]
        return same or [run]

    def activities(self, run: dict) -> list[ActivityRun]:
        start = _parse(run.get("runStart")) or datetime.now(timezone.utc) - timedelta(days=45)
        end = _parse(run.get("runEnd")) or datetime.now(timezone.utc)
        body = {"lastUpdatedAfter": _stamp(start - timedelta(minutes=5)), "lastUpdatedBefore": _stamp(end + timedelta(days=1))}
        rows = self._post(f"{self.factory.base}/pipelineruns/{run['runId']}/queryActivityruns?api-version={ADF_API_VERSION}",
                          body).get("value", [])
        return [ActivityRun(name=str(a.get("activityName", "")), type=str(a.get("activityType", "")),
                            status=str(a.get("status", "")), output=a.get("output") if isinstance(a.get("output"), dict) else {},
                            iteration=str(a.get("iterationHash") or "")) for a in rows]


def _stamp(d: datetime) -> str:
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(s: Any) -> Optional[datetime]:
    """Both services report UTC, sometimes without a zone suffix: a naive time is UTC, never this machine's local time."""
    try:
        d = datetime.fromisoformat(str(s).replace("Z", "+00:00")) if s else None
    except ValueError:
        return None
    return d.replace(tzinfo=timezone.utc) if d is not None and d.tzinfo is None else d


# --- one pipeline -----------------------------------------------------------------------------------------------------
@dataclass
class Verification:
    pipeline: str
    verdict: str  # one of VERDICTS
    fabric_status: str = ""
    job_id: str = ""
    ran_at: str = ""
    baseline: str = ""  # adf | fabric | ""
    baseline_run: str = ""
    baseline_at: str = ""
    matched: int = 0
    baseline_only: list[str] = field(default_factory=list)  # activities the baseline ran that this run did not
    fabric_only: list[str] = field(default_factory=list)
    differences: list[dict] = field(default_factory=list)
    activities: dict[str, dict] = field(default_factory=dict)
    detail: str = ""

    def to_dict(self) -> dict:
        return dict(self.__dict__)


def verify_pipeline(client: FabricClient, workspace: str, pipeline: str, *, start: bool,
                    adf: Optional[AdfRuns] = None, previous: Optional[dict] = None, timeout_seconds: int = 1800,
                    runner: Callable[..., RunResult] = run_pipeline, called_by: Optional[list[str]] = None,
                    ids: Optional[dict[str, str]] = None) -> Verification:
    """Run (``start``) or read the latest run of one pipeline in Fabric and compare it with the best baseline there is.

    Nothing raised here stops the caller's loop: a pipeline that is not deployed, or a Fabric or ADF call that fails, is
    reported on that pipeline's own verdict."""
    if called_by:
        return _verify_batch(client, workspace, pipeline, called_by, adf, previous, ids)
    try:
        if start:
            run = runner(client, workspace, pipeline, wait=True, timeout_seconds=timeout_seconds)
        else:
            run = latest_run(client, workspace, pipeline, ids) if ids is not None else latest_run(client, workspace, pipeline)
    except DeployError as exc:
        return Verification(pipeline, "not_run", detail=f"Could not run or read it in Fabric: {getattr(exc, 'detail', None) or exc}")
    except Exception as exc:  # noqa: BLE001 - network errors and the like: this pipeline only
        return Verification(pipeline, "not_run", detail=f"Could not reach Fabric for it ({type(exc).__name__}).")
    if run is None:
        return Verification(pipeline, "not_run", detail="No finished Fabric run of this pipeline yet. Run it, or pass --apply.")
    v = Verification(pipeline, "failed", fabric_status=run.status, job_id=run.job_id, ran_at=run.ended or run.started)
    v.activities = summarize(run.activities)
    if not run.ok:
        v.detail = short_failure(run.failure or run.detail or f"the Fabric run ended {run.status}")
        return v
    base: Optional[dict[str, dict]] = None
    if adf is not None:
        try:
            last = adf.last_success(pipeline)
            if last:
                base = summarize(adf.activities(last))
                v.baseline, v.baseline_run, v.baseline_at = "adf", str(last.get("runId", "")), str(last.get("runEnd", ""))
        except DeployError as exc:
            base, v.detail = None, f"ADF history not read: {exc}"
    good = last_completed(previous)
    if base is None and good and good.get("job_id") != run.job_id:
        base = good.get("activities") or {}
        v.baseline, v.baseline_run, v.baseline_at = "fabric", str(good.get("job_id", "")), str(good.get("ran_at", ""))
    if base is None:
        v.verdict = "no_baseline"
        v.detail = v.detail or "No successful ADF run in the last 45 days and no earlier Fabric run: this run is the baseline."
        return v
    return _judge(v, base)


def _judge(v: Verification, base: dict[str, dict], how: str = "") -> Verification:
    """The verdict from comparing ``v.activities`` with ``base``; ``how`` says what was compared (for a batch)."""
    v.differences, v.matched = compare(v.activities, base)
    v.baseline_only = sorted(set(base) - set(v.activities), key=str.lower)
    v.fabric_only = sorted(set(v.activities) - set(base), key=str.lower)
    notes = [how] if how else []
    if v.differences:
        v.verdict = "differs"
    elif not v.matched:
        v.verdict = "no_baseline"
        notes.append("The baseline run has no activity in common with this one.")
    else:
        v.verdict = "partial" if (v.baseline_only or v.fabric_only) else "matches"
    if v.verdict == "partial":
        notes.append("Not every activity could be compared: "
                     + "; ".join(x for x in (f"only in the baseline: {', '.join(v.baseline_only)}" if v.baseline_only else "",
                                             f"only in Fabric: {', '.join(v.fabric_only)}" if v.fabric_only else "") if x)
                     + ". A renamed activity, a branch that did not run, or a run of an older deployed version can cause this.")
    v.detail = " ".join(notes) or v.detail
    return v


def verify_all(make_client: Callable[[], FabricClient], workspace: str, names: list[str], *, start: bool,
               adf: Optional[AdfRuns] = None, previous: Optional[dict[str, dict]] = None,
               called: Optional[dict[str, list[str]]] = None, timeout_seconds: int = 1800, workers: int = 10,
               log: Callable[[str], None] = lambda _m: None) -> list[Verification]:
    """Verify ``names`` and return the results in the same order.

    The workspace's pipeline list and the factory's run history are read once for all of them. Reading is done in
    parallel (each worker has its own HTTP sessions); starting runs (``start``) stays one at a time so a verification
    never floods the capacity."""
    from concurrent.futures import ThreadPoolExecutor

    previous, called = previous or {}, called or {}
    first = make_client()
    try:
        ids: Optional[dict[str, str]] = first.list_pipelines(workspace)
    except Exception:  # noqa: BLE001 - each pipeline then reports its own problem
        ids = None
    if adf is not None:
        try:
            adf.prefetch()
        except DeployError as exc:
            log(f"ADF history not prefetched ({exc}); reading it per pipeline")

    def one(name: str) -> Verification:
        log(f"comparing {name}")
        return verify_pipeline(make_client(), workspace, name, start=start, adf=adf.clone() if adf else None,
                               previous=previous.get(name), timeout_seconds=timeout_seconds,
                               called_by=called.get(name), ids=ids)

    if start or workers <= 1 or len(names) <= 1:
        # A pipeline another one calls is judged by the runs its caller starts, so callers go first; results keep the order asked for.
        order = [n for n in names if not called.get(n)] + [n for n in names if called.get(n)] if start else list(names)
        done = {n: one(n) for n in order}
        return [done[n] for n in names]
    with ThreadPoolExecutor(max_workers=min(workers, len(names))) as pool:
        return list(pool.map(one, names))


def _verify_batch(client: FabricClient, workspace: str, pipeline: str, called_by: list[str], adf: Optional[AdfRuns],
                  previous: Optional[dict], ids: Optional[dict[str, str]] = None) -> Verification:
    """A pipeline another one calls (often once per item, with different parameters) is compared as a batch: every run
    that started together, merged. Comparing one run with one run would pair different items. It is never started on its
    own here (it needs its parent's parameters): run the parent."""
    parents = ", ".join(called_by)
    adf_runs: Optional[list[dict]] = None
    adf_error = ""
    if adf is not None:
        try:
            last = adf.last_success(pipeline)
            adf_runs = adf.successes_near(last) if last else None
        except DeployError as exc:
            adf_error = f"ADF history not read: {exc}"
    # Fabric records every run as Manual: a single ADF run started by hand means compare single runs
    by_hand = adf_runs is not None and len(adf_runs) == 1 and         (adf_runs[0].get("invokedBy") or {}).get("invokedByType") != "PipelineActivity"
    try:
        runs = recent_runs(client, workspace, pipeline, window_minutes=0 if by_hand else 10, ids=ids)
    except DeployError as exc:
        return Verification(pipeline, "not_run", detail=f"Could not read it in Fabric: {getattr(exc, 'detail', None) or exc}")
    except Exception as exc:  # noqa: BLE001
        return Verification(pipeline, "not_run", detail=f"Could not reach Fabric for it ({type(exc).__name__}).")
    if not runs:
        return Verification(pipeline, "not_run", detail=f"No finished Fabric run yet. It is called by {parents}: run that.")
    newest = runs[0]
    v = Verification(pipeline, "failed", fabric_status="Completed" if all(r.ok for r in runs) else "Failed",
                     job_id=newest.job_id, ran_at=newest.ended or newest.started)
    v.activities = summarize([a for r in runs for a in r.activities])
    bad = next((r for r in runs if not r.ok), None)
    if bad is not None:
        v.detail = short_failure(bad.failure or f"a run ended {bad.status}")
        return v
    batch = ("its latest run (the ADF baseline was started by hand)" if by_hand else
             f"a batch of {len(runs)} run(s) started together (it is called by {parents})")
    base: Optional[dict[str, dict]] = None
    v.detail = adf_error
    if adf_runs:
        try:
            base = summarize([a for r in adf_runs for a in adf.activities(r)])
            v.baseline, v.baseline_run, v.baseline_at = "adf", str(adf_runs[0].get("runId", "")), str(adf_runs[0].get("runEnd", ""))
        except DeployError as exc:
            base, v.detail = None, f"ADF history not read: {exc}"
    good = last_completed(previous)
    if base is None and good and good.get("job_id") != v.job_id:
        base = good.get("activities") or {}
        v.baseline, v.baseline_run, v.baseline_at = "fabric", str(good.get("job_id", "")), str(good.get("ran_at", ""))
    if base is None:
        v.verdict, v.detail = "no_baseline", v.detail or f"Nothing to compare {batch} with yet."
        return v
    return _judge(v, base, f"Compared as {batch}.")


def short_failure(text: str) -> str:
    """The useful part of a pipeline failure: the innermost 'Message=' (what the data source said), else the start."""
    msgs = [m.split(",Source=")[0].strip().rstrip(",}]' ").strip(" '") for m in text.split("Message=")[1:]]
    msgs = [m for m in msgs if m]
    head = text.split(":", 1)[0] if ":" in text[:12] else ""
    best = msgs[-1] if msgs else text
    out = (f"{head}: " if head and msgs else "") + best
    return out if len(out) <= _FAILURE_CHARS else out[:_FAILURE_CHARS - 1] + "…"


def _bound_children(conversion_dir: Path) -> dict[str, str]:
    """Item id (lower case) -> pipeline name for the child pipelines bound in the resolutions file (``ItemToId`` ``pipeline:NAME``)."""
    try:
        rows = json.loads((conversion_dir / "resolutions.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out: dict[str, str] = {}
    for r in rows if isinstance(rows, list) else []:
        key = str(r.get("key", "")) if isinstance(r, dict) else ""
        if isinstance(r, dict) and r.get("type") == "ItemToId" and key.lower().startswith("pipeline:") and r.get("value"):
            out[str(r["value"]).lower()] = key.split(":", 1)[1]
    return out


def _invoked_ids(content: Any) -> list[str]:
    """Every ``pipelineId`` an InvokePipeline activity holds, at any depth (iterative: a deep pipeline must not hit the recursion limit)."""
    found: list[str] = []
    stack = [content]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if node.get("type") == "InvokePipeline" and isinstance(node.get("typeProperties"), dict):
                pid = node["typeProperties"].get("pipelineId")
                if isinstance(pid, str):
                    found.append(pid.lower())
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return found


def callers(conversion_dir: Path, pipelines: list) -> dict[str, list[str]]:
    """Pipeline -> the converted pipelines that invoke it, from the converted JSON (``pipelines`` = PipelineConversion).

    A child is found by its ``<item:pipeline:...>`` placeholder, or, once its id is bound, by that id in the resolutions file:
    binding a child must not make its caller look like it calls nothing."""
    from adf2fabric.agent.deployment.fabric_deploy import child_refs

    bound = _bound_children(conversion_dir)
    out: dict[str, list[str]] = {}
    for p in pipelines:
        if not p.artifact_path:
            continue
        try:
            content = json.loads((conversion_dir / p.artifact_path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        children = list(child_refs(content))
        children += [bound[i] for i in _invoked_ids(content) if i in bound and bound[i] not in children]
        for child in children:
            out.setdefault(child, []).append(p.source_pipeline)
    return out


# --- the record -------------------------------------------------------------------------------------------------------
def record_path(project_dir: Path) -> Path:
    return project_dir / "verification" / "verification.json"


def load_record(path: Path) -> dict[str, dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def last_completed(entry: Optional[dict]) -> Optional[dict]:
    """The last completed Fabric run kept for a pipeline (a later failed run does not replace it)."""
    if not entry:
        return None
    if entry.get("last_completed"):
        return entry["last_completed"]
    return entry if entry.get("fabric_status") == "Completed" else None  # records written before last_completed existed


def save_record(path: Path, results: list[Verification]) -> dict[str, dict]:
    """Merge results into the record. Each pipeline keeps its latest result and, separately, its last completed run, so
    a failed run never takes away the baseline the next run is compared with."""
    record = load_record(path)
    for v in results:
        if v.verdict == "not_run":
            continue
        old = record.get(v.pipeline)
        if old and old.get("job_id") == v.job_id and old.get("baseline") and not v.baseline:
            continue  # the same run, already compared with a real baseline: a weaker re-read does not replace it
        entry = v.to_dict()
        good = entry if v.fabric_status == "Completed" else last_completed(old)
        entry["last_completed"] = ({k: good.get(k) for k in ("job_id", "ran_at", "fabric_status", "activities")}
                                   if good else None)
        record[v.pipeline] = entry
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return record


def render_report(record: dict[str, dict]) -> str:
    out = ["# Run-and-compare", "",
           "Each converted pipeline's Fabric run compared, activity by activity, with the last successful ADF run (or the "
           "previous Fabric run). A difference can also mean the source data changed between the two runs.", "",
           "| Pipeline | Verdict | Fabric run | Baseline | Facts matched | Differences |", "| --- | --- | --- | --- | --- | --- |"]
    for name in sorted(record, key=str.lower):
        v = record[name]
        base = f"{v.get('baseline') or '-'} {v.get('baseline_at') or ''}".strip()
        out.append(f"| `{name}` | **{v.get('verdict')}** | {v.get('fabric_status') or '-'} {v.get('ran_at') or ''} | {base} | "
                   f"{v.get('matched', 0)} | {len(v.get('differences') or [])} |")
    for name in sorted(record, key=str.lower):
        v = record[name]
        if v.get("differences") or v.get("detail"):
            out += ["", f"## `{name}`", ""]
            if v.get("detail"):
                out.append(v["detail"])
            for d in v.get("differences") or []:
                out.append(f"- `{d['activity']}` {d['fact']}: baseline {d['baseline']} -> Fabric {d['fabric']}")
    return "\n".join(out) + "\n"
