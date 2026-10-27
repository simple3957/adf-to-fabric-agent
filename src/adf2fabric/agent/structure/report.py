"""The pipeline report: what each pipeline consists of in ADF and in the deployed Fabric item, whether they match, and how the latest
Fabric run behaved. Everything is read-only (GETs, plus the read-only ``getDefinition``); nothing is started or changed.

The reads (one definition and one run history per pipeline) are independent, so they run in parallel, each worker with its own client.
"""

from __future__ import annotations

import base64
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Optional

import yaml

from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, DeployError, FabricClient, _operation_url
from adf2fabric.agent.deployment.fabric_run import RunResult, recent_runs
from adf2fabric.agent.deployment.verify import metrics as activity_metrics
from adf2fabric.agent.deployment.verify import summarize
from adf2fabric.agent.structure import confidence as conf
from adf2fabric.agent.structure.compare import Parity, compare
from adf2fabric.agent.structure.tree import Node, Structure, adf_structure, fabric_structure
from adf2fabric.agent.validation.views import scan_source


def get_definition(client: FabricClient, workspace: str, item_id: str) -> dict:
    """The deployed pipeline's ``pipeline-content.json`` (read-only ``getDefinition``; waits for the long-running operation)."""
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/dataPipelines/{item_id}/getDefinition")
    if resp.status_code == 202:
        op = _operation_url(resp.headers)
        if not op:
            raise DeployError("Fabric accepted the definition request but returned no operation address.")
        for _ in range(60):
            client._sleep(float(resp.headers.get("Retry-After", 2)))
            state = client._call("GET", op)
            if state.status_code == 200 and str((state.json() or {}).get("status", "")).lower() == "succeeded":
                resp = client._call("GET", op + "/result")
                break
            if str((state.json() or {}).get("status", "")).lower() == "failed":
                raise DeployError("Fabric could not read the pipeline definition.")
            resp = state
        else:
            raise DeployError("Reading the pipeline definition did not finish in time.")
    if resp.status_code != 200:
        raise DeployError(f"Reading the pipeline definition failed (HTTP {resp.status_code}).")
    for part in ((resp.json() or {}).get("definition") or {}).get("parts", []):
        if part.get("path") == "pipeline-content.json":
            return json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
    raise DeployError("The definition has no pipeline-content.json part.")


def _item_names(client: FabricClient, workspace: str) -> dict[str, str]:
    names: dict[str, str] = {}
    url: Optional[str] = f"{FABRIC_API}/workspaces/{workspace}/items"
    while url:
        resp = client._call("GET", url)
        if resp.status_code != 200:
            break
        body = resp.json() or {}
        names.update({i["id"].lower(): i["displayName"] for i in body.get("value", [])})
        token = body.get("continuationToken")
        url = f"{FABRIC_API}/workspaces/{workspace}/items?continuationToken={token}" if token else None
    return names


def _expected_types(conv: Any) -> dict[str, dict[str, tuple[str, ...]]]:
    out: dict[str, dict[str, tuple[str, ...]]] = {}
    for p in getattr(conv, "pipelines", []) or []:
        out[p.source_pipeline.lower()] = {a.source_name.lower(): tuple(c.fabric_type for c in a.components if c.role != "helper") for a in p.activities}
    return out


def _timing(runs: list[RunResult]) -> dict[str, Any]:
    """One run, or a batch (a pipeline another one calls runs once per item: the runs started together are merged, as `verify` does)."""
    if not runs:
        return {}
    acts = []
    for run in runs:
        for a in run.activities:
            acts.append({"name": a.name, "type": a.type, "status": a.status, "duration_ms": a.duration_ms, "started": a.started, "error": a.error,
                         "numbers": {k: v for k, v in activity_metrics(a.type, a.output).items() if not k.endswith("#")}})
    acts.sort(key=lambda x: (x["started"] or "", x["name"]))
    everything = [a for r in runs for a in r.activities]
    failed = next((r for r in runs if not r.ok), None)
    return {"status": "Completed" if all(r.ok for r in runs) else (failed.status if failed else "Failed"), "runs": len(runs),
            "started": min(r.started for r in runs if r.started) if any(r.started for r in runs) else "",
            "ended": max(r.ended for r in runs if r.ended) if any(r.ended for r in runs) else "",
            "failure": next((r.failure for r in runs if r.failure), ""), "job_id": runs[0].job_id, "activities": acts, "metrics": summarize(everything)}


def collect(store: Any, pid: str, workspace: str, make_client: Callable[[], FabricClient], *, workers: int = 8,
            log: Callable[[str], None] = lambda _m: None) -> dict[str, Any]:
    project_dir = store.project_dir(pid)
    scan = scan_source(store.source_dir(pid))
    conv = store.load_conversion(pid) if store.has_conversion(pid) else None
    names = sorted({p.source_pipeline for p in conv.pipelines}) if conv else sorted(f.name for f in scan.of("pipeline") if f.data)
    adf = {f.name.lower(): adf_structure(f.data) for f in scan.of("pipeline") if f.data}
    callees = {c.lower() for s in adf.values() for c in s.called}  # run once per item by their callers: judged as a batch
    expected = _expected_types(conv)
    first = make_client()
    ids = first.list_pipelines(workspace)
    by_id = _item_names(first, workspace)

    def one(name: str) -> dict[str, Any]:
        low, entry = name.lower(), {"pipeline": name}
        item = ids.get(low)
        entry["deployed"] = item is not None
        entry["adf"] = adf.get(low)
        if item is None:
            return entry
        client = make_client()
        log(f"reading {name}")
        try:
            entry["fabric"] = fabric_structure(name, get_definition(client, workspace, item), by_id)
        except DeployError as exc:
            entry["fabric_error"] = str(exc)
        try:
            entry["run"] = _timing(recent_runs(client, workspace, name, window_minutes=10 if low in callees else 0, ids=ids))
        except DeployError as exc:
            entry["run_error"] = str(exc)
        if entry.get("adf") is not None and entry.get("fabric") is not None:
            entry["parity"] = compare(entry["adf"], entry["fabric"], expected.get(low))
        return entry

    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(names) or 1))) as pool:
        rows = list(pool.map(one, names))
    data: dict[str, Any] = {"project": pid, "workspace_known": True, "pipelines": rows,
                            "workspace_pipelines": sorted(by_id.get(v.lower(), k) for k, v in ids.items())}  # every pipeline in the workspace, by display name
    data["progress"] = _progress(project_dir, rows, conv)
    val, ver = _load(project_dir, "validation/validation.yaml") or {}, _load(project_dir, "verification/verification.json") or {}
    by_conv = {p.source_pipeline.lower(): p for p in (conv.pipelines if conv else [])}
    assisted = {n.pipeline.lower() for n in (conv.assisted if conv else [])}
    for r in rows:
        r["confidence"] = conf.assess(r, by_conv.get(r["pipeline"].lower()), (val.get("checks") or []) if isinstance(val, dict) else [],
                                      ver.get(r["pipeline"]) if isinstance(ver, dict) else None, conf.generated_mtime(project_dir, r["pipeline"]),
                                      assisted=r["pipeline"].lower() in assisted)
    data["confidence"] = conf.estate([r["confidence"] for r in rows])
    return data


def _load(project_dir: Path, rel: str) -> Any:
    f = project_dir / rel
    try:
        return json.loads(f.read_text(encoding="utf-8")) if rel.endswith(".json") else yaml.safe_load(f.read_text(encoding="utf-8"))
    except (OSError, ValueError, yaml.YAMLError):
        return None


def _progress(project_dir: Path, rows: list[dict], conv: Any) -> dict[str, Any]:
    ver, val = _load(project_dir, "verification/verification.json") or {}, _load(project_dir, "validation/validation.yaml") or {}
    parity = [r.get("parity") for r in rows if r.get("parity")]
    runs = [r.get("run") for r in rows if r.get("run")]
    return {
        "pipelines": len(rows), "deployed": sum(1 for r in rows if r.get("deployed")),
        "structure_equivalent": sum(1 for p in parity if p.verdict != "differs"), "structure_differs": sum(1 for p in parity if p.verdict == "differs"),
        "ran": len(runs), "last_run_completed": sum(1 for r in runs if r.get("status") == "Completed"),
        "matches_baseline": sum(1 for r in rows if (ver.get(r["pipeline"]) or {}).get("verdict") == "matches"),
        "validation_overall": (val or {}).get("overall") or ((val or {}).get("summary") or {}).get("overall"),
        "conversion_statuses": getattr(conv, "status_counts", None),
    }


# --- rendering -----------------------------------------------------------------------------------------------------------


def _confidence_summary(data: dict[str, Any]) -> list[str]:
    c = data.get("confidence")
    if not c:
        return []
    labels = ", ".join(f"{n} {k}" for k, n in sorted(c["labels"].items()))
    return [f"* Evidence confidence: average **{c['average_score']}/100** ({labels}); **{c['exercised_share']}%** of the {c['activities']} activities ran to success in the latest runs "
            "(the rest sit on branches not taken). This is how much is proven, not a probability that it is correct."]


def _line(n: Node) -> str:
    bits = []
    if n.depends:
        bits.append("after " + ", ".join(f"{p}({'/'.join(c) or '-'})" for p, c in n.depends))
    for label, vals in (("starts", n.calls), ("datasets", n.datasets), ("uses", n.connections), ("runs", n.items)):
        if vals:
            bits.append(f"{label} {', '.join(dict.fromkeys(vals))}")
    return f"{'  ' * n.depth}- `{n.type}` **{n.name}**" + (f" - {'; '.join(bits)}" if bits else "")


def _tree(s: Optional[Structure]) -> list[str]:
    return [_line(n) for n in s.nodes] if s and s.nodes else ["- (no activities)"]


def _ms(v: Any) -> str:
    if v is None:
        return "-"
    return f"{v / 1000:.1f}s" if v >= 1000 else f"{v}ms"


def _duration_ms(run: dict) -> Optional[int]:
    if run.get("started") and run.get("ended"):
        from datetime import datetime
        try:
            return int((datetime.fromisoformat(run["ended"].replace("Z", "+00:00")) - datetime.fromisoformat(run["started"].replace("Z", "+00:00"))).total_seconds() * 1000)
        except ValueError:
            return None
    return None


def _behaviour(rows: list[dict]) -> list[str]:
    """How the pipelines behaved in their latest run, estate-wide: what ran, what moved, what was slow, what did not execute."""
    out = ["", "## Behaviour in the latest Fabric run", "",
           "| Pipeline | Status | Duration | Activities run / defined | Not executed (branch not taken) | Rows read | Rows copied | Slowest activity |", "|---|---|---|---|---|---|---|---|"]
    slow: list[tuple[int, str, str]] = []
    for r in rows:
        run, f = r.get("run"), r.get("fabric")
        if not run:
            out.append(f"| {r['pipeline']} | {'never run' if r.get('deployed') else 'not deployed'} | - | - | - | - | - | - |")
            continue
        ran = {a["name"].lower() for a in run["activities"]}
        defined = [n.name for n in f.nodes] if f else []
        skipped = sorted(n for n in defined if n.lower() not in ran)
        rows_read = sum(m.get("metrics", {}).get("rowsRead", 0) for m in run["metrics"].values())
        rows_copied = sum(m.get("metrics", {}).get("rowsCopied", 0) for m in run["metrics"].values())
        timed = [(a["duration_ms"], a["name"]) for a in run["activities"] if a["duration_ms"] is not None and a["type"] not in ("ForEach", "Until", "IfCondition", "Switch")]
        top = max(timed, default=None)
        slow += [(d, r["pipeline"], n) for d, n in timed]
        out.append(f"| {r['pipeline']} | {run['status']} | {_ms(_duration_ms(run))} | {len(ran)} / {len(defined) or '?'} | "
                   f"{', '.join(skipped) or 'none'} | {rows_read or '-'} | {rows_copied or '-'} | {f'{top[1]} ({_ms(top[0])})' if top else '-'} |")
    out += ["", "Slowest activities across the estate (containers excluded, since they include their children):", ""]
    out += [f"{i}. `{p}` / `{n}`: {_ms(d)}" for i, (d, p, n) in enumerate(sorted(slow, reverse=True)[:8], 1)] or ["(no timings reported)"]
    failed = [(r["pipeline"], a) for r in rows for a in (r.get("run") or {}).get("activities", []) if a["status"] not in ("Succeeded", "Skipped")]
    out += ["", "Failed or cancelled activities:", ""] + ([f"* `{p}` / `{a['name']}`: {a['status']} {a['error']}" for p, a in failed] or ["None."])
    return out


def render(data: dict[str, Any]) -> str:
    rows, prog = data["pipelines"], data["progress"]
    out = [f"# Pipeline report: {data['project']}", "",
           "Read-only: the ADF side is the source JSON, the Fabric side is each deployed pipeline's definition read back from the workspace, "
           "the run is its latest finished run. A matching structure says the pieces are carried; it is not a claim that the pipeline runs correctly "
           "(only a run compared with its baseline says that).", "", "## Progress", "",
           f"* Pipelines: **{prog['pipelines']}**, deployed in the workspace: **{prog['deployed']}**",
           f"* Structure equal to ADF: **{prog['structure_equivalent']}**, differs: **{prog['structure_differs']}**",
           f"* Latest Fabric run completed: **{prog['last_run_completed']}** of {prog['ran']} that have run",
           f"* Matches its ADF baseline (run-and-compare): **{prog['matches_baseline']}**",
           f"* Validation overall: **{prog['validation_overall'] or 'not recorded'}**"] + _confidence_summary(data) + [""]
    out += ["## Overview", "", "| Pipeline | ADF activities | Fabric activities | Depth | Starts | Structure | Latest run | Duration | Confidence |",
            "|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        a, f, p, run = r.get("adf"), r.get("fabric"), r.get("parity"), r.get("run") or {}
        dur = None
        if run.get("started") and run.get("ended"):
            try:
                from datetime import datetime
                dur = int((datetime.fromisoformat(run["ended"].replace("Z", "+00:00")) - datetime.fromisoformat(run["started"].replace("Z", "+00:00"))).total_seconds() * 1000)
            except ValueError:
                dur = None
        out.append(f"| {r['pipeline']} | {len(a.nodes) if a else '-'} | {len(f.nodes) if f else ('not deployed' if not r.get('deployed') else '-')} | "
                   f"{a.max_depth if a else '-'} | {', '.join(a.called) if a and a.called else '-'} | {p.verdict.replace('_', ' ') if p else '-'} | "
                   f"{run.get('status', 'never run' if r.get('deployed') else '-')} | {_ms(dur)} | "
                   f"{r['confidence'].label + ' ' + str(r['confidence'].score) if r.get('confidence') else '-'} |")
    out += _behaviour(rows)
    calls = [(r["pipeline"], n.name, c) for r in rows if r.get("adf") for n in r["adf"].nodes for c in n.calls]
    out += ["", "## Pipeline-to-pipeline calls", ""]
    out += [f"* `{a}` -> `{c}` (activity `{n}`)" for a, n, c in calls] or ["None: no pipeline starts another."]
    for r in rows:
        out += ["", f"## {r['pipeline']}", ""]
        if not r.get("deployed"):
            out += ["Not deployed in the workspace."]
        a, f, p, run = r.get("adf"), r.get("fabric"), r.get("parity"), r.get("run")
        cf = r.get("confidence")
        if cf:
            out += [f"**Confidence: {cf.label} ({cf.score}/100)**" + (f", {cf.executed} of {cf.activities} activities exercised" if cf.activities else ""), "",
                    "| Evidence | Points | Detail |", "|---|---|---|"] + [f"| {e.layer} | {e.points}/{e.maximum} | {e.detail} |" for e in cf.evidence]
            out += [f"* Gap: {g}" for g in cf.gaps] + [f"* Limit: {x}" for x in cf.limits] + [""]
        out += ["**ADF** (source)", ""] + _tree(a) + ["", "**Fabric** (deployed)", ""] + _tree(f)
        if r.get("fabric_error"):
            out += ["", f"Definition not read: {r['fabric_error']}"]
        if p:
            out += ["", f"**Structure: {p.verdict.replace('_', ' ')}**", "", "| Check | Result | Detail |", "|---|---|---|"]
            out += [f"| {c.name} | {'ok' if c.ok else 'DIFFERS'} | {c.detail} |" for c in p.checks]
            out += [f"* Difference: {d}" for d in p.differences] + [f"* Carried differently, as documented: {d}" for d in p.documented[:6]]
        if run:
            out += ["", f"**Latest Fabric run: {run['status']}** ({run['started']} to {run['ended']})"
                    + (f", a batch of {run['runs']} runs started together because another pipeline calls it once per item" if run.get("runs", 1) > 1 else "")
                    + (f" - {run['failure']}" if run.get("failure") else ""), "",
                    "| Activity | Type | Status | Duration | Numbers | Error |", "|---|---|---|---|---|---|"]
            for ac in run["activities"]:
                nums = ", ".join(f"{k} {v}" for k, v in (ac.get("numbers") or {}).items()) or "-"
                out.append(f"| {ac['name']} | {ac['type']} | {ac['status']} | {_ms(ac['duration_ms'])} | {nums} | {ac['error'] or '-'} |")
        elif r.get("deployed"):
            out += ["", "**Latest Fabric run:** none (this pipeline has never run, or is only started by its caller)."]
        if r.get("run_error"):
            out += ["", f"Run history not read: {r['run_error']}"]
    return "\n".join(out) + "\n"


def to_json(data: dict[str, Any]) -> str:
    def conv(o: Any) -> Any:
        if hasattr(o, "__dataclass_fields__"):
            d = asdict(o)
            if isinstance(o, Parity):
                d["verdict"] = o.verdict
            return d
        raise TypeError(type(o))
    return json.dumps(data, default=conv, indent=2)
