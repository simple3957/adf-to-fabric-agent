"""The migration results after deploy: for each pipeline, whether it is deployed, how its latest Fabric run went, what it was compared
with, and what it moved (rows and files, Fabric beside ADF), activity by activity. Read-only, built from the project's own records:
the deploy record, the run-and-compare record (``verify``) and the output checks. Nothing here calls Fabric or ADF.

The ADF figures are the baseline run's own numbers, kept by ``verify`` since this report exists. A run compared before that keeps only
which facts matched, so its ADF column is empty and the row says so; reading the latest runs again (``verify``, no new run) fills it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.deployment import output_check
from adf2fabric.agent.deployment import record as deploy_record
from adf2fabric.agent.deployment import verify

MOVED = ("rowsRead", "rowsCopied", "filesRead", "filesWritten")
RESULT = {"matches": "Matches", "partial": "Partly compared", "differs": "Differs", "no_baseline": "Nothing to compare with",
          "failed": "Run failed", "same_failure": "Failed, as in ADF", "not_run": "Not run"}


def _when(at: Any) -> str:
    """An ISO time (a run) or epoch seconds (a deploy) as ISO UTC; '' when there is none."""
    if isinstance(at, (int, float)) and at:
        return datetime.fromtimestamp(float(at), timezone.utc).isoformat(timespec="seconds")
    return str(at or "")


def _moved(activities: Optional[dict]) -> Optional[dict[str, int]]:
    """Rows and files the Copy activities of one run read and wrote, summed. None when the run is not known."""
    if activities is None:
        return None
    out = {k: 0 for k in MOVED}
    for a in activities.values():
        for k in MOVED:
            v = (a.get("metrics") or {}).get(k)
            if isinstance(v, (int, float)):
                out[k] += int(v)
    return out


def _activities(entry: dict) -> list[dict]:
    """Each activity of the run beside the baseline's: status, runs, rows and files on both sides, and the result of the comparison."""
    fab, base = entry.get("activities") or {}, entry.get("baseline_activities")
    diffs: dict[str, list[dict]] = {}
    for d in entry.get("differences") or []:
        diffs.setdefault(d.get("activity", ""), []).append(d)
    only_base, only_fab = set(entry.get("baseline_only") or []), set(entry.get("fabric_only") or [])
    compared = bool(entry.get("baseline")) and entry.get("verdict") in ("matches", "partial", "differs")
    rows = []
    for name in sorted(set(fab) | set(base or {}) | only_base, key=str.lower):
        f, b = fab.get(name) or {}, (base or {}).get(name) or {}
        if name in diffs:
            result = "differs: " + "; ".join(f"{d.get('fact')} ADF {d.get('baseline')} / Fabric {d.get('fabric')}" for d in diffs[name])
        elif name in only_base:
            result = "ran in the baseline only"
        elif name in only_fab:
            result = "ran in Fabric only"
        elif compared and name in fab:
            result = "matches"
        else:
            result = ""
        row = {"activity": name, "type": f.get("type") or b.get("type") or "", "fabric_status": f.get("status", ""),
               "baseline_status": b.get("status", "") if base is not None else "", "fabric_runs": f.get("runs"),
               "baseline_runs": b.get("runs") if base is not None else None, "result": result}
        for k in MOVED:
            row[f"fabric_{k}"] = (f.get("metrics") or {}).get(k)
            row[f"baseline_{k}"] = (b.get("metrics") or {}).get(k) if base is not None else None
        rows.append(row)
    return rows


def build(project_dir: Path, pipelines: list[str], workspace: str = "") -> dict[str, Any]:
    """The results of every converted pipeline in ``pipelines`` (source names). ``workspace``, when given, counts only deploys to it."""
    deployed = {k.lower(): v for k, v in (deploy_record.load(project_dir).get("pipelines") or {}).items() if isinstance(v, dict)}
    runs = {k.lower(): v for k, v in verify.current_record(project_dir, workspace).items() if isinstance(v, dict)}
    checks = {k.lower(): v for k, v in output_check.load(project_dir).items() if isinstance(v, dict)}
    rows = []
    for name in sorted(pipelines, key=str.lower):
        d = deployed.get(name.lower()) or {}
        here = bool(d) and (not workspace or str(d.get("workspace", "")).lower() == workspace.lower())
        r = runs.get(name.lower()) or {}
        verdict = r.get("verdict") or "not_run"
        fab, base = _moved(r.get("activities")) if r else None, _moved(r.get("baseline_activities"))
        acts = r.get("activities") or {}
        oc = [{"name": n, "verdict": c.get("verdict"), "source_rows": (c.get("source_summary") or {}).get("rows"),
               "target_rows": (c.get("target_summary") or {}).get("rows"), "checked_at": c.get("checked_at")}
              for n, c in sorted((checks.get(name.lower()) or {}).items())]
        rows.append({
            "pipeline": name,
            "deployed": here and bool(d.get("ok")), "deploy_failed": here and not d.get("ok"), "deployed_at": _when(d.get("at")) if here else "",
            "deploy_action": d.get("action", "") if here else "", "deploy_detail": d.get("detail", "") if here and not d.get("ok") else "",
            "run_status": r.get("fabric_status", ""), "ran_at": r.get("ran_at", ""), "stale_run": bool(r.get("stale_run")),
            "baseline": r.get("baseline", ""), "baseline_at": r.get("baseline_at", ""), "baseline_chosen": r.get("baseline_choice") == "fabric",
            "verdict": verdict, "result": RESULT.get(verdict, verdict), "detail": r.get("detail", ""),
            "checks_matched": int(r.get("matched") or 0), "differences": len(r.get("differences") or []),
            "activities_total": len(acts), "activities_succeeded": sum(1 for a in acts.values() if a.get("status") == "Succeeded"),
            "fabric_moved": fab, "baseline_moved": base, "baseline_figures": base is not None or not r.get("baseline"),
            "output_checks": oc, "activities": _activities(r) if r else [],
        })
    both = [x for x in rows if x["fabric_moved"] and x["baseline_moved"] and not x["stale_run"]]  # like for like: runs whose baseline figures are known
    summary = {
        "pipelines": len(rows), "deployed": sum(x["deployed"] for x in rows), "deploy_failed": sum(x["deploy_failed"] for x in rows),
        "ran": sum(1 for x in rows if x["run_status"] and not x["stale_run"]),
        "succeeded": sum(1 for x in rows if x["run_status"] == "Completed" and not x["stale_run"]),
        "matches": sum(1 for x in rows if x["verdict"] == "matches"),
        "compared_with_adf": sum(1 for x in rows if x["verdict"] in ("matches", "partial", "differs") and x["baseline"] == "adf"),
        "compared_with_fabric": sum(1 for x in rows if x["verdict"] in ("matches", "partial", "differs") and x["baseline"] == "fabric"),
        "partial": sum(1 for x in rows if x["verdict"] == "partial"), "differs": sum(1 for x in rows if x["verdict"] == "differs"),
        "failed": sum(1 for x in rows if x["verdict"] in ("failed", "same_failure")),
        "not_run": sum(1 for x in rows if x["verdict"] in ("not_run", "no_baseline")),
        "checks_matched": sum(x["checks_matched"] for x in rows), "differences_total": sum(x["differences"] for x in rows),
        "fabric_moved": {k: sum(x["fabric_moved"][k] for x in rows if x["fabric_moved"] and not x["stale_run"]) for k in MOVED},
        "compared_moved": {"pipelines": len(both), "fabric": {k: sum(x["fabric_moved"][k] for x in both) for k in MOVED},
                           "baseline": {k: sum(x["baseline_moved"][k] for x in both) for k in MOVED}},
        "baseline_figures_missing": sum(1 for x in rows if not x["baseline_figures"]),
        "output_checks": sum(len(x["output_checks"]) for x in rows),
        "output_checks_matching": sum(1 for x in rows for c in x["output_checks"] if c["verdict"] == output_check.MATCHES),
    }
    return {"summary": summary, "pipelines": rows}
