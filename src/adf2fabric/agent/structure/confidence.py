"""How much of each pipeline's migration is actually proven, and what is not.

A pipeline earns points from independent layers of evidence, each read from a record that layer wrote (never from the converter's own success
message). The score is **evidence coverage**, not a probability that the pipeline is correct: it says how much has been shown and names what has
not (``gaps``) and what a run can never show (``limits``). Caps stop a pipeline that proves little from looking proven (three Wait activities
that pass prove nothing about data movement).

    conversion  15  every activity converted (10 when some carry documented differences, 0 when any needs a person or is blocked)
    validation  15  no failed or blocked static/deployment check on its components (7 when only blocked)
    structure   20  the deployed Fabric twin has the same activities, nesting, dependencies, calls, datasets and connections as ADF
    run         15  the latest Fabric run completed and is newer than the generated file (5 when it completed but is older)
    baseline    15  its run-and-compare matched the ADF baseline (7 partial, 3 when there was no baseline)
    coverage    20  the share of its activities that ran, in that run, to success

Labels: High (90 and above), Good (75), Partial (50), Low (below 50), Not verified (it has never run). Only the validation engine's overall status may
say the migration is verified; this says how well each pipeline is evidenced.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

TRIVIAL = frozenset({"wait", "setvariable", "appendvariable"})  # a pipeline made only of these moves no data and exercises no logic worth proving


@dataclass
class Evidence:
    layer: str
    points: int
    maximum: int
    detail: str


@dataclass
class Confidence:
    pipeline: str
    score: int
    label: str
    evidence: list[Evidence] = field(default_factory=list)
    gaps: list[str] = field(default_factory=list)  # something that could be shown and has not been
    limits: list[str] = field(default_factory=list)  # something the evidence cannot show
    coverage: Optional[float] = None  # share of activities that ran to success in the latest run (0..1)
    activities: int = 0
    executed: int = 0


def label_for(score: int, ran: bool) -> str:
    if not ran:
        return "Not verified"
    return "High" if score >= 90 else "Good" if score >= 75 else "Partial" if score >= 50 else "Low"


def _checks_for(checks: list[dict], name: str) -> list[dict]:
    prefix = f"pipeline:{name}"
    return [c for c in checks if str(c.get("component") or "").lower().startswith(prefix.lower())
            and (len(str(c["component"])) == len(prefix) or str(c["component"])[len(prefix)] in "/:")]


def assess(entry: dict[str, Any], conv_pipeline: Any, checks: list[dict], verification: Optional[dict], generated_at: Optional[float],
           assisted: bool = False) -> Confidence:
    """``entry``: a row of the pipeline report. ``conv_pipeline``: its conversion record. ``checks``: the validation checks.
    ``verification``: its run-and-compare record. ``generated_at``: the mtime of its generated file (epoch seconds)."""
    name = entry["pipeline"]
    c = Confidence(name, 0, "Not verified")
    caps: list[tuple[int, str]] = []

    # --- conversion -----------------------------------------------------------------------------------------------------------
    acts = list(getattr(conv_pipeline, "activities", []) or [])
    statuses = [a.status.value for a in acts]
    needs = [a.source_name for a in acts if a.status.value in ("manual_required", "blocked")]
    diffs = [a.source_name for a in acts if a.status.value == "converted_with_differences"]
    if conv_pipeline is None:
        c.evidence.append(Evidence("conversion", 0, 15, "no conversion record"))
        c.gaps.append("It has no conversion record: run `convert`.")
    elif needs:
        c.evidence.append(Evidence("conversion", 0, 15, f"{len(needs)} activity(ies) need a person or are blocked: {', '.join(needs[:4])}"))
        c.gaps.append(f"Resolve the activities that were not converted: {', '.join(needs[:6])}.")
        caps.append((50, "an activity was not converted"))
    elif diffs:
        c.evidence.append(Evidence("conversion", 10, 15, f"all {len(acts)} converted, {len(diffs)} with documented differences"))
        c.limits.append(f"Differences the converter documents: {', '.join(diffs[:4])}.")
    else:
        c.evidence.append(Evidence("conversion", 15, 15, f"all {len(acts)} activities converted without differences"))
    if assisted:
        caps.append((85, "it was converted from a model-assisted edit of the source"))
        c.limits.append("It was converted from a model-assisted edit of its ADF source that a person reviewed; the original file differs, and only a run compared with "
                        "a baseline shows the edit kept its behaviour.")
    inferred = [a.source_name for a in acts if getattr(a, "inferred_elements", None)]
    if inferred:
        c.limits.append(f"Elements that are not in the ADF source (inferred): {', '.join(inferred[:4])}.")
        caps.append((85, "it contains inferred elements"))

    # --- validation (this pipeline's own components) --------------------------------------------------------------------------------
    mine = _checks_for(checks, name)
    if not checks:
        c.evidence.append(Evidence("validation", 0, 15, "validation has not been recorded"))
        c.gaps.append("Run `validate` (static, API and deployment stages).")
    else:
        failed = [x for x in mine if x.get("result") == "FAIL"]
        blocked = [x for x in mine if x.get("result") == "BLOCKED"]
        if failed:
            c.evidence.append(Evidence("validation", 0, 15, f"{len(failed)} check(s) failed: {', '.join(sorted({x['id'] for x in failed})[:3])}"))
            c.gaps.append("Fix the failed validation checks: " + "; ".join(f"{x['id']} ({str(x.get('summary'))[:70]})" for x in failed[:3]))
        elif blocked:
            c.evidence.append(Evidence("validation", 7, 15, f"{len(blocked)} check(s) blocked: {', '.join(sorted({x['id'] for x in blocked})[:3])}"))
            c.gaps.append("Unblock: " + "; ".join(f"{x['id']} ({str(x.get('summary'))[:70]})" for x in blocked[:3]))
        else:
            c.evidence.append(Evidence("validation", 15, 15, f"{len(mine)} check(s) on its components, none failed or blocked"))

    # --- structure parity with the deployed twin ------------------------------------------------------------------------------------
    parity = entry.get("parity")
    if not entry.get("deployed"):
        c.evidence.append(Evidence("structure", 0, 20, "not deployed in the workspace"))
        c.gaps.append("Deploy it, then re-run the report.")
    elif parity is None:
        c.evidence.append(Evidence("structure", 0, 20, f"the deployed definition could not be read ({entry.get('fabric_error', 'unknown')})"))
    elif parity.verdict == "differs":
        c.evidence.append(Evidence("structure", 0, 20, "differs from ADF: " + "; ".join(parity.differences[:3])))
        c.gaps.append("Reconcile the deployed pipeline with the ADF structure.")
    else:
        c.evidence.append(Evidence("structure", 20, 20, f"same activities, nesting, dependencies, calls and connections ({len(parity.documented)} documented difference(s))"))

    # --- the latest run --------------------------------------------------------------------------------------------------------------
    run = entry.get("run") or {}
    fab = entry.get("fabric")
    defined = [n.name for n in fab.nodes] if fab else []
    c.activities = len(defined)
    ran = bool(run)
    if not ran:
        c.evidence.append(Evidence("run", 0, 15, "it has never run in Fabric (or only runs when its caller starts it)"))
        c.gaps.append("Run it in Fabric (`verify --apply`).")
    elif run.get("status") != "Completed":
        c.evidence.append(Evidence("run", 0, 15, f"the latest run ended {run.get('status')}: {str(run.get('failure'))[:100]}"))
        c.gaps.append("Find out why the latest run did not complete.")
        caps.append((50, "its latest run did not complete"))
    else:
        stale = False
        if generated_at and run.get("ended"):
            from datetime import datetime, timezone
            try:
                ended = datetime.fromisoformat(run["ended"].replace("Z", "+00:00"))
                ended = ended if ended.tzinfo else ended.replace(tzinfo=timezone.utc)
                stale = ended.timestamp() < generated_at
            except ValueError:
                stale = False
        if stale:
            c.evidence.append(Evidence("run", 5, 15, "completed, but before the generated file last changed"))
            c.gaps.append("The run is older than the generated file: deploy the current file and run again.")
        else:
            c.evidence.append(Evidence("run", 15, 15, f"completed in {run.get('runs', 1)} run(s)" if run.get("runs", 1) > 1 else "the latest run completed"))

    # --- baseline ----------------------------------------------------------------------------------------------------------------------
    v = verification or {}
    verdict = v.get("verdict")
    if verdict == "matches":
        c.evidence.append(Evidence("baseline", 15, 15, f"{v.get('matched', 0)} fact(s) matched the {v.get('baseline') or 'earlier'} run"))
    elif verdict == "partial":
        c.evidence.append(Evidence("baseline", 7, 15, "only part of it could be compared with the baseline"))
        c.gaps.append("Not every activity could be compared with the baseline.")
    elif verdict == "no_baseline":
        c.evidence.append(Evidence("baseline", 3, 15, "nothing earlier to compare with"))
        c.gaps.append("Run it in ADF too (or run it twice in Fabric) to get a baseline.")
    elif verdict in ("differs", "failed"):
        c.evidence.append(Evidence("baseline", 0, 15, f"its run differs from the baseline ({len(v.get('differences') or [])} difference(s))"))
        c.gaps.append("Explain the differences against the ADF baseline.")
        caps.append((50, "it differs from its baseline"))
    else:
        c.evidence.append(Evidence("baseline", 0, 15, "no run-and-compare record"))
        c.gaps.append("Run `verify` to compare it with the ADF baseline.")

    # --- coverage: how much of it actually ran -----------------------------------------------------------------------------------------
    if ran and defined:
        ok_names = {a["name"].lower() for a in run.get("activities", []) if a["status"] == "Succeeded"}
        executed = [n for n in defined if n.lower() in ok_names]
        c.executed, c.coverage = len(executed), len(executed) / len(defined)
        skipped = sorted(n for n in defined if n.lower() not in ok_names)
        c.evidence.append(Evidence("coverage", round(20 * c.coverage), 20, f"{len(executed)} of {len(defined)} activities ran to success"))
        if skipped:
            c.gaps.append(f"Never exercised (a branch not taken, or skipped): {', '.join(skipped[:6])}"
                          + (f" and {len(skipped) - 6} more" if len(skipped) > 6 else "") + ". Run it with inputs that take those paths.")
    else:
        c.evidence.append(Evidence("coverage", 0, 20, "nothing ran"))

    # --- what a run cannot show / caps -------------------------------------------------------------------------------------------------
    types = {n.type.lower() for n in (entry.get("adf").nodes if entry.get("adf") else [])}
    if types and types <= TRIVIAL:
        caps.append((60, "it only holds Wait/variable activities: it moves no data"))
        c.limits.append("It only holds Wait and variable activities, so a pass proves little about the migration.")
    if any(w.code == "dataset_type_not_live_verified" for a in acts for w in a.warnings):
        caps.append((90, "a dataset type has not run in Fabric"))
        c.limits.append("A dataset type it uses has not been run in Fabric by this tool.")
    if any(w.code == "connector_unverified" for a in acts for w in a.warnings):
        caps.append((90, "a connector is unverified"))
        c.limits.append("A connector it uses is not confirmed in Microsoft's connector list.")
    if getattr(conv_pipeline, "triggers", None):
        caps.append((90, "its trigger has never fired in Fabric"))
        c.limits.append("Its trigger or schedule was created disabled and has never fired in Fabric.")
    c.limits.append("The run used the pipeline's default parameters and the data present at the time; row counts and hashes are compared, not every value.")

    raw = sum(e.points for e in c.evidence)
    cap = min((x for x, _ in caps), default=100)
    c.score = min(raw, cap)
    if cap < raw:
        c.limits.append("Score capped at " + str(cap) + ": " + "; ".join(r for x, r in caps if x == cap) + ".")
    c.label = label_for(c.score, ran and run.get("status") == "Completed")
    return c


def estate(rows: list[Confidence]) -> dict[str, Any]:
    total = sum(r.activities for r in rows)
    ran = sum(r.executed for r in rows)
    counts: dict[str, int] = {}
    for r in rows:
        counts[r.label] = counts.get(r.label, 0) + 1
    return {"pipelines": len(rows), "average_score": round(sum(r.score for r in rows) / len(rows)) if rows else 0, "labels": counts,
            "activities": total, "activities_exercised": ran, "exercised_share": round(100 * ran / total) if total else 0}


def generated_mtime(project_dir: Path, name: str) -> Optional[float]:
    f = project_dir / "conversion" / "pipelines" / f"{name}.DataPipeline" / "pipeline-content.json"
    try:
        return f.stat().st_mtime
    except OSError:
        return None
