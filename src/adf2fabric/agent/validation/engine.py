"""The validation engine: runs the requested stages, then decides the overall status honestly.

Overall status (never better than the evidence):

* FAILED          any required check FAILED, or a source component is missing from the target
* BLOCKED         no failure, but a required check is BLOCKED (something it needs is unbound or unreadable)
* NOT_VALIDATED   no failure or block, but a required check was NOT_RUN (a stage not requested, no authorised environment), or a component
                  is altered / unverified / extra
* MIGRATION_VERIFIED  every required check PASSED including the runtime stage, and every component is matched / transformed / not applicable

Only MIGRATION_VERIFIED sets ``full_success_claimed``. A converter that says "converted", a deployment that returned 200, or a clean
static pass can never produce it by themselves.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from adf2fabric.agent.provisioning.transport import FabricGateway
from adf2fabric.agent.validation import remote, static, views
from adf2fabric.project.provisioning import VerificationReport as ProvVerification
from adf2fabric.project.source import SourceManifest
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore
from adf2fabric.project.validation import (CheckResult, ComponentStatus as S, InventoryCounts, Result as R, Stage, StageSummary, ValidationReport)

ALL_STAGES = ("static", "api", "deployment", "runtime")


def _optional(fn):
    try:
        return fn()
    except (ProjectNotFoundError, ValueError, OSError):
        return None


def load_inputs(store: ProjectStore, pid: str) -> static.Inputs:
    pdir = store.project_dir(pid)
    src = views.scan_source(store.source_dir(pid))
    tgt = views.scan_target(store.conversion_dir(pid))
    paired, unpaired = views.pair_pipelines(src, tgt)
    return static.Inputs(
        project_id=pid, src=src, tgt=tgt, paired=paired, unpaired=unpaired,
        manifest=_optional(lambda: store.load_source_manifest(pid)),
        discovery=_optional(lambda: store.load_adf_semantic_discovery(pid)) if store.has_adf_semantic_discovery(pid) else None,
        workloads=_optional(lambda: store.load_workload_manifest(pid)) if store.has_workload_manifest(pid) else None,
        provisioning=_optional(lambda: store.load_provisioning(pid, "verification", ProvVerification)) if store.has_provisioning(pid, "verification") else None,
        source_dir=store.source_dir(pid))


def _stage_summary(stage: Stage, checks: list[CheckResult]) -> StageSummary:
    mine = [x for x in checks if x.stage is stage and x.required]  # informational checks never decide a stage
    n = {r: sum(1 for x in mine if x.result is r) for r in R}
    if n[R.FAIL]:
        status = R.FAIL
    elif n[R.BLOCKED]:
        status = R.BLOCKED
    elif not mine or n[R.NOT_RUN] and not n[R.PASS]:
        status = R.NOT_RUN
    elif n[R.NOT_RUN]:
        status = R.NOT_RUN  # some of the stage did not run: it is not a pass
    else:
        status = R.PASS
    return StageSummary(stage=stage, status=status, passed=n[R.PASS], failed=n[R.FAIL], blocked=n[R.BLOCKED], not_run=n[R.NOT_RUN])


def overall(checks: list[CheckResult], components, stages_requested: set[str]) -> tuple[str, list[str]]:
    req = [x for x in checks if x.required]
    reasons: list[str] = []
    failed = [x for x in req if x.result is R.FAIL]
    missing = [c for c in components if c.status is S.missing]
    if failed or missing:
        if failed:
            reasons.append(f"{len(failed)} required check(s) FAILED: " + ", ".join(sorted({x.id for x in failed})[:6]))
        if missing:
            reasons.append(f"{len(missing)} source component(s) are missing from the target.")
        return "FAILED", reasons
    blocked = [x for x in req if x.result is R.BLOCKED]
    if blocked:
        return "BLOCKED", [f"{len(blocked)} required check(s) are BLOCKED: " + ", ".join(sorted({x.id for x in blocked})[:6])]
    notrun = [x for x in req if x.result is R.NOT_RUN]
    weak = [c for c in components if c.status in (S.altered, S.unverified, S.extra)]
    stage_gap = [s for s in ALL_STAGES if s not in stages_requested]
    if notrun or weak or stage_gap or not req:
        if notrun:
            reasons.append(f"{len(notrun)} required check(s) were NOT RUN: " + ", ".join(sorted({x.id for x in notrun})[:6]))
        if weak:
            reasons.append(f"{len(weak)} component(s) are altered, unverified or extra and have not been reviewed.")
        if stage_gap:
            reasons.append("Stages not requested: " + ", ".join(stage_gap) + ".")
        if not req:
            reasons.append("There was nothing to validate.")
        return "NOT_VALIDATED", reasons
    if not any(x.stage is Stage.runtime and x.result is R.PASS for x in req):
        return "NOT_VALIDATED", ["No runtime check passed: generated files and API answers are not evidence that the pipelines run correctly."]
    return "MIGRATION_VERIFIED", ["Every required check passed across static, API, deployment and runtime stages."]


def validate(store: ProjectStore, pid: str, *, stages: set[str], fab: Optional[FabricGateway] = None, workspace: Optional[str] = None,
             now: Optional[datetime] = None) -> ValidationReport:
    now = now or datetime.now(timezone.utc)
    inp = load_inputs(store, pid)
    c = static.Collector()
    if "static" in stages:
        c = static.run_static(inp)
    remote.api_and_deployment(inp, c, fab, workspace, "api" in stages, "deployment" in stages)
    remote.runtime(inp, c, store.project_dir(pid), "runtime" in stages)
    checks = c.checks
    comps = sorted(c.components.values(), key=lambda x: x.id)
    by_status: dict[str, int] = {}
    for x in comps:
        by_status[x.status.value] = by_status.get(x.status.value, 0) + 1
    tgt_count = sum(1 for x in comps if x.status not in (S.missing,))
    status, reasons = overall(checks, comps, stages)
    return ValidationReport(
        project_id=pid, created_at=now,
        scope={"stages_requested": ",".join(sorted(stages)), "workspace": workspace or "(none)",
               "source": "unreadable" if inp.src.root is None else "read", "runtime_evidence": "requested" if "runtime" in stages else "not requested"},
        stages=[_stage_summary(s, checks) for s in Stage], checks=checks, components=comps,
        inventory=InventoryCounts(source_components=sum(1 for x in comps if x.status is not S.extra), target_components=tgt_count, by_status=dict(sorted(by_status.items()))),
        overall=status, overall_reasons=reasons, full_success_claimed=status == "MIGRATION_VERIFIED")
