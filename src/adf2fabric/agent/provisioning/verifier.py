"""Verification: re-read the ACTUAL state and compare it with the plan. Nothing here writes.

This is the only place that may say a resource exists. A step the executor marked ``applied`` stays "applied, not verified"
until it is read back here. A resource the plan expected (created, configured or reused) that is not found is a ``mismatch``,
whatever the apply step reported. Delegated resources are only reported (present or not): provisioning does not own them.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from adf2fabric.agent.provisioning.handlers import HANDLERS, Ctx
from adf2fabric.agent.provisioning.state import find_workspace
from adf2fabric.agent.provisioning.transport import ArmGateway, FabricGateway, ProvisionError
from adf2fabric.project.provisioning import (Action, ProvisioningPlan, ProvisioningRun, StepStatus, TargetSpec, VerificationReport, VerifiedResource)


def verify_plan(plan: ProvisioningPlan, spec: TargetSpec, fab: FabricGateway, arm: Optional[ArmGateway] = None,
                run: Optional[ProvisioningRun] = None, now: Optional[datetime] = None, sleep=None,
                approved: Optional[set] = None) -> VerificationReport:
    """``approved``: when given, create/configure steps outside it were never meant to happen and are reported ``skipped``, not as missing."""
    now = now or datetime.now(timezone.utc)
    ctx = Ctx(spec=spec, state=_empty_state(now), fab=fab, arm=arm, sleep=sleep)
    ws, why = find_workspace(fab, spec)
    if ws is not None:
        ctx.workspace_id = str(ws["id"])
    results: list[VerifiedResource] = []
    ran = {r.step_id: r for r in (run.results if run else [])}
    for step in plan.steps:
        if step.action in (Action.blocked, Action.conflict, Action.unknown):
            continue  # nothing was supposed to exist as a result of this step
        handler = HANDLERS[step.type]
        if approved is not None and step.action in (Action.create, Action.configure) and step.id not in approved:
            results.append(VerifiedResource(step_id=step.id, status=StepStatus.skipped, detail="not approved, so not expected to exist"))
            continue
        if step.action is Action.delegate:
            try:
                found = handler.find(ctx, step) if (ctx.workspace_id or step.type.value == "connection") else None
            except ProvisionError as exc:
                results.append(VerifiedResource(step_id=step.id, status=StepStatus.skipped, detail=f"delegated; could not read: {exc}"))
                continue
            results.append(VerifiedResource(step_id=step.id, status=StepStatus.verified if found else StepStatus.skipped, resource_id=found,
                                            detail="delegated; present" if found else f"delegated to {step.delegated_to}; not present yet"))
            continue
        r = ran.get(step.id)
        if r is not None and r.status in (StepStatus.failed, StepStatus.skipped) and step.action in (Action.create, Action.configure):
            results.append(VerifiedResource(step_id=step.id, status=StepStatus.mismatch if r.status is StepStatus.failed else StepStatus.skipped,
                                            detail=f"not applied ({r.status.value}): {r.detail}"))
            continue
        if ctx.workspace_id is None and step.type.value not in ("azure_capacity", "workspace"):
            results.append(VerifiedResource(step_id=step.id, status=StepStatus.mismatch, detail="the workspace does not exist, so this cannot exist"))
            continue
        results.append(handler.verify(ctx, step))
    expected = [x for x in results if not x.detail.startswith(("delegated", "not approved"))]
    return VerificationReport(plan_hash=plan.plan_hash, verified_at=now, results=results,
                              all_verified=bool(expected) and all(x.status is StepStatus.verified for x in expected))


def _empty_state(now):
    from adf2fabric.project.provisioning import TargetState

    return TargetState(read_at=now)


def mark_run(run: ProvisioningRun, report: VerificationReport) -> ProvisioningRun:
    """Upgrade ``applied`` results to ``verified`` / ``mismatch`` from the report. Nothing else changes."""
    by = {v.step_id: v for v in report.results}
    for r in run.results:
        v = by.get(r.step_id)
        if r.status is StepStatus.applied and v is not None:
            r.status = StepStatus.verified if v.status is StepStatus.verified else StepStatus.mismatch
            r.detail = (r.detail.replace(" Not yet verified.", "") + f" Verified: {v.detail}") if r.status is StepStatus.verified else r.detail + f" MISMATCH: {v.detail}"
    return run
