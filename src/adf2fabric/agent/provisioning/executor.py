"""Apply an APPROVED plan: idempotent, ordered, retried with care, journaled step by step, never destructive.

Guarantees, each enforced here and covered by tests:

* Nothing is written without an approval that names the step AND the plan hash; and not if the world changed since the plan:
  a fresh plan is built from freshly read state and must hash the same, or apply refuses (re-plan, re-approve).
* Every create re-reads the live state first (the handler's ``apply`` does it): an existing resource is reused, never overwritten
  or duplicated, whatever the plan said.
* A failure never stops independent steps; the steps that depend on it are skipped, not attempted.
* A retry of a write is never blind. After an ambiguous failure (5xx, timeout, dropped connection) the live state is re-read: if
  the resource now exists the step is recovered, not repeated.
* The journal (``run.yaml``) is replaced atomically after every step, so a crash leaves a truthful record and a re-run continues.
* Nothing is deleted or modified: the only operations are creates and additive configuration (assign, add, provision).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Callable, Optional

from adf2fabric.agent.provisioning.handlers import HANDLERS, Ctx
from adf2fabric.agent.provisioning.planner import make_plan_and_state
from adf2fabric.agent.provisioning.transport import ArmGateway, FabricGateway, ProvisionError
from adf2fabric.connections.redaction import redact
from adf2fabric.project.provisioning import (Action, ProvisioningApproval, ProvisioningPlan, ProvisioningRun, ProvisionStep, StepResult, StepStatus, TargetSpec)


class ApplyRefused(Exception):
    """Apply did not start. Nothing was written."""


def check_approval(plan: ProvisioningPlan, approval: Optional[ProvisioningApproval]) -> set[str]:
    if approval is None:
        raise ApplyRefused("There is no approval. Review the plan, then run provision-approve.")
    if approval.plan_hash != plan.plan_hash:
        raise ApplyRefused("The approval is for a different plan (the plan changed after it was approved). Run provision-approve again.")
    known = {s.id for s in plan.steps}
    unknown = sorted(set(approval.step_ids) - known)
    if unknown:
        raise ApplyRefused(f"The approval names steps that are not in the plan: {', '.join(unknown)}.")
    return set(approval.step_ids)


def drift(plan: ProvisioningPlan, fresh: ProvisioningPlan) -> list[str]:
    old = {s.id: (s.action.value, s.request) for s in plan.steps}
    new = {s.id: (s.action.value, s.request) for s in fresh.steps}
    out = [f"{i}: was {old[i][0]}, now {new[i][0] if i in new else 'gone'}" for i in old if new.get(i) != old[i]]
    out += [f"{i}: new in the current state ({new[i][0]})" for i in new if i not in old]
    return out


def apply_plan(store, project_id: str, plan: ProvisioningPlan, approval: Optional[ProvisioningApproval], spec: TargetSpec, fab: FabricGateway,
               arm: Optional[ArmGateway] = None, *, apply: bool = False, max_attempts: int = 3, sleep: Callable[[float], None] = time.sleep,
               now: Optional[datetime] = None) -> ProvisioningRun:
    now = now or datetime.now(timezone.utc)
    approved = check_approval(plan, approval)
    fresh, fresh_state = make_plan_and_state(store, project_id, spec, fab, arm, now, sleep)
    if fresh.plan_hash != plan.plan_hash:
        raise ApplyRefused("The target changed since the plan was made, so the approval no longer describes what would happen: "
                           + "; ".join(drift(plan, fresh)[:6]) + ". Run provision-plan and provision-approve again.")
    previous = None
    if store.has_provisioning(project_id, "run"):
        previous = store.load_provisioning(project_id, "run", ProvisioningRun)
    run = ProvisioningRun(plan_hash=plan.plan_hash, dry_run=not apply, started_at=now, interrupted=bool(previous and previous.finished_at is None))
    ctx = Ctx(spec=spec, state=fresh_state, fab=fab, arm=arm, sleep=sleep)
    for s in plan.steps:  # reused resources are real ids that later steps (an environment's pool) may need
        if s.action is Action.reuse and s.existing is not None and s.existing.id:
            ctx.created[s.id] = s.existing.id
    results: dict[str, StepResult] = {}

    def journal() -> None:
        run.results = list(results.values())
        store.save_provisioning(project_id, "run", run)

    for step in plan.steps:
        res = StepResult(step_id=step.id, status=StepStatus.skipped)
        results[step.id] = res
        if step.action in (Action.reuse, Action.delegate, Action.blocked, Action.conflict, Action.unknown):
            res.detail = {Action.reuse: "exists; nothing to do", Action.delegate: f"delegated to {step.delegated_to}", Action.blocked: "blocked: " + "; ".join(step.blockers[:2]),
                          Action.conflict: "conflict: not changed", Action.unknown: "state unknown: nothing was assumed"}[step.action]
            journal()
            continue
        if step.id not in approved:
            res.detail = "not approved"
            journal()
            continue
        unmet = [d for d in step.depends_on if d in results and results[d].status not in (StepStatus.applied, StepStatus.verified)
                 and not _satisfied_without_change(plan, d)]
        if unmet:
            res.detail = "skipped because a step it depends on did not complete: " + ", ".join(unmet)
            journal()
            continue
        if not apply:
            res.detail = f"dry run: would {step.action.value} ({step.endpoint})"
            journal()
            continue
        res.started_at = datetime.now(timezone.utc)
        _run_step(ctx, step, res, max_attempts, sleep)
        res.finished_at = datetime.now(timezone.utc)
        if res.status is StepStatus.applied and res.resource_id:
            ctx.created[step.id] = res.resource_id
        journal()
    run.finished_at = datetime.now(timezone.utc)
    journal()
    return run


def _satisfied_without_change(plan: ProvisioningPlan, step_id: str) -> bool:
    s = next((x for x in plan.steps if x.id == step_id), None)
    return s is not None and s.action in (Action.reuse, Action.delegate)


def _run_step(ctx: Ctx, step: ProvisionStep, res: StepResult, max_attempts: int, sleep: Callable[[float], None]) -> None:
    handler = HANDLERS[step.type]
    for attempt in range(1, max_attempts + 1):
        res.attempts = attempt
        try:
            out = handler.apply(ctx, step)
            res.status = StepStatus.applied
            res.resource_id = out.resource_id
            res.detail = redact(out.detail + ("" if out.created else " (reused)") + " Not yet verified.")
            return
        except ProvisionError as exc:
            if not exc.ambiguous:  # a clear refusal (403, 400, 409, a failed operation): retrying will not change it
                res.status, res.detail = StepStatus.failed, redact(str(exc))
                return
            try:  # the write may have happened: look before doing anything again
                found = handler.find(ctx, step)
            except ProvisionError as read_exc:
                res.status, res.detail = StepStatus.failed, redact(f"{exc} The live state could not be re-read ({read_exc}), so the write was not repeated; check the target.")
                return
            if found:
                res.status, res.resource_id = StepStatus.applied, found
                res.detail = "found after an ambiguous failure, so it was not created again. Not yet verified."
                return
            if attempt == max_attempts:
                res.status, res.detail = StepStatus.failed, redact(f"{exc} Gave up after {attempt} attempts; nothing was found in the target.")
                return
            sleep(2.0 * (2 ** (attempt - 1)))
    res.status = StepStatus.failed
