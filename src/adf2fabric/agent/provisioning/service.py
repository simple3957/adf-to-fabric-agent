"""The four provisioning stages as separate functions: plan, approve, apply, verify. Each reads and writes its own record.

* ``plan``    reads (discovery) and writes ``state.yaml`` + ``plan.yaml`` + ``plan.md``. Writes nothing in Azure or Fabric.
* ``approve`` writes ``approval.yaml`` only: a person's decision, bound to the plan hash and to named steps.
* ``apply``   dry-run unless told otherwise; the only stage that writes to Fabric / Azure, and only approved steps.
* ``verify``  re-reads the target and writes ``verification.yaml``; never writes to Fabric / Azure.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from adf2fabric.agent.provisioning.executor import ApplyRefused, apply_plan
from adf2fabric.agent.provisioning.planner import make_plan_and_state
from adf2fabric.agent.provisioning.report import render_plan, render_verification
from adf2fabric.agent.provisioning.transport import ArmGateway, FabricGateway
from adf2fabric.agent.provisioning.verifier import mark_run, verify_plan
from adf2fabric.project.provisioning import (Action, ProvisioningApproval, ProvisioningPlan, ProvisioningRun, StepStatus, TargetSpec, TargetState,
                                             VerificationReport)
from adf2fabric.project.store import ProjectStore


def load_spec(store: ProjectStore, pid: str) -> Optional[TargetSpec]:
    return store.load_provisioning(pid, "target", TargetSpec) if store.has_provisioning(pid, "target") else None


def plan(store: ProjectStore, pid: str, spec: TargetSpec, fab: FabricGateway, arm: Optional[ArmGateway] = None, now: Optional[datetime] = None) -> ProvisioningPlan:
    p, state = make_plan_and_state(store, pid, spec, fab, arm, now)
    store.save_provisioning(pid, "target", spec)
    store.save_provisioning(pid, "state", state)
    store.save_provisioning(pid, "plan", p)
    store.provisioning_dir(pid).joinpath("plan.md").write_text(render_plan(p), encoding="utf-8")
    return p


def approvable(p: ProvisioningPlan) -> list[str]:
    return [s.id for s in p.steps if s.requires_approval and s.action in (Action.create, Action.configure) and not s.blockers]


def approve(store: ProjectStore, pid: str, step_ids: Optional[list[str]], by: str, note: str = "", now: Optional[datetime] = None) -> ProvisioningApproval:
    p = store.load_provisioning(pid, "plan", ProvisioningPlan)
    if not by.strip():
        raise ApplyRefused("Say who is approving (--by).")
    failing = [c for c in p.checks if c.blocking]
    if failing:
        raise ApplyRefused("Blocking checks failed, so nothing can be approved: " + "; ".join(f"{c.name}: {c.detail}" for c in failing[:3]))
    allowed = set(approvable(p))
    chosen = list(allowed) if step_ids is None else step_ids
    bad = [s for s in chosen if s not in allowed]
    if bad:
        raise ApplyRefused("These steps cannot be approved (they are not a create/configure, or they are blocked): " + ", ".join(bad))
    if not chosen:
        raise ApplyRefused("There is nothing to approve in this plan.")
    a = ProvisioningApproval(plan_hash=p.plan_hash, step_ids=sorted(chosen), approved_by=by.strip(), approved_at=now or datetime.now(timezone.utc), note=note[:300])
    store.save_provisioning(pid, "approval", a)
    return a


def apply(store: ProjectStore, pid: str, fab: FabricGateway, arm: Optional[ArmGateway] = None, *, do_apply: bool = False, sleep=None,
          now: Optional[datetime] = None) -> tuple[ProvisioningRun, Optional[VerificationReport]]:
    p = store.load_provisioning(pid, "plan", ProvisioningPlan)
    approval = store.load_provisioning(pid, "approval", ProvisioningApproval) if store.has_provisioning(pid, "approval") else None
    spec = p.target
    kw = {"sleep": sleep} if sleep is not None else {}
    run = apply_plan(store, pid, p, approval, spec, fab, arm, apply=do_apply, now=now, **kw)
    report = None
    if do_apply:
        report = verify_plan(p, spec, fab, arm, run, now, sleep, approved=set(approval.step_ids) if approval else None)
        mark_run(run, report)
        store.save_provisioning(pid, "run", run)
        store.save_provisioning(pid, "verification", report)
        store.provisioning_dir(pid).joinpath("verification.md").write_text(render_verification(p, report, run), encoding="utf-8")
    return run, report


def verify(store: ProjectStore, pid: str, fab: FabricGateway, arm: Optional[ArmGateway] = None, now: Optional[datetime] = None, sleep=None) -> VerificationReport:
    p = store.load_provisioning(pid, "plan", ProvisioningPlan)
    run = store.load_provisioning(pid, "run", ProvisioningRun) if store.has_provisioning(pid, "run") else None
    approval = store.load_provisioning(pid, "approval", ProvisioningApproval) if store.has_provisioning(pid, "approval") else None
    report = verify_plan(p, p.target, fab, arm, run, now, sleep, approved=set(approval.step_ids) if approval else None)
    if run is not None:
        mark_run(run, report)
        store.save_provisioning(pid, "run", run)
    store.save_provisioning(pid, "verification", report)
    store.provisioning_dir(pid).joinpath("verification.md").write_text(render_verification(p, report, run), encoding="utf-8")
    return report
