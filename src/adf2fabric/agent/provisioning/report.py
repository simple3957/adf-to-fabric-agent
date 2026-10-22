"""The plan and verification, as the person approving them will read them."""

from __future__ import annotations

from typing import Optional

from adf2fabric.project.provisioning import ProvisioningPlan, ProvisioningRun, VerificationReport

_VERB = {"create": "CREATE", "configure": "CONFIGURE", "reuse": "reuse", "delegate": "delegated", "conflict": "CONFLICT", "blocked": "BLOCKED", "unknown": "UNKNOWN"}


def _c(text: str) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ")


def render_plan(p: ProvisioningPlan) -> str:
    s = p.summary
    out = [f"# Fabric target provisioning plan: {p.project_id}", "",
           f"State read at {p.state_read_at:%Y-%m-%d %H:%M UTC}. Plan hash `{p.plan_hash[:16]}`. **Nothing has been created.** An approval covers only the steps "
           "you name, only for this hash.", "",
           f"{s.total} steps: {s.to_create} to create, {s.to_configure} to configure, {s.reused} reusable, {s.delegated} delegated, "
           f"{s.blocked} blocked, {s.conflicts} conflicts, {s.unknown} unknown.", ""]
    if p.blockers:
        out += ["## Blockers", ""] + [f"- {b}" for b in p.blockers] + [""]
    if p.estimates:
        e = p.estimates
        out += ["## Estimated requirements", "",
                f"- {e.get('pools', 0)} Spark pool(s); the largest peaks at {e.get('largest_pool_spark_vcores', 0)} Spark vCores "
                f"= {e.get('largest_pool_capacity_units', 0):g} capacity units" + (f" (at least {e['minimum_sku_for_largest_pool']})" if e.get('minimum_sku_for_largest_pool') else "") + ".",
                f"- Sum of pool peaks: {e.get('sum_of_pool_peaks_capacity_units', 0):g} capacity units (pools rarely peak together).", f"- {e.get('basis', '')}", ""]
    out += ["## Checks", "", "| Check | Status | Detail | Proved | Not proved |", "|---|---|---|---|---|"]
    out += [f"| {c.name}{' (blocking)' if c.blocking else ''} | {c.status.value} | {_c(c.detail)} | {_c(c.proved)} | {_c(c.not_proved)} |" for c in p.checks]
    out += ["", "## Steps, in dependency order", "", "| # | Action | Type | Name | API | Existing (as read) | Needs approval | Notes |", "|---|---|---|---|---|---|---|---|"]
    for i, st in enumerate(p.steps, 1):
        ex = f"exists ({st.existing.id or st.existing.name})" if st.existing else "not found" if st.action.value in ("create", "configure") else ""
        out.append(f"| {i} | {_VERB[st.action.value]} | {st.type.value} | {_c(st.name)} | {st.api.value} | {_c(ex)} | {'yes' if st.requires_approval else 'no'} | "
                   f"{_c('; '.join(st.blockers + st.notes)[:240])} |")
    out += ["", "## Request previews (what would be sent; no secrets)", ""]
    for st in p.steps:
        if st.action.value in ("create", "configure") and st.request:
            out += [f"- **{st.type.value} {st.name}**: `{st.endpoint}` {st.request}"]
    if p.notes:
        out += ["", "## Notes", ""] + [f"- {n}" for n in p.notes]
    return "\n".join(out) + "\n"


def render_verification(p: ProvisioningPlan, v: VerificationReport, run: Optional[ProvisioningRun] = None) -> str:
    names = {s.id: f"{s.type.value} {s.name}" for s in p.steps}
    out = [f"# Provisioning verification: {p.project_id}", "", f"Read back from the target at {v.verified_at:%Y-%m-%d %H:%M UTC}. "
           f"**All verified: {'yes' if v.all_verified else 'NO'}.** Only this report may say a resource exists.", "",
           "| Resource | Status | Detail |", "|---|---|---|"]
    out += [f"| {_c(names.get(r.step_id, r.step_id))} | {r.status.value} | {_c(r.detail)} |" for r in v.results]
    if run is not None:
        failed = [r for r in run.results if r.status.value in ("failed", "mismatch")]
        if failed:
            out += ["", "## Not completed", ""] + [f"- {names.get(r.step_id, r.step_id)}: {r.status.value}. {r.detail}" for r in failed]
    return "\n".join(out) + "\n"
