"""The Review, Plan, Validate and Report steps of the migration page.

Every number, state and sentence here comes from the API, which builds it on the server from the project's own records and removes
what an ordinary view must not show (paths, secrets, code, generated definitions, identifiers that are not needed). Nothing is decided in
the browser: a hidden button is never the protection, and a step is only "finished" when the backend says the thing happened.
"""

from __future__ import annotations

import streamlit as st

import options
import panels_confidence
import panels_waves
import theme
from api_client import ApiClient
from components import call, can, empty_state, intent, run_job, state_legend, typed_confirm

ACTION_TONE = {"create": "warning", "configure": "warning", "reuse": "success", "delegate": "info", "blocked": "error", "conflict": "error", "unknown": "neutral"}
ACTION_TEXT = {"create": "Will create", "configure": "Will configure", "reuse": "Exists: reuse", "delegate": "Done by another step", "blocked": "Blocked",
               "conflict": "Conflict: not changed", "unknown": "State unknown"}
RESULT_ICON = {"PASS": "✅ pass", "FAIL": "❌ fail", "BLOCKED": "⛔ blocked", "NOT_RUN": "➖ not run"}
CHECK_ICON = {"passed": "✅ passed", "failed": "❌ failed", "unknown": "❔ not established", "skipped": "➖ skipped"}


def _table(rows: list[dict], empty: str, **kw) -> None:
    if rows:
        st.dataframe(rows, hide_index=True, **kw)
    else:
        st.caption(empty)


# ---- 2. review ------------------------------------------------------------------------------------------------------------
def review(api: ApiClient, pid: str, inv: dict | None) -> None:
    if not inv:
        empty_state("Nothing to review yet", "Run discovery first: the inventory is built from what was discovered.")
        return
    comp = inv["completeness"]
    st.markdown(theme.badge("Discovery complete" if comp["complete"] else "Discovery has gaps", "success" if comp["complete"] else "warning"),
                unsafe_allow_html=True)
    cols = st.columns(3) + st.columns(3)  # two rows: six narrow tiles cut "Linked services" off
    for col, (label, key) in zip(cols, (("Pipelines", "pipelines"), ("Activities", "activities"), ("Linked services", "linked_services"),
                                        ("Datasets", "datasets"), ("Triggers", "triggers"), ("Data flows", "data_flows"))):
        col.metric(label, comp[key])
    if not comp["complete"]:
        gaps = []
        if comp["failed"]:
            gaps.append(f"{comp['failed']} source file(s) could not be parsed")
        if comp["unprocessed_total"]:
            gaps.append(f"{comp['unprocessed_total']} artifact(s) were seen but not processed")
        if comp["inventory_mismatches"]:
            gaps.append(f"{comp['inventory_mismatches']} pipeline(s) whose activity count did not reconcile with the source")
        if comp["unresolved_references"]:
            gaps.append(f"{comp['unresolved_references']} reference(s) point at something not in the source")
        st.warning("Discovery is not complete, so anything downstream may be missing pieces: " + "; ".join(gaps) + ".")
        _table(comp["unprocessed"], "", column_config={"kind": "Kind", "name": "Name", "reason": "Why it was not processed"})

    st.markdown("##### What each state means")
    state_legend(inv.get("states") or {})
    t_pipes, t_acts, t_waves, t_deps, t_compute, t_compat = st.tabs(["Pipelines", "Activities", "Waves & order", "Dependencies", "Compute", "Compatibility"])
    with t_pipes:
        _table([{"Pipeline": p["name"], "State": f"{state_chip_text(p['state'])}", "Activities": p["activities"], "Calls": ", ".join(p["calls"]),
                 "Why": "; ".join(p["reasons"])} for p in inv["pipelines"]], "No pipelines.",
               column_config={"Pipeline": st.column_config.TextColumn(width="large"), "State": st.column_config.TextColumn(width="medium"),
                              "Activities": st.column_config.NumberColumn(width="small"), "Calls": st.column_config.TextColumn(width="large"),
                              "Why": st.column_config.TextColumn(width="large")})
    with t_acts:
        _table([{"Activity type": t, "Count": n} for t, n in inv["activity_types"].items()], "No activities.")
        conf = inv.get("confidence") or {}
        st.caption("Understanding: " + ", ".join(f"{n} {k}" for k, n in sorted(conf.items())) +
                   ". 'medium' and 'low' mean something was dynamic or missing; 'unsupported' means the type is not known.")
    with t_waves:
        panels_waves.waves_panel(api, pid)
    with t_deps:
        _dependencies(api, pid, inv["dependencies"])
    with t_compute:
        _compute(api, pid, inv["compute"])
    with t_compat:
        _compatibility(api, pid)


def state_chip_text(state: str) -> str:
    from components import PSTATE_ICON, PSTATE_LABEL

    return f"{PSTATE_ICON.get(state, '')} {PSTATE_LABEL.get(state, state)}".strip()


def _dependencies(api: ApiClient, pid: str, dep: dict) -> None:
    if not dep.get("available"):
        empty_state("Dependencies not resolved yet", dep.get("needs", ""))
        if st.button("Resolve dependencies", key="rv-deps", type="primary", disabled=not can("discovery:run")):
            if (job := run_job(lambda: api.run_stage(pid, "dependencies", idempotency_key=intent("dependencies")), "Resolving dependencies", intent_name="dependencies")) \
                    and job["state"] == "succeeded":
                st.rerun()
        return
    c = st.columns(4)
    c[0].metric("Dependencies", dep["total"])
    c[1].metric("Blocking", dep["blocking"])
    c[2].metric("Errors", dep["errors"])
    c[3].metric("Circular groups", dep["cycles"])
    if not dep.get("source_complete"):
        st.warning("The source was not fully retrieved, so some references may resolve once it is.")
    st.caption("Access checks: " + dep.get("access_checks", ""))
    _table([{"Severity": f["severity"], "Finding": f["code"], "About": f["subject"], "What": f["message"], "Do this": f["action"]} for f in dep["findings"]],
           "No findings: every reference resolves.")
    st.markdown("**Migration order** (children before the pipelines that call them)")
    _table([{"Wave": o["wave"], "Pipeline": o["pipeline"], "Ready": "yes" if o["ready"] else "no", "Blocked by": o["blocked_by"], "In a cycle": "yes" if o["in_cycle"] else ""}
            for o in dep["order"]], "No pipelines.")


def _compute(api: ApiClient, pid: str, comp: dict) -> None:
    if not comp.get("available"):
        empty_state("No Spark compute found", comp.get("needs", ""))
        return
    _table([{"Platform": s["platform"], "Mode": s["mode"], "Runtime": s["runtime"] or "unknown", "Sized": "yes" if s["sized"] else "no", "Activities": s["activities"],
             "Libraries": s["libraries"]} for s in comp.get("sources", [])], "No Spark compute sources.")
    if comp.get("pools"):
        st.markdown("**Fabric Spark pools proposed**")
        _table([{"Pool": p["name"], "Status": p["status"], "Node size": p["node_size"], "Peak vCores": p["peak_vcores"]} for p in comp["pools"]], "")
    est = comp.get("estimate") or {}
    if est.get("largest_pool_capacity_units"):
        st.caption(f"The largest pool peaks at {est['largest_pool_capacity_units']:g} capacity units (at least {est.get('minimum_sku_for_largest_pool', '?')}).")
    for g in comp.get("gaps", [])[:8]:
        st.caption(f"• {g['message']} {g['action']}")
    if comp.get("workloads"):
        st.markdown("**Notebooks, jobs and JARs**")
        _table([{"Workload": w["name"], "Kind": w["kind"], "Classification": w["classification"].replace("_", " "), "Code": w["code"].replace("_", " "),
                 "Generated": w["generation"].replace("_", " "), "Open issues": w["open_issues"]} for w in comp["workloads"]], "")
    if st.button("Assess Spark workloads", key="rv-workloads", disabled=not can("plans:generate"),
                 help="Read-only: classifies each notebook / job. No code is fetched without a credential and nothing is sent to Fabric."):
        if (job := run_job(lambda: api.run_stage(pid, "workloads", idempotency_key=intent("workloads")), "Assessing workloads", intent_name="workloads")) \
                and job["state"] == "succeeded":
            st.rerun()


def _compatibility(api: ApiClient, pid: str) -> None:
    comp = call(api.compatibility, pid)
    if not comp or not comp.get("available"):
        empty_state("Compatibility is judged against a conversion", (comp or {}).get("needs", "Run the conversion locally to assess it."))
        if st.button("Assess compatibility", key="rv-compat", type="primary", disabled=not can("plans:generate"),
                     help="Runs the conversion on this machine. Nothing is sent to Fabric."):
            if (job := run_job(lambda: api.run_stage(pid, "convert", idempotency_key=intent("convert")), "Assessing compatibility", intent_name="convert")) \
                    and job["state"] == "succeeded":
                st.session_state.pop("conversion", None)
                st.rerun()
        return
    s = comp["activity_status"]
    c = st.columns(4)
    c[0].metric("Converted", s.get("converted", 0))
    c[1].metric("With differences", s.get("converted_with_differences", 0))
    c[2].metric("Manual work", s.get("manual_required", 0))
    c[3].metric("Blocked", s.get("blocked", 0))
    if comp["needs_attention"]:
        st.markdown(f"**{comp['needs_attention_total']} activity(ies) need attention, with the proposed alternative**")
        _table([{"Pipeline": a["pipeline"], "Activity": a["activity"], "Type": a["type"], "Status": a["status"].replace("_", " "), "What changes": a["summary"],
                 "Alternative / next step": "; ".join([*a["alternatives"], *a["manual_steps"]])} for a in comp["needs_attention"]], "")
    if comp["retired_or_preview"]:
        st.markdown("**Retired, deprecated or preview components**")
        _table([{"Component": f["subject"], "Type": f["adf_type"], "Status": f["status"], "Date": f["date"] or "", "Alternative": "; ".join(f["alternatives"])}
                for f in comp["retired_or_preview"]], "")
    if comp["connectors"]:
        st.markdown("**Connectors**")
        _table([{"Linked service": c_["linked_service"], "Type": c_["type"], "Support": c_["level"], "Note": c_["note"]} for c_ in comp["connectors"]], "")
    if comp["source_problems"]:
        st.markdown("**Problems in the source that conversion cannot fix**")
        _table([{"Severity": f["severity"], "About": f["subject"], "Problem": f["message"], "Fix": f["fix"]} for f in comp["source_problems"]], "")
    if comp["workloads"]:
        st.markdown("**Spark workloads that need a different approach**")
        for w in comp["workloads"]:
            with st.expander(f"{w['name']}: {w['classification'].replace('_', ' ')}"):
                for r in w["reasons"]:
                    st.write(r)
                for a in w["alternatives"]:
                    st.info(a["summary"] + (" Limits: " + " ".join(a["limitations"]) if a["limitations"] else ""))


# ---- 4. plan the target ---------------------------------------------------------------------------------------------------
def plan(api: ApiClient, pid: str, dest: dict, prov: dict | None) -> None:
    st.markdown(theme.badge("Planning changes nothing", "info"), unsafe_allow_html=True)
    st.caption("The plan reads what exists in the target and says what this migration needs. Creating anything is a separate, approved step, "
               "and a dry run comes first.")
    has_ws = bool(dest.get("workspace_id"))
    if not has_ws:  # an existing plan can still be read (by anyone who may view reports); a new one needs a workspace
        st.warning("Connect a Fabric workspace on the **Connections** page to build a plan.")
        if not (prov and prov.get("available")):
            return
    with st.form("plan-form", border=False):
        a, b = st.columns(2)
        lakehouses = a.text_input("Lakehouse name(s), comma-separated", key="plan-lh", help="A name is never invented: leave empty if none is needed.")
        warehouses = b.text_input("Warehouse name(s), comma-separated", key="plan-wh")
        identity = st.checkbox("Plan a workspace identity", key="plan-id")
        go = st.form_submit_button("Build / refresh the plan", type="primary", disabled=not (has_ws and can("plans:generate")))
    if go:
        split = lambda s: [x.strip() for x in s.split(",") if x.strip()]  # noqa: E731
        if (job := run_job(lambda: api.plan_provisioning(pid, lakehouses=split(lakehouses), warehouses=split(warehouses), provision_identity=identity,
                                                         idempotency_key=intent("plan")), "Planning the target (read-only)", intent_name="plan")) \
                and job["state"] == "succeeded":
            st.rerun()
    if not prov or not prov.get("available"):
        empty_state("No plan yet", (prov or {}).get("needs", "Build the plan to see what exists and what is needed."))
        return
    s = prov["summary"]
    c = st.columns(5)
    c[0].metric("To create", s["to_create"])
    c[1].metric("To configure", s["to_configure"])
    c[2].metric("Reused", s["reused"])
    c[3].metric("Blocked", s["blocked"] + s["conflicts"], help="Steps that cannot run, or that conflict with what already exists.")
    c[4].metric("Unknown", s["unknown"])
    est = prov.get("estimates") or {}
    if est.get("largest_pool_capacity_units"):
        st.caption(f"Estimated requirement: the largest planned Spark pool peaks at {est['largest_pool_capacity_units']:g} capacity units "
                   f"(at least {est.get('minimum_sku_for_largest_pool', '?')}).")
    for b_ in prov["blockers"]:
        st.error(b_)
    with st.expander("Checks: what was and was not established", expanded=bool(prov["blockers"])):
        _table([{"Check": ch["name"] + (" (blocking)" if ch["blocking"] else ""), "Result": CHECK_ICON.get(ch["status"], ch["status"]), "Detail": ch["detail"],
                 "Proved": ch["proved"], "NOT proved": ch["not_proved"], "Do this": ch["action"]} for ch in prov["checks"]], "")
    st.markdown("**Steps, in the order they would run**")
    rows = [{"Action": ACTION_TEXT.get(x["action"], x["action"]), "Resource": x["type"].replace("_", " "), "Name": x["name"],
             "Approved": "yes" if x["approved"] else ("needs approval" if x["needs_approval"] else ""), "Result": x["result"] or "",
             "Verified": x["verification"] or "", "Why / note": "; ".join([*x["blockers"], *x["notes"]])} for x in prov["steps"]]
    for col in ("Result", "Verified"):  # nothing has run yet: two empty columns only push the notes off the screen
        if not any(r[col] for r in rows):
            for r in rows:
                r.pop(col)
    _table(rows, "", column_config={"Action": st.column_config.TextColumn(width="medium"), "Resource": st.column_config.TextColumn(width="medium"),
                                    "Name": st.column_config.TextColumn(width="large"), "Approved": st.column_config.TextColumn(width="medium"),
                                    "Why / note": st.column_config.TextColumn(width="large")})
    _approval_and_apply(api, pid, prov)


def _approval_and_apply(api: ApiClient, pid: str, prov: dict) -> None:
    ref = prov["plan_ref_full"]
    approvable = [x for x in prov["steps"] if x["needs_approval"] and not x["blockers"] and x["action"] in ("create", "configure")]
    appr = prov.get("approval")
    st.markdown("##### 1. Approve")
    if not approvable:
        st.caption("Nothing in this plan creates or configures anything, so there is nothing to approve.")
    elif appr and appr["current"]:
        st.success(f"{appr['steps']} step(s) approved by {appr['by']} for this plan.")
    else:
        chosen = st.multiselect("Steps to approve", [x["ref"] for x in approvable], default=[x["ref"] for x in approvable], key="plan-approve-steps",
                                format_func=lambda i: next(f"{ACTION_TEXT[x['action']]}: {x['type'].replace('_', ' ')} {x['name']}" for x in approvable if x["ref"] == i))
        if st.button("Approve the selected steps", key="plan-approve", disabled=not (chosen and can("provision:approve")),
                     help=None if can("provision:approve") else "Your role cannot approve provisioning (needs provision:approve). Nothing is created by approving."):
            if call(api.approve_provisioning, pid, ref, chosen) is not None:
                st.rerun()
        st.caption("Approving records your decision. It does not create anything.")
    st.markdown("##### 2. Dry run, then apply")
    run = prov.get("run")
    if not (appr and appr["current"]) and approvable:
        st.caption("Approve first.")
        return
    if not approvable:
        return
    a, b = st.columns(2)
    if a.button("Dry run (nothing is changed)", key="plan-dry", disabled=not can("migration:execute")):
        if (job := run_job(lambda: api.apply_provisioning(pid, ref, apply=False, idempotency_key=intent("plan-dry")), "Provisioning dry run", intent_name="plan-dry")) \
                and job["state"] == "succeeded":
            st.rerun()
    if run:
        st.markdown(theme.badge("Last run: DRY RUN, nothing changed" if run["dry_run"] else "Last run: APPLIED to Fabric (verify it below)",
                                "info" if run["dry_run"] else "warning"), unsafe_allow_html=True)
        if run.get("interrupted"):
            st.warning("A previous run did not finish. Applying again continues; nothing already created is created twice.")
    dry_done = bool(run and run["dry_run"] and run["finished"])
    with b.container():
        sure = typed_confirm("Apply for real: this creates or configures resources in Fabric", "APPLY", "plan-apply-confirm", disabled=not dry_done)
        if st.button("Apply for real", key="plan-apply", type="primary", disabled=not (dry_done and sure and can("migration:execute")),
                     help="Run the dry run first, then type APPLY." if not (dry_done and sure) else None):
            if (job := run_job(lambda: api.apply_provisioning(pid, ref, apply=True, confirm=True, idempotency_key=intent("plan-apply")), "Applying the approved steps",
                               intent_name="plan-apply")) and job["state"] == "succeeded":
                st.rerun()
    st.markdown("##### 3. Verify")
    ver = prov.get("verification")
    if ver:
        st.markdown(theme.badge("Verified: every resource was read back" if ver["all_verified"] else "NOT everything is verified", "success" if ver["all_verified"] else "warning"),
                    unsafe_allow_html=True)
    st.caption("Only this read-back says a resource exists: 'applied' means a call succeeded, nothing more.")
    if st.button("Verify (read-only)", key="plan-verify", disabled=not can("plans:generate")):
        if (job := run_job(lambda: api.verify_provisioning(pid), "Reading the target back", intent_name="plan-verify")) and job["state"] == "succeeded":
            st.rerun()


# ---- 6. validate ----------------------------------------------------------------------------------------------------------
VALIDATION_STAGES = {
    "static": "Static: re-read the source and the generated files, independently of the converter",
    "api": "API: check the workspace answers and the pipelines can be read back",
    "deployment": "Deployment: compare what is in the workspace with what was generated",
    "runtime": "Run evidence: compare recorded Fabric runs with the ADF runs (starts nothing)",
}


def validate(api: ApiClient, pid: str, val: dict | None, dest: dict | None = None) -> None:
    has_ws = bool((dest or {}).get("workspace_id"))
    brief = options.agreed(api, pid)
    ver = brief["version"] if brief else 0
    with options.card("validate", "Validation options", "Which checks run. Only a recorded real run that matches ADF makes a pipeline runtime-validated.",
                      options.source_label(brief)):
        options.seed(f"val-stages-{pid}-{ver}", [s for s in (brief or {}).get("validate_stages") or ["static"] if s in VALIDATION_STAGES])
        stages = st.multiselect("Checks to run", list(VALIDATION_STAGES), key=f"val-stages-{pid}-{ver}", format_func=lambda s: VALIDATION_STAGES[s])
        needs_ws = [s for s in stages if s in ("api", "deployment")]
        if needs_ws and not has_ws:
            st.warning("The API and deployment checks read the Fabric workspace: choose it on the Connections page first.")
    st.markdown(theme.badge("Nothing here starts a run or changes Fabric", "info"), unsafe_allow_html=True)
    if st.button("Run validation", key="val-run", type="primary", disabled=not (can("plans:generate") and stages and not (needs_ws and not has_ws)),
                 help="Re-reads the source and the generated files and compares them, independently of the converter."):
        if (job := run_job(lambda: api.run_stage(pid, "validate", validate_stages=stages, idempotency_key=intent("validate")), "Validating", intent_name="validate")) \
                and job["state"] == "succeeded":
            st.rerun()
    if not val or not val.get("available"):
        empty_state("Not validated yet", (val or {}).get("needs", "Run validation."))
        return
    _validation_body(val)


def _validation_body(val: dict) -> None:
    tone = {"MIGRATION_VERIFIED": "success", "FAILED": "error", "BLOCKED": "warning", "NOT_VALIDATED": "warning"}.get(val["overall"], "neutral")
    st.markdown(theme.badge(val["overall"].replace("_", " "), tone), unsafe_allow_html=True)
    for r in val["reasons"]:
        st.caption(r)
    _table([{"Stage": s["stage"], "Result": RESULT_ICON.get(s["status"], s["status"]), "Pass": s["passed"], "Fail": s["failed"], "Blocked": s["blocked"], "Not run": s["not_run"]}
            for s in val["stages"]], "")
    if val["open_checks"]:
        st.markdown(f"**{val['open_checks_total']} check(s) are not passing**")
        _table([{"Result": RESULT_ICON.get(c["result"], c["result"]), "Stage": c["stage"], "About": c["component"], "What": c["summary"], "Do this": c["remediation"]}
                for c in val["open_checks"]], "")
    if val["components_needing_review"]:
        with st.expander(f"{len(val['components_needing_review'])} component(s) missing, extra, altered or unverified"):
            _table([{"Component": c["id"], "Status": c["status"], "Note": c["note"]} for c in val["components_needing_review"]], "")


# ---- 8. report ------------------------------------------------------------------------------------------------------------
def report(api: ApiClient, pid: str, rep: dict | None, dest: dict) -> None:
    ws = bool(dest.get("workspace_id"))
    with st.form("report-form", border=False):
        runtime = st.checkbox("Include recorded run evidence (from `verify`)", key="rep-runtime", help="Reads results of real Fabric runs. It never starts a run.")
        go = st.form_submit_button("Validate the migration now (files, Fabric, deployment)", type="primary", disabled=not (ws and can("plans:generate")))
    if not ws:
        st.caption("Connect a Fabric workspace to validate the deployment.")
    if go:
        stages = ["static", "api", "deployment"] + (["runtime"] if runtime else [])
        if (job := run_job(lambda: api.run_stage(pid, "validate", validate_stages=stages, idempotency_key=intent("report")), "Validating the migration", intent_name="report")) \
                and job["state"] == "succeeded":
            st.rerun()
    if not rep:
        empty_state("No report yet", "Run validation to get an honest account of what is and is not confirmed.")
        return
    {"success": st.success, "error": st.error, "warning": st.warning}.get(rep["tone"], st.info)(rep["headline"])
    panels_confidence.confidence_panel(api, pid)
    st.markdown("##### Where each pipeline stands")
    state_legend(rep["states"])
    _table([{"Pipeline": p["name"], "State": state_chip_text(p["state"]), "Why": "; ".join(p["reasons"])} for p in rep["pipelines"]], "No pipelines.",
           column_config={"Pipeline": st.column_config.TextColumn(width="large"), "State": st.column_config.TextColumn(width="medium"),
                          "Why": st.column_config.TextColumn(width="large")})
    if rep["unresolved"]:
        st.markdown(f"##### Unresolved ({rep['unresolved_total']})")
        _table([{"About": u["scope"], "State": u["state"], "Issue": u["issue"], "Do this": u.get("action", "")} for u in rep["unresolved"]], "",
               column_config={"About": st.column_config.TextColumn(width="large"), "State": st.column_config.TextColumn(width="small"),
                              "Issue": st.column_config.TextColumn(width="large"), "Do this": st.column_config.TextColumn(width="large")})
    if rep["validation"].get("available"):
        with st.expander("Validation detail"):
            _validation_body(rep["validation"])
    _run_and_compare(api, pid, rep, ws)
    st.caption("A generated file, a clean check or a successful call is never proof that a pipeline runs correctly: only a recorded real run compared with the source is.")


def _run_and_compare(api: ApiClient, pid: str, rep: dict, ws: bool) -> None:
    """Prove pipelines by running them: a REAL run in Fabric, compared with the last successful ADF run. Opt-in, confirmed, audited."""
    deployed = [p["name"] for p in rep["pipelines"] if p["state"] in ("deployed", "runtime_validated")]
    with st.expander("Run in Fabric and compare with ADF (real runs)"):
        st.markdown(theme.badge("REAL RUNS: they read the pipelines' sources and write their targets", "warning"), unsafe_allow_html=True)
        if not deployed:
            st.caption("Deploy pipelines first: only deployed pipelines can be run.")
            return
        chosen = st.multiselect("Pipelines to run (up to 20)", deployed, key="run-pipes", max_selections=20)
        wait = st.select_slider("Wait for each run up to", [5, 10, 20, 30, 50], value=30, key="run-wait", format_func=lambda m: f"{m} min",
                                help="A run that takes longer keeps running in Fabric; it is simply not compared. Kept within one sign-in's lifetime.")
        sure = typed_confirm("Start real runs of the selected pipelines in Fabric", "RUN", "run-confirm", disabled=not chosen)
        allowed = can("migration:execute")
        if st.button("Run and compare", key="run-go", type="primary", disabled=not (ws and chosen and sure and allowed),
                     help=None if allowed else "Your role cannot start runs (needs migration:execute)."):
            if (job := run_job(lambda: api.run_stage(pid, "verify", pipelines=chosen, run=True, confirm=True, run_timeout=wait * 60, idempotency_key=intent("run-verify")),
                               "Running in Fabric and comparing", intent_name="run-verify")) and job["state"] == "succeeded":
                st.rerun()
        st.caption("A pipeline becomes runtime-validated only when its run completes and matches the ADF baseline.")
