"""The body of each migration step, shown under the step boxes on the Migration page.

Secrets (Connect step): typed into password fields and sent ONLY to the secrets endpoint, held in the API's memory for
30 minutes, never saved and never returned. Everything else goes into the saved choice.
"""

from __future__ import annotations

import streamlit as st

import theme
from api_client import ApiClient
import options
from components import STATE_LABELS, call, can, empty_state, intent, run_job, short_source, state_badge, status_badge, test_report, typed_confirm
import panels_assist
import panels_bindings


# ---- 1. discover ----------------------------------------------------------------------------------------------------------
def discover(api: ApiClient, pid: str, project: dict | None, found: dict | None) -> None:
    src = (project or {}).get("source") or {}
    st.markdown(f"Source: **{src.get('provider') or '-'}** {short_source(src.get('url'))} · commit `{(src.get('commit') or '-')[:12]}`")
    label = "Run discovery again" if (project or {}).get("has_semantic") else "Run discovery"
    if (project or {}).get("semantic_stale"):
        st.warning("The source changed after discovery last ran, so conversion would use old data. Run discovery again.")
    if st.button(label, type="primary", key="run-discover"):
        for n, stage in enumerate(("discovery", "semantic"), 1):
            job = run_job(lambda s=stage: api.run_stage(pid, s, idempotency_key=intent(f"discover-{s}")), f"Step {n} of 2: {stage}", intent_name=f"discover-{stage}")
            if not job or job["state"] != "succeeded":
                return
        st.rerun()
    if not found:
        empty_state("Nothing discovered yet", "Run discovery to read the pipelines, linked services and runtimes. Nothing is changed in the source.")
        return
    lss, irs = found["linked_services"], found["integration_runtimes"]
    c = st.columns(3)
    c[0].metric("Linked services", len(lss))
    c[1].metric("Integration runtimes", len(irs))
    c[2].metric("Self-hosted runtimes", sum(1 for r in irs if r.get("kind") == "self_hosted"),
                help="These need an on-premises data gateway in Fabric.")
    t1, t2 = st.tabs(["Linked services", "Integration runtimes"])
    t1.dataframe(lss, hide_index=True)
    t2.dataframe(irs, hide_index=True)


# ---- 2. connect -----------------------------------------------------------------------------------------------------------
READY = ("connection_tested", "connection_configured", "skipped")
PROBLEM = ("manual_action_required", "validation_failed")


def _grid(items: list, render) -> None:
    """Lay ``items`` out two to a row so a form is not one long column."""
    for i in range(0, len(items), 2):
        for col, item in zip(st.columns(2), items[i:i + 2]):
            with col:
                render(item)


def _editor(api: ApiClient, pid: str, card: dict, known: dict[str, str]) -> None:
    """The one linked service being set up: its state once, the few facts that matter, and one form."""
    name = card["name"]
    held = set(card.get("secrets_held", []))
    st.markdown(f"#### {name} &nbsp; {state_badge(card['state'])}" + (" 🔑" if held else ""))
    st.caption(f"{card.get('adf_type') or '?'} → {card.get('connector_label') or 'no matching Fabric connector'}"
               f"{' (' + card['fabric_connection_type'] + ')' if card.get('fabric_connection_type') else ''}"
               f" · authored auth: {card.get('authentication_type') or '-'}")
    if not card.get("supported"):
        # one message: the card's own missing/warning/next-step lines say the same thing for a type with no connector
        st.warning("No automatic Fabric connector for this type. Create the connection in Fabric, then choose "
                   "'Use an existing Fabric connection' below, or handle it manually.")
    else:
        problems = [*card.get("missing", []), *card.get("warnings", [])]
        if problems:
            st.warning("\n".join(f"- {x}" for x in problems))
        if card.get("next_steps"):
            st.info("**Next:** " + " ".join(card["next_steps"]))

    options = card.get("auth_options", [])
    usable = [o for o in options if o.get("available")]
    blocked = [o for o in options if not o.get("available")]
    chosen = card.get("choice") or {}
    # A linked service need not get a connection: skipping is a decision, accepted as done. Pipelines that use it keep an
    # unbound placeholder and the deploy says so.
    skipped = chosen.get("method") == "skip"
    if skipped and not st.session_state.get(f"reopen-{name}"):
        st.info("Skipped: no Fabric connection will be created or bound for this linked service.")
        if st.button("Set up a connection after all", key=f"reopen-btn-{name}"):
            st.session_state[f"reopen-{name}"] = True
            st.rerun()
        return
    if not skipped:
        if st.button("Skip this one: it needs no connection", key=f"skip-{name}",
                     help="Nothing is created for it. Pipelines that use it stay unbound until you bind it."):
            if call(api.set_choice, pid, name, "skip") is not None:
                st.rerun()
    if not usable:
        st.caption("No sign-in method can be set up for this linked service here; skip it, or bind it by hand.")
        return
    ids = [o["id"] for o in usable]
    method_id = st.selectbox("How should Fabric sign in?", ids, key=f"m-{name}",
                             index=ids.index(chosen["method"]) if chosen.get("method") in ids else 0,
                             format_func=lambda i: next(o["label"] for o in usable if o["id"] == i))
    method = next(o for o in usable if o["id"] == method_id)
    if method.get("notes"):
        st.caption(method["notes"])
    if not method.get("api_creatable", True):
        st.info("This kind of connection can't be created automatically. Create it in the Fabric portal, "
                "then choose 'existing connection'.")

    with st.form(f"f-{name}", clear_on_submit=True, border=False):
        endpoint: dict = {}

        def endpoint_field(f: dict) -> None:
            current = (chosen.get("endpoint") or {}).get(f["name"]) or f.get("value") or ""
            help_ = f"from: {f.get('source')}"
            if f.get("candidates"):
                opts = list(dict.fromkeys([current, *f["candidates"]] if current else f["candidates"]))
                endpoint[f["name"]] = st.selectbox(f["label"], opts, key=f"e-{name}-{f['name']}", help=help_)
            else:
                endpoint[f["name"]] = st.text_input(f["label"] + (" *" if f.get("required") else ""), value=current,
                                                    key=f"e-{name}-{f['name']}", help=help_)

        _grid(card.get("endpoint_fields", []), endpoint_field)
        values: dict = {}
        secrets: dict = {}

        def method_field(f: dict) -> None:
            label = f["label"] + (" *" if f.get("required") else "")
            if f["kind"] == "secret":
                state = "held for this session" if f["name"] in held else "not given"
                secrets[f["name"]] = st.text_input(f"{label} ({state})", type="password", key=f"s-{name}-{f['name']}",
                                                   help="Kept in the accelerator's memory for 30 minutes. Never saved or shown again.")
            elif f["name"] == "connection_id":
                current = (chosen.get("values") or {}).get("connection_id", "")
                names = {f"{n} · {i[:8]}…": i for n, i in known.items()}
                labels = ["- choose a Fabric connection -", *names]
                at = next((k for k, v in names.items() if v == current), labels[0])
                kind = card.get("fabric_connection_type") or "matching"
                picked = st.selectbox(f"Fabric connection ({kind}) *", labels, index=labels.index(at), key=f"c-{name}",
                                      help=f"Existing {kind} connections in your Fabric workspace. Only the same type is listed.")
                if current and current not in names.values():
                    st.warning("The saved connection ID isn't one of this type's known connections.")
                pasted = st.text_input("…or paste a connection ID", value="" if current in names.values() else current,
                                       key=f"v-{name}-connection_id",
                                       help="Fabric: Settings ⚙ → Manage connections and gateways → the connection → Settings.")
                values["connection_id"] = pasted.strip() or names.get(picked, "")
            else:
                values[f["name"]] = st.text_input(label, value=(chosen.get("values") or {}).get(f["name"], ""),
                                                  key=f"v-{name}-{f['name']}", help=f.get("help") or None)

        _grid(method.get("fields", []), method_field)
        display = st.text_input("Fabric connection name (optional)", value=chosen.get("display_name") or "", key=f"d-{name}")
        a, b, _ = st.columns([1, 1.5, 2.5])
        save = a.form_submit_button("Save")
        save_test = b.form_submit_button("Save and test", type="primary")
        if save or save_test:
            saved = call(api.set_choice, pid, name, method_id, {k: v for k, v in endpoint.items() if v},
                         {k: v for k, v in values.items() if v}, display or None)
            if saved is not None:
                for field, value in secrets.items():
                    if value:
                        call(api.put_secret, pid, name, field, value)
                if save_test:
                    st.session_state["conn_test"] = (name, call(api.test_connection, pid, name))
                st.rerun()
    shown = st.session_state.get("conn_test")
    if shown and shown[0] == name and shown[1]:
        test_report(shown[1])

    rt = card.get("runtime") or {}
    with st.expander("Details"):
        if rt:
            st.markdown(f"Runtime `{rt.get('runtime_name') or 'default Azure'}` ({rt.get('runtime_kind')}) → Fabric path: "
                        f"**{(rt.get('fabric_path') or '-').replace('_', ' ')}**")
            if rt.get("recommendation"):
                st.caption(rt["recommendation"])
            for r in rt.get("requirements", []):
                st.caption(f"• {r}")
        if blocked:
            st.markdown("**Unavailable sign-in methods**")
            for o in blocked:
                st.caption(f"• {o['label']}: {o.get('reason') or 'not available'}")
        if card.get("existing_matches"):
            st.markdown(f"**Existing Fabric connections that may fit ({len(card['existing_matches'])})**")
            st.dataframe([{"Connection": m.get("display_name"), "Type": m.get("type") or m.get("connection_type") or ""} for m in card["existing_matches"]], hide_index=True)
        if card.get("used_by_activities"):
            st.markdown(f"**Used by {len(card['used_by_activities'])} activity(ies)**")
            st.dataframe(card["used_by_activities"], hide_index=True)


def _apply_table(res: dict) -> None:
    st.markdown(theme.badge("Preview: nothing was created", "info") if res.get("dry_run")
                else theme.badge("Created in Fabric", "success"), unsafe_allow_html=True)
    st.dataframe([{"linked service": i["linked_service"], "action": i["action"], "bound": i["bound"],
                   "Fabric validated": i["fabric_validated"], "detail": i["detail"]} for i in res.get("items", [])], hide_index=True)
    for i in res.get("items", []):
        for a in i.get("actions", []):
            st.caption(f"• {i['linked_service']}: {a}")


def fabric_read_notice(read: dict | None) -> None:
    """Say why the 'existing connection' list may be empty: no Fabric sign-in, a read that failed, or nothing visible to this account."""
    read = read or {}
    state = read.get("state")
    if state in ("not_connected", "unreadable") or (state == "ok" and not read.get("connections")):
        text = {"not_connected": "Fabric is not connected for this session, so existing connections can't be listed.",
                "unreadable": "Fabric's connections could not be read, so the list of existing ones is empty.",
                "ok": "Fabric returned no connections for this account."}[state]
        st.warning(f"{text} {read.get('todo', '')}".strip())
        if read.get("detail") and state != "ok":
            st.caption(f"Reason: {read['detail']}")


def connect(api: ApiClient, pid: str, cards: list[dict] | None, workspace_id: str | None) -> None:
    if not cards:
        st.info("No linked services found." if cards == [] else "Run discovery first.")
        return
    fabric_read_notice((getattr(api, "fabric_read", None) or {}).get(pid))
    ready = sum(1 for c in cards if c["state"] in READY)
    problems = sum(1 for c in cards if c["state"] in PROBLEM)
    st.markdown(f"**{len(cards)} linked services** · {ready} ready" + (f" · {problems} need attention" if problems else "")
                + f" · {len(cards) - ready - problems} to do")
    # Existing Fabric connections seen on any card, keyed by Fabric connection type so a storage linked service is never
    # offered a SQL connection (and vice versa).
    known: dict[str, dict[str, str]] = {}
    for c in cards:
        for m in c.get("existing_matches", []):
            known.setdefault(c.get("fabric_connection_type") or "?", {})[m["display_name"]] = m["id"]

    rows = [{"Linked service": c["name"], "Type": c.get("adf_type") or "?",
             "Fabric connector": c.get("connector_label") or "none", "Status": STATE_LABELS.get(c["state"], (c["state"],))[0]} for c in cards]
    event = st.dataframe(rows, hide_index=True, on_select="rerun", selection_mode="single-row", key="conn-table", width="stretch",
                         column_config={"Linked service": st.column_config.TextColumn(width="medium"),
                                        "Type": st.column_config.TextColumn(width="small"),
                                        "Status": st.column_config.TextColumn(width="medium")})
    picked = event.selection.rows
    first_open = next((i for i, c in enumerate(cards) if c["state"] not in READY), None)
    card = cards[picked[0] if picked else (first_open or 0)]
    if not picked:
        st.caption("Showing the first one still to do. Select a row above to set up another." if first_open is not None
                   else "Every linked service is set. Select a row above to change one.")
    with st.container(border=True):
        _editor(api, pid, card, known.get(card.get("fabric_connection_type") or "?", {}))

    st.markdown("#### Create the connections in Fabric")
    if not workspace_id:
        st.warning("Connect a Fabric workspace on the **Connections** page first.")
    names = st.session_state.get("apply-names", [])
    auto = st.checkbox("Reuse matching connections that already exist in Fabric", key="apply-auto")
    sig = (tuple(sorted(names)), auto, workspace_id)
    prev = st.session_state.get("apply_preview")
    valid = bool(prev and prev["sig"] == sig)
    a, b, c = st.columns([1, 1.4, 1])
    if a.button("Preview", key="apply-dry", disabled=not workspace_id, help="Shows what would be created. Changes nothing."):
        res = call(api.apply_connections, pid, names, dry_run=True, auto_match=auto, workspace_id=workspace_id)
        if res:
            st.session_state.apply_preview = {"sig": sig, "res": res}
            st.session_state.pop("apply_result", None)
            st.rerun()
    allowed = can("migration:execute")
    sure = typed_confirm("Create these connections in Fabric", "CREATE", "apply-confirm", disabled=not (workspace_id and valid and allowed))
    if b.button("Create in Fabric", type="primary", key="apply-go", disabled=not (workspace_id and valid and allowed and sure),
                help=None if (valid and allowed and sure) else ("Your role cannot change Fabric (needs migration:execute)." if not allowed
                                                              else "Run Preview first, then type CREATE.")):
        res = call(api.apply_connections, pid, names, dry_run=False, auto_match=auto, workspace_id=workspace_id, confirm=True)
        if res:
            st.session_state.apply_result = res
            st.session_state.pop("apply_preview", None)
            st.rerun()
    with c.popover("More", width="stretch"):
        st.multiselect("Only these linked services (empty = all)", [x["name"] for x in cards], key="apply-names")
        if st.button("Set up workspace identity", disabled=not workspace_id, key="apply-wi",
                     help="Needed by connections that sign in as the workspace."):
            out = call(api.provision_workspace_identity, pid, workspace_id, True)
            if out:
                (st.success if out.get("provisioned") else st.error)(out.get("next") or out.get("error") or str(out))
        if st.button("Build connections report", key="apply-report"):
            rep = call(api.connections_report, pid)
            if rep:
                st.session_state.conn_report = rep["markdown"]
        if st.button("Forget all secrets for this project", key="apply-forget"):
            n = call(api.clear_secrets, pid)
            if n is not None:
                st.success(f"Cleared {n} secret(s).")
        st.caption("🔒 Passwords and keys are never saved: they stay in the accelerator's memory for 30 minutes.")
    if valid:
        _apply_table(prev["res"])
    elif st.session_state.get("apply_result"):
        _apply_table(st.session_state.apply_result)
    if st.session_state.get("conn_report"):
        with st.expander("Connections report"):
            st.markdown(st.session_state.conn_report)


# ---- 3. convert -----------------------------------------------------------------------------------------------------------
def convert(api: ApiClient, pid: str, result: dict | None) -> None:
    st.caption("'Converted' is not the same as 'ready to run': a pipeline is ready only when all its connections are bound. "
               "Apply connections in the Connect step first.")
    brief = options.agreed(api, pid)
    known = [p["name"] for p in (call(api.inventory, pid) or {}).get("pipelines", [])]
    ver = brief["version"] if brief else 0
    with options.card("convert", "Conversion options", "What this run converts and how. A choice here is for this run only.", options.source_label(brief)):
        options.seed(f"conv-pipes-{pid}-{ver}", [p for p in (brief or {}).get("pipelines") or [] if p in known])
        pipes = st.multiselect("Pipelines to convert (empty = all)", known, key=f"conv-pipes-{pid}-{ver}",
                               help="A pipeline that calls another needs that one converted too, or already deployed.")
        options.seed(f"conv-store-{pid}-{ver}", "lakehouse" if (brief or {}).get("lakehouse") else "connections")
        route = st.radio("Storage", ["connections", "lakehouse"], key=f"conv-store-{pid}-{ver}", horizontal=True,
                         format_func=lambda v: {"connections": "Keep Fabric connections to the original storage",
                                                "lakehouse": "Repoint to a lakehouse through OneLake shortcuts"}[v],
                         help="Repointing reads and writes a lakehouse for CSV and Parquet datasets (Copy, Lookup, GetMetadata). Others stay on their connection, "
                              "each with the reason. Run the storage map first so the shortcuts are planned.")
        lake, problem = "", ""
        if route == "lakehouse":
            options.seed(f"conv-lake-{pid}-{ver}", (brief or {}).get("lakehouse") or "")
            lake = st.text_input("Lakehouse name", key=f"conv-lake-{pid}-{ver}", placeholder="e.g. LH_Migration").strip()
            problem = options.lakehouse_problem(lake)
            if problem:
                st.error(problem)
        options.seed(f"conv-recon-{pid}-{ver}", bool((brief or {}).get("apply_reconstruction")))
        recon = st.checkbox("Apply the safe reconstruction proposals for incomplete pipelines (every inferred element is marked)",
                            key=f"conv-recon-{pid}-{ver}", help="Off: incomplete pipelines stay blocked until the source is fixed.")
        offer = panels_assist.accepted(api, pid)
        assisted = False
        if offer:
            assisted = st.checkbox(f"Use the accepted model-assisted drafts ({len(offer)}: {', '.join(offer[:3])}{'...' if len(offer) > 3 else ''})",
                                   key=f"conv-assisted-{pid}", help="Each replaces that pipeline's original source for this conversion. The converter is still deterministic, "
                                                                     "the original file is unchanged, and every such pipeline is marked in the report.")
    if st.button("Convert again" if result else "Convert", type="primary", key="conv-go", disabled=bool(problem)):
        job = run_job(lambda: api.run_stage(pid, "convert", pipelines=pipes, lakehouse=lake if route == "lakehouse" else "", apply_reconstruction=recon,
                                            assisted=assisted, idempotency_key=intent("convert-step")), "Stage: convert", intent_name="convert-step")
        if job and job["state"] == "succeeded":
            st.session_state.setdefault("conversion", {})[pid] = job["result"]
            st.rerun()
    if not result:
        panels_assist.section(api, pid)
        return
    st.markdown(f"Overall: {status_badge(result.get('status'))} · connection bindings "
                f"{'used' if result.get('resolutions_used') else '**not applied yet**'} · "
                f"{result.get('compatibility_findings', 0)} compatibility finding(s)")
    counts = result.get("status_counts") or {}
    if counts:
        for col, (k, n) in zip(st.columns(len(counts)), counts.items()):
            col.metric(k.replace("_", " "), n)
    st.dataframe([{"pipeline": p["name"], "status": p["status"], "structurally valid": "✅" if p["structurally_valid"] else "❌",
                   "ready to run": "✅" if p["runtime_ready"] else "⬜"} for p in result.get("pipelines", [])], hide_index=True)
    for f in result.get("preflight", []):
        show = {"error": st.error, "warning": st.warning}.get(f.get("severity"), st.info)
        show(f"**{f['subject']}**: {f['message']}  \n*Fix:* {f['fix']}")
    if result.get("unbound"):
        st.warning(f"{len(result['unbound'])} id(s) are still open: pipelines that need them won't be ready to run.")
    panels_bindings.section(api, pid, result.get("unbound") or [])
    if result.get("filled_at_deploy"):
        st.caption(f"{len(result['filled_at_deploy'])} child pipeline id(s) are filled in automatically when you deploy.")
    if result.get("assisted"):
        st.info("Converted from a reviewed model-assisted edit of the source (the original file is unchanged): " + ", ".join(f"`{n}`" for n in result["assisted"]))
    panels_assist.section(api, pid)


# ---- 4. deploy ------------------------------------------------------------------------------------------------------------
def deploy(api: ApiClient, pid: str, dest: dict, conversion: dict | None, result: dict | None) -> None:
    if not dest.get("workspace_id"):
        st.warning("Connect a Fabric workspace on the **Connections** page first.")
        return
    if dest.get("provider", "azure_cli") != "azure_cli":
        st.warning("Deploying uses the Azure CLI sign-in. Switch the destination to Azure CLI on the **Connections** page.")
    st.markdown(f"Target: the Fabric workspace you connected (…{dest['workspace_id'][-6:]})")
    known = [p["name"] for p in (conversion or {}).get("pipelines", [])]
    brief = options.agreed(api, pid)
    ver = brief["version"] if brief else 0
    with options.card("deploy", "Deployment options", "What is published, and how. Deploying always starts with a dry run of exactly these pipelines.",
                      options.source_label(brief)):
        scoped = [p for p in ((brief or {}).get("pipelines") or []) if p in known]
        options.seed(f"dep-pipes-{pid}-{ver}", scoped if (brief or {}).get("pipelines") is not None else [])
        pipes = st.multiselect("Pipelines (empty = all converted)", known, key=f"dep-pipes-{pid}-{ver}")
        children = st.checkbox("Include child pipelines they call", value=True, key="dep-children",
                               help="A caller is refused unless every child is selected or already in the workspace.")
        unbound = st.checkbox("Allow unbound connections (not recommended)", value=False, key="dep-unbound",
                              help="Deploys pipelines whose connections aren't set yet. They won't run until fixed.")
    a, b = st.columns(2)
    stage = None
    a.markdown(theme.badge("Dry run: changes nothing", "info"), unsafe_allow_html=True)
    if a.button("Dry run", type="primary", key="dep-dry"):
        stage = "deploy_dry_run"
    dry_done = bool(result)
    b.markdown(theme.badge("Deploy: changes Fabric", "warning"), unsafe_allow_html=True)
    with b:
        sure = typed_confirm("Deploy for real: this creates or updates pipelines in Fabric", "DEPLOY", "dep-sure", disabled=not dry_done)
    if b.button("Deploy", disabled=not (dry_done and sure and can("migration:execute")), key="dep-go",
                help=None if can("migration:execute") else "Your role cannot deploy (needs migration:execute)."):
        stage = "deploy_apply"
    if stage:
        job = run_job(lambda: api.run_stage(pid, stage, pipelines=pipes, with_children=children, allow_unbound=unbound, confirm=(stage == "deploy_apply"),
                                            idempotency_key=intent(stage)),
                      "Deploy (dry run)" if stage == "deploy_dry_run" else "Deploy", intent_name=stage)
        if job and job["state"] == "succeeded":
            st.session_state.setdefault("deploy", {})[pid] = job["result"]
            st.rerun()
    if result:
        st.markdown(theme.badge("Deployed to Fabric", "success") if result.get("applied")
                    else theme.badge("Dry run: nothing was changed in Fabric", "info"), unsafe_allow_html=True)
        st.dataframe([{"pipeline": x["name"], "action": x["action"], "detail": x.get("detail") or "", "children": ", ".join(x.get("children") or [])}
                      for x in result.get("results", [])], hide_index=True)
    _compare(api, pid)


VERDICT_LABELS = {"matches": "✅ matches", "partial": "🟡 partly compared", "differs": "🟠 differs", "no_baseline": "⬜ no baseline",
                  "failed": "❌ run failed", "not_run": "⬜ not run yet"}


def _compare(api: ApiClient, pid: str) -> None:
    """Run-and-compare on the latest Fabric runs: nothing is started from here (`adf2fabric verify --apply` runs them)."""
    st.markdown("#### Prove it: compare with the ADF runs")
    st.caption("Compares each pipeline's latest Fabric run with its last successful ADF run (or the previous Fabric run): "
               "status, rows and files copied, lookup results. Nothing is started.")
    if st.button("Compare latest runs", key="verify-go"):
        job = run_job(lambda: api.run_stage(pid, "verify"), "Run-and-compare")
        if job and job["state"] == "succeeded":
            st.session_state.setdefault("verify", {})[pid] = job["result"]
            st.rerun()
    res = st.session_state.get("verify", {}).get(pid)
    try:  # quiet: before a conversion exists there is simply no report to offer
        handover = api.handover(pid)
    except Exception:  # noqa: BLE001
        handover = None
    if handover:
        st.download_button("Download handover report (Markdown)", handover.encode("utf-8"), file_name=f"{pid}-handover.md",
                           mime="text/markdown", key="handover-dl",
                           help="Each pipeline's readiness and next step, the source problems and what is still to supply.")
    if not res:
        return
    st.dataframe([{"pipeline": r["pipeline"], "verdict": VERDICT_LABELS.get(r["verdict"], r["verdict"]),
                   "Fabric run": r.get("ran_at") or "-", "baseline": f"{r.get('baseline') or '-'} {r.get('baseline_at') or ''}",
                   "facts matched": r.get("matched", 0), "detail": r.get("detail") or ""} for r in res["results"]],
                 hide_index=True, column_config={"Fabric run": st.column_config.TextColumn(width="small"),
                                                 "baseline": st.column_config.TextColumn(width="small"),
                                                 "facts matched": st.column_config.NumberColumn(width="small")})
    for r in res["results"]:
        for d in r.get("differences") or []:
            st.warning(f"**{r['pipeline']}** · `{d['activity']}` {d['fact']}: ADF {d['baseline']} → Fabric {d['fabric']}")
