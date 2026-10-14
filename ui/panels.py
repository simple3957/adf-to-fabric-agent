"""The body of each migration step, shown under the step boxes on the Migration page.

Secrets (Connect step): typed into password fields and sent ONLY to the secrets endpoint, held in the API's memory for
30 minutes, never saved and never returned. Everything else goes into the saved choice.
"""

from __future__ import annotations

import streamlit as st

import theme
from api_client import ApiClient
from components import STATE_LABELS, call, run_job, state_badge, status_badge, test_report


# ---- 1. discover ----------------------------------------------------------------------------------------------------------
def discover(api: ApiClient, pid: str, project: dict | None, found: dict | None) -> None:
    src = (project or {}).get("source") or {}
    st.markdown(f"Source: **{src.get('provider') or '-'}** {src.get('url') or ''} · commit `{(src.get('commit') or '-')[:12]}`")
    label = "Run discovery again" if (project or {}).get("has_semantic") else "Run discovery"
    if st.button(label, type="primary", key="run-discover"):
        for stage in ("discovery", "semantic"):
            job = run_job(lambda s=stage: api.run_stage(pid, s), f"Stage: {stage}")
            if not job or job["state"] != "succeeded":
                return
        st.rerun()
    if not found:
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
def _card(api: ApiClient, pid: str, card: dict, known: dict[str, str]) -> None:
    name = card["name"]
    st.markdown(f"**{card.get('adf_type') or '?'}** → {card.get('connector_label') or 'no matching Fabric connector'}"
                f" ({card.get('fabric_connection_type') or '-'}) · authored auth: `{card.get('authentication_type') or '-'}`")
    if not card.get("supported"):
        st.warning("No automatic Fabric connector for this type. Use an existing connection, or handle it manually.")
    rt = card.get("runtime") or {}
    if rt:
        st.markdown(f"Runtime: `{rt.get('runtime_name') or 'default Azure'}` ({rt.get('runtime_kind')}) → "
                    f"Fabric path: **{(rt.get('fabric_path') or '-').replace('_', ' ')}**")
        if rt.get("recommendation"):
            st.caption(rt["recommendation"])
        for r in rt.get("requirements", []):
            st.caption(f"• {r}")
    for label, items, show in (("Missing", card.get("missing"), st.warning), ("Next steps", card.get("next_steps"), st.info),
                               ("Warnings", card.get("warnings"), st.warning)):
        if items:
            show(f"**{label}:**\n" + "".join(f"\n- {i}" for i in items))

    options = card.get("auth_options", [])
    usable = [o for o in options if o.get("available")]
    blocked = [o for o in options if not o.get("available")]
    if not usable:
        st.error("No sign-in method is available for this linked service.")
        return
    chosen = card.get("choice") or {}
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
    if blocked:
        with st.expander(f"{len(blocked)} unavailable method(s)"):
            for o in blocked:
                st.caption(f"• **{o['label']}**: {o.get('reason') or 'not available'}")
    if card.get("existing_matches"):
        with st.expander(f"Existing Fabric connections that may fit ({len(card['existing_matches'])})"):
            st.dataframe(card["existing_matches"], hide_index=True)

    held = set(card.get("secrets_held", []))
    with st.form(f"f-{name}", clear_on_submit=True):
        endpoint = {}
        for f in card.get("endpoint_fields", []):
            current = (chosen.get("endpoint") or {}).get(f["name"]) or f.get("value") or ""
            help_ = f"from: {f.get('source')}"
            if f.get("candidates"):
                opts = list(dict.fromkeys([current, *f["candidates"]] if current else f["candidates"]))
                endpoint[f["name"]] = st.selectbox(f["label"], opts, key=f"e-{name}-{f['name']}", help=help_)
            else:
                endpoint[f["name"]] = st.text_input(f["label"] + (" *" if f.get("required") else ""), value=current,
                                                    key=f"e-{name}-{f['name']}", help=help_)
        values, secrets = {}, {}
        for f in method.get("fields", []):
            label = f["label"] + (" *" if f.get("required") else "")
            if f["kind"] == "secret":
                state = "held for this session" if f["name"] in held else "not given"
                secrets[f["name"]] = st.text_input(f"{label} ({state})", type="password", key=f"s-{name}-{f['name']}",
                                                   help="Kept in API memory for 30 minutes. Never saved or shown again.")
            elif f["name"] == "connection_id":
                # Pick an existing Fabric connection by name; the ID is filled in for you.
                current = (chosen.get("values") or {}).get("connection_id", "")
                names = {f"{n} · {i[:8]}…": i for n, i in known.items()}
                labels = ["- choose a Fabric connection -", *names]
                at = next((k for k, v in names.items() if v == current), labels[0])
                kind = card.get("fabric_connection_type") or "matching"
                picked = st.selectbox(f"Fabric connection ({kind}) *", labels, index=labels.index(at), key=f"c-{name}",
                                      help=f"Existing {kind} connections in your Fabric workspace. Only the same type is listed.")
                if current and current not in names.values():
                    st.warning("The saved connection ID isn't one of this type's known connections. Check it's the right one.")
                pasted = st.text_input("…or paste a connection ID", value="" if current in names.values() else current,
                                       key=f"v-{name}-connection_id",
                                       help="Only needed if the connection isn't in the list. Find it in Fabric: "
                                            "Settings ⚙ → Manage connections and gateways → the connection → Settings.")
                values["connection_id"] = pasted.strip() or names.get(picked, "")
            else:
                values[f["name"]] = st.text_input(label, value=(chosen.get("values") or {}).get(f["name"], ""),
                                                  key=f"v-{name}-{f['name']}", help=f.get("help") or None)
        display = st.text_input("Fabric connection name (optional)", value=chosen.get("display_name") or "", key=f"d-{name}")
        if st.form_submit_button("Save"):
            saved = call(api.set_choice, pid, name, method_id, {k: v for k, v in endpoint.items() if v},
                         {k: v for k, v in values.items() if v}, display or None)
            if saved is not None:
                for field, value in secrets.items():
                    if value:
                        call(api.put_secret, pid, name, field, value)
                st.rerun()
    if st.button("Test", key=f"t-{name}"):
        rep = call(api.test_connection, pid, name)
        if rep:
            test_report(rep)
    if card.get("used_by_activities"):
        with st.expander(f"Used by {len(card['used_by_activities'])} activity(ies)"):
            st.dataframe(card["used_by_activities"], hide_index=True)


def _apply_table(res: dict) -> None:
    st.markdown(theme.badge("Dry run: nothing was created", "info") if res.get("dry_run")
                else theme.badge("Applied to Fabric", "success"), unsafe_allow_html=True)
    st.dataframe([{"linked service": i["linked_service"], "action": i["action"], "bound": i["bound"],
                   "Fabric validated": i["fabric_validated"], "connection ids": ", ".join(i["connection_ids"]),
                   "detail": i["detail"]} for i in res.get("items", [])], hide_index=True)
    for i in res.get("items", []):
        for a in i.get("actions", []):
            st.caption(f"• {i['linked_service']}: {a}")


def connect(api: ApiClient, pid: str, cards: list[dict] | None, workspace_id: str | None) -> None:
    st.caption("🔒 Passwords and keys are never saved: they stay in the accelerator's memory for 30 minutes.")
    if not cards:
        st.info("No linked services found." if cards == [] else "Run discovery first.")
        return
    counts: dict[str, int] = {}
    for c in cards:
        counts[c["state"]] = counts.get(c["state"], 0) + 1
    st.markdown(" ".join(f"{state_badge(s)} ×{n}" for s, n in counts.items()))
    # Existing Fabric connections seen on any card, so any linked service can reuse them by name.
    # Keyed by Fabric connection type, so a storage linked service is never offered a SQL connection (and vice versa).
    known: dict[str, dict[str, str]] = {}
    for c in cards:
        for m in c.get("existing_matches", []):
            known.setdefault(c.get("fabric_connection_type") or "?", {})[m["display_name"]] = m["id"]
    for card in cards:
        label, color = STATE_LABELS.get(card["state"], (card["state"], "gray"))
        title = f"**{card['name']}** · {card.get('adf_type') or '?'} · :{color}[{label}]"
        if card.get("secrets_held"):
            title += " · 🔑"
        with st.expander(title, expanded=False):
            st.markdown(state_badge(card["state"]) + ("  🔑 " + ", ".join(card["secrets_held"]) + " held"
                                                      if card.get("secrets_held") else ""))
            _card(api, pid, card, known.get(card.get("fabric_connection_type") or "?", {}))

    st.markdown("#### Create the connections in Fabric")
    if not workspace_id:
        st.warning("Connect a Fabric workspace on the **Connections** page first.")
    names = st.multiselect("Linked services (empty = all)", [c["name"] for c in cards], key="apply-names")
    auto = st.checkbox("Reuse matching existing Fabric connections automatically", key="apply-auto")
    a, b, c = st.columns(3)
    if a.button("Dry run", disabled=not workspace_id, key="apply-dry"):
        res = call(api.apply_connections, pid, names, dry_run=True, auto_match=auto, workspace_id=workspace_id)
        if res:
            _apply_table(res)
    sure = b.checkbox("Create them for real", key="apply-sure")
    if b.button("Apply", type="primary", disabled=not (workspace_id and sure), key="apply-go"):
        res = call(api.apply_connections, pid, names, dry_run=False, auto_match=auto, workspace_id=workspace_id)
        if res:
            _apply_table(res)
    if c.button("Set up workspace identity", disabled=not workspace_id, key="apply-wi",
                help="Needed by connections that sign in as the workspace."):
        out = call(api.provision_workspace_identity, pid, workspace_id)
        if out:
            (st.success if out.get("provisioned") else st.error)(out.get("next") or out.get("error") or str(out))
    with st.expander("Connections report"):
        if st.button("Build report", key="apply-report"):
            rep = call(api.connections_report, pid)
            if rep:
                st.markdown(rep["markdown"])
    if st.button("Forget all secrets for this project", key="apply-forget"):
        n = call(api.clear_secrets, pid)
        if n is not None:
            st.success(f"Cleared {n} secret(s).")


# ---- 3. convert -----------------------------------------------------------------------------------------------------------
def convert(api: ApiClient, pid: str, result: dict | None) -> None:
    st.caption("'Converted' is not the same as 'ready to run': a pipeline is ready only when all its connections are bound. "
               "Apply connections in the Connect step first.")
    raw = st.text_input("Only these pipelines (comma-separated, empty = all)", key="conv-pipes")
    if st.button("Convert again" if result else "Convert", type="primary", key="conv-go"):
        pipes = [p.strip() for p in raw.split(",") if p.strip()]
        job = run_job(lambda: api.run_stage(pid, "convert", pipelines=pipes), "Stage: convert")
        if job and job["state"] == "succeeded":
            st.session_state.setdefault("conversion", {})[pid] = job["result"]
            st.rerun()
    if not result:
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
    if result.get("unbound"):
        st.warning("Still unbound (pipelines that need these won't be ready to run):\n" +
                   "".join(f"\n- `{u}`" for u in result["unbound"]))


# ---- 4. deploy ------------------------------------------------------------------------------------------------------------
def deploy(api: ApiClient, pid: str, dest: dict, conversion: dict | None, result: dict | None) -> None:
    if not dest.get("workspace_id"):
        st.warning("Connect a Fabric workspace on the **Connections** page first.")
        return
    if dest.get("provider", "azure_cli") != "azure_cli":
        st.warning("Deploying uses the Azure CLI sign-in. Switch the destination to Azure CLI on the **Connections** page.")
    st.markdown(f"Target workspace: `{dest['workspace_id']}`")
    known = [p["name"] for p in (conversion or {}).get("pipelines", [])]
    pipes = st.multiselect("Pipelines (empty = all converted)", known, key="dep-pipes")
    children = st.checkbox("Include child pipelines they call", value=True, key="dep-children")
    unbound = st.checkbox("Allow unbound connections (not recommended)", value=False, key="dep-unbound",
                          help="Deploys pipelines whose connections aren't set yet. They won't run until fixed.")
    a, b = st.columns(2)
    stage = None
    if a.button("Dry run", type="primary", key="dep-dry"):
        stage = "deploy_dry_run"
    sure = b.checkbox("I have checked the dry run and want to deploy for real", key="dep-sure")
    if b.button("Deploy", disabled=not sure, key="dep-go"):
        stage = "deploy_apply"
    if stage:
        job = run_job(lambda: api.run_stage(pid, stage, pipelines=pipes, with_children=children, allow_unbound=unbound),
                      "Deploy (dry run)" if stage == "deploy_dry_run" else "Deploy")
        if job and job["state"] == "succeeded":
            st.session_state.setdefault("deploy", {})[pid] = job["result"]
            st.rerun()
    if result:
        st.markdown(theme.badge("Deployed to Fabric", "success") if result.get("applied")
                    else theme.badge("Dry run: nothing was changed in Fabric", "info"), unsafe_allow_html=True)
        st.dataframe([{"pipeline": x["name"], "action": x["action"], "item id": x.get("item_id") or "",
                       "detail": x.get("detail") or "", "children": ", ".join(x.get("children") or [])}
                      for x in result.get("results", [])], hide_index=True)
