"""Connections: choose the route, connect both sides, start the migration (same layout as the Synapse accelerator)."""

import streamlit as st

import theme
from api_client import ApiError
from components import call, client, run_job, show_error, sign_in, test_report
from journey import STEPS

api = client()
SOURCES = {"adf": "Azure Data Factory", "synapse": "Azure Synapse Analytics (coming soon)",
           "ssis": "SQL Server Integration Services (coming soon)"}
DESTINATIONS = {"fabric": "Microsoft Fabric"}
route = st.session_state.setdefault("route", {"source": "adf", "destination": "fabric"})
session = call(api.session) or {}
pid = st.session_state.get("project_id")

# ---- hero -----------------------------------------------------------------------------------------------------------------
chips = '<span class="ma-arrow">›</span>'.join(f'<span class="ma-chip"><b>{i}</b>{s.title}</span>' for i, s in enumerate(STEPS, 1))
theme.html_block(f'<div class="ma-hero"><span class="ma-eyebrow">{theme.esc(pid or "New migration")}</span>'
                 f'<h1>Move your Data Factory<br>to <span class="grad">Microsoft Fabric</span></h1><p>Pick the route, connect both '
                 f'sides, then go from discovery to deployment one confirmed step at a time.</p>'
                 f'<div class="ma-chips">{chips}</div></div>')
if st.session_state.get("mode") == "demo":
    theme.html_block('<div class="ma-note">ℹ️ <span><strong>Demo data.</strong> Sample Azure and Fabric accounts are '
                     'already signed in, so no Azure sign-in is needed. Pick the project <code>demo-estate</code> in the '
                     'top bar, or import a ZIP.</span></div>')

# ---- 1. route -------------------------------------------------------------------------------------------------------------
route_ok = route["source"] == "adf" and route["destination"] == "fabric"
theme.section(1, "Choose the route", "The platform you are leaving, and the one you are moving to.", done=route_ok)
c1, arrow, c2 = st.columns([10, 1, 10], vertical_alignment="bottom")
route["source"] = c1.selectbox("From · Source", list(SOURCES), format_func=SOURCES.get,
                               index=list(SOURCES).index(route["source"]))
arrow.markdown("### →")
route["destination"] = c2.selectbox("To · Destination", list(DESTINATIONS), format_func=DESTINATIONS.get)
if route["source"] != "adf":
    st.info("Only Azure Data Factory → Microsoft Fabric is available in this accelerator. "
            "Synapse has its own accelerator.")
    route_ok = False

# ---- 2. connect both sides ------------------------------------------------------------------------------------------------
dest = session.get("destination") or {}
dst_tested = bool((session.get("tested") or {}).get("destination"))
st.session_state.setdefault("dest_caps", {})
capacity_missing = dst_tested and st.session_state.dest_caps.get(dest.get("workspace_id")) is False
src_ok = bool(pid)
dst_ok = dst_tested and not capacity_missing
theme.section(2, "Connect both sides", "Sign-in happens on the machine running the accelerator. No password or token "
              "is shown in, or stored by, this browser.", done=src_ok and dst_ok)


def side_head(letters: str, color: str, side: str, name: str, label: str, tone: str) -> None:
    theme.html_block(f'<div class="ma-side">{theme.mark(letters, color)}<div class="grow"><span class="ma-eyebrow">{side}'
                     f'</span><h3>{theme.esc(name)}</h3></div>{theme.badge(label, tone)}</div>')


def import_source(name: str, source: dict, pipelines: list[str]) -> None:
    if not name.strip():
        st.error("Give the project a name.")
        return
    call(api.set_source, {k: v for k, v in source.items() if k != "token"})
    job = run_job(lambda: api.import_project(name, source, pipelines), f"Importing '{name}'")
    if job and job["state"] == "succeeded":
        st.session_state.project_id = job["result"]["project_id"]
        st.session_state.pop("confirmed", None)
        st.rerun()


def pipelines_field(key: str, known: list[str] | None = None) -> list[str]:
    if known:
        return st.multiselect("Pipelines (empty = all)", known, key=key)
    raw = st.text_input("Pipelines (comma-separated, empty = all)", key=key)
    return [p.strip() for p in raw.split(",") if p.strip()]


def source_body() -> None:
    az = call(api.auth_status, "azure_cli") or {}
    if az.get("authenticated"):
        a, b = st.columns([3, 2], vertical_alignment="center")
        a.caption(f"Signed in as **{az.get('account') or '?'}** · tenant `{(az.get('tenant_id') or '-')[:8]}…`")
        with b:
            sign_in("azure_cli", "Azure CLI", "src-switch", button="Switch account")
    if pid:
        info = call(api.project, pid) or {}
        src = info.get("source") or {}
        st.success(f"Using project **{pid}** · {src.get('provider') or '?'} {src.get('url') or ''}")
        if not st.toggle("Import a different source", key="src-again"):
            return
    method = st.segmented_control("How do we reach the ADF source?", ["Azure factory", "Git repository", "ZIP export"],
                                  default="Azure factory", key="src-method")
    if method == "Azure factory":
        if not az.get("authenticated"):
            st.caption("Sign in to Azure to list your data factories.")
            sign_in("azure_cli", "Azure CLI", "src-az")
            return
        subs = call(api.subscriptions) or []
        if not subs:
            st.info("No subscriptions for this sign-in.")
            return
        names = {f"{s['name']} ({s['id'][:8]}…)": s["id"] for s in subs}
        sub = names[st.selectbox("Subscription", list(names), key="src-sub")]
        found = call(api.discover, sub) or {}
        factories = found.get("factories", [])
        if not factories:
            st.info("No data factories in this subscription.")
            return
        opts = {f"{f['name']} · {f.get('resource_group')}": f for f in factories}
        f = opts[st.selectbox("Data factory", list(opts), key="src-factory")]
        source = {"kind": "azure", "subscription_id": sub, "resource_group": f.get("resource_group"), "factory_name": f["name"]}
        if st.button("Test", key="src-az-test"):
            rep = call(api.test_source, source)
            if rep:
                test_report(rep)
        with st.form("src-az-form"):
            name = st.text_input("Project name", value=f["name"])
            pipes = pipelines_field("src-az-pipes")
            if st.form_submit_button("Import", type="primary"):
                import_source(name, source, pipes)
    elif method == "Git repository":
        url = st.text_input("Repository URL (https)", key="src-git-url", placeholder="https://github.com/org/adf-repo")
        a, b = st.columns(2)
        rev = a.text_input("Branch, tag or commit", value="HEAD", key="src-git-rev")
        sub = b.text_input("Sub-folder (optional)", key="src-git-sub")
        a, b = st.columns(2)
        user = a.text_input("Username (optional)", key="src-git-user")
        token = b.text_input("Access token (private repos)", type="password", key="src-git-token",
                             help="Used for this request only. Never saved.")
        source = {"kind": "git", "repository_url": url or None, "revision": rev or "HEAD", "subdirectory": sub or None,
                  "username": user or None, "token": token or None}
        if st.button("Test", key="src-git-test", disabled=not url):
            rep = call(api.test_source, source)
            if rep:
                test_report(rep)
        with st.form("src-git-form"):
            name = st.text_input("Project name", value=url.rstrip("/").split("/")[-1].removesuffix(".git") if url else "")
            pipes = pipelines_field("src-git-pipes")
            if st.form_submit_button("Import", type="primary", disabled=not url):
                import_source(name, source, pipes)
    elif method == "ZIP export":
        up = st.file_uploader("ADF export (.zip)", type=["zip"], key="src-zip")
        sub = st.text_input("Sub-folder inside the ZIP (optional)", key="src-zip-sub")
        if up is not None and st.session_state.get("zip_name") != (up.name, up.size):
            try:
                st.session_state.zip_upload = api.upload_zip(up.getvalue())["upload_id"]
                st.session_state.zip_name = (up.name, up.size)
                st.session_state.pop("zip_summary", None)
            except ApiError as err:
                show_error(err)
        if up is None or not st.session_state.get("zip_upload"):
            return
        source = {"kind": "zip", "upload_id": st.session_state.zip_upload, "subdirectory": sub or None}
        if st.button("Check the ZIP", key="src-zip-test"):
            st.session_state.zip_summary = call(api.test_source, source)
        res = st.session_state.get("zip_summary")
        if res:
            s = res["summary"]
            st.markdown(theme.badge("Looks like ADF" if res["ok"] else "Not a usable ADF export",
                                    "success" if res["ok"] else "error") + f" &nbsp; flavor: `{s['flavor']}`",
                        unsafe_allow_html=True)
            st.dataframe([{"kind": k, "count": n} for k, n in s.get("counts", {}).items()], hide_index=True)
            if s.get("synapse_only"):
                st.warning("Synapse-only items (not converted): " + ", ".join(f"{k}: {n}" for k, n in s["synapse_only"].items()))
            if s.get("findings"):
                st.dataframe(s["findings"], hide_index=True)
        with st.form("src-zip-form"):
            name = st.text_input("Project name", value=up.name.removesuffix(".zip"))
            pipes = pipelines_field("src-zip-pipes", res["summary"].get("pipelines") if res else None)
            if st.form_submit_button("Import", type="primary"):
                import_source(name, source, pipes)


def destination_body() -> None:
    provider = st.segmented_control("Sign-in used for Fabric", ["azure_cli", "fabric_cli"], key="dst-provider",
                                    default=dest.get("provider") or "azure_cli",
                                    format_func={"azure_cli": "Azure CLI", "fabric_cli": "Fabric CLI"}.get) or "azure_cli"
    status = call(api.auth_status, provider) or {}
    if not status.get("authenticated"):
        st.caption("Sign in so the accelerator can reach your Fabric workspaces.")
        sign_in(provider, "Azure CLI" if provider == "azure_cli" else "Fabric CLI", "dst-login",
                device_option=provider == "azure_cli")
        return
    label = "Azure CLI" if provider == "azure_cli" else "Fabric CLI"
    st.caption(f"Signed in as **{status.get('account') or '?'}**")
    try:
        workspaces = api.workspaces(provider)
    except ApiError as err:
        st.error(f"{err.message}")
        st.markdown(f"**{status.get('account') or 'This account'}** can't use Fabric. Usually this means:\n"
                    "- the account has **no Fabric licence** (Fabric answers *User is not licensed*), or\n"
                    "- it is signed in to a **different tenant** from the Fabric workspace, or\n"
                    "- it has **no access** to any Fabric workspace.\n\n"
                    "Sign in with the account that has Fabric access (set its tenant ID if it has several).")
        for a in err.actions:
            st.info(a)
        sign_in(provider, label, "dst-switch", device_option=provider == "azure_cli", button="Sign in with a different account")
        return
    if not workspaces:
        st.info("No Fabric workspaces for this sign-in. The account needs a Fabric licence and workspace access.")
        sign_in(provider, label, "dst-switch", device_option=provider == "azure_cli", button="Sign in with a different account")
        return
    ids = [w["id"] for w in workspaces]
    by_id = {w["id"]: w for w in workspaces}
    ws_id = st.selectbox("Workspace", ids, key="dst-ws", index=ids.index(dest["workspace_id"]) if dest.get("workspace_id") in ids else 0,
                         format_func=lambda i: f"{by_id[i]['display_name']} · {by_id[i].get('capacity_region') or 'no capacity'}")
    has_capacity = bool(by_id[ws_id].get("capacity_id"))
    if not has_capacity:
        st.warning("This workspace has no capacity. Migration needs one.")
    if st.button("Test connection", key="dst-test", type="primary"):
        call(api.set_destination, provider, ws_id)
        rep = call(api.test_destination, provider, ws_id)
        st.session_state.dest_caps[ws_id] = has_capacity
        st.session_state.dest_report = rep
        st.rerun()
    if st.session_state.get("dest_report") and dest.get("workspace_id") == ws_id:
        with st.expander("Last test", expanded=not dst_tested):
            test_report(st.session_state.dest_report)


left, right = st.columns(2)
with left, st.container(key=f"card-{'ok-' if src_ok else ''}src"):
    side_head("DF", "#2563eb", "Source", "Azure Data Factory",
              "Connected" if src_ok else "Not connected", "success" if src_ok else "neutral")
    if route_ok:
        source_body()
with right, st.container(key=f"card-{'ok-' if dst_ok else ''}dst"):
    side_head("FB", "#0d9488", "Destination", "Microsoft Fabric",
              "No capacity" if capacity_missing else ("Connected" if dst_tested else "Not connected"),
              "warning" if capacity_missing else ("success" if dst_tested else "neutral"))
    if route_ok:
        destination_body()

# ---- launch bar -----------------------------------------------------------------------------------------------------------
checks = [("Route chosen", route_ok), ("Azure Data Factory connected", src_ok), ("Microsoft Fabric connected", dst_ok)]
startable = all(ok for _, ok in checks)
with st.container(key="launchbar"):
    a, b = st.columns([3, 1], vertical_alignment="center")
    hint = "" if startable else (" · Choose the route first." if not route_ok else
                                 " · The Fabric workspace needs a capacity." if capacity_missing else
                                 " · Connect both sides to start.")
    a.markdown('<div class="ma-checks">' + "".join(
        f'<span class="{"ok" if ok else ""}"><b>{"✓" if ok else "○"}</b> {theme.esc(t)}</span>' for t, ok in checks)
        + f'<span>{theme.esc(hint)}</span></div>', unsafe_allow_html=True)
    started = bool(st.session_state.get("confirmed", {}).get(pid))
    if b.button(("Continue migration" if started else "Start migration") + " →", type="primary",
                disabled=not startable, width="stretch"):
        st.switch_page(st.session_state.pages["migration"])
