"""Connections: pick an account for each side, connect the ADF source and the Fabric workspace, start the migration.

The two cards are separate fragments, so a click in one does not redraw the other, and every slow lookup (accounts,
Azure discovery, Fabric workspaces) is cached for a short time (see ``cache.py``) instead of being asked again on each click.
"""

import streamlit as st

import theme
from api_client import ApiError
from cache import DISCOVERY_TTL, SUBSCRIPTIONS_TTL, WORKSPACES_TTL, cached, forget
from components import account_picker, call, client, identity, run_job, show_error, test_report

api = client()
pid = st.session_state.get("project_id")
session = call(api.session) or {}
dest = session.get("destination") or {}
dst_tested = bool((session.get("tested") or {}).get("destination"))
st.session_state.setdefault("dest_caps", {})
capacity_missing = dst_tested and st.session_state.dest_caps.get(dest.get("workspace_id")) is False
src_ok = bool(pid)
dst_ok = dst_tested and not capacity_missing

# ---- head -----------------------------------------------------------------------------------------------------------------
theme.html_block(f'<div class="ma-pagehead"><span class="ma-eyebrow">{theme.esc(pid or "New migration")}</span>'
                 '<h1>Connect your source and Fabric</h1>'
                 '<p>Azure Data Factory → Microsoft Fabric. Each side signs in with its own account, so they can differ. '
                 'Sign-in happens on the machine running the accelerator; no password or token reaches this browser.</p></div>')
if st.session_state.get("mode") == "demo":
    theme.html_block('<div class="ma-note">ℹ️ <span><strong>Demo data.</strong> Sample accounts are already signed in. '
                     'Pick the project <code>demo-estate</code> in the top bar.</span></div>')


def side_head(letters: str, color: str, side: str, name: str, label: str, tone: str) -> None:
    theme.html_block(f'<div class="ma-side">{theme.mark(letters, color)}<div class="grow"><span class="ma-eyebrow">{side}'
                     f'</span><h3>{theme.esc(name)}</h3></div>{theme.badge(label, tone)}</div>')


def load(key: str, ttl: float, fn):
    """A cached API call; shows the error and returns None when it fails."""
    try:
        return cached(key, ttl, fn)
    except ApiError as err:
        show_error(err)
        return None


# ---- source ---------------------------------------------------------------------------------------------------------------
def import_source(name: str, source: dict, pipelines: list[str]) -> None:
    """Check access first, then import. A failed check is shown in full and nothing is imported."""
    if not name.strip():
        st.error("Give the project a name.")
        return
    if source.get("kind") in ("azure", "git"):
        rep = call(api.test_source, source)
        if rep is None:
            return
        if rep.get("state") != "connection_tested":
            st.error("This source could not be read, so nothing was imported.")
            test_report(rep)
            return
    call(api.set_source, {k: v for k, v in source.items() if k != "token"})
    job = run_job(lambda: api.import_project(name, source, pipelines), f"Importing '{name}'")
    if job and job["state"] == "succeeded":
        st.session_state.project_id = job["result"]["project_id"]
        st.session_state.pop("confirmed", None)
        st.rerun()


def pipelines_field(key: str, known: list[str] | None = None) -> list[str]:
    if known:
        return st.multiselect("Only these pipelines (empty = all)", known, key=key)
    raw = st.text_input("Only these pipelines (comma-separated, empty = all)", key=key)
    return [p.strip() for p in raw.split(",") if p.strip()]


def azure_source() -> None:
    account = account_picker("src", "ADF", hint="The account that can read the data factory. It can differ from the Fabric account.")
    if not account:
        return
    subs = load(f"subs:{account}", SUBSCRIPTIONS_TTL, lambda: api.subscriptions(account))
    if not subs:
        if subs is not None:
            st.info(f"{account} has no subscriptions.")
        return
    names = {f"{s['name']} ({s['id'][:8]}…)": s["id"] for s in subs}
    a, b = st.columns([5, 1], vertical_alignment="bottom")
    sub = names[a.selectbox("Subscription", list(names), key="src-sub")]
    if b.button("↻", key="src-refresh", help="Look again (new factories, new access).", width="stretch"):
        forget("subs", "disc")
        st.rerun()
    found = load(f"disc:{sub}", DISCOVERY_TTL, lambda: api.discover(sub)) or {}
    factories = found.get("factories", [])
    errors = list(dict.fromkeys(e["name"] for e in found.get("_errors", [])))
    if errors:
        forget(f"disc:{sub}")  # a failed lookup is not kept: the next click asks again
    if not factories:
        if errors:  # say why, instead of claiming the subscription has no factories
            st.error(errors[0])
            for more in errors[1:3]:
                st.caption(more)
            st.caption("Choose another account, or sign in with this one again.")
        else:
            st.info("No data factories in this subscription.")
        return
    opts = {f"{f['name']} · {f.get('resource_group')}": f for f in factories}
    f = opts[st.selectbox("Data factory", list(opts), key="src-factory")]
    source = {"kind": "azure", "subscription_id": sub, "resource_group": f.get("resource_group"), "factory_name": f["name"]}
    with st.form("src-az-form", border=False):
        name = st.text_input("Project name", value=f["name"])
        with st.expander("Options"):
            pipes = pipelines_field("src-az-pipes")
        if st.form_submit_button("Import factory", type="primary"):
            import_source(name, source, pipes)


def git_source() -> None:
    url = st.text_input("Repository URL (https)", key="src-git-url", placeholder="https://github.com/org/adf-repo")
    a, b = st.columns(2)
    rev = a.text_input("Branch, tag or commit", value="HEAD", key="src-git-rev")
    sub = b.text_input("Sub-folder (optional)", key="src-git-sub")
    with st.expander("Private repository"):
        a, b = st.columns(2)
        user = a.text_input("Username (optional)", key="src-git-user")
        token = b.text_input("Access token", type="password", key="src-git-token", help="Used for this request only. Never saved.")
    source = {"kind": "git", "repository_url": url or None, "revision": rev or "HEAD", "subdirectory": sub or None,
              "username": user or None, "token": token or None}
    with st.form("src-git-form", border=False):
        name = st.text_input("Project name", value=url.rstrip("/").split("/")[-1].removesuffix(".git") if url else "")
        pipes = pipelines_field("src-git-pipes")
        if st.form_submit_button("Import repository", type="primary", disabled=not url):
            import_source(name, source, pipes)


def zip_source() -> None:
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
    key = (st.session_state.zip_upload, sub)
    if st.session_state.get("zip_checked") != key:  # checked once per upload, not by a button
        st.session_state.zip_summary, st.session_state.zip_checked = call(api.test_source, source), key
    res = st.session_state.get("zip_summary")
    if res:
        s = res["summary"]
        st.markdown(theme.badge("Looks like ADF" if res["ok"] else "Not a usable ADF export", "success" if res["ok"] else "error")
                    + f" &nbsp; flavor: `{s['flavor']}` · " + ", ".join(f"{n} {k}" for k, n in s.get("counts", {}).items()),
                    unsafe_allow_html=True)
        if s.get("synapse_only"):
            st.warning("Synapse-only items (not converted): " + ", ".join(f"{k}: {n}" for k, n in s["synapse_only"].items()))
        if s.get("findings"):
            with st.expander(f"{len(s['findings'])} finding(s)"):
                st.dataframe(s["findings"], hide_index=True)
    with st.form("src-zip-form", border=False):
        name = st.text_input("Project name", value=up.name.removesuffix(".zip"))
        pipes = pipelines_field("src-zip-pipes", res["summary"].get("pipelines") if res else None)
        if st.form_submit_button("Import ZIP", type="primary"):
            import_source(name, source, pipes)


@st.fragment
def source_body() -> None:
    if pid:
        info = call(api.project, pid) or {}
        src = info.get("source") or {}
        st.success(f"Using project **{pid}** · {src.get('provider') or '?'} {src.get('url') or ''}")
        if not st.toggle("Import a different source", key="src-again"):
            return
    method = st.segmented_control("Where is the ADF source?", ["Azure factory", "Git repository", "ZIP export"],
                                  default="Azure factory", key="src-method") or "Azure factory"
    {"Azure factory": azure_source, "Git repository": git_source, "ZIP export": zip_source}[method]()


# ---- destination ----------------------------------------------------------------------------------------------------------
@st.fragment
def destination_body() -> None:
    use_cli = st.session_state.get("dst-fab", dest.get("provider") == "fabric_cli")
    provider = "fabric_cli" if use_cli else "azure_cli"
    account = None
    if provider == "azure_cli":
        account = account_picker("dst", "Fabric", hint="The account that has Fabric access. It can differ from the ADF account.")
        ready = bool(account)
    else:
        ready = bool(identity("fabric_cli", "the Fabric CLI", "dst-fabric_cli", device_option=False,
                              purpose="Use the account that has Fabric access."))
    if ready:
        _workspace_picker(provider, account)
    with st.expander("Advanced"):
        st.toggle("Use the Fabric CLI instead of the Azure CLI", key="dst-fab", value=use_cli,
                  help="Only if Fabric must be reached through `fab` rather than the Azure sign-in.")


def _workspace_picker(provider: str, account: str | None) -> None:
    try:
        workspaces = cached(f"ws:{provider}:{account}", WORKSPACES_TTL, lambda: api.workspaces(provider, account))
    except ApiError as err:
        st.error(err.message)
        st.markdown(f"**{account or 'This account'}** can't use Fabric. Usually this means it has **no Fabric licence**, is "
                    "signed in to a **different directory** from the Fabric workspace, or has **no access** to any workspace. "
                    "Choose another account above.")
        for a in err.actions:
            st.info(a)
        return
    if not workspaces:
        st.info("No Fabric workspaces for this account. It needs a Fabric licence and access to a workspace.")
        return
    ids = [w["id"] for w in workspaces]
    by_id = {w["id"]: w for w in workspaces}
    a, b = st.columns([5, 1], vertical_alignment="bottom")
    ws_id = a.selectbox("Workspace", ids, key="dst-ws", index=ids.index(dest["workspace_id"]) if dest.get("workspace_id") in ids else 0,
                        format_func=lambda i: f"{by_id[i]['display_name']} · {by_id[i].get('capacity_region') or 'no capacity'}")
    if b.button("↻", key="dst-refresh", help="Look again (new workspaces).", width="stretch"):
        forget("ws")
        st.rerun()
    has_capacity = bool(by_id[ws_id].get("capacity_id"))
    if not has_capacity:
        st.warning("This workspace has no capacity. Migration needs one.")
    if st.button("Test connection", key="dst-test", type="primary"):
        call(api.set_destination, provider, ws_id, None, account)
        rep = call(api.test_destination, provider, ws_id, None, account)
        st.session_state.dest_caps[ws_id] = has_capacity
        st.session_state.dest_report = rep
        st.rerun()
    if st.session_state.get("dest_report") and dest.get("workspace_id") == ws_id:
        with st.expander("Last test", expanded=not dst_tested):
            test_report(st.session_state.dest_report)


# ---- the two cards --------------------------------------------------------------------------------------------------------
left, right = st.columns(2)
with left, st.container(key=f"card-{'ok-' if src_ok else ''}src"):
    side_head("DF", "#2563eb", "Source", "Azure Data Factory", "Connected" if src_ok else "Not connected",
              "success" if src_ok else "neutral")
    source_body()
with right, st.container(key=f"card-{'ok-' if dst_ok else ''}dst"):
    side_head("FB", "#0d9488", "Destination", "Microsoft Fabric",
              "No capacity" if capacity_missing else ("Connected" if dst_tested else "Not connected"),
              "warning" if capacity_missing else ("success" if dst_tested else "neutral"))
    destination_body()

# ---- launch bar -----------------------------------------------------------------------------------------------------------
startable = src_ok and dst_ok
if startable:
    status = f"✓ {pid} · ✓ Fabric workspace connected"
else:
    todo = [t for t, ok in (("an ADF source", src_ok), ("a Fabric workspace with capacity", dst_ok)) if not ok]
    status = ("○ Still needed: " + " and ".join(todo)) + ("" if not capacity_missing else " (the chosen workspace has no capacity)")
with st.container(key="launchbar"):
    a, b = st.columns([3, 1], vertical_alignment="center")
    a.markdown(f'<div class="ma-checks"><span class="{"ok" if startable else ""}">{theme.esc(status)}</span></div>',
               unsafe_allow_html=True)
    started = bool(st.session_state.get("confirmed", {}).get(pid))
    if b.button(("Continue migration" if started else "Start migration") + " →", type="primary", disabled=not startable,
                width="stretch"):
        st.switch_page(st.session_state.pages["migration"])
