"""ADF → Fabric Studio UI. Run: ``streamlit run ui/app.py`` (with the API running: ``python -m adf2fabric.api``).

Same process as the Synapse accelerator (its own look): a Connections page (choose the route, connect both sides, start) and a Migration
page (one confirmed step at a time), under a top bar with the connection state and the Live/Demo switch.
"""

from __future__ import annotations

import streamlit as st

import theme
from api_client import ApiError
from components import call, client

st.set_page_config(page_title="ADF → Fabric Studio", page_icon="◆", layout="wide")
theme.inject()

PAGES = {
    "setup": st.Page("views/setup.py", title="Connections", icon="🔗", default=True),
    "migration": st.Page("views/migration.py", title="Migration", icon="🧭"),
    "activity": st.Page("views/jobs.py", title="Activity", icon="🧾"),
}
nav = st.navigation(list(PAGES.values()), position="hidden")
st.session_state.pages = PAGES


def _sign_in(api) -> None:
    """The API requires a token. Each person pastes their own; it is kept in this browser session only (never in a file or the URL)."""
    st.markdown("### Sign in")
    st.caption("This service requires an API token. Ask an administrator for yours (`adf2fabric auth-user-create`).")
    token = st.text_input("API token", type="password", key="signin-token", label_visibility="collapsed", placeholder="a2f_...")
    if st.button("Sign in", type="primary") and token:
        api.set_token(token)
        try:
            api.me()
        except ApiError as err:
            api.clear_token()
            st.error("That token was not accepted." if err.status == 401 else err.message)
        else:
            st.session_state.pop("signin-token", None)
            st.rerun()


def _who(api) -> dict:
    """Reach the service and say who is asking, BEFORE the top bar is drawn: a sign-in form or an error drawn inside the bar's rounded container is
    squeezed and clipped. Stops the page unless a person is signed in."""
    try:
        api.health()
    except ApiError as err:
        st.markdown(f'<div class="ma-note">⚠️ <span><strong>The accelerator service is not reachable.</strong> '
                    f'{theme.esc(err.message)} {theme.esc(" ".join(err.actions))}</span></div>', unsafe_allow_html=True)
        st.stop()
    try:
        return api.me()
    except ApiError as err:
        if err.status not in (401, 429):  # never a traceback on the sign-in page: it would show paths
            st.error(err.message)
            st.caption(" ".join(err.actions) or "Try again in a moment.")
            st.stop()
        st.session_state.pop("permissions", None)
        if err.status == 429:  # a valid token is never held back by the lockout, so the form still works
            st.warning("Too many failed sign-in attempts from this machine. Wait a minute. A correct token still signs in.")
        _sign_in(api)
        st.stop()


def top_bar() -> None:
    api = client()
    who = _who(api)
    with st.container(key="topbar"):
        brand, links, dots, project, mode_col = st.columns([2.0, 4.4, 1.4, 2.0, 1.3], vertical_alignment="center")
        brand.markdown('<div class="ma-brand"><span class="ma-brand-mark">◆</span>ADF&nbsp;→&nbsp;Fabric&nbsp;<small>Studio</small></div>',
                       unsafe_allow_html=True)
        with links, st.container(horizontal=True, gap="small", vertical_alignment="center"):
            for key in ("setup", "migration", "activity"):
                st.page_link(PAGES[key], width="content")
        st.session_state.permissions = set(who.get("permissions") or [])
        st.session_state.demo_available = bool(who.get("demo_available", True))
        if who.get("production"):
            st.session_state.pop("mode", None)
        if who.get("auth_mode") == "token":
            with st.popover(f"{who['id']} ({who['org']})"):
                st.caption("Roles: " + ", ".join(who.get("roles") or []))
                if st.button("Sign out"):
                    api.clear_token()
                    st.session_state.pop("permissions", None)
                    st.rerun()

        if not st.session_state.get("url_applied"):  # ?demo=1&project=<id> opens straight into demo data
            st.session_state.url_applied = True
            if st.query_params.get("demo") == "1":
                call(api.set_mode, "demo")
            if st.query_params.get("project"):
                st.session_state.project_id = st.query_params["project"]
        mode = call(api.get_mode) or "live"
        session = call(api.session) or {}
        projects = call(api.projects) or []
        ids = [p["id"] for p in projects]
        current = st.session_state.get("project_id")
        if current not in ids:
            st.session_state.pop("project_id", None)
            current = None
        src_ok = bool(current)
        dst_ok = bool((session.get("tested") or {}).get("destination"))
        dots.markdown(f'<div class="ma-dots"><span><span class="ma-dot{" ok" if src_ok else ""}"></span>ADF</span>'
                      f'<span><span class="ma-dot{" ok" if dst_ok else ""}"></span>Fabric</span></div>',
                      unsafe_allow_html=True)
        if ids:
            choice = project.selectbox("Project", ["- none -", *ids], label_visibility="collapsed",
                                       index=ids.index(current) + 1 if current else 0)
            if choice != "- none -":
                st.session_state.project_id = choice
            elif current:
                st.session_state.pop("project_id", None)
        demo_ok = st.session_state.get("demo_available", True)
        demo = mode_col.toggle("Demo", value=mode == "demo", disabled=not demo_ok,
                               help="Sample data and canned Azure/Fabric, no sign-in needed." if demo_ok else "Demo data is switched off in production mode.")
        if demo_ok and demo != (mode == "demo"):
            call(api.set_mode, "demo" if demo else "live")
            for k in ("project_id", "conversion", "deploy", "confirmed", "confirmed_loaded", "step"):
                st.session_state.pop(k, None)
            st.rerun()
    st.session_state.mode = mode
    if mode == "demo":  # never let sample data pass for a real result: say so on every page
        theme.html_block('<div class="ma-note" role="status">🧪 <span><strong>DEMO DATA.</strong> Everything on this page is a sample. '
                         'Nothing was read from or sent to Azure or Fabric, and no result here is real.</span></div>')


top_bar()
nav.run()
