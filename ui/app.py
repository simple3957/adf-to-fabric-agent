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


def top_bar() -> None:
    api = client()
    with st.container(key="topbar"):
        brand, links, dots, project, mode_col = st.columns([2.0, 4.4, 1.4, 2.0, 1.3], vertical_alignment="center")
        brand.markdown('<div class="ma-brand"><span class="ma-brand-mark">◆</span>ADF&nbsp;→&nbsp;Fabric&nbsp;<small>Studio</small></div>',
                       unsafe_allow_html=True)
        with links, st.container(horizontal=True, gap="small", vertical_alignment="center"):
            for key in ("setup", "migration", "activity"):
                st.page_link(PAGES[key], width="content")
        try:
            api.health()
        except ApiError as err:
            st.markdown(f'<div class="ma-note">⚠️ <span><strong>The accelerator service is not reachable.</strong> '
                        f'{theme.esc(err.message)} {theme.esc(" ".join(err.actions))}</span></div>', unsafe_allow_html=True)
            st.stop()

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
        demo = mode_col.toggle("Demo", value=mode == "demo", help="Sample data and canned Azure/Fabric, no sign-in needed.")
        if demo != (mode == "demo"):
            call(api.set_mode, "demo" if demo else "live")
            for k in ("project_id", "conversion", "deploy", "confirmed", "step"):
                st.session_state.pop(k, None)
            st.rerun()
    st.session_state.mode = mode


top_bar()
nav.run()
