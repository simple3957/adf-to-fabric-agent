"""Job history from the API (kept in API memory: a restart clears it)."""

import streamlit as st

from components import call, client, current_project

st.title("Activity")
st.caption("Jobs live in the API's memory, so they disappear when the API restarts. Project files stay.")
api = client()
only = st.checkbox("Only the current project", value=bool(current_project()))
jobs = call(api.jobs, current_project() if only else None) or []
if not jobs:
    st.info("No jobs yet.")
for j in jobs:
    icon = {"succeeded": "✅", "failed": "❌", "running": "⏳", "queued": "⏳"}.get(j["state"], "")
    with st.expander(f"{icon} {j['kind']} · {j.get('project_id') or '-'} · {j['id']}"):
        if j.get("error"):
            st.error(j["error"])
        for e in j.get("events", []):
            st.write(f"`{e['t']:>6}s` {e['message']}")
        if j.get("result"):
            st.json(j["result"], expanded=False)
