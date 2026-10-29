"""The ids a person must supply by hand after conversion, asked for where they are needed (the Convert step).

A notebook, Spark job, dataflow or free connection (Invoke pipeline, a Web base URL) an activity needs only has an id once it exists in Fabric. The
converter leaves a placeholder; here the person picks the Fabric item (listed live) or pastes its id, the server checks it exists, and the next
Convert uses it. A connection that belongs to a linked service is set on the Connect step; child pipelines and the workspace are filled in at deploy.
"""

from __future__ import annotations

import streamlit as st

from api_client import ApiClient
from components import call, can

CHOOSE = "- choose a Fabric item -"


def section(api: ApiClient, pid: str, unbound: list[str]) -> None:
    """``unbound``: names the conversion still reports as open (some belong to the Connect step, not to this list)."""
    got = call(api.bindings, pid) or {}
    items = got.get("items") or []
    mine = {i["name"] for i in items} | {i["key"] for i in items}
    elsewhere = [u for u in unbound if u not in mine and not u.startswith("connection:")]
    open_ = [i for i in items if not i["bound"]]
    if not items and not elsewhere:
        return
    st.markdown(f"#### Things you need to set · {len(open_)} open" + (f" · {len(items) - len(open_)} set" if len(items) > len(open_) else ""))
    st.caption("These ids only exist once the item is in Fabric. Pick it from the list (read live from your workspace) or paste its id, then Convert again.")
    if got.get("fabric_note") and open_:
        st.info(got["fabric_note"])
    for i in items:
        mark = "✅" if i["bound"] else ("⏳" if i.get("value") else "⬜")
        with st.expander(f"{mark} {i['label']}: {i['name']}", expanded=not i["bound"]):
            if i.get("detail"):
                st.caption(i["detail"])
            if i.get("needed_by"):
                st.caption("Needed by: " + ", ".join(f"`{n}`" for n in i["needed_by"]))
            if i["bound"]:
                st.success("Set and used by the last conversion.")
            elif i.get("value"):
                st.info("Saved. Convert again to use it.")
            names = {f"{c['name']} · {c['id'][:8]}…": c["id"] for c in i.get("candidates") or []}
            with st.form(f"bind-{i['key']}", border=False):
                picked = st.selectbox(f"{i['label']} in Fabric", [CHOOSE, *names], key=f"bind-pick-{i['key']}") if names else CHOOSE
                pasted = st.text_input("…or paste its id", value="" if names else (i.get("value") or ""), key=f"bind-id-{i['key']}",
                                       help="Fabric: open the item, the id is the last GUID in the address bar (connections: Settings > Manage connections and gateways).")
                if st.form_submit_button("Save", disabled=not can("plans:generate")):
                    value = pasted.strip() or names.get(picked, "")
                    if not value:
                        st.warning("Choose an item or paste its id.")
                    elif call(api.set_binding, pid, i["key"], value) is not None:
                        st.rerun()
    if elsewhere:
        st.info("Set on the **Connect** step (they belong to linked services): " + ", ".join(f"`{u}`" for u in elsewhere))
