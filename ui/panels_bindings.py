"""The ids a person must supply by hand after conversion, asked for where they are needed (the Convert step).

A notebook, Spark job, dataflow or free connection (Invoke pipeline, a Web base URL) an activity needs only has an id once it exists in Fabric. The
converter leaves a placeholder; here the person picks the Fabric item (listed live) or pastes its id, the server checks it exists, and the next
Convert uses it. A connection that belongs to a linked service is set on the Connect step; child pipelines and the workspace are filled in at deploy.
"""

from __future__ import annotations

import streamlit as st

from api_client import ApiClient
from components import call, can, read
from panels_ext import _plain

CHOOSE = "- choose a Fabric item -"


def section(api: ApiClient, pid: str, unbound: list[str]) -> None:
    """``unbound``: names the conversion still reports as open (some belong to the Connect step, not to this list)."""
    got = read(api, "bindings", pid) or {}
    if got.get("workspace_note"):  # bindings made for one workspace, the person is working in another
        st.warning(got["workspace_note"])
    items = got.get("items") or []
    mine = {i["name"] for i in items} | {i["key"] for i in items}
    elsewhere = [u for u in unbound if u not in mine and not u.startswith("connection:") and u != "workspace"]
    open_ = [i for i in items if not i["bound"]]
    if not items and not elsewhere:
        return
    if note := st.session_state.pop("bind-note", None):
        st.warning(note)
    if done := st.session_state.pop("bind-done", None):
        st.success(done)
    generated = {n["name"] for n in (read(api, "workload_notebooks", pid) or {}).get("items") or [] if n.get("generated")}
    st.markdown(f"#### Things you need to set · {len(open_)} open" + (f" · {len(items) - len(open_)} set" if len(items) > len(open_) else ""))
    st.caption("These ids only exist once the item is in Fabric. Pick it from the list (read live from your workspace), paste its id, or create a "
               "generated notebook in Fabric from here; then Convert again.")
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
                same_name = next((n for n in names if n.startswith(f"{i['name']} · ")), None)  # the item the pipelines were pointed at by name
                picked = st.selectbox(f"{i['label']} in Fabric", [CHOOSE, *names], index=([CHOOSE, *names].index(same_name) if same_name else 0),
                                      key=f"bind-pick-{i['key']}") if names else CHOOSE
                pasted = st.text_input("…or paste its id", value="" if names else (i.get("value") or ""), key=f"bind-id-{i['key']}",
                                       help="Fabric: open the item, the id is the last GUID in the address bar (connections: Settings > Manage connections and gateways).")
                if st.form_submit_button("Save", disabled=not can("plans:generate")):
                    value = pasted.strip() or names.get(picked, "")
                    if not value:
                        st.warning("Choose an item or paste its id.")
                    elif (done := call(api.set_binding, pid, i["key"], value)) is not None:
                        if done.get("verified") is False:  # kept across the rerun below
                            st.session_state["bind-note"] = f"'{i['name']}': that connection id is not among the connections this account can see. It is saved; the deploy fails if it does not exist."
                        st.rerun()
            if i["kind"] == "notebook" and i["name"] in generated:
                _create(api, pid, i["name"])
    if elsewhere:
        st.info("Set on the **Connect** step (a linked service's connection; one you skipped there stays open): " + ", ".join(f"`{u}`" for u in elsewhere))


def _create(api: ApiClient, pid: str, name: str) -> None:
    """Create the generated notebook in the chosen workspace and save its id. The first click asks Fabric what would happen; the second, after
    the person reads it, creates it and records them as accepting the generated notebook."""
    key = f"nb-create-{pid}-{name}"
    ask = st.session_state.get(key)
    allowed = can("migration:execute")
    if not ask:
        if st.button("Create it in Fabric from the generated notebook", key=f"{key}-go", disabled=not allowed,
                     help="Puts the generated notebook into the chosen workspace and saves its id here. You are asked to confirm first."
                     if allowed else "Creating items in Fabric needs migration:execute."):
            if (r := call(api.create_workload_notebook, pid, name)) is not None:
                st.session_state[key] = (r["detail"], bool(r.get("exists")))
                st.rerun()
        return
    detail, exists = ask
    verb = "Replace the content of" if exists else "Create"
    st.warning(f"{_plain(detail)}. {verb} '{_plain(name)}' in the chosen workspace and use its id? You are recorded as accepting the generated notebook.")
    yes, no = st.columns(2)
    if yes.button("Yes, replace it" if exists else "Yes, create", type="primary", key=f"{key}-yes", disabled=not (allowed and can("provision:approve")),
                  help=None if can("provision:approve") else "Accepting a generated notebook needs provision:approve."):
        st.session_state.pop(key, None)
        if (r := call(api.create_workload_notebook, pid, name, True)) is not None:
            st.session_state["bind-done"] = f"{name}: {r['detail']}"
        st.rerun()
    if no.button("Cancel", key=f"{key}-no"):
        st.session_state.pop(key, None)
        st.rerun()

