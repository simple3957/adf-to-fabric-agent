"""Waves and execution order: what depends on what, in the order things are created, laid out like the Synapse accelerator's Waves panel.

A summary strip, the waves as rows (what each holds, sized to the largest), the order pipelines are deployed in (children before the
pipelines that call them), and an explorer: filter the objects and pick one to see what it needs and what needs it. Everything comes
from the API, which builds it from the resolved references; nothing is worked out here.
"""

from __future__ import annotations

import streamlit as st

import theme
from api_client import ApiClient
from components import call, can, empty_state, intent, run_job

#: Most objects listed at once; beyond it a list is unreadable. The most connected come first, as in the accelerator's graph.
MAX_LIST = 250


def waves_panel(api: ApiClient, pid: str) -> None:
    data = call(api.waves, pid)
    if not data or not data.get("available"):
        empty_state("Waves are not built yet", (data or {}).get("needs", "Resolve the dependencies first."))
        if st.button("Resolve dependencies", key="waves-resolve", type="primary", disabled=not can("discovery:run")):
            if (job := run_job(lambda: api.run_stage(pid, "dependencies", idempotency_key=intent("dependencies")), "Resolving dependencies",
                               intent_name="dependencies")) and job["state"] == "succeeded":
                st.rerun()
        return
    s = data["stats"]
    st.markdown(theme.stats([
        ("Objects", f"{s['objects']:,}", "", ""),
        ("Dependencies", f"{s['dependencies']:,}", "", "Links between discovered objects"),
        ("Waves", s["waves"], "accent", "Nothing is created before what it needs"),
        ("Connected", f"{s['with_dependencies']:,}", "", "Objects with at least one link"),
        ("Circular groups", s["cycles"], "error" if s["cycles"] else "ok", "Pipelines that call each other" if s["cycles"] else "None"),
    ]), unsafe_allow_html=True)
    if s["cycles"]:
        st.error("Some pipelines call each other, so no order puts them all after what they need. Break the cycle in the source first.")

    t_waves, t_order, t_explore = st.tabs([f"Waves ({s['waves']})", f"Execution order ({len(data['order'])})", "Explorer"])
    with t_waves:
        _waves(data)
    with t_order:
        _order(data)
    with t_explore:
        _explorer(data)


def _waves(data: dict) -> None:
    st.markdown(theme.wave_rows(data["waves"]), unsafe_allow_html=True)
    st.caption(data["note"])
    labels = {f"Wave {w['number']} · {w['kind']} ({w['count']})": w for w in data["waves"]}
    pick = st.selectbox("Objects in a wave", list(labels), key="waves-pick")
    w = labels[pick]
    st.caption(w["note"])
    st.dataframe([{"Object": x} for x in w["items"]], hide_index=True, height=min(360, 40 + 35 * len(w["items"])))
    if w["count"] > len(w["items"]):
        st.caption(f"Showing {len(w['items'])} of {w['count']:,}.")


def _order(data: dict) -> None:
    st.caption("Pipelines are deployed top to bottom: a pipeline comes after every pipeline it calls. The order inside a wave is alphabetical, "
               "and nothing in a wave depends on another in the same wave.")
    from components import PSTATE_ICON, PSTATE_LABEL

    rows = [{"#": o["position"], "Pipeline": o["pipeline"], "Wave": o["wave"], "Calls first": ", ".join(o["calls"]) or "-",
             "Called by": ", ".join(o["called_by"]) or "-", "Ready": "yes" if o["ready"] else "blocked",
             "Waiting for": ", ".join(o["blocked_by"]) or "", "In a cycle": "yes" if o["in_cycle"] else "",
             "State": f"{PSTATE_ICON.get(o['state'] or '', '')} {PSTATE_LABEL.get(o['state'] or '', '')}".strip()} for o in data["order"]]
    if rows:
        st.dataframe(rows, hide_index=True)
    else:
        st.caption("No pipelines.")
    blocked = [o for o in data["order"] if not o["ready"]]
    if blocked:
        st.warning(f"{len(blocked)} pipeline(s) wait for something that is not resolved yet (see 'Waiting for').")


def _explorer(data: dict) -> None:
    a, b, c = st.columns([2, 1.3, 1])
    term = a.text_input("Search objects", key="waves-search", placeholder="Search objects", label_visibility="collapsed").strip().lower()
    kind = b.selectbox("Object type", ["All object types", *data["types"]], key="waves-type", label_visibility="collapsed")
    wave = c.selectbox("Wave", ["All waves", *[f"Wave {w['number']}" for w in data["waves"]]], key="waves-wave", label_visibility="collapsed")
    nodes = [n for n in data["nodes"] if (not term or term in n["name"].lower()) and (kind.startswith("All") or n["type"] == kind)
             and (wave.startswith("All") or f"Wave {n['wave']}" == wave)]
    nodes.sort(key=lambda n: -(len(n["needs"]) + len(n["needed_by"])))  # connected objects first: an isolated one says nothing about dependencies
    shown = nodes[:MAX_LIST]
    if len(nodes) > MAX_LIST:
        st.info(f"Showing the {MAX_LIST} most connected of {len(nodes):,} matching objects. Narrow the filters to see others.")
    if data.get("truncated"):
        st.caption(f"{data['truncated']:,} more objects exist than this view carries.")
    if not shown:
        empty_state("No objects match these filters", "")
        return
    left, right = st.columns([1.6, 1])
    with left:
        st.dataframe([{"Object": n["name"], "Type": n["type"].replace("_", " "), "Wave": n["wave"], "Needs": len(n["needs"]), "Needed by": len(n["needed_by"])}
                      for n in shown], hide_index=True, height=420)
    with right:
        names = [f"{n['name']} ({n['type'].replace('_', ' ')})" for n in shown]
        chosen = st.selectbox("Selected object", names, key="waves-selected")
        n = shown[names.index(chosen)]
        st.markdown(f'<div class="ma-eyebrow">Selected</div><h3 style="overflow-wrap:anywhere">{theme.esc(n["name"])}</h3>', unsafe_allow_html=True)
        tags = f"{n['type'].replace('_', ' ')} · Wave {n['wave']}"
        st.caption(tags)
        if n.get("state"):
            from components import PSTATE_ICON, PSTATE_LABEL
            st.markdown(theme.badge(PSTATE_LABEL.get(n["state"], n["state"]), "info"), unsafe_allow_html=True)
        elif n.get("category"):
            st.markdown(theme.badge(n["category"].replace("_", " "), "warning" if n.get("blocks") else "neutral"), unsafe_allow_html=True)
        for title, items in (("Needs", n["needs"]), ("Needed by", n["needed_by"])):
            st.markdown(f'<div class="ma-eyebrow">{title} ({len(items)})</div>', unsafe_allow_html=True)
            if items:
                st.markdown('<ul class="ma-need-list">' + "".join(f"<li>{theme.esc(x)}</li>" for x in items[:30]) + "</ul>", unsafe_allow_html=True)
                if len(items) > 30:
                    st.caption(f"and {len(items) - 30} more")
            else:
                st.caption("Nothing")
