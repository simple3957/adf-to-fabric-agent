"""Migration: the steps left to right, the selected step below, and a bar that says where it stands and how to move on."""

import csv
import io

import streamlit as st

import panels
import theme
from components import call, client
from journey import STEPS, connect_facts, convert_facts, deploy_facts, discover_facts, done_count, first_open, statuses

api = client()
pid = st.session_state.get("project_id")
if not pid:
    theme.html_block('<div class="ma-placeholder">No project yet. Connect the ADF source and Microsoft Fabric on the '
                     '<b>Connections</b> page, then start the migration.</div>')
    st.page_link(st.session_state.pages["setup"], label="Go to Connections", icon="🔗")
    st.stop()

# ---- facts from the API ---------------------------------------------------------------------------------------------------
project = call(api.project, pid)
discovered = bool(project and project.get("has_semantic"))
found = call(api.linked_services, pid) if discovered else None
cards = call(api.cards, pid) if discovered else None
session = call(api.session) or {}
dest = session.get("destination") or {}
conversion = st.session_state.get("conversion", {}).get(pid)
deployed = st.session_state.get("deploy", {}).get(pid)

facts = {"discover": discover_facts(project, found), "connect": connect_facts(cards),
         "convert": convert_facts(conversion), "deploy": deploy_facts(deployed)}
confirmed: set = st.session_state.setdefault("confirmed", {}).setdefault(pid, set())
steps = statuses(facts, confirmed)
by_key = {s.step.key: s for s in steps}
selected = st.session_state.get("step", {}).get(pid)
if selected not in by_key or by_key[selected].locked:
    selected = first_open(steps)

# ---- layout: stepper on the left, the open step on the right ---------------------------------------------------------------
done = done_count(steps)
left, right = st.columns([0.95, 3.3], gap="large")
with left, st.container(key="stepper"):
    theme.html_block(theme.ring(done, len(steps), (project or {}).get("name") or pid, "ADF → Microsoft Fabric"))
    for s in steps:
        variant = "locked-" if s.locked else ("sel-" if s.step.key == selected else "")
        last = "-last" if s.index == len(steps) - 1 else ""
        with st.container(key=f"vstep-{variant}{s.step.key}{last}"):
            dot = "done" if s.confirmed and not s.stale else ("attention" if s.tone == "attention" else
                                                              "current" if not s.locked else "")
            mark = "✓" if dot == "done" else ("🔒" if s.locked else str(s.index + 1))
            warn = " ⚠️" if s.facts.attention and not s.locked else ""
            theme.html_block(f'<div class="ma-vrow"><span class="ma-vdot {dot}">{mark}</span><div>'
                             f'<div class="ma-vtitle">{theme.esc(s.step.title)}{warn}</div>'
                             f'<div class="ma-vmetric">{theme.esc(s.step.purpose if s.locked else s.facts.metric)}</div>'
                             f'<span class="ma-vstate {s.tone}">{theme.esc(s.label)}</span></div></div>')
            if s.step.key != selected and st.button("Open step →", key=f"open-{s.step.key}", disabled=s.locked, type="tertiary",
                                                    help="Confirm the step before to open this one" if s.locked else s.step.purpose):
                st.session_state.setdefault("step", {})[pid] = s.step.key
                st.rerun()

with right:
    # ---- completion -----------------------------------------------------------------------------------------------------------
    if done == len(steps):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["pipeline", "conversion status", "ready to run", "deploy action", "item id", "detail"])
        deploys = {r["name"]: r for r in (deployed or {}).get("results", [])}
        for p in (conversion or {}).get("pipelines", []):
            d = deploys.get(p["name"], {})
            w.writerow([p["name"], p["status"], p["runtime_ready"], d.get("action", ""), d.get("item_id", ""), d.get("detail", "")])
        with st.container(key="done"):
            st.markdown("### 🎉 Migration complete")
            st.caption(facts["deploy"].outcome)
            st.download_button("Download report (CSV)", buf.getvalue().encode("utf-8-sig"), file_name=f"{pid}-migration-report.csv",
                               mime="text/csv")

    # ---- selected step --------------------------------------------------------------------------------------------------------
    cur = by_key[selected]
    with st.container(key="panel"):
        t, r = st.columns([4, 1], vertical_alignment="center")
        t.markdown(f'<div class="ma-panel-title"><span class="n">{cur.index + 1}</span><div><span class="ma-eyebrow">'
                   f'{theme.esc(cur.label)}</span><h3>{theme.esc(cur.step.title)}</h3></div></div>', unsafe_allow_html=True)
        t.caption(cur.step.purpose)
        with r.popover("Reset step", disabled=not (cur.confirmed or cur.facts.complete)):
            later = [s.step.title for s in steps[cur.index + 1:] if s.confirmed]
            st.write("This clears the confirmation of this step" + (f" and of: {', '.join(later)}." if later else "."))
            if cur.step.key in ("convert", "deploy"):
                st.write("It also forgets this session's results for the step. Files in the project are kept.")
            if st.button("Reset", type="primary", key="reset-go"):
                for s in steps[cur.index:]:
                    confirmed.discard(s.step.key)
                if cur.step.key == "convert":
                    st.session_state.get("conversion", {}).pop(pid, None)
                if cur.step.key in ("convert", "deploy"):
                    st.session_state.get("deploy", {}).pop(pid, None)
                st.rerun()

        if selected == "discover":
            panels.discover(api, pid, project, found)
        elif selected == "connect":
            panels.connect(api, pid, cards, dest.get("workspace_id"))
        elif selected == "convert":
            panels.convert(api, pid, conversion)
        else:
            panels.deploy(api, pid, dest, conversion, deployed)

        # ---- next bar: where the step stands, and the way forward ----------------------------------------------------------------
        nxt = steps[cur.index + 1] if cur.index + 1 < len(steps) else None
        prev = steps[cur.index - 1] if cur.index > 0 else None
        f = cur.facts
        if cur.confirmed and not cur.stale:
            icon, title, detail, action, enabled = "✅", f"{cur.step.title} confirmed", f.outcome or "Reviewed and confirmed.", \
                (f"Go to {nxt.step.title} →" if nxt else None), True
        elif f.complete:
            icon = "⚠️" if f.attention else "✅"
            title = f"{cur.step.title} finished" + (f" · {f.attention}" if f.attention else "")
            detail = (f"{f.outcome} Review the issues above, or continue and come back to them." if f.attention
                      else f"{f.outcome} If everything looks right, confirm it to move on.")
            action = (f"Continue to {nxt.step.title} anyway →" if f.attention else f"Looks good, continue to {nxt.step.title} →") \
                if nxt else "Finish migration"
            enabled = True
        else:
            icon = "⚠️" if cur.stale else "○"
            title = f"{cur.step.title} needs to run again" if cur.stale else f"{cur.step.title} is not finished"
            detail = ("Confirmed earlier, but its results are gone. " if cur.stale else "") + f.waiting
            action, enabled = (f"Continue to {nxt.step.title} →" if nxt else "Finish migration"), False
        with st.container(key="nextbar"):
            a, b, c = st.columns([5, 1.4, 2.2], vertical_alignment="center")
            a.markdown(f'<div class="ma-next"><span class="ic">{icon}</span><div><strong>{theme.esc(title)}</strong>'
                       f'<span class="muted">{theme.esc(detail)}</span></div></div>',
                       unsafe_allow_html=True)
            if prev and b.button(f"← {prev.step.title}", key="go-prev", type="tertiary"):
                st.session_state.setdefault("step", {})[pid] = prev.step.key
                st.rerun()
            if action and c.button(action, key="go-next", type="primary", disabled=not enabled, help=None if enabled else f.waiting,
                                   width="stretch"):
                confirmed.add(cur.step.key)
                if nxt:
                    st.session_state.setdefault("step", {})[pid] = nxt.step.key
                st.rerun()
