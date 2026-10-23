"""The Consult step: how the migration should happen, agreed with the client after discovery and before anything moves.

Only questions about what discovery found are shown, each with a recommendation and the reason for it, and a badge that says
whether the answer drives the run, makes the agent stop for a person, or is only recorded in the brief. Answers are kept by
the API per project; the brief is submitted, then approved by someone who holds the approval permission. Changing an answer
after approval puts the brief back to draft, and the run stops following it until it is approved again.
"""

from __future__ import annotations

import json

import streamlit as st

import theme
from api_client import ApiClient
from components import call, can, empty_state

EFFECT_TONE = {"run": "info", "gate": "warning", "record": "neutral"}
SOURCE_TEXT = {"config": "From the configuration", "person": "Answered", "recommendation": "Recommendation (not yet accepted)", "open": "Open"}
STATE_TONE = {"draft": "neutral", "submitted": "info", "approved": "success"}


def consult(api: ApiClient, pid: str, brief: dict | None) -> None:
    if not brief:
        empty_state("Nothing to ask yet", "Run discovery first: the consultation asks only about what discovery found.")
        return
    c = brief["counts"]
    head = f"Brief v{brief['current_version']['version']} {brief['state']}" if brief["current_version"] else "Draft brief"
    st.markdown(theme.badge(head, STATE_TONE.get(brief["state"], "neutral")), unsafe_allow_html=True)
    st.caption(f"{c['applicable']} questions apply to this estate. Recommendations come from what discovery found, with the reason; "
               "a recommendation is used only once someone accepts it.")
    cols = st.columns(4)
    cols[0].metric("Answered", f"{c['answered']}/{c['applicable']}")
    cols[1].metric("To accept", c["assumed"], help="Recommendations not yet accepted as answers.")
    cols[2].metric("Required and open", c["need_person"])
    cols[3].metric("Conflicts", c["conflicts"])
    if brief.get("changed_since_approval"):
        st.warning("The answers changed since the last approved version, so the run no longer follows a brief. Submit and approve them again.")
    for x in brief["conflicts"]:
        st.error(x)

    _actions(api, pid, brief)
    by_topic: dict[str, list[dict]] = {}
    for q in brief["questions"]:
        by_topic.setdefault(q["topic"], []).append(q)
    for t in brief["topics"]:
        items = by_topic.get(t["id"])
        if not items:
            continue
        open_required = sum(1 for q in items if q["required"] and q["source"] == "open")
        label = f"{t['title']} · {len(items)} question(s)" + (f" · {open_required} open" if open_required else "")
        with st.expander(label, expanded=bool(open_required)):
            st.caption(t["summary"])
            _topic_form(api, pid, t["id"], items, editable=can("plans:generate"))
    _settings(brief)
    _exchange(api, pid, brief)


def _actions(api: ApiClient, pid: str, brief: dict) -> None:
    c = brief["counts"]
    a, b, d = st.columns(3)
    if brief["state"] == "draft":
        if a.button(f"Accept {c['assumed']} recommendation(s)", key="consult-accept", disabled=not (c["assumed"] and can("plans:generate")),
                    help="Records every recommendation not yet answered as the answer. You can change any of them afterwards."):
            if call(api.brief_accept, pid) is not None:
                st.rerun()
        blocked = c["need_person"] or c["conflicts"]
        if b.button("Submit for approval", key="consult-submit", type="primary", disabled=bool(blocked) or not can("plans:generate"),
                    help="Answer the open questions and resolve the conflicts first." if blocked else "Freezes these answers as a version to approve."):
            if call(api.brief_submit, pid) is not None:
                st.rerun()
    elif brief["state"] == "submitted":
        v = brief["current_version"]
        a.caption(f"Version {v['version']} submitted by {v.get('submitted_by') or 'someone'}. It is approved by the signed-in approver, "
                  "and only while the answers stay as submitted.")
        if b.button(f"Approve version {v['version']}", key="consult-approve", type="primary", disabled=not can("provision:approve"),
                    help=None if can("provision:approve") else "Your role cannot approve (needs provision:approve)."):
            if call(api.brief_approve, pid, brief["fingerprint"]) is not None:
                st.rerun()
    else:
        v = brief["current_version"]
        a.success(f"Approved by {v['approved_by']}. The run follows this brief until an answer changes.")
    md = call(api.brief_markdown, pid)
    if md:
        d.download_button("Download brief (Markdown)", md.encode("utf-8"), file_name=f"{pid}-migration-brief.md", mime="text/markdown", key="consult-md")


def _widget(q: dict, key: str):
    opts = [o["value"] for o in q["options"]]
    labels = {o["value"]: o["label"] + (f" — {o['description']}" if o.get("description") else "") for o in q["options"]}
    current = q["value"] if q["source"] in ("person", "recommendation") else None
    if q["kind"] == "text":
        return st.text_input(q["text"], value=current or "", key=key, placeholder=q.get("placeholder") or "", label_visibility="collapsed")
    if q["kind"] == "multi":
        default = [v for v in (current or []) if v in opts]
        return st.multiselect(q["text"], opts, default=default, format_func=lambda v: labels.get(v, v), key=key, label_visibility="collapsed")
    index = opts.index(current) if current in opts else None
    return st.radio(q["text"], opts, index=index, format_func=lambda v: labels.get(v, v), key=key, label_visibility="collapsed")


def _topic_form(api: ApiClient, pid: str, topic: str, items: list[dict], editable: bool) -> None:
    with st.form(f"consult-form-{topic}", border=False):
        chosen: dict[str, object] = {}
        for q in items:
            st.markdown(f"**{theme.esc(q['text'])}** " + theme.badge(q["effect_label"], EFFECT_TONE.get(q["effect"], "neutral"))
                        + ("" if not q["required"] else " " + theme.badge("Required", "neutral")), unsafe_allow_html=True)
            st.caption(q["help"])
            if q["source"] == "config":
                st.info(f"{q['label']} — read from the configuration, not asked. {q['recommendation']['reason'] if q.get('recommendation') else ''}")
                continue
            rec = q.get("recommendation")
            if rec:
                st.caption(f"💡 Recommended: **{rec['label']}** — {rec['reason']}")
            st.caption(f"{SOURCE_TEXT.get(q['source'], q['source'])}" + (f": {q['label']}" if q["source"] == "person" else "")
                       + f" · Changes: {q['applies_to']}")
            chosen[q["id"]] = _widget(q, f"consult-q-{q['id']}")
            st.divider()
        save = st.form_submit_button("Save answers", type="primary", disabled=not editable)
    if save:
        changed = {}
        by_id = {q["id"]: q for q in items}
        for qid, value in chosen.items():
            q = by_id[qid]
            if value is None or value == "" or (q["source"] == "person" and value == q["value"]):
                continue
            if q["source"] == "recommendation" and value == q["value"] and q["kind"] != "text":
                changed[qid] = value  # saving the recommendation as shown makes it an answer
            elif value != q["value"] or q["source"] != "person":
                changed[qid] = value
        if changed and call(api.brief_answers, pid, changed) is not None:
            st.rerun()
        elif not changed:
            st.caption("Nothing changed.")


def _settings(brief: dict) -> None:
    s = brief.get("settings") or brief.get("draft_settings") or {}
    title = "What the run follows (approved)" if brief.get("settings") else "What the run would follow once approved"
    with st.expander(title, expanded=bool(brief.get("settings"))):
        rows = [
            ("Pipelines", "All" if s.get("pipelines") is None else f"{len(s['pipelines'])}: " + ", ".join(s["pipelines"][:8]) + (" …" if len(s["pipelines"]) > 8 else "")),
            ("Repoint storage to lakehouse", s.get("lakehouse") or "No (keep connections)"),
            ("Apply reconstruction proposals", "Yes" if s.get("apply_reconstruction") else "No"),
            ("Notebooks to Fabric Spark", "Yes" if s.get("workloads_to_fabric") else "No (keep the source compute)"),
            ("Schedules", "Created switched off" if s.get("schedules") else "Left out"),
            ("Validation stages", ", ".join(s.get("validate_stages") or [])),
            ("Real runs in Fabric", "Allowed" if s.get("allow_real_runs") else "Not allowed"),
            ("Commit to Git", "Yes" if s.get("git_commit") else "No"),
        ]
        st.dataframe([{"Setting": k, "Value": v} for k, v in rows], hide_index=True)
        for p in s.get("prerequisites") or []:
            st.warning(f"Prerequisite: {p}")


def _exchange(api: ApiClient, pid: str, brief: dict) -> None:
    with st.expander("Share with the client: questionnaire, import, notes"):
        q = call(api.brief_questionnaire, pid)
        if q:
            st.download_button("Questionnaire for the client (JSON)", json.dumps(q, indent=2).encode("utf-8"), file_name=f"{pid}-questionnaire.json",
                               mime="application/json", key="consult-q-json")
        up = st.file_uploader("Import a filled questionnaire", type=["json"], key="consult-import", disabled=not can("plans:generate"))
        if up is not None and st.button("Import answers", key="consult-import-go"):
            try:
                payload = json.loads(up.getvalue().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                st.error("That file is not a questionnaire exported from here (JSON).")
            else:
                r = call(api.brief_import, pid, payload)
                if r is not None:
                    st.success(f"Imported {r['imported']} answer(s). Review them before submitting.")
        note = st.text_input("Add a note to the brief", key="consult-note", max_chars=2000)
        if st.button("Add note", key="consult-note-go", disabled=not (note.strip() and can("plans:generate"))):
            if call(api.brief_note, pid, note) is not None:
                st.rerun()
        for n in reversed(brief.get("notes") or []):
            st.caption(f"{n['at']} · {n.get('by') or n['kind']}: {n['text']}")
