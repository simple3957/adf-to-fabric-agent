"""Model-assisted drafts: optional, review-gated, never authoritative. Shown on the Convert step only when the converter left something for a person.

A model drafts corrected ADF source for a pipeline the converter could not finish; the person sees exactly what would be sent, the draft next to the
original, the automatic checks, and accepts it or not. Nothing uses a draft until `Convert` is run with the 'use accepted drafts' option, the original file
is never changed, and the deterministic converter still produces every Fabric file. The model's words are shown as plain text, never as markup.
"""

from __future__ import annotations

import streamlit as st

from api_client import ApiClient
from components import call, can

LEVEL = {"error": st.error, "warning": st.warning, "ok": st.success}


def _overview(api: ApiClient, pid: str) -> dict | None:
    return call(api.assist, pid)


def accepted(api: ApiClient, pid: str) -> list[str]:
    """Pipelines with an accepted draft that is still current (the ones 'use accepted drafts' would convert)."""
    o = _overview(api, pid)
    items = ((o or {}).get("candidates") or {}).get("items") or []
    return [i["pipeline"] for i in items if i["state"] == "accepted" and not i.get("stale")]


def section(api: ApiClient, pid: str) -> None:
    o = _overview(api, pid)
    found = (o or {}).get("candidates")
    if not o or not found or not found["items"]:
        return
    s, items = o["settings"], found["items"]
    sm = found["summary"]
    with st.expander(f"Model-assisted drafts (optional) · {sm['total']} pipeline(s) left for a person · {sm['drafts']} draft(s) · {sm['accepted']} accepted"):
        st.caption("A model can draft corrected ADF source for a pipeline the converter could not finish. You see exactly what is sent (secrets removed), the draft next to the "
                   "original and the automatic checks, and nothing uses it until you accept it and convert with the drafts switched on. The original file is never changed.")
        if not s["enabled"]:
            st.info(s["disabled_reason"])
        elif not s["available"]:
            st.warning(s["message"])
        st.dataframe([{"pipeline": i["pipeline"], "problems": len(i["problems"]), "draft": i["state"] + (" (stale)" if i.get("stale") else ""),
                       "confidence": i.get("confidence") or "-"} for i in items], hide_index=True)
        name = st.selectbox("Pipeline", [i["pipeline"] for i in items], key=f"assist-pick-{pid}")
        _one(api, pid, name, s, can("plans:generate"))


def _one(api: ApiClient, pid: str, name: str, settings: dict, editable: bool) -> None:
    d = call(api.assist_detail, pid, name)
    if not d:
        return
    st.markdown("**What the converter could not do**")
    for p in d["source"]["problems"]:
        st.text(p)
    a, b, c, e = st.columns(4)
    if a.button("Preview what is sent", key=f"assist-prev-{pid}-{name}"):
        pr = call(api.assist_prompt, pid, name)
        if pr:
            st.session_state[f"assist-prompt-{pid}-{name}"] = pr["prompt"]
    ready = settings["enabled"] and settings["available"] and editable and d["state"] not in ("queued", "running")
    if b.button("Generate draft", key=f"assist-gen-{pid}-{name}", type="primary", disabled=not ready,
                help=None if ready else "Switch ADF2FABRIC_LLM=on, install and sign in to Claude Code, and hold the 'plans:generate' permission."):
        if call(api.assist_generate, pid, [name]) is not None:
            st.rerun()
    if c.button("Refresh", key=f"assist-ref-{pid}-{name}"):
        st.rerun()
    if e.button("Discard everything", key=f"assist-del-{pid}-{name}", disabled=not editable or d["state"] == "none"):
        if call(api.assist_discard, pid, name, "all") is not None:
            st.rerun()
    shown = st.session_state.get(f"assist-prompt-{pid}-{name}")
    if shown:
        st.caption(f"This is the whole prompt ({len(shown):,} characters). Nothing else is sent: no credentials, no connection strings, no data.")
        st.code(shown, language="markdown")
    if d["state"] in ("queued", "running"):
        st.info("A draft is being made. It usually takes 20 to 90 seconds; use Refresh.")
    if d["state"] == "failed":
        st.error(d["error"])
    draft = d.get("draft")
    if not draft:
        if d.get("accepted"):
            _accepted(d["accepted"])
        return
    if draft.get("stale"):
        st.warning("The source changed after this draft was made. Generate a new one: this one cannot be accepted.")
    st.markdown(f"**Draft** · confidence **{draft['confidence']}** · {draft.get('model') or 'model'} · {int(draft.get('duration_ms') or 0) // 1000}s")
    for n in draft["notes"]:
        st.text(f"Change: {n}")
    for u in draft["unconverted"]:
        st.warning("Left for a person: " + u)
    for ch in draft["checks"]:
        LEVEL[ch["level"]](ch["message"])
    left, right = st.columns(2)
    left.markdown("**Original source** (secrets removed)")
    left.code(d["source"]["code"], language="json")
    right.markdown("**Draft** (edit it, then accept)")
    edited = right.text_area("Draft", draft["code"], height=420, key=f"assist-code-{pid}-{name}-{draft['generated_at']}", label_visibility="collapsed")
    x, y = st.columns(2)
    if x.button("Accept" + (" (edited)" if edited.strip() != draft["code"].strip() else ""), key=f"assist-acc-{pid}-{name}", type="primary",
                disabled=not editable or bool(draft.get("stale"))):
        if call(api.assist_accept, pid, name, edited if edited.strip() != draft["code"].strip() else None) is not None:
            st.rerun()
    if y.button("Discard draft", key=f"assist-dd-{pid}-{name}", disabled=not editable):
        if call(api.assist_discard, pid, name, "draft") is not None:
            st.rerun()


def _accepted(acc: dict) -> None:
    st.success(f"Accepted by {acc.get('accepted_by') or 'someone'} on {acc.get('accepted_at') or '?'}"
               f"{' (edited by the reviewer)' if acc.get('edited') else ''}. Convert with the accepted drafts switched on to use it."
               + (" It is out of date: the source changed since, so it will be ignored." if acc.get("stale") else ""))
