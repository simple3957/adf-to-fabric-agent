"""Migration confidence: a range per area (and per item) from the criteria the project's records meet, and the questions that would narrow it.

The low end counts only what is evidenced; the high end also counts what nobody has checked. Everything is worked out by the API from what the earlier
stages recorded; nothing is computed here.
"""

from __future__ import annotations

import streamlit as st

import theme
from api_client import ApiClient
from components import call

TONE = {"proven": "success", "unproven": "info", "needs_input": "warning", "failing": "error", "blocked": "error"}
STATE_ICON = {"met": "✅", "unmet": "❌", "unknown": "❔"}


def confidence_panel(api: ApiClient, pid: str) -> None:
    data = call(api.confidence, pid)
    if not data:
        return
    if not data.get("ready"):
        st.caption(data.get("message", "Convert first: confidence is judged on what conversion produced."))
        return
    o = data.get("overall")
    st.markdown("##### How much of this is established")
    if o:
        st.markdown(theme.stats([
            ("Established (low)", f"{o['low']}%", "ok" if o["low"] >= 90 else "", "Only what has evidence"),
            ("If open questions go well (high)", f"{o['high']}%", "accent", "What is unproven counts as met"),
            ("Still assumed", f"{o['gap']} pts", "error" if o["gap"] > 25 else "", "The gap between the two"),
            ("Checked", f"{o['known']}%", "", "Met or shown unmet"),
        ]), unsafe_allow_html=True)
        st.markdown(theme.badge(o["label"].replace("_", " "), TONE.get(o["label"], "neutral")), unsafe_allow_html=True)
    if data.get("stale"):
        st.warning("The source changed after the conversion, so nothing proven earlier is assumed to hold. Run discovery and convert again.")
    st.caption(data["note"])
    rows = [{"Area": a["title"], "Range": f"{a['low']}% to {a['high']}%", "Checked": f"{a['known']}%",
             "Items": ", ".join(f"{n} {k.replace('_', ' ')}" for k, n in a["labels"].items())} for a in data["areas"].values()]
    st.dataframe(rows, hide_index=True)
    if data.get("absent_areas"):
        st.caption("Not in this estate, so not scored: " + ", ".join(data["absent_areas"]) + ".")
    if data["questions"]:
        st.markdown("##### What to find out first")
        for q in data["questions"][:8]:
            with st.expander(f"{q['question']}  ·  +{q['raises_low_by']} low, +{q['raises_high_by']} high"):
                st.markdown(f"**Why it matters.** {q['why']}")
                if q.get("do"):
                    st.markdown(f"**Do.** {q['do']}")
                st.caption(f"{q['count']} item(s): " + ", ".join(q["items"][:8]))
    for a in data["areas"].values():
        with st.expander(f"{a['title']}: each item's criteria"):
            for it in a["items"]:
                st.markdown(f"**{it['name']}**: {it['low']}% to {it['high']}% ({it['label'].replace('_', ' ')})")
                st.dataframe([{"": STATE_ICON.get(c["state"], ""), "Criterion": c["label"], "Evidence": c["evidence"]} for c in it["criteria"]], hide_index=True)
