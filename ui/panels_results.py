"""Migration results after deploy: each pipeline's deploy, its latest Fabric run, what it was compared with, and what it moved beside the
baseline (ADF's last successful run, or an earlier Fabric run), down to each activity. Read from the project's records; starts nothing."""

from __future__ import annotations

import csv
import io
from datetime import datetime

import streamlit as st

import theme
from components import read
from panels_ext import _plain

MOVED = (("rowsRead", "Rows read"), ("rowsCopied", "Rows copied"), ("filesRead", "Files read"), ("filesWritten", "Files written"))
RESULT_ICON = {"matches": "✅ Matches", "partial": "🟡 Partly compared", "differs": "🟠 Differs", "no_baseline": "⬜ Nothing to compare with",
               "failed": "❌ Run failed", "same_failure": "❌ Failed, as in ADF", "not_run": "⬜ Not run"}
BASELINE = {"adf": "ADF run", "fabric": "Earlier Fabric run"}


def _day(stamp: object) -> str:
    try:
        return datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).strftime("%d %b %H:%M")
    except ValueError:
        return ""


def _num(v: object) -> object:
    return "" if v is None else v


def _pipeline_rows(rows: list[dict]) -> list[dict]:
    out = []
    for p in rows:
        fab, base = p.get("fabric_moved") or {}, p.get("baseline_moved") or {}
        oc = p.get("output_checks") or []
        out.append({
            "Pipeline": p["pipeline"],
            "Deployed": ("❌ failed" if p["deploy_failed"] else f"✅ {_day(p['deployed_at'])}" if p["deployed"] else "not deployed"),
            "Fabric run": (f"{p['run_status']} {_day(p['ran_at'])}".strip() + (" (before the latest deploy)" if p["stale_run"] else "")) if p["run_status"] else "-",
            "Compared with": f"{BASELINE.get(p['baseline'], '-')} {_day(p['baseline_at'])}".strip() + (" (chosen)" if p.get("baseline_chosen") else ""),
            "Result": RESULT_ICON.get(p["verdict"], p["result"]),
            "Steps OK": f"{p['activities_succeeded']}/{p['activities_total']}" if p["activities_total"] else "-",
            "Checks matched": p["checks_matched"], "Differences": p["differences"],
            "Rows copied (baseline)": _num(base.get("rowsCopied")) if base else ("not kept" if not p["baseline_figures"] else ""),
            "Rows copied (Fabric)": _num(fab.get("rowsCopied")) if fab else "",
            "Output check": (f"{sum(c['verdict'] == 'matches' for c in oc)}/{len(oc)} match" if oc else ""),
        })
    return out


def _csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    if rows:
        w = csv.DictWriter(buf, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")


def results(api, pid: str, key: str = "res") -> None:
    """The results panel. ``key`` keeps widget keys apart when the panel is shown on more than one step."""
    data = read(api, "run_results", pid) or {}
    if not data.get("available"):
        return
    s, rows = data["summary"], data["pipelines"]
    n = s["pipelines"] or 1
    st.markdown("#### Migration results: deployed, run in Fabric, compared with ADF")
    st.caption("From this project's records: each pipeline's deploy, its latest Fabric run, and that run compared activity by activity with its "
               "baseline (the last successful ADF run, or an earlier Fabric run): status, runs, rows and files. Nothing is started or changed here.")
    cm = s["compared_moved"]
    theme.html_block(theme.stats([
        ("Deployed", f"{s['deployed']}/{s['pipelines']}", "ok" if s["deployed"] == s["pipelines"] else "accent",
         f"{s['deploy_failed']} failed" if s["deploy_failed"] else "to this workspace"),
        ("Ran in Fabric", f"{s['succeeded']}/{s['pipelines']}", "ok" if s["succeeded"] == s["pipelines"] else "accent",
         f"{s['failed']} failed" if s["failed"] else "succeeded"),
        ("Match the baseline", f"{s['matches']}/{s['pipelines']}", "ok" if s["matches"] == s["pipelines"] else "accent",
         f"{s['compared_with_adf']} vs ADF · {s['compared_with_fabric']} vs Fabric"),
        ("Checks matched", f"{s['checks_matched']:,}", "ok" if not s["differences_total"] else "",
         f"{s['differences_total']} difference(s)"),
        ("Rows copied in Fabric", f"{s['fabric_moved']['rowsCopied']:,}", "",
         f"{s['fabric_moved']['filesWritten']} file(s) written"),
    ]))
    if s["matches"] < n and (s["differs"] or s["failed"] or s["not_run"] or s["partial"]):
        st.warning(f"{s['differs']} differ, {s['failed']} failed, {s['partial']} partly compared, {s['not_run']} not run or with nothing to compare. "
                   "The table below says which; the Report step's 'What to fix first' says what to do.")
    if cm["pipelines"]:
        st.markdown(f"**Data moved, ADF beside Fabric** · the {cm['pipelines']} pipeline(s) whose ADF figures are kept")
        st.dataframe([{"Measure": label, "ADF (baseline run)": cm["baseline"][k], "Fabric (latest run)": cm["fabric"][k],
                       "Same": "✅" if cm["baseline"][k] == cm["fabric"][k] else "🟠"} for k, label in MOVED],
                     hide_index=True, use_container_width=False)
    table = _pipeline_rows(rows)
    st.markdown("**By pipeline**")
    st.dataframe(table, hide_index=True, use_container_width=True,
                 column_config={"Pipeline": st.column_config.TextColumn(width="medium"),
                                "Compared with": st.column_config.TextColumn(help="'(chosen)': compared with earlier Fabric runs on purpose (verify --no-adf), "
                                                                                  "because its ADF run is not comparable; re-reading the runs keeps that"), "Steps OK": st.column_config.TextColumn(help="Activities that succeeded in the Fabric run"),
                                "Checks matched": st.column_config.NumberColumn(help="Facts (status, runs, rows, files, lookup results) equal to the baseline run"),
                                "Rows copied (baseline)": st.column_config.TextColumn(help="'not kept': compared before the baseline's own figures were recorded")})
    if s["baseline_figures_missing"]:
        st.caption(f"{s['baseline_figures_missing']} pipeline(s) were compared before the baseline's own figures were kept: their result and checks stand, "
                   "only the baseline's figures are not shown.")
    st.download_button("Download results (CSV)", _csv(table), file_name=f"{pid}-migration-results.csv", mime="text/csv", key=f"{key}-csv")
    names = [p["pipeline"] for p in rows if p["activities"]]
    if not names:
        return
    pick = st.selectbox("Activity by activity", names, key=f"{key}-pick-{pid}")
    p = next(x for x in rows if x["pipeline"] == pick)
    if p["detail"]:
        st.caption(_plain(p["detail"]))
    has_base = p.get("baseline_moved") is not None
    acts = []
    for a in p["activities"]:
        row = {"Activity": a["activity"], "Type": a["type"]}
        if has_base:
            row["Status (baseline)"] = a["baseline_status"]
        row["Status (Fabric)"] = a["fabric_status"]
        row["Runs"] = f"{_num(a['baseline_runs'])} / {_num(a['fabric_runs'])}" if has_base else _num(a["fabric_runs"])
        for k, label in MOVED:
            if k == "filesRead":  # files written says it; the table has to fit the page with the result
                continue
            f, b = a.get(f"fabric_{k}"), a.get(f"baseline_{k}")
            if f is None and b is None:
                row[label] = ""
            else:
                row[label] = f"{_num(b)} / {_num(f)}" if has_base else _num(f)
        row["Result"] = a["result"]
        acts.append(row)
    st.dataframe(acts, hide_index=True, use_container_width=True, column_config={"Result": st.column_config.TextColumn(width="medium")})
    st.caption(("Two figures read baseline / Fabric. " if has_base else "Fabric figures only: the baseline's were not kept for this run. ")
               + "Rows and files come from Copy activities; a loop's inner activity is summed over its iterations.")
