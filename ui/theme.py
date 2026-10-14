"""ADF → Fabric Studio look: dark charcoal surfaces, emerald → cyan accents, a vertical stepper.

Streamlit renders the widgets; this file adds the CSS and small HTML helpers. Keyed containers (``st.container(key=...)``)
get a ``st-key-<key>`` class, which is how cards, the stepper and the bars are styled.
"""

from __future__ import annotations

import html

import streamlit as st

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;600;700;800&family=Inter:wght@400;500;600&display=swap');
:root {
  --bg:#0d0f13; --surface:#14171d; --surface-2:#1b1f27; --surface-3:#232833; --border:#262b35; --border-strong:#343b48;
  --text:#e6e9ef; --muted:#9ba3b4; --faint:#6b7385;
  --em:#10b981; --cy:#06b6d4; --grad:linear-gradient(120deg,#10b981 0%,#06b6d4 100%); --em-soft:rgba(16,185,129,.14);
  --ok:#22c55e; --ok-soft:rgba(34,197,94,.13); --warn:#f59e0b; --warn-soft:rgba(245,158,11,.13);
  --err:#ef4444; --err-soft:rgba(239,68,68,.13); --info:#38bdf8; --info-soft:rgba(56,189,248,.13); --neutral:#6b7385;
  --head:'Plus Jakarta Sans', Inter, 'Segoe UI', sans-serif;
}
html, body, .stApp, .stMarkdown, button, input, textarea, select { font-family: Inter, 'Segoe UI', system-ui, sans-serif; }
.stApp {
  background:
    radial-gradient(900px 420px at 0% 0%, rgba(16,185,129,.07), transparent 60%),
    linear-gradient(rgba(255,255,255,.018) 1px, transparent 1px) 0 0 / 100% 32px,
    var(--bg);
}
[data-testid="stHeader"] { background: transparent; }
[data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"] { display:none; }
.block-container { max-width: 1280px; padding-top: 1rem; }
h1, h2, h3, h4 { font-family: var(--head) !important; letter-spacing: -0.015em; }
code { background: var(--surface-3); color: #6ee7b7; }

/* buttons */
.stButton > button, .stFormSubmitButton > button, .stDownloadButton > button { border-radius: 999px; font-weight: 600; }
.stButton > button[kind="primary"], .stFormSubmitButton > button[kind="primaryFormSubmit"] {
  background: var(--grad); color: #04130e; border: 0; box-shadow: 0 8px 22px rgba(16,185,129,.25); }
.stButton > button[kind="primary"]:hover { filter: brightness(1.08); }
.stButton > button[kind="primary"]:disabled { background: var(--surface-3); color: var(--faint); box-shadow: none; }
.stButton > button[kind="secondary"] { background: var(--surface-2); border: 1px solid var(--border-strong); }

/* top bar */
.st-key-topbar { background: rgba(20,23,29,.82); backdrop-filter: blur(14px); border: 1px solid var(--border);
  border-radius: 999px; padding: 4px 18px; margin-bottom: 18px; box-shadow: 0 10px 30px rgba(0,0,0,.35); }
.ma-brand { display:flex; align-items:center; gap:10px; font-family: var(--head); font-weight:800; font-size:16.5px; white-space:nowrap; }
.ma-brand-mark { flex:none; width:30px; height:30px; border-radius:50%; display:grid; place-items:center; color:#04130e; font-size:15px;
  background: var(--grad); box-shadow: 0 0 0 4px var(--em-soft); }
.ma-brand small { font-weight:600; color: var(--muted); font-family: Inter, sans-serif; }
[data-testid="stPageLink"] a { border-radius: 999px; padding: 2px 12px; }
.ma-dots { display:flex; gap:12px; font-size:13px; color: var(--muted); }
.ma-dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:6px; background: var(--neutral); }
.ma-dot.ok { background: var(--em); box-shadow: 0 0 10px var(--em); }

/* hero */
.ma-eyebrow { font-size:11.5px; text-transform:uppercase; letter-spacing:.14em; color: var(--em); font-weight:700; }
.ma-hero { position:relative; overflow:hidden; display:flex; flex-direction:column; gap:12px; padding:34px 36px; margin-bottom:10px;
  border-radius:22px; border:1px solid var(--border); background: var(--surface); }
.ma-hero::before { content:""; position:absolute; inset:0 0 auto 0; height:3px; background: var(--grad); }
.ma-hero::after { content:""; position:absolute; right:-120px; top:-120px; width:380px; height:380px; border-radius:50%;
  background: radial-gradient(circle, rgba(6,182,212,.18), transparent 65%); }
.ma-hero h1 { font-size:38px; font-weight:800; margin:0; padding:0; line-height:1.1; }
.ma-hero .grad { background: var(--grad); -webkit-background-clip: text; background-clip: text; color: transparent; }
.ma-hero p { color: var(--muted); max-width: 700px; font-size:16px; margin:0; }
.ma-chips { display:flex; flex-wrap:wrap; gap:6px; align-items:center; margin-top:6px; position:relative; z-index:1; }
.ma-chip { padding:5px 12px; border-radius:999px; background: var(--surface-2); border:1px solid var(--border-strong); font-size:13px; color: var(--text); }
.ma-chip b { color: var(--em); margin-right:4px; }
.ma-arrow { color: var(--faint); }

/* numbered sections */
.ma-section { display:flex; gap:14px; align-items:center; margin: 26px 0 12px; }
.ma-section h2 { font-size:19px; font-weight:700; margin:0; padding:0; }
.ma-section p { color: var(--muted); margin:2px 0 0; font-size:14.5px; }
.ma-num { width:34px; height:34px; flex:none; border-radius:10px; display:grid; place-items:center; font-weight:800;
  font-family: var(--head); background: var(--surface-2); border:1px solid var(--border-strong); color: var(--muted); }
.ma-num.done { background: var(--grad); border:0; color:#04130e; }
.ma-note { display:flex; gap:10px; align-items:flex-start; padding:12px 16px; border-radius:14px; background: var(--em-soft);
  border:1px solid rgba(16,185,129,.3); font-size:14px; }
.ma-placeholder { padding:30px; border:1px dashed var(--border-strong); border-radius:18px; color: var(--muted); text-align:center; }

/* side cards */
[class*="st-key-card-"] { position:relative; background: var(--surface); border:1px solid var(--border); border-radius:18px; padding: 20px 22px; }
[class*="st-key-card-ok-"] { border-color: rgba(16,185,129,.45); box-shadow: 0 0 0 1px rgba(16,185,129,.12), 0 14px 40px rgba(16,185,129,.06); }
.ma-side { display:flex; align-items:center; gap:12px; margin-bottom:6px; }
.ma-mark { width:42px; height:42px; border-radius:12px; display:grid; place-items:center; font-weight:800; color:#fff; font-family: var(--head); }
.ma-side h3 { margin:0; padding:0; font-size:17px; }
.ma-side .grow { flex:1; }

/* badges */
.ma-badge { display:inline-flex; align-items:center; gap:6px; padding:3px 10px; border-radius:999px; font-size:12.5px; font-weight:600; white-space:nowrap; }
.ma-badge::before { content:""; width:6px; height:6px; border-radius:50%; background: currentColor; }
.ma-badge.success { color: var(--ok); background: var(--ok-soft); }
.ma-badge.warning { color: var(--warn); background: var(--warn-soft); }
.ma-badge.error { color: var(--err); background: var(--err-soft); }
.ma-badge.info { color: var(--info); background: var(--info-soft); }
.ma-badge.neutral { color: var(--muted); background: var(--surface-3); }

/* launch bar */
.st-key-launchbar { position: sticky; bottom: 14px; z-index: 15; margin-top: 24px; padding: 10px 12px 10px 24px; border-radius: 999px;
  border:1px solid var(--border-strong); background: rgba(27,31,39,.94); backdrop-filter: blur(10px); box-shadow: 0 18px 50px rgba(0,0,0,.5); }
.ma-checks { display:flex; flex-wrap:wrap; gap:18px; font-size:14px; color: var(--faint); }
.ma-checks .ok { color: var(--text); }
.ma-checks .ok b { color: var(--em); }

/* vertical stepper */
.st-key-stepper { position: sticky; top: 12px; background: var(--surface); border:1px solid var(--border); border-radius:20px; padding: 18px 14px; }
.ma-ring { display:flex; align-items:center; gap:14px; padding: 2px 6px 14px; border-bottom:1px solid var(--border); margin-bottom: 6px; }
.ma-ring .r { width:58px; height:58px; border-radius:50%; display:grid; place-items:center; flex:none;
  background: conic-gradient(var(--em) calc(var(--p) * 1%), var(--surface-3) 0); }
.ma-ring .r span { width:44px; height:44px; border-radius:50%; background: var(--surface); display:grid; place-items:center;
  font-family: var(--head); font-weight:800; font-size:14px; }
.ma-ring b { display:block; font-family: var(--head); font-size:15px; }
.ma-ring small { color: var(--muted); }
[class*="st-key-vstep-"] { position:relative; padding: 10px; border-radius:14px; border:1px solid transparent; gap: 2px; }
.ma-vrow { display:flex; gap:12px; align-items:flex-start; }
.ma-vrow > div { min-width:0; }
[class*="st-key-vstep-"]::before { content:""; position:absolute; left:26px; top:46px; bottom:-14px; width:2px; background: var(--border-strong); }
.st-key-vstep-last::before, [class*="st-key-vstep-"][class*="-last"]::before { display:none; }
[class*="st-key-vstep-sel-"] { background: linear-gradient(90deg, rgba(16,185,129,.12), transparent); border-color: rgba(16,185,129,.35); }
[class*="st-key-vstep-locked-"] { opacity:.55; }
.ma-vdot { flex:none; width:34px; height:34px; border-radius:50%; display:grid; place-items:center;
  font-weight:800; font-size:14px; background: var(--surface-3); color: var(--muted); border:1px solid var(--border-strong); }
.ma-vdot.done { background: var(--grad); color:#04130e; border:0; }
.ma-vdot.current { background: var(--surface); color: var(--em); border:2px solid var(--em); box-shadow: 0 0 0 4px var(--em-soft); }
.ma-vdot.attention { color: var(--warn); border:2px solid var(--warn); background: var(--surface); }
.ma-vtitle { font-family: var(--head); font-weight:700; font-size:15.5px; display:flex; align-items:center; gap:8px; }
.ma-vmetric { font-size:13px; color: var(--muted); margin: 1px 0 4px; }
.ma-vstate { font-size:10.5px; font-weight:700; letter-spacing:.08em; text-transform:uppercase; padding:2px 8px; border-radius:999px;
  background: var(--surface-3); color: var(--muted); }
.ma-vstate.done { background: var(--ok-soft); color: var(--ok); }
.ma-vstate.current { background: var(--em-soft); color: var(--em); }
.ma-vstate.attention { background: var(--warn-soft); color: var(--warn); }
[class*="st-key-vstep-"] .stButton { margin-left: 46px; }
[class*="st-key-vstep-"] .stButton > button { min-height: 0; height: 26px; padding: 0 4px; font-size: 13px; color: var(--em); }

/* step panel + next bar */
.st-key-panel { background: var(--surface); border:1px solid var(--border); border-radius:20px; padding: 24px 28px; }
.ma-panel-title { display:flex; align-items:center; gap:12px; }
.ma-panel-title .n { width:38px; height:38px; border-radius:12px; display:grid; place-items:center; background: var(--grad);
  color:#04130e; font-weight:800; font-family: var(--head); }
.ma-panel-title h3 { margin:0; padding:0; font-size:22px; }
.st-key-nextbar { margin-top: 18px; padding: 14px 18px; border-radius: 16px; background: var(--surface-2); border:1px solid var(--border-strong); }
.ma-next { display:flex; gap:12px; align-items:center; }
.ma-next .ic { font-size:20px; }
.ma-next strong { font-size:15px; display:block; font-family: var(--head); }
.ma-next .muted { color: var(--muted); font-size:13.5px; }
.st-key-done { background: var(--surface); border:1px solid rgba(16,185,129,.45); border-radius:20px; padding: 18px 24px; margin-bottom: 14px; }

/* streamlit widgets */
[data-testid="stMetric"] { background: var(--surface-2); border:1px solid var(--border); border-radius:16px; padding: 14px 18px; }
[data-testid="stMetricLabel"] p { font-size:11.5px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color: var(--muted); }
[data-testid="stMetricValue"] { font-family: var(--head); }
[data-testid="stExpander"] details { background: var(--surface-2); border-color: var(--border); border-radius: 14px; }
[data-testid="stProgress"] > div > div > div > div { background: var(--grad); }
</style>
"""

TONES = ("success", "warning", "error", "info", "neutral")


def inject() -> None:
    st.markdown(CSS, unsafe_allow_html=True)


def esc(text: object) -> str:
    return html.escape(str(text))


def badge(label: str, tone: str = "neutral") -> str:
    return f'<span class="ma-badge {tone if tone in TONES else "neutral"}">{esc(label)}</span>'


def section(num: int, title: str, text: str, done: bool = False) -> None:
    st.markdown(f'<div class="ma-section"><span class="ma-num{" done" if done else ""}">{"✓" if done else num}</span>'
                f'<div><h2>{esc(title)}</h2><p>{esc(text)}</p></div></div>', unsafe_allow_html=True)


def mark(letters: str, color: str) -> str:
    return f'<span class="ma-mark" style="background:{color}">{esc(letters)}</span>'


def ring(done: int, total: int, title: str, sub: str) -> str:
    pct = round(100 * done / total) if total else 0
    return (f'<div class="ma-ring"><div class="r" style="--p:{pct}"><span>{done}/{total}</span></div>'
            f'<div><b>{esc(title)}</b><small>{esc(sub)}</small></div></div>')


def html_block(markup: str) -> None:
    st.markdown(markup, unsafe_allow_html=True)
