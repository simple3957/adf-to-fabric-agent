"""Shared Streamlit pieces: the per-browser API client, error display, state badges and the job runner."""

from __future__ import annotations

import uuid
from typing import Any, Callable, Optional

import streamlit as st

from api_client import ApiClient, ApiError

# Connection states, worded plainly. "Tested" is shown ONLY for connection_tested.
STATE_LABELS = {
    "not_configured": ("Not configured", "gray"),
    "authentication_required": ("Sign-in required", "orange"),
    "authenticated": ("Signed in (not tested)", "blue"),
    "resource_discovered": ("Found, not configured", "gray"),
    "connection_configured": ("Configured (not tested)", "blue"),
    "connection_tested": ("Tested", "green"),
    "validation_failed": ("Test failed", "red"),
    "manual_action_required": ("Manual action required", "orange"),
}
CONVERSION_COLORS = {"converted": "green", "converted_with_differences": "orange", "manual_required": "orange",
                     "blocked": "red", "partial": "orange", "failed": "red"}
CHECK_ICONS = {"passed": "✅", "failed": "❌", "skipped": "➖", "unverified": "❔"}


def client() -> ApiClient:
    """One API client per browser session, with its own X-Session-Id."""
    if "api" not in st.session_state:
        st.session_state.api = ApiClient(session_id=uuid.uuid4().hex)
    return st.session_state.api


def show_error(err: ApiError) -> None:
    st.error(err.message)
    for d in err.details:
        st.caption(f"• {d['field']}: {d['problem']}" if isinstance(d, dict) else f"• {d}")
    for a in err.actions:
        st.info(a)


def call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Optional[Any]:
    """Run an API call; on failure show the error and return None."""
    try:
        return fn(*args, **kwargs)
    except ApiError as err:
        show_error(err)
        return None


def state_badge(state: Optional[str]) -> str:
    label, color = STATE_LABELS.get(state or "", (state or "unknown", "gray"))
    return f":{color}-badge[{label}]"


def status_badge(status: Optional[str]) -> str:
    color = CONVERSION_COLORS.get(status or "", "gray")
    return f":{color}-badge[{(status or 'unknown').replace('_', ' ')}]"


def checks_table(checks: list[dict]) -> None:
    if checks:
        st.dataframe([{"": CHECK_ICONS.get(c["status"], ""), "check": c["name"], "status": c["status"],
                       "detail": c.get("detail", "")} for c in checks], hide_index=True)


def test_report(rep: dict) -> None:
    """A TestReport: always shows what was NOT verified next to what was."""
    st.markdown(f"**{rep.get('target', 'Test')}** {state_badge(rep.get('state'))}")
    if rep.get("error"):
        st.error(rep["error"])
    cols = st.columns(2)
    cols[0].markdown("**Verified**\n" + "".join(f"\n- {v}" for v in rep.get("verified", [])) if rep.get("verified")
                     else "**Verified**\n\n_nothing_")
    cols[1].markdown("**Not verified**\n" + "".join(f"\n- {v}" for v in rep.get("not_verified", [])) if rep.get("not_verified")
                     else "**Not verified**\n\n_nothing_")
    checks_table(rep.get("checks", []))
    for a in rep.get("actions", []):
        st.info(a)


def run_job(start: Callable[[], dict], label: str) -> Optional[dict]:
    """Start a background job, stream its log lines, and return the finished job (or None if it couldn't start)."""
    try:
        job = start()
    except ApiError as err:
        show_error(err)
        return None
    with st.status(label, expanded=True) as box:
        try:
            final = client().wait_job(job, on_event=lambda e: st.write(f"`{e['t']:>6}s` {e['message']}"))
        except ApiError as err:
            box.update(label=f"{label}: still running", state="error")
            show_error(err)
            return None
        if final["state"] == "succeeded":
            box.update(label=f"{label}: done", state="complete", expanded=False)
        else:
            box.update(label=f"{label}: failed", state="error")
            st.error(final.get("error") or "The job failed.")
    st.session_state.setdefault("job_history", []).insert(0, final)
    return final


def sign_in(provider: str, label: str, key: str, device_option: bool = True, button: Optional[str] = None) -> None:
    """Start a CLI sign-in on the API machine and follow it until it ends. No password field, ever."""
    import time

    with st.popover(button or f"Sign in with {label}"):
        if button:
            st.caption("A new sign-in replaces the current account on the machine running the accelerator.")
        tenant = st.text_input("Tenant ID (optional)", key=f"{key}-tenant")
        device = st.checkbox("Use a device code", key=f"{key}-device",
                             help="Sign in from another browser with a code.") if device_option else False
        if not st.button("Start sign-in", key=f"{key}-go", type="primary"):
            return
        api = client()
        attempt = call(api.start_login, provider, tenant_id=tenant or None, device_code=device)
        if not attempt:
            return
        out = st.empty()
        deadline = time.time() + 300
        while attempt["state"] == "running" and time.time() < deadline:
            out.code("\n".join(attempt.get("output", [])) or "Waiting for the sign-in window…")
            time.sleep(2)
            attempt = call(api.login_state, provider, attempt["id"]) or {"state": "failed", "output": []}
        if attempt["state"] == "succeeded":
            st.rerun()
        out.code("\n".join(attempt.get("output", [])) or "(no output)")
        st.error(f"Sign-in {attempt['state']}.")


def current_project() -> Optional[str]:
    return st.session_state.get("project_id")


def require_project() -> Optional[str]:
    pid = current_project()
    if not pid:
        st.warning("Pick a project in the sidebar, or import one on the **Source** page.")
    return pid
