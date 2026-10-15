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


def identity(provider: str, label: str, key: str, *, purpose: str, device_option: bool = True) -> Optional[dict]:
    """Who this card works as. Nobody is assumed: the user signs in, or confirms the account already on this machine.

    Returns the sign-in status once an account is confirmed for this browser session, otherwise renders the sign-in
    choices and returns None. Sign-in itself runs on the API machine (``az login`` / ``fab auth login``); this browser
    never sees a password or token.
    """
    api = client()
    ids = st.session_state.setdefault("confirmed_ids", {})
    mode_key = f"{key}-mode"
    status = call(api.auth_status, provider) or {}
    account = status.get("account") if status.get("authenticated") else None
    if ids.get(key) and ids[key] != account:  # the machine's account changed underneath this session
        ids.pop(key)

    if ids.get(key) and st.session_state.get(mode_key) != "signin":
        st.markdown(f"Signed in as **{account}**")
        st.caption(f"Tenant {status.get('tenant_id') or '-'}")
        a, b, _ = st.columns([1.3, 1, 1.7])
        if a.button("Switch account", key=f"{key}-switch", width="stretch"):
            st.session_state[mode_key] = "signin"
            st.rerun()
        if b.button("Sign out", key=f"{key}-out", width="stretch"):
            _sign_out(provider, account, key)
        return status

    if st.session_state.get(mode_key) != "signin":
        if not status.get("available"):
            st.error(status.get("error") or f"{label} is not installed on the machine running the accelerator.")
            for x in status.get("actions", []):
                st.caption(x)
            return None
        if account:
            st.markdown(f"This computer is signed in to {label} as **{account}**. {purpose}")
            a, b, c = st.columns([1.1, 1.4, 1])
            if a.button("Continue", key=f"{key}-continue", type="primary", width="stretch",
                        help=f"Use {account} for this session."):
                ids[key] = account
                st.rerun()
            if b.button("Another account", key=f"{key}-other", width="stretch"):
                st.session_state[mode_key] = "signin"
                st.rerun()
            if c.button("Sign out", key=f"{key}-out0", width="stretch"):
                _sign_out(provider, account, key)
        else:
            st.markdown(f"Not signed in. {purpose}")
            if st.button(f"Sign in with {label}", key=f"{key}-start", type="primary"):
                st.session_state[mode_key] = "signin"
                st.rerun()
        return None

    # ---- the sign-in form ----------------------------------------------------------------------------------------
    import time

    with st.container(border=True):
        st.markdown(f"**Sign in with {label}**")
        st.caption("You get a short code and a link here. Open the link on any device, enter the code, then sign in and "
                   "approve with Authenticator. Untick the device code to sign in through a browser tab on the machine "
                   "running the accelerator instead.")
        tenant = st.text_input("Tenant ID (needed if the account belongs to several directories)", key=f"{key}-tenant",
                               placeholder="e.g. 1680abbe-1a01-4cb0-99e9-452fe05bf615")
        device = st.checkbox("Use a device code (recommended)", key=f"{key}-device", value=True,
                             help="Shows a code to enter at microsoft.com/devicelogin from any device.") if device_option else False
        a, b = st.columns([1, 1])
        go = a.button("Start sign-in", key=f"{key}-go", type="primary", width="stretch")
        if b.button("Cancel", key=f"{key}-cancel", width="stretch"):
            st.session_state.pop(mode_key, None)
            st.rerun()
        if not go:
            return None
        attempt = call(api.start_login, provider, tenant_id=tenant.strip() or None, device_code=device)
        if not attempt:
            return None
        with st.status("Waiting for you to finish signing in…", expanded=True) as box:
            out = st.empty()
            deadline = time.time() + 300
            if not device:
                st.info("A Microsoft sign-in page has opened in the browser on the machine running the accelerator. "
                        "Check your browser tabs (it may be behind other windows).")
            while attempt["state"] == "running" and time.time() < deadline:
                out.code("\n".join(attempt.get("output", [])) or "Starting the sign-in…")
                time.sleep(2)
                attempt = call(api.login_state, provider, attempt["id"]) or {"state": "failed", "output": []}
            out.code("\n".join(attempt.get("output", [])) or "(no output)")
            if attempt["state"] != "succeeded":
                box.update(label=f"Sign-in {attempt['state'].replace('_', ' ')}", state="error")
                st.error("The sign-in did not complete. The message from Azure is shown above.")
                return None
            box.update(label="Signed in", state="complete")
        new = call(api.auth_status, provider) or {}
        if new.get("authenticated"):
            ids[key] = new.get("account")
            st.session_state.pop(mode_key, None)
            if new.get("tenant_id"):
                st.session_state[f"{key}-tenant-id"] = new["tenant_id"]
        st.rerun()


def _sign_out(provider: str, account: Optional[str], key: str) -> None:
    api = client()
    if call(api.logout, provider, account) is not None:
        st.session_state.setdefault("confirmed_ids", {}).pop(key, None)
        st.session_state.pop(f"{key}-mode", None)
        st.rerun()


def current_project() -> Optional[str]:
    return st.session_state.get("project_id")


def require_project() -> Optional[str]:
    pid = current_project()
    if not pid:
        st.warning("Pick a project in the sidebar, or import one on the **Source** page.")
    return pid
