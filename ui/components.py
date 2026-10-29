"""Shared Streamlit pieces: the per-browser API client, error display, state badges and the job runner."""

from __future__ import annotations

import uuid
from typing import Any, Callable, Optional

import streamlit as st

import theme
from api_client import ApiClient, ApiError
from cache import ACCOUNTS_TTL, cached, forget

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
    "skipped": ("Skipped", "gray"),
}
CONVERSION_COLORS = {"converted": "green", "converted_with_differences": "orange", "manual_required": "orange",
                     "blocked": "red", "partial": "orange", "failed": "red"}
CHECK_ICONS = {"passed": "✅", "failed": "❌", "skipped": "➖", "unverified": "❔"}


def client() -> ApiClient:
    """One API client per browser session, with its own X-Session-Id."""
    if "api" not in st.session_state:
        st.session_state.api = ApiClient(session_id=uuid.uuid4().hex)
    return st.session_state.api


PSTATE_ICON = {"discovered": "🔎", "inferred": "❓", "unsupported": "🚫", "blocked": "⛔", "converted": "🔁", "deployed": "🚀", "runtime_validated": "✅"}
PSTATE_LABEL = {"discovered": "Discovered", "inferred": "Inferred", "unsupported": "Unsupported", "blocked": "Blocked", "converted": "Converted",
               "deployed": "Deployed", "runtime_validated": "Runtime-validated"}
PSTATE_TONE = {"discovered": "info", "inferred": "warning", "unsupported": "error", "blocked": "error", "converted": "info", "deployed": "success",
              "runtime_validated": "success"}
PSTATE_HELP = {"discovered": "Read from the source and fully understood.", "inferred": "Read, but something was dynamic or filled in: confirm it.",
              "unsupported": "No Fabric counterpart for an activity or connector.", "blocked": "Cannot proceed until something is supplied or decided.",
              "converted": "Fabric JSON was generated. This is not proof it works.", "deployed": "A deploy call reported success. This is not proof it runs correctly.",
              "runtime_validated": "A real Fabric run completed and matched its baseline."}


def state_chip(state: str) -> str:
    """HTML for one state: an icon AND a word, so meaning never depends on colour alone."""
    return theme.badge(f"{PSTATE_ICON.get(state, '')} {PSTATE_LABEL.get(state, state)}".strip(), PSTATE_TONE.get(state, "neutral"))


def state_legend(counts: dict) -> None:
    """The seven states with how many pipelines are in each and what the word does and does not promise."""
    cells = "".join(f'<div class="ma-legend-cell">{state_chip(key)}<div class="ma-legend-help">{counts.get(key, 0)} · {theme.esc(PSTATE_HELP[key])}</div></div>'
                    for key in PSTATE_LABEL)
    theme.html_block(f'<div class="ma-legend">{cells}</div>')  # wraps to the width it has; seven columns made the chips touch and overflow


def empty_state(title: str, hint: str = "") -> None:
    theme.html_block(f'<div class="ma-placeholder" role="status"><b>{theme.esc(title)}</b>' + (f'<br>{theme.esc(hint)}' if hint else "") + "</div>")


def intent(name: str) -> str:
    """A key that stays the same for one intended action until it has finished, so a second click or a rerun re-attaches to the first job."""
    import uuid as _uuid

    return st.session_state.setdefault("intents", {}).setdefault(name, _uuid.uuid4().hex[:24])


def typed_confirm(label: str, phrase: str, key: str, disabled: bool = False) -> bool:
    """A deliberate confirmation for something that changes the target: type the word. True only while it matches exactly."""
    typed = st.text_input(f"{label}. Type {phrase} to confirm.", key=key, disabled=disabled, autocomplete="off")
    return (not disabled) and typed.strip() == phrase


def can(permission: str) -> bool:
    """Whether the signed-in person holds a permission, per the API. A CONVENIENCE for hiding buttons: the API enforces every permission
    itself, so a hidden button is never the protection. Unknown (not asked yet) counts as allowed so nothing is hidden by mistake."""
    held = st.session_state.get("permissions")
    return True if held is None else permission in held


def show_error(err: ApiError) -> None:
    st.error(err.message)
    if err.status == 429:
        st.info("Too many requests: wait a moment and try again.")
    if getattr(err, "request_id", "") and err.status >= 500:
        st.caption(f"Reference {err.request_id}: give this to your administrator; the details are in the server log.")
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


def run_job(start: Callable[[], dict], label: str, intent_name: Optional[str] = None) -> Optional[dict]:
    """Start a background job, stream its log lines, and return the finished job (or None if it couldn't start).

    ``intent_name`` ties the submission to one intended action: while the job runs, a repeated click sends the same idempotency key and the
    server hands back the same job, so nothing is started twice. The key is dropped once the job has finished.
    """
    try:
        job = start()
    except ApiError as err:
        if intent_name and err.status == 422:  # a key the server bound to a different request (options changed): use a new one next time
            st.session_state.get("intents", {}).pop(intent_name, None)
        if err.status == 409 and "already running" in err.message:  # a job in progress; any other 409 has its own reason, which is all that is shown
            st.warning(err.message + " Watch it from the banner at the top of this page; a second one is not started.")
        elif err.status == 409:
            st.warning(err.message)
        else:
            show_error(err)
        return None
    return watch_job(job, label, intent_name)


def active_jobs(project_id: str) -> list[dict]:
    """Jobs of this project still queued or running on the server (they outlive a reload, a closed tab and the 30-minute wait)."""
    try:
        return [j for j in client().jobs(project_id) if j.get("state") in ("queued", "running")]
    except ApiError:
        return []


def watch_job(job: dict, label: str, intent_name: Optional[str] = None) -> Optional[dict]:
    """Follow a job already started (by this page, an earlier visit or another tab) to its end."""
    with st.status(label, expanded=True) as box:
        try:
            final = client().wait_job(job, on_event=lambda e: st.write(f"`{e['t']:>6}s` {e['message']}"))
        except ApiError as err:
            box.update(label=f"{label}: still running on the server", state="error")
            show_error(err)
            st.info("The job keeps running on the server. Reload this page to watch it again.")
            return None
        if final["state"] == "succeeded":
            box.update(label=f"{label}: done in {final.get('elapsed', 0):g}s", state="complete", expanded=False)
        else:
            box.update(label=f"{label}: failed after {final.get('elapsed', 0):g}s", state="error")
            st.error(final.get("error") or "The job failed.")
    history = st.session_state.setdefault("job_history", [])
    history.insert(0, final)
    del history[50:]  # a long session does not keep every job's result in memory
    if intent_name:
        st.session_state.get("intents", {}).pop(intent_name, None)
    return final


NEW_ACCOUNT = "__new__"


def _sign_in_panel(provider: str, label: str, key: str, *, device_option: bool = True,
                   on_cancel: Optional[Callable[[], None]] = None) -> Optional[dict]:
    """The sign-in form. Returns the sign-in status once a sign-in has succeeded, otherwise None.

    Sign-in runs on the API machine (``az login`` / ``fab auth login``); this browser never sees a password or token. The
    tenant (directory) is asked for last, under Advanced: most accounts do not need it.
    """
    import time

    api = client()
    with st.container(border=True):
        st.markdown(f"**Sign in with {label}**")
        st.caption("A Microsoft sign-in page opens in the browser on the machine running the accelerator. Sign in and approve "
                   "with Authenticator. Tick the box below to get a code to enter on another device instead.")
        device = st.checkbox("Use a device code instead of the browser", key=f"{key}-device", value=False,
                             help="Shows a code to enter at microsoft.com/devicelogin from any device. Many directories "
                                  "block this (security defaults, error AADSTS530035): then use the browser sign-in.") if device_option else False
        a, b = st.columns(2)
        go = a.button("Start sign-in", key=f"{key}-go", type="primary", width="stretch")
        cancel = b.button("Cancel", key=f"{key}-cancel", width="stretch")
        with st.expander("Advanced · a specific directory (tenant)"):
            st.caption("Only needed if signing in lands in the wrong directory, for example when the account belongs to "
                       "several of them.")
            tenant = st.text_input("Tenant ID", key=f"{key}-tenant", placeholder="e.g. 1680abbe-1a01-4cb0-99e9-452fe05bf615")
        if cancel:
            if on_cancel:
                on_cancel()
            else:
                st.session_state[f"{key}-pending"] = ""  # applied at the top of the next run, before the widget exists
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
    forget("accounts", "subs", "disc", "ws")  # a sign-in changes who is available and what they can see
    return call(api.auth_status, provider, True) or {}


def _accounts() -> list[dict]:
    try:
        return cached("accounts", ACCOUNTS_TTL, lambda: client().accounts())
    except ApiError as err:
        show_error(err)
        return []


def account_picker(side: str, who: str, *, hint: str) -> Optional[str]:
    """Which signed-in account this side works as. Each side picks its own, so ADF and Fabric can use different accounts.

    Nobody is assumed: the dropdown starts empty. Signing in with another account is one of its choices, not a separate
    screen. Returns the chosen account's name, or None until one is chosen.
    """
    key = f"{side}-account"
    pending = st.session_state.pop(f"{key}-pending", None)
    if pending is not None:  # set by a sign-in or a cancel in the previous run; widget state can only change before it exists
        if pending:
            st.session_state[key] = pending
        else:
            st.session_state.pop(key, None)
    accounts = _accounts()
    names = [a["username"] for a in accounts]
    default = next((a["username"] for a in accounts if a.get("is_default")), None)
    options = [*names, NEW_ACCOUNT]
    if st.session_state.get(key) not in options:
        st.session_state.pop(key, None)

    def label(v: str) -> str:
        if v == NEW_ACCOUNT:
            return "＋ Sign in with another account…"
        return f"{v}  ·  machine default" if v == default and len(names) > 1 else v

    if not names:
        st.markdown(f"No account is signed in on this machine yet. {hint}")
        _after_sign_in(side, key, names, _sign_in_panel("azure_cli", "Azure", key))
        return None
    chosen = st.selectbox(f"{who} account", options, index=None, key=key, format_func=label,
                          placeholder="Choose an account…", help=hint)
    if chosen == NEW_ACCOUNT:
        _after_sign_in(side, key, names, _sign_in_panel("azure_cli", "Azure", key))
        return None
    if chosen:
        row = next((a for a in accounts if a["username"] == chosen), {})
        a, b = st.columns([3, 1], vertical_alignment="center")
        subs, dirs = int(row.get("subscription_count", 0) or 0), len(row.get("tenants", []))
        a.caption(" · ".join(x for x in (f"{subs} subscription{'' if subs == 1 else 's'}" if subs else "",  # a Fabric-only sign-in has none: say nothing, not "0"
                                         f"{dirs} director{'y' if dirs == 1 else 'ies'}") if x))
        if b.button("Sign out", key=f"{key}-out", type="tertiary", width="stretch", help=f"Sign {chosen} out on this machine."):
            if call(client().logout, "azure_cli", chosen) is not None:
                forget("accounts", "subs", "disc", "ws")
                st.session_state[f"{key}-pending"] = ""
                st.rerun()
        return chosen
    return None


def _after_sign_in(side: str, key: str, before: list[str], status: Optional[dict]) -> None:
    """Select the account that just signed in (the new one, else whoever the CLI now reports) and run again."""
    if status is None:
        return
    after = [a["username"] for a in (call(client().accounts, True) or [])]
    forget("accounts")
    new = [n for n in after if n not in before]
    pick = new[0] if new else status.get("account")
    if pick:
        st.session_state[f"{key}-pending"] = pick
    st.rerun()


def identity(provider: str, label: str, key: str, *, purpose: str, device_option: bool = True) -> Optional[dict]:
    """The single-identity sign-in used by the Fabric CLI (it has one account at a time, unlike the Azure CLI).

    Returns the sign-in status once an account is confirmed for this browser session, otherwise renders the sign-in
    choices and returns None.
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

    new = _sign_in_panel(provider, label, key, device_option=device_option,
                         on_cancel=lambda: st.session_state.pop(mode_key, None))
    if new is not None:
        if new.get("authenticated"):
            ids[key] = new.get("account")
            st.session_state.pop(mode_key, None)
        st.rerun()
    return None


def _sign_out(provider: str, account: Optional[str], key: str) -> None:
    api = client()
    if call(api.logout, provider, account) is not None:
        st.session_state.setdefault("confirmed_ids", {}).pop(key, None)
        st.session_state.pop(f"{key}-mode", None)
        forget("accounts", "subs", "disc", "ws")
        st.rerun()


def short_source(url: Optional[str]) -> str:
    """A readable form of a source address: an Azure factory is its name, not the full ARM path."""
    url = url or ""
    if url.startswith("azure://"):
        return "factory " + url.rstrip("/").rsplit("/", 1)[-1]
    return url


def current_project() -> Optional[str]:
    return st.session_state.get("project_id")


def require_project() -> Optional[str]:
    pid = current_project()
    if not pid:
        st.warning("Pick a project in the sidebar, or import one on the **Source** page.")
    return pid
