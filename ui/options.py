"""Step options: the choices a person makes for one step, laid out like the Synapse accelerator's stage cards.

Each step that has real choices shows them in a card at the top: what the choice changes, the default, and where the default comes
from (the approved consultation brief, or the tool's own). A choice made here is for that run only, and beats the brief's default.
The server checks every choice again, so the card is a convenience and never the protection.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from typing import Iterator, Optional

import streamlit as st

import theme
from api_client import ApiClient
from components import call

ITEM_NAME = re.compile(r"^[A-Za-z0-9_.\-]{1,256}$")


@contextmanager
def card(key: str, title: str, note: str = "", source: str = "") -> Iterator[None]:
    """A bordered options card: a heading, where its defaults come from, a line saying what the choices change."""
    with st.container(key=f"opt-{key}"):
        head = f'<div class="ma-opt-head"><strong>{theme.esc(title)}</strong>' + (f'<span class="src">{theme.esc(source)}</span>' if source else "") + "</div>"
        st.markdown(head + (f'<p class="ma-opt-note">{theme.esc(note)}</p>' if note else ""), unsafe_allow_html=True)
        yield


def agreed(api: ApiClient, pid: str) -> Optional[dict]:
    """The settings of the approved, current consultation brief (with its version), or None: the defaults a card starts from."""
    v = call(api.brief, pid)
    if not v or not v.get("settings"):
        return None
    return {**v["settings"], "version": v["current_version"]["version"]}


def source_label(brief: Optional[dict]) -> str:
    return f"Defaults from the approved brief v{brief['version']}" if brief else "Tool defaults"


def seed(key: str, value) -> None:
    """Set a widget's starting value once (and again when the brief it came from changes), never over what the person picked."""
    st.session_state.setdefault(key, value)


def scope_picker(pid: str, known: list[str], key: str, brief: Optional[dict], label: str = "Narrow to these pipelines") -> list[str]:
    """Which pipelines this run covers, asked once per project instead of once per step.

    The first answer anywhere (Convert or Deploy) is remembered for the project and is where the next step starts, so the same question is not put
    again; with nothing chosen the whole estate is meant, and the picker stays closed. The approved brief's scope is the very first default."""
    saved = st.session_state.setdefault("scope", {}).get(pid)
    start = saved if saved is not None else ((brief or {}).get("pipelines") or [])
    start = [p for p in start if p in known]
    picked = list(start)
    st.caption(f"Scope: {'all ' + str(len(known)) if not start else str(len(start)) + ' of ' + str(len(known))} pipeline(s)"
               + (" · remembered from an earlier step" if saved is not None else ""))
    with st.expander("Change the scope", expanded=False):
        picked = st.multiselect(label, known, default=start, key=key,
                                help="Empty means all. A pipeline that calls another needs that one converted or already deployed. This choice is kept for the "
                                     "next steps, so it is not asked again.")
    st.session_state["scope"][pid] = picked
    return picked


def lakehouse_problem(name: str) -> str:
    name = name.strip()
    if not name:
        return "Name the lakehouse that holds the shortcuts."
    return "" if ITEM_NAME.match(name) else "Use letters, digits, _ . - only (no spaces or commas), up to 256 characters."
