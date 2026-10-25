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


def lakehouse_problem(name: str) -> str:
    name = name.strip()
    if not name:
        return "Name the lakehouse that holds the shortcuts."
    return "" if ITEM_NAME.match(name) else "Use letters, digits, _ . - only (no spaces or commas), up to 256 characters."
