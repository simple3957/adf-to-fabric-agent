"""A small time-limited cache for slow API calls.

Streamlit runs the whole page again on every click and every committed keystroke. A few of the calls a page makes take
seconds (listing signed-in accounts, Azure resource discovery, listing Fabric workspaces), and their answers change only when
the user signs in or out or refreshes. Asking again each time made the Connections page take about 12 seconds per click.

``TtlCache`` is plain Python (testable without Streamlit). ``cached``/``forget`` keep one per browser session. A call that
raises is never cached, so an error is retried on the next run.
"""

from __future__ import annotations

import time
from typing import Any, Callable


class TtlCache:
    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._items: dict[str, tuple[float, Any]] = {}

    def get(self, key: str, ttl: float, load: Callable[[], Any]) -> Any:
        hit = self._items.get(key)
        now = self._clock()
        if hit is not None and now - hit[0] < ttl:
            return hit[1]
        value = load()  # may raise: nothing is stored then
        self._items[key] = (now, value)
        return value

    def forget(self, *prefixes: str) -> None:
        """Drop every key starting with one of ``prefixes`` (everything when none are given)."""
        for key in [k for k in self._items if not prefixes or k.startswith(prefixes)]:
            del self._items[key]

    def __len__(self) -> int:
        return len(self._items)


def _store() -> TtlCache:
    import streamlit as st

    if "_ttl_cache" not in st.session_state:
        st.session_state["_ttl_cache"] = TtlCache()
    return st.session_state["_ttl_cache"]


def cached(key: str, ttl: float, load: Callable[[], Any]) -> Any:
    return _store().get(key, ttl, load)


def forget(*prefixes: str) -> None:
    _store().forget(*prefixes)


# Lifetimes, in seconds. Short enough that a change made elsewhere shows up soon; every one has a Refresh in the UI.
ACCOUNTS_TTL = 20.0
SUBSCRIPTIONS_TTL = 60.0
DISCOVERY_TTL = 300.0
WORKSPACES_TTL = 60.0

__all__ = ["TtlCache", "cached", "forget", "ACCOUNTS_TTL", "SUBSCRIPTIONS_TTL", "DISCOVERY_TTL", "WORKSPACES_TTL"]
