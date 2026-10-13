"""In-memory, time-limited holder for secrets a user types into the UI.

Contract (tested): a value goes in once, is readable only by backend code that asks for it by key, expires, can be wiped,
is never persisted, never appears in ``repr``/``str``/exceptions, and is never returned by any API. Python cannot reliably
zero a ``str``; the defence is a short lifetime, no copies in logs, and exclusion from every serialisation path.
"""

from __future__ import annotations

import threading
import time
from typing import Optional


class SecretVault:
    def __init__(self, ttl_seconds: float = 1800, clock=time.monotonic):
        self._ttl = ttl_seconds
        self._clock = clock
        self._items: dict[tuple[str, str, str, str], tuple[float, str]] = {}
        self._lock = threading.Lock()

    # keys: (session, scope, owner, field), e.g. ("s1", "linked_service", "LS_SQL", "password")
    def put(self, session: str, scope: str, owner: str, field: str, value: str) -> None:
        with self._lock:
            self._purge()
            self._items[(session, scope, owner, field)] = (self._clock() + self._ttl, value)

    def get(self, session: str, scope: str, owner: str, field: str) -> Optional[str]:
        with self._lock:
            self._purge()
            hit = self._items.get((session, scope, owner, field))
            return hit[1] if hit else None

    def has(self, session: str, scope: str, owner: str, field: str) -> bool:
        return self.get(session, scope, owner, field) is not None

    def fields(self, session: str, scope: str, owner: str) -> list[str]:
        """Names (never values) of the fields currently held for an owner."""
        with self._lock:
            self._purge()
            return sorted(f for (s, sc, o, f) in self._items if (s, sc, o) == (session, scope, owner))

    def wipe(self, session: str, scope: Optional[str] = None, owner: Optional[str] = None) -> int:
        with self._lock:
            doomed = [k for k in self._items
                      if k[0] == session and (scope is None or k[1] == scope) and (owner is None or k[2] == owner)]
            for k in doomed:
                del self._items[k]
            return len(doomed)

    def _purge(self) -> None:
        now = self._clock()
        for k in [k for k, (exp, _) in self._items.items() if exp <= now]:
            del self._items[k]

    def __len__(self) -> int:
        with self._lock:
            self._purge()
            return len(self._items)

    def __repr__(self) -> str:  # never reveals content
        return f"<SecretVault items={len(self)}>"

    __str__ = __repr__

    def __reduce__(self):  # refuse pickling/copying of secrets
        raise TypeError("SecretVault cannot be serialised")
