"""Rate limiting: a sliding window per key, in memory, with a separate, stricter lockout for failed sign-ins.

Per-process and non-persistent by design (a restart forgets it); a multi-instance deployment needs its limits at the gateway as
well. Keys are principal ids for authenticated requests and a hash of the client address for unauthenticated ones.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Callable, Optional


class RateLimiter:
    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._hits: dict[tuple[str, str], deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str, bucket: str, limit: int, window: float = 60.0) -> Optional[float]:
        """None if the request is allowed (and counted); else the seconds to wait."""
        now = self._clock()
        with self._lock:
            if len(self._hits) > 20000:  # bounded memory: drop keys whose window has fully passed
                for k in [k for k, d in self._hits.items() if not d or d[-1] <= now - 3600]:
                    del self._hits[k]
            q = self._hits[(key, bucket)]
            while q and q[0] <= now - window:
                q.popleft()
            if len(q) >= limit:
                return max(1.0, window - (now - q[0]))
            q.append(now)
            return None

    def blocked(self, key: str, bucket: str, limit: int, window: float) -> Optional[float]:
        """Without counting: seconds until ``key`` may try again, or None."""
        now = self._clock()
        with self._lock:
            q = self._hits[(key, bucket)]
            while q and q[0] <= now - window:
                q.popleft()
            return max(1.0, window - (now - q[0])) if len(q) >= limit else None

    def record(self, key: str, bucket: str) -> None:
        with self._lock:
            self._hits[(key, bucket)].append(self._clock())

    def clear(self, key: str, bucket: str) -> None:
        with self._lock:
            self._hits.pop((key, bucket), None)
