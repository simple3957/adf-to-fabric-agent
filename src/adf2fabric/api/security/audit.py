"""Audit events for sensitive actions: who did what to which resource, and whether it was allowed. Never secrets, never bodies.

One JSON object per line in ``<projects_root>/_audit/audit.jsonl``. Each event records a timestamp, a request id, the actor (id and
organisation), the action name, the resource kind and id, the outcome (``allowed`` | ``denied`` | ``failed`` | ``ok``) and a small
``detail`` of short, redacted strings. Request bodies, tokens, URLs with credentials, connection strings and client addresses are
not recorded (a failed sign-in records a short hash of the address so repeated attempts can be correlated without keeping it).

Each line carries ``prev`` = the SHA-256 of the previous line, so removing or editing a line breaks the chain and ``verify`` says
where. That is tamper EVIDENCE, not tamper PROOF: someone who can rewrite the whole file can rewrite the chain. Ship the log
somewhere append-only for real assurance.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any, Optional

from adf2fabric.connections.redaction import redact
from adf2fabric.api.security.guards import scrub

MAX_BYTES = 10 * 1024 * 1024
_ALLOWED_DETAIL = 12


class AuditLog:
    def __init__(self, root: Path, sink: Optional[Any] = None):
        self.path = Path(root) / "_audit" / "audit.jsonl"
        self._lock = threading.Lock()
        self._last = ""
        self.sink = sink  # optional extra destination (for example a logger); receives the same redacted record

    @staticmethod
    def _clean(value: Any) -> Any:
        if isinstance(value, (int, float, bool)) or value is None:
            return value
        return scrub(redact(str(value)))[:200]

    def event(self, action: str, outcome: str, *, actor: str = "", org: str = "", resource: str = "", resource_id: str = "", request_id: str = "",
              **detail: Any) -> dict[str, Any]:
        rec = {"ts": round(time.time(), 3), "request_id": request_id, "actor": self._clean(actor), "org": self._clean(org), "action": action,
               "outcome": outcome, "resource": resource, "resource_id": self._clean(resource_id),
               "detail": {str(k)[:40]: self._clean(v) for k, v in list(detail.items())[:_ALLOWED_DETAIL]}}
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.path.exists() and self.path.stat().st_size > MAX_BYTES:
                self.path.replace(self.path.with_name(f"audit-{int(time.time())}.jsonl"))
                self._last = ""
            if not self._last:
                self._last = self._tail_hash()
            rec["prev"] = self._last
            line = json.dumps(rec, sort_keys=True, separators=(",", ":"))
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
            self._last = hashlib.sha256(line.encode("utf-8")).hexdigest()
        if self.sink is not None:
            try:
                self.sink(rec)
            except Exception:  # noqa: BLE001 - a failing sink must not stop the action it records
                pass
        return rec

    def _tail_hash(self) -> str:
        if not self.path.exists():
            return ""
        last = ""
        with self.path.open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    last = line.rstrip("\n")
        return hashlib.sha256(last.encode("utf-8")).hexdigest() if last else ""

    def read(self, limit: int = 200, org: Optional[str] = None) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        rows = [json.loads(x) for x in self.path.read_text(encoding="utf-8").splitlines() if x.strip()]
        if org is not None:
            rows = [r for r in rows if r.get("org") == org]
        return rows[-limit:]

    def verify(self) -> Optional[int]:
        """None if the hash chain is intact; else the 1-based line number where it breaks."""
        if not self.path.exists():
            return None
        prev = ""
        for i, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                return i
            if i == 1 and rec.get("prev", "") not in ("", prev):
                prev = rec.get("prev", "")  # a rotated file starts mid-chain
            if rec.get("prev", "") != prev:
                return i
            prev = hashlib.sha256(line.encode("utf-8")).hexdigest()
        return None


def addr_hash(client: Optional[str]) -> str:
    """A short, non-reversible tag for a client address, for correlating failed sign-ins without storing the address."""
    return hashlib.sha256((client or "?").encode("utf-8")).hexdigest()[:10]
