"""One lock per file, shared by every thread of this process, for read-modify-write records (the brief, the journey).

Two requests that load the same record, change it and save it would otherwise lose one change (or collide on the temp file).
This does not cover several server processes writing the same folder: run one API process per projects root.
"""
from __future__ import annotations

import os
import threading
import uuid
from pathlib import Path

_guard = threading.Lock()
_locks: dict[str, threading.RLock] = {}


def lock_for(path: Path) -> threading.RLock:
    key = os.path.normcase(os.path.abspath(path))
    with _guard:
        return _locks.setdefault(key, threading.RLock())


def write_atomic(path: Path, text: str) -> None:
    """Write through a temp file with a name no other writer shares, then replace: readers never see half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
