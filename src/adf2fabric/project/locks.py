"""One lock per file for read-modify-write records (the brief, the journey): shared by every thread AND every process on this machine.

Two requests that load the same record, change it and save it would otherwise lose one change (or collide on the temp file). The
thread lock keeps this process's requests apart; an OS lock on a ``<name>.lock`` sidecar keeps several server processes (or a CLI run
beside the API) apart. Network file systems that do not honour OS locks are not covered.
"""
from __future__ import annotations

import os
import threading
import time
import uuid
from pathlib import Path

if os.name == "nt":
    import msvcrt

    def _os_lock(fh) -> None:
        deadline = time.monotonic() + 30
        while True:
            try:
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("Another process holds this record for too long.") from None
                time.sleep(0.02)

    def _os_unlock(fh) -> None:
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _os_lock(fh) -> None:
        fcntl.flock(fh, fcntl.LOCK_EX)

    def _os_unlock(fh) -> None:
        fcntl.flock(fh, fcntl.LOCK_UN)


class _FileLock:
    """Re-entrant within a thread, exclusive across threads and processes."""

    def __init__(self, path: Path) -> None:
        self._path = path.with_name(path.name + ".lock")
        self._thread = threading.RLock()
        self._depth = 0
        self._fh = None

    def __enter__(self):
        self._thread.acquire()
        try:
            if self._depth == 0:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                self._fh = open(self._path, "a+b")
                self._fh.write(b" ") if self._fh.tell() == 0 else None  # msvcrt needs a byte to lock
                self._fh.flush()
                try:
                    _os_lock(self._fh)
                except BaseException:
                    self._fh.close()
                    self._fh = None
                    raise
            self._depth += 1
        except BaseException:
            self._thread.release()
            raise
        return self

    def __exit__(self, *exc) -> None:
        try:
            self._depth -= 1
            if self._depth == 0 and self._fh is not None:
                try:
                    _os_unlock(self._fh)
                finally:
                    self._fh.close()
                    self._fh = None
        finally:
            self._thread.release()


_guard = threading.Lock()
_locks: dict[str, _FileLock] = {}


def lock_for(path: Path) -> _FileLock:
    key = os.path.normcase(os.path.abspath(path))
    with _guard:
        if key not in _locks:
            _locks[key] = _FileLock(Path(key))
        return _locks[key]


def write_atomic(path: Path, text: str) -> None:
    """Write through a temp file with a name no other writer shares, then replace: readers never see half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
