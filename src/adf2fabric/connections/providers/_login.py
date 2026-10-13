"""Starting a CLI sign-in (``az login`` / ``fab auth login``) in the background on the machine running the accelerator.

Sign-in opens a browser (or prints a device code) on this machine; no password or token ever passes through this process or
the browser the UI runs in. One sign-in per manager at a time. Output is collected redacted, capped, and only ever
displayed to explain what to do (for example a device code and its URL).
"""

from __future__ import annotations

import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

from adf2fabric.connections.redaction import redact

RUNNING, SUCCEEDED, FAILED, TIMED_OUT = "running", "succeeded", "failed", "timed_out"
_MAX_LINES = 60


@dataclass
class LoginAttempt:
    id: str
    command: str  # the program only, never its arguments (they may name a tenant or client)
    state: str = RUNNING
    lines: list[str] = field(default_factory=list)
    started: float = field(default_factory=time.time)

    def public(self) -> dict:
        return {"id": self.id, "command": self.command, "state": self.state, "output": list(self.lines)}


class LoginManager:
    def __init__(self, popen: Callable[..., subprocess.Popen] = subprocess.Popen, timeout: float = 300):
        self._popen = popen
        self._timeout = timeout
        self._attempts: dict[str, LoginAttempt] = {}
        self._active: Optional[str] = None
        self._lock = threading.Lock()

    def start(self, argv: list[str], *, env_extra: Optional[dict[str, str]] = None) -> LoginAttempt:
        import os

        with self._lock:
            if self._active and self._attempts[self._active].state == RUNNING:
                raise RuntimeError("A sign-in is already in progress; finish or wait for it first.")
            attempt = LoginAttempt(id=uuid.uuid4().hex[:12], command=argv[0])
            self._attempts[attempt.id] = attempt
            self._active = attempt.id
        env = {**os.environ, "NO_COLOR": "1", "PYTHONIOENCODING": "utf-8", **(env_extra or {})}
        # Resolve the program like run_cli does: on Windows 'az' is az.cmd, which Popen won't find by its bare name.
        exe = shutil.which(argv[0]) or argv[0]
        try:
            proc = self._popen([exe, *argv[1:]], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
                               shell=False)
        except OSError as exc:
            attempt.state = FAILED
            attempt.lines.append(redact(f"Could not start '{argv[0]}' ({type(exc).__name__})."))
            return attempt
        threading.Thread(target=self._pump, args=(attempt, proc), daemon=True).start()
        return attempt

    def get(self, attempt_id: str) -> Optional[LoginAttempt]:
        return self._attempts.get(attempt_id)

    def _pump(self, attempt: LoginAttempt, proc) -> None:
        deadline = attempt.started + self._timeout

        def watchdog() -> None:
            while attempt.state == RUNNING and time.time() < deadline:
                time.sleep(0.5)
            if attempt.state == RUNNING:
                attempt.state = TIMED_OUT
                try:
                    proc.kill()
                except Exception:  # noqa: BLE001
                    pass

        threading.Thread(target=watchdog, daemon=True).start()
        try:
            for line in proc.stdout or []:
                if len(attempt.lines) < _MAX_LINES:
                    attempt.lines.append(redact(line.rstrip()))
            rc = proc.wait()
        except Exception:  # noqa: BLE001
            rc = 1
        if attempt.state == RUNNING:
            attempt.state = SUCCEEDED if rc == 0 else FAILED
