"""Running external CLIs (az, fab, git) safely.

* never through a shell, so no argument is ever interpreted by one;
* output is decoded leniently and ALWAYS redacted before it is returned;
* a missing executable, a timeout and a non-zero exit are all ordinary results, not exceptions;
* tests inject a fake runner, so nothing here is needed to exercise the providers.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Callable, Optional

from adf2fabric.connections.redaction import redact


@dataclass
class CliResult:
    found: bool
    returncode: int = 0
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.found and not self.timed_out and self.returncode == 0

    @property
    def text(self) -> str:
        return (self.stdout + "\n" + self.stderr).strip()


Runner = Callable[..., CliResult]


def find_executable(name: str, override_env: Optional[str] = None) -> Optional[str]:
    """Path of ``name`` on PATH, or the path in the environment variable ``override_env`` when that file exists."""
    if override_env:
        candidate = os.environ.get(override_env)
        if candidate and os.path.isfile(candidate):
            return candidate
    return shutil.which(name)


def run_cli(args: list[str], *, timeout: float = 60, env_extra: Optional[dict[str, str]] = None,
            exe_env: Optional[str] = None, secrets: tuple[str, ...] = (), raw: bool = False) -> CliResult:
    """Run ``args`` (``args[0]`` is the program name). Output is redacted, including any ``secrets`` passed in.

    ``raw=True`` skips redaction of stdout and exists ONLY for fetching a token that the caller keeps in a local variable.
    """
    exe = find_executable(args[0], exe_env)
    if exe is None:
        return CliResult(found=False, returncode=127, stderr=f"'{args[0]}' was not found on PATH")
    env = {**os.environ, "NO_COLOR": "1", "PYTHONIOENCODING": "utf-8", **(env_extra or {})}
    try:
        proc = subprocess.run([exe, *args[1:]], capture_output=True, timeout=timeout, env=env, shell=False)
    except subprocess.TimeoutExpired:
        return CliResult(found=True, returncode=124, stderr=f"'{args[0]}' timed out after {timeout:.0f}s", timed_out=True)
    except OSError as exc:
        return CliResult(found=False, returncode=126, stderr=redact(f"'{args[0]}' could not be started ({type(exc).__name__})"))
    out = proc.stdout.decode("utf-8", errors="replace")
    return CliResult(
        found=True, returncode=proc.returncode,
        stdout=out if raw else redact(out, *secrets),
        stderr=redact(proc.stderr.decode("utf-8", errors="replace"), *secrets))
