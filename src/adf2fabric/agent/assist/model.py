"""The model, reached through the Claude Code CLI in headless mode. Optional, and off unless ``ADF2FABRIC_LLM=on``.

``claude -p`` reads the prompt on stdin and prints one JSON result. It runs with every tool switched off, no MCP servers, no saved session, and
from an empty temporary directory, so it can only answer: it cannot read this project's files, run commands or reach Azure or Fabric.
``--json-schema`` makes it answer in a fixed shape. It signs in the way the CLI on this machine already is, so this tool never holds an API key.

The model never produces Fabric JSON and nothing it says is used as it comes: an answer is a draft of corrected *ADF source* that a person reviews
(``service.py``), and the deterministic converter still turns whatever is accepted into Fabric JSON.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

DEFAULT_TIMEOUT = 180
CONFIDENCE = ("high", "medium", "low")
RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "code": {"type": "string"},
        "notes": {"type": "array", "items": {"type": "string"}},
        "unconverted": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "string", "enum": list(CONFIDENCE)},
    },
    "required": ["code", "notes", "unconverted", "confidence"],
    "additionalProperties": False,
}
SYSTEM_PROMPT = ("You help migrate Azure Data Factory pipelines to Microsoft Fabric Data Factory. "
                 "Answer only with the JSON object the user asks for. You have no tools and need none.")
_ON = {"on", "1", "true", "yes", "enabled"}


class ModelError(Exception):
    """The model could not be reached or gave no usable answer; the message is for a person."""


@dataclass
class Answer:
    code: str
    notes: list[str] = field(default_factory=list)
    unconverted: list[str] = field(default_factory=list)
    confidence: str = "low"
    model: str = ""
    cost_usd: Optional[float] = None
    duration_ms: int = 0


def enabled() -> bool:
    """Off by default: nothing about a project leaves the machine unless this is switched on."""
    return (os.environ.get("ADF2FABRIC_LLM") or "off").strip().lower() in _ON


def executable() -> Optional[str]:
    configured = os.environ.get("CLAUDE_CLI")
    if configured:
        return configured if os.path.isfile(configured) else shutil.which(configured)
    return shutil.which("claude")


def _strip_fences(code: str) -> str:
    text = code.strip()
    if text.startswith("```") and text.endswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        text = text.rsplit("```", 1)[0]
    return text.strip()


def parse(stdout: str) -> Answer:
    """The CLI's ``--output-format json`` result -> an Answer."""
    try:
        envelope = json.loads(stdout)
    except ValueError as exc:
        raise ModelError("Claude Code did not return JSON. Run `claude -p \"hello\"` in a terminal to check it works.") from exc
    if not isinstance(envelope, dict):
        raise ModelError("Claude Code returned an unexpected result.")
    if envelope.get("is_error") or envelope.get("subtype") not in (None, "success"):
        raise ModelError(f"Claude Code reported an error: {str(envelope.get('result') or envelope.get('subtype') or 'unknown error')[:300]}")
    data: Any = envelope.get("structured_output")
    if data is None:
        try:
            data = json.loads(_strip_fences(str(envelope.get("result") or "")))
        except ValueError as exc:
            raise ModelError("Claude Code answered, but not with the JSON object that was asked for.") from exc
    if not isinstance(data, dict) or not isinstance(data.get("code"), str) or not data["code"].strip():
        raise ModelError("Claude Code's answer has no code.")
    usage = envelope.get("modelUsage") or {}
    confidence = str(data.get("confidence") or "low").lower()
    return Answer(
        code=_strip_fences(data["code"]),
        notes=[str(n) for n in data.get("notes") or [] if str(n).strip()],
        unconverted=[str(n) for n in data.get("unconverted") or [] if str(n).strip()],
        confidence=confidence if confidence in CONFIDENCE else "low",
        model=next(iter(usage), "") if isinstance(usage, dict) else "",
        cost_usd=envelope.get("total_cost_usd") if isinstance(envelope.get("total_cost_usd"), (int, float)) else None)


class ClaudeCli:
    """One ``claude -p`` call per prompt."""

    def __init__(self, model: Optional[str] = None, timeout: Optional[int] = None,
                 run: Callable[..., "subprocess.CompletedProcess[str]"] = subprocess.run) -> None:
        self.model = model if model is not None else (os.environ.get("ADF2FABRIC_LLM_MODEL") or None)
        self.timeout = timeout or int(os.environ.get("ADF2FABRIC_LLM_TIMEOUT") or DEFAULT_TIMEOUT)
        self._run = run

    def available(self) -> dict[str, Any]:
        exe = executable()
        return {"available": exe is not None, "model": self.model or "Claude Code default", "timeout_seconds": self.timeout,
                "message": None if exe else "The Claude Code CLI (`claude`) is not on this machine's PATH. Install it and sign in once, or set CLAUDE_CLI."}

    def command(self, exe: str) -> list[str]:
        args = [exe, "-p", "--output-format", "json", "--tools", "", "--strict-mcp-config", "--no-session-persistence",
                "--system-prompt", SYSTEM_PROMPT, "--json-schema", json.dumps(RESULT_SCHEMA, separators=(",", ":"))]
        if self.model:
            args += ["--model", self.model]
        return args

    def complete(self, prompt: str) -> Answer:
        exe = executable()
        if exe is None:
            raise ModelError(str(self.available()["message"]))
        started = time.monotonic()
        with tempfile.TemporaryDirectory(prefix="adf2fabric-assist-") as workdir:  # an empty folder: there is nothing here for it to read
            try:
                done = self._run(self.command(exe), input=prompt, capture_output=True, text=True, encoding="utf-8",
                                 timeout=self.timeout, cwd=workdir, check=False)
            except subprocess.TimeoutExpired as exc:
                raise ModelError(f"Claude Code did not answer within {self.timeout} seconds. Try again, or raise ADF2FABRIC_LLM_TIMEOUT.") from exc
            except OSError as exc:
                raise ModelError(f"Claude Code could not be started: {exc.strerror or exc}.") from exc
        if done.returncode != 0 and not (done.stdout or "").strip():
            detail = (done.stderr or "").strip().splitlines()
            raise ModelError(f"Claude Code failed ({detail[-1][:300] if detail else f'exit code {done.returncode}'}). "
                             "If it asks you to sign in, run `claude` once in a terminal.")
        answer = parse(done.stdout or "")
        answer.duration_ms = int((time.monotonic() - started) * 1000)
        return answer
