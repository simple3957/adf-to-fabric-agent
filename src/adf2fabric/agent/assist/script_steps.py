"""Model-drafted code for the steps of a U-SQL script or an SSIS package the fixed rules cannot translate (2026-11-18). Optional, off unless
``ADF2FABRIC_LLM=on``. The same rules as ``dataflow_steps.py``:

* the translator (``workloads/usql.py``, ``workloads/ssis.py``) never guesses: a statement or task outside its rules becomes a cell that raises;
* a person may ask Claude (the Claude Code CLI with no tools, ``assist/model.py``) for the PySpark / Python of that ONE step. The draft is stored,
  not used; a person reads it and accepts it; only then does generation put it in place of the raising cell, marked as model-drafted with who
  accepted it and when;
* a draft is pinned to a hash of the whole script, so a changed script stops using it; a draft the model says is incomplete is refused;
* what leaves the machine when a draft is asked for: the script (a U-SQL file or a package's XML, which holds connection strings but no stored
  passwords: SSIS removes sensitive values unless a protection level keeps them; check yours) and the step's key.

Files: ``projects/<p>/assist/<kind>/<item>/<step key>.json`` (kind: ``usql`` | ``ssis``).
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.assist import model as M

KINDS = ("usql", "ssis")
_SYSTEM = {
    "usql": ("You translate ONE statement of a U-SQL script (Azure Data Lake Analytics) into PySpark for a Microsoft Fabric notebook. Answer only with the JSON "
             "object asked for. The code reads the DataFrames named in the request (Python variables) and assigns the statement's output DataFrame variable "
             "(or, for OUTPUT, writes the file with the helper `_p(path)` and `notebookutils.fs`). Use pyspark.sql functions imported as F and Window; no "
             "other imports. If any part cannot be translated faithfully, say so in 'unconverted' rather than approximating."),
    "ssis": ("You translate ONE task (or one data flow component) of a SQL Server Integration Services package into Python / PySpark for a Microsoft Fabric "
             "notebook. Answer only with the JSON object asked for. Package variables are Python variables named v_<name>, package parameters p_<name>; the "
             "helpers `_p(path)`, `_jdbc_read`, `_jdbc_write`, `_sql_exec` and `notebookutils` exist. A data flow component assigns its output DataFrame "
             "variable named in the request. Use pyspark.sql functions imported as F and Window; no other imports. If any part cannot be translated "
             "faithfully, say so in 'unconverted' rather than approximating."),
}


class StepDraftError(Exception):
    pass


def _safe(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.@#-]", "_", text)[:120] or "item"


def _dir(project_dir: Path, kind: str, item: str) -> Path:
    if kind not in KINDS:
        raise StepDraftError(f"Unknown kind '{kind}' (use one of: {', '.join(KINDS)}).")
    return Path(project_dir) / "assist" / kind / _safe(item)


def _hash(script: str) -> str:
    return hashlib.sha256((script or "").encode("utf-8")).hexdigest()


def load(project_dir: Path, kind: str, item: str, key: str) -> Optional[dict[str, Any]]:
    p = _dir(project_dir, kind, item) / f"{_safe(key)}.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def _save(project_dir: Path, kind: str, item: str, key: str, data: dict[str, Any]) -> Path:
    d = _dir(project_dir, kind, item)
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{_safe(key)}.json"
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return p


def prompt(kind: str, script: str, key: str, why: str, ctx: dict[str, Any]) -> str:
    frames = ctx.get("rowsets") or ctx.get("dataframes") or {}
    have = "; ".join(f"{name}({', '.join(cols)})" if cols else name for name, cols in frames.items()) or "(none yet)"
    extra = ""
    if kind == "ssis":
        extra = (f"Package variables (Python names): {', '.join(ctx.get('variables') or []) or '(none)'}. "
                 f"Output DataFrame variable to assign: `{ctx.get('output') or '(none: a control flow task)'}`.\n")
    else:
        extra = f"Output DataFrame variable to assign: `{ctx.get('output') or '(OUTPUT: write the file)'}`.\n"
    return (f"Script:\n```\n{script}\n```\n\nTranslate only '{key}' ({why}).\nStatement / task detail:\n```\n{ctx.get('text', '')}\n```\n"
            f"DataFrames available: {have}.\n{extra}"
            "Answer with JSON: {\"code\": \"<python>\", \"notes\": [..], \"unconverted\": [..], \"confidence\": \"low|medium|high\"}.")


class _StepModel(M.ClaudeCli):
    def __init__(self, kind: str, **kw: Any) -> None:
        super().__init__(**kw)
        self._kind = kind

    def command(self, exe: str) -> list[str]:
        args = super().command(exe)
        args[args.index("--system-prompt") + 1] = _SYSTEM[self._kind]
        return args


def draft(project_dir: Path, kind: str, item: str, script: str, key: str, why: str, ctx: dict[str, Any],
          model: Optional[M.ClaudeCli] = None) -> dict[str, Any]:
    """Ask the model for one step's code and store it as a draft (not used until a person accepts it)."""
    if not M.enabled():
        raise StepDraftError("Model-assisted drafts are off. Set ADF2FABRIC_LLM=on to allow the script to be sent to Claude.")
    ans = (model or _StepModel(kind)).complete(prompt(kind, script, key, why, ctx))
    data = {"kind": kind, "item": item, "step": key, "status": "draft", "code": ans.code, "notes": ans.notes, "unconverted": ans.unconverted,
            "confidence": ans.confidence, "model": ans.model, "script_sha256": _hash(script),
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    _save(project_dir, kind, item, key, data)
    return data


def accept(project_dir: Path, kind: str, item: str, script: str, key: str, by: str) -> dict[str, Any]:
    """A person accepts a draft after reading it. Refused when the script changed since the draft or the model left parts unconverted."""
    d = load(project_dir, kind, item, key)
    if d is None:
        raise StepDraftError(f"There is no draft for '{key}'.")
    if d.get("script_sha256") != _hash(script):
        raise StepDraftError("The script changed since this draft was made: draft it again.")
    if d.get("unconverted"):
        raise StepDraftError("The model said parts of this step are not converted: " + "; ".join(d["unconverted"][:3]))
    if not by.strip():
        raise StepDraftError("Say who is accepting it.")
    d.update(status="accepted", accepted_by=by.strip(), accepted_at=datetime.now(timezone.utc).date().isoformat())
    _save(project_dir, kind, item, key, d)
    return d


def accepted(project_dir: Path, kind: str, item: str, script: str) -> dict[str, dict[str, Any]]:
    """step key -> accepted draft still pinned to this script (what generation uses)."""
    d = _dir(project_dir, kind, item)
    out: dict[str, dict[str, Any]] = {}
    for p in sorted(d.glob("*.json")) if d.is_dir() else []:
        data = json.loads(p.read_text(encoding="utf-8"))
        if data.get("status") == "accepted" and data.get("script_sha256") == _hash(script):
            out[data["step"]] = data
    return out


def cache_file(project_dir: Path, kind: str, item: str) -> Path:
    """Where a script / package is kept in the project: ``workloads/code/<kind>/<its folders>``; a Windows drive or a character a path cannot hold becomes ``_``."""
    parts = [re.sub(r'[<>:"|?*]', "_", x) for x in re.split(r"[\\/]+", item) if x and x not in (".", "..")]
    return Path(project_dir) / "workloads" / "code" / kind / Path(*(parts or ["item"]))


def cached_script(project_dir: Path, kind: str, item: str) -> Optional[str]:
    """The script the last generate cached or a person supplied, for the CLI to list and draft steps."""
    f = cache_file(project_dir, kind, item)
    return f.read_text(encoding="utf-8-sig", errors="replace") if f.is_file() else None
