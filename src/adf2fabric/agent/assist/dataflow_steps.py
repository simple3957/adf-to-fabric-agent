"""Model-drafted code for mapping data flow steps the fixed rules cannot translate (2026-11-17). Optional, off unless ``ADF2FABRIC_LLM=on``.

The translator (``agent/workloads/dataflow.py``) never guesses: a step outside its rules becomes a cell that raises. Here a person may ask
Claude (the Claude Code CLI, no tools, as ``agent/assist/model.py``) for the PySpark of that one step. The draft is stored, not used; a
person reads it and accepts it; only then does generation put it in place of the raising cell, marked as model-drafted with who accepted
it. A draft is pinned to a hash of the data flow script: when the script changes, the draft is no longer used. The notebook itself still
needs its normal approval before Deploy creates it, and only a run compared with ADF proves it. What leaves the machine when a draft is
asked for: the data flow script (no credentials are in it) and the step's name.

Files: ``projects/<p>/assist/dataflow/<data flow>/<step>.json``.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.assist import model as M
from adf2fabric.agent.workloads import dataflow as DF

SYSTEM_PROMPT = ("You translate ONE step of an Azure Data Factory mapping data flow (data flow script) into PySpark for a Microsoft Fabric "
                 "notebook. Answer only with the JSON object asked for. The code must read the step's input DataFrame variable(s) and assign "
                 "its output DataFrame variable, using pyspark.sql functions imported as F and Window; no reads, writes, prints or imports of "
                 "other libraries. If any part cannot be translated faithfully, say so in 'unconverted' rather than approximating.")


class StepDraftError(Exception):
    pass


def _dir(project_dir: Path, dataflow: str) -> Path:
    return Path(project_dir) / "assist" / "dataflow" / re.sub(r"[^A-Za-z0-9_.-]", "_", dataflow)


def _hash(script: str) -> str:
    return hashlib.sha256((script or "").encode("utf-8")).hexdigest()


def script_of(project_dir: Path, dataflow: str) -> str:
    """The data flow's script from the imported source (the same text generation translates)."""
    from adf2fabric.agent.workloads.engine import _dataflow_file

    path = _dataflow_file(Path(project_dir), dataflow)
    if path is None:
        raise StepDraftError(f"Data flow '{dataflow}' is not in the imported source.")
    tp = (json.loads(path.read_text(encoding="utf-8-sig")).get("properties") or {}).get("typeProperties") or {}
    return DF.script_text(tp) or ""


def candidates(project_dir: Path, dataflow: str, script: str) -> list[dict[str, Any]]:
    """The transformation steps the fixed rules do not translate, with their input / output variables and any draft already made.
    Sources and sinks are not offered: they read and write storage, which a model never writes (their datasets are a manual fix)."""
    tr = DF.translate(script, {}, {}, {})
    steps = {s.name: s for s in DF.parse(script).statements}
    out = []
    for u in tr.untranslated:
        name = u.split(" ", 1)[0]
        st = steps.get(name)
        if st is None or st.transform in ("source", "sink"):
            continue
        d = load(project_dir, dataflow, name)
        out.append({"step": name, "why": u, "inputs": [DF._var(i) for i in (st.inputs if st else [])], "output": DF._var(name),
                    "draft": None if d is None else {k: d.get(k) for k in ("status", "confidence", "accepted_by", "current")}})
    return out


def load(project_dir: Path, dataflow: str, step: str) -> Optional[dict[str, Any]]:
    p = _dir(project_dir, dataflow) / f"{re.sub(r'[^A-Za-z0-9_.-]', '_', step)}.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def _save(project_dir: Path, dataflow: str, step: str, data: dict[str, Any]) -> Path:
    d = _dir(project_dir, dataflow)
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{re.sub(r'[^A-Za-z0-9_.-]', '_', step)}.json"
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return p


def prompt(script: str, step: dict[str, Any]) -> str:
    return (f"Data flow script:\n```\n{script}\n```\n\nTranslate the step '{step['step']}' only ({step['why']}).\n"
            f"Input DataFrame variable(s): {', '.join(step['inputs']) or '(none)'}. Assign the result to `{step['output']}`.\n"
            "Answer with JSON: {\"code\": \"<python>\", \"notes\": [..], \"unconverted\": [..], \"confidence\": \"low|medium|high\"}.")


class _StepModel(M.ClaudeCli):
    def command(self, exe: str) -> list[str]:
        args = super().command(exe)
        args[args.index("--system-prompt") + 1] = SYSTEM_PROMPT
        return args


def draft(project_dir: Path, dataflow: str, script: str, step_name: str, model: Optional[M.ClaudeCli] = None) -> dict[str, Any]:
    """Ask the model for one step's code and store it as a draft (not used until a person accepts it)."""
    if not M.enabled():
        raise StepDraftError("Model-assisted drafts are off. Set ADF2FABRIC_LLM=on to allow the data flow script to be sent to Claude.")
    step = next((c for c in candidates(project_dir, dataflow, script) if c["step"] == step_name), None)
    if step is None:
        raise StepDraftError(f"Step '{step_name}' is translated by the fixed rules (or is not in the data flow); there is nothing to draft.")
    ans = (model or _StepModel()).complete(prompt(script, step))
    data = {"dataflow": dataflow, "step": step_name, "status": "draft", "code": ans.code, "notes": ans.notes, "unconverted": ans.unconverted,
            "confidence": ans.confidence, "model": ans.model, "script_sha256": _hash(script),
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    _save(project_dir, dataflow, step_name, data)
    return data


def accept(project_dir: Path, dataflow: str, script: str, step_name: str, by: str) -> dict[str, Any]:
    """A person accepts a draft after reading it. Refused when the script changed since the draft or the model left parts unconverted."""
    d = load(project_dir, dataflow, step_name)
    if d is None:
        raise StepDraftError(f"There is no draft for step '{step_name}'.")
    if d.get("script_sha256") != _hash(script):
        raise StepDraftError("The data flow changed since this draft was made: draft it again.")
    if d.get("unconverted"):
        raise StepDraftError("The model said parts of this step are not converted: " + "; ".join(d["unconverted"][:3]))
    if not by.strip():
        raise StepDraftError("Say who is accepting it.")
    d.update(status="accepted", accepted_by=by.strip(), accepted_at=datetime.now(timezone.utc).date().isoformat())
    _save(project_dir, dataflow, step_name, d)
    return d


def accepted(project_dir: Path, dataflow: str, script: str) -> dict[str, dict[str, Any]]:
    """step -> accepted draft still pinned to this script (what generation uses)."""
    d = _dir(project_dir, dataflow)
    out: dict[str, dict[str, Any]] = {}
    for p in sorted(d.glob("*.json")) if d.is_dir() else []:
        data = json.loads(p.read_text(encoding="utf-8"))
        if data.get("status") == "accepted" and data.get("script_sha256") == _hash(script):
            out[data["step"]] = data
    return out
