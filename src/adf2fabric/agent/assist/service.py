"""Drafts on request, a person accepts them, ``convert --assisted`` uses what was accepted.

What this changes about the project's principles, deliberately and only on request: the pipeline's definition (secrets removed) is sent to a model
(Claude, through the Claude Code CLI on this machine), and a model's answer is never used as it comes. It is checked, shown to a person next to the
source and what the converter did, and used by a conversion only once that person accepts it, only while the source still has the hash it had when the
draft was made, and only after the deterministic converter has read it as ordinary ADF source. Every pipeline converted that way is marked.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.assist import checks, context, model as model_mod, templates
from adf2fabric.agent.assist.model import ModelError
from adf2fabric.project.locks import lock_for, write_atomic

MAX_NAMES = 25
MAX_ACCEPT_BYTES = 1024 * 1024
REPO_ROOT = Path(__file__).resolve().parents[4]
QUEUED, RUNNING, FAILED = "queued", "running", "failed"
_JOBS: dict[tuple[str, str], dict[str, Any]] = {}  # (project dir, pipeline lower) -> {"state", "error", "started_at"}; memory only
_JOBS_LOCK = threading.Lock()


class AssistError(Exception):
    """A request that cannot be done; the message is for a person. ``status`` is the HTTP status an API should use."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def prompts_dir() -> Path:
    return Path(os.environ.get("ADF2FABRIC_PROMPTS_DIR") or REPO_ROOT / "prompts" / "assist")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class AssistStore:
    """One JSON file per pipeline under ``<project>/assist/``: its draft and its accepted version. No secrets: names, hashes, code, notes."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def path(self, name: str) -> Path:
        readable = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:60].strip("._") or "pipeline"
        return self.directory / f"{readable}.{hashlib.sha1(name.lower().encode('utf-8'), usedforsecurity=False).hexdigest()[:10]}.json"

    def get(self, name: str) -> dict[str, Any]:
        try:
            return json.loads(self.path(name).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"pipeline": name, "draft": None, "accepted": None}

    def put(self, record: dict[str, Any]) -> None:
        with lock_for(self.path(record["pipeline"])):
            write_atomic(self.path(record["pipeline"]), json.dumps(record, indent=2, ensure_ascii=False))

    def delete(self, name: str) -> None:
        with lock_for(self.path(name)):
            self.path(name).unlink(missing_ok=True)

    def all(self) -> list[dict[str, Any]]:
        out = []
        for f in sorted(self.directory.glob("*.json")) if self.directory.is_dir() else []:
            try:
                out.append(json.loads(f.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                continue
        return out


class AssistService:
    def __init__(self, store: Any, pid: str, model: Any = None, prompts: Optional[Path] = None, synchronous: bool = False,
                 workers: Optional[int] = None) -> None:
        self.store, self.pid = store, pid
        self._dir = store.project_dir(pid)
        self.drafts = AssistStore(self._dir / "assist")
        self._model = model or model_mod.ClaudeCli()
        self._prompts = prompts or prompts_dir()
        self._synchronous = synchronous
        self._workers = workers or int(os.environ.get("ADF2FABRIC_LLM_WORKERS") or 2)

    # -- state ----------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def enabled() -> bool:
        return model_mod.enabled()

    def _job(self, name: str) -> dict[str, Any]:
        with _JOBS_LOCK:
            return dict(_JOBS.get((str(self._dir), name.lower())) or {})

    def _set_job(self, name: str, value: Optional[dict[str, Any]]) -> None:
        with _JOBS_LOCK:
            if value is None:
                _JOBS.pop((str(self._dir), name.lower()), None)
            else:
                _JOBS[(str(self._dir), name.lower())] = value

    def settings(self) -> dict[str, Any]:
        return {"enabled": self.enabled(), **self._model.available(), "prompts_dir": self._prompts.name,
                "disabled_reason": None if self.enabled() else "Model-assisted drafts are off. Set ADF2FABRIC_LLM=on to allow them (the pipeline's definition, "
                                                               "with secrets removed, is then sent to the model on request)."}

    # -- views ----------------------------------------------------------------------------------------------------------------------
    def _subject(self, name: str) -> context.Subject:
        try:
            return context.load_subject(self.store, self.pid, name)
        except context.NotAssistable as exc:
            raise AssistError(str(exc), 400) from exc

    def _view(self, name: str, current_hash: Optional[str]) -> dict[str, Any]:
        record, job = self.drafts.get(name), self._job(name)

        def part(p: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
            return None if not p else {**p, "stale": (p.get("source_hash") != current_hash) if current_hash else None}

        draft, accepted = part(record.get("draft")), part(record.get("accepted"))
        state = job["state"] if job.get("state") in (QUEUED, RUNNING, FAILED) else "draft" if draft else "accepted" if accepted else "none"
        return {"pipeline": name, "state": state, "error": job.get("error"), "draft": draft, "accepted": accepted}

    def candidates(self) -> dict[str, Any]:
        try:
            found = context.candidates(self.store, self.pid)
        except context.NotAssistable as exc:
            raise AssistError(str(exc), 409) from exc
        items = []
        for c in found:
            try:
                h = context.source_hash(context._raw_pipeline(self.store, self.pid, c["pipeline"]))
            except context.NotAssistable:
                h = None
            v = self._view(c["pipeline"], h)
            latest = v["draft"] or v["accepted"] or {}
            items.append({**c, "state": v["state"], "error": v["error"], "confidence": latest.get("confidence"), "stale": latest.get("stale")})
        count = lambda s: sum(1 for i in items if i["state"] == s)  # noqa: E731
        return {"items": items, "summary": {"total": len(items), "drafts": count("draft"), "accepted": count("accepted"),
                                            "working": count(QUEUED) + count(RUNNING), "failed": count(FAILED)}}

    def detail(self, name: str) -> dict[str, Any]:
        s = self._subject(name)
        v = self._view(s.name, s.source_hash)
        v["source"] = {"code": s.code, "hash": s.source_hash, "problems": s.problems, "summary": s.summary, "removed": s.removed}
        return v

    def prompt(self, name: str) -> dict[str, Any]:
        s = self._subject(name)
        try:
            text = context.build(s, self._prompts)
        except templates.TemplateError as exc:
            raise AssistError(str(exc), 500) from exc
        return {"pipeline": s.name, "prompt": text, "characters": len(text)}

    # -- actions --------------------------------------------------------------------------------------------------------------------
    def generate(self, names: list[str]) -> dict[str, Any]:
        if not self.enabled():
            raise AssistError("Model-assisted drafts are off on this server (ADF2FABRIC_LLM is not 'on').", 409)  # not 403: that means the caller's role is denied
        avail = self._model.available()
        if not avail.get("available"):
            raise AssistError(str(avail.get("message") or "Claude Code is not available on this machine."), 409)
        if not names or len(names) > MAX_NAMES or not all(isinstance(n, str) and n for n in names):
            raise AssistError(f"Send between 1 and {MAX_NAMES} pipeline names.", 400)
        results, work = [], []
        for name in dict.fromkeys(names):
            if self._job(name).get("state") in (QUEUED, RUNNING):
                results.append({"pipeline": name, "state": QUEUED, "error": None})
                continue
            try:
                s = self._subject(name)
                prompt = context.build(s, self._prompts)
            except (AssistError, templates.TemplateError) as exc:
                if len(names) == 1:
                    raise
                results.append({"pipeline": name, "state": "rejected", "error": str(exc)})
                continue
            self._set_job(s.name, {"state": QUEUED, "error": None, "started_at": _now()})
            work.append((s, prompt))
            results.append({"pipeline": s.name, "state": QUEUED, "error": None})
        if self._synchronous:
            for s, prompt in work:
                self._work(s, prompt)
        elif work:
            pool = ThreadPoolExecutor(max_workers=self._workers, thread_name_prefix="assist")
            for s, prompt in work:
                pool.submit(self._work, s, prompt)
            pool.shutdown(wait=False)
        return {"items": results}

    def _work(self, s: context.Subject, prompt: str) -> None:
        self._set_job(s.name, {"state": RUNNING, "error": None, "started_at": _now()})
        try:
            answer = self._model.complete(prompt)
            found = checks.run(self.store, self.pid, s, answer.code, answer.unconverted)
            record = self.drafts.get(s.name)
            record["pipeline"] = s.name
            record["draft"] = {"code": answer.code, "notes": answer.notes, "unconverted": answer.unconverted, "confidence": answer.confidence, "checks": found,
                               "source_hash": s.source_hash, "generated_at": _now(), "model": answer.model, "cost_usd": answer.cost_usd,
                               "duration_ms": answer.duration_ms}
            self.drafts.put(record)
        except ModelError as exc:
            self._set_job(s.name, {"state": FAILED, "error": str(exc), "started_at": _now()})
            return
        except Exception as exc:  # noqa: BLE001 - one draft never takes the service down
            self._set_job(s.name, {"state": FAILED, "error": f"The draft could not be made ({type(exc).__name__}).", "started_at": _now()})
            return
        self._set_job(s.name, None)

    def accept(self, name: str, code: Optional[str] = None, by: str = "") -> dict[str, Any]:
        s = self._subject(name)
        record = self.drafts.get(s.name)
        draft = record.get("draft")
        if not draft:
            raise AssistError("There is no draft to accept. Generate one first.", 409)
        if draft.get("source_hash") != s.source_hash:
            raise AssistError("The source changed since this draft was made. Generate a new draft.", 409)
        text = draft["code"] if code is None else code
        if not isinstance(text, str) or not text.strip() or len(text.encode("utf-8")) > MAX_ACCEPT_BYTES:
            raise AssistError("The code to accept is empty or too long.", 400)
        edited = text.strip() != draft["code"].strip()
        found = checks.run(self.store, self.pid, s, text, draft.get("unconverted") or []) if edited else draft.get("checks") or []
        if checks.blocking(found):
            raise AssistError("It cannot be accepted yet: " + " ".join(c["message"] for c in found if c["level"] == "error"), 400)
        record["accepted"] = {"code": text.strip(), "notes": draft.get("notes") or [], "unconverted": draft.get("unconverted") or [],
                              "confidence": draft.get("confidence"), "checks": found, "source_hash": s.source_hash, "generated_at": draft.get("generated_at"),
                              "accepted_at": _now(), "accepted_by": by or "local", "edited": edited, "model": draft.get("model")}
        record["draft"] = None
        self.drafts.put(record)
        return self.detail(name)

    def discard(self, name: str, scope: str = "draft") -> dict[str, Any]:
        if scope not in ("draft", "all"):
            raise AssistError("scope must be draft or all.", 400)
        if self._job(name).get("state") in (QUEUED, RUNNING):
            raise AssistError("A draft is being made for this pipeline; wait for it to finish.", 409)
        self._set_job(name, None)
        record = self.drafts.get(name)
        record["draft"] = None
        if scope == "all":
            record["accepted"] = None
        if record.get("accepted"):
            self.drafts.put(record)
        else:
            self.drafts.delete(name)
        try:
            return self.detail(name)
        except AssistError:
            return {"pipeline": name, "state": "none", "draft": None, "accepted": None}


def apply(store: Any, pid: str, discovery: Any) -> tuple[Any, list[dict[str, Any]], list[str]]:
    """The semantic record with every accepted, still-current draft in place of its pipeline, the notes to record, and log lines.

    A draft whose source changed since is ignored (the original is converted) and the log says so. The drafts are ordinary ADF source from here on:
    the deterministic converter reads them exactly as it reads the original."""
    from adf2fabric.agent.discovery.adf_semantic import parser_pipelines as pp

    svc = AssistService(store, pid)
    replaced, notes, log = {}, [], []
    for record in svc.drafts.all():
        accepted = record.get("accepted")
        name = str(record.get("pipeline") or "")
        if not accepted or not name:
            continue
        try:
            current = context.source_hash(context._raw_pipeline(store, pid, name))
        except context.NotAssistable:
            continue
        if accepted.get("source_hash") != current:
            log.append(f"{name}: the accepted model-assisted draft is out of date (the source changed); the original source is converted instead.")
            continue
        try:
            replaced[name.lower()] = pp.parse_pipeline(json.loads(accepted["code"]), f"pipeline/{name}.json")
        except (ValueError, KeyError):
            log.append(f"{name}: the accepted draft could not be read; the original source is converted instead.")
            continue
        notes.append({"pipeline": name, "accepted_at": accepted.get("accepted_at"), "accepted_by": accepted.get("accepted_by"),
                      "edited": bool(accepted.get("edited")), "model": accepted.get("model") or "", "source_hash": current})
    if not replaced:
        return discovery, [], log
    others = [p for p in discovery.pipelines if p.name.lower() not in replaced]
    new = list(replaced.values())
    pp.resolve_pipelines(new, discovery.datasets, discovery.linked_services)
    return discovery.model_copy(update={"pipelines": [*others, *new]}), notes, log
