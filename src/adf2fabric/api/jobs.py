"""Background jobs for long-running stages (imports, discovery, conversion, deployment).

Agents have no progress hooks, so progress is stage-level: a job is a function that may call ``log`` between steps. At most one
job per project runs at a time (a second request gets a conflict, not a race), results are secret-free summaries, and every
event passes through the redactor. Jobs live in memory and, when the registry is given a folder, each is also saved there as it starts and as it
ends, so the Activity page survives a restart and a job that was running when the server stopped is shown as interrupted, not as if it never
existed. They are still not a source of truth: the project files are.
"""

from __future__ import annotations

import json
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import tempfile
from pathlib import Path

from adf2fabric.project.locks import read_text_retry, write_atomic

from adf2fabric.api.security.guards import scrub
from adf2fabric.connections.redaction import redact, redact_mapping

_ROOTS = [Path(tempfile.gettempdir())]  # server paths in a job's log or error are replaced before anyone reads them

QUEUED, RUNNING, SUCCEEDED, FAILED = "queued", "running", "succeeded", "failed"
KEEP_FINISHED = 200  # finished jobs kept for the Activity page; older ones are dropped so a long-running server does not grow without bound
MAX_EVENTS = 2000  # log lines kept per job


class JobConflict(Exception):
    """Another job is already running for this project."""


@dataclass
class Job:
    id: str
    kind: str
    project_id: Optional[str]
    state: str = QUEUED
    events: list[dict[str, Any]] = field(default_factory=list)
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    created: float = field(default_factory=time.time)
    finished: Optional[float] = None
    org: Optional[str] = None  # the organisation that started it: only that organisation can see it
    owner: Optional[str] = None

    def record(self) -> dict[str, Any]:
        """Everything the job holds, as saved (the log, the result and the error were redacted when they were recorded)."""
        return {"id": self.id, "kind": self.kind, "project_id": self.project_id, "state": self.state, "events": self.events, "result": self.result,
                "error": self.error, "created": self.created, "finished": self.finished, "org": self.org, "owner": self.owner}

    @staticmethod
    def from_record(data: dict[str, Any]) -> "Job":
        return Job(id=str(data["id"]), kind=str(data["kind"]), project_id=data.get("project_id"), state=str(data.get("state") or FAILED),
                   events=list(data.get("events") or [])[:MAX_EVENTS], result=data.get("result"), error=data.get("error"),
                   created=float(data.get("created") or time.time()), finished=data.get("finished"), org=data.get("org"), owner=data.get("owner"))

    def public(self, since: int = 0) -> dict[str, Any]:
        end = self.finished or time.time()
        return {"id": self.id, "kind": self.kind, "project_id": self.project_id, "state": self.state,
                "events": self.events[since:], "next_event": len(self.events), "result": self.result, "error": self.error,
                "elapsed": round(end - self.created, 1), "created": self.created, "finished": self.finished}


class JobRegistry:
    def __init__(self, workers: int = 4, store: Optional[Path] = None):
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="a2f-job")
        self._jobs: dict[str, Job] = {}
        self._active: dict[str, str] = {}  # project id -> job id
        self._lock = threading.Lock()
        self._store = Path(store) if store else None
        if self._store is not None:
            self._load()

    # ---- saving: a restart must not erase what ran ---------------------------------------------------------------------
    def _file(self, job_id: str) -> Optional[Path]:
        return (self._store / f"{job_id}.json") if self._store is not None and re.fullmatch(r"[0-9a-f]{12}", job_id) else None

    def _persist(self, job: Job) -> None:
        f = self._file(job.id)
        if f is None:
            return
        try:
            f.parent.mkdir(parents=True, exist_ok=True)
            write_atomic(f, json.dumps(job.record(), default=str))
        except OSError:  # a job that cannot be saved still runs; the Activity page then forgets it on a restart, as it always did
            pass

    def _load(self) -> None:
        """The jobs saved by an earlier server. One that was queued or running then was cut off: it is shown as failed (interrupted), so nobody waits on it."""
        if self._store is None or not self._store.is_dir():
            return
        loaded: list[Job] = []
        for f in self._store.glob("*.json"):
            try:
                loaded.append(Job.from_record(json.loads(read_text_retry(f))))
            except (OSError, ValueError, KeyError, TypeError):
                continue  # one damaged file hides no other job
        for job in loaded:
            if job.state in (QUEUED, RUNNING):
                job.state, job.finished = FAILED, job.finished or time.time()
                job.error = "Interrupted: the server stopped while this job was running. Check the project, then run it again."
                self._persist(job)
            self._jobs[job.id] = job
        self._evict()

    def submit(self, kind: str, project_id: Optional[str], fn: Callable[[Callable[[str], None]], dict[str, Any]], *, org: Optional[str] = None,
               owner: Optional[str] = None) -> Job:
        with self._lock:
            if project_id and project_id in self._active and self._jobs[self._active[project_id]].state in (QUEUED, RUNNING):
                raise JobConflict(f"A '{self._jobs[self._active[project_id]].kind}' job is already running for this project.")
            self._evict()
            job = Job(id=uuid.uuid4().hex[:12], kind=kind, project_id=project_id, org=org, owner=owner)
            self._jobs[job.id] = job
            self._persist(job)
            if project_id:
                self._active[project_id] = job.id
        self._pool.submit(self._run, job, fn)
        return job

    def _evict(self) -> None:
        """Called with the lock held: drop the oldest finished jobs beyond KEEP_FINISHED (running ones are never dropped)."""
        done = sorted((j for j in self._jobs.values() if j.state in (SUCCEEDED, FAILED)), key=lambda j: j.finished or j.created)
        for old in done[:max(0, len(done) - KEEP_FINISHED)]:
            del self._jobs[old.id]
            gone = self._file(old.id)
            if gone is not None:
                gone.unlink(missing_ok=True)
            if self._active.get(old.project_id or "") == old.id:
                del self._active[old.project_id or ""]

    def _run(self, job: Job, fn) -> None:
        with self._lock:
            job.state = RUNNING

        def log(message: str) -> None:
            with self._lock:
                if len(job.events) < MAX_EVENTS:
                    job.events.append({"t": round(time.time() - job.created, 2), "message": scrub(redact(message), _ROOTS)})

        try:
            result = redact_mapping(fn(log))
            with self._lock:
                job.result, job.state = result, SUCCEEDED  # type: ignore[assignment]
        except Exception as exc:  # noqa: BLE001 - reported, redacted; the traceback never leaves the server
            error = scrub(redact(f"{type(exc).__name__}: {exc}"), _ROOTS)[:400]
            with self._lock:
                job.error, job.state = error, FAILED
        finally:
            with self._lock:
                job.finished = time.time()
                self._persist(job)

    def get(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def list(self, project_id: Optional[str] = None, org: Optional[str] = None) -> list[Job]:
        jobs = [j for j in self._jobs.values() if (project_id is None or j.project_id == project_id) and (org is None or j.org == org)]
        return sorted(jobs, key=lambda j: j.created, reverse=True)

    def wait(self, job_id: str, timeout: float = 30) -> Optional[Job]:
        """For tests and scripts: block until the job finishes."""
        end = time.time() + timeout
        job = self._jobs.get(job_id)
        while job and job.state in (QUEUED, RUNNING) and time.time() < end:
            time.sleep(0.01)
        return job
