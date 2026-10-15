"""Compute assessment agent: estimate Fabric compute for a project's pipelines and write the report.

Writes ``projects/<id>/assessment/compute-report.json`` (the full result) and ``compute-report.md`` (for people).
Run history is optional: pass a ``history_fetcher`` that returns ``adf_history.fetch(...)`` results.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Optional

from pydantic import BaseModel

from adf2fabric.agent.assessment.compute import ComputeAssessment, PipelineHistory, estimate, render_markdown
from adf2fabric.project.store import ProjectStore, slugify


class ComputeRequest(BaseModel):
    project_name: str
    default_runs_per_day: float = 1.0
    include_stopped_triggers: bool = True


class ComputeResult(BaseModel):
    success: bool
    assessment: Optional[ComputeAssessment] = None
    report_json: Optional[str] = None
    report_markdown: Optional[str] = None
    message: str = ""
    error: Optional[str] = None


HistoryFetcher = Callable[[list[str]], dict[str, PipelineHistory]]


def assessment_dir(store: ProjectStore, project_id: str) -> Path:
    return store.project_dir(project_id) / "assessment"


class ComputeAgent:
    def __init__(self, store: ProjectStore):
        self.store = store

    def run(self, request: ComputeRequest, history_fetcher: Optional[HistoryFetcher] = None,
            log: Callable[[str], None] = lambda _m: None) -> ComputeResult:
        pid = slugify(request.project_name)
        if not self.store.exists(pid):
            return ComputeResult(success=False, error=f"Project '{request.project_name}' does not exist.")
        if not self.store.adf_semantic_discovery_path(pid).exists():
            return ComputeResult(success=False, error="Run the 'discovery' then 'semantic' stages first.")
        d = self.store.load_adf_semantic_discovery(pid)
        history = None
        if history_fetcher:
            log("reading ADF run history")
            try:
                history = history_fetcher([p.name for p in d.pipelines])
            except Exception as exc:  # noqa: BLE001 - reported to the user; the estimate needs the history they asked for
                return ComputeResult(success=False, error=f"Could not read ADF run history: {exc}")
            log(f"run history found for {len(history)} of {len(d.pipelines)} pipeline(s)")
        a = estimate(d, history, default_runs_per_day=request.default_runs_per_day,
                     include_stopped_triggers=request.include_stopped_triggers)
        out = assessment_dir(self.store, pid)
        out.mkdir(parents=True, exist_ok=True)
        jpath, mpath = out / "compute-report.json", out / "compute-report.md"
        jpath.write_text(json.dumps(json.loads(a.model_dump_json()), indent=2) + "\n", encoding="utf-8")
        mpath.write_text(render_markdown(a), encoding="utf-8")
        return ComputeResult(success=True, assessment=a, report_json=str(jpath), report_markdown=str(mpath),
                             message=f"{len(a.pipelines)} pipeline(s): {a.cu_hours_per_day:g} CU-hours/day, "
                                     f"recommended {a.recommended_sku}.")
