"""Handover readiness: the most blocking reason wins, and only Fabric-breaking problems block."""

from __future__ import annotations

from datetime import datetime, timezone

from adf2fabric.agent.handover import assess
from adf2fabric.project.conversion import PipelineConversion, PreflightFinding, Prerequisite, ProjectConversion


def pipe(name, status="converted", ready=True, prereqs=(), **counts):
    return PipelineConversion(source_pipeline=name, source_path=f"pipeline/{name}.json", fabric_pipeline_name=name,
                              status=status, status_counts=counts, prerequisites=list(prereqs), runtime_ready=ready)


def conv(pipes, preflight=()):
    now = datetime.now(timezone.utc)
    return ProjectConversion(project_id="p", source_commit_sha="c" * 40, status="converted", created_at=now, updated_at=now,
                             pipelines=pipes, preflight=list(preflight))


def levels(c, v=None):
    return {r.pipeline: r.level for r in assess(c, v or {})}


def test_each_level_from_its_cause():
    c = conv([pipe("B", "blocked", blocked=1), pipe("M", "manual_required", manual_required=2),
              pipe("S"), pipe("W"), pipe("U", prereqs=[Prerequisite(kind="connection", name="LS_X")]),
              pipe("K", prereqs=[Prerequisite(kind="workspace_item", name="pipeline:Child")]),
              pipe("P"), pipe("D"), pipe("F"), pipe("N")],
             [PreflightFinding(code="reserved_sql_alias", severity="error", subject="S", pipelines=["S"], message="m", fix="f"),
              PreflightFinding(code="linked_service_parameter_without_value", severity="warning", subject="LS", pipelines=["W"],
                               message="m", fix="f")])
    got = levels(c, {"P": {"verdict": "matches", "baseline": "adf", "matched": 3}, "D": {"verdict": "differs"},
                     "F": {"verdict": "failed", "detail": "paused"}})
    assert got == {"B": "blocked", "M": "needs_work", "S": "source_fix", "W": "not_run", "U": "needs_bindings",
                   "K": "not_run", "P": "proven", "D": "differs", "F": "run_failed", "N": "not_run"}


def test_the_most_blocking_level_is_listed_first():
    order = [r.pipeline for r in assess(conv([pipe("Z"), pipe("A", "blocked", blocked=1)]), {"Z": {"verdict": "matches"}})]
    assert order == ["A", "Z"]
