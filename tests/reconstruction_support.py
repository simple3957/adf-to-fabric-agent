"""Shared helpers for the reconstruction tests: real projects on disk, real discovery, real analysis."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from adf2fabric.agent.reconstruction.agent import ReconstructionAgent
from adf2fabric.agent.reconstruction.models import ReconstructionRequest
from adf2fabric.project.discovery import SqlDiscovery, SqlDiscoveryStatus, SqlObject
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.reconstruction import Confidence, ProjectReconstruction
from adf2fabric.project.store import ProjectStore
from tests.conversion_support import (  # noqa: F401  (re-exported)
    DS_LAKE, DS_SQL, LAKE_LS, SQL_LS, act, copy_activity, dataset, dep, expr, linked_service, pipe, ref, trigger,
)
from tests.test_adf_semantic_discovery import _git_commit

PID = "test-proj"


def make_project(tmp_path: Path, files: dict[str, Any]) -> ProjectStore:
    """Write ``files`` (dict -> JSON, str -> raw text, so broken files are possible), commit, run Discovery."""
    store = ProjectStore(tmp_path / "projects")
    src = store.source_dir(PID)
    for rel, content in files.items():
        f = src / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(content if isinstance(content, str) else json.dumps(content, indent=2), encoding="utf-8")
    sha = _git_commit(src)
    store.save(Project(id=PID, name="Test Proj", repository=RepositoryMetadata(
        provider="github", repository_url="https://example.invalid/x.git", branch="main", commit_sha=sha,
        local_source_path=str(src))))
    DiscoveryAgent(store).discover_repository(DiscoveryRequest(project_name=PID))
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name=PID))
    return store


def reconstruct(tmp_path: Path, files: dict[str, Any], *, sql_procedures: Optional[list[tuple[str, str]]] = None
                ) -> tuple[ProjectStore, ProjectReconstruction]:
    store = make_project(tmp_path, files)
    if sql_procedures is not None:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        store.save_sql_discovery(PID, SqlDiscovery(
            project_id=PID, server="s", database="d", authentication="integrated", status=SqlDiscoveryStatus.discovered,
            created_at=now, updated_at=now, procedure_count=len(sql_procedures),
            procedures=[SqlObject(schema=s, name=n) for s, n in sql_procedures]))
    r = ReconstructionAgent(store).run(ReconstructionRequest(project_name=PID))
    assert r.success, r.error
    return store, r.reconstruction


def fingerprint(root: Path) -> dict:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file() and ".git" not in p.parts}


def pipeline_of(rec: ProjectReconstruction, name: str):
    return next(p for p in rec.pipelines if p.pipeline == name)


def proposals(rec: ProjectReconstruction, rule: Optional[str] = None, pipeline: Optional[str] = None) -> list:
    return [p for a in rec.pipelines if pipeline in (None, a.pipeline) for p in a.proposals
            if rule in (None, p.rule)]


def findings(rec: ProjectReconstruction, code: Optional[str] = None) -> list:
    allf = [f for a in rec.pipelines for f in a.findings] + list(rec.source_findings)
    return [f for f in allf if code in (None, f.code)]


def blockers(rec: ProjectReconstruction) -> list:
    return [*rec.source_blockers, *[b for a in rec.pipelines for b in a.blockers
                                    if not (a.completeness.value == "unusable")]]


def completed(store: ProjectStore, rel: str) -> dict:
    return json.loads((store.reconstruction_dir(PID) / "completed" / rel).read_text(encoding="utf-8"))


def original(store: ProjectStore, rel: str) -> dict:
    return json.loads((store.source_dir(PID) / rel).read_text(encoding="utf-8"))


HIGH, MEDIUM, LOW = Confidence.high, Confidence.medium, Confidence.low

ESTATE = {
    "dataset/DS_SQL.json": DS_SQL, "dataset/DS_LAKE.json": DS_LAKE,
    "linkedService/LS_SQL.json": SQL_LS, "linkedService/LS_LAKE.json": LAKE_LS,
}


def pl(*activities, name="PL", **kw) -> dict:
    return pipe(*activities, name=name, **kw)


def with_estate(*pipelines: dict, **extra) -> dict:
    files = dict(ESTATE)
    for p in pipelines:
        files[f"pipeline/{p['name']}.json"] = p
    files.update(extra)
    return files
