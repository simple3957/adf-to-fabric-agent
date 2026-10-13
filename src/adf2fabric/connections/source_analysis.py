"""Describe a source folder (from Git or a ZIP) before anything is migrated: where the ADF/Synapse layout is, what it holds,
what is missing, and whether references resolve. Read-only; nothing in the source is executed, and only JSON is parsed.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from adf2fabric.agent.discovery.repository.artifacts import _ADF_DIR_KIND
from adf2fabric.agent.intake import source_validation as sv
from adf2fabric.project.source import FindingSeverity

# Folder names (lower case) that mark an ADF / Synapse git layout, and the Synapse-only ones.
LAYOUT_DIRS = frozenset(_ADF_DIR_KIND)
SYNAPSE_ONLY_DIRS = {"notebook": "notebooks", "sqlscript": "SQL scripts", "sparkjobdefinition": "Spark job definitions",
                     "bigdatapool": "Spark pools", "sqlpool": "dedicated SQL pools", "kqlscript": "KQL scripts"}
_MAX_DEPTH = 4


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FindingView(_M):
    severity: str
    code: str
    message: str
    artifact: Optional[str] = None
    action: str = ""


class SourceSummary(_M):
    root: str  # the detected source root, relative to what was scanned ('' = the scanned folder itself)
    flavor: str  # adf | synapse | unknown
    layout_ok: bool
    counts: dict[str, int] = Field(default_factory=dict)  # artifact kind -> number of definitions
    synapse_only: dict[str, int] = Field(default_factory=dict)  # present in a Synapse export; NOT converted by this tool
    pipelines: list[str] = Field(default_factory=list)
    arm_templates: list[str] = Field(default_factory=list)
    findings: list[FindingView] = Field(default_factory=list)
    unresolved_references: int = 0
    complete: bool = False  # no error-level finding

    @property
    def total(self) -> int:
        return sum(self.counts.values())


def _children_dirs(p: Path) -> list[str]:
    try:
        return [c.name.lower() for c in p.iterdir() if c.is_dir() and c.name != ".git"]
    except OSError:
        return []


def _has_json(d: Path) -> bool:
    try:
        return any(f.is_file() and f.suffix.lower() == ".json" for f in d.iterdir())
    except OSError:
        return False


def detect_root(base: Path) -> tuple[Path, list[str]]:
    """The shallowest folder at or under ``base`` holding ADF/Synapse artifact folders, plus notes (for example when several
    candidates exist). Falls back to ``base`` itself when nothing matches."""
    notes: list[str] = []
    candidates: list[tuple[int, int, Path]] = []
    base_depth = len(base.parts)
    for dirpath, dirnames, _files in os.walk(base):
        dirnames[:] = sorted(d for d in dirnames if d != ".git")
        here = Path(dirpath)
        depth = len(here.parts) - base_depth
        if depth > _MAX_DEPTH:
            dirnames[:] = []
            continue
        try:
            hits = [c for c in here.iterdir()
                    if c.is_dir() and c.name != ".git" and c.name.lower() in LAYOUT_DIRS and _has_json(c)]
        except OSError:
            hits = []
        if hits:
            candidates.append((depth, -len(hits), here))
    if not candidates:
        return base, notes
    candidates.sort(key=lambda c: (c[0], c[1], str(c[2])))
    if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
        notes.append("Several folders hold artifact definitions at the same depth; the one with the most artifact "
                     f"folders was chosen ('{candidates[0][2].relative_to(base).as_posix() or '.'}'). Pick a subdirectory "
                     "to choose another.")
    return candidates[0][2], notes


def analyze_source(base: Path, subdirectory: Optional[str] = None) -> SourceSummary:
    """Analyse ``base`` (or ``base/subdirectory``). Safe on untrusted input: reads JSON only, executes nothing."""
    notes: list[str] = []
    if subdirectory:
        root = (base / subdirectory).resolve()
        if base.resolve() not in root.parents and root != base.resolve():
            raise ValueError("The subdirectory escapes the source.")
        if not root.is_dir():
            raise ValueError(f"The subdirectory '{subdirectory}' does not exist in the source.")
    else:
        root, notes = detect_root(base)
    rel = root.relative_to(base).as_posix() if root != base else ""
    loaded = sv.load_source(root)
    findings = [FindingView(severity=f.severity.value, code=f.code, message=f.message, artifact=f.artifact, action=f.action)
                for f in sv.validate_source(loaded)]
    findings += [FindingView(severity="warning", code="root_choice", message=n) for n in notes]
    counts: dict[str, int] = {}
    for a in loaded.artifacts:
        counts[a.kind.value] = counts.get(a.kind.value, 0) + 1
    dirs = set(_children_dirs(root))
    extras = {label: sum(1 for f in (root / d).rglob("*.json")) if (root / d).is_dir() else 0
              for d, label in SYNAPSE_ONLY_DIRS.items() if d in dirs}
    extras = {k: v for k, v in extras.items() if v}
    pipelines = sorted(a.name for a in loaded.artifacts if a.kind.value == "pipeline")
    layout_ok = bool(counts)
    if not layout_ok:
        hint = (" An ARM template was found; export the factory as Git-layout JSON files instead."
                if loaded.arm_templates else "")
        findings.append(FindingView(
            severity="error", code="no_artifacts",
            message="No pipeline, dataset, linked service or other ADF/Synapse definition folders were found." + hint,
            action="Upload or point at the repository folder that contains the 'pipeline', 'dataset' and 'linkedService' folders."))
    elif not pipelines:
        findings.append(FindingView(severity="warning", code="no_pipelines",
                                    message="The source has artifacts but no pipelines to migrate.",
                                    action="Check that the 'pipeline' folder was included in the export."))
    flavor = "synapse" if extras or any(d in dirs for d in ("sqlpool", "bigdatapool")) else ("adf" if layout_ok else "unknown")
    unresolved = sum(1 for f in findings if f.code == "missing_reference")
    return SourceSummary(root=rel, flavor=flavor, layout_ok=layout_ok, counts=dict(sorted(counts.items())), synapse_only=extras,
                         pipelines=pipelines, arm_templates=loaded.arm_templates, findings=findings,
                         unresolved_references=unresolved,
                         complete=not any(f.severity == FindingSeverity.error.value for f in findings))
