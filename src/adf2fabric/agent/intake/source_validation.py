"""Source-mode-independent inspection of a normalized ADF source.

Every Intake mode (GitHub, Azure) ends with ADF artifact JSON laid out in the ADF
Git convention (``pipeline/``, ``dataset/``, ``linkedService/``, ``trigger/``,
``factory/`` ...). This module reads that layout and answers, deterministically:

* which artifacts exist (and their content hashes),
* which artifacts a pipeline selection transitively needs,
* whether the source has duplicates, invalid JSON, dangling references, or looks
  like an incomplete export.

It performs NO semantic analysis of activities — that remains the job of the
Discovery parser. It only follows explicit ``*Reference`` objects.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.discovery.repository.artifacts import (
    _ADF_DIR_KIND,
    _kind_from_type,
)
from adf2fabric.agent.intake.arm_template import EXTRACT_DIR, ArmExtraction, extract as extract_arm_template
from adf2fabric.project.discovery import AdfArtifactKind
from adf2fabric.project.source import (
    FindingSeverity,
    SourceArtifact,
    SourceFinding,
)

# ``type`` of an ADF reference object -> the artifact kind it points at.
REFERENCE_KINDS: dict[str, AdfArtifactKind] = {
    "PipelineReference": AdfArtifactKind.pipeline,
    "DatasetReference": AdfArtifactKind.dataset,
    "LinkedServiceReference": AdfArtifactKind.linked_service,
    "DataFlowReference": AdfArtifactKind.dataflow,
    "IntegrationRuntimeReference": AdfArtifactKind.integration_runtime,
    "CredentialReference": AdfArtifactKind.credential,
}

# Kinds Intake normalizes (everything the Discovery parser or its references need).
LAYOUT_KINDS = frozenset(_ADF_DIR_KIND.values())

_MAX_DEPTH = 40
_ARM_TEMPLATE_NAMES = ("armtemplateforfactory.json",)

Key = tuple[str, str]  # (kind value, lowercased name)


def key_of(kind: AdfArtifactKind | str, name: str) -> Key:
    value = kind.value if isinstance(kind, AdfArtifactKind) else str(kind)
    return (value, name.lower())  # ADF resource names are case-insensitive


@dataclass
class LoadedArtifact:
    kind: AdfArtifactKind
    name: str
    path: str  # relative to the scanned root, POSIX
    sha256: str
    data: Optional[dict]
    error: Optional[str] = None
    etag: Optional[str] = None


@dataclass
class LoadedSource:
    artifacts: list[LoadedArtifact] = field(default_factory=list)
    arm_templates: list[str] = field(default_factory=list)

    def by_key(self) -> dict[Key, list[LoadedArtifact]]:
        index: dict[Key, list[LoadedArtifact]] = {}
        for a in self.artifacts:
            index.setdefault(key_of(a.kind, a.name), []).append(a)
        return index


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def load_source(root: Path) -> LoadedSource:
    """Read every ADF-layout artifact under ``root`` (deterministic order)."""
    loaded = LoadedSource()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in (".git", EXTRACT_DIR))  # the extract is derived, never source
        here = Path(dirpath)
        kind = _ADF_DIR_KIND.get(here.name.lower())
        for fname in sorted(filenames):
            full = here / fname
            rel = full.relative_to(root).as_posix()
            if fname.lower() in _ARM_TEMPLATE_NAMES:
                loaded.arm_templates.append(rel)
            if kind is None or not fname.lower().endswith(".json"):
                continue
            loaded.artifacts.append(_load_one(full, rel, kind))
    loaded.artifacts.sort(key=lambda a: a.path)
    loaded.arm_templates.sort()
    return loaded


def with_arm_extraction(
    root: Path, loaded: LoadedSource
) -> tuple[Path, LoadedSource, Optional[ArmExtraction]]:
    """If ``loaded`` holds only an ``adf_publish`` ARM template, write its Git layout and load that instead.

    Returns ``(root, loaded, None)`` unchanged when there is nothing to extract.
    """
    if loaded.artifacts or not loaded.arm_templates:
        return root, loaded, None
    extraction = extract_arm_template(root, loaded.arm_templates)
    if extraction is None:
        return root, loaded, None
    return extraction.root, load_source(extraction.root), extraction


def extraction_findings(extraction: Optional[ArmExtraction]) -> list[SourceFinding]:
    """Say, in the manifest, that the artifacts came from an ARM template and what could not be carried over."""
    if extraction is None:
        return []
    parts = [f"{n} {kind}" for kind, n in extraction.counts.items()]
    message = f"Extracted {extraction.total} artifact(s) from the ARM template {extraction.template} ({', '.join(parts)})."
    notes = []
    if extraction.unresolved_expressions:
        notes.append(f"{extraction.unresolved_expressions} ARM expression(s) stay unresolved, for example secure "
                     "parameters, so connection details come from the linked service cards, not the template")
    if extraction.skipped:
        shown = ", ".join(extraction.skipped[:5]) + (" ..." if len(extraction.skipped) > 5 else "")
        notes.append(f"skipped: {shown}")
    if extraction.other_templates:
        notes.append(f"other templates were not used: {', '.join(extraction.other_templates)}")
    return [SourceFinding(
        severity=FindingSeverity.warning,
        code="arm_template_extracted",
        message=message + (" " + "; ".join(notes) + "." if notes else ""),
        action="Check the counts against the factory. Pick a collaboration-branch subdirectory if you need the original JSON.",
    )]


def _load_one(full: Path, rel: str, dir_kind: AdfArtifactKind) -> LoadedArtifact:
    stem = full.stem
    try:
        raw = full.read_bytes()
    except OSError as exc:
        return LoadedArtifact(dir_kind, stem, rel, "", None, f"unreadable ({type(exc).__name__})")
    digest = sha256_bytes(raw)
    try:
        data = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return LoadedArtifact(dir_kind, stem, rel, digest, None, f"invalid JSON ({type(exc).__name__})")
    if not isinstance(data, dict):
        return LoadedArtifact(dir_kind, stem, rel, digest, None, "top-level JSON is not an object")

    # A top-level ARM ``type`` is stronger evidence than the directory name.
    raw_type = data.get("type")
    kind = (_kind_from_type(raw_type) if isinstance(raw_type, str) else None) or dir_kind
    name = data.get("name") if isinstance(data.get("name"), str) and data.get("name") else stem
    etag = data.get("etag") if isinstance(data.get("etag"), str) else None
    return LoadedArtifact(kind, name, rel, digest, data, None, etag)


def find_references(node: Any, _depth: int = 0) -> dict[Key, str]:
    """Every explicit ``*Reference`` object inside ``node``.

    Maps the case-insensitive key to the name as written in the source (for messages).
    """
    found: dict[Key, str] = {}
    if _depth > _MAX_DEPTH:
        return found
    if isinstance(node, dict):
        ref_type = node.get("type")
        name = node.get("referenceName")
        if isinstance(ref_type, str) and isinstance(name, str) and name:
            kind = REFERENCE_KINDS.get(ref_type)
            if kind is not None:
                found.setdefault(key_of(kind, name), name)
        for value in node.values():
            found.update({k: v for k, v in find_references(value, _depth + 1).items() if k not in found})
    elif isinstance(node, list):
        for item in node:
            found.update({k: v for k, v in find_references(item, _depth + 1).items() if k not in found})
    return found


@dataclass
class Closure:
    keys: set[Key]
    paths: list[str]
    missing_pipelines: list[str]


def resolve_selection(source: LoadedSource, pipelines: list[str]) -> Closure:
    """The selected pipelines + everything they transitively reference.

    Triggers that start a selected pipeline are included (they are part of how
    the pipeline runs) but are never followed as dependencies themselves.
    """
    index = source.by_key()
    wanted: list[Key] = []
    missing: list[str] = []
    for name in pipelines:
        k = key_of(AdfArtifactKind.pipeline, name)
        if k in index:
            wanted.append(k)
        else:
            missing.append(name)

    seen: set[Key] = set()
    queue = list(wanted)
    while queue:
        k = queue.pop()
        if k in seen:
            continue
        seen.add(k)
        for art in index.get(k, []):
            if art.data is None:
                continue
            for ref in find_references(art.data):
                if ref not in seen:
                    queue.append(ref)

    for art in source.artifacts:
        if art.kind != AdfArtifactKind.trigger or art.data is None:
            continue
        if any(
            ref[0] == AdfArtifactKind.pipeline.value and ref in seen
            for ref in find_references(art.data)
        ):
            seen.add(key_of(art.kind, art.name))

    paths = sorted(a.path for a in source.artifacts if key_of(a.kind, a.name) in seen)
    return Closure(keys=seen, paths=paths, missing_pipelines=missing)


def validate_source(
    source: LoadedSource,
    *,
    scope: Optional[set[Key]] = None,
    expected_kinds: Optional[set[AdfArtifactKind]] = None,
) -> list[SourceFinding]:
    """Integrity findings for a loaded source, in deterministic order.

    ``scope`` limits reference checking to a selection's closure. When
    ``expected_kinds`` is given, a dangling reference to a kind outside it is
    reported with an action naming the kind to include.
    """
    findings: list[SourceFinding] = []

    for art in source.artifacts:
        if art.error:
            findings.append(
                SourceFinding(
                    severity=FindingSeverity.error,
                    code="invalid_json",
                    message=f"{art.path}: {art.error}.",
                    artifact=art.path,
                    action="Fix or re-export this file; Discovery cannot parse it.",
                )
            )

    index = source.by_key()
    for k, group in sorted(index.items()):
        if len(group) > 1:
            paths = ", ".join(sorted(a.path for a in group))
            findings.append(
                SourceFinding(
                    severity=FindingSeverity.error,
                    code="duplicate_artifact",
                    message=f"{k[0]} '{group[0].name}' is defined {len(group)} times: {paths}.",
                    artifact=sorted(a.path for a in group)[0],
                    action="Remove or rename the duplicates; ADF names are case-insensitive.",
                )
            )

    dangling: dict[tuple[str, Key], str] = {}
    for art in source.artifacts:
        if art.data is None:
            continue
        if scope is not None and key_of(art.kind, art.name) not in scope:
            continue
        refs = find_references(art.data)
        for ref in sorted(refs):
            if ref not in index:
                dangling[(art.path, ref)] = refs[ref]
    for (path, ref), written in dangling.items():
        hint = ""
        if expected_kinds is not None and not any(k.value == ref[0] for k in expected_kinds):
            hint = f" Include the '{ref[0]}' artifact kind in the retrieval."
        findings.append(
            SourceFinding(
                severity=FindingSeverity.error,
                code="missing_reference",
                message=f"{path} references {ref[0]} '{written}' which is not in the source.",
                artifact=path,
                action=f"Add the missing {ref[0]} or re-export the full factory.{hint}",
            )
        )

    if not source.artifacts:
        if source.arm_templates:
            findings.append(
                SourceFinding(
                    severity=FindingSeverity.warning,
                    code="arm_template_only",
                    message=(
                        "Only an ARM template export was found "
                        f"({', '.join(source.arm_templates)}); no pipeline/dataset/... "
                        "folders, and the template holds no Data Factory resources that could be extracted."
                    ),
                    action="Check the template is the factory's ARMTemplateForFactory.json, or point Intake at the "
                    "collaboration branch (or a subdirectory) that holds the ADF JSON folders.",
                )
            )
        else:
            findings.append(
                SourceFinding(
                    severity=FindingSeverity.error,
                    code="no_adf_artifacts",
                    message="No ADF artifact folders (pipeline/, dataset/, ...) were found.",
                    action="Check the branch and the repository subdirectory.",
                )
            )
    elif not any(a.kind == AdfArtifactKind.pipeline for a in source.artifacts):
        findings.append(
            SourceFinding(
                severity=FindingSeverity.error,
                code="no_pipelines",
                message="The source contains ADF artifacts but no pipelines.",
                action="Check the branch, subdirectory, and pipeline selection.",
            )
        )
    return findings


def to_source_artifacts(source: LoadedSource, paths: Optional[set[str]] = None) -> list[SourceArtifact]:
    return [
        SourceArtifact(kind=a.kind.value, name=a.name, path=a.path, sha256=a.sha256, etag=a.etag)
        for a in source.artifacts
        if paths is None or a.path in paths
    ]


def content_digest(artifacts: list[SourceArtifact]) -> str:
    """Order-independent digest of the normalized artifacts (path + file hash)."""
    h = hashlib.sha256()
    for a in sorted(artifacts, key=lambda a: a.path):
        h.update(f"{a.path}\0{a.sha256}\n".encode("utf-8"))
    return h.hexdigest()
