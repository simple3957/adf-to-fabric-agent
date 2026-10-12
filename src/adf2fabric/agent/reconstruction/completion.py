"""The proposed ADF completion: original JSON + safe proposals, built in memory.

Only proposals marked ``safe_to_generate`` participate. Patches are applied to deep copies of the
ORIGINAL documents, in proposal-id order, so the result is deterministic and the originals on disk are
never written. The completed documents are re-parsed with the existing semantic parser (through
``AdfSemanticDiscoverer.discover_in_memory``), so the completed semantic representation is produced by
the same code that produced the original one.
"""

from __future__ import annotations

from typing import Any

from adf2fabric.agent.reconstruction import patching
from adf2fabric.agent.reconstruction.index import ProjectEvidence
from adf2fabric.project.reconstruction import Proposal


def completed_documents(ev: ProjectEvidence, proposals: list[Proposal]) -> dict[str, Any]:
    """{repository-relative path: completed document} for every file with at least one applied proposal."""
    docs: dict[str, Any] = {}
    for prop in sorted(proposals, key=lambda p: p.id):
        if not prop.safe_to_generate or not prop.patches:
            continue
        trial = {pt.file: docs.get(pt.file, ev.doc(pt.file)) for pt in prop.patches}
        try:
            for pt in prop.patches:
                trial[pt.file] = patching.apply_patch(trial[pt.file], pt)
        except (patching.PatchError, TypeError):
            continue  # analysis already demoted these; defensive only
        docs.update(trial)
    return docs


def applied_proposal_ids(proposals: list[Proposal]) -> list[str]:
    return [p.id for p in sorted(proposals, key=lambda p: p.id) if p.safe_to_generate and p.patches]
