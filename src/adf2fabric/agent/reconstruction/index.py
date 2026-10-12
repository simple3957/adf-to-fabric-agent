"""Read-only view of everything the analysis may use as evidence.

``ProjectEvidence`` bundles the parsed semantic record, the repository inventory (which lists the
files that failed to parse), optional SQL discovery, and READ-ONLY access to the original ADF JSON.
Documents are cached and handed out as-is; callers must treat them as immutable — every patch is
applied to a deep copy (see ``patching.apply_ops``) so the original is never altered.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.agent.reconstruction import patching
from adf2fabric.project.discovery import (
    AdfActivity,
    AdfDataset,
    AdfLinkedService,
    AdfPipeline,
    AdfSemanticDiscovery,
    RepositoryDiscovery,
    SqlDiscovery,
)


def normalize(name: str) -> str:
    """A separator- and case-insensitive key (``PL-Util_Log`` == ``pl_util_log`` == ``PLUtilLog``)."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


class ProjectEvidence:
    def __init__(
        self,
        discovery: AdfSemanticDiscovery,
        source_root: Optional[Path] = None,
        repository: Optional[RepositoryDiscovery] = None,
        sql: Optional[SqlDiscovery] = None,
    ):
        self.discovery = discovery
        self.source_root = Path(source_root) if source_root else None
        self.repository = repository
        self.sql = sql
        self.pipelines: dict[str, AdfPipeline] = {p.name.lower(): p for p in discovery.pipelines}
        self.datasets: dict[str, AdfDataset] = {d.name.lower(): d for d in discovery.datasets}
        self.linked_services: dict[str, AdfLinkedService] = {s.name.lower(): s for s in discovery.linked_services}
        self.global_parameters = {g.name.lower(): g for f in discovery.factory for g in f.global_parameters}
        self._docs: dict[str, Optional[Any]] = {}
        self._acts: dict[str, list[AdfActivity]] = {}

    # -- original JSON (read-only) -------------------------------------------------------------------
    def doc(self, relative_path: str) -> Optional[Any]:
        if relative_path in self._docs:
            return self._docs[relative_path]
        doc = None
        if self.source_root is not None:
            try:
                doc = json.loads((self.source_root / relative_path).read_text(encoding="utf-8"))
            except (OSError, ValueError):
                doc = None
        self._docs[relative_path] = doc
        return doc

    # -- activities -------------------------------------------------------------------------------------
    def acts(self, p: AdfPipeline) -> list[AdfActivity]:
        if p.name not in self._acts:
            self._acts[p.name] = list(walk(p.activities))
        return self._acts[p.name]

    def by_id(self, p: AdfPipeline) -> dict[str, AdfActivity]:
        return {a.activity_id: a for a in self.acts(p)}

    @staticmethod
    def tokens(a: AdfActivity) -> list[patching.Token]:
        return patching.ir_pointer_tokens(a.source_location.pointer) if a.source_location else []

    def act_json(self, p: AdfPipeline, a: AdfActivity) -> Optional[dict]:
        doc = self.doc(p.path)
        if doc is None:
            return None
        try:
            node = patching.get(doc, self.tokens(a))
        except patching.PatchError:
            return None
        return node if isinstance(node, dict) else None

    def siblings(self, p: AdfPipeline, a: AdfActivity) -> list[AdfActivity]:
        return [x for x in self.acts(p) if x.parent_activity_id == a.parent_activity_id and x.branch == a.branch]

    def ancestors(self, p: AdfPipeline, a: AdfActivity) -> list[AdfActivity]:
        ids, out, cur = self.by_id(p), [], a
        while cur.parent_activity_id and cur.parent_activity_id in ids:
            cur = ids[cur.parent_activity_id]
            out.append(cur)
        return out

    def upstream(self, p: AdfPipeline, a: AdfActivity) -> set[str]:
        """Lowercase names of every activity that must finish before ``a`` starts (own scope + ancestors')."""
        result: set[str] = set()
        scope_by_name = {s.name.lower(): s for s in self.siblings(p, a)}
        stack = [d.activity.lower() for d in a.depends_on]
        while stack:
            n = stack.pop()
            if n in result:
                continue
            result.add(n)
            t = scope_by_name.get(n)
            if t:
                stack.extend(d.activity.lower() for d in t.depends_on)
        ids = self.by_id(p)
        if a.parent_activity_id and a.parent_activity_id in ids:
            result |= self.upstream(p, ids[a.parent_activity_id])
        return result

    def descendants_of(self, p: AdfPipeline, a: AdfActivity) -> list[AdfActivity]:
        return list(walk(a.activities))

    def downstream_names(self, p: AdfPipeline, a: AdfActivity) -> set[str]:
        """Lowercase names of sibling activities that (transitively) depend on ``a``."""
        sibs = self.siblings(p, a)
        out: set[str] = set()
        frontier = {a.name.lower()}
        while frontier:
            nxt = {s.name.lower() for s in sibs
                   if s.name.lower() not in out and any(d.activity.lower() in frontier for d in s.depends_on)}
            out |= nxt
            frontier = nxt
        return out

    # -- artifacts -----------------------------------------------------------------------------------------
    def broken_files(self) -> list:
        """Repository candidates that could not be read as JSON."""
        if self.repository is None:
            return []
        return [c for c in self.repository.adf_candidates
                if getattr(c.parse_status, "value", c.parse_status) != "discovered"]

    def sql_procedures(self) -> set[str]:
        if self.sql is None:
            return set()
        return {f"{o.schema}.{o.name}".lower() for o in self.sql.procedures}

    def signature(self, p: AdfPipeline) -> tuple:
        return tuple(f"{len(self.nesting(p, a))}:{a.type or '?'}" for a in self.acts(p))

    def nesting(self, p: AdfPipeline, a: AdfActivity) -> list[str]:
        return a.nesting_path
