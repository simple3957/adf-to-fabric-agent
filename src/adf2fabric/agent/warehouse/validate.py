"""Compare the source catalog (and the manifest's expected target types) with the target inventory."""

from __future__ import annotations

import re

from adf2fabric.agent.warehouse.models import Check, ManifestEntry, ObjectKind, Provenance, SourceCatalog, TargetObject
from adf2fabric.agent.warehouse.typemap import map_type


def _base(t: str) -> str:
    return re.sub(r"\(.*\)$", "", t.strip().lower())


def validate(catalog: SourceCatalog, manifest: list[ManifestEntry], target: dict[str, TargetObject],
             default_varchar: int = 8000) -> list[Check]:
    checks: list[Check] = []
    for e in manifest:
        if e.kind == ObjectKind.other or e.action == "skip":
            continue
        o = catalog.get(e.source_key)
        t = target.get(f"{e.target_schema}.{e.target_name}".lower())
        if e.kind == ObjectKind.external_table:
            checks.append(Check(object_key=e.source_key, check="presence", status="not_applicable",
                                detail="External table: manual (no PolyBase in Fabric)." + (" A target object with this name exists." if t else "")))
            e.validation = "not_run"
            continue
        if t is None:
            checks.append(Check(object_key=e.source_key, check="presence", status="missing", detail="Not in the target."))
            e.validation = "missing_in_target"
            continue
        checks.append(Check(object_key=e.source_key, check="presence", status="match", detail=f"Target {t.kind.value}."))
        results = []
        if e.kind == ObjectKind.table and o is not None:
            src_cols = {c.name.lower(): c for c in o.columns}
            tgt_cols = {c.name.lower(): c for c in t.columns}
            missing, extra = sorted(set(src_cols) - set(tgt_cols)), sorted(set(tgt_cols) - set(src_cols))
            st = "match" if not missing and not extra else "differs"
            checks.append(Check(object_key=e.source_key, check="columns", status=st,
                                detail=f"{len(src_cols)} source / {len(tgt_cols)} target"
                                       + (f"; missing in target {missing}" if missing else "") + (f"; extra in target {extra}" if extra else "")))
            results.append(st)
            diffs, reviews = [], []
            for name, c in src_cols.items():
                tc = tgt_cols.get(name)
                if not tc:
                    continue
                expected = map_type(c.source_type, c.identity, default_varchar).target or "?"
                if expected.lower() == tc.source_type.lower():
                    continue
                if c.provenance != Provenance.verified and _base(expected) == _base(tc.source_type):
                    reviews.append(f"{c.name}: inferred {c.source_type}, target {tc.source_type}")
                else:
                    diffs.append(f"{c.name}: expected {expected}, target {tc.source_type}")
            st = "differs" if diffs else ("review" if reviews else "match")
            checks.append(Check(object_key=e.source_key, check="types", status=st,
                                detail="; ".join(diffs + reviews) or "All mapped types match."))
            results.append(st)
            if o.row_count is None:
                checks.append(Check(object_key=e.source_key, check="row_count", status="review",
                                    detail=f"Source count unavailable; target {t.row_count}."))
                results.append("review")
            else:
                st = "match" if o.row_count == t.row_count else "differs"
                checks.append(Check(object_key=e.source_key, check="row_count", status=st,
                                    detail=f"source {o.row_count} ({o.row_count_source or 'live'}) / target {t.row_count}"))
                results.append(st)
        e.validation = "differs" if "differs" in results else ("review" if "review" in results else "match")
    return checks
