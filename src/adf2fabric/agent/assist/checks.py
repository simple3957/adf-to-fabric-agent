"""Checks on a draft before a person reviews it. They catch the obvious, not every mistake: they test structure and what the deterministic
converter makes of the draft, never whether the pipeline still does what the business needs, so the review is the real safeguard.

Each check gives ``{"level": "error" | "warning" | "ok", "message": ...}``. A draft with an error cannot be accepted until it is edited.
The draft is ADF source, so the project's own converter is the judge: it runs on the draft and the result is compared with what it did before.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from adf2fabric.agent.assist.context import NEEDS_A_PERSON, Subject, _walk
from adf2fabric.agent.conversion.agent import build_project_context
from adf2fabric.agent.conversion.context import Resolutions
from adf2fabric.agent.conversion.engine import ConversionEngine
from adf2fabric.agent.discovery.adf_semantic import parser_pipelines as pp
from adf2fabric.agent.discovery.adf_semantic.parser_pipelines import _Sanitizer


def _check(level: str, message: str) -> dict[str, str]:
    return {"level": level, "message": message}


def _edges(a: dict) -> set[tuple[str, tuple[str, ...]]]:
    return {(str(d.get("activity", "")).lower(), tuple(sorted(str(c) for c in d.get("dependencyConditions", []) or [])))
            for d in a.get("dependsOn", []) or [] if isinstance(d, dict)}


def _references(raw: dict) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for a in _walk((raw.get("properties") or {}).get("activities")):
        tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
        for key in ("inputs", "outputs"):
            for r in a.get(key, []) or []:
                if isinstance(r, dict) and isinstance(r.get("referenceName"), str):
                    out.add(("dataset", r["referenceName"].lower()))
        for key, kind in (("dataset", "dataset"), ("pipeline", "pipeline"), ("notebook", "notebook"), ("dataflow", "dataflow"), ("dataFlow", "dataflow")):
            if isinstance(tp.get(key), dict) and isinstance(tp[key].get("referenceName"), str):
                out.add((kind, tp[key]["referenceName"].lower()))
        if isinstance(a.get("linkedServiceName"), dict) and isinstance(a["linkedServiceName"].get("referenceName"), str):
            out.add(("linked service", a["linkedServiceName"]["referenceName"].lower()))
    return out


def _unresolved(rec: Any) -> list[str]:
    return [a.source_name for a in rec.activities if a.status.value in NEEDS_A_PERSON]


def run(store: Any, pid: str, subject: Subject, code: str, unconverted: Optional[list[Any]] = None) -> list[dict[str, str]]:
    try:
        draft = json.loads(code)
    except ValueError as exc:
        return [_check("error", f"It is not valid JSON ({exc.msg} at line {exc.lineno}).")]
    acts = (draft.get("properties") or {}).get("activities") if isinstance(draft, dict) else None
    if not isinstance(acts, list):
        return [_check("error", "It has no properties.activities list, so it is not a pipeline resource.")]
    out: list[dict[str, str]] = []

    if draft.get("name") != subject.name:
        out.append(_check("error", f"The pipeline name is '{draft.get('name')}', not '{subject.name}': a draft cannot rename the pipeline."))

    cleaner = _Sanitizer()
    cleaner.clean(draft)
    if cleaner.redacted:
        out.append(_check("error", "It holds a value that looks like a secret (" + ", ".join(cleaner.redacted[:3]) + "). Never put a credential in a pipeline: "
                                   "use a Fabric connection or a Key Vault reference."))

    new = {str(a.get("name", "")).lower(): a for a in _walk(acts)}
    old = {str(a.get("name", "")).lower(): a for a in _walk((subject.raw.get("properties") or {}).get("activities"))}
    gone = sorted(old[k].get("name", k) for k in old if k not in new)
    if gone:
        out.append(_check("error", f"These activities were removed: {', '.join(gone)}. A draft keeps every activity; list what it cannot fix instead."))
    retyped = sorted(f"{old[k].get('name')} ({old[k].get('type')} -> {new[k].get('type')})" for k in old if k in new and old[k].get("type") != new[k].get("type"))
    if retyped:
        out.append(_check("error", "These activities changed type: " + "; ".join(retyped) + ". A draft cannot replace one activity type with another."))
    rewired = sorted(old[k].get("name", k) for k in old if k in new and _edges(old[k]) != _edges(new[k]))
    if rewired:
        out.append(_check("error", f"The dependencies (dependsOn) changed for: {', '.join(rewired)}."))
    added = sorted(new[k].get("name", k) for k in new if k not in old)
    if added:
        out.append(_check("warning", f"It adds activities that are not in the source: {', '.join(added)}. Check each one is needed."))

    pa, pb = (subject.raw.get("properties") or {}), (draft.get("properties") or {})
    for kind in ("parameters", "variables"):
        before, after = set((pa.get(kind) or {})), set((pb.get(kind) or {}))
        if before - after:
            out.append(_check("error", f"These {kind} were removed or renamed: {', '.join(sorted(before - after))}."))
        if after - before:
            out.append(_check("warning", f"It adds {kind}: {', '.join(sorted(after - before))}. Whatever starts the pipeline must now supply "
                                         f"{'them' if len(after - before) > 1 else 'it'} (or rely on the default)."))

    fresh = sorted(f"{k} '{n}'" for k, n in _references(draft) - _references(subject.raw))
    if fresh:
        out.append(_check("error", "It refers to things that are not in the source: " + ", ".join(fresh[:5]) + ". The converter resolves references by name."))

    out += _converter(store, pid, subject, draft, old)
    if unconverted:
        out.append(_check("warning", f"The model left {len(unconverted)} item{'s' if len(unconverted) != 1 else ''} for a person; see below."))
    if not any(c["level"] == "error" for c in out):
        out.insert(0, _check("ok", "Passed the automatic checks. They are not a substitute for reading it."))
    return out


def _converter(store: Any, pid: str, subject: Subject, draft: dict, old: dict) -> list[dict[str, str]]:
    """Run the deterministic converter on the draft and compare with what it did with the original."""
    if not store.has_adf_semantic_discovery(pid):
        return [_check("warning", "The converter was not run on the draft: there is no semantic discovery record.")]
    d = store.load_adf_semantic_discovery(pid)
    try:
        parsed = pp.parse_pipeline(draft, f"pipeline/{subject.name}.json")
        others = [p for p in d.pipelines if p.name.lower() != subject.name.lower()]
        pp.resolve_pipelines([parsed], d.datasets, d.linked_services)
        trial = d.model_copy(update={"pipelines": [*others, parsed]})
        ctx = build_project_context(trial, Resolutions())
        rec, _ = ConversionEngine(ctx).convert_pipeline(parsed)
        before = _unresolved(next(p for p in store.load_conversion(pid).pipelines if p.source_pipeline.lower() == subject.name.lower()))
    except Exception as exc:  # noqa: BLE001 - a draft that breaks the converter is reported, not raised
        return [_check("error", f"The converter could not read the draft ({type(exc).__name__}).")]
    after = _unresolved(rec)
    out: list[dict[str, str]] = []
    if not rec.structural_valid and not after:  # while something still needs a person, an invalid result is expected and is reported below
        out.append(_check("error", "The converter calls every activity converted, but the converted pipeline is not structurally valid: it rejects the draft."))
    worse = sorted(set(x.lower() for x in after) - set(x.lower() for x in before))
    if worse:
        out.append(_check("error", f"The draft makes it worse: {', '.join(worse)} now need a person and did not before."))
    if len(after) < len(before):
        out.append(_check("ok", f"The converter now needs a person for {len(after)} of {len(rec.activities)} activities (before: {len(before)})."))
    elif not worse:
        out.append(_check("warning", f"It does not help: the converter still needs a person for {len(after)} activities (before: {len(before)})."))
    if after:
        out.append(_check("warning", "Still not converted: " + ", ".join(after[:6]) + ("..." if len(after) > 6 else "") + "."))
    return out


def blocking(found: list[dict[str, str]]) -> bool:
    return any(c["level"] == "error" for c in found)
