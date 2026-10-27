"""What a prompt is made of: the pipeline's source (secrets removed), what the deterministic converter did with it, and what discovery knows around it.

The hash of the source (computed over the raw JSON, key order ignored) is what an accepted draft is pinned to: a run uses the draft only while the
source still has that hash.

Only names, types, the pipeline's own JSON and the converter's findings are sent. Never a linked-service definition, a credential, a connection string
or any data value; ``_Sanitizer`` (the one discovery uses) removes secret-looking values from the source, and ``preview`` shows exactly what would go.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.assist import templates
from adf2fabric.agent.conversion.registry import registered_rules
from adf2fabric.agent.discovery.adf_semantic.parser_pipelines import _Sanitizer
from adf2fabric.agent.validation.views import scan_source

MAX_SOURCE_CHARS = 200_000
ENVELOPE = ("id", "etag", "type")  # the ARM wrapper of an exported pipeline: its id holds the subscription, resource group and factory
_RESOURCE_ID = re.compile(r"/subscriptions/[0-9A-Fa-f-]{36}[^\"\s]*")
NEEDS_A_PERSON = ("manual_required", "blocked")


class NotAssistable(Exception):
    """This pipeline cannot be sent for a draft; the message says why."""


@dataclass
class Subject:
    name: str
    raw: dict[str, Any]
    code: str  # the source as it is sent: secrets removed, pretty-printed
    source_hash: str
    activities: dict[str, str] = field(default_factory=dict)  # name (lower) -> type
    problems: list[str] = field(default_factory=list)
    summary: str = ""
    notes: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)  # paths of values taken out before sending


def source_hash(raw: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(raw, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def _walk(acts: Any):
    stack = [acts if isinstance(acts, list) else []]
    while stack:
        for a in stack.pop():
            if isinstance(a, dict):
                yield a
                tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
                for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
                    if isinstance(tp.get(key), list):
                        stack.append(tp[key])
                for c in tp.get("cases", []) if isinstance(tp.get("cases"), list) else []:
                    if isinstance(c, dict) and isinstance(c.get("activities"), list):
                        stack.append(c["activities"])


def _raw_pipeline(store: Any, pid: str, name: str) -> dict[str, Any]:
    scan = scan_source(store.source_dir(pid))
    found = scan.names("pipeline").get(name.lower())
    if found is None or not isinstance(found.data, dict):
        raise NotAssistable(f"Pipeline '{name}' is not in the project's source.")
    return found.data


def _conversion(store: Any, pid: str):
    if not store.has_conversion(pid):
        raise NotAssistable("Run `convert` first: a draft starts from what the converter could not do.")
    return store.load_conversion(pid)


def _record(conv: Any, name: str):
    rec = next((p for p in conv.pipelines if p.source_pipeline.lower() == name.lower()), None)
    if rec is None:
        raise NotAssistable(f"Pipeline '{name}' was not converted in the last run: convert it first.")
    return rec


def _problems(rec: Any) -> list[str]:
    out = []
    for a in rec.activities:
        if a.status.value not in NEEDS_A_PERSON:
            continue
        why = [w.message for w in a.warnings if w.impact.value in ("blocker", "manual")] or [a.summary]
        out.append(f"`{a.source_name}` ({a.source_type}) is {a.status.value.replace('_', ' ')}: " + " ".join(dict.fromkeys(why))[:400])
    return out


def _summary(rec: Any) -> str:
    counts: dict[str, int] = {}
    for a in rec.activities:
        counts[a.status.value] = counts.get(a.status.value, 0) + 1
    shown = ", ".join(f"{n} {k.replace('_', ' ')}" for k, n in sorted(counts.items()))
    return f"{len(rec.activities)} activities: {shown}. Pipeline status: {rec.status.value.replace('_', ' ')}."


def _dependencies(store: Any, pid: str, raw: dict[str, Any]) -> list[str]:
    if not store.has_adf_semantic_discovery(pid):
        return []
    d = store.load_adf_semantic_discovery(pid)
    ds = {x.name.lower(): x for x in d.datasets}
    ls = {x.name.lower(): x for x in d.linked_services}
    out: list[str] = []
    seen: set[str] = set()
    for a in _walk((raw.get("properties") or {}).get("activities")):
        refs: list[tuple[str, str]] = []
        for key in ("inputs", "outputs"):
            refs += [("dataset", r.get("referenceName")) for r in a.get(key, []) if isinstance(r, dict)]
        tp = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
        for key, kind in (("dataset", "dataset"), ("pipeline", "pipeline")):
            if isinstance(tp.get(key), dict):
                refs.append((kind, tp[key].get("referenceName")))
        if isinstance(a.get("linkedServiceName"), dict):
            refs.append(("linked service", a["linkedServiceName"].get("referenceName")))
        for kind, name in refs:
            if not isinstance(name, str) or (kind, name) in seen:
                continue
            seen.add((kind, name))
            if kind == "dataset" and name.lower() in ds:
                x = ds[name.lower()]
                via = f" on `{x.linked_service_ref.name}` ({ls[x.linked_service_ref.name.lower()].type})" if x.linked_service_ref and x.linked_service_ref.name.lower() in ls else ""
                out.append(f"`{name}` (dataset, {x.type}{via})")
            elif kind == "linked service" and name.lower() in ls:
                out.append(f"`{name}` (linked service, {ls[name.lower()].type})")
            else:
                out.append(f"`{name}` ({kind})")
    return out[:40]


def load_subject(store: Any, pid: str, name: str) -> Subject:
    raw = _raw_pipeline(store, pid, name)
    conv = _conversion(store, pid)
    rec = _record(conv, name)
    problems = _problems(rec)
    if not problems:
        raise NotAssistable(f"Pipeline '{name}' was converted completely: there is nothing for a model to fix.")
    clean = _Sanitizer()
    sendable = {k: v for k, v in raw.items() if k not in ENVELOPE}
    if isinstance(sendable.get("properties"), dict):
        sendable["properties"] = {k: v for k, v in sendable["properties"].items() if k != "lastPublishTime"}
    code = _RESOURCE_ID.sub("<azure-resource-id>", json.dumps(clean.clean(sendable), indent=2, ensure_ascii=False))
    if len(code) > MAX_SOURCE_CHARS:
        raise NotAssistable(f"The pipeline is longer than {MAX_SOURCE_CHARS:,} characters; fix it by hand.")
    notes = [f.message for f in conv.preflight if any(p.lower() == name.lower() for p in f.pipelines)]
    return Subject(name=str(raw.get("name") or name), raw=raw, code=code, source_hash=source_hash(raw),
                   activities={str(a.get("name", "")).lower(): str(a.get("type", "")) for a in _walk((raw.get("properties") or {}).get("activities"))},
                   problems=problems, summary=_summary(rec), notes=notes, dependencies=_dependencies(store, pid, raw), removed=clean.redacted)


def candidates(store: Any, pid: str) -> list[dict[str, Any]]:
    """Every pipeline the last conversion left with something for a person, those with most problems first."""
    conv = _conversion(store, pid)
    out = []
    for rec in conv.pipelines:
        problems = _problems(rec)
        if problems:
            out.append({"pipeline": rec.source_pipeline, "status": rec.status.value, "problems": problems, "activities": len(rec.activities)})
    return sorted(out, key=lambda c: (-len(c["problems"]), c["pipeline"].lower()))


def _fence(*texts: str) -> str:
    longest = max([len(m) for t in texts for m in re.findall(r"`+", t or "")] or [0])
    return "`" * max(3, longest + 1)


def supported_types() -> str:
    return ", ".join(sorted({t for r in registered_rules() for t in r.adf_types}, key=str.lower))


def build(subject: Subject, prompts_dir: Path, workspace: str = "") -> str:
    """The full prompt for one pipeline, from the templates in ``prompts_dir``."""
    ctx: dict[str, Any] = {
        "object": {"name": subject.name, "activities": len(subject.activities)},
        "source": {"code": subject.code, "fence": _fence(subject.code)},
        "rules": {"summary": subject.summary, "problems": subject.problems, "supported": supported_types()},
        "discovery": {"notes": subject.notes},
        "dependencies": subject.dependencies,
        "target": {"workspace": workspace},
    }
    base = templates.read(prompts_dir / "base.md")
    if not base.strip():
        raise templates.TemplateError(f"The prompt template base.md is missing from {prompts_dir}.")
    ctx["type_instructions"] = templates.render(templates.read(prompts_dir / "pipeline.md"), ctx).strip()
    ctx["project"] = templates.render(templates.read(prompts_dir / "project.md"), ctx).strip()
    return templates.render(base, ctx)
