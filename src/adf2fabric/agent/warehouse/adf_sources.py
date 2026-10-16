"""ADF from both places: the live factory (ARM, read-only GETs) and the Git repository (stored semantic record).

Live definitions go through the same parsers as the Git files, so both become an ``AdfSemanticDiscovery`` and are
compared model to model (secret-masked), never as raw JSON. Raw ARM responses stay in memory only.
Live is what actually runs, so it is used for the analysis when available; Git differences are reported.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from adf2fabric.agent.discovery.adf.arm import API_VERSION, ArmHttpError, factory_base_url
from adf2fabric.agent.discovery.adf_semantic import parser, parser_pipelines as pp
from adf2fabric.project.discovery import AdfSemanticDiscovery, AdfSemanticStatus

COLLECTIONS = ("pipelines", "datasets", "linkedservices", "triggers")
_MAX_PAGES = 1000


def fetch_live(transport: Any, subscription: str, resource_group: str, factory: str, token: str,
               timeout: int = 30) -> dict[str, list[dict]]:
    """GET the pipeline, dataset, linked-service and trigger collections (full definitions). Read-only."""
    base = factory_base_url(subscription, resource_group, factory)
    out: dict[str, list[dict]] = {}
    for coll in COLLECTIONS:
        items: list[dict] = []
        url: Optional[str] = f"{base}/{coll}?api-version={API_VERSION}"
        for _ in range(_MAX_PAGES):
            if not url:
                break
            resp = transport.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
            if resp.status_code != 200:
                raise ArmHttpError(resp.status_code, url)
            body = resp.json() or {}
            items += [{"name": i["name"], "properties": i.get("properties") or {}} for i in body.get("value", [])]
            url = body.get("nextLink")
        out[coll] = items
    return out


def to_semantic(raw: dict[str, list[dict]], project_id: str, label: str) -> AdfSemanticDiscovery:
    pls = [pp.parse_pipeline(p, f"live:pipeline/{p['name']}") for p in raw.get("pipelines", [])]
    dss = [parser.parse_dataset(d, f"live:dataset/{d['name']}") for d in raw.get("datasets", [])]
    lss = [parser.parse_linked_service(s, f"live:linkedService/{s['name']}") for s in raw.get("linkedservices", [])]
    trs = [parser.parse_trigger(t, f"live:trigger/{t['name']}") for t in raw.get("triggers", [])]
    pp.resolve_pipelines(pls, dss, lss)
    now = datetime.now(timezone.utc)
    return AdfSemanticDiscovery(
        project_id=project_id, source_commit_sha=label, status=AdfSemanticStatus.discovered, created_at=now,
        updated_at=now, pipeline_count=len(pls), dataset_count=len(dss), linked_service_count=len(lss),
        trigger_count=len(trs), pipelines=sorted(pls, key=lambda p: p.name), datasets=sorted(dss, key=lambda d: d.name),
        linked_services=sorted(lss, key=lambda s: s.name), triggers=sorted(trs, key=lambda t: t.name))


def _comparable(model: Any) -> Any:
    return model.model_dump(mode="json", exclude={"path", "source_location"})


# Fields that say where a definition was read from or that are derived by cross-artifact resolution, not authored.
_NOT_AUTHORED = {"path", "source_location", "resolved", "integration_runtime_kind"}


def _strip_locations(node: Any) -> Any:
    """Drop location/derived fields and empty values (ARM fills in defaults such as ``parameters: {}``)."""
    if isinstance(node, dict):
        out = {k: _strip_locations(v) for k, v in node.items() if k not in _NOT_AUTHORED}
        return {k: v for k, v in out.items() if v not in (None, {}, [])}
    if isinstance(node, list):
        return [_strip_locations(v) for v in node]
    return node


def compare(live: AdfSemanticDiscovery, git: AdfSemanticDiscovery) -> list[str]:
    """Human-readable differences between the live factory and Git (names, then changed definitions)."""
    diffs: list[str] = []
    for label, a, b in (("pipeline", live.pipelines, git.pipelines), ("dataset", live.datasets, git.datasets),
                        ("linked service", live.linked_services, git.linked_services)):
        la = {x.name.lower(): x for x in a}
        gb = {x.name.lower(): x for x in b}
        for n in sorted(set(la) - set(gb)):
            diffs.append(f"{label} {la[n].name}: in live ADF, not in Git")
        for n in sorted(set(gb) - set(la)):
            diffs.append(f"{label} {gb[n].name}: in Git, not in live ADF")
        for n in sorted(set(la) & set(gb)):
            if _strip_locations(_comparable(la[n])) != _strip_locations(_comparable(gb[n])):
                diffs.append(f"{label} {la[n].name}: definition differs between live ADF and Git")
    return diffs
