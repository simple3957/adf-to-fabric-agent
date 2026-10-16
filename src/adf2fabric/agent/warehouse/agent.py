"""Warehouse migration runs.

``analyze``  source discovery (any registered adapter) + ADF from live and Git + usage + scope. Read-only.
``run``      analyze + plan (dry-run DDL) + read-only check of an existing target + report  (``warehouse-assess``).
``migrate``  analyze + plan + find/create the target Warehouse + deploy + load + validate + report
             (``warehouse-migrate``). Dry run unless ``apply``.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from adf2fabric.agent.warehouse import adapter, dependencies, planner, report, validate
from adf2fabric.agent.warehouse import adapters  # noqa: F401  (registers built-in adapters)
from adf2fabric.agent.warehouse.config import MigrationConfig
from adf2fabric.agent.warehouse.models import (
    AdfReference, ManifestEntry, MigrationReport, ObjectKind, Resolution, SourceCatalog, TargetObject,
)
from adf2fabric.project.discovery import AdfSemanticDiscovery
from adf2fabric.project.store import ProjectStore

Log = Callable[[str], None]
AdfFetcher = Callable[[MigrationConfig], dict[str, list[dict]]]


@dataclass
class Analysis:
    catalog: SourceCatalog
    refs: list[AdfReference] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    deps: dict[str, list[str]] = field(default_factory=dict)
    usage: dict[str, str] = field(default_factory=dict)
    included: set[str] = field(default_factory=set)
    excluded: dict[str, str] = field(default_factory=dict)
    adf: dict[str, Any] = field(default_factory=dict)


def _live_fetcher(cfg: MigrationConfig) -> dict[str, list[dict]]:
    import requests

    from adf2fabric.agent.warehouse.adf_sources import fetch_live
    from adf2fabric.connections.providers.azure_cli import AzureCliProvider

    lv = cfg.adf.live
    token = AzureCliProvider().token(subscription=lv.subscription_id)
    return fetch_live(requests, lv.subscription_id, lv.resource_group, lv.factory, token)


def _adf_record(cfg: MigrationConfig, store: Optional[ProjectStore], fetch: Optional[AdfFetcher],
                log: Log) -> tuple[Optional[AdfSemanticDiscovery], dict[str, Any]]:
    """The ADF definitions to analyse (live preferred, Git otherwise) and what was read."""
    from adf2fabric.agent.warehouse.adf_sources import compare, to_semantic

    info: dict[str, Any] = {"live": None, "git": None, "used": None, "differences": []}
    git = live = None
    if cfg.adf.project and store is not None:
        git = store.load_adf_semantic_discovery(cfg.adf.project)
        info["git"] = {"project": cfg.adf.project, "commit": git.source_commit_sha[:7], "pipelines": git.pipeline_count,
                       "datasets": git.dataset_count, "linked_services": git.linked_service_count}
        log(f"ADF Git record: {git.pipeline_count} pipelines, {git.dataset_count} datasets (commit {git.source_commit_sha[:7]})")
    if cfg.adf.live:
        try:
            live = to_semantic((fetch or _live_fetcher)(cfg), cfg.project, f"live:{cfg.adf.live.factory}")
            info["live"] = {"factory": cfg.adf.live.factory, "pipelines": live.pipeline_count,
                            "datasets": live.dataset_count, "linked_services": live.linked_service_count}
            log(f"ADF live {cfg.adf.live.factory}: {live.pipeline_count} pipelines, {live.dataset_count} datasets")
        except Exception as exc:  # noqa: BLE001 - fall back to Git, and say so
            info["live_error"] = str(exc)[:300]
            log(f"ADF live could not be read ({str(exc)[:120]}); using Git only")
    if live and git:
        info["differences"] = compare(live, git)
        log(f"ADF live vs Git: {len(info['differences'])} difference(s)")
    chosen = live or git
    info["used"] = "live" if live else ("git" if git else None)
    return chosen, info


def analyze(cfg: MigrationConfig, store: Optional[ProjectStore], adf_fetcher: Optional[AdfFetcher] = None,
            log: Log = lambda m: None) -> Analysis:
    log(f"discovering source with adapter {cfg.source.adapter}")
    catalog = adapter.create(cfg.source.adapter, cfg.source.options).discover()
    a = Analysis(catalog=catalog, notes=list(catalog.notes))
    adf, a.adf = _adf_record(cfg, store, adf_fetcher, log)
    if adf is not None:
        a.refs, adf_notes = dependencies.analyze(adf, catalog, cfg.source.database)
        a.notes += adf_notes
    elif cfg.scope.mode == "adf_related":
        raise ValueError("scope.mode adf_related needs ADF definitions: set adf.project and/or adf.live.")
    a.deps = dependencies.object_dependencies(catalog)
    a.usage = dependencies.classify(catalog, a.refs, a.deps)
    a.included, a.excluded = dependencies.select_scope(catalog, a.usage, a.deps, cfg.scope.mode)
    log(f"scope {cfg.scope.mode}: {len(a.included)} object(s) in, {len(a.excluded)} out")
    return a


def _manifest(cfg: MigrationConfig, a: Analysis, target: Optional[dict[str, TargetObject]]) -> list[ManifestEntry]:
    return planner.build_manifest(a.catalog, a.usage, a.refs, a.deps, target, cfg.scope.schema_map, cfg.scope.include,
                                  cfg.scope.exclude, cfg.loading.offline_method, cfg.default_varchar_length,
                                  out_of_scope=a.excluded)


def _reasons(a: Analysis) -> dict[str, str]:
    out = {}
    for k in a.included:
        u = a.usage.get(k, "standalone")
        out[k] = f"used by ADF ({u})" if u in ("direct", "dynamic") else "dependency of an ADF-related object"
    return out


def _report(cfg: MigrationConfig, a: Analysis, manifest: list[ManifestEntry], checks: list, target_info: dict[str, Any],
            target: Optional[dict[str, TargetObject]], deploy: Optional[dict[str, Any]] = None) -> MigrationReport:
    notes = list(a.notes)
    if target is not None:
        known = {f"{e.target_schema}.{e.target_name}".lower() for e in manifest}
        extra = sorted(k for k in target if k not in known)
        if extra:
            notes.append(f"In the target but not in the source catalog: {', '.join(extra)}.")
    unresolved = [f"{o.key}: {o.notes[0]}" for o in a.catalog.objects if o.provenance.value != "verified" and o.notes]
    unresolved += [f"{x.asset}: {x.note}" for x in a.refs if x.resolution == Resolution.dynamic and x.asset_kind != "catalog_query"]
    kinds = Counter(o.kind.value for o in a.catalog.objects)
    return MigrationReport(
        project_id=cfg.project, created_at=datetime.now(timezone.utc),
        source={"type": a.catalog.source_type, "mode": a.catalog.mode, "adapter": cfg.source.adapter,
                "database": cfg.source.database, "secrets_masked": a.catalog.secrets_masked},
        target=target_info,
        catalog_summary={**{f"source {k}s": v for k, v in sorted(kinds.items()) if k != ObjectKind.other.value},
                         "server-level objects (manual)": kinds.get(ObjectKind.other.value, 0),
                         "ADF references": len(a.refs), "in scope": len(a.included), "out of scope": len(a.excluded)},
        manifest=manifest, references=a.refs, checks=checks, unresolved=unresolved, notes=notes, adf=a.adf,
        scope={"mode": cfg.scope.mode, "included": dict(sorted(_reasons(a).items())),
               "excluded": dict(sorted(a.excluded.items()))},
        deploy=deploy)


def _write(out_dir: Path, a: Analysis, rep: MigrationReport) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "catalog.json").write_text(a.catalog.model_dump_json(indent=1), encoding="utf-8")
    (out_dir / "report.json").write_text(rep.model_dump_json(indent=1), encoding="utf-8")
    (out_dir / "report.md").write_text(report.markdown(rep), encoding="utf-8")
    ddl = [f"-- {e.source_key}\n{e.ddl}\n" for e in rep.manifest if e.ddl and e.action != "skip"]
    (out_dir / "ddl-dry-run.sql").write_text(
        "-- Generated DDL for the in-scope tables (for review; the deploy step runs its own copy).\n\n" + "\n".join(ddl),
        encoding="utf-8")
    (out_dir / "dependency-graph.json").write_text(json.dumps(
        {"object_dependencies": a.deps, "adf_usage": a.usage, "scope": rep.scope,
         "references": [x.model_dump(mode="json") for x in a.refs]}, indent=1), encoding="utf-8")


def run(cfg: MigrationConfig, store: Optional[ProjectStore], out_dir: Path, inspect_target: bool = True,
        target_conn_factory: Optional[Callable[[], Any]] = None, adf_fetcher: Optional[AdfFetcher] = None,
        log: Log = lambda m: None) -> MigrationReport:
    """Read-only assessment of a source against an existing target (``warehouse-assess``)."""
    a = analyze(cfg, store, adf_fetcher, log)
    target: Optional[dict[str, TargetObject]] = None
    if inspect_target and cfg.target.sql_endpoint and cfg.target.warehouse:
        log(f"reading target {cfg.target.warehouse} (read-only)")
        from adf2fabric.agent.warehouse import target_fabric

        conn = (target_conn_factory or (lambda: target_fabric.entra_connection(
            cfg.target.sql_endpoint, cfg.target.warehouse, cfg.target.tenant_id)))()
        try:
            target = target_fabric.inventory(conn)
        finally:
            conn.close()
    manifest = _manifest(cfg, a, target)
    checks = validate.validate(a.catalog, manifest, target, cfg.default_varchar_length) if target is not None else []
    rep = _report(cfg, a, manifest, checks, {"workspace": cfg.target.workspace, "warehouse": cfg.target.warehouse,
                                             "inspected": target is not None, "objects": len(target or {})}, target)
    _write(out_dir, a, rep)
    log(f"wrote report to {out_dir}")
    return rep


def migrate(cfg: MigrationConfig, store: Optional[ProjectStore], out_dir: Path, *, apply: bool,
            create_warehouse: bool = False, load: bool = True, client: Any = None,
            conn_factory: Optional[Callable[[str], Any]] = None, adf_fetcher: Optional[AdfFetcher] = None,
            log: Log = lambda m: None) -> MigrationReport:
    """Discover, decide scope, then create/load the in-scope objects in the target Fabric Warehouse and validate.

    Writes only with ``apply`` and ``target.read_only: false``. A missing Warehouse is created only with
    ``create_warehouse`` and ``target.create_if_missing``. Existing objects follow ``loading.conflict_policy``.
    """
    from adf2fabric.agent.warehouse import deploy as wh_deploy, fabric_items, target_fabric

    t = cfg.target
    if apply and t.read_only:
        raise ValueError("target.read_only is true: set it to false in the config to migrate (after approval).")
    if not (t.workspace and t.warehouse):
        raise ValueError("target.workspace and target.warehouse are required.")
    a = analyze(cfg, store, adf_fetcher, log)

    if client is None:
        from adf2fabric.connections.providers.azure_cli import AzureCliProvider

        client = fabric_items.WarehouseClient(AzureCliProvider().token(tenant=t.tenant_id, resource=fabric_items.FABRIC_RESOURCE))
    ws = client.workspace_id(t.workspace)
    wh = client.find_warehouse(ws, t.warehouse)
    target_info: dict[str, Any] = {"workspace": t.workspace, "warehouse": t.warehouse, "created": False}
    if wh is None:
        if not (create_warehouse and t.create_if_missing):
            raise ValueError(f"Warehouse '{t.warehouse}' does not exist in '{t.workspace}'. Creating one needs "
                             "target.create_if_missing: true and --create-warehouse.")
        if not apply:
            log(f"would create Warehouse '{t.warehouse}' in '{t.workspace}' (dry run); every in-scope object would be created")
            rep = _report(cfg, a, _manifest(cfg, a, None), [], {**target_info, "inspected": False, "would_create": True}, None)
            _write(out_dir, a, rep)
            return rep
        log(f"creating Warehouse '{t.warehouse}' in '{t.workspace}'")
        wh = client.create_warehouse(ws, t.warehouse, f"Migrated from {cfg.source.database or cfg.source.adapter} by adf2fabric",
                                     collation=t.collation)
        target_info["created"] = True
    endpoint = fabric_items.sql_endpoint(wh) or t.sql_endpoint
    if not endpoint:
        raise ValueError("The Warehouse has no SQL endpoint yet; retry in a minute.")
    target_info.update({"warehouse_id": wh["id"], "sql_endpoint": endpoint,
                        "collation": (wh.get("properties") or {}).get("collationType")})
    log(f"target {t.warehouse} ({wh['id']})")

    conn = (conn_factory or (lambda ep: target_fabric.entra_connection(ep, t.warehouse, t.tenant_id)))(endpoint)
    try:
        conn.autocommit = True
        manifest = _manifest(cfg, a, None)
        run_ = wh_deploy.deploy(manifest, a.catalog, conn, apply=apply, conflict_policy=cfg.loading.conflict_policy,
                                load=load, method=cfg.loading.method, url_template=cfg.loading.source_url_template,
                                parquet_dir=cfg.source.options.get("parquet_dir"), target=target_info, log=log)
        log("validating target (read-only)")
        target = target_fabric.inventory(conn)
    finally:
        conn.close()
    created = {k.lower() for k in run_.created}
    for e in manifest:  # after the run, report what the target holds
        key = f"{e.target_schema}.{e.target_name}".lower()
        if e.action in ("create", "load_only") and key in target:
            e.action = "created" if key in created else ("loaded" if e.action == "load_only" and run_.applied else "exists")
    checks = validate.validate(a.catalog, manifest, target, cfg.default_varchar_length)
    target_info.update({"inspected": True, "objects": len(target)})
    rep = _report(cfg, a, manifest, checks, target_info, target, deploy=json.loads(run_.model_dump_json()))
    _write(out_dir, a, rep)
    if run_.created or run_.created_schemas:
        kinds = {f"{e.target_schema}.{e.target_name}".lower(): e.kind for e in manifest}
        stamp = run_.started_at.strftime("%Y%m%dT%H%M%SZ")
        (out_dir / f"rollback-{stamp}.sql").write_text(run_.rollback_sql(kinds), encoding="utf-8")
    log(f"migration {run_.status if apply else 'dry run (' + run_.status + ')'}; report: {out_dir / 'report.md'}")
    return rep


def repoint_pipelines(cfg: MigrationConfig, out_dir: Path, *, apply: bool, client: Any = None,
                      log: Log = lambda m: None) -> dict[str, Any]:
    """Point the configured Fabric pipelines' reads of the old source warehouse at the target Warehouse.

    Every pipeline is backed up before it is changed. Dry run unless ``apply``.
    """
    from adf2fabric.agent.warehouse import fabric_items, repoint

    t, rp = cfg.target, cfg.repoint
    if not rp.pipelines or not rp.from_connections:
        raise ValueError("Set repoint.pipelines and repoint.from_connections in the config.")
    if apply and t.read_only:
        raise ValueError("target.read_only is true: set it to false in the config to change pipelines (after approval).")
    if client is None:
        from adf2fabric.connections.providers.azure_cli import AzureCliProvider

        client = fabric_items.WarehouseClient(AzureCliProvider().token(tenant=t.tenant_id, resource=fabric_items.FABRIC_RESOURCE))
    ws = client.workspace_id(rp.workspace or t.workspace)
    wh = client.find_warehouse(client.workspace_id(t.workspace), t.warehouse)
    if wh is None:
        raise ValueError(f"Warehouse '{t.warehouse}' not found in '{t.workspace}'.")
    ls = repoint.warehouse_linked_service(t.warehouse, wh["id"], wh.get("workspaceId") or ws, fabric_items.sql_endpoint(wh))
    ids = client.list_pipelines(ws)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {"applied": apply, "warehouse": t.warehouse, "pipelines": {}}
    for name in rp.pipelines:
        pid = ids.get(name.lower())
        if not pid:
            result["pipelines"][name] = {"status": "not found"}
            log(f"{name}: not found in the workspace")
            continue
        content = client.pipeline_definition(ws, pid)
        new, changes = repoint.repoint(content, rp.from_connections, ls)
        entry: dict[str, Any] = {"changes": changes, "status": "unchanged" if new == content else "planned"}
        if new != content and apply:
            backup = out_dir / f"backup-{name}-{stamp}.json"
            backup.write_text(json.dumps(content, indent=1), encoding="utf-8")
            client.update(ws, pid, name, new)
            entry.update(status="updated", backup=str(backup))
        result["pipelines"][name] = entry
        log(f"{name}: {entry['status']} ({len(changes)} change(s))")
        for c in changes:
            log(f"    {c}")
    (out_dir / f"repoint-{stamp}.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
    return result
