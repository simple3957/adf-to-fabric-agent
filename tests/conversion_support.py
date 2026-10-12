"""Shared fixtures for the conversion tests: build the semantic IR in memory, run the engine.

No filesystem, no network, no model. Pipelines/datasets/linked services are written the way ADF
authors them, parsed by the real parsers, resolved, and handed to the real engine.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from adf2fabric.agent.conversion.agent import build_project_context
from adf2fabric.agent.conversion.context import Resolutions
from adf2fabric.agent.conversion.engine import ConversionEngine
from adf2fabric.agent.discovery.adf_semantic import parser, parser_pipelines as pp
from adf2fabric.project.conversion import ActivityConversion, PipelineConversion
from adf2fabric.project.discovery import (
    AdfFactoryConfig,
    AdfSemanticDiscovery,
    AdfSemanticStatus,
)
from tests.test_parser_pipelines import act, dep, expr, pipe, ref  # noqa: F401  (re-exported)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def dataset(name: str, type_: str, ls: Optional[str], tp: Optional[dict] = None, *,
            params: Optional[dict] = None, columns: Optional[list] = None) -> dict:
    props: dict[str, Any] = {"type": type_, "typeProperties": tp or {}}
    if ls:
        props["linkedServiceName"] = ref("LinkedService", ls)
    if params:
        props["parameters"] = params
    if columns:
        props["schema"] = columns
    return {"name": name, "properties": props}


def linked_service(name: str, type_: str, tp: Optional[dict] = None, connect_via: Optional[str] = None) -> dict:
    props: dict[str, Any] = {"type": type_, "typeProperties": tp or {}}
    if connect_via:
        props["connectVia"] = ref("IntegrationRuntime", connect_via)
    return {"name": name, "properties": props}


def trigger(name: str, pipeline_name: str, type_: str = "ScheduleTrigger") -> dict:
    return {"name": name, "properties": {
        "type": type_, "runtimeState": "Started",
        "pipelines": [{"pipelineReference": ref("Pipeline", pipeline_name)}],
        "typeProperties": {"recurrence": {"frequency": "Day", "interval": 1, "startTime": "2024-01-01T00:00:00Z",
                                          "timeZone": "UTC"}}}}


def semantic(pipelines: list[dict], datasets: list[dict] = (), linked_services: list[dict] = (),
             triggers: list[dict] = (), global_params: Optional[dict] = None) -> AdfSemanticDiscovery:
    """Parse + resolve exactly as ADF Semantic Discovery does, without touching disk."""
    pls = [pp.parse_pipeline(p, f"pipeline/{p['name']}.json") for p in pipelines]
    dss = [parser.parse_dataset(d, f"dataset/{d['name']}.json") for d in datasets]
    lss = [parser.parse_linked_service(s, f"linkedService/{s['name']}.json") for s in linked_services]
    trs = [parser.parse_trigger(t, f"trigger/{t['name']}.json") for t in triggers]
    pp.resolve_pipelines(pls, dss, lss)
    factory = [AdfFactoryConfig(name="factory", path="factory/f.json", global_parameters=parser._parse_param_map(
        global_params))] if global_params else []
    return AdfSemanticDiscovery(
        project_id="t", source_commit_sha="deadbeef", status=AdfSemanticStatus.discovered, created_at=NOW,
        updated_at=NOW, pipelines=sorted(pls, key=lambda p: p.name), datasets=dss, linked_services=lss,
        triggers=trs, factory=factory)


def convert(pipelines: list[dict], datasets: list[dict] = (), linked_services: list[dict] = (),
            triggers: list[dict] = (), global_params: Optional[dict] = None,
            resolutions: Optional[list[dict]] = None) -> dict[str, tuple[PipelineConversion, dict]]:
    d = semantic(pipelines, datasets, linked_services, triggers, global_params)
    engine = ConversionEngine(build_project_context(d, Resolutions(resolutions)))
    return {p.name: engine.convert_pipeline(p) for p in d.pipelines}


def convert_one(pipeline: dict, **kw) -> tuple[PipelineConversion, dict]:
    return convert([pipeline], **kw)[pipeline["name"]]


def by_name(record: PipelineConversion) -> dict[str, ActivityConversion]:
    return {a.source_name: a for a in record.activities}


def activities(content: dict) -> list[dict]:
    return content["properties"]["activities"]


def find(content: dict, name: str) -> dict:
    """An activity of the generated pipeline by name, searching nested lists too."""
    def walk(items):
        for a in items:
            yield a
            tp = a.get("typeProperties", {})
            for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
                yield from walk(tp.get(key, []))
            for c in tp.get("cases", []):
                yield from walk(c.get("activities", []))
    return next(a for a in walk(activities(content)) if a["name"] == name)


def codes(a: ActivityConversion) -> set[str]:
    return {w.code for w in a.warnings}


# --- common ADF building blocks ---------------------------------------------------------------------------

SQL_LS = linked_service("LS_SQL", "AzureSqlDatabase")
LAKE_LS = linked_service("LS_LAKE", "AzureBlobFS", {"url": "https://acct.dfs.core.windows.net"})
DS_SQL = dataset("DS_SQL", "AzureSqlTable", "LS_SQL", {"schema": "dbo", "table": "Customer"},
                 columns=[{"name": "Id", "type": "int"}])
DS_LAKE = dataset("DS_LAKE", "Parquet", "LS_LAKE", {"location": {
    "type": "AzureBlobFSLocation", "fileSystem": "raw",
    "folderPath": expr("@dataset().folder"), "fileName": "data.parquet"}}, params={"folder": {"type": "String"}})


def copy_activity(name: str = "Copy", **tp) -> dict:
    return act(name, "Copy", {"source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT * FROM dbo.Customer"},
                              "sink": {"type": "ParquetSink", "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                                       "formatSettings": {"type": "ParquetWriteSettings"}}, **tp},
               inputs=[ref("Dataset", "DS_SQL")],
               outputs=[ref("Dataset", "DS_LAKE", folder="customer")])
