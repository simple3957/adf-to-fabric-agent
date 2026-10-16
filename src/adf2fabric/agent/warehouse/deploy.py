"""Deploy the planned objects into a Fabric Warehouse and load the tables. Dry run unless ``apply``.

Safety:
- Preflight first: with conflict policy ``fail`` (default) the run stops before any change if a planned object
  already exists in the target; ``skip`` leaves existing objects (and their data) untouched; ``if_empty`` also
  loads an existing table when it has no rows (resuming a run whose load failed). Nothing is dropped, truncated
  or overwritten.
- Only ``create`` entries of the manifest are deployed; ``manual``/``skip`` entries are reported.
- Every executed statement and its outcome go to a run log, with a rollback script (DROP statements in reverse
  order) that is written for review and never executed.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from pydantic import BaseModel, ConfigDict, Field

from adf2fabric.agent.warehouse.models import CatalogObject, ManifestEntry, ObjectKind, SourceCatalog

LOAD_METHODS = ("copy_into", "insert_from_parquet")
INSERT_BATCH_PARAMS = 2000  # SQL Server allows 2100 parameters per statement


class Step(BaseModel):
    model_config = ConfigDict(extra="forbid")
    object_key: str
    action: str  # create_schema | create | load | verify
    statement: str = ""
    status: str = "planned"  # planned | done | failed | skipped
    detail: str = ""
    seconds: Optional[float] = None


class DeployRun(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: str = "warehouse-deploy-run/1"
    started_at: datetime
    finished_at: Optional[datetime] = None
    applied: bool
    target: dict[str, Any] = Field(default_factory=dict)
    conflicts: list[str] = Field(default_factory=list)
    steps: list[Step] = Field(default_factory=list)
    created: list[str] = Field(default_factory=list)  # schema.object, in creation order
    created_schemas: list[str] = Field(default_factory=list)
    status: str = "planned"  # planned | refused | succeeded | failed

    def rollback_sql(self, kinds: dict[str, ObjectKind]) -> str:
        word = {ObjectKind.view: "VIEW", ObjectKind.procedure: "PROCEDURE", ObjectKind.function: "FUNCTION"}
        lines = ["-- Rollback for the deploy run started " + self.started_at.isoformat(),
                 "-- REVIEW BEFORE RUNNING. Generated only; the framework never executes it.", ""]
        for key in reversed(self.created):
            s, n = key.split(".", 1)
            lines.append(f"DROP {word.get(kinds.get(key.lower()), 'TABLE')} IF EXISTS [{s}].[{n}];")
        for s in reversed(self.created_schemas):
            lines.append(f"DROP SCHEMA [{s}];")
        return "\n".join(lines) + "\n"


def _q(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _rows(cur: Any, sql: str) -> list[tuple]:
    cur.execute(sql)
    return cur.fetchall()


def plan_steps(manifest: list[ManifestEntry], catalog: SourceCatalog, existing_schemas: set[str],
               load: bool, method: str, url_template: Optional[str]) -> list[Step]:
    steps: list[Step] = []
    schemas = []
    for e in manifest:
        if e.action == "create" and e.target_schema.lower() not in existing_schemas and e.target_schema not in schemas:
            schemas.append(e.target_schema)
    steps += [Step(object_key=s, action="create_schema", statement=f"CREATE SCHEMA {_q(s)}") for s in schemas]
    for e in manifest:
        if e.action not in ("create", "load_only"):
            continue
        tkey = f"{e.target_schema}.{e.target_name}"
        o = catalog.get(e.source_key)
        if e.action == "load_only":
            if load:
                steps.append(_load_step(tkey, e, o, method, url_template))
        elif e.kind == ObjectKind.table:
            steps.append(Step(object_key=tkey, action="create", statement=e.ddl or ""))
            if load:
                steps.append(_load_step(tkey, e, o, method, url_template))
        elif o and o.definition:
            steps.append(Step(object_key=tkey, action="create", statement=o.definition.strip().rstrip(";")))
    return steps


def _load_step(tkey: str, e: ManifestEntry, o: Optional[CatalogObject], method: str, url_template: Optional[str]) -> Step:
    file = f"{o.schema_name}_{o.name}.parquet" if o else ""
    if o is None or o.row_count is None:
        return Step(object_key=tkey, action="load", status="skipped", detail="No export to load from.")
    if method == "copy_into":
        if not url_template:
            return Step(object_key=tkey, action="load", status="skipped", detail="loading.source_url_template is not set.")
        url = url_template.format(file=file, schema=o.schema_name, name=o.name)
        return Step(object_key=tkey, action="load", detail=file,
                    statement=f"COPY INTO {_q(e.target_schema)}.{_q(e.target_name)} FROM '{url}' WITH (FILE_TYPE = 'PARQUET')")
    return Step(object_key=tkey, action="load", detail=file, statement=f"-- insert_from_parquet {file}")


def _insert_from_parquet(cur: Any, step: Step, entry: ManifestEntry, parquet_dir: str) -> int:
    import pyarrow.parquet as pq
    from pathlib import Path

    table = pq.read_table(Path(parquet_dir) / step.detail)
    cols = table.column_names
    per = max(1, INSERT_BATCH_PARAMS // max(1, len(cols)))
    collist = ", ".join(_q(c) for c in cols)
    rows = table.to_pylist()
    for i in range(0, len(rows), per):
        chunk = rows[i : i + per]
        values = ", ".join("(" + ", ".join("?" * len(cols)) + ")" for _ in chunk)
        params = [r[c] for r in chunk for c in cols]
        cur.execute(f"INSERT INTO {_q(entry.target_schema)}.{_q(entry.target_name)} ({collist}) VALUES {values}", params)
    return len(rows)


def deploy(manifest: list[ManifestEntry], catalog: SourceCatalog, conn: Any, *, apply: bool,
           conflict_policy: str = "fail", load: bool = True, method: str = "copy_into",
           url_template: Optional[str] = None, parquet_dir: Optional[str] = None,
           target: Optional[dict[str, Any]] = None, log: Callable[[str], None] = lambda m: None) -> DeployRun:
    if conflict_policy not in ("fail", "skip", "if_empty"):
        raise ValueError(f"Conflict policy '{conflict_policy}' is not supported for deploy (use fail, skip or if_empty).")
    if method not in LOAD_METHODS:
        raise ValueError(f"Unknown load method '{method}' (use {' or '.join(LOAD_METHODS)}).")
    run = DeployRun(started_at=datetime.now(timezone.utc), applied=apply, target=target or {})
    cur = conn.cursor()

    # Preflight (read-only): what already exists in the target.
    existing = {f"{s}.{n}".lower() for s, n in _rows(cur, "SELECT s.name, o.name FROM sys.objects o JOIN sys.schemas s "
                                                          "ON s.schema_id = o.schema_id WHERE o.is_ms_shipped = 0 "
                                                          "AND o.type IN ('U','V','P','FN','IF','TF')")}
    schemas = {r[0].lower() for r in _rows(cur, "SELECT name FROM sys.schemas")}
    for e in manifest:
        if e.action == "create" and f"{e.target_schema}.{e.target_name}".lower() in existing:
            run.conflicts.append(f"{e.target_schema}.{e.target_name}")
            if conflict_policy == "if_empty" and e.kind == ObjectKind.table and not _rows(
                    cur, f"SELECT COUNT_BIG(*) FROM {_q(e.target_schema)}.{_q(e.target_name)}")[0][0]:
                e.action = "load_only"
                e.warnings.append("Already in the target and empty: loaded (conflict policy 'if_empty').")
            elif conflict_policy in ("skip", "if_empty"):
                e.action = "exists"
                e.warnings.append(f"Already in the target: left untouched (conflict policy '{conflict_policy}').")
    by_key = {f"{e.target_schema}.{e.target_name}".lower(): e for e in manifest}
    run.steps = plan_steps(manifest, catalog, schemas, load, method, url_template)
    if run.conflicts and conflict_policy == "fail":
        run.status = "refused"
        for s in run.steps:
            s.status = "skipped"
        log(f"refused: {len(run.conflicts)} object(s) already exist ({', '.join(run.conflicts[:5])}); nothing changed")
        run.finished_at = datetime.now(timezone.utc)
        return run
    if not apply:
        run.finished_at = datetime.now(timezone.utc)
        return run

    for step in run.steps:
        if step.status == "skipped":
            continue
        t0 = time.monotonic()
        try:
            if step.action == "load" and method == "insert_from_parquet":
                if not parquet_dir:
                    raise ValueError("insert_from_parquet needs the source parquet_dir.")
                n = _insert_from_parquet(cur, step, by_key[step.object_key.lower()], parquet_dir)
                step.detail = f"{step.detail}: {n} rows inserted"
            else:
                cur.execute(step.statement)
            if step.action == "create_schema":
                run.created_schemas.append(step.object_key)
            elif step.action == "create":
                run.created.append(step.object_key)
            elif step.action == "load":
                s, n = step.object_key.split(".", 1)
                got = _rows(cur, f"SELECT COUNT_BIG(*) FROM {_q(s)}.{_q(n)}")[0][0]
                o = catalog.get(by_key[step.object_key.lower()].source_key)
                step.detail += f"; {got} rows in target, {o.row_count} in source"
                if got != o.row_count:
                    raise RuntimeError(f"row count mismatch after load: {got} in target, {o.row_count} expected")
            step.status = "done"
            log(f"{step.action:<13} {step.object_key}  ok")
        except Exception as exc:  # noqa: BLE001 - recorded, then the run stops
            step.status, step.detail = "failed", f"{step.detail} {exc}".strip()[:800]
            log(f"{step.action:<13} {step.object_key}  FAILED: {step.detail}")
            run.status = "failed"
            break
        finally:
            step.seconds = round(time.monotonic() - t0, 2)
    else:
        run.status = "succeeded"
    if run.status == "failed":
        for s in run.steps:
            if s.status == "planned":
                s.status = "skipped"
    run.finished_at = datetime.now(timezone.utc)
    return run
