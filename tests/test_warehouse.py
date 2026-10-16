"""Warehouse migration framework: offline Synapse adapter, secret masking, ADF usage, planner, validation, run.

Offline: Synapse scripts and Parquet exports are written to tmp_path, the ADF record is parsed by the real parsers,
and the Fabric target is a fake DB-API connection.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from adf2fabric.agent.warehouse import adapter, dependencies as dep, planner, target_fabric, validate
from adf2fabric.agent.warehouse import agent as wh_agent
from adf2fabric.agent.warehouse.adapters.synapse_offline import parse_script
from adf2fabric.agent.warehouse.config import MigrationConfig
from adf2fabric.agent.warehouse.models import ObjectKind, Provenance, Resolution
from adf2fabric.agent.warehouse.secrets import MASK, mask
from adf2fabric.agent.warehouse.typemap import map_type, tsql_findings
from tests.conversion_support import dataset, linked_service, semantic
from tests.test_parser_pipelines import act, expr, pipe, ref

SENTINEL = "S3ntinel-Pa55word-42"

SCHEMA_SQL = """CREATE SCHEMA dw;
GO
CREATE TABLE [dw].[DimVendor](
\t[VendorKey] [int] NOT NULL,
\t[VendorName] [varchar](100) NULL
)
WITH ( DISTRIBUTION = REPLICATE, CLUSTERED INDEX (VendorKey) );
GO
CREATE TABLE dw.FactTrip
    (
    TripKey      int identity(1,1) NOT NULL,
    VendorKey    int,
    PickupAt     datetime,
    Passengers   tinyint,
    Amount       decimal(10,2),
    Note         nvarchar(50) NULL,
    CONSTRAINT PK_FactTrip PRIMARY KEY NONCLUSTERED (TripKey) NOT ENFORCED
    )
WITH ( DISTRIBUTION = HASH (VendorKey), CLUSTERED COLUMNSTORE INDEX );
GO
CREATE INDEX IX_Vendor ON [dw].[FactTrip] (VendorKey);
GO
"""
EXTERNAL_SQL = f"""CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{SENTINEL}';
CREATE DATABASE SCOPED CREDENTIAL Cred WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = 'sv=2022&sig={SENTINEL}';
CREATE EXTERNAL DATA SOURCE [lake] WITH ( LOCATION = 'abfss://c@acct.dfs.core.windows.net', TYPE = HADOOP );
GO
CREATE EXTERNAL TABLE dw.ExtTrip ( a varchar(8000), b varchar(8000) )
WITH ( LOCATION = 'trips', DATA_SOURCE = [lake], FILE_FORMAT = [pq] )
GO
"""
MODULES_SQL = """CREATE VIEW dw.vw_TripCount
AS
SELECT COUNT_BIG(*) AS n FROM [dw].[FactTrip];
------------------------------------------------------
CREATE PROCEDURE dw.usp_Check
AS
BEGIN
    SELECT COUNT_BIG(*) FROM dw.vw_TripCount;
END;

SELECT * FROM sys.objects;
"""


def _script(folder: Path, name: str, query: str, pool: str = "Pool1") -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{name}.json").write_text(json.dumps({"name": name, "properties": {"content": {
        "query": query, "currentConnection": {"databaseName": pool, "poolName": pool}}}}), encoding="utf-8")


@pytest.fixture()
def source(tmp_path: Path) -> dict:
    scripts, exports = tmp_path / "sqlscript", tmp_path / "export"
    _script(scripts, "schema", SCHEMA_SQL)
    _script(scripts, "external_tables", EXTERNAL_SQL)
    _script(scripts, "views", MODULES_SQL)
    _script(scripts, "seed", "INSERT INTO dw.DimVendor VALUES (1,'A');\nINSERT INTO dw.DimVendor VALUES (2,'B');")
    _script(scripts, "serverless", "CREATE VIEW dbo.ShouldNotAppear AS SELECT 1 AS x;", pool="Built-in")
    exports.mkdir()
    pq.write_table(pa.table({"VendorKey": pa.array([1, 2], pa.int32()), "VendorName": ["A", "B"]}), exports / "dw_DimVendor.parquet")
    pq.write_table(pa.table({"TripKey": pa.array([1, 2, 3], pa.int32())}), exports / "dw_FactTrip.parquet")
    pq.write_table(pa.table({"City": ["X"], "Total": pa.array([5], pa.int64()),
                             "At": pa.array([0], pa.timestamp("us"))}), exports / "dw_OnlyInExport.parquet")
    return {"scripts_dir": str(scripts), "parquet_dir": str(exports), "export_label": "export 2026-01-01"}


@pytest.fixture()
def catalog(source):
    return adapter.create("synapse_offline", source).discover()


# ---- secrets -----------------------------------------------------------------------------------------------------

def test_mask_hides_password_secret_and_connection_string_values():
    text, n = mask(f"CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{SENTINEL}'; x SECRET = N'{SENTINEL}'; "
                   f"Server=s;PWD={SENTINEL};AccountKey={SENTINEL}; https://a/b?sv=1&sig={SENTINEL}")
    assert SENTINEL not in text and n == 5 and text.count(MASK) == 5


def test_secrets_never_reach_the_catalog(catalog):
    assert SENTINEL not in catalog.model_dump_json()
    assert catalog.secrets_masked == 2
    assert any("masked" in n for n in catalog.notes)


# ---- offline adapter -----------------------------------------------------------------------------------------------

def test_tables_columns_and_synapse_options_are_parsed(catalog):
    v = catalog.get("dw.dimvendor")
    assert [(c.name, c.source_type, c.nullable) for c in v.columns] == [("VendorKey", "int", False), ("VendorName", "varchar(100)", True)]
    assert v.properties["distribution"] == "REPLICATE" and v.properties["storage"] == "CLUSTERED INDEX (VendorKey)"
    assert v.properties["seed_inserts"] == 2
    f = catalog.get("dw.facttrip")
    assert f.properties["distribution"] == "HASH (VendorKey)"
    assert f.properties["indexes"] == ["INDEX IX_Vendor (VendorKey)"]
    assert f.columns[0].identity and f.columns[2].source_type == "datetime"
    assert any("PRIMARY KEY" in c for c in f.properties["constraints"])


def test_modules_external_and_server_level_objects(catalog):
    view, proc = catalog.get("dw.vw_tripcount"), catalog.get("dw.usp_check")
    assert view.kind == ObjectKind.view and proc.kind == ObjectKind.procedure
    assert proc.definition.rstrip().endswith("END;") and "sys.objects" not in proc.definition  # trailing ad-hoc query cut
    ext = catalog.get("dw.exttrip")
    assert ext.kind == ObjectKind.external_table and ext.properties["data_source"] == "lake"
    others = {o.name for o in catalog.objects if o.kind == ObjectKind.other}
    assert {"MASTER KEY", "Cred", "lake"} <= others


def test_serverless_scripts_are_skipped(catalog):
    assert catalog.get("dbo.shouldnotappear") is None


def test_export_gives_row_counts_and_inferred_objects(catalog):
    assert catalog.get("dw.dimvendor").row_count == 2 and catalog.get("dw.dimvendor").row_count_source == "export 2026-01-01"
    o = catalog.get("dw.onlyinexport")
    assert o.provenance == Provenance.inferred and o.row_count == 1
    assert [(c.source_type, c.provenance) for c in o.columns] == [
        ("varchar", Provenance.inferred), ("bigint", Provenance.inferred), ("datetime2", Provenance.inferred)]
    assert "Export columns differ" in " ".join(catalog.get("dw.facttrip").notes)


def test_unknown_adapter_is_refused():
    with pytest.raises(ValueError, match="No source adapter"):
        adapter.create("oracle_live", {})


def test_parse_script_handles_bare_and_bracketed_types():
    [t] = parse_script("CREATE TABLE s.t ( [a] [decimal](18, 2) NOT NULL, b varchar(max), c datetime2(7) )", "x")
    assert [c.source_type for c in t.columns] == ["decimal(18,2)", "varchar(max)", "datetime2(7)"]


# ---- type rules ----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("src,identity,expected", [
    ("int", False, "int"), ("int", True, "bigint"), ("tinyint", False, "smallint"), ("datetime", False, "datetime2(3)"),
    ("datetime2(7)", False, "datetime2(6)"), ("money", False, "decimal(19,4)"), ("nvarchar(50)", False, "varchar(100)"),
    ("decimal(10,2)", False, "decimal(10,2)"), ("varchar", False, "varchar(8000)"), ("text", False, "varchar(max)"),
    ("geography", False, None),
])
def test_type_map(src, identity, expected):
    assert map_type(src, identity).target == expected


def test_tsql_rules_flag_synapse_only_constructs():
    assert tsql_findings("SELECT * FROM t OPTION (LABEL = 'x')") and not tsql_findings("SELECT 1")


# ---- ADF usage ---------------------------------------------------------------------------------------------------

def _adf():
    sql = "SqlPool"
    datasets = [
        dataset("DS_Vendor", "AzureSqlDWTable", sql, {"schema": "dw", "table": "DimVendor"}),
        dataset("DS_NoTable", "AzureSqlDWTable", sql, {}),
        dataset("DS_Dynamic", "AzureSqlDWTable", sql, {"schema": expr("@dataset().S"), "table": expr("@dataset().T")},
                params={"S": {"type": "String"}, "T": {"type": "String"}}),
        dataset("DS_Out", "Parquet", "Lake", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "c"}}),
    ]
    lss = [linked_service(sql, "AzureSqlDW", {"server": "srv.sql.azuresynapse.net", "database": "otherdb"}),
           linked_service("Lake", "AzureBlobFS", {"url": "https://acct.dfs.core.windows.net"})]
    count = act("CountFact", "Lookup", {"source": {"type": "SqlDWSource", "sqlReaderQuery": "SELECT COUNT(*) FROM [dw].[FactTrip]"},
                                        "dataset": ref("Dataset", "DS_NoTable")})
    lookup = act("ListObjects", "Lookup", {"source": {"type": "SqlDWSource", "sqlReaderQuery":
                 "SELECT s.name, o.name FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE o.type IN ('U','V')"},
                 "dataset": ref("Dataset", "DS_Vendor"), "firstRowOnly": False})
    copy = act("CopyEach", "Copy", {"source": {"type": "SqlDWSource", "sqlReaderQuery": expr("@concat('SELECT * FROM ', item().n)")},
                                    "sink": {"type": "ParquetSink"}},
               inputs=[ref("Dataset", "DS_Dynamic", S="@item().s", T="@item().n")], outputs=[ref("Dataset", "DS_Out")])
    loop = act("Each", "ForEach", {"items": expr("@activity('ListObjects').output.value"), "activities": [copy]},
               deps=[{"activity": "ListObjects", "dependencyConditions": ["Succeeded"]}])
    named = {"name": "PL_Named", "properties": {"parameters": {"Tables": {"type": "array", "defaultValue": ["dw.DimVendor"]}},
                                                "activities": [act("W", "Wait", {"waitTimeInSeconds": 1})]}}
    return semantic([pipe(count, name="PL_Count"), pipe(lookup, loop, name="PL_Export"), named], datasets, lss)


def test_adf_references_static_dynamic_broken_and_mentions(catalog):
    refs, notes = dep.analyze(_adf(), catalog, source_database="pool1")
    by = {(r.asset_kind, r.asset, r.object_key): r for r in refs}
    assert by[("dataset", "DS_Vendor", "dw.dimvendor")].resolution == Resolution.static
    assert by[("dataset", "DS_NoTable", None)].resolution == Resolution.broken
    assert by[("dataset", "DS_Dynamic", None)].resolution == Resolution.dynamic
    assert ("activity_query", "PL_Count/CountFact", "dw.facttrip") in by
    assert by[("parameter_value", "PL_Named", "dw.dimvendor")]
    enumerated = {r.object_key for r in refs if r.asset_kind == "catalog_query"}
    assert "dw.vw_tripcount" in enumerated and "dw.exttrip" not in enumerated  # U and V only
    assert any("otherdb" in n for n in notes)


def test_usage_classification(catalog):
    refs, _ = dep.analyze(_adf(), catalog)
    deps = dep.object_dependencies(catalog)
    assert deps["dw.usp_check"] == ["dw.vw_tripcount"] and deps["dw.vw_tripcount"] == ["dw.facttrip"]
    usage = dep.classify(catalog, refs, deps)
    assert usage["dw.dimvendor"] == "direct" and usage["dw.facttrip"] == "direct"
    assert usage["dw.vw_tripcount"] == "dynamic" and usage["dw.usp_check"] == "standalone"
    assert usage["dw.exttrip"] == "standalone"


# ---- planner + validation ----------------------------------------------------------------------------------------

def test_manifest_order_ddl_and_manual_items(catalog):
    deps = dep.object_dependencies(catalog)
    m = planner.build_manifest(catalog, dep.classify(catalog, [], deps), [], deps)
    keys = [e.source_key for e in m]
    assert keys.index("dw.facttrip") < keys.index("dw.vw_tripcount") < keys.index("dw.usp_check")
    fact = next(e for e in m if e.source_key == "dw.facttrip")
    assert "[TripKey] bigint NOT NULL" in fact.ddl and "[Passengers] smallint" in fact.ddl and "[PickupAt] datetime2(3)" in fact.ddl
    assert "WITH" not in fact.ddl and "IDENTITY" not in fact.ddl
    assert any("HASH" in x for x in fact.transformations) and any("IX_Vendor" in x for x in fact.transformations)
    assert next(e for e in m if e.source_key == "dw.exttrip").action == "manual"
    assert all(e.action == "manual" for e in m if e.kind == ObjectKind.other)
    assert SENTINEL not in json.dumps([e.model_dump(mode="json") for e in m])


class _Cursor:
    def __init__(self, objects, columns, counts):
        self.objects, self.columns, self.counts, self._rows, self.description = objects, columns, counts, [], []
        self.executed: list[str] = []

    def execute(self, sql):
        self.executed.append(sql)
        if sql == target_fabric.OBJECTS_SQL:
            self.description, self._rows = [("sch",), ("obj",), ("typ",)], self.objects
        elif sql == target_fabric.COLUMNS_SQL:
            self.description = [(x,) for x in ("sch", "obj", "col", "typ", "max_length", "precision", "scale", "is_nullable")]
            self._rows = self.columns
        else:
            m = re.search(r"FROM \[(.+)\]\.\[(.+)\]", sql)
            self.description, self._rows = [("n",)], [(self.counts[f"{m.group(1)}.{m.group(2)}"],)]

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, cur):
        self.cur, self.closed = cur, False

    def cursor(self):
        return self.cur

    def close(self):
        self.closed = True


def _target():
    objects = [("dw", "DimVendor", "U"), ("dw", "FactTrip", "U"), ("dw", "vw_TripCount", "V"), ("dw", "Extra", "P")]
    columns = [("dw", "DimVendor", "VendorKey", "int", 4, 10, 0, 0), ("dw", "DimVendor", "VendorName", "varchar", 100, 0, 0, 1),
               ("dw", "FactTrip", "TripKey", "bigint", 8, 19, 0, 0), ("dw", "FactTrip", "VendorKey", "int", 4, 10, 0, 1),
               ("dw", "FactTrip", "PickupAt", "datetime2", 8, 23, 6, 1), ("dw", "FactTrip", "Passengers", "smallint", 2, 5, 0, 1),
               ("dw", "FactTrip", "Amount", "decimal", 9, 10, 2, 1), ("dw", "FactTrip", "Note", "varchar", 100, 0, 0, 1)]
    return _Cursor(objects, columns, {"dw.DimVendor": 2, "dw.FactTrip": 4})


def test_target_inventory_runs_only_selects():
    cur = _target()
    inv = target_fabric.inventory(_Conn(cur))
    assert inv["dw.facttrip"].row_count == 4 and inv["dw.dimvendor"].columns[1].source_type == "varchar(100)"
    assert all(s.lstrip().upper().startswith("SELECT") for s in cur.executed)


def test_validation_reports_matches_and_differences(catalog):
    deps = dep.object_dependencies(catalog)
    inv = target_fabric.inventory(_Conn(_target()))
    m = planner.build_manifest(catalog, dep.classify(catalog, [], deps), [], deps, inv)
    checks = {(c.object_key, c.check): c for c in validate.validate(catalog, m, inv)}
    assert checks[("dw.dimvendor", "row_count")].status == "match"
    assert checks[("dw.facttrip", "row_count")].status == "differs"  # 3 exported, 4 in target
    assert checks[("dw.facttrip", "types")].status == "differs"  # PickupAt: expected datetime2(3), target datetime2(6)
    assert checks[("dw.usp_check", "presence")].status == "missing"
    assert checks[("dw.exttrip", "presence")].status == "not_applicable"
    assert next(e for e in m if e.source_key == "dw.dimvendor").action == "exists"  # never recreated or overwritten


# ---- config + run --------------------------------------------------------------------------------------------------

def test_config_rejects_secrets_and_deploy_needs_write_mode(source, tmp_path):
    with pytest.raises(ValueError, match="Secret-like key"):
        MigrationConfig.model_validate({"project": "p", "source": {"adapter": "synapse_offline", "options": {"password": "x"}}})
    cfg = MigrationConfig.model_validate({"project": "p", "source": {"adapter": "synapse_offline", "options": source},
                                          "target": {"workspace": "w", "warehouse": "wh"}})
    with pytest.raises(ValueError, match="read_only"):
        wh_agent.migrate(cfg, None, tmp_path / "out", apply=True)


def test_full_run_writes_reports_without_secrets(source, tmp_path):
    cfg = MigrationConfig.model_validate({
        "project": "p", "source": {"adapter": "synapse_offline", "database": "pool1", "options": source},
        "scope": {"mode": "all"},
        "target": {"warehouse": "wh", "sql_endpoint": "x.datawarehouse.fabric.microsoft.com", "tenant_id": "t"}})
    conn = _Conn(_target())
    out = tmp_path / "out"
    rep = wh_agent.run(cfg, None, out, target_conn_factory=lambda: conn)
    assert conn.closed and rep.target["inspected"]
    assert {p.name for p in out.iterdir()} == {"catalog.json", "report.json", "report.md", "ddl-dry-run.sql", "dependency-graph.json"}
    for p in out.iterdir():
        assert SENTINEL not in p.read_text(encoding="utf-8"), p.name
    md = (out / "report.md").read_text(encoding="utf-8")
    assert "Nothing was deployed or loaded" in md and "dw.extra" in md


# ---- deploy (phase 6) ----------------------------------------------------------------------------------------------

class _DeployCursor:
    def __init__(self, existing=(), fail_on=None):
        self.existing, self.fail_on, self.executed, self._rows, self.loaded = list(existing), fail_on, [], [], {}

    def execute(self, sql, params=None):
        self.executed.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("boom")
        if sql.startswith("SELECT s.name, o.name"):
            self._rows = [tuple(x.split(".")) for x in self.existing]
        elif sql.startswith("SELECT name FROM sys.schemas"):
            self._rows = [("dbo",)]
        elif sql.startswith("SELECT COUNT_BIG"):
            m = re.search(r"FROM \[(.+)\]\.\[(.+)\]", sql)
            self._rows = [(self.loaded.get(f"{m.group(1)}.{m.group(2)}", 0),)]
        elif sql.startswith("COPY INTO"):
            m = re.search(r"COPY INTO \[(.+)\]\.\[(.+)\] FROM '.*/(\w+)\.parquet'", sql)
            self.loaded[f"{m.group(1)}.{m.group(2)}"] = pq.read_metadata(self.export / f"{m.group(3)}.parquet").num_rows
        elif sql.startswith("INSERT INTO"):
            m = re.search(r"INSERT INTO \[(.+)\]\.\[(.+)\] \(", sql)
            k = f"{m.group(1)}.{m.group(2)}"
            self.loaded[k] = self.loaded.get(k, 0) + sql.count("(?")

    def fetchall(self):
        return self._rows


def _deploy(catalog, cursor, source, **kw):
    from adf2fabric.agent.warehouse import deploy as wh_deploy

    cursor.export = Path(source["parquet_dir"])
    deps = dep.object_dependencies(catalog)
    m = planner.build_manifest(catalog, dep.classify(catalog, [], deps), [], deps, None,
                               exclude=["dw.onlyinexport"])
    return wh_deploy.deploy(m, catalog, _Conn(cursor), url_template="https://onelake/x/{file}",
                            parquet_dir=source["parquet_dir"], **kw)


def test_deploy_dry_run_executes_only_preflight(catalog, source):
    cur = _DeployCursor()
    run = _deploy(catalog, cur, source, apply=False)
    assert run.status == "planned" and all(s.lstrip().upper().startswith("SELECT") for s in cur.executed)
    actions = [(s.action, s.object_key) for s in run.steps]
    assert actions[0] == ("create_schema", "dw")
    assert actions.index(("create", "dw.FactTrip")) < actions.index(("create", "dw.vw_TripCount")) < actions.index(("create", "dw.usp_Check"))
    assert ("load", "dw.DimVendor") in actions and not any(k == "dw.ExtTrip" for _, k in actions)  # external: manual


def test_deploy_refuses_when_an_object_exists(catalog, source):
    cur = _DeployCursor(existing=["dw.DimVendor"])
    run = _deploy(catalog, cur, source, apply=True)
    assert run.status == "refused" and run.conflicts == ["dw.DimVendor"]
    assert all(s.lstrip().upper().startswith("SELECT") for s in cur.executed)  # nothing changed


def test_deploy_skip_policy_leaves_existing_objects(catalog, source):
    cur = _DeployCursor(existing=["dw.DimVendor"])
    run = _deploy(catalog, cur, source, apply=True, conflict_policy="skip")
    assert not any(s.object_key == "dw.DimVendor" for s in run.steps)
    assert not any("DimVendor" in s and not s.startswith("SELECT") for s in cur.executed)


def test_deploy_applies_loads_verifies_and_writes_rollback(catalog, source):
    cur = _DeployCursor()
    run = _deploy(catalog, cur, source, apply=True)
    assert run.status == "succeeded"
    assert "COPY INTO [dw].[FactTrip] FROM 'https://onelake/x/dw_FactTrip.parquet' WITH (FILE_TYPE = 'PARQUET')" in cur.executed
    cur2 = _DeployCursor()
    run2 = _deploy(catalog, cur2, source, apply=True, method="insert_from_parquet")
    loads = {s.object_key: s for s in run2.steps if s.action == "load"}
    assert loads["dw.DimVendor"].status == "done" and "2 rows in target, 2 in source" in loads["dw.DimVendor"].detail
    assert run2.status == "succeeded" and run2.created_schemas == ["dw"]
    sql = run2.rollback_sql({"dw.vw_tripcount": ObjectKind.view, "dw.usp_check": ObjectKind.procedure})
    assert sql.index("DROP PROCEDURE IF EXISTS [dw].[usp_Check]") < sql.index("DROP TABLE IF EXISTS [dw].[DimVendor]")
    assert sql.rstrip().endswith("DROP SCHEMA [dw];")


def test_deploy_stops_at_the_first_failure(catalog, source):
    cur = _DeployCursor(fail_on="CREATE VIEW")
    run = _deploy(catalog, cur, source, apply=True, method="insert_from_parquet")
    assert run.status == "failed"
    failed = [s for s in run.steps if s.status == "failed"]
    assert len(failed) == 1 and failed[0].object_key == "dw.vw_TripCount"
    assert all(s.status == "skipped" for s in run.steps[run.steps.index(failed[0]) + 1:])


def test_deploy_fails_on_row_count_mismatch(catalog, source):
    class Short(_DeployCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if sql.startswith("COPY INTO"):
                self.loaded = {k: v - 1 for k, v in self.loaded.items()}

    run = _deploy(catalog, Short(), source, apply=True)
    failed = next(s for s in run.steps if s.status == "failed")
    assert run.status == "failed" and failed.action == "load" and "row count mismatch" in failed.detail


def test_deploy_if_empty_resumes_loads_without_touching_filled_tables(catalog, source):
    cur = _DeployCursor(existing=["dw.DimVendor", "dw.FactTrip"])
    cur.loaded = {"dw.DimVendor": 2}  # filled; FactTrip exists but is empty
    run = _deploy(catalog, cur, source, apply=True, conflict_policy="if_empty")
    acts = {(s.action, s.object_key) for s in run.steps}
    assert ("load", "dw.FactTrip") in acts and ("create", "dw.FactTrip") not in acts
    assert not any(k == "dw.DimVendor" for _, k in acts)
    assert run.status == "succeeded"


# ---- ADF live + Git, scope, one-command migration --------------------------------------------------------------------

class _Resp:
    def __init__(self, body, status=200):
        self._b, self.status_code = body, status

    def json(self):
        return self._b


class _ArmTransport:
    """Fake ARM: serves a factory as live JSON, pipelines over two pages. It has only GET."""

    def __init__(self, raw):
        self.raw, self.urls = raw, []

    def get(self, url, headers=None, timeout=None):
        self.urls.append(url)
        coll = url.split("/factories/f/")[1].split("?")[0]
        items = self.raw[coll]
        if coll == "pipelines" and "page2" not in url:
            return _Resp({"value": items[:1], "nextLink": url + "&page2"})
        return _Resp({"value": items[1:] if coll == "pipelines" else items})


def _adf_raw():
    sql = "SqlPool"
    return {
        "linkedservices": [linked_service(sql, "AzureSqlDW", {"server": "srv", "database": "pool1"})],
        "datasets": [dataset("DS_Vendor", "AzureSqlDWTable", sql, {"schema": "dw", "table": "DimVendor"})],
        "triggers": [],
        "pipelines": [pipe(act("CountFact", "Lookup", {"source": {"type": "SqlDWSource", "sqlReaderQuery":
                                                                  "SELECT COUNT(*) FROM [dw].[FactTrip]"},
                                                       "dataset": ref("Dataset", "DS_Vendor")}), name="PL_A"),
                      pipe(act("W", "Wait", {"waitTimeInSeconds": 1}), name="PL_B")],
    }


def _git(raw):
    return semantic(raw["pipelines"], raw["datasets"], raw["linkedservices"])


def test_live_adf_is_fetched_read_only_and_matches_git():
    from adf2fabric.agent.warehouse import adf_sources
    raw = _adf_raw()
    t = _ArmTransport(raw)
    fetched = adf_sources.fetch_live(t, "sub", "rg", "f", "tok")
    assert [p["name"] for p in fetched["pipelines"]] == ["PL_A", "PL_B"]  # pagination followed
    assert not hasattr(t, "post") and len(t.urls) == 5
    assert adf_sources.compare(adf_sources.to_semantic(fetched, "p", "live"), _git(raw)) == []


def test_live_vs_git_differences_are_reported():
    from adf2fabric.agent.warehouse import adf_sources
    raw = _adf_raw()
    live = adf_sources.to_semantic(raw, "p", "live")
    changed = json.loads(json.dumps(raw))
    changed["datasets"][0]["properties"]["typeProperties"]["table"] = "DimOther"
    changed["pipelines"] = changed["pipelines"][:1]
    diffs = adf_sources.compare(live, _git(changed))
    assert "pipeline PL_B: in live ADF, not in Git" in diffs
    assert "dataset DS_Vendor: definition differs between live ADF and Git" in diffs


def test_scope_adf_related_takes_used_objects_and_their_dependencies(catalog):
    refs, _ = dep.analyze(_git(_adf_raw()), catalog)
    deps = dep.object_dependencies(catalog)
    usage = dep.classify(catalog, refs, deps)
    inc, exc = dep.select_scope(catalog, usage, deps, "adf_related")
    assert {"dw.dimvendor", "dw.facttrip"} <= inc  # used by ADF
    assert {"dw.vw_tripcount", "dw.usp_check"} <= inc  # the view reads FactTrip; the proc reads the view
    assert exc["dw.exttrip"] == "not used by ADF" and "dw.onlyinexport" in exc
    assert dep.select_scope(catalog, usage, deps, "all")[1] == {}


class _FakeFabric:
    def __init__(self, exists=True):
        self.exists, self.created = exists, []

    def workspace_id(self, name):
        return "ws-1"

    def find_warehouse(self, ws, name):
        return {"id": "wh-1", "properties": {"connectionString": "x.fabric", "collationType": "c"}} if self.exists else None

    def create_warehouse(self, ws, name, description="", collation=None):
        self.created.append(name)
        self.exists = True
        return self.find_warehouse(ws, name)


class _MigrateCursor(_DeployCursor):
    """Deploy cursor that also answers the target inventory queries from what exists or was created."""

    def __init__(self, existing=()):
        super().__init__(existing)
        self.made = {k: "U" for k in existing}

    def execute(self, sql, params=None):
        if sql == target_fabric.OBJECTS_SQL:
            self.executed.append(sql)
            self.description = [("sch",), ("obj",), ("typ",)]
            self._rows = [(*k.split("."), t) for k, t in self.made.items()]
            return
        if sql == target_fabric.COLUMNS_SQL:
            self.executed.append(sql)
            self.description = [(x,) for x in ("sch", "obj", "col", "typ", "max_length", "precision", "scale", "is_nullable")]
            self._rows = []
            return
        super().execute(sql, params)
        self.description = [("n",)]
        m = re.match(r"CREATE (TABLE|VIEW|PROCEDURE) \[?(\w+)\]?\.\[?(\w+)\]?", sql)
        if m:
            self.made[f"{m.group(2)}.{m.group(3)}"] = {"TABLE": "U", "VIEW": "V", "PROCEDURE": "P"}[m.group(1)]


def _migrate_cfg(source, **target):
    return MigrationConfig.model_validate({
        "project": "p", "source": {"adapter": "synapse_offline", "database": "pool1", "options": source},
        "adf": {"live": {"subscription_id": "s", "resource_group": "rg", "factory": "f"}},
        "target": {"workspace": "w", "warehouse": "wh", "read_only": False, **target},
        "loading": {"source_url_template": "https://onelake/x/{file}"}})


def test_migrate_dry_run_writes_nothing(source, tmp_path):
    cur = _MigrateCursor()
    cur.export = Path(source["parquet_dir"])
    rep = wh_agent.migrate(_migrate_cfg(source), None, tmp_path / "out", apply=False, client=_FakeFabric(),
                           conn_factory=lambda ep: _Conn(cur), adf_fetcher=lambda c: _adf_raw())
    assert rep.deploy["status"] == "planned" and not rep.deploy["applied"]
    assert all(s.lstrip().upper().startswith("SELECT") for s in cur.executed)
    assert rep.adf["used"] == "live" and rep.scope["excluded"]["dw.exttrip"] == "not used by ADF"
    assert "Dry run" in (tmp_path / "out" / "report.md").read_text(encoding="utf-8")


def test_migrate_creates_warehouse_deploys_in_scope_objects_and_validates(source, tmp_path):
    cur = _MigrateCursor()
    cur.export = Path(source["parquet_dir"])
    fabric = _FakeFabric(exists=False)
    cfg = _migrate_cfg(source, create_if_missing=True)
    with pytest.raises(ValueError, match="create-warehouse"):
        wh_agent.migrate(cfg, None, tmp_path / "o1", apply=True, client=fabric, conn_factory=lambda ep: _Conn(cur),
                         adf_fetcher=lambda c: _adf_raw())
    rep = wh_agent.migrate(cfg, None, tmp_path / "out", apply=True, create_warehouse=True, client=fabric,
                           conn_factory=lambda ep: _Conn(cur), adf_fetcher=lambda c: _adf_raw())
    assert fabric.created == ["wh"] and rep.deploy["status"] == "succeeded"
    made = {s["object_key"] for s in rep.deploy["steps"] if s["action"] == "create"}
    assert made == {"dw.DimVendor", "dw.FactTrip", "dw.vw_TripCount", "dw.usp_Check"}  # in scope only
    actions = {e.source_key: e.action for e in rep.manifest}
    assert actions["dw.onlyinexport"] == "skip" and actions["dw.dimvendor"] == "created"
    assert list((tmp_path / "out").glob("rollback-*.sql"))
    assert SENTINEL not in (tmp_path / "out" / "report.json").read_text(encoding="utf-8")


def test_migrate_into_a_filled_target_with_skip_changes_nothing(source, tmp_path):
    cur = _MigrateCursor(existing=["dw.DimVendor", "dw.FactTrip", "dw.vw_TripCount", "dw.usp_Check"])
    cur.export = Path(source["parquet_dir"])
    cfg = _migrate_cfg(source)
    cfg.loading.conflict_policy = "skip"
    rep = wh_agent.migrate(cfg, None, tmp_path / "out", apply=True, client=_FakeFabric(),
                           conn_factory=lambda ep: _Conn(cur), adf_fetcher=lambda c: _adf_raw())
    assert rep.deploy["status"] == "succeeded" and rep.deploy["created"] == []
    assert all(s.lstrip().upper().startswith("SELECT") for s in cur.executed)


# ---- repoint pipelines to the Fabric Warehouse (phase 7) ---------------------------------------------------------

_OLD = "d47b78d9-0000-0000-0000-000000000000"


def _lookup(name, conn=_OLD, src="AzureSqlSource"):
    return {"name": name, "type": "Lookup", "typeProperties": {
        "source": {"type": src, "sqlReaderQuery": "SELECT COUNT_BIG(*) AS n FROM [dw].[T]", "partitionOption": "None"},
        "datasetSettings": {"annotations": [], "type": "AzureSqlDWTable", "schema": [],
                            "typeProperties": {"schema": "dw", "table": "T"}, "externalReferences": {"connection": conn}},
        "firstRowOnly": True}}


def _repoint_pipeline():
    copy_act = {"name": "CopyIt", "type": "Copy", "typeProperties": {
        "source": {"type": "SqlDWSource", "sqlReaderQuery": "SELECT 1",
                   "datasetSettings": {"type": "AzureSqlDWTable", "externalReferences": {"connection": _OLD}}},
        "sink": {"type": "ParquetSink", "datasetSettings": {"type": "Parquet", "externalReferences": {"connection": "lake"}}}}}
    return {"properties": {"activities": [
        _lookup("Count1"), _lookup("Other", conn="someother"),
        {"name": "Each", "type": "ForEach", "typeProperties": {"items": {"value": "@x", "type": "Expression"},
                                                               "activities": [copy_act]}}]}}


def test_repoint_switches_old_warehouse_reads_and_keeps_everything_else():
    from adf2fabric.agent.warehouse import repoint
    ls = repoint.warehouse_linked_service("WH", "wh-id", "ws-id", "x.fabric")
    original = _repoint_pipeline()
    new, changes = repoint.repoint(original, [_OLD.upper()], ls)
    acts = new["properties"]["activities"]
    tp = acts[0]["typeProperties"]
    assert tp["source"] == {"type": "DataWarehouseSource", "sqlReaderQuery": "SELECT COUNT_BIG(*) AS n FROM [dw].[T]",
                            "partitionOption": "None"}
    ds = tp["datasetSettings"]
    assert ds["type"] == "DataWarehouseTable" and "externalReferences" not in ds
    assert ds["typeProperties"] == {"schema": "dw", "table": "T"}
    assert ds["linkedService"]["properties"]["typeProperties"] == {"endpoint": "x.fabric", "artifactId": "wh-id", "workspaceId": "ws-id"}
    assert acts[1] == original["properties"]["activities"][1]  # other connection untouched
    inner = acts[2]["typeProperties"]["activities"][0]["typeProperties"]
    assert inner["source"]["type"] == "DataWarehouseSource" and inner["sink"]["datasetSettings"]["externalReferences"] == {"connection": "lake"}
    assert len(changes) == 2 and original["properties"]["activities"][0]["typeProperties"]["source"]["type"] == "AzureSqlSource"
    again, changes2 = repoint.repoint(new, [_OLD], ls)
    assert again == new and changes2 == []


def test_repoint_reports_sinks_and_non_sql_sources_without_changing_them():
    from adf2fabric.agent.warehouse import repoint
    ls = repoint.warehouse_linked_service("WH", "w", "s", "e")
    p = {"properties": {"activities": [
        {"name": "Load", "type": "Copy", "typeProperties": {
            "source": {"type": "ParquetSource", "datasetSettings": {"type": "Parquet", "externalReferences": {"connection": "lake"}}},
            "sink": {"type": "SqlDWSink", "datasetSettings": {"type": "AzureSqlDWTable", "externalReferences": {"connection": _OLD}}}}},
        _lookup("Weird", src="RestSource")]}}
    new, changes = repoint.repoint(p, [_OLD], ls)
    assert new == p
    assert any("sink writes to the old warehouse" in c for c in changes) and any("not a SQL source" in c for c in changes)


def test_repoint_brackets_reserved_aliases_also_on_already_repointed_activities():
    from adf2fabric.agent.warehouse import repoint
    ls = repoint.warehouse_linked_service("WH", "wh-id", "ws-id", "e")
    p = {"properties": {"activities": [_lookup("Count1")]}}
    p["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"] = \
        "SELECT 'x' AS ObjectName, COUNT_BIG(*) AS RowCount FROM [dw].[T]"
    new, changes = repoint.repoint(p, [_OLD], ls)
    assert new["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"] == \
        "SELECT 'x' AS ObjectName, COUNT_BIG(*) AS [RowCount] FROM [dw].[T]"
    assert any("alias RowCount -> [RowCount]" in c for c in changes)
    assert repoint.repoint(new, [_OLD], ls) == (new, [])  # idempotent
    # an activity repointed by an earlier version (bare alias, already on the Warehouse) is fixed too
    old_style = json.loads(json.dumps(new))
    old_style["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"] = "SELECT 1 AS RowCount"
    fixed, ch = repoint.repoint(old_style, [_OLD], ls)
    assert fixed["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"] == "SELECT 1 AS [RowCount]" and ch
    assert repoint.fix_query({"value": "@x", "type": "Expression"}) == ({"value": "@x", "type": "Expression"}, [])
