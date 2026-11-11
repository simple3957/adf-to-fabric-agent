"""Warehouse migration framework: offline Synapse adapter, secret masking, ADF usage, planner, validation, run.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from adf2fabric.agent.warehouse import adapter, dependencies as dep, planner
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


# ---- config + run --------------------------------------------------------------------------------------------------

def test_config_rejects_secrets_and_deploy_needs_write_mode(source, tmp_path):
    with pytest.raises(ValueError, match="Secret-like key"):
        MigrationConfig.model_validate({"project": "p", "source": {"adapter": "synapse_offline", "options": {"password": "x"}}})
    cfg = MigrationConfig.model_validate({"project": "p", "source": {"adapter": "synapse_offline", "options": source},
                                          "target": {"workspace": "w", "warehouse": "wh"}})
    with pytest.raises(ValueError, match="read_only"):
        wh_agent.migrate(cfg, None, tmp_path / "out", apply=True)


# ---- deploy (phase 6) ----------------------------------------------------------------------------------------------


# ---- ADF live + Git, scope, one-command migration --------------------------------------------------------------------


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
