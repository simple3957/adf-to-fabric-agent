"""Mapping data flow script -> PySpark notebook by fixed rules (agent/workloads/dataflow.py, 2026-11-15).

The scripts below are written in Microsoft's data flow script syntax ("Data flow script", learn.microsoft.com/azure/data-factory/
data-flow-script); the first is the real test data flow DF_Sales_By_Product of the factory adf2fabricv1, whose ADF output is known.
No Spark runs here (none is installed): the generated code is checked to parse and to say exactly what it should; the Fabric run of
the generated notebook is the proof that it computes the same rows."""

from __future__ import annotations

import ast

import pytest

from adf2fabric.agent.workloads import dataflow as DF

ENDPOINTS = {"ls_lake": "https://acct.dfs.core.windows.net/"}


def _ds(name, kind, folder=None, file=None, **tp):
    loc = {"type": "AzureBlobFSLocation", "fileSystem": "data", **({"folderPath": folder} if folder else {}), **({"fileName": file} if file else {})}
    return {"name": name, "type": kind, "linked_service_ref": {"name": "LS_Lake"}, "type_properties": {"location": loc, **tp}}


DATASETS = {d["name"].lower(): d for d in (
    _ds("DS_Sales", "DelimitedText", file="sales.csv", columnDelimiter=",", firstRowAsHeader=True, quoteChar='"', escapeChar="\\"),
    _ds("DS_Customers", "Parquet", folder="customers"),
    _ds("DS_Out", "Parquet", folder="out"),
    _ds("DS_Other", "Parquet", folder="other"),
)}
SALES_BY_PRODUCT = """source(output(
          SaleId as string,
          CustomerId as string,
          Product as string,
          Amount as long
     ),
     allowSchemaDrift: true,
     validateSchema: false,
     ignoreNoFilesFound: false) ~> SalesCsv
SalesCsv filter(Amount > 0L) ~> PositiveAmount
PositiveAmount derive(AmountBand = iif(Amount >= 50000L, 'high', 'low')) ~> Banded
Banded aggregate(groupBy(Product,
          AmountBand),
     TotalAmount = sum(Amount),
          Sales = count()) ~> ByProduct
ByProduct sink(allowSchemaDrift: true,
     validateSchema: false,
     format: 'parquet',
     truncate: true,
     umask: 0022,
     preCommands: [],
     postCommands: [],
     skipDuplicateMapInputs: true,
     skipDuplicateMapOutputs: true) ~> ParquetSink"""


def _code(script, nodes):
    tr = DF.translate(script, nodes, DATASETS, ENDPOINTS)
    code = "\n".join("".join(c["source"]) for c in tr.cells)
    ast.parse(code)  # always valid Python, even where a step is not translated
    return tr, code


def test_the_real_test_data_flow_translates_completely():
    tr, code = _code(SALES_BY_PRODUCT, {"SalesCsv": "DS_Sales", "ParquetSink": "DS_Out"})
    assert tr.untranslated == []
    assert ".schema(StructType([StructField('SaleId', StringType(), True), StructField('CustomerId', StringType(), True), " \
           "StructField('Product', StringType(), True), StructField('Amount', LongType(), True)])).csv('abfss://data@acct.dfs.core.windows.net/sales.csv')" in code
    assert "df_PositiveAmount = df_SalesCsv.filter((F.col('Amount') > F.lit(0)))" in code
    assert ".withColumn('AmountBand', F.when((F.col('Amount') >= F.lit(50000)), F.lit('high')).otherwise(F.lit('low')))" in code
    assert ("df_ByProduct = df_Banded.groupBy(F.col('Product'), F.col('AmountBand')).agg((F.sum(F.col('Amount'))).alias('TotalAmount'), "
            "(F.count(F.lit(1))).alias('Sales'))") in code
    assert "df_ByProduct.write.mode('overwrite').parquet('abfss://data@acct.dfs.core.windows.net/out')" in code  # truncate: true
    assert tr.reads == ["abfss://data@acct.dfs.core.windows.net/sales.csv"] and tr.writes == ["abfss://data@acct.dfs.core.windows.net/out"]


def test_joins_splits_selects_sorts_windows_unions_and_parameters():
    script = """parameters{
     region as string ('west'),
     minAmount as integer (10)
}
source(allowSchemaDrift: true) ~> Sales
source(allowSchemaDrift: true) ~> Customers
Sales, Customers join(Sales@CustomerId == Customers@CustomerId,
     joinType:'left',
     broadcast: 'auto') ~> Joined
Joined split(Amount >= $minAmount, isNull(Region),
     disjoint: false) ~> Split@(big, noRegion, rest)
Split@big select(mapColumn(SaleId, Total = Amount),
     skipDuplicateMapInputs: true) ~> Picked
Picked sort(desc(Total, false)) ~> Sorted
Sorted window(over(SaleId),
     asc(Total, true),
     rn = rowNumber()) ~> Ranked
Split@rest, Split@noRegion union(byName: true) ~> Others
Ranked sink(truncate: true) ~> Out1
Others sink() ~> Out2"""
    tr, code = _code(script, {"Sales": "DS_Customers", "Customers": "DS_Other", "Out1": "DS_Out", "Out2": "DS_Other"})
    assert tr.untranslated == [] and tr.parameters == [("region", "'west'"), ("minAmount", "10")]
    assert "df_Joined = df_Sales.alias('L').join(df_Customers.alias('R'), (F.col('L.CustomerId') == F.col('R.CustomerId')), 'left')" in code
    # disjoint: false sends a row to the FIRST matching output only; the last output takes what matched none
    assert "df_Split__noRegion = df_Joined.filter(~F.coalesce((F.col('Amount') >= F.lit(minAmount)), F.lit(False)) & " \
           "F.coalesce(F.col('Region').isNull(), F.lit(False)))" in code
    assert "df_Split__rest = df_Joined.filter(~F.coalesce((F.col('Amount') >= F.lit(minAmount)), F.lit(False)) & ~F.coalesce(F.col('Region').isNull(), F.lit(False)))" in code
    assert "df_Picked = df_Split__big.select(F.col('SaleId'), F.col('Amount').alias('Total'))" in code
    assert "df_Sorted = df_Picked.orderBy(F.col('Total').desc_nulls_last())" in code
    assert "_w = Window.partitionBy(F.col('SaleId')).orderBy(F.col('Total').asc())" in code and ".withColumn('rn', (F.row_number()).over(_w))" in code
    assert "df_Others = df_Split__rest.unionByName(df_Split__noRegion, allowMissingColumns=True)" in code
    assert "df_Others.write.mode('append').parquet('abfss://data@acct.dfs.core.windows.net/other')" in code  # no truncate: appended
    assert tr.cells[0]["metadata"] == {"tags": ["parameters"]} and "minAmount = 10" in "".join(tr.cells[0]["source"])


@pytest.mark.parametrize("step,needle", [
    ("Sales pivot(groupBy(Product), pivotBy(Region), Total = sum(Amount)) ~> P", "transformation 'pivot'"),
    ("Sales derive(each(match(true()), $$ = upper($$))) ~> D", "column patterns"),
    ("Sales derive(x = someNewFunction(Amount)) ~> D", "function someNewFunction()"),
    ("Sales alterRow(upsertIf(true())) ~> A", "transformation 'alterRow'"),
])
def test_what_the_rules_do_not_map_is_named_and_raises_never_guessed(step, needle):
    tr, code = _code("source(allowSchemaDrift: true) ~> Sales\n" + step, {"Sales": "DS_Customers"})
    assert any(needle in u for u in tr.untranslated), tr.untranslated
    assert "raise NotImplementedError(" in code


def test_a_sink_with_row_level_writes_and_an_inline_dataset_are_named():
    tr, _ = _code("source(allowSchemaDrift: true) ~> Sales\nSales sink(upsertable: true) ~> S\nSales sink(format: 'delta') ~> T",
                  {"Sales": "DS_Customers", "S": "DS_Out"})
    assert any("upsertable: true" in u for u in tr.untranslated) and any("names no dataset" in u for u in tr.untranslated)


def test_expressions():
    def e(text):
        return DF.expr(DF._Parser(DF.tokenize(text)).expr())

    assert e("toInteger(Amount) * 2") == "(F.col('Amount').cast('int') * F.lit(2))"
    assert e("iif(isNull(x), 'none', upper(x))") == "F.when(F.col('x').isNull(), F.lit('none')).otherwise(F.upper(F.col('x')))"
    assert e("!(a == 1) && b != 'z'") == "((~(F.col('a') == F.lit(1))) & (F.col('b') != F.lit('z')))"
    assert e("{order date} > toDate('2024-01-01')") == "(F.col('order date') > F.to_date(F.lit('2024-01-01')))"
    assert e("concat(first, ' ', last)") == "F.concat(F.col('first'), F.lit(' '), F.col('last'))"
    assert e("'id-' + toString(id)") == "F.concat(F.lit('id-'), F.col('id').cast('string'))"
    assert e("substring(code, 1, 3)") == "F.col('code').substr(F.lit(1), F.lit(3))"
    assert e("in(['a','b'], kind)") == "F.col('kind').isin(F.lit('a'), F.lit('b'))"
    assert e("x === null()") == "(F.col('x')).eqNullSafe(F.lit(None))"
