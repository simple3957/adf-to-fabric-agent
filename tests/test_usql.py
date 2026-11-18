"""U-SQL script -> PySpark notebook by fixed rules (agent/workloads/usql.py, 2026-11-17).

The first script is the real test script of the factory adf2fabricv1 (sales_by_product.usql, run by the activity USQL_Sales_By_Product).
No Spark runs here (none is installed): the generated code is checked to parse and to say exactly what it should; the Fabric run of the
generated notebook is the proof that it computes the same rows."""

from __future__ import annotations

import ast

import pytest

from adf2fabric.agent.workloads import usql

ROOT = "abfss://migrationcontainer@acct.dfs.core.windows.net/"
REAL = '''// Same result as the data flow DF_Sales_By_Product: positive amounts, banded, totalled per product and band.
// @in and @out are passed by the ADF activity (parameters "in" and "out").
DECLARE EXTERNAL @in string = "/emp.csv";
DECLARE EXTERNAL @out string = "/migration-test/usql/sales_by_product/sales_by_product.csv";

@sales =
    EXTRACT SaleId string,
            CustomerId string,
            Product string,
            Amount long
    FROM @in
    USING Extractors.Csv(skipFirstNRows : 1);

@banded =
    SELECT Product,
           (Amount >= 50000 ? "high" : "low") AS AmountBand,
           Amount
    FROM @sales
    WHERE Amount > 0;

@byProduct =
    SELECT Product,
           AmountBand,
           SUM(Amount) AS TotalAmount,
           COUNT(*) AS Sales
    FROM @banded
    GROUP BY Product, AmountBand;

OUTPUT @byProduct
TO @out
USING Outputters.Csv(outputHeader : true);
'''
PREFIX = "DECLARE EXTERNAL @in string = \"/a.csv\";\n@t = EXTRACT Id int, Name string, Amount long FROM @in USING Extractors.Csv(skipFirstNRows : 1);\n"


def _code(script):
    tr = usql.translate(script, ROOT)
    code = "\n".join("".join(c["source"]) for c in tr.cells)
    ast.parse(code)  # always valid Python, even where a statement is not translated
    return tr, code


def test_the_real_script_is_translated_with_the_parameters_the_activity_passes():
    tr, code = _code(REAL)
    assert tr.untranslated == []
    assert tr.param_map == {"in": "in_", "out": "out"}  # `in` is a Python keyword: the notebook parameter is in_
    assert tr.cells[0]["metadata"] == {"tags": ["parameters"]} and "in_ = '/emp.csv'" in "".join(tr.cells[0]["source"])
    assert ".option('header', True).option('sep', ',').option('mode', 'FAILFAST')" in code  # skipFirstNRows:1, a bad row fails as in U-SQL
    assert "StructField('Amount', LongType(), True)" in code and ".csv(_p(in_))" in code
    assert ".filter((F.col('Amount') > F.lit(0)))" in code
    assert "F.when((F.col('Amount') >= F.lit(50000)), F.lit('high')).otherwise(F.lit('low'))).alias('AmountBand')" in code
    assert ".groupBy(F.col('Product'), F.col('AmountBand')).agg(F.sum(F.col('Amount')).alias('TotalAmount'), F.count(F.lit(1)).alias('Sales'))" in code
    assert "coalesce(1).write.mode('overwrite').option('header', True)" in code and "notebookutils.fs.mv(_part, _out)" in code
    assert "if notebookutils.fs.exists(_out):  # rm raises" in code and "    notebookutils.fs.rm(_out, True)" in code  # rm raises on a missing path (live run)
    assert f"_root = {ROOT!r}" in code and any("default store" in n for n in tr.notes)


@pytest.mark.parametrize("select,needle", [
    ("SELECT Id, Name FROM @t WHERE Amount > 10 && Name != \"x\"", ".filter(((F.col('Amount') > F.lit(10)) & (F.col('Name') != F.lit('x'))))"),
    ("SELECT Id, Name ?? \"none\" AS N FROM @t", "F.coalesce(F.col('Name'), F.lit('none')).alias('N')"),
    ("SELECT Id FROM @t WHERE Name == null", ".filter(F.col('Name').isNull())"),
    ("SELECT Id FROM @t WHERE Name IS NOT NULL AND Id IN (1, 2)", "F.col('Name').isNotNull() & F.col('Id').isin(1, 2)"),
    ("SELECT Name.ToUpper() AS U, Name.Length AS L FROM @t", "F.upper(F.col('Name')).alias('U'), F.length(F.col('Name')).alias('L')"),
    ("SELECT Id FROM @t WHERE Name.Contains(\"a\")", ".filter(F.col('Name').contains(F.lit('a')))"),
    ("SELECT DISTINCT Name FROM @t ORDER BY Name DESC FETCH 5 ROWS", ".distinct()"),
    ("SELECT Name, SUM(Amount) AS S FROM @t GROUP BY Name HAVING S > 5", ".filter((F.col('S') > F.lit(5)))"),
    ("SELECT Name + \"-x\" AS N FROM @t", "F.concat(F.col('Name'), F.lit('-x')).alias('N')"),  # a string column's '+' concatenates
])
def test_rowset_operations_become_dataframe_operations(select, needle):
    tr, code = _code(PREFIX + "@r = " + select + ";")
    assert tr.untranslated == [], tr.untranslated
    assert needle in code


def test_order_by_and_fetch_become_orderby_and_limit():
    _, code = _code(PREFIX + "@r = SELECT DISTINCT Name FROM @t ORDER BY Name DESC FETCH 5 ROWS;")
    assert ".orderBy(F.col('Name').desc())" in code and ".limit(5)" in code


def test_a_join_keeps_both_aliases_and_the_join_type():
    script = PREFIX + "@u = EXTRACT Id int, City string FROM \"/c.csv\" USING Extractors.Csv();\n" \
                      "@j = SELECT a.Name, b.City FROM @t AS a LEFT OUTER JOIN @u AS b ON a.Id == b.Id;"
    tr, code = _code(script)
    assert tr.untranslated == []
    assert "df___t.alias('a').join(df___u.alias('b'), (F.col('a.Id') == F.col('b.Id')), 'left')" in code
    assert ".select(F.col('a.Name').alias('Name'), F.col('b.City').alias('City'))" in code


@pytest.mark.parametrize("statement,needle", [
    ("@r = PROCESS @t PRODUCE Id int USING new MyProcessor();", "PROCESS"),
    ("@r = REDUCE @t ON Id PRODUCE Id int USING new MyReducer();", "REDUCE"),
    ("@r = SELECT Id, DateTime.Now AS N FROM @t;", "DateTime"),
    ("@r = SELECT Id, ROW_NUMBER() OVER (PARTITION BY Name) AS rn FROM @t;", "without ORDER BY"),
    ("@r = SELECT Id, NTILE(4) OVER (ORDER BY Id) AS q FROM @t;", "NTILE"),
    ("@r = SELECT MyFunc(Id) AS X FROM @t;", "MyFunc"),
    ("@r = SELECT Id AS a, Id FROM @t GROUP BY Name;", "neither an aggregate nor a GROUP BY column"),
    ("@r = SELECT Id + 1 FROM @t;", "AS <name>"),
    ("CREATE TABLE IF NOT EXISTS db.t (Id int);", "CREATE"),
    ("@x = EXTRACT A string FROM \"/x.dat\" USING new MyExtractor();", "user-defined"),
    ("@x = EXTRACT A string FROM \"/x.dat\" USING Extractors.Parquet();", "Parquet"),
])
def test_what_the_rules_do_not_map_is_named_and_raises_never_guessed(statement, needle):
    tr, code = _code(PREFIX + statement)
    assert any(needle in u for u in tr.untranslated), tr.untranslated
    assert "raise NotImplementedError(" in code


def test_a_statement_that_fails_leaves_the_others_translated():
    tr, code = _code(PREFIX + "@bad = PROCESS @t PRODUCE Id int USING new P();\n@ok = SELECT Id FROM @t;")
    assert len(tr.untranslated) == 1 and "df___ok = (df___t" in code


@pytest.mark.parametrize("name,expected", [("in", "in_"), ("out", "out"), ("class", "class_"), ("@from", "from_"), ("spark", "spark_")])
def test_a_parameter_that_is_not_a_valid_variable_gets_a_trailing_underscore(name, expected):
    assert usql.param_name(name) == expected


def test_typed_parameters_are_cast_in_the_notebook():
    _, code = _code("DECLARE EXTERNAL @n int = 5;\nDECLARE EXTERNAL @on bool = true;\n" + PREFIX.split("\n", 1)[1])
    assert "n = int(n)" in code and "on = (str(on).lower() == 'true')" in code



def test_window_functions_become_window_specs():
    tr, code = _code(PREFIX + "@r = SELECT Id, ROW_NUMBER() OVER (PARTITION BY Name ORDER BY Amount DESC) AS rn, "
                              "SUM(Amount) OVER (PARTITION BY Name) AS total, COUNT(*) OVER (PARTITION BY Name) AS n FROM @t;")
    assert tr.untranslated == []
    assert "F.row_number().over(Window.partitionBy(F.col('Name')).orderBy(F.col('Amount').desc()))" in code
    assert "F.sum(F.col('Amount')).over(Window.partitionBy(F.col('Name')))" in code and "F.count(F.lit(1)).over(" in code
    assert "from pyspark.sql import Window" in code


def test_union_all_and_distinct_union_by_name():
    tr, code = _code(PREFIX + "@a = SELECT Id, Name FROM @t WHERE Id > 1;\n@u = SELECT Id, Name FROM @t UNION ALL SELECT Id, Name FROM @a;\n"
                              "@d = SELECT Id, Name FROM @t UNION SELECT Id, Name FROM @a;")
    assert tr.untranslated == []
    assert "df___u = df___u__u0.unionByName(df___u__u1)\n" in code
    assert "df___d = df___d__u0.unionByName(df___d__u1).distinct()" in code


def test_a_union_of_selects_with_different_widths_is_named():
    tr, _ = _code(PREFIX + "@u = SELECT Id FROM @t UNION ALL SELECT Id, Name FROM @t;")
    assert any("different number of columns" in u for u in tr.untranslated)


def test_statement_keys_name_each_untranslated_statement_and_carry_what_a_draft_needs():
    tr, _ = _code(PREFIX + "@bad = PROCESS @t PRODUCE Id int USING new P();\n@ok = SELECT Id FROM @t;\nOUTPUT @ok TO \"/o.csv\" USING Outputters.Csv();")
    assert [u.split(":")[0] for u in tr.untranslated] == ["@bad"]
    assert tr.context["@bad"]["rowsets"] == {"df___t": ["Id", "Name", "Amount"]} and "PROCESS" in tr.context["@bad"]["text"]
    assert "OUTPUT#1" in tr.context


def test_an_accepted_draft_replaces_the_raising_cell_and_is_marked():
    draft = {"@bad": {"code": "df___bad = df___t.withColumn('X', F.lit(1))", "accepted_by": "reviewer", "accepted_at": "2026-11-18"}}
    script = PREFIX + "@bad = PROCESS @t PRODUCE Id int USING new P();\n@ok = SELECT Id FROM @bad;"
    tr = usql.translate(script, ROOT, draft)
    code = "\n".join("".join(c["source"]) for c in tr.cells)
    ast.parse(code)
    assert tr.untranslated == [] and tr.drafted == ["@bad"]
    assert "MODEL-DRAFTED" in code and "accepted by reviewer" in code and "df___bad = df___t.withColumn('X', F.lit(1))" in code
    assert "U-SQL statement not translated" not in code
