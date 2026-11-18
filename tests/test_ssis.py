"""SSIS package -> Fabric notebook by fixed rules (agent/workloads/ssis.py and ssis_expr.py, 2026-11-18).

The fixture tests/fixtures/ssis/orders_etl.dtsx is written in the format SQL Server Data Tools produces (its structure follows real packages from public
repositories, which the translator was also run against: see docs/workload-migration.md). The smaller packages below are built here, one rule each.
No Spark runs here (none is installed): the generated code is checked to parse and to say exactly what it should; the Fabric run of the generated
notebook is the proof (docs/workload-migration.md, run live 2026-11-18)."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from adf2fabric.agent.workloads import ssis, ssis_expr as X

FIXTURE = Path(__file__).parent / "fixtures" / "ssis" / "orders_etl.dtsx"
NS = 'xmlns:DTS="www.microsoft.com/SqlServer/Dts"'


def _code(tr):
    code = "\n".join("".join(c["source"]) for c in tr.cells)
    ast.parse(code)  # always valid Python, even where a step is not translated
    return code


def pkg(body: str = "", variables: str = "", cms: str = "", constraints: str = "", params: str = "") -> str:
    return (f'<?xml version="1.0"?>\n<DTS:Executable {NS} DTS:refId="Package" DTS:CreationName="Microsoft.Package" DTS:ExecutableType="Microsoft.Package" '
            f'DTS:ObjectName="P">\n<DTS:ConnectionManagers>{cms}</DTS:ConnectionManagers>\n<DTS:Variables>{variables}</DTS:Variables>\n{params}'
            f'<DTS:Executables>{body}</DTS:Executables>\n<DTS:PrecedenceConstraints>{constraints}</DTS:PrecedenceConstraints>\n</DTS:Executable>')


def var(name, dtype, value="", expr=""):
    e = f' DTS:Expression="{expr}"' if expr else ""
    return f'<DTS:Variable DTS:Namespace="User" DTS:ObjectName="{name}"{e}><DTS:VariableValue DTS:DataType="{dtype}">{value}</DTS:VariableValue></DTS:Variable>'


def sql(name, stmt, conn="{CM-SQL}", extra=""):
    return (f'<DTS:Executable DTS:refId="Package\\{name}" DTS:ExecutableType="Microsoft.ExecuteSQLTask" DTS:ObjectName="{name}"><DTS:ObjectData>'
            f'<SQLTask:SqlTaskData xmlns:SQLTask="www.microsoft.com/sqlserver/dts/tasks/sqltask" SQLTask:Connection="{conn}" SQLTask:SqlStatementSource="{stmt}">{extra}'
            f'</SQLTask:SqlTaskData></DTS:ObjectData></DTS:Executable>')


SQL_CM = ('<DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[Db]" DTS:CreationName="OLEDB" DTS:DTSID="{CM-SQL}" DTS:ObjectName="Db"><DTS:ObjectData>'
          '<DTS:ConnectionManager DTS:ConnectionString="Data Source=srv1;Initial Catalog=sales;Provider=SQLNCLI11.1;"/></DTS:ObjectData></DTS:ConnectionManager>')


def con(frm, to, value="0", expr="", logical="True"):
    e = f' DTS:Expression="{expr}" DTS:EvalOp="3"' if expr else ""
    return (f'<DTS:PrecedenceConstraint DTS:From="Package\\{frm}" DTS:To="Package\\{to}" DTS:Value="{value}" DTS:LogicalAnd="{logical}"{e}/>')


# ---- the fixture package -------------------------------------------------------------------------------------------------------------------
def test_the_fixture_package_is_translated_end_to_end():
    tr = ssis.translate(FIXTURE.read_text(encoding="utf-8"), "Files/ssis/")
    code = _code(tr)
    assert tr.untranslated == [] and tr.drafted == []
    assert "def dataflow_1():" in code and "\ndataflow_1()\n" in code
    assert ".schema(StructType([StructField('OrderId', IntegerType(), True), StructField('Region', StringType(), True)" in code
    assert ".withColumn('Total', (F.col('Quantity') * F.col('UnitPrice')).cast('double'))" in code and ".withColumn('RegionUpper', F.upper(F.col('Region')))" in code
    assert "df_Split_by_size__Big = df_Add_total.filter(F.coalesce((F.col('Total') >= F.lit(v_Threshold)), F.lit(False)))" in code
    assert "__Conditional_Split_Default_Output = df_Add_total.filter(~(F.coalesce((F.col('Total') >= F.lit(v_Threshold)), F.lit(False))))" in code
    assert ".groupBy(F.col('Region').alias('Region')).agg(F.sum(F.col('Total')).alias('BigTotal'), F.count(F.lit(1)).alias('Orders'))" in code
    assert ".orderBy(F.col('BigTotal').desc(), F.col('Region').asc())" in code  # a negative key position is descending; positions order the keys
    assert "_write_one(" in code and "_p(cm_Big_csv)" in code and "_p(cm_Small_csv)" in code
    assert "def _write_one" in code and "def _jdbc" not in code  # only the helpers the package needs


def test_parameters_cover_variables_and_connection_strings_and_the_root():
    tr = ssis.translate(FIXTURE.read_text(encoding="utf-8"), "Files/ssis/")
    names = [n for n, _ in tr.parameters]
    assert names == ["v_Threshold", "cm_Orders_csv", "cm_Big_csv", "cm_Small_csv", "ssis_root"]
    assert tr.param_map["User::Threshold"] == "v_Threshold" and tr.param_map["Orders.csv"] == "cm_Orders_csv"
    assert tr.cells[0]["metadata"] == {"tags": ["parameters"]}
    assert any("ssis_root" in n for n in tr.notes)  # the review item about Windows paths


def test_a_windows_path_is_read_under_the_root_with_the_drive_dropped():
    code = _code(ssis.translate(FIXTURE.read_text(encoding="utf-8"), "Files/ssis/"))
    fn = re.search(r"def _p\(path\):.*?\n    return [^\n]+\n", code, re.S).group(0)
    ns = {"re": re, "ssis_root": "Files/ssis/"}
    exec(fn, ns)  # noqa: S102 - the generated helper, on its own
    p = ns["_p"]
    assert p("C:\\data\\ssis\\orders.csv") == "Files/ssis/data/ssis/orders.csv"
    assert p("\\\\srv\\share\\in\\x.txt") == "Files/ssis/in/x.txt"
    assert p("abfss://c@a.dfs.core.windows.net/x.csv") == "abfss://c@a.dfs.core.windows.net/x.csv"


# ---- refusals ------------------------------------------------------------------------------------------------------------------------------
def test_a_document_with_an_entity_or_that_is_not_a_package_is_refused():
    with pytest.raises(ssis.Untranslated, match="DOCTYPE or an ENTITY"):
        ssis.translate('<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "b">]><DTS:Executable ' + NS + "/>")
    with pytest.raises(ssis.Untranslated, match="not an SSIS package"):
        ssis.translate("<root/>")
    with pytest.raises(ssis.Untranslated, match="not valid XML"):
        ssis.translate("<a><b></a>")


# ---- control flow --------------------------------------------------------------------------------------------------------------------------
def test_execute_sql_runs_through_jdbc_and_precedence_orders_the_tasks():
    xml = pkg(sql("Second", "update t set a = 1;") + sql("First", "truncate table t;"), cms=SQL_CM, constraints=con("First", "Second"))
    tr = ssis.translate(xml)
    code = _code(tr)
    assert tr.untranslated == []
    assert code.index("truncate table t;") < code.index("update t set a = 1;")  # listed second, runs first: the constraint says so
    assert "_sql_exec(cm_Db, 'truncate table t;', cm_user, cm_password)" in code and "def _sql_exec" in code
    assert ("cm_user", "''") in tr.parameters and ("cm_password", "''") in tr.parameters  # never stored
    assert any("Windows authentication" in n for n in tr.notes)


def test_an_execute_sql_with_bindings_a_failure_constraint_or_a_script_task_is_named_never_guessed():
    bound = sql("Q", "select 1", extra='<SQLTask:ParameterBinding xmlns:SQLTask="x" SQLTask:ParameterName="0"/>')
    script = '<DTS:Executable DTS:refId="Package\\Show" DTS:ExecutableType="Microsoft.ScriptTask" DTS:ObjectName="Show"/>'
    on_fail = sql("Cleanup", "delete from t")
    xml = pkg(bound + script + sql("Main", "select 2") + on_fail, cms=SQL_CM, constraints=con("Main", "Cleanup", value="1"))
    tr = ssis.translate(xml)
    code = _code(tr)
    why = dict(u.split(": ", 1) for u in tr.untranslated)
    assert "binds parameters or a result set" in why[r"Package\Q"]
    assert "Microsoft.ScriptTask" in why[r"Package\Show"]
    assert "failure precedence constraint" in why[r"Package\Cleanup"]
    assert code.count("raise NotImplementedError(") == 3


def test_an_expression_constraint_becomes_a_condition_and_an_expression_task_assigns_a_variable():
    task = ('<DTS:Executable DTS:refId="Package\\Set" DTS:ExecutableType="Microsoft.ExpressionTask" DTS:ObjectName="Set"><DTS:ObjectData>'
            '<ExpressionTask Expression="@[User::n] = @[User::n] + 1"/></DTS:ObjectData></DTS:Executable>')
    xml = pkg(task + sql("Run", "exec p"), variables=var("n", "3", "1"), cms=SQL_CM, constraints=con("Set", "Run", expr="@[User::n] &gt; 1"))
    tr = ssis.translate(xml)
    code = _code(tr)
    assert tr.untranslated == []
    assert "v_n = (v_n + 1)" in code and "if (v_n > 1):\n    _sql_exec(cm_Db, 'exec p'" in code


def test_a_variable_expression_is_computed_and_a_literal_one_is_a_parameter():
    xml = pkg(variables=var("y", "3", "2026") + var("label", "8", "", '(DT_WSTR, 4) @[User::y] + &quot;-12-25&quot;') + var("d", "7", "2026-12-25"))
    tr = ssis.translate(xml)
    code = _code(tr)
    assert [n for n, _ in tr.parameters if n.startswith("v_")] == ["v_y", "v_d"]  # v_label is computed
    assert "v_label = (str(str(v_y)) + str('-12-25'))" in code
    assert "v_y = int(v_y)" in code and "v_d = _to_dt(v_d)" in code and "def _to_dt" in code


def test_a_modern_foreach_loop_and_a_for_loop_nest_their_tasks():
    foreach = ('<DTS:Executable DTS:refId="Package\\Each" DTS:ExecutableType="Microsoft.ForEachLoop" DTS:ObjectName="Each">'
               '<DTS:Property DTS:Name="Enumerator">ForeachFile</DTS:Property><DTS:Property DTS:Name="DirectoryExpression">@[User::land]</DTS:Property>'
               '<DTS:Property DTS:Name="FileSpec">sales_*.csv</DTS:Property><DTS:Property DTS:Name="VariableName">User::cur</DTS:Property>'
               '<DTS:Executables>' + sql("Load", "exec load") + '</DTS:Executables></DTS:Executable>')
    forloop = ('<DTS:Executable DTS:refId="Package\\Loop" DTS:ExecutableType="STOCK:FORLOOP" DTS:ObjectName="Loop" DTS:InitExpression="@[User::i] = 0" '
               'DTS:EvalExpression="@[User::i] &lt; 3" DTS:AssignExpression="@[User::i] = @[User::i] + 1"><DTS:Executables>' + sql("Tick", "exec tick") + "</DTS:Executables></DTS:Executable>")
    tr = ssis.translate(pkg(foreach + forloop, variables=var("land", "8", "C:\\\\in") + var("cur", "8") + var("i", "3", "0"), cms=SQL_CM))
    code = _code(tr)
    assert tr.untranslated == []
    assert "for v_cur in _ssis_files(_p(v_land), 'sales_*.csv', False, 0):\n    # --- sql: Load\n    _sql_exec(cm_Db, 'exec load'" in code
    assert "v_i = 0\nwhile (v_i < 3):" in code and "    v_i = (v_i + 1)" in code and "def _ssis_files" in code


def test_another_enumerator_is_named():
    each = ('<DTS:Executable DTS:refId="Package\\E" DTS:ExecutableType="Microsoft.ForEachLoop" DTS:ObjectName="E"><DTS:Property DTS:Name="Enumerator">ForeachADO</DTS:Property></DTS:Executable>')
    tr = ssis.translate(pkg(each))
    assert any("ForeachADO" in u and "only the file enumerator" in u for u in tr.untranslated)


# ---- connection managers -------------------------------------------------------------------------------------------------------------------
def test_a_project_connection_manager_is_named_and_a_conmgr_file_resolves_it():
    task = sql("Q", "select 1", conn="{ABC00000-0000-4000-8000-000000000001}")
    tr = ssis.translate(pkg(task))
    assert any("project connection manager" in u and ".conmgr" in u for u in tr.untranslated)
    conmgr = ('<DTS:ConnectionManager ' + NS + ' DTS:refId="ConnectionManagers[Proj]" DTS:CreationName="OLEDB" DTS:DTSID="{ABC00000-0000-4000-8000-000000000001}" DTS:ObjectName="Proj"><DTS:ObjectData>'
              '<DTS:ConnectionManager DTS:ConnectionString="Data Source=srv2;Initial Catalog=dw;"/></DTS:ObjectData></DTS:ConnectionManager>')
    tr2 = ssis.translate(pkg(task), conmgrs=(conmgr,))
    assert tr2.untranslated == [] and "_sql_exec(cm_Proj, 'select 1'" in _code(tr2)


# ---- model drafts --------------------------------------------------------------------------------------------------------------------------
def test_an_accepted_draft_replaces_the_raising_cell_and_is_marked():
    script = '<DTS:Executable DTS:refId="Package\\Show" DTS:ExecutableType="Microsoft.ScriptTask" DTS:ObjectName="Show"/>'
    xml = pkg(script)
    assert [u.split(":")[0] for u in ssis.translate(xml).untranslated] == [r"Package\Show"]
    tr = ssis.translate(xml, drafts={r"Package\Show": {"code": "print(v_x)", "accepted_by": "reviewer", "accepted_at": "2026-11-18"}})
    code = _code(tr)
    assert tr.untranslated == [] and tr.drafted == [r"Package\Show"]
    assert "MODEL-DRAFTED" in code and "accepted by reviewer" in code and "print(v_x)" in code and "SSIS step not translated" not in code


def test_a_component_key_names_the_data_flow_and_the_component_and_carries_what_a_draft_needs():
    xml = FIXTURE.read_text(encoding="utf-8").replace('contactInfo="Sort;', 'contactInfo="Fuzzy Grouping;')
    tr = ssis.translate(xml)
    key = r"Package\Process orders~Order regions"
    assert any(u.startswith(key + ": the component type 'Fuzzy Grouping' is not translated") for u in tr.untranslated)
    ctx = tr.context[key]
    assert ctx["output"] == "df_Order_regions" and "df_Totals_by_region__Aggregate_Output_1" in ctx["dataframes"]
    code = _code(tr)
    assert "df_Order_regions" not in code.split("raise NotImplementedError")[0].split("# --- Fuzzy Grouping")[-1]  # nothing is invented for it


# ---- the activity's parameters -------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("name,expected", [("Region", "p_Region"), ("my param", "p_my_param")])
def test_parameter_names(name, expected):
    assert ssis.package_param(name) == expected


@pytest.mark.parametrize("key,expected", [
    (r"\Package.Variables[User::Threshold].Value", "v_Threshold"), ("Package.Variables[User::Threshold].Value", "v_Threshold"),
    (r"\Package.MaxConcurrentExecutables", None)])
def test_a_property_override_that_sets_a_variable_maps_to_its_parameter(key, expected):
    assert ssis.variable_param(key) == expected


def test_connection_manager_overrides():
    assert ssis.cm_param("Orders.csv", "ConnectionString") == "cm_Orders_csv" and ssis.cm_param("Db", "userName") == "cm_user"
    assert ssis.cm_param("Db", "passWord") is None


def test_the_package_location_of_an_activity():
    assert ssis.package_location({"packageLocation": {"type": "File", "typeProperties": {"packagePath": "\\\\share\\x.dtsx"}}}) == ("File", "\\\\share\\x.dtsx")
    assert ssis.package_location({"packageLocation": {"type": "SSISDB", "packagePath": "f/p/x.dtsx"}}) == ("SSISDB", "f/p/x.dtsx")
    assert ssis.package_location({}) == ("", "")


# ---- expressions ---------------------------------------------------------------------------------------------------------------------------
ENV = X.Env(cols={"City": "str", "Amount": "num", "Name": "str"}, vars={"User::n": ("v_n", "num"), "User::s": ("v_s", "str"), "User::d": ("v_d", "date")})


@pytest.mark.parametrize("text,expected", [
    ('City == "London"', "(F.col('City') == F.lit('London'))"),
    ('Amount > 10 && Name != "x"', "((F.col('Amount') > F.lit(10)) & (F.col('Name') != F.lit('x')))"),
    ('UPPER(Name) + "-" + City', "F.concat(F.concat(F.upper(F.col('Name')), F.lit('-')), F.col('City'))"),
    ('ISNULL(City) ? "n/a" : City', "F.when(F.col('City').isNull(), F.lit('n/a')).otherwise(F.col('City'))"),
    ('REPLACE(Name, "a", "b")', "F.regexp_replace(F.col('Name'), 'a', 'b')"),
    ('(DT_STR,50,936)REPLACE([Name],"Cole","W")', "F.regexp_replace(F.col('Name'), 'Cole', 'W').cast('string')"),
    ("FINDSTRING(Name, \"x\", 1) > 0", "(F.locate('x', F.col('Name')) > F.lit(0))"),
    ("Amount > @[User::n]", "(F.col('Amount') > F.lit(v_n))"),
])
def test_column_expressions(text, expected):
    assert X.column_expr(text, ENV)[0] == expected


@pytest.mark.parametrize("text,expected", [
    ('(DT_WSTR, 4) @[User::n] + "-12-25"', "(str(str(v_n)) + str('-12-25'))"),
    ('YEAR( @[User::d] )', "v_d.year"),
    ('DATEPART("dw", @[User::d]) == 6', "(((v_d.isoweekday() % 7) + 1) == 6)"),
    ('@[User::n] > 1 ? "big" : "small"', "('big' if (v_n > 1) else 'small')"),
    ('(DT_DBTIMESTAMP) @[User::s]', "_to_dt(v_s)"),
])
def test_python_expressions(text, expected):
    assert X.python_expr(text, ENV)[0] == expected


@pytest.mark.parametrize("text,needle", [
    ("MYFUNC(City)", "function MYFUNC()"), ("Foo == 1", "column 'Foo' is not in the data flow's input"), ("Amount > ", "ends too early"),
    ("(DT_GUID) Name", "cast to DT_GUID"), ('REPLACE(Name, City, "b")', "computed search or replacement"), ("@[User::nope] == 1", "not declared")])
def test_what_the_expression_rules_do_not_cover_is_named(text, needle):
    with pytest.raises(X.Untranslated, match=re.escape(needle)):
        X.column_expr(text, ENV)


def test_a_column_has_no_row_in_a_variable_expression():
    with pytest.raises(X.Untranslated, match="no row"):
        X.python_expr("City == 1", ENV)


def test_an_assignment_names_its_variable():
    assert X.assignment('@[User::n] = 1 + 2', ENV) == ("v_n", "(1 + 2)")
    with pytest.raises(X.Untranslated, match="not '@\\[User::name\\] = expression'"):
        X.assignment("1 + 2", ENV)
