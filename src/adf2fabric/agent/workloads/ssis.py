"""SSIS package (.dtsx) -> a Fabric notebook, by fixed rules (2026-11-18).

Why a notebook: Fabric's own "Invoke SSIS package" activity has no documented JSON (the user's decision, 2026-11-18: translate the package
instead). The package is read as XML (the format SQL Server Data Tools writes, ``xmlns:DTS="www.microsoft.com/SqlServer/Dts"``; checked against
real packages from public repositories, see docs/workload-migration.md) and the control flow and data flows are rebuilt as Python / PySpark.

Translated:

* control flow: Sequence container, Foreach Loop over files (``Folder`` / ``FileSpec`` / ``Recurse``, the file name into a variable), For Loop,
  Execute SQL Task (a direct statement, no result or parameter bindings), Expression Task, precedence constraints (success / completion order, and
  an expression as a condition; a *failure* constraint is not translated);
* data flow: Flat File Source and Destination, OLE DB Source (table or query) and Destination (table), Derived Column, Conditional Split,
  Aggregate, Sort, Union All, Lookup (OLE DB reference query), Multicast, Data Conversion, Row Count;
* variables (literal or with an expression), package parameters, connection managers (each connection string becomes a notebook parameter);
* expressions: ssis_expr.py.

Not translated, never guessed: Script Task and Script Component, Execute Process / Package / Web Service / FTP / Send Mail / File System tasks,
other enumerators, Merge Join, Fuzzy components, Excel and other sources and destinations, and anything a rule above does not cover. Each is a
named finding and a cell that raises, so the package still has a notebook; a person can accept a model draft for it (assist/script_steps.py).

Paths in a package are paths on the machine that ran it (``C:\\Data\\x.csv``): the notebook reads them under the root parameter ``ssis_root``,
keeping the folders and dropping the drive, and that is a review finding. SQL (OLE DB) connections go through JDBC with the connection string
turned into a URL; the user and password are notebook parameters (never stored); Windows authentication has no equivalent.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any, Optional

from adf2fabric.agent.workloads import ssis_expr as X
from adf2fabric.agent.workloads.dataflow import _code_cell


class Untranslated(Exception):
    pass


# ---------------------------------------------------------------------------------------------------------------------------------------------
# reading the XML
# ---------------------------------------------------------------------------------------------------------------------------------------------
def _local(tag: str) -> str:
    return tag.split("}")[-1]


def _a(e: ET.Element, name: str, default: str = "") -> str:
    for k, v in e.attrib.items():
        if _local(k) == name:
            return v
    return default


def _kids(e: ET.Element, name: str) -> list[ET.Element]:
    return [c for c in e if _local(c.tag) == name]


def _kid(e: ET.Element, name: str) -> Optional[ET.Element]:
    return next((c for c in e if _local(c.tag) == name), None)


def _unesc(s: str) -> str:
    """``_x0009_`` -> a tab (SSIS escapes control characters in attribute text)."""
    return re.sub(r"_x([0-9A-Fa-f]{4})_", lambda m: chr(int(m.group(1), 16)), s)


def _ident(s: str) -> str:
    out = re.sub(r"\W", "_", s)
    return out if out and not out[0].isdigit() else "_" + out


_VAR_TYPES = {"2": "num", "3": "num", "4": "num", "5": "num", "6": "num", "7": "date", "8": "str", "11": "bool", "14": "num", "16": "num", "17": "num",
              "18": "num", "19": "num", "20": "num", "21": "num", "72": "str", "129": "str", "130": "str", "131": "num", "133": "date", "135": "date"}
_FF_TYPES = {"2": "smallint", "3": "int", "4": "float", "5": "double", "6": "decimal(19,4)", "7": "timestamp", "8": "string", "11": "boolean",
             "14": "decimal(38,10)", "16": "tinyint", "17": "smallint", "18": "int", "19": "bigint", "20": "bigint", "21": "decimal(20,0)",
             "72": "string", "129": "string", "130": "string", "131": "decimal(38,10)", "133": "date", "134": "string", "135": "timestamp"}
_DF_TYPES = {"wstr": "str", "str": "str", "text": "str", "ntext": "str", "guid": "str", "bool": "bool", "dbdate": "date", "dbtimestamp": "date",
             "date": "date", "dbtime": "str"}
_DF_SPARK = {"wstr": "string", "str": "string", "i1": "tinyint", "i2": "smallint", "i4": "int", "i8": "bigint", "ui1": "smallint", "ui2": "int",
             "ui4": "bigint", "ui8": "decimal(20,0)", "r4": "float", "r8": "double", "bool": "boolean", "dbdate": "date", "dbtimestamp": "timestamp",
             "date": "timestamp", "cy": "decimal(19,4)", "guid": "string"}
_CODEPAGES = {"1252": "windows-1252", "65001": "UTF-8", "936": "GBK", "1200": "UTF-16", "1250": "windows-1250", "1251": "windows-1251", "28591": "ISO-8859-1"}


@dataclass
class Var:
    ns: str
    name: str
    dtype: str
    value: str
    expr: str
    py: str

    @property
    def ref(self) -> str:
        return f"{self.ns}::{self.name}"

    @property
    def kind(self) -> Optional[str]:
        return _VAR_TYPES.get(self.dtype)


@dataclass
class CM:
    id: str
    name: str
    kind: str  # OLEDB | FLATFILE | ADO.NET | EXCEL | FILE ...
    props: dict[str, str]
    columns: list[dict[str, str]]
    prop_exprs: dict[str, str]
    py: str


@dataclass
class Task:
    kind: str
    name: str
    path: str
    id: str
    el: ET.Element
    children: list["Task"] = field(default_factory=list)
    constraints: list[dict[str, str]] = field(default_factory=list)  # of this container: From, To (refIds), Value, Expression, EvalOp, LogicalAnd
    props: dict[str, str] = field(default_factory=dict)


@dataclass
class Package:
    name: str
    vars: list[Var]
    params: list[Var]
    cms: list[CM]
    root: Task


def _kind(et: str) -> str:
    low = et.lower()
    if low.endswith("pipeline") or low in ("ssis.pipeline.3",):
        return "dataflow"
    if "executesqltask" in low:
        return "sql"
    if "foreachloop" in low:
        return "foreach"
    if "forloop" in low:
        return "forloop"
    if "sequence" in low:
        return "sequence"
    if "expressiontask" in low:
        return "expression"
    if low.endswith("package") or low.startswith("ssis.package"):
        return "package"
    return "other"


def _task(e: ET.Element) -> Task:
    t = Task(_kind(_a(e, "ExecutableType")), _a(e, "ObjectName"), _a(e, "refId") or _a(e, "ObjectName"), _a(e, "DTSID"), e)
    holder = _kid(e, "Executables")
    t.children = [_task(c) for c in _kids(holder, "Executable")] if holder is not None else []
    pc = _kid(e, "PrecedenceConstraints")
    for c in (_kids(pc, "PrecedenceConstraint") if pc is not None else []):
        t.constraints.append({k: _a(c, k) for k in ("From", "To", "Value", "Expression", "EvalOp", "LogicalAnd", "ObjectName")})
    return t


def _cm(cm: ET.Element) -> CM:
    od = _kid(cm, "ObjectData")
    inner = next((c for c in (od if od is not None else []) if _local(c.tag) == "ConnectionManager"), None)
    props = {_local(k): _unesc(v) for k, v in (inner.attrib.items() if inner is not None else [])}
    cols = []
    fc = _kid(inner, "FlatFileColumns") if inner is not None else None
    for col in (_kids(fc, "FlatFileColumn") if fc is not None else []):
        cols.append({"name": _a(col, "ObjectName"), "type": _a(col, "DataType"), "delim": _unesc(_a(col, "ColumnDelimiter")), "width": _a(col, "MaximumWidth")})
    exprs = {_a(p, "Name"): (p.text or "").strip() for p in _kids(cm, "PropertyExpression")}
    name = _a(cm, "ObjectName")
    return CM(_a(cm, "DTSID"), name, _a(cm, "CreationName").upper(), props, cols, exprs, "cm_" + _ident(name))


def _safe_xml(xml: str, what: str) -> ET.Element:
    if re.search(r"<!(DOCTYPE|ENTITY)", xml, re.I):
        raise Untranslated(f"the {what} XML declares a DOCTYPE or an ENTITY, which is refused")
    try:
        return ET.fromstring(xml.lstrip("\ufeff"))
    except ET.ParseError as exc:
        raise Untranslated(f"the {what} is not valid XML ({exc})") from None


def parse(xml: str, conmgrs: tuple[str, ...] = ()) -> Package:
    """The package as plain data. Refuses a document with a DOCTYPE / ENTITY (no entity expansion is ever done). ``conmgrs``: the project's
    connection manager files (``*.conmgr``), for the connection managers a package refers to as ``{GUID}:external``."""
    root = _safe_xml(xml, "package")
    if _local(root.tag) != "Executable" or _kind(_a(root, "ExecutableType")) != "package":
        raise Untranslated("the file is not an SSIS package (.dtsx)")
    cms: list[CM] = []
    holder = _kid(root, "ConnectionManagers")
    for cm in (_kids(holder, "ConnectionManager") if holder is not None else []):
        cms.append(_cm(cm))
    for extra in conmgrs:
        el = _safe_xml(extra, "connection manager file")
        if _local(el.tag) == "ConnectionManager":
            cms.append(_cm(el))
    variables: list[Var] = []
    for v in root.iter():
        if _local(v.tag) == "Variable":
            vv = _kid(v, "VariableValue")
            ns = _a(v, "Namespace") or "User"
            if ns == "System":
                continue
            variables.append(Var(ns, _a(v, "ObjectName"), _a(vv, "DataType") if vv is not None else "8", (vv.text or "").strip() if vv is not None else "",
                                 _a(v, "Expression"), "v_" + _ident(_a(v, "ObjectName") if ns == "User" else f"{ns}_{_a(v, 'ObjectName')}")))
    params: list[Var] = []
    ph = _kid(root, "PackageParameters")
    for p in (_kids(ph, "PackageParameter") if ph is not None else []):
        pv = next((x for x in p if _local(x.tag) in ("Property", "VariableValue") and (_a(x, "Name") in ("ParameterValue", "") or _local(x.tag) == "VariableValue")), None)
        params.append(Var("$Package", _a(p, "ObjectName"), _a(p, "DataType"), (pv.text or "").strip() if pv is not None else "", "", "p_" + _ident(_a(p, "ObjectName"))))
    return Package(_a(root, "ObjectName"), variables, params, cms, _task(root))


# ---------------------------------------------------------------------------------------------------------------------------------------------
# translation
# ---------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class Translation:
    cells: list[dict] = field(default_factory=list)
    parameters: list[tuple[str, str]] = field(default_factory=list)  # (python name, default literal)
    param_map: dict[str, str] = field(default_factory=dict)  # ADF name (package parameter, "User::var", connection manager) -> notebook parameter
    untranslated: list[str] = field(default_factory=list)  # "<key>: <why>"
    notes: list[str] = field(default_factory=list)
    reads: list[str] = field(default_factory=list)
    writes: list[str] = field(default_factory=list)
    drafted: list[str] = field(default_factory=list)
    context: dict[str, dict] = field(default_factory=dict)  # key -> {"text", "dataframes", "variables", "output"}


_SQL_HELPERS = '''
def _jdbc(cm):
    """(url, user, password) from an OLE DB / ADO.NET connection string; user and password are notebook parameters, never in the package."""
    kv = {k.strip().lower(): v.strip() for k, _, v in (p.partition("=") for p in cm.split(";") if "=" in p)}
    server = (kv.get("data source") or kv.get("server") or "").replace("tcp:", "")
    db = kv.get("initial catalog") or kv.get("database") or ""
    return f"jdbc:sqlserver://{server};databaseName={db};encrypt=true;trustServerCertificate=false", kv.get("user id", ""), kv.get("password", "")

def _jdbc_opts(r, cm, user, password):
    url, u, p = _jdbc(cm)
    return r.option("url", url).option("user", user or u).option("password", password or p)

def _jdbc_read(cm, table, user="", password=""):
    return _jdbc_opts(spark.read.format("jdbc"), cm, user, password).option("dbtable", table).load()

def _jdbc_write(df, cm, table, user="", password=""):
    _jdbc_opts(df.write.format("jdbc"), cm, user, password).option("dbtable", table).mode("append").save()

def _sql_exec(cm, statement, user="", password=""):
    url, u, p = _jdbc(cm)
    jvm = spark._sc._gateway.jvm
    conn = jvm.java.sql.DriverManager.getConnection(url, user or u, password or p)
    try:
        conn.createStatement().execute(statement)
    finally:
        conn.close()
'''
_WRITE_ONE = '''
def _write_one(df, path, sep=",", header=True, quote=None, encoding=None):
    """One file at ``path`` (SSIS writes one file; Spark writes a folder, so its single part file is moved)."""
    tmp = path + ".adf2fabric-tmp"
    w = df.coalesce(1).write.mode("overwrite").option("header", header).option("sep", sep)
    if quote is not None:
        w = w.option("quote", quote)
    if encoding:
        w = w.option("encoding", encoding)
    w.csv(tmp)
    part = next(f.path for f in notebookutils.fs.ls(tmp) if f.name.startswith("part-"))
    if notebookutils.fs.exists(path):
        notebookutils.fs.rm(path, True)
    notebookutils.fs.mv(part, path)
    notebookutils.fs.rm(tmp, True)
'''
_FILES = '''
def _ssis_files(folder, spec="*", recurse=False, how=0):
    """The files of a Foreach File enumerator (``how``: 0 full path, 1 name and extension, 2 name only)."""
    import fnmatch
    out, stack = [], [folder]
    while stack:
        d = stack.pop(0)
        for f in notebookutils.fs.ls(d):
            if f.isDir:
                if recurse:
                    stack.append(f.path)
            elif fnmatch.fnmatch(f.name, spec):
                out.append(f.path if how == 0 else f.name if how == 1 else f.name.rsplit(".", 1)[0])
    return sorted(out)
'''


class _T:
    def __init__(self, pkg: Package, root: str, drafts: Optional[dict[str, dict]]):
        self.pkg, self.root, self.drafts = pkg, root, drafts or {}
        self.tr = Translation()
        self.cm_by: dict[str, CM] = {}
        for c in pkg.cms:
            for key in (c.id, c.name, f"Package.ConnectionManagers[{c.name}]"):
                if key:
                    self.cm_by[key] = c
        self.vars = {v.ref: v for v in pkg.vars}
        self.env = X.Env(vars={v.ref: (v.py, v.kind) for v in pkg.vars + pkg.params})
        for p in pkg.params:
            self.env.vars[f"$Package::{p.name}"] = (p.py, p.kind)
            self.env.vars[f"Package::{p.name}"] = (p.py, p.kind)
        self.uses = {"jdbc": False, "write_one": False, "files": False, "dt": False, "math": False}
        self.flow_cells: list[dict] = []
        self.n_df = 0

    # -- helpers -------------------------------------------------------------------------------------------------------------------------
    def fail(self, key: str, why: str, text: str, ctx: Optional[dict] = None) -> None:
        self.tr.untranslated.append(f"{key}: {why}")

    def draft_for(self, key: str) -> Optional[dict]:
        d = self.drafts.get(key)
        return d if d and d.get("code") else None

    def marked(self, key: str, why: str, d: dict) -> str:
        self.tr.drafted.append(key)
        self.tr.notes.append(f"{key} runs model-drafted code accepted by {d.get('accepted_by') or '?'}: review it")
        return (f"# [adf2fabric] MODEL-DRAFTED: '{key}' is not translated by the fixed rules ({why}).\n"
                f"# This code was drafted by a model and accepted by {d.get('accepted_by') or '?'} on {d.get('accepted_at') or '?'}.\n"
                + d["code"].rstrip() + "\n")

    def raising(self, key: str, why: str) -> str:
        return (f"# [adf2fabric] '{key}' is not translated by the fixed rules: {why}.\n"
                f"raise NotImplementedError({f'SSIS step not translated: {key}: {why}'!r})\n")

    def cm_of(self, ref: str) -> CM:
        m = re.search(r"\{[0-9A-Fa-f-]+\}", ref or "")
        for cand in (ref, m.group(0) if m else "", (re.search(r"ConnectionManagers\[(.+?)\]", ref or "") or [None, ""])[1]):
            if cand in self.cm_by:
                return self.cm_by[cand]
        if (ref or "").endswith(":external") or m:
            raise Untranslated(f"connection manager {m.group(0) if m else ref} is a project connection manager: it is defined in the project's .conmgr file, "
                               "not in this package (pass that file with the package)")
        raise Untranslated(f"connection manager '{ref}' is not in the package")

    def path_expr(self, cm: CM) -> str:
        """The python expression for a file connection's path: a property expression (a variable), else the connection string parameter."""
        e = cm.prop_exprs.get("ConnectionString")
        if e:
            return f"_p({X.python_expr(e, self.env)[0]})"
        return f"_p({cm.py})"

    def ensure_dt(self) -> None:
        self.uses["dt"] = True

    # -- control flow --------------------------------------------------------------------------------------------------------------------
    def order(self, tasks: list[Task], constraints: list[dict]) -> list[Task]:
        by = {t.path: t for t in tasks}
        deps: dict[str, set[str]] = {t.path: set() for t in tasks}
        for c in constraints:
            if c["From"] in by and c["To"] in by:
                deps[c["To"]].add(c["From"])
        out, left = [], list(tasks)
        while left:
            ready = next((t for t in left if not (deps[t.path] - {x.path for x in out})), None)
            if ready is None:
                self.tr.notes.append("precedence constraints form a cycle: the tasks run in the order they are listed")
                return out + left
            out.append(ready)
            left.remove(ready)
        return out

    def guard(self, t: Task, constraints: list[dict]) -> Optional[str]:
        """A python condition from the constraints that lead into ``t``; raises Untranslated for a failure constraint or an expression."""
        inc = [c for c in constraints if c["To"] == t.path]
        parts: list[str] = []
        for c in inc:
            if c["Value"] == "1":
                raise Untranslated("it runs only when a task before it fails (a failure precedence constraint)")
            evalop = c["EvalOp"]
            if c["Expression"] and evalop in ("1", "3", "4", ""):
                parts.append(X.python_expr(c["Expression"], self.env)[0])
        if not parts:
            return None
        both = all(c["LogicalAnd"].lower() != "false" for c in inc)
        return parts[0] if len(parts) == 1 else (" and " if both else " or ").join(f"({p})" for p in parts)

    def flow(self, t: Task, ind: int) -> list[str]:
        """The Python lines for the executables of container ``t`` (in precedence order), at indent ``ind``."""
        pad = "    " * ind
        lines: list[str] = []
        for task in self.order(t.children, t.constraints):
            key = task.path
            why = ""
            try:
                body = self.task_lines(task, ind + (0))
                g = self.guard(task, t.constraints)
            except (Untranslated, X.Untranslated) as exc:
                body, g, why = None, None, str(exc) or type(exc).__name__
            self.tr.context[key] = {"text": self.describe(task), "dataframes": {}, "variables": [v.py for v in self.pkg.vars], "output": ""}
            if body is None:
                d = self.draft_for(key)
                if d:
                    body = [pad + ln for ln in self.marked(key, why, d).splitlines()]
                else:
                    self.fail(key, why, "")
                    body = [pad + ln for ln in self.raising(key, why).splitlines()]
                g = None
            lines.append(f"{pad}# --- {task.kind}: {task.name}")
            if g:
                lines.append(f"{pad}if {g}:")
                lines += ["    " + ln if ln.strip() else ln for ln in body]
            else:
                lines += body
        return lines

    def describe(self, task: Task) -> str:
        e = task.el
        bits = [f"{task.kind} task '{task.name}' ({_a(e, 'ExecutableType')})"]
        for x in e.iter():
            loc = _local(x.tag)
            if loc in ("SqlTaskData", "ExpressionTask", "ForEachEnumerator", "FEFEProperty"):
                bits.append(f"{loc}: " + "; ".join(f"{_local(k)}={v[:200]}" for k, v in x.attrib.items()))
            if len(bits) > 8:
                break
        return "\n".join(bits)[:1500]

    def task_lines(self, task: Task, ind: int) -> list[str]:
        pad = "    " * ind
        k = task.kind
        if k == "sequence":
            return self.flow(task, ind)
        if k == "sql":
            return [pad + ln for ln in self.sql_task(task)]
        if k == "expression":
            et = next((x for x in task.el.iter() if _local(x.tag) == "ExpressionTask"), None)
            var, py = X.assignment(_a(et, "Expression") if et is not None else "", self.env)
            self.uses["dt"] = self.uses["dt"] or "datetime" in py or "_to_dt" in py
            self.uses["math"] = self.uses["math"] or "math." in py
            return [f"{pad}{var} = {py}"]
        if k == "foreach":
            return self.foreach(task, ind)
        if k == "forloop":
            return self.forloop(task, ind)
        if k == "dataflow":
            self.n_df += 1
            fn = f"dataflow_{self.n_df}"
            self.flow_cells.append(_code_cell(self.dataflow_function(task, fn)))
            return [f"{pad}{fn}()"]
        raise Untranslated(f"a {_a(task.el, 'ExecutableType') or 'task'} is not translated")

    def sql_task(self, task: Task) -> list[str]:
        d = next((x for x in task.el.iter() if _local(x.tag) == "SqlTaskData"), None)
        if d is None:
            raise Untranslated("the Execute SQL task has no SqlTaskData")
        if any(_local(x.tag) in ("ParameterBinding", "ResultBinding") for x in task.el.iter()):
            raise Untranslated("the Execute SQL task binds parameters or a result set")
        if _a(d, "SqlStmtSourceType") not in ("", "DirectInput", "1"):
            raise Untranslated("the Execute SQL task reads its statement from a variable or a file")
        cm = self.cm_of(_a(d, "Connection"))
        if cm.kind not in ("OLEDB", "ADO.NET", "ADO.NET:SQL"):
            raise Untranslated(f"connection '{cm.name}' ({cm.kind}) is not a SQL connection")
        self.uses["jdbc"] = True
        return [f"_sql_exec({cm.py}, {_a(d, 'SqlStatementSource')!r}, cm_user, cm_password)"]

    def foreach(self, task: Task, ind: int) -> list[str]:
        pad = "    " * ind
        own = {_a(p, "Name"): (p.text or "").strip() for p in _kids(task.el, "Property")}
        en = next((x for x in task.el.iter() if _local(x.tag) == "ForEachEnumerator"), None)
        kind = _a(en, "CreationName") if en is not None else own.get("Enumerator", "")
        if "ForEachFileEnumerator" not in kind and kind != "ForeachFile":
            raise Untranslated(f"a Foreach Loop over '{kind or 'an unknown enumerator'}' is not translated (only the file enumerator is)")
        props = {}
        for x in task.el.iter():
            if _local(x.tag) == "FEFEProperty":
                props.update({_local(k): _unesc(v) for k, v in x.attrib.items()})
        if own.get("FileSpec"):
            props["FileSpec"] = own["FileSpec"]
        maps = [x for x in task.el.iter() if _local(x.tag) == "ForEachVariableMapping"]
        if maps:
            if len(maps) != 1 or _a(maps[0], "ValueIndex") not in ("0", ""):
                raise Untranslated("the Foreach Loop maps more or other than the file name to one variable")
            vname = _a(maps[0], "VariableName")
        else:
            vname = own.get("VariableName", "")
        var = self.vars.get(vname)
        if var is None:
            raise Untranslated(f"variable {vname or '(none)'} is not declared in the package")
        self.uses["files"] = True
        if own.get("DirectoryExpression"):  # a path computed from variables
            folder_code = f"_p({X.python_expr(own['DirectoryExpression'], self.env)[0]})"
            where = own["DirectoryExpression"]
        else:
            folder = props.get("Folder", own.get("Directory", ""))
            folder_code, where = f"_p({folder!r})", folder
        self.tr.notes.append(f"the Foreach Loop reads '{where}' under the root parameter ssis_root (a Windows folder cannot exist in Fabric)")
        inner = self.flow(task, ind + 1)
        return [f"{pad}for {var.py} in _ssis_files({folder_code}, {props.get('FileSpec', '*')!r}, {props.get('Recurse', '0') in ('1', 'True', 'true')}, "
                f"{int(props.get('FileNameRetrievalType', '0') or 0)}):"] + (inner or [pad + "    pass"])

    def forloop(self, task: Task, ind: int) -> list[str]:
        pad = "    " * ind
        init, ev, asg = _a(task.el, "InitExpression"), _a(task.el, "EvalExpression"), _a(task.el, "AssignExpression")
        if not ev:
            raise Untranslated("the For Loop has no EvalExpression")
        lines = []
        if init:
            v, py = X.assignment(init, self.env)
            lines.append(f"{pad}{v} = {py}")
        cond = X.python_expr(ev, self.env)[0]
        inner = self.flow(task, ind + 1)
        lines.append(f"{pad}while {cond}:")
        lines += inner or [pad + "    pass"]
        if asg:
            v, py = X.assignment(asg, self.env)
            lines.append(f"{pad}    {v} = {py}")
        return lines

    # -- data flow -----------------------------------------------------------------------------------------------------------------------
    def dataflow_function(self, task: Task, fn: str) -> str:
        od = _kid(task.el, "ObjectData")
        pipe = od[0] if od is not None and len(od) else None
        if pipe is None or _local(pipe.tag) != "pipeline":
            return f"def {fn}():\n" + "".join("    " + ln + "\n" for ln in self.raising(task.path, "the data flow has no pipeline definition").splitlines())
        comps = {}
        order: list[str] = []
        for comp in pipe.iter():
            if _local(comp.tag) == "component":
                comps[_a(comp, "refId")] = comp
                order.append(_a(comp, "refId"))
        edges: list[tuple[str, str, str, str]] = []
        for p in pipe.iter():
            if _local(p.tag) == "path":
                a = re.match(r"^(.*)\.Outputs\[(.*)\]$", _a(p, "startId"), re.S)
                b = re.match(r"^(.*)\.Inputs\[(.*)\]$", _a(p, "endId"), re.S)
                if a and b:
                    edges.append((a.group(1), a.group(2), b.group(1), b.group(2)))
        frames: dict[tuple[str, str], tuple[str, dict]] = {}  # (component, output) -> (python variable, columns)
        body: list[str] = []
        globals_: set[str] = set()
        done: list[str] = []
        left = list(order)
        while left:
            ready = next((c for c in left if all(e[0] in done for e in edges if e[2] == c)), None)
            if ready is None:
                ready = left[0]
            left.remove(ready)
            done.append(ready)
            comp = comps[ready]
            name = _a(comp, "name")
            key = f"{task.path}~{name}"
            ins = {}
            for a_c, a_o, b_c, b_o in edges:
                if b_c == ready:
                    ins.setdefault(b_o, []).append(frames.get((a_c, a_o)))
            lines: list[str] = []
            try:
                lines = self.component(comp, ins, frames, globals_, task)
                err = ""
            except (Untranslated, X.Untranslated, KeyError, IndexError, StopIteration) as exc:
                err = str(exc) or type(exc).__name__
            self.tr.context[key] = {"text": self.describe_comp(comp), "dataframes": {v: list(c) for v, c in {f[0]: f[1] for f in frames.values()}.items()},
                                    "variables": [v.py for v in self.pkg.vars], "output": _dv(name)}
            if err:
                d = self.draft_for(key)
                if d:
                    lines = self.marked(key, err, d).splitlines()
                else:
                    self.fail(key, err, "")
                    lines = self.raising(key, err).splitlines()
                for o in comp.iter():
                    if _local(o.tag) == "output" and _a(o, "isErrorOut").lower() != "true":
                        frames.setdefault((ready, _a(o, "name")), (_dv(name), {}))
            body += [f"# --- {_ctype(comp)}: {name}"] + lines
        head = f"def {fn}():\n"
        if globals_:
            head += f"    global {', '.join(sorted(globals_))}\n"
        return head + "".join(("    " + ln if ln.strip() else ln) + "\n" for ln in body)

    def describe_comp(self, comp: ET.Element) -> str:
        props = [f"{_a(p, 'name')}={(p.text or '').strip()[:200]}" for pp in _kids(comp, "properties") for p in pp if (p.text or "").strip()]
        return f"data flow component '{_a(comp, 'name')}' ({_ctype(comp)}); " + "; ".join(props[:12])

    def props(self, comp: ET.Element) -> dict[str, str]:
        return {_a(p, "name"): (p.text or "").strip() for pp in _kids(comp, "properties") for p in pp}

    def outputs(self, comp: ET.Element) -> list[ET.Element]:
        outs = next((x for x in comp if _local(x.tag) == "outputs"), None)
        return [o for o in (_kids(outs, "output") if outs is not None else []) if _a(o, "isErrorOut").lower() != "true"]

    def out_cols(self, out: ET.Element) -> dict[str, Optional[str]]:
        cols = next((x for x in out if _local(x.tag) == "outputColumns"), None)
        return {_a(c, "name"): _DF_TYPES.get(_a(c, "dataType")) or ("num" if _a(c, "dataType") else None) for c in (_kids(cols, "outputColumn") if cols is not None else [])}

    def input_cols(self, comp: ET.Element) -> list[ET.Element]:
        ins = next((x for x in comp if _local(x.tag) == "inputs"), None)
        out = []
        for inp in (_kids(ins, "input") if ins is not None else []):
            cols = next((x for x in inp if _local(x.tag) == "inputColumns"), None)
            out += _kids(cols, "inputColumn") if cols is not None else []
        return out

    def only_input(self, ins: dict, comp: ET.Element) -> tuple[str, dict]:
        frames = [f for lst in ins.values() for f in lst]
        if len(frames) != 1 or frames[0] is None:
            raise Untranslated("the component's input is not connected to exactly one upstream output")
        return frames[0]

    def component(self, comp: ET.Element, ins: dict, frames: dict, globals_: set, task: Task) -> list[str]:
        ref, name, ct = _a(comp, "refId"), _a(comp, "name"), _ctype(comp)
        props = self.props(comp)
        out = _dv(name)
        outs = self.outputs(comp)

        def set_out(var: str, cols: dict, output: Optional[ET.Element] = None) -> None:
            frames[(ref, _a(output if output is not None else outs[0], "name"))] = (var, cols)

        if ct == "Flat File Source":
            cm = self.cm_of(self.conn_id(comp))
            self.flat_ok(cm)
            cols = {c["name"]: _DF_TYPES.get(_FF_TYPES.get(c["type"], ""), "str" if _FF_TYPES.get(c["type"]) == "string" else "num") for c in cm.columns}
            schema = ", ".join(f"StructField({c['name']!r}, {_spark_type(_FF_TYPES.get(c['type'], 'string'))}, True)" for c in cm.columns)
            header = cm.props.get("ColumnNamesInFirstDataRow", "").lower() == "true"
            delim = cm.columns[0]["delim"] or ","
            if any(c["delim"] != delim for c in cm.columns[:-1]):
                raise Untranslated("the flat file has different delimiters between columns")
            q = cm.props.get("TextQualifier", "")
            q = "" if q in ("<none>", "") else q
            enc = _CODEPAGES.get(cm.props.get("CodePage", ""))
            if cm.props.get("CodePage") and not enc:
                self.tr.notes.append(f"flat file '{cm.name}' uses code page {cm.props['CodePage']}: read as UTF-8; check the characters")
            self.tr.reads.append(cm.name)
            self.tr.notes.append(f"flat file '{cm.name}': empty fields read as null in Spark; SSIS reads them as null only with 'Retain null values' (RetainNulls={props.get('RetainNulls', 'false')})")
            set_out(out, cols)
            return [f"{out} = (spark.read.option('header', {header}).option('sep', {delim!r}).option('quote', {q!r}).option('mode', 'FAILFAST')"
                    + (f".option('encoding', {enc!r})" if enc else "") + f"\n    .schema(StructType([{schema}]))\n    .csv({self.path_expr(cm)}))"]
        if ct == "OLE DB Source":
            cm = self.cm_of(self.conn_id(comp))
            mode = props.get("AccessMode", "0")
            if mode == "0":
                src = props["OpenRowset"]
            elif mode == "2":
                src = f"({props['SqlCommand']}) q"
            else:
                raise Untranslated(f"OLE DB Source access mode {mode} (only table name and SQL command are translated)")
            self.uses["jdbc"] = True
            cols = self.out_cols(outs[0])
            self.tr.reads.append(cm.name)
            set_out(out, cols)
            names = ", ".join(f"F.col({n!r})" for n in cols)
            return [f"{out} = _jdbc_read({cm.py}, {src!r}, cm_user, cm_password)" + (f".select({names})" if cols else "")]
        if ct not in _KNOWN:
            raise Untranslated(f"the component type '{ct}' is not translated")
        src_var, src_cols = (self.only_input(ins, comp) if ct != "Union All" else ("", {}))
        if ct == "Flat File Destination":
            cm = self.cm_of(self.conn_id(comp))
            self.flat_ok(cm)
            header = cm.props.get("ColumnNamesInFirstDataRow", "").lower() == "true"
            delim = cm.columns[0]["delim"] if cm.columns else ","
            q = cm.props.get("TextQualifier", "")
            sel = self.mapped(comp, src_cols)
            self.uses["write_one"] = True
            self.tr.writes.append(cm.name)
            return [f"_write_one({src_var}{sel}, {self.path_expr(cm)}, {delim!r}, {header}, {'' if q in ('<none>', '') else q!r})"]
        if ct == "OLE DB Destination":
            cm = self.cm_of(self.conn_id(comp))
            if props.get("AccessMode", "0") not in ("0", "3"):
                raise Untranslated(f"OLE DB Destination access mode {props.get('AccessMode')} (only a table name is translated)")
            self.uses["jdbc"] = True
            self.tr.writes.append(cm.name)
            return [f"_jdbc_write({src_var}{self.mapped(comp, src_cols)}, {cm.py}, {props['OpenRowset']!r}, cm_user, cm_password)"]
        if ct == "Derived Column":
            cols = dict(src_cols)
            lines = [f"{out} = ({src_var}"]
            ctxe = X.Env(cols=cols, vars=self.env.vars, notes=self.tr.notes)
            exprs = []
            for holder in (self.input_cols(comp), [c for o in outs for c in _kids(next(x for x in o if _local(x.tag) == "outputColumns"), "outputColumn")]
                           if any(_local(x.tag) == "outputColumns" for o in outs for x in o) else []):
                for c in holder:
                    p = {_a(x, "name"): (x.text or "").strip() for pp in _kids(c, "properties") for x in pp}
                    if p.get("FriendlyExpression") or p.get("Expression"):
                        exprs.append((_a(c, "name") or _a(c, "cachedName"), p.get("FriendlyExpression") or p["Expression"], _DF_TYPES.get(_a(c, "dataType"))))
            if not exprs:
                raise Untranslated("the Derived Column has no expressions")
            for cname, e, typ in exprs:
                code, t = X.column_expr(e, ctxe)
                lines.append(f"    .withColumn({cname!r}, {code})")
                ctxe.cols[cname] = typ or t
            set_out(out, ctxe.cols)
            return lines + [")"]
        if ct == "Conditional Split":
            specs = []
            default = None
            ctxe = X.Env(cols=src_cols, vars=self.env.vars, notes=self.tr.notes)
            for o in outs:
                p = {_a(x, "name"): (x.text or "").strip() for pp in _kids(o, "properties") for x in pp}
                if p.get("IsDefaultOut", "").lower() == "true":
                    default = o
                elif p.get("FriendlyExpression") or p.get("Expression"):
                    specs.append((int(p.get("EvaluationOrder", "0") or 0), o, X.column_expr(p.get("FriendlyExpression") or p["Expression"], ctxe)[0]))
            if not specs:
                raise Untranslated("the Conditional Split has no conditions")
            lines, taken = [], []
            for _, o, cond in sorted(specs, key=lambda s: s[0]):
                v = _dv(name, _a(o, "name"))
                prior = "".join(f" & ~({t})" for t in taken)
                lines.append(f"{v} = {src_var}.filter(F.coalesce({cond}, F.lit(False)){prior})")
                taken.append(f"F.coalesce({cond}, F.lit(False))")
                set_out(v, dict(src_cols), o)
            if default is not None:
                v = _dv(name, _a(default, "name"))
                lines.append(f"{v} = {src_var}.filter(~({' | '.join(taken)}))")
                set_out(v, dict(src_cols), default)
            return lines
        if ct == "Aggregate":
            lines = []
            for o in outs:
                keys, aggs, cols = [], [], {}
                for c in _kids(next(x for x in o if _local(x.tag) == "outputColumns"), "outputColumn"):
                    p = {_a(x, "name"): (x.text or "").strip() for pp in _kids(c, "properties") for x in pp}
                    kind = p.get("AggregationType", "0")
                    srcc = (re.search(r"Columns\[([^\]]+)\]", p.get("AggregationColumnId", "")) or [None, _a(c, "name")])[1]
                    nm = _a(c, "name")
                    if kind == "0":
                        keys.append(f"F.col({srcc!r}).alias({nm!r})")
                    else:
                        fn = {"1": f"F.count(F.col({srcc!r}))", "2": "F.count(F.lit(1))", "3": f"F.countDistinct(F.col({srcc!r}))", "4": f"F.sum(F.col({srcc!r}))",
                              "5": f"F.avg(F.col({srcc!r}))", "6": f"F.max(F.col({srcc!r}))", "7": f"F.min(F.col({srcc!r}))"}.get(kind)
                        if fn is None:
                            raise Untranslated(f"aggregation type {kind}")
                        aggs.append(f"{fn}.alias({nm!r})")
                    cols[nm] = src_cols.get(srcc) if kind in ("0", "6", "7") else "num"
                v = _dv(name, _a(o, "name"))
                grp = f".groupBy({', '.join(keys)})" if keys else ""
                lines.append(f"{v} = {src_var}{grp}.agg({', '.join(aggs)})" if aggs else f"{v} = {src_var}.select({', '.join(keys)}).distinct()")
                set_out(v, cols, o)
            return lines
        if ct == "Sort":
            keyed = []
            for c in self.input_cols(comp):
                p = {_a(x, "name"): (x.text or "").strip() for pp in _kids(c, "properties") for x in pp}
                pos = int(p.get("NewSortKeyPosition", "0") or 0)
                if pos:
                    keyed.append((abs(pos), pos < 0, _a(c, "cachedName")))
            if not keyed:
                raise Untranslated("the Sort has no sort keys")
            keyed.sort()
            orders = ", ".join(f"F.col({n!r}).{'desc' if d else 'asc'}()" for _, d, n in keyed)
            dedupe = props.get("EliminateDuplicates", "false").lower() == "true"
            set_out(out, dict(src_cols))
            return [f"{out} = {src_var}" + (f".dropDuplicates({[n for _, _, n in keyed]!r})" if dedupe else "") + f".orderBy({orders})"]
        if ct == "Multicast":
            for o in outs:
                v = _dv(name, _a(o, "name"))
                frames[(ref, _a(o, "name"))] = (src_var, dict(src_cols))
            return [f"# Multicast: every output is {src_var}"]
        if ct == "Data Conversion":
            cols = dict(src_cols)
            lines = [f"{out} = ({src_var}"]
            for c in _kids(next(x for x in outs[0] if _local(x.tag) == "outputColumns"), "outputColumn"):
                p = {_a(x, "name"): (x.text or "").strip() for pp in _kids(c, "properties") for x in pp}
                srcc = (re.search(r"Columns\[([^\]]+)\]", p.get("SourceInputColumnLineageID", "")) or [None, None])[1]
                tgt = _DF_SPARK.get(_a(c, "dataType"))
                if not srcc or not tgt:
                    raise Untranslated(f"a converted column of type {_a(c, 'dataType')}")
                lines.append(f"    .withColumn({_a(c, 'name')!r}, F.col({srcc!r}).cast({tgt!r}))")
                cols[_a(c, "name")] = _DF_TYPES.get(_a(c, "dataType"), "num")
            set_out(out, cols)
            return lines + [")"]
        if ct == "Row Count":
            var = self.vars.get(props.get("VariableName", ""))
            if var is None:
                raise Untranslated(f"variable {props.get('VariableName')!r} is not declared in the package")
            globals_.add(var.py)
            set_out(src_var, src_cols)
            return [f"{var.py} = {src_var}.count()"]
        if ct == "Union All":
            parts = []
            cols: dict = {}
            for iname, lst in sorted(ins.items()):
                if len(lst) != 1 or lst[0] is None:
                    raise Untranslated("a Union All input is not connected to exactly one upstream output")
                inp = next(i for i in next(x for x in comp if _local(x.tag) == "inputs") if _a(i, "name") == iname)
                mapping = []
                for c in _kids(next((x for x in inp if _local(x.tag) == "inputColumns"), inp), "inputColumn"):
                    p = {_a(x, "name"): (x.text or "").strip() for pp in _kids(c, "properties") for x in pp}
                    target = (re.search(r"Columns\[([^\]]+)\]", p.get("OutputColumnLineageID", "")) or [None, None])[1]
                    if target:
                        mapping.append(f"F.col({_a(c, 'cachedName')!r}).alias({target!r})")
                        cols[target] = lst[0][1].get(_a(c, "cachedName"))
                parts.append(f"{lst[0][0]}.select({', '.join(mapping)})")
            if len(parts) < 2:
                raise Untranslated("a Union All with fewer than two connected inputs")
            set_out(out, cols)
            return [f"{out} = (" + "\n    .unionByName(".join([parts[0]] + [p + ")" for p in parts[1:]]) + ")"]
        if ct == "Lookup":
            return self.lookup(comp, props, src_var, src_cols, outs, set_out)
        raise Untranslated(f"the component type '{ct}' is not translated")

    def lookup(self, comp, props, src_var, src_cols, outs, set_out) -> list[str]:
        if props.get("ConnectionType", "0") != "0":
            raise Untranslated("a Lookup against a cache connection manager")
        cm = self.cm_of(self.conn_id(comp))
        self.uses["jdbc"] = True
        keys = []
        for c in self.input_cols(comp):
            p = {_a(x, "name"): (x.text or "").strip() for pp in _kids(c, "properties") for x in pp}
            if p.get("JoinToReferenceColumn"):
                keys.append((_a(c, "cachedName"), p["JoinToReferenceColumn"]))
        copies = []
        for o in outs:
            oc = next((x for x in o if _local(x.tag) == "outputColumns"), None)
            for c in (_kids(oc, "outputColumn") if oc is not None else []):
                p = {_a(x, "name"): (x.text or "").strip() for pp in _kids(c, "properties") for x in pp}
                if p.get("CopyFromReferenceColumn"):
                    copies.append((_a(c, "name"), p["CopyFromReferenceColumn"], _DF_TYPES.get(_a(c, "dataType")) or "num"))
        if not keys:
            raise Untranslated("the Lookup has no join columns")
        name = _a(comp, "name")
        cond = " & ".join(f"(F.col('L.{l}') == F.col('R.{r}'))" for l, r in keys)
        left_cols = ", ".join(f"F.col('L.{c}').alias({c!r})" for c in src_cols) if src_cols else "F.col('L.*')"
        ref = props.get("SqlCommand", "")
        if not ref:
            raise Untranslated("the Lookup has no reference query")
        behavior = props.get("NoMatchBehavior", "0")
        lines = [f"_ref = _jdbc_read({cm.py}, {'(' + ref + ') ref'!r}, cm_user, cm_password)",
                 f"_j = {src_var}.alias('L').join(_ref.alias('R'), {cond}, {'inner' if behavior == '2' else 'left'!r})"]
        sel = left_cols + "".join(f", F.col('R.{r}').alias({n!r})" for n, r, _ in copies)
        cols = dict(src_cols)
        cols.update({n: t for n, _, t in copies})
        match_o = next((o for o in outs if "no match" not in _a(o, "name").lower()), outs[0])
        nomatch_o = next((o for o in outs if "no match" in _a(o, "name").lower()), None)
        mv = _dv(name, _a(match_o, "name"))
        if behavior == "0":
            lines.append(f"if _j.filter(F.col('R.{keys[0][1]}').isNull()).limit(1).count():")
            lines.append(f"    raise Exception('Lookup {name}: a row has no match (the component is set to fail on no match)')")
        lines.append(f"{mv} = _j.select({sel})")
        set_out(mv, cols, match_o)
        if behavior == "2" and nomatch_o is not None:
            nv = _dv(name, _a(nomatch_o, "name"))
            lines.append(f"{nv} = {src_var}.alias('L').join(_ref.alias('R'), {cond}, 'left_anti')")
            set_out(nv, dict(src_cols), nomatch_o)
        return lines

    # -- small things --------------------------------------------------------------------------------------------------------------------
    def conn_id(self, comp: ET.Element) -> str:
        cs = next((x for x in comp if _local(x.tag) == "connections"), None)
        cn = next(iter(_kids(cs, "connection")), None) if cs is not None else None
        if cn is None:
            raise Untranslated("the component has no connection")
        return _a(cn, "connectionManagerID") or _a(cn, "connectionManagerRefId")

    def flat_ok(self, cm: CM) -> None:
        if cm.kind != "FLATFILE":
            raise Untranslated(f"connection '{cm.name}' ({cm.kind}) is not a flat file")
        if cm.props.get("Format", "Delimited") != "Delimited":
            raise Untranslated(f"flat file '{cm.name}' is {cm.props.get('Format')} (only delimited files are translated)")
        if not cm.columns:
            raise Untranslated(f"flat file '{cm.name}' declares no columns")

    def mapped(self, comp: ET.Element, src_cols: dict) -> str:
        """``.select(...)`` for a destination: the upstream columns in the destination's order, named as the destination column (empty = all)."""
        sel = []
        for c in self.input_cols(comp):
            ext = re.search(r"ExternalColumns\[([^\]]+)\]", _a(c, "externalMetadataColumnId"))
            sel.append((_a(c, "cachedName"), ext.group(1) if ext else _a(c, "cachedName")))
        return ".select(" + ", ".join(f"F.col({s!r}).alias({d!r})" for s, d in sel) + ")" if sel else ""


_KNOWN = {"Flat File Destination", "OLE DB Destination", "Derived Column", "Conditional Split", "Aggregate", "Sort", "Multicast", "Data Conversion",
          "Row Count", "Union All", "Lookup"}


def _ctype(comp: ET.Element) -> str:
    ci = _a(comp, "contactInfo").split(";")[0].strip()
    if ci:
        return ci
    return {"Microsoft.FlatFileSource": "Flat File Source", "Microsoft.OLEDBSource": "OLE DB Source", "Microsoft.OLEDBDestination": "OLE DB Destination",
            "Microsoft.FlatFileDestination": "Flat File Destination"}.get(_a(comp, "componentClassID"), _a(comp, "componentClassID"))


def _dv(comp: str, out: Optional[str] = None) -> str:
    return "df_" + _ident(comp) + (f"__{_ident(out)}" if out else "")


def _spark_type(name: str) -> str:
    return {"string": "StringType()", "int": "IntegerType()", "bigint": "LongType()", "smallint": "ShortType()", "tinyint": "ByteType()", "float": "FloatType()",
            "double": "DoubleType()", "boolean": "BooleanType()", "date": "DateType()", "timestamp": "TimestampType()"}.get(name) or (
        f"DecimalType({name[8:-1]})" if name.startswith("decimal(") else "StringType()")


def _literal(v: Var) -> Any:
    k = v.kind
    if k == "num":
        try:
            return int(v.value) if re.fullmatch(r"-?\d+", v.value) else float(v.value)
        except ValueError:
            return 0
    if k == "bool":
        return v.value.lower() in ("true", "1", "-1")
    return v.value


def package_param(name: str) -> str:
    """The notebook parameter of a package or project parameter (ADF ``packageParameters`` / ``projectParameters``)."""
    return "p_" + _ident(name)


def variable_param(ref: str) -> Optional[str]:
    """The notebook parameter for an ADF ``propertyOverrides`` key that sets a package variable (a key such as ``Package.Variables[User::x].Value``), else None."""
    m = re.fullmatch(r"\\?(?:Package)?\.?Variables\[(?:(\w+)::)?([^\]]+)\]\.Value", ref.strip().lstrip("\\"))
    if not m:
        m = re.search(r"Variables\[(?:(\w+)::)?([^\]]+)\]\.Value", ref)
    if not m:
        return None
    ns = m.group(1) or "User"
    return "v_" + _ident(m.group(2) if ns == "User" else f"{ns}_{m.group(2)}")


def cm_param(name: str, prop: str = "ConnectionString") -> Optional[str]:
    """The notebook parameter an ADF connection manager override sets: its connection string, or the SQL user name."""
    p = prop.lower()
    if p == "connectionstring":
        return "cm_" + _ident(name)
    if p == "username":
        return "cm_user"
    return None


def package_location(raw: dict) -> tuple[str, str]:
    """(type, path) of an Execute SSIS package activity's ``packageLocation``: SSISDB | File | InlinePackage | PackageStore."""
    loc = raw.get("packageLocation") if isinstance(raw.get("packageLocation"), dict) else {}
    tp = loc.get("typeProperties") if isinstance(loc.get("typeProperties"), dict) else loc
    path = tp.get("packagePath") if isinstance(tp.get("packagePath"), str) else ""
    return str(loc.get("type") or ""), path


def translate(xml: str, root: str = "Files/ssis/", drafts: Optional[dict[str, dict]] = None, conmgrs: tuple[str, ...] = ()) -> Translation:
    """The notebook for a package. ``root``: where the package's file paths are read from (a notebook parameter, ``ssis_root``).
    ``drafts``: step key (a task's path, or ``<data flow path>~<component name>``) -> an accepted model draft."""
    pkg = parse(xml, conmgrs)
    t = _T(pkg, root, drafts)
    tr = t.tr
    # the flow first (it decides which helpers are needed)
    run = t.flow(pkg.root, 0)
    params: list[tuple[str, Any]] = []
    for p in pkg.params:
        params.append((p.py, _literal(p)))
        tr.param_map[p.name] = p.py
        tr.param_map[f"$Package::{p.name}"] = p.py
    for v in pkg.vars:
        if not v.expr:
            params.append((v.py, _literal(v)))
            tr.param_map[v.ref] = v.py
    for c in pkg.cms:
        if c.kind in ("OLEDB", "ADO.NET", "ADO.NET:SQL", "FLATFILE", "FILE"):
            cs = c.props.get("ConnectionString", "")
            params.append((c.py, cs))
            tr.param_map[c.name] = c.py
    if t.uses["jdbc"]:
        params += [("cm_user", ""), ("cm_password", "")]
        tr.notes.append("SQL connections go through JDBC: pass cm_user and cm_password (a Key Vault secret, never stored); Windows authentication has no equivalent")
    params.append(("ssis_root", root))
    tr.parameters = [(n, repr(v)) for n, v in params]
    tr.cells.append(_code_cell("# Parameters: the pipeline's notebook activity overrides these by name (package parameters p_*, variables v_*, connection strings cm_*).\n"
                               + "".join(f"{n} = {v!r}\n" for n, v in params), ["parameters"]))
    setup = ("# Generated by adf2fabric from the SSIS package (fixed translation rules).\nimport re\nfrom pyspark.sql import functions as F\nfrom pyspark.sql import Window\n"
             "from pyspark.sql.types import *  # noqa: F403 - the declared column types\n"
             + ("from datetime import datetime\n" if t.uses["dt"] else "") + ("import math\n" if t.uses["math"] else "") +
             "\ndef _p(path):\n    \"\"\"A path in the package (C:\\\\data\\\\x.csv) read under the root parameter: the folders are kept, the drive is dropped.\"\"\"\n"
             "    path = str(path)\n    if '://' in path:\n        return path\n    path = re.sub(r'^([A-Za-z]:|\\\\\\\\[^\\\\]+\\\\[^\\\\]+)', '', path).replace('\\\\', '/').lstrip('/')\n"
             "    return ssis_root.rstrip('/') + '/' + path\n"
             + ("\ndef _to_dt(x):\n    if isinstance(x, datetime):\n        return x\n    for f in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y'):\n"
                "        try:\n            return datetime.strptime(str(x), f)\n        except ValueError:\n            pass\n    raise ValueError(f'not a date: {x}')\n" if t.uses["dt"] else "")
             + (_SQL_HELPERS if t.uses["jdbc"] else "") + (_WRITE_ONE if t.uses["write_one"] else "") + (_FILES if t.uses["files"] else ""))
    tr.cells.append(_code_cell(setup))
    # variables with an expression, in the order they are declared (SSIS evaluates them on use; a dependency on a later one is named)
    expr_lines = []
    for v in pkg.vars:
        if v.expr:
            key = f"var~{v.name}"
            try:
                py, _ = X.python_expr(v.expr, t.env)
                expr_lines.append(f"{v.py} = {py}")
                t.uses["dt"] = t.uses["dt"] or "datetime" in py
            except X.Untranslated as exc:
                d = t.draft_for(key)
                if d:
                    expr_lines += t.marked(key, str(exc), d).splitlines()
                else:
                    tr.untranslated.append(f"{key}: {exc}")
                    expr_lines += t.raising(key, str(exc)).splitlines()
            tr.context[key] = {"text": f"variable {v.ref} expression: {v.expr}", "dataframes": {}, "variables": [x.py for x in pkg.vars], "output": v.py}
    # typed starting values for variables the notebook receives as text
    typed = []
    for v in pkg.vars + pkg.params:
        if v.expr:
            continue
        k = v.kind
        typed.append(f"{v.py} = " + ("int({0})" if k == "num" and re.fullmatch(r"-?\d+", v.value or "0") else "float({0})" if k == "num" else
                                     "(str({0}).lower() in ('true', '1', '-1'))" if k == "bool" else "_to_dt({0})" if k == "date" and v.value else "{0}").format(v.py))
        if k == "date" and v.value:
            t.uses["dt"] = True
    if t.uses["dt"] and "from datetime import datetime" not in setup:  # a date needed only by a variable or an expression
        tr.cells[-1] = _code_cell(setup.replace("import re\n", "import re\nfrom datetime import datetime\n", 1) + (
            "\ndef _to_dt(x):\n    if isinstance(x, datetime):\n        return x\n    for f in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y'):\n"
            "        try:\n            return datetime.strptime(str(x), f)\n        except ValueError:\n            pass\n    raise ValueError(f'not a date: {x}')\n"
            if "def _to_dt" not in setup else ""))
    if typed or expr_lines:
        tr.cells.append(_code_cell("# Variables: typed from the parameters above, then those the package computes with an expression.\n" + "\n".join(typed + expr_lines) + "\n"))
    tr.cells += t.flow_cells
    tr.cells.append(_code_cell(f"# Package '{pkg.name}': the control flow, in precedence order.\n" + ("\n".join(run) if run else "pass") + "\n"))
    tr.notes = list(dict.fromkeys(tr.notes))
    tr.notes.append("paths in a package are paths on the machine that ran it: they are read under the notebook parameter ssis_root (folders kept, drive dropped); "
                    "check the files are there")
    return tr
