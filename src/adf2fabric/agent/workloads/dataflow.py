"""ADF mapping data flows -> a PySpark notebook, by fixed rules (since 2026-11-15, the user's decision: a fixed translator, and a model
draft only for what it cannot map).

A mapping data flow's logic is its data flow script (``typeProperties.script`` / ``scriptLines``; Microsoft, "Data flow script"): an
optional ``parameters{...}`` block, then statements ``[inputs] transform(args) ~> name`` (``~> name@(a, b)`` for a conditional split).
``parse`` reads it into statements; ``translate`` writes one notebook cell per statement:

* ``source`` / ``sink`` read and write the dataset the data flow names for them (``typeProperties.sources|sinks``): DelimitedText, Parquet,
  Json on ADLS Gen2 / Blob, at the dataset's own location (the storage map's OneLake shortcuts re-point it later, like any notebook);
* ``filter``, ``derive``, ``select`` (``mapColumn``), ``aggregate`` (``groupBy``), ``join``, ``lookup``, ``exists``, ``union``,
  ``split`` (conditional split), ``sort``, ``window``, ``keyGenerate`` (surrogate key);
* expressions (``EXPR``) to ``pyspark.sql.functions``; data flow parameters (``$name``) to the notebook's parameter cell.

Anything else (a transformation, function or option outside these) is NOT guessed: its cell raises ``NotImplementedError`` naming the
construct, and it is listed in ``Translation.untranslated`` so the step can be drafted by a model and approved by a person, or done by hand.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Optional

# =====================================================================================================================
# 1. tokens and the script's syntax tree
# =====================================================================================================================

_TOKEN = re.compile(r"""
    (?P<ws>\s+|/\*.*?\*/|//[^\n]*)
  | (?P<arrow>~>)
  | (?P<op>===|<=>|==|!=|>=|<=|&&|\|\||[-+*/%^<>!=@:,()\[\]{}?.])
  | (?P<str>'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")
  | (?P<num>\d+(?:\.\d+)?(?:[eE][-+]?\d+)?[lLdDfF]?)
  | (?P<name>\$\$|\$?[A-Za-z_][A-Za-z0-9_]*)
""", re.X | re.S)


class ScriptError(ValueError):
    pass


def tokenize(text: str) -> list[tuple[str, str]]:
    out, i = [], 0
    while i < len(text):
        m = _TOKEN.match(text, i)
        if not m:
            raise ScriptError(f"unexpected character {text[i]!r} at {i}")
        i = m.end()
        kind = m.lastgroup
        if kind == "ws":
            continue
        out.append((kind, m.group(0)))
    return out


@dataclass
class Node:
    kind: str  # call | name | col | str | num | bool | null | param | binop | unop | assign | option | list | typed
    value: Any = None  # name / literal / operator
    args: list["Node"] = field(default_factory=list)
    stream: Optional[str] = None  # col: the stream of stream@column
    type: Optional[str] = None  # typed: the declared type


@dataclass
class Statement:
    name: str  # output stream
    transform: str
    inputs: list[str]  # input streams ("A" or "A@out")
    args: list[Node]
    outputs: list[str] = field(default_factory=list)  # split: the named outputs


@dataclass
class Script:
    parameters: list[tuple[str, str, Optional[Node]]] = field(default_factory=list)  # (name, type, default expression)
    statements: list[Statement] = field(default_factory=list)


class _Parser:
    PREC = [("||",), ("&&",), ("==", "!=", "===", "<=>"), ("<", ">", "<=", ">="), ("+", "-"), ("*", "/", "%"), ("^",)]

    def __init__(self, tokens: list[tuple[str, str]]):
        self.t, self.i = tokens, 0

    def peek(self, k: int = 0) -> tuple[str, str]:
        return self.t[self.i + k] if self.i + k < len(self.t) else ("eof", "")

    def take(self, value: Optional[str] = None) -> tuple[str, str]:
        tok = self.peek()
        if value is not None and tok[1] != value:
            raise ScriptError(f"expected {value!r}, found {tok[1]!r}")
        self.i += 1
        return tok

    def at(self, value: str) -> bool:
        return self.peek()[1] == value

    # -- script --------------------------------------------------------------------------------------------------------
    def script(self) -> Script:
        s = Script()
        if self.peek()[1] == "parameters" and self.peek(1)[1] == "{":
            self.take()
            self.take("{")
            while not self.at("}"):
                name = self.take()[1]
                self.take("as")
                typ = self.type_name()
                default = None
                if self.at("("):
                    self.take("(")
                    default = self.expr()
                    self.take(")")
                s.parameters.append((name, typ, default))
                if self.at(","):
                    self.take(",")
            self.take("}")
        while self.peek()[0] != "eof":
            s.statements.append(self.statement())
        return s

    def statement(self) -> Statement:
        inputs: list[str] = []
        if not (self.peek()[0] == "name" and self.peek(1)[1] == "("):  # a transformation with input streams
            while True:
                inputs.append(self.stream_ref())
                if not self.at(","):
                    break
                self.take(",")
        transform = self.take()[1]
        self.take("(")
        args = self.arg_list(")")
        self.take(")")
        self.take("~>")
        name = self.take()[1]
        outputs: list[str] = []
        if self.at("@"):
            self.take("@")
            self.take("(")
            while not self.at(")"):
                outputs.append(self.take()[1])
                if self.at(","):
                    self.take(",")
            self.take(")")
        return Statement(name, transform, inputs, args, outputs)

    def stream_ref(self) -> str:
        name = self.take()[1]
        if self.at("@") and self.peek(1)[0] == "name":
            self.take("@")
            name += "@" + self.take()[1]
        return name

    def arg_list(self, close: str) -> list[Node]:
        out = []
        while not self.at(close):
            out.append(self.arg())
            if self.at(","):
                self.take(",")
            elif not self.at(close):
                raise ScriptError(f"expected ',' or {close!r}, found {self.peek()[1]!r}")
        return out

    def arg(self) -> Node:
        kind, val = self.peek()
        if kind == "name" and self.peek(1)[1] == ":" and not val.startswith("$"):
            self.take()
            self.take(":")
            return Node("option", val, [self.expr()])
        if kind == "name" and self.peek(1)[1] == "=" and self.peek(2)[1] != "=":
            self.take()
            self.take("=")
            return Node("assign", val, [self.expr()])
        if kind == "op" and val == "{":  # {quoted name} = expr  /  {quoted name} as type
            j = self.i
            name = self.braced()
            if self.at("="):
                self.take("=")
                return Node("assign", name, [self.expr()])
            self.i = j
        node = self.expr()
        if self.at("as") and node.kind in ("col",):
            self.take("as")
            return Node("typed", node.value, type=self.type_name())
        return node

    def braced(self) -> str:
        self.take("{")
        parts = []
        while not self.at("}"):
            parts.append(self.take()[1])
        self.take("}")
        return " ".join(parts)

    def type_name(self) -> str:
        name = self.take()[1]
        if name.lower() == "decimal" and self.at("("):  # decimal(10,2); any other '(' after a type is a parameter's default
            self.take("(")
            nums = []
            while not self.at(")"):
                nums.append(self.take()[1])
            self.take(")")
            name += "(" + "".join(nums) + ")"
        if self.at("["):
            self.take("[")
            self.take("]")
            name += "[]"
        return name

    # -- expressions -----------------------------------------------------------------------------------------------------
    def expr(self, level: int = 0) -> Node:
        if level == len(self.PREC):
            return self.unary()
        left = self.expr(level + 1)
        while self.peek()[0] == "op" and self.peek()[1] in self.PREC[level]:
            op = self.take()[1]
            left = Node("binop", op, [left, self.expr(level + 1)])
        return left

    def unary(self) -> Node:
        if self.at("!") or self.at("-"):
            op = self.take()[1]
            return Node("unop", op, [self.unary()])
        return self.primary()

    def primary(self) -> Node:
        kind, val = self.peek()
        if kind == "str":
            self.take()
            return Node("str", bytes(val[1:-1], "utf-8").decode("unicode_escape") if "\\" in val else val[1:-1])
        if kind == "num":
            self.take()
            core = val.rstrip("lLdDfF")
            return Node("num", float(core) if ("." in core or "e" in core.lower() or val[-1] in "dDfF") else int(core))
        if val == "(":
            self.take("(")
            n = self.expr()
            self.take(")")
            return n
        if val == "[":
            self.take("[")
            items = self.arg_list("]")
            self.take("]")
            return Node("list", args=items)
        if val == "{":
            return Node("col", self.braced())
        if kind == "name":
            self.take()
            if val.startswith("$"):
                return Node("param", val.lstrip("$"))
            if val in ("true", "false") and not self.at("("):
                return Node("bool", val == "true")
            if self.at("("):
                self.take("(")
                args = self.arg_list(")")
                self.take(")")
                if val in ("true", "false") and not args:
                    return Node("bool", val == "true")
                return Node("call", val, args)
            if self.at("@") and self.peek(1)[0] in ("name",) or (self.at("@") and self.peek(1)[1] == "{"):
                self.take("@")
                col = self.braced() if self.at("{") else self.take()[1]
                return Node("col", col, stream=val)
            return Node("col", val)
        raise ScriptError(f"unexpected {val!r}")


def parse(text: str) -> Script:
    return _Parser(tokenize(text)).script()


# =====================================================================================================================
# 2. expressions -> pyspark.sql.functions
# =====================================================================================================================


class Untranslated(Exception):
    """A construct the fixed rules do not map: never guessed."""


def _lit(v: Any) -> str:
    return f"F.lit({v!r})"


_SIMPLE = {  # data flow function -> pyspark.sql.functions name, same arguments
    "upper": "upper", "toUpper": "upper", "lower": "lower", "toLower": "lower", "trim": "trim", "ltrim": "ltrim", "rtrim": "rtrim",
    "length": "length", "abs": "abs", "ceil": "ceil", "floor": "floor", "sqrt": "sqrt", "exp": "exp", "md5": "md5", "sha1": "sha1",
    "year": "year", "month": "month", "dayOfMonth": "dayofmonth", "dayOfWeek": "dayofweek", "dayOfYear": "dayofyear", "hour": "hour",
    "minute": "minute", "second": "second", "weekOfYear": "weekofyear", "quarter": "quarter", "reverse": "reverse",
    "initCap": "initcap", "isNaN": "isnan", "coalesce": "coalesce", "greatest": "greatest", "least": "least", "concat": "concat",
    "sum": "sum", "avg": "avg", "mean": "mean", "min": "min", "max": "max", "first": "first", "last": "last",
    "stddev": "stddev", "stddevSample": "stddev_samp", "stddevPopulation": "stddev_pop", "variance": "variance",
    "countDistinct": "countDistinct", "collect": "collect_list", "rowNumber": "row_number", "rank": "rank", "denseRank": "dense_rank",
    "cumeDist": "cume_dist", "percentRank": "percent_rank", "currentDate": "current_date", "currentTimestamp": "current_timestamp",
}
_CAST = {"toString": "string", "toInteger": "int", "toLong": "long", "toShort": "short", "toDouble": "double", "toFloat": "float",
         "toBoolean": "boolean"}
_TYPES = {"string": "StringType()", "integer": "IntegerType()", "int": "IntegerType()", "long": "LongType()", "short": "ShortType()",
          "double": "DoubleType()", "float": "FloatType()", "boolean": "BooleanType()", "date": "DateType()", "timestamp": "TimestampType()",
          "binary": "BinaryType()", "byte": "ByteType()"}


def spark_type(t: str) -> str:
    m = re.fullmatch(r"decimal\((\d+),(\d+)\)", t.replace(" ", ""))
    if m:
        return f"DecimalType({m.group(1)}, {m.group(2)})"
    if t.lower() not in _TYPES:
        raise Untranslated(f"type '{t}'")
    return _TYPES[t.lower()]


@dataclass
class ExprContext:
    aliases: dict[str, str] = field(default_factory=dict)  # stream name -> alias used in a join


def expr(n: Node, cx: Optional[ExprContext] = None) -> str:
    """The PySpark Column expression (code) for a data flow expression."""
    cx = cx or ExprContext()
    k = n.kind
    if k == "col":
        if n.stream:
            alias = cx.aliases.get(n.stream)
            if alias is None:
                raise Untranslated(f"column {n.stream}@{n.value} of a stream this step does not join")
            return f"F.col({alias + '.' + n.value!r})"
        return f"F.col({n.value!r})"
    if k == "str":
        return _lit(n.value)
    if k == "num":
        return _lit(n.value)
    if k == "bool":
        return _lit(bool(n.value))
    if k == "null":
        return "F.lit(None)"
    if k == "param":
        return f"F.lit({n.value})"
    if k == "unop":
        inner = expr(n.args[0], cx)
        return f"(~{inner})" if n.value == "!" else f"(-{inner})"
    if k == "binop":
        a, b = (expr(x, cx) for x in n.args)
        op = n.value
        if op == "+" and any(x.kind == "str" for x in n.args):
            return f"F.concat({a}, {b})"
        py = {"&&": "&", "||": "|", "==": "==", "!=": "!=", "<": "<", ">": ">", "<=": "<=", ">=": ">=", "+": "+", "-": "-", "*": "*",
              "/": "/", "%": "%"}.get(op)
        if op in ("===", "<=>"):
            return f"({a}).eqNullSafe({b})"
        if op == "^":
            return f"F.pow({a}, {b})"
        if py is None:
            raise Untranslated(f"operator '{op}'")
        return f"({a} {py} {b})"
    if k == "call":
        return _call(n, cx)
    raise Untranslated(f"expression of kind {k}")


def _call(n: Node, cx: ExprContext) -> str:
    f, args = n.value, n.args
    a = [expr(x, cx) for x in args if x.kind not in ("option", "list")]  # a list argument is read by the function that takes one
    if f in ("true", "false") and not args:
        return _lit(f == "true")
    if f == "null" and not args:
        return "F.lit(None)"
    if f == "iif":
        if len(a) == 2:
            a.append("F.lit(None)")
        return f"F.when({a[0]}, {a[1]}).otherwise({a[2]})"
    if f == "case":  # case(c1, v1, c2, v2, ..., [default])
        out = f"F.when({a[0]}, {a[1]})"
        rest = a[2:]
        while len(rest) >= 2:
            out += f".when({rest[0]}, {rest[1]})"
            rest = rest[2:]
        return out + (f".otherwise({rest[0]})" if rest else "")
    if f == "isNull":
        return f"{a[0]}.isNull()"
    if f == "not":
        return f"(~{a[0]})"
    if f in ("and", "or"):
        return "(" + (" & " if f == "and" else " | ").join(a) + ")"
    if f in ("equals", "notEquals", "greater", "lesser", "greaterOrEqual", "lesserOrEqual"):
        op = {"equals": "==", "notEquals": "!=", "greater": ">", "lesser": "<", "greaterOrEqual": ">=", "lesserOrEqual": "<="}[f]
        return f"({a[0]} {op} {a[1]})"
    if f in ("add", "minus", "multiply", "divide", "mod"):
        op = {"add": "+", "minus": "-", "multiply": "*", "divide": "/", "mod": "%"}[f]
        return f"({a[0]} {op} {a[1]})"
    if f == "count":
        return "F.count(F.lit(1))" if not a else f"F.count({a[0]})"
    if f == "countIf":
        return f"F.count(F.when({a[0]}, F.lit(1)))"
    if f in ("sumIf", "avgIf", "minIf", "maxIf"):
        return f"F.{f[:-2]}(F.when({a[0]}, {a[1]}))"
    if f in _CAST:
        if len(a) > 1:
            raise Untranslated(f"{f} with a format argument")
        return f"{a[0]}.cast({_CAST[f]!r})"
    if f in ("toDate", "toTimestamp"):
        fn = "to_date" if f == "toDate" else "to_timestamp"
        if len(a) == 1:
            return f"F.{fn}({a[0]})"
        if len(args) >= 2 and args[1].kind == "str":
            return f"F.{fn}({a[0]}, {_java_format(args[1].value)!r})"
        raise Untranslated(f"{f} with a computed format")
    if f == "toDecimal":
        if len(args) >= 3 and all(x.kind == "num" for x in args[1:3]):
            return f"{a[0]}.cast('decimal({args[1].value},{args[2].value})')"
        raise Untranslated("toDecimal without a literal precision and scale")
    if f == "round":
        return f"F.round({a[0]}, {args[1].value if len(args) > 1 and args[1].kind == 'num' else 0})"
    if f == "substring":  # data flow positions are 1-based like Spark's substring
        if len(a) == 2:
            return f"{a[0]}.substr({a[1]}, F.length({a[0]}))"
        return f"{a[0]}.substr({a[1]}, {a[2]})"
    if f == "left":
        return f"{a[0]}.substr(F.lit(1), {a[1]})"
    if f == "right":
        return f"{a[0]}.substr(F.length({a[0]}) - {a[1]} + 1, {a[1]})"
    if f == "replace":  # replace(text, find, [with]): F.replace exists from Spark 3.5 (Fabric runtime 1.3)
        return f"F.replace({a[0]}, {a[1]}, {a[2] if len(a) > 2 else _lit('')})"
    if f == "regexReplace":
        if len(args) >= 3 and args[1].kind == "str":
            return f"F.regexp_replace({a[0]}, {args[1].value!r}, {a[2]})"
        raise Untranslated("regexReplace with a computed pattern")
    if f in ("regexMatch", "rlike"):
        if len(args) >= 2 and args[1].kind == "str":
            return f"{a[0]}.rlike({args[1].value!r})"
        raise Untranslated(f"{f} with a computed pattern")
    if f == "like":
        if len(args) >= 2 and args[1].kind == "str":
            return f"{a[0]}.like({args[1].value!r})"
        raise Untranslated("like with a computed pattern")
    if f in ("startsWith", "endsWith"):
        return f"{a[0]}.{f}({a[1]})"
    if f == "contains" and len(args) == 2:
        return f"{a[0]}.contains({a[1]})"
    if f == "instr":
        if len(args) >= 2 and args[1].kind == "str":
            return f"F.instr({a[0]}, {args[1].value!r})"
        raise Untranslated("instr with a computed substring")
    if f == "split":
        if len(args) >= 2 and args[1].kind == "str":
            return f"F.split({a[0]}, {re.escape(args[1].value)!r})"
        raise Untranslated("split with a computed delimiter")
    if f == "in":  # in(['a','b'], col)
        if args and args[0].kind == "list" and len(args) == 2:
            return f"{expr(args[1], cx)}.isin({', '.join(expr(x, cx) for x in args[0].args)})"
        raise Untranslated("in() without a literal list")
    if f in ("addDays", "subDays"):
        return f"F.date_{'add' if f == 'addDays' else 'sub'}({a[0]}, {a[1]})"
    if f == "addMonths":
        return f"F.add_months({a[0]}, {a[1]})"
    if f == "daysBetween":  # daysBetween(from, to) = to - from in days
        return f"F.datediff({a[1]}, {a[0]})"
    if f == "toUTC" and len(a) == 1:
        return f"F.to_utc_timestamp({a[0]}, 'UTC')"
    if f == "byName":
        if args and args[0].kind == "str":
            return f"F.col({args[0].value!r})"
        raise Untranslated("byName with a computed column name")
    if f == "columnNames" or f == "byNames" or f == "each" or f == "match":
        raise Untranslated(f"schema-pattern function {f}()")
    if f in ("lag", "lead"):
        off = args[1].value if len(args) > 1 and args[1].kind == "num" else 1
        return f"F.{f}({a[0]}, {off})"
    if f in _SIMPLE:
        return f"F.{_SIMPLE[f]}({', '.join(a)})"
    raise Untranslated(f"function {f}()")


def _java_format(fmt: str) -> str:
    """Data flow date formats are Java SimpleDateFormat patterns, which Spark's datetime patterns accept for the common letters."""
    return fmt


# =====================================================================================================================
# 3. the notebook
# =====================================================================================================================


@dataclass
class Translation:
    cells: list[dict] = field(default_factory=list)  # ipynb cells
    parameters: list[tuple[str, str]] = field(default_factory=list)  # (name, python default literal)
    untranslated: list[str] = field(default_factory=list)  # "<step> (<transform>): <why>"
    reads: list[str] = field(default_factory=list)  # paths read
    writes: list[str] = field(default_factory=list)  # paths written
    notes: list[str] = field(default_factory=list)  # review items that are translated but worth a look
    drafted: list[str] = field(default_factory=list)  # steps whose code is a model draft a person accepted (agent/assist/dataflow_steps.py)


def _code_cell(src: str, tags: Optional[list[str]] = None) -> dict:
    return {"cell_type": "code", "source": src.splitlines(keepends=True), "execution_count": None, "outputs": [],
            "metadata": {"tags": tags} if tags else {}}


def _opt(args: list[Node], key: str) -> Optional[Node]:
    return next((x.args[0] for x in args if x.kind == "option" and x.value == key), None)


def _opt_value(args: list[Node], key: str, default: Any = None) -> Any:
    n = _opt(args, key)
    if n is None:
        return default
    if n.kind in ("str", "num", "bool"):
        return n.value
    if n.kind == "call" and n.value in ("true", "false") and not n.args:
        return n.value == "true"
    if n.kind == "list":
        return [x.value for x in n.args if x.kind in ("str", "num", "bool")]
    return None


def _var(stream: str) -> str:
    return "df_" + re.sub(r"[^A-Za-z0-9_]", "_", stream.replace("@", "__"))


def dataset_path(ds: dict, endpoint: Optional[str]) -> Optional[str]:
    """abfss path of a file dataset on ADLS Gen2 (or Blob), from its location and its linked service's endpoint."""
    loc = (ds.get("type_properties") or {}).get("location") or {}
    if not isinstance(loc, dict) or loc.get("type") not in ("AzureBlobFSLocation", "AzureBlobStorageLocation"):
        return None
    container = loc.get("fileSystem") or loc.get("container")
    if not isinstance(container, str) or not endpoint:
        return None
    host = re.sub(r"^https?://", "", endpoint).split("/")[0]
    account = host.split(".")[0]
    parts = [p for p in (loc.get("folderPath"), loc.get("fileName")) if isinstance(p, str) and p]
    if any(not isinstance(loc.get(k), (str, type(None))) for k in ("folderPath", "fileName")):
        return None  # computed (an expression): not a literal path
    return f"abfss://{container}@{account}.dfs.core.windows.net/" + "/".join(p.strip("/") for p in parts)


def translate(script_text: str, nodes: dict[str, dict], datasets: dict[str, dict], endpoints: dict[str, str],
              drafts: Optional[dict[str, dict]] = None) -> Translation:
    """The notebook for a data flow. ``nodes``: source/sink name -> its dataset name (``typeProperties.sources|sinks``);
    ``datasets``: dataset name (lower) -> the discovery record (dict); ``endpoints``: linked service name (lower) -> its endpoint URL.
    ``drafts``: step name -> an accepted model draft (``code``, ``accepted_by``, ``accepted_at``) used for a step the fixed rules cannot
    translate, instead of a cell that raises; each one is marked in the cell and listed in ``drafted``."""
    tr = Translation()
    script = parse(script_text)
    for name, typ, default in script.parameters:
        val = default.value if default is not None and default.kind in ("str", "num", "bool") else None
        tr.parameters.append((name, repr(val if val is not None else "")))
        if default is not None and default.kind not in ("str", "num", "bool"):
            tr.notes.append(f"parameter ${name} has a computed default; the notebook's default is empty: pass it from the pipeline")
    if tr.parameters:
        tr.cells.append(_code_cell("# Parameters: the pipeline's notebook activity overrides these values by name.\n"
                                   + "".join(f"{n} = {d}\n" for n, d in tr.parameters), ["parameters"]))
    tr.cells.append(_code_cell("# Generated by adf2fabric from the data flow script (fixed translation rules).\n"
                               "from pyspark.sql import functions as F\nfrom pyspark.sql import Window\n"
                               "from pyspark.sql.types import *  # noqa: F403 - the declared source schemas\n"))
    for st in script.statements:
        try:
            code = _step(st, nodes, datasets, endpoints, tr)
        except (Untranslated, ScriptError, KeyError, IndexError) as exc:
            why = str(exc) or type(exc).__name__
            draft = (drafts or {}).get(st.name)
            if draft and draft.get("code") and st.transform not in ("source", "sink"):  # storage I/O is never model-written
                tr.drafted.append(f"{st.name} ({st.transform})")
                tr.notes.append(f"step {st.name} ({st.transform}) runs model-drafted code accepted by {draft.get('accepted_by') or '?'}: review it")
                code = (f"# [adf2fabric] MODEL-DRAFTED: step '{st.name}' ({st.transform}) is not translated by the fixed rules ({why}).\n"
                        f"# This code was drafted by a model and accepted by {draft.get('accepted_by') or '?'} on {draft.get('accepted_at') or '?'}.\n"
                        + draft["code"].rstrip() + "\n")
            else:
                tr.untranslated.append(f"{st.name} ({st.transform}): {why}")
                code = (f"# [adf2fabric] step '{st.name}' ({st.transform}) is not translated by the fixed rules: {why}.\n"
                        f"raise NotImplementedError({f'data flow step {st.name} ({st.transform}) is not translated: {why}'!r})\n")
        tr.cells.append(_code_cell(f"# {st.name}: {st.transform}" + (f" of {', '.join(st.inputs)}" if st.inputs else "") + "\n" + code))
    return tr


def _src(st: Statement) -> str:
    return _var(st.inputs[0])


def _step(st: Statement, nodes: dict[str, dict], datasets: dict[str, dict], endpoints: dict[str, str], tr: Translation) -> str:
    t, args, out = st.transform, st.args, _var(st.name)
    if t == "source":
        return _source(st, nodes, datasets, endpoints, tr)
    if t == "sink":
        return _sink(st, nodes, datasets, endpoints, tr)
    if t == "filter":
        return f"{out} = {_src(st)}.filter({expr(args[0])})\n"
    if t == "derive":
        cols = []
        for a in args:
            if a.kind != "assign":
                raise Untranslated("a derived column that is not 'name = expression' (column patterns)")
            cols.append(f"    .withColumn({a.value!r}, {expr(a.args[0])})")
        return f"{out} = (\n    {_src(st)}\n" + "\n".join(cols) + "\n)\n"
    if t == "select":
        mc = next((a for a in args if a.kind == "call" and a.value == "mapColumn"), None)
        if mc is None:
            raise Untranslated("select without mapColumn")
        cols = []
        for c in mc.args:
            if c.kind == "col" and not c.stream:
                cols.append(f"F.col({c.value!r})")
            elif c.kind == "assign" and c.args[0].kind == "col":
                src = c.args[0]
                cols.append(f"F.col({(src.value)!r}).alias({c.value!r})")
            else:
                raise Untranslated("a select mapping that is not 'column' or 'new = column' (rule-based mapping)")
        return f"{out} = {_src(st)}.select({', '.join(cols)})\n"
    if t == "aggregate":
        gb = next((a for a in args if a.kind == "call" and a.value == "groupBy"), None)
        aggs = [a for a in args if a.kind == "assign"]
        if not aggs:
            raise Untranslated("an aggregate with no 'name = expression' (column patterns)")
        keys = ", ".join(expr(g) for g in (gb.args if gb else []))
        agg = ", ".join(f"({expr(a.args[0])}).alias({a.value!r})" for a in aggs)
        return f"{out} = {_src(st)}" + (f".groupBy({keys})" if keys else "") + f".agg({agg})\n"
    if t in ("join", "lookup", "exists"):
        if len(st.inputs) != 2:
            raise Untranslated(f"{t} with {len(st.inputs)} inputs")
        left, right = st.inputs
        aliases = {left.split("@")[0]: "L", right.split("@")[0]: "R"}
        cond_node = next((a for a in args if a.kind not in ("option",)), None)
        cross = _opt_value(args, "joinType") == "cross"
        cond = expr(cond_node, ExprContext(aliases)) if cond_node is not None and not cross else None
        how = {"inner": "inner", "outer": "full", "left": "left", "right": "right", "cross": "cross"}
        if t == "join":
            jt = _opt_value(args, "joinType", "inner")
            if jt not in how:
                raise Untranslated(f"joinType '{jt}'")
            cond_arg = f", {cond}" if cond else ""
            return f"{out} = {_var(left)}.alias('L').join({_var(right)}.alias('R'){cond_arg}, {how[jt]!r})\n"
        if t == "exists":
            neg = bool(_opt_value(args, "negate", False))
            return f"{out} = {_var(left)}.alias('L').join({_var(right)}.alias('R'), {cond}, {'left_anti' if neg else 'left_semi'!r})\n"
        if _opt_value(args, "multiple", False) is not True:
            tr.notes.append(f"{st.name} (lookup): ADF keeps one match per row ({_opt_value(args, 'pickup', 'any')}); the left join keeps every "
                            "match. Check the right side has one row per key, or de-duplicate it first")
        return f"{out} = {_var(left)}.alias('L').join({_var(right)}.alias('R'), {cond}, 'left')\n"
    if t == "union":
        if len(st.inputs) < 2:
            raise Untranslated("union with fewer than two inputs")
        by_name = _opt_value(args, "byName", True)
        first, rest = _var(st.inputs[0]), [_var(x) for x in st.inputs[1:]]
        chain = "".join(f".unionByName({r}, allowMissingColumns=True)" if by_name else f".union({r})" for r in rest)
        return f"{out} = {first}{chain}\n"
    if t == "split":
        conds = [expr(a) for a in args if a.kind != "option"]
        if not st.outputs or len(st.outputs) not in (len(conds), len(conds) + 1):
            raise Untranslated("a conditional split whose named outputs do not match its conditions")
        disjoint = bool(_opt_value(args, "disjoint", False))
        src = _src(st)
        lines = []
        for i, c in enumerate(conds):
            earlier = conds[:i]
            guard = (" & ".join(f"~F.coalesce({e}, F.lit(False))" for e in earlier) + " & ") if (disjoint is False and earlier) else ""
            # disjoint: false (ADF's default) sends a row to the FIRST matching stream only
            lines.append(f"{_var(st.name + '@' + st.outputs[i])} = {src}.filter({guard}F.coalesce({c}, F.lit(False)))")
        if len(st.outputs) == len(conds) + 1:
            none = " & ".join(f"~F.coalesce({c}, F.lit(False))" for c in conds)
            lines.append(f"{_var(st.name + '@' + st.outputs[-1])} = {src}.filter({none})")
        if disjoint:
            tr.notes.append(f"{st.name} (split): disjoint: true sends a row to every matching stream; translated as written")
        return "\n".join(lines) + "\n"
    if t == "sort":
        keys = []
        for a in args:
            if a.kind == "call" and a.value in ("asc", "desc"):
                col = expr(a.args[0])
                nulls_first = a.args[1].value if len(a.args) > 1 and a.args[1].kind == "bool" else (a.value == "asc")
                keys.append(f"{col}.{a.value}_nulls_{'first' if nulls_first else 'last'}()")
            elif a.kind != "option":
                raise Untranslated("a sort key that is not asc()/desc()")
        return f"{out} = {_src(st)}.orderBy({', '.join(keys)})\n"
    if t == "window":
        over = next((a for a in args if a.kind == "call" and a.value == "over"), None)
        orders = [a for a in args if a.kind == "call" and a.value in ("asc", "desc")]
        cols = [a for a in args if a.kind == "assign"]
        w = "Window" + (f".partitionBy({', '.join(expr(x) for x in over.args)})" if over else "")
        if orders:
            w += ".orderBy(" + ", ".join(f"{expr(o.args[0])}.{o.value}()" for o in orders) + ")"
        lines = [f"_w = {w}", f"{out} = {_src(st)}"]
        for c in cols:
            lines[-1] += f".withColumn({c.value!r}, ({expr(c.args[0])}).over(_w))"
        return "\n".join(lines) + "\n"
    if t == "keyGenerate":
        out_decl = next((a for a in args if a.kind == "call" and a.value == "output"), None)
        name = out_decl.args[0].value if out_decl and out_decl.args else "SurrogateKey"
        start = _opt_value(args, "startAt", 1)
        return (f"{out} = {_src(st)}.withColumn({name!r}, F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) "
                f"+ F.lit({int(start) - 1}))\n")
    raise Untranslated(f"transformation '{t}'")


def _schema(args: list[Node]) -> Optional[str]:
    out = next((a for a in args if a.kind == "call" and a.value == "output"), None)
    if out is None or not out.args:
        return None
    fields = []
    for c in out.args:
        if c.kind != "typed":
            raise Untranslated("a source column without a simple type")
        fields.append(f"StructField({c.value!r}, {spark_type(c.type)}, True)")
    return "StructType([" + ", ".join(fields) + "])"


def _dataset_for(st: Statement, nodes: dict[str, dict], datasets: dict[str, dict]) -> dict:
    ref = nodes.get(st.name)
    if not ref:
        raise Untranslated(f"'{st.name}' names no dataset (an inline dataset is not translated)")
    ds = datasets.get(ref.lower())
    if ds is None:
        raise Untranslated(f"dataset '{ref}' is not in the source")
    return ds


def _path(ds: dict, endpoints: dict[str, str]) -> str:
    ls = ((ds.get("linked_service_ref") or {}).get("name") or "").lower()
    path = dataset_path(ds, endpoints.get(ls))
    if path is None:
        raise Untranslated(f"dataset '{ds.get('name')}' is not a file dataset with a literal location on ADLS Gen2 or Blob")
    return path


def _source(st: Statement, nodes: dict[str, dict], datasets: dict[str, dict], endpoints: dict[str, str], tr: Translation) -> str:
    ds = _dataset_for(st, nodes, datasets)
    path, tp, fmt = _path(ds, endpoints), ds.get("type_properties") or {}, ds.get("type")
    wild = _opt_value(st.args, "wildcardPaths")
    if wild:
        if not isinstance(wild, list) or len(wild) != 1:
            raise Untranslated("more than one wildcard path")
        container = path.split("/", 3)[2]
        path = f"abfss://{container}/{str(wild[0]).lstrip('/')}"
    schema = _schema(st.args)
    out = _var(st.name)
    tr.reads.append(path)
    if fmt == "DelimitedText":
        opts = {"header": str(bool(tp.get("firstRowAsHeader", False))).lower(), "sep": tp.get("columnDelimiter", ","),
                "quote": tp.get("quoteChar", '"'), "escape": tp.get("escapeChar", "\\"), "encoding": tp.get("encodingName") or "UTF-8"}
        if not all(isinstance(v, str) for v in opts.values()):
            raise Untranslated("a delimited-text option that is an expression")
        reader = "spark.read" + "".join(f".option({k!r}, {v!r})" for k, v in opts.items())
        reader += f".schema({schema})" if schema else ".option('inferSchema', 'true')"
        return f"{out} = {reader}.csv({path!r})\n"
    if fmt == "Parquet":
        code = f"{out} = spark.read.parquet({path!r})\n"
    elif fmt == "Json":
        code = f"{out} = spark.read.json({path!r})\n"
    else:
        raise Untranslated(f"a {fmt} source dataset")
    if schema:  # a typed source: keep the declared columns and types
        code += f"{out} = {out}.select([F.col(f.name).cast(f.dataType) for f in {schema}.fields])\n"
    return code


def _sink(st: Statement, nodes: dict[str, dict], datasets: dict[str, dict], endpoints: dict[str, str], tr: Translation) -> str:
    ds = _dataset_for(st, nodes, datasets)
    path, tp, fmt = _path(ds, endpoints), ds.get("type_properties") or {}, ds.get("type")
    mode = "overwrite" if _opt_value(st.args, "truncate", False) else "append"
    for unsupported in ("insertable", "updateable", "upsertable", "deletable"):
        if _opt_value(st.args, unsupported) is True:
            raise Untranslated(f"a sink with {unsupported}: true (row-level writes)")
    if ((tp.get("location") or {}).get("fileName")):
        tr.notes.append(f"{st.name} (sink): the dataset names a file ({tp['location']['fileName']}); Spark writes a folder of part files there")
    src = _src(st)
    tr.writes.append(path)
    if fmt == "Parquet":
        return f"{src}.write.mode({mode!r}).parquet({path!r})\n"
    if fmt == "DelimitedText":
        opts = {"header": str(bool(tp.get("firstRowAsHeader", False))).lower(), "sep": tp.get("columnDelimiter", ",")}
        return f"{src}.write.mode({mode!r})" + "".join(f".option({k!r}, {v!r})" for k, v in opts.items()) + f".csv({path!r})\n"
    if fmt == "Json":
        return f"{src}.write.mode({mode!r}).json({path!r})\n"
    raise Untranslated(f"a {fmt} sink dataset")


def script_text(type_properties: dict) -> Optional[str]:
    """The data flow script of a MappingDataFlow's typeProperties (``script`` or ``scriptLines``)."""
    lines = type_properties.get("scriptLines")
    if isinstance(lines, list) and all(isinstance(x, str) for x in lines):
        return "\n".join(lines)
    s = type_properties.get("script")
    return s if isinstance(s, str) else None


def nodes_of(type_properties: dict) -> dict[str, str]:
    """source/sink name -> the dataset it uses (``typeProperties.sources|sinks``)."""
    out = {}
    for key in ("sources", "sinks"):
        for n in type_properties.get(key) or []:
            if isinstance(n, dict) and isinstance(n.get("name"), str) and isinstance((n.get("dataset") or {}).get("referenceName"), str):
                out[n["name"]] = n["dataset"]["referenceName"]
    return out


def dumps(tr: Translation) -> str:
    return json.dumps({"untranslated": tr.untranslated, "notes": tr.notes, "reads": tr.reads, "writes": tr.writes}, indent=1)
