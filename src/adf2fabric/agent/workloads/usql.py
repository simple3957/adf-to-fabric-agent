"""U-SQL script -> PySpark notebook cells, by fixed rules (2026-11-17, the user's decision: "yes, fixed rules").

Azure Data Lake Analytics is retired, but the *script* an ADF U-SQL activity runs is still ordinary text in storage (``scriptPath`` in
``scriptLinkedService``). This module translates the part of U-SQL that has a faithful Spark equivalent, the same way
``dataflow.py`` translates mapping data flows:

* ``DECLARE EXTERNAL @p type = literal;``  -> a notebook parameter (the activity's ``parameters`` override it by name; a name that is a
  Python keyword, such as ``in``, gets a trailing ``_``, see ``param_name``), cast to its declared type;
* ``@rs = EXTRACT col type, ... FROM <path> USING Extractors.Csv|Tsv|Text(...)``  -> ``spark.read`` with the declared schema;
* ``@rs = SELECT [DISTINCT] items FROM @x [AS a] [INNER|LEFT|RIGHT|FULL [OUTER]|CROSS JOIN @y [AS b] ON ...] [WHERE ...] [GROUP BY ...]
  [HAVING ...] [ORDER BY ...] [FETCH n]``  -> DataFrame operations, with C# expressions (``? :``, ``??``, ``==``, ``&&``, string
  methods, aggregates) mapped to ``pyspark.sql.functions``;
* ``OUTPUT @rs TO <path> USING Outputters.Csv|Tsv|Text(...)``  -> one file at the path (U-SQL writes a file; Spark writes a folder, so
  the part file is moved).

Anything else (``PROCESS`` / ``REDUCE`` / ``COMBINE``, user-defined operators and assemblies, ``CREATE TABLE``, window functions,
``UNION``, DateTime arithmetic, ...) is named and left as a cell that raises, never guessed. Paths in a U-SQL script are relative to the
account's default store, which is not part of the ADF source: they are read from the root the caller names (the script's own storage
container) and that assumption is a review finding.
"""

from __future__ import annotations

import keyword
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from adf2fabric.agent.workloads.dataflow import _code_cell, _var

NOTEBOOK_FORMATS = ("csv", "tsv", "text")
_TYPES = {"string": ("StringType()", "string"), "int": ("IntegerType()", "num"), "long": ("LongType()", "num"),
          "short": ("ShortType()", "num"), "byte": ("ByteType()", "num"), "double": ("DoubleType()", "num"),
          "float": ("FloatType()", "num"), "decimal": ("DecimalType(38, 18)", "num"), "bool": ("BooleanType()", "bool"),
          "boolean": ("BooleanType()", "bool"), "datetime": ("TimestampType()", "date"), "guid": ("StringType()", "string")}
_AGG = {"sum": "F.sum", "min": "F.min", "max": "F.max", "avg": "F.avg", "count": "F.count"}
_TOKEN = re.compile(r'''\s*(?:(?P<str>@?"(?:[^"\\]|\\.)*")|(?P<num>\d+(?:\.\d+)?[lLmMfFdD]?)|(?P<par>@[A-Za-z_]\w*)'''
                    r'''|(?P<id>[A-Za-z_]\w*)|(?P<op>==|!=|<>|<=|>=|&&|\|\||\?\?|[-+*/%<>!?:(),.=*]))''')


class Untranslated(Exception):
    pass


def param_name(n: str) -> str:
    """The notebook variable for a U-SQL parameter (``@in`` -> ``in_``): a Python keyword cannot be assigned."""
    n = re.sub(r"\W", "_", n.lstrip("@"))
    return n + "_" if keyword.iskeyword(n) or n in ("F", "spark") else n


@dataclass
class Tok:
    kind: str
    text: str


def _tokens(text: str) -> list[Tok]:
    out, pos = [], 0
    text = text.strip()
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise Untranslated(f"unreadable text near {text[pos:pos + 20]!r}")
        pos = m.end()
        kind = m.lastgroup or ""
        out.append(Tok(kind, m.group(kind)))
    return out


def _strip(script: str) -> str:
    """The script without // and /* */ comments (a string keeps its // text)."""
    out, i, n = [], 0, len(script)
    while i < n:
        c = script[i]
        if c == '"':
            j = i + 1
            while j < n and script[j] != '"':
                j += 2 if script[j] == "\\" else 1
            out.append(script[i:j + 1])
            i = j + 1
        elif script.startswith("//", i):
            while i < n and script[i] != "\n":
                i += 1
        elif script.startswith("/*", i):
            j = script.find("*/", i + 2)
            i = n if j < 0 else j + 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


def _statements(script: str) -> list[str]:
    out, cur, in_s, i, text = [], [], False, 0, _strip(script)
    while i < len(text):
        c = text[i]
        if c == '"':
            in_s = not in_s if not (i and text[i - 1] == "\\") else in_s
        if c == ";" and not in_s:
            s = "".join(cur).strip()
            if s:
                out.append(s)
            cur = []
        else:
            cur.append(c)
        i += 1
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


def _lit(t: Tok) -> Any:
    if t.kind == "str":
        body = t.text[2:-1] if t.text.startswith("@") else bytes(t.text[1:-1], "utf-8").decode("unicode_escape")
        return body
    if t.kind == "num":
        s = t.text.rstrip("lLmMfFdD")
        return float(s) if "." in s else int(s)
    if t.kind == "id" and t.text.lower() in ("true", "false"):
        return t.text.lower() == "true"
    raise Untranslated(f"{t.text!r} is not a literal")


def fetch_script(host: str, container: str, path: str) -> tuple[Optional[str], str]:
    """(the script text, why not): a read-only GET of one file from ADLS Gen2 with a token of each account signed in to ``az`` (the
    person's own storage access; nothing is written). The first account that can read it wins."""
    import requests

    from adf2fabric.connections.providers.azure_cli import AzureCliProvider, ProviderError

    cli = AzureCliProvider()
    tried: list[str] = []
    try:
        accounts = [a.username for a in cli.accounts()]
    except ProviderError as exc:
        return None, str(exc)
    for user in accounts:
        try:
            token = cli.token(account=user, resource="https://storage.azure.com/")
        except ProviderError as exc:
            tried.append(f"{user}: {exc}")
            continue
        try:
            r = requests.get(f"https://{host}/{container}/{path}", headers={"Authorization": f"Bearer {token}", "x-ms-version": "2023-11-03"}, timeout=60)
        except requests.RequestException as exc:
            tried.append(f"{user}: {type(exc).__name__}")
            continue
        if r.status_code == 200:
            return r.content.decode("utf-8-sig", errors="replace"), ""
        tried.append(f"{user}: HTTP {r.status_code}")
    return None, "No signed-in Azure account could read it. Tried: " + "; ".join(tried or ["no account is signed in"])


# ---------------------------------------------------------------------------------------------------------------------
# expressions
# ---------------------------------------------------------------------------------------------------------------------


@dataclass
class Ex:
    code: str
    type: Optional[str] = None  # string | num | bool | date | None (unknown)
    agg: bool = False
    name: Optional[str] = None  # the column name when this is a plain column reference


@dataclass
class Ctx:
    cols: dict[str, Optional[str]] = field(default_factory=dict)  # column -> type, of the rowset being read
    aliases: set[str] = field(default_factory=set)
    params: dict[str, str] = field(default_factory=dict)  # @name -> python variable
    notes: list[str] = field(default_factory=list)


class _P:
    def __init__(self, toks: list[Tok], ctx: Ctx):
        self.t, self.i, self.ctx = toks, 0, ctx

    def peek(self, k: int = 0) -> Optional[Tok]:
        return self.t[self.i + k] if self.i + k < len(self.t) else None

    def eat(self, text: Optional[str] = None) -> Tok:
        t = self.peek()
        if t is None or (text is not None and t.text.lower() != text.lower()):
            raise Untranslated(f"expected {text!r}")
        self.i += 1
        return t

    def is_(self, *texts: str) -> bool:
        t = self.peek()
        return t is not None and t.kind in ("op", "id") and t.text.lower() in texts

    def done(self) -> bool:
        return self.i >= len(self.t)

    # precedence climbing -------------------------------------------------------------------------------------------
    def expr(self) -> Ex:
        c = self.coalesce()
        if self.is_("?"):
            self.eat()
            a = self.expr()
            self.eat(":")
            b = self.expr()
            return Ex(f"F.when({c.code}, {a.code}).otherwise({b.code})", a.type or b.type, c.agg or a.agg or b.agg)
        return c

    def coalesce(self) -> Ex:
        a = self.or_()
        while self.is_("??"):
            self.eat()
            b = self.or_()
            a = Ex(f"F.coalesce({a.code}, {b.code})", a.type or b.type, a.agg or b.agg)
        return a

    def or_(self) -> Ex:
        a = self.and_()
        while self.is_("||", "or"):
            self.eat()
            b = self.and_()
            a = Ex(f"({a.code} | {b.code})", "bool", a.agg or b.agg)
        return a

    def and_(self) -> Ex:
        a = self.not_()
        while self.is_("&&", "and"):
            self.eat()
            b = self.not_()
            a = Ex(f"({a.code} & {b.code})", "bool", a.agg or b.agg)
        return a

    def not_(self) -> Ex:
        if self.is_("!", "not"):
            self.eat()
            a = self.not_()
            return Ex(f"(~{a.code})", "bool", a.agg)
        return self.cmp()

    def cmp(self) -> Ex:
        a = self.add()
        if self.is_("is"):
            self.eat()
            neg = self.is_("not")
            if neg:
                self.eat()
            self.eat("null")
            return Ex(f"{a.code}.{'isNotNull' if neg else 'isNull'}()", "bool", a.agg)
        if self.is_("in"):
            self.eat()
            self.eat("(")
            vals = [self.expr()]
            while self.is_(","):
                self.eat()
                vals.append(self.expr())
            self.eat(")")
            return Ex(f"{a.code}.isin({', '.join(v.code.removeprefix('F.lit(').removesuffix(')') if v.code.startswith('F.lit(') else v.code for v in vals)})", "bool", a.agg)
        if self.is_("like"):
            self.eat()
            b = self.add()
            return Ex(f"{a.code}.like({b.code.removeprefix('F.lit(').removesuffix(')') if b.code.startswith('F.lit(') else b.code})", "bool", a.agg)
        if self.is_("==", "!=", "<>", "<", "<=", ">", ">="):
            op = self.eat().text
            b = self.add()
            py = {"==": "==", "!=": "!=", "<>": "!="}.get(op, op)
            if b.code == "F.lit(None)" or a.code == "F.lit(None)":
                side = a if b.code == "F.lit(None)" else b
                return Ex(f"{side.code}.{'isNull' if py == '==' else 'isNotNull'}()", "bool", side.agg)
            return Ex(f"({a.code} {py} {b.code})", "bool", a.agg or b.agg)
        return a

    def add(self) -> Ex:
        a = self.mul()
        while self.is_("+", "-"):
            op = self.eat().text
            b = self.mul()
            if op == "+" and "string" in (a.type, b.type):
                a = Ex(f"F.concat({a.code}, {b.code})", "string", a.agg or b.agg)
                continue
            if op == "+" and a.type is None and b.type is None:
                self.ctx.notes.append("a '+' between values of unknown type is numeric addition here; C# concatenates strings")
            a = Ex(f"({a.code} {op} {b.code})", a.type if a.type == b.type else (a.type or b.type), a.agg or b.agg)
        return a

    def mul(self) -> Ex:
        a = self.unary()
        while self.is_("*", "/", "%"):
            op = self.eat().text
            b = self.unary()
            if op == "/":
                self.ctx.notes.append("a '/' is C# division (integer when both sides are integers); Spark '/' gives a decimal: check the result")
            a = Ex(f"({a.code} {op} {b.code})", "num", a.agg or b.agg)
        return a

    def unary(self) -> Ex:
        if self.is_("-"):
            self.eat()
            a = self.unary()
            return Ex(f"(-{a.code})", "num", a.agg)
        return self.postfix(self.primary())

    def postfix(self, a: Ex) -> Ex:
        while self.is_(".") and self.peek(1) is not None and self.peek(1).kind == "id":
            self.eat()
            m = self.eat().text
            args: list[Ex] = []
            if self.is_("("):
                self.eat()
                while not self.is_(")"):
                    args.append(self.expr())
                    if self.is_(","):
                        self.eat()
                self.eat(")")
            a = _method(a, m, args, bool(args) or self.t[self.i - 1].text == ")")
        return a

    def primary(self) -> Ex:
        t = self.peek()
        if t is None:
            raise Untranslated("the expression ends too early")
        if t.kind == "op" and t.text == "(":
            self.eat()
            if self.peek() and self.peek().kind == "id" and self.peek().text.lower() in _TYPES and self.peek(1) and self.peek(1).text == ")":
                raise Untranslated("a C# cast")
            e = self.expr()
            self.eat(")")
            return Ex(f"({e.code})", e.type, e.agg)
        if t.kind == "str":
            self.eat()
            return Ex(f"F.lit({_lit(t)!r})", "string")
        if t.kind == "num":
            self.eat()
            if t.text[-1] in "mM":
                raise Untranslated("a decimal literal")
            return Ex(f"F.lit({_lit(t)!r})", "num")
        if t.kind == "par":
            self.eat()
            v = self.ctx.params.get(t.text)
            if v is None:
                raise Untranslated(f"{t.text} is not declared")
            return Ex(f"F.lit({v})", None)
        if t.kind == "id":
            low = t.text.lower()
            if low in ("true", "false"):
                self.eat()
                return Ex(f"F.lit({low == 'true'})", "bool")
            if low == "null":
                self.eat()
                return Ex("F.lit(None)")
            self.eat()
            if self.is_("("):
                return self.call(t.text)
            if low == "string" and self.is_(".") and self.peek(1) and self.peek(1).text.lower() == "isnullorempty":
                self.eat()
                self.eat()
                self.eat("(")
                x = self.expr()
                self.eat(")")
                return Ex(f"({x.code}.isNull() | ({x.code} == F.lit('')))", "bool", x.agg)
            if self.is_(".") and t.text in self.ctx.aliases and self.peek(1) is not None and self.peek(1).kind == "id":
                self.eat()
                col = self.eat().text
                return Ex(f"F.col({t.text + '.' + col!r})", self.ctx.cols.get(col), name=col)
            if t.text not in self.ctx.cols and self.ctx.cols:
                raise Untranslated(f"column '{t.text}' is not in the rowset")
            return Ex(f"F.col({t.text!r})", self.ctx.cols.get(t.text), name=t.text)
        raise Untranslated(f"unexpected {t.text!r}")

    def window(self, name: str, low: str, args: list[Ex], distinct: bool) -> Ex:
        if distinct:
            raise Untranslated("COUNT(DISTINCT ...) OVER")
        spec = _window_spec(self)
        if low in ("row_number", "rank", "dense_rank"):
            if args:
                raise Untranslated(f"{name}() takes no arguments")
            if "orderBy" not in spec:
                raise Untranslated(f"{name}() OVER without ORDER BY")
            return Ex(f"F.{low}().over({spec})", "num")
        if low in _AGG and len(args) == 1:
            if args[0].agg:
                raise Untranslated("an aggregate inside a window aggregate")
            if low == "count" and args[0].code == "F.lit(1)":
                return Ex(f"F.count(F.lit(1)).over({spec})", "num")
            return Ex(f"{_AGG[low]}({args[0].code}).over({spec})", "num")
        raise Untranslated(f"function {name}() OVER")

    def call(self, name: str) -> Ex:
        low = name.lower()
        self.eat("(")
        if low == "count" and self.is_("*"):
            self.eat()
            self.eat(")")
            if self.is_("over"):
                return self.window(name, low, [Ex("F.lit(1)")], False)
            return Ex("F.count(F.lit(1))", "num", True)
        distinct = False
        if low == "count" and self.is_("distinct"):
            self.eat()
            distinct = True
        args: list[Ex] = []
        while not self.is_(")"):
            args.append(self.expr())
            if self.is_(","):
                self.eat()
        self.eat(")")
        if self.is_("over"):
            return self.window(name, low, args, distinct)
        if low in ("row_number", "rank", "dense_rank"):
            raise Untranslated(f"{name}() without OVER")
        if low in _AGG and len(args) == 1:
            if any(x.agg for x in args):
                raise Untranslated("an aggregate inside an aggregate")
            fn = "F.countDistinct" if distinct else _AGG[low]
            return Ex(f"{fn}({args[0].code})", "num", True)
        raise Untranslated(f"function {name}()")


def _window_spec(p: "_P") -> str:
    """``OVER (PARTITION BY a, b ORDER BY c [DESC], ...)`` -> ``Window.partitionBy(..).orderBy(..)`` (the tokens up to the closing parenthesis)."""
    p.eat("over")
    p.eat("(")
    parts, orders = [], []
    if p.is_("partition"):
        p.eat()
        p.eat("by")
        parts.append(p.expr().code)
        while p.is_(","):
            p.eat()
            parts.append(p.expr().code)
    if p.is_("order"):
        p.eat()
        p.eat("by")
        while True:
            e = p.expr()
            desc = p.is_("desc")
            if p.is_("asc", "desc"):
                p.eat()
            orders.append(e.code + (".desc()" if desc else ".asc()"))
            if not p.is_(","):
                break
            p.eat()
    p.eat(")")
    spec = "Window.partitionBy(" + ", ".join(parts) + ")" if parts else "Window.partitionBy()"
    return spec + (".orderBy(" + ", ".join(orders) + ")" if orders else "")


def _method(a: Ex, m: str, args: list[Ex], called: bool) -> Ex:
    low = m.lower()
    lit = lambda x: x.code[6:-1] if x.code.startswith("F.lit(") else None  # noqa: E731
    if low == "length" and not called:
        return Ex(f"F.length({a.code})", "num", a.agg)
    simple = {"toupper": "F.upper", "tolower": "F.lower", "trim": "F.trim"}
    if low in simple and not args:
        return Ex(f"{simple[low]}({a.code})", "string", a.agg)
    if low in ("contains", "startswith", "endswith") and len(args) == 1:
        py = {"contains": "contains", "startswith": "startswith", "endswith": "endswith"}[low]
        return Ex(f"{a.code}.{py}({args[0].code})", "bool", a.agg)
    if low == "substring" and len(args) in (1, 2) and all(lit(x) and lit(x).lstrip("-").isdigit() for x in args):
        start = int(lit(args[0])) + 1
        n = f", {int(lit(args[1]))}" if len(args) == 2 else ", 2147483647"
        return Ex(f"{a.code}.substr({start}{n})", "string", a.agg)
    if low == "replace" and len(args) == 2 and all(lit(x) is not None for x in args):
        return Ex(f"F.regexp_replace({a.code}, re.escape({lit(args[0])}), {lit(args[1])})", "string", a.agg)
    raise Untranslated(f"method .{m}()")


# ---------------------------------------------------------------------------------------------------------------------
# statements
# ---------------------------------------------------------------------------------------------------------------------


@dataclass
class Translation:
    cells: list[dict] = field(default_factory=list)
    parameters: list[tuple[str, str]] = field(default_factory=list)  # (python name, default literal)
    param_map: dict[str, str] = field(default_factory=dict)  # U-SQL parameter name -> notebook parameter name
    untranslated: list[str] = field(default_factory=list)  # "<statement key>: <why>"
    notes: list[str] = field(default_factory=list)
    reads: list[str] = field(default_factory=list)
    writes: list[str] = field(default_factory=list)
    drafted: list[str] = field(default_factory=list)  # statement keys whose code is a model draft a person accepted (assist/script_steps.py)
    context: dict[str, dict] = field(default_factory=dict)  # statement key -> {"text": the statement, "rowsets": {python DataFrame name: [columns]}}


_KW = ("from", "where", "group", "having", "order", "fetch", "inner", "left", "right", "full", "cross", "join", "union", "on", "using")


def _split(toks: list[Tok], stops: tuple[str, ...]) -> list[list[Tok]]:
    """Split a token list at top-level commas (``stops`` empty) ."""
    out, cur, depth = [], [], 0
    for t in toks:
        if t.text == "(":
            depth += 1
        elif t.text == ")":
            depth -= 1
        if depth == 0 and t.kind == "op" and t.text == ",":
            out.append(cur)
            cur = []
        else:
            cur.append(t)
    out.append(cur)
    return out


def _clauses(toks: list[Tok]) -> dict[str, list[Tok]]:
    """The top-level clauses of a SELECT: select / from / where / group / having / order / fetch (the join text stays in from)."""
    keys = {"from": "from", "where": "where", "having": "having", "fetch": "fetch"}
    out: dict[str, list[Tok]] = {"select": []}
    cur, depth, i = "select", 0, 0
    while i < len(toks):
        t = toks[i]
        if t.text == "(":
            depth += 1
        elif t.text == ")":
            depth -= 1
        low = t.text.lower() if t.kind == "id" else ""
        if depth == 0 and low in keys:
            cur = keys[low]
            out.setdefault(cur, [])
        elif depth == 0 and low in ("group", "order") and i + 1 < len(toks) and toks[i + 1].text.lower() == "by":
            cur = low
            out.setdefault(cur, [])
            i += 1
        else:
            out[cur].append(t)
        i += 1
    return out


def _path(toks: list[Tok], ctx: Ctx) -> str:
    if len(toks) == 1 and toks[0].kind == "par":
        v = ctx.params.get(toks[0].text)
        if v is None:
            raise Untranslated(f"{toks[0].text} is not declared")
        return f"_p({v})"
    if len(toks) == 1 and toks[0].kind == "str":
        return f"_p({_lit(toks[0])!r})"
    raise Untranslated("a computed path")


def _options(toks: list[Tok]) -> tuple[str, dict[str, Any]]:
    """``Extractors.Csv(a: 1, b: "x")`` -> ("csv", {a: 1, b: "x"})."""
    if len(toks) < 3 or toks[1].text != ".":
        raise Untranslated("a user-defined extractor or outputter")
    if toks[0].text.lower() not in ("extractors", "outputters"):
        raise Untranslated(f"{toks[0].text}.{toks[2].text}")
    fmt = toks[2].text.lower()
    if fmt not in NOTEBOOK_FORMATS:
        raise Untranslated(f"{toks[0].text}.{toks[2].text} (a built-in {fmt} format needs an assembly)")
    opts: dict[str, Any] = {}
    rest = toks[3:]
    if rest:
        if rest[0].text != "(" or rest[-1].text != ")":
            raise Untranslated("options not in parentheses")
        for part in _split(rest[1:-1], ()):
            if not part:
                continue
            if len(part) != 3 or part[1].text != ":":
                raise Untranslated("an option that is not 'name: literal'")
            opts[part[0].text.lower()] = _lit(part[2])
    return fmt, opts


def _reader(fmt: str, o: dict) -> str:
    known = {"skipfirstnrows", "delimiter", "silent", "encoding", "quoting", "escapecharacter", "rowdelimiter", "nullescape", "charset"}
    if set(o) - known:
        raise Untranslated(f"extractor option {sorted(set(o) - known)[0]}")
    if o.get("rowdelimiter") not in (None, "\n", "\r\n") or o.get("nullescape") not in (None, ""):
        raise Untranslated("extractor options rowDelimiter / nullEscape")
    skip = int(o.get("skipfirstnrows", 0))
    if skip > 1:
        raise Untranslated("skipFirstNRows greater than 1")
    delim = o.get("delimiter", "\t" if fmt == "tsv" else "," if fmt == "csv" else "\t")
    opts = [f".option('header', {skip == 1})", f".option('sep', {delim!r})",
            f".option('mode', {'DROPMALFORMED' if o.get('silent') else 'FAILFAST'!r})"]
    if o.get("quoting") is False:
        opts.append(".option('quote', '')")
    enc = o.get("encoding") or o.get("charset")
    if enc:
        opts.append(f".option('encoding', {str(enc).replace('Encoding.', '')!r})")
    return "".join(opts)


def _extract(name: str, toks: list[Tok], ctx: Ctx, tr: Translation) -> tuple[str, dict[str, Optional[str]]]:
    low = [t.text.lower() for t in toks]
    if "from" not in low or "using" not in low:
        raise Untranslated("EXTRACT without FROM ... USING")
    f, u = low.index("from"), low.index("using")
    cols: dict[str, Optional[str]] = {}
    fields = []
    for part in _split(toks[:f], ()):
        if len(part) != 2:
            raise Untranslated("an EXTRACT column that is not 'name type'")
        cname, ctype = part[0].text, part[1].text.rstrip("?").lower()
        if ctype not in _TYPES:
            raise Untranslated(f"column type {part[1].text}")
        if ctype == "datetime":
            tr.notes.append(f"column {cname} is a DateTime: Spark parses it with its own timestamp format; check the values")
        fields.append(f"StructField({cname!r}, {_TYPES[ctype][0]}, True)")
        cols[cname] = _TYPES[ctype][1]
    fmt, opts = _options(toks[u + 1:])
    path = _path(toks[f + 1:u], ctx)
    tr.reads.append(path)
    schema = "StructType([" + ", ".join(fields) + "])"
    tr.notes.append("empty fields: Spark reads them as null, U-SQL reads an empty string for a string column; check")
    return (f"{_var(name)} = (spark.read{_reader(fmt, opts)}\n    .schema({schema})\n    .csv({path}))\n"), cols


def _item(toks: list[Tok], ctx: Ctx) -> tuple[Ex, str]:
    alias = None
    if len(toks) >= 3 and toks[-2].text.lower() == "as" and toks[-1].kind == "id":
        alias, toks = toks[-1].text, toks[:-2]
    p = _P(toks, ctx)
    e = p.expr()
    if not p.done():
        raise Untranslated(f"unexpected {p.peek().text!r}")
    if alias is None:
        if e.name is None:
            raise Untranslated("a select expression without AS <name>")
        alias = e.name
    return e, alias


def _from(toks: list[Tok], env: dict[str, dict], ctx: Ctx) -> tuple[str, dict[str, Optional[str]]]:
    """(the DataFrame expression for the FROM clause, its columns)."""
    def source(ts: list[Tok]) -> tuple[str, str, str]:
        if not ts or ts[0].kind != "par" and ts[0].kind != "id":
            raise Untranslated("a FROM that is not a rowset")
        rs = ts[0].text
        if rs not in env:
            raise Untranslated(f"rowset {rs} is not defined before it is used")
        alias = ts[2].text if len(ts) == 3 and ts[1].text.lower() == "as" else (None if len(ts) == 1 else "?")
        if alias == "?":
            raise Untranslated("a FROM with something other than '@rowset [AS alias]'")
        return rs, alias, f"{_var(rs)}" + (f".alias({alias!r})" if alias else "")

    low = [t.text.lower() for t in toks]
    joins = [i for i, w in enumerate(low) if w == "join"]
    if not joins:
        rs, alias, code = source(toks)
        ctx.aliases |= {alias} if alias else set()
        return code, dict(env[rs])
    cols: dict[str, Optional[str]] = {}
    first_end = next(i for i, w in enumerate(low) if w in ("inner", "left", "right", "full", "cross", "join"))
    rs, alias, code = source(toks[:first_end])
    cols.update(env[rs])
    ctx.aliases |= {alias} if alias else set()
    segs, starts = [], [first_end] + [i for i in range(first_end + 1, len(toks)) if low[i] in ("inner", "left", "right", "full", "cross")
                                      and low[i - 1] not in ("inner", "left", "right", "full", "cross", "outer", "semi")]
    for n, s in enumerate(starts):
        segs.append(toks[s:starts[n + 1] if n + 1 < len(starts) else len(toks)])
    for seg in segs:
        sl = [t.text.lower() for t in seg]
        j = sl.index("join")
        kind = " ".join(w for w in sl[:j] if w != "outer") or "inner"
        how = {"inner": "inner", "left": "left", "right": "right", "full": "full", "cross": "cross"}.get(kind)
        if how is None:
            raise Untranslated(f"{kind.upper()} JOIN")
        on_at = sl.index("on") if "on" in sl else None
        rs2, alias2, code2 = source(seg[j + 1:on_at] if on_at else seg[j + 1:])
        ctx.aliases |= {alias2} if alias2 else set()
        cols.update(env[rs2])
        cond = ""
        if on_at is not None:
            lctx = Ctx(cols=cols, aliases=ctx.aliases, params=ctx.params, notes=ctx.notes)
            p = _P(seg[on_at + 1:], lctx)
            cond = ", " + p.expr().code
        elif how != "cross":
            raise Untranslated("a JOIN without ON")
        code = f"{code}.join({code2}{cond}, {how!r})"
    return code, cols


def _unions(toks: list[Tok]) -> tuple[list[list[Tok]], list[bool]]:
    """Split a SELECT at its top-level UNION: (the SELECT parts, for each junction whether it is UNION ALL). Plain UNION removes duplicates."""
    parts, alls, cur, depth, i = [], [], [], 0, 0
    while i < len(toks):
        t = toks[i]
        depth += 1 if t.text == "(" else -1 if t.text == ")" else 0
        if depth == 0 and t.kind == "id" and t.text.lower() == "union":
            nxt = toks[i + 1].text.lower() if i + 1 < len(toks) else ""
            alls.append(nxt == "all")
            i += 2 if nxt in ("all", "distinct") else 1
            parts.append(cur)
            cur = []
            continue
        cur.append(t)
        i += 1
    parts.append(cur)
    return parts, alls


def _select(name: str, toks: list[Tok], env: dict[str, dict], ctx: Ctx, tr: Translation) -> tuple[str, dict[str, Optional[str]]]:
    parts, alls = _unions(toks)
    if len(parts) > 1:
        code, names, cols = "", [], {}
        for n, part in enumerate(parts):
            if n and (not part or part[0].text.lower() != "select"):  # the first part arrives without its SELECT keyword
                raise Untranslated("UNION of something other than SELECT statements")
            sub = f"{name}__u{n}"
            c, oc = _select_one(sub, part[1:] if n else part, env, ctx, tr)
            code += c
            names.append(sub)
            cols = cols or oc
            if len(oc) != len(cols):
                raise Untranslated("the SELECTs of a UNION have a different number of columns")
        expr = _var(names[0])
        for sub, keep in zip(names[1:], alls):
            expr = f"{expr}.unionByName({_var(sub)})" + ("" if keep else ".distinct()")
        return code + f"{_var(name)} = {expr}\n", cols
    return _select_one(name, toks, env, ctx, tr)


def _select_one(name: str, toks: list[Tok], env: dict[str, dict], ctx: Ctx, tr: Translation) -> tuple[str, dict[str, Optional[str]]]:
    cl = _clauses(toks)
    if "from" not in cl:
        raise Untranslated("SELECT without FROM")
    sel = cl["select"]
    distinct = bool(sel and sel[0].text.lower() == "distinct")
    if distinct:
        sel = sel[1:]
    src, cols = _from(cl["from"], env, ctx)
    sctx = Ctx(cols=cols, aliases=ctx.aliases, params=ctx.params, notes=tr.notes)
    lines = [f"{_var(name)} = ({src}"]
    if "where" in cl:
        w = _P(cl["where"], sctx)
        e = w.expr()
        if not w.done():
            raise Untranslated(f"unexpected {w.peek().text!r} in WHERE")
        if e.agg:
            raise Untranslated("an aggregate in WHERE")
        lines.append(f"    .filter({e.code})")
    outcols: dict[str, Optional[str]] = {}
    items: list[tuple[Ex, str]] = []
    if len(sel) == 1 and sel[0].text == "*":
        outcols = dict(cols)
        star = True
    else:
        star = False
        for part in _split(sel, ()):
            e, alias = _item(part, sctx)
            items.append((e, alias))
            outcols[alias] = e.type
    has_agg = any(e.agg for e, _ in items)
    if "group" in cl or has_agg:
        keys = []
        for part in _split(cl.get("group", []), ()):
            if not part:
                continue
            p = _P(part, sctx)
            k = p.expr()
            if k.name is None or not p.done():
                raise Untranslated("a GROUP BY that is not a plain column")
            keys.append(k)
        aggs = [(e, a) for e, a in items if e.agg]
        plain = [(e, a) for e, a in items if not e.agg]
        for e, a in plain:
            if e.name is None or e.name not in {k.name for k in keys}:
                raise Untranslated(f"select item '{a}' is neither an aggregate nor a GROUP BY column")
        gb = ", ".join(k.code for k in keys)
        agg_code = ", ".join(f"{e.code}.alias({a!r})" for e, a in aggs)
        lines.append(f"    .groupBy({gb}).agg({agg_code})" if keys else f"    .agg({agg_code})")
        lines.append("    .select(" + ", ".join(f"F.col({a!r})" if e.agg or e.name == a else f"F.col({e.name!r}).alias({a!r})" for e, a in items) + ")")
        if "having" in cl:
            hp = _P(cl["having"], Ctx(cols=outcols, aliases=set(), params=ctx.params, notes=tr.notes))
            h = hp.expr()
            if h.agg or not hp.done():
                raise Untranslated("HAVING with an aggregate expression (use the select item's alias)")
            lines.append(f"    .filter({h.code})")
    elif not star:
        lines.append("    .select(" + ", ".join(e.code if e.code == f"F.col({a!r})" else f"{e.code}.alias({a!r})" for e, a in items) + ")")
    if "having" in cl and not ("group" in cl or has_agg):
        raise Untranslated("HAVING without grouping")
    if distinct:
        lines.append("    .distinct()")
    if "order" in cl:
        ords = []
        octx = Ctx(cols=outcols, aliases=set(), params=ctx.params, notes=tr.notes)
        for part in _split(cl["order"], ()):
            desc = part and part[-1].text.lower() == "desc"
            if part and part[-1].text.lower() in ("asc", "desc"):
                part = part[:-1]
            op = _P(part, octx)
            ords.append(op.expr().code + (".desc()" if desc else ".asc()"))
        lines.append(f"    .orderBy({', '.join(ords)})")
    if "fetch" in cl:
        n = [t for t in cl["fetch"] if t.kind == "num"]
        if len(n) != 1:
            raise Untranslated("FETCH that is not 'FETCH n ROWS'")
        lines.append(f"    .limit({int(_lit(n[0]))})")
    return "\n".join(lines) + "\n)\n", outcols


def _output(toks: list[Tok], env: dict[str, dict], ctx: Ctx, tr: Translation) -> str:
    low = [t.text.lower() for t in toks]
    if len(toks) < 5 or toks[0].kind not in ("par", "id") or "to" not in low or "using" not in low:
        raise Untranslated("OUTPUT that is not '@rowset TO path USING Outputters.Csv(...)'")
    rs, t, u = toks[0].text, low.index("to"), low.index("using")
    if rs not in env:
        raise Untranslated(f"rowset {rs} is not defined before it is used")
    if t != 1:
        raise Untranslated("OUTPUT of an expression or with ORDER BY / FETCH")
    fmt, o = _options(toks[u + 1:])
    known = {"outputheader", "delimiter", "quoting", "encoding", "dateformat", "nullescape", "escapecharacter", "rowdelimiter"}
    if set(o) - known:
        raise Untranslated(f"outputter option {sorted(set(o) - known)[0]}")
    delim = o.get("delimiter", "\t" if fmt == "tsv" else ",")
    header = bool(o.get("outputheader", False))
    path = _path(toks[t + 1:u], ctx)
    tr.writes.append(path)
    tr.notes.append("OUTPUT writes one file as U-SQL does (a single part file moved to the path); an existing file at the path is replaced; "
                    "string quoting follows Spark's rules, not U-SQL's")
    quote = ".option('quote', '')" if o.get("quoting") is False else ""
    return (f"_out = {path}\n_tmp = _out + '.adf2fabric-tmp'\n"
            f"({_var(rs)}.coalesce(1).write.mode('overwrite').option('header', {header}).option('sep', {delim!r}){quote}.csv(_tmp))\n"
            "_part = next(f.path for f in notebookutils.fs.ls(_tmp) if f.name.startswith('part-'))\n"
            "if notebookutils.fs.exists(_out):  # rm raises on a path that is not there (found by the first live run, 2026-11-18)\n    notebookutils.fs.rm(_out, True)\n"
            "notebookutils.fs.mv(_part, _out)\nnotebookutils.fs.rm(_tmp, True)\n")


def translate(script: str, root: str, drafts: Optional[dict[str, dict]] = None) -> Translation:
    """The notebook for a U-SQL script. ``root``: the storage root (``abfss://<container>@<account>.dfs.core.windows.net/``) its
    paths are read from. ``drafts``: statement key (``@rowset`` or ``OUTPUT#n``) -> an accepted model draft (``code``, ``accepted_by``,
    ``accepted_at``) used for a statement the fixed rules cannot translate, instead of a cell that raises; each one is marked in its cell."""
    tr = Translation()
    ctx = Ctx()
    env: dict[str, dict] = {}
    pdecl: list[tuple[str, str, str, Any]] = []  # (name, python name, type, default)
    stmts = _statements(script)
    consts: list[str] = []
    for text in stmts:  # declarations first: they are the parameters
        try:
            toks = _tokens(text)
        except Untranslated:
            continue
        if toks and toks[0].text.lower() == "declare":
            rest = toks[1:]
            external = bool(rest and rest[0].text.lower() == "external")
            rest = rest[1:] if external else rest
            try:
                if len(rest) < 3 or rest[0].kind != "par" or rest[-2].text != "=":
                    raise Untranslated("a DECLARE that is not '@name [type] = literal'")
                typ = rest[1].text.lower() if len(rest) == 4 else "string"
                val = _lit(rest[-1])
            except Untranslated as exc:
                tr.untranslated.append(f"{text.split()[1] if len(text.split()) > 1 else 'DECLARE'} (DECLARE): {exc}")
                continue
            py = param_name(rest[0].text)
            ctx.params[rest[0].text] = py
            if external:
                pdecl.append((rest[0].text.lstrip("@"), py, typ, val))
                tr.param_map[rest[0].text.lstrip("@")] = py
            else:
                consts.append(f"{py} = {val!r}\n")
    if pdecl:
        tr.parameters = [(py, repr(v)) for _, py, _, v in pdecl]
        tr.cells.append(_code_cell("# Parameters: the pipeline's notebook activity overrides these values by name.\n"
                                   "# (a U-SQL parameter whose name is a Python keyword, such as in, has a trailing _)\n"
                                   + "".join(f"{py} = {v!r}\n" for _, py, _, v in pdecl), ["parameters"]))
    tr.cells.append(_code_cell(
        "# Generated by adf2fabric from the U-SQL script (fixed translation rules).\nimport re\nfrom pyspark.sql import functions as F\nfrom pyspark.sql import Window\n"
        "from pyspark.sql.types import *  # noqa: F403 - the declared column types\n"
        f"_root = {root!r}  # U-SQL paths are relative to the ADLA account's default store; read here from the script's own container\n"
        "_p = lambda p: p if '://' in p else _root + p.lstrip('/')\n"
        + "".join(f"{py} = {'int' if t in ('int', 'long', 'short', 'byte') else 'float' if t in ('double', 'float', 'decimal') else 'str'}({py})\n"
                  if t in ('int', 'long', 'short', 'byte', 'double', 'float', 'decimal') else
                  f"{py} = (str({py}).lower() == 'true')\n" if t in ('bool', 'boolean') else f"{py} = str({py})\n" for _, py, t, _ in pdecl)
        + "".join(consts)))
    n_out = 0
    for text in stmts:
        key = ""
        try:
            toks = _tokens(text)
            if not toks or toks[0].text.lower() == "declare":
                continue
            if toks[0].kind == "par" and len(toks) > 2 and toks[1].text == "=":
                key = toks[0].text
            else:
                n_out += 1
                key = f"{toks[0].text.upper()}#{n_out}"
            tr.context[key] = {"text": text, "rowsets": {_var(k): [c for c in v] for k, v in env.items()}}
            if toks[0].kind == "par" and len(toks) > 2 and toks[1].text == "=":
                name, body = toks[0].text, toks[2:]
                head = body[0].text.lower() if body else ""
                if head == "extract":
                    code, cols = _extract(name, body[1:], ctx, tr)
                elif head == "select":
                    code, cols = _select(name, body[1:], env, ctx, tr)
                else:
                    raise Untranslated(f"{(body[0].text if body else '?').upper()} (not translated)")
                env[name] = cols
            elif toks[0].text.lower() == "output":
                name = None
                code = _output(toks[1:], env, ctx, tr)
            else:
                raise Untranslated(f"{toks[0].text.upper()} (not translated)")
        except (Untranslated, KeyError, IndexError, StopIteration) as exc:
            why = str(exc) or type(exc).__name__
            key = key or (text.split()[0] if text.split() else "?")
            draft = (drafts or {}).get(key)
            if draft and draft.get("code"):
                tr.drafted.append(key)
                tr.notes.append(f"statement {key} runs model-drafted code accepted by {draft.get('accepted_by') or '?'}: review it")
                code = (f"# [adf2fabric] MODEL-DRAFTED: statement {key} is not translated by the fixed rules ({why}).\n"
                        f"# This code was drafted by a model and accepted by {draft.get('accepted_by') or '?'} on {draft.get('accepted_at') or '?'}.\n"
                        + draft["code"].rstrip() + "\n")
            else:
                tr.untranslated.append(f"{key}: {why}")
                code = (f"# [adf2fabric] this statement is not translated by the fixed rules: {why}.\n# {text[:200]!r}\n"
                        f"raise NotImplementedError({f'U-SQL statement not translated: {why}'!r})\n")
            if toks and toks[0].kind == "par" and len(toks) > 2 and toks[1].text == "=":
                env.setdefault(toks[0].text, {})
        tr.cells.append(_code_cell(code))
    tr.notes = list(dict.fromkeys(tr.notes + ctx.notes))
    tr.notes.append("paths in a U-SQL script are relative to the ADLA account's default store, which is not part of the ADF source: "
                    "they are read from the script's own storage container; check that is where the data is")
    return tr
