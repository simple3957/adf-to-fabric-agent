"""SSIS expressions -> PySpark column expressions or Python expressions, by fixed rules (2026-11-18).

SSIS has one expression language for Derived Column / Conditional Split (column context), variable expressions, precedence constraint expressions,
Expression Tasks, For Loop containers and property expressions (scalar context). Its operators are C-like (``==``, ``!=``, ``&&``, ``||``, ``!``,
``? :``), a column is ``[Name]`` (or a bare name in the *friendly* form the package stores next to each expression), a variable is
``@[User::name]``, a cast is ``(DT_WSTR, 10)``. Documented in Microsoft Learn, "Integration Services (SSIS) Expressions".

Translated: the operators above, ``+`` (string concatenation when an operand is a string), ``? :``, casts to string / integer / floating point /
boolean / date, and the functions in ``_FUNCS``. Anything else is named (``Untranslated``); nothing is guessed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional


class Untranslated(Exception):
    pass


_TOKEN = re.compile(r'''\s*(?:(?P<str>"(?:[^"\\]|\\.)*")|(?P<num>\d+(?:\.\d+)?(?:[eE][-+]?\d+)?[LlFf]?)|(?P<var>@\[[^\]]+\])'''
                    r'''|(?P<cast>\(\s*DT_[A-Za-z0-9]+\s*(?:,[^)]*)?\))|(?P<col>\[[^\]]+\])|(?P<id>[A-Za-z_][\w]*)'''
                    r'''|(?P<op>==|!=|<=|>=|&&|\|\||[-+*/%<>!?:(),]))''')


@dataclass
class Env:
    """What an expression may refer to: column -> type (None = unknown), SSIS variable name -> (python name, type), and notes it adds."""

    cols: dict[str, Optional[str]] = field(default_factory=dict)
    vars: dict[str, tuple[str, Optional[str]]] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@dataclass
class N:
    kind: str  # lit | col | var | op | not | neg | cond | call | cast
    a: Any = None
    b: Any = None
    c: Any = None
    d: Any = None


def _tokens(text: str) -> list[tuple[str, str]]:
    out, pos = [], 0
    text = text.strip()
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise Untranslated(f"unreadable text near {text[pos:pos + 20]!r}")
        pos = m.end()
        out.append((m.lastgroup or "", m.group(m.lastgroup or "")))
    return out


class _Parser:
    def __init__(self, toks: list[tuple[str, str]]):
        self.t, self.i = toks, 0

    def peek(self, k: int = 0) -> Optional[tuple[str, str]]:
        return self.t[self.i + k] if self.i + k < len(self.t) else None

    def op(self, *ops: str) -> bool:
        t = self.peek()
        return t is not None and t[0] == "op" and t[1] in ops

    def eat(self, text: Optional[str] = None) -> tuple[str, str]:
        t = self.peek()
        if t is None or (text is not None and t[1] != text):
            raise Untranslated(f"expected {text!r}")
        self.i += 1
        return t

    def done(self) -> bool:
        return self.i >= len(self.t)

    def expr(self) -> N:
        c = self.or_()
        if self.op("?"):
            self.eat()
            a = self.expr()
            self.eat(":")
            return N("cond", c, a, self.expr())
        return c

    def or_(self) -> N:
        a = self.and_()
        while self.op("||"):
            self.eat()
            a = N("op", "||", a, self.and_())
        return a

    def and_(self) -> N:
        a = self.eq()
        while self.op("&&"):
            self.eat()
            a = N("op", "&&", a, self.eq())
        return a

    def eq(self) -> N:
        a = self.rel()
        while self.op("==", "!="):
            o = self.eat()[1]
            a = N("op", o, a, self.rel())
        return a

    def rel(self) -> N:
        a = self.add()
        while self.op("<", "<=", ">", ">="):
            o = self.eat()[1]
            a = N("op", o, a, self.add())
        return a

    def add(self) -> N:
        a = self.mul()
        while self.op("+", "-"):
            o = self.eat()[1]
            a = N("op", o, a, self.mul())
        return a

    def mul(self) -> N:
        a = self.unary()
        while self.op("*", "/", "%"):
            o = self.eat()[1]
            a = N("op", o, a, self.unary())
        return a

    def unary(self) -> N:
        t = self.peek()
        if t and t[0] == "cast":
            self.eat()
            return N("cast", t[1], self.unary())
        if self.op("!"):
            self.eat()
            return N("not", self.unary())
        if self.op("-"):
            self.eat()
            return N("neg", self.unary())
        return self.primary()

    def primary(self) -> N:
        t = self.peek()
        if t is None:
            raise Untranslated("the expression ends too early")
        kind, text = t
        if kind == "op" and text == "(":
            self.eat()
            e = self.expr()
            self.eat(")")
            return e
        self.eat()
        if kind == "str":
            return N("lit", _unescape(text[1:-1]), "str")
        if kind == "num":
            raw = text.rstrip("LlFf")
            return N("lit", float(raw) if re.search(r"[.eE]", raw) else int(raw), "num")
        if kind == "var":
            return N("var", text[2:-1])
        if kind == "col":
            return N("col", text[1:-1])
        if kind == "id":
            if self.op("("):
                self.eat()
                args: list[N] = []
                while not self.op(")"):
                    args.append(self.expr())
                    if self.op(","):
                        self.eat()
                self.eat(")")
                return N("call", text.upper(), args)
            if text.upper() == "TRUE":
                return N("lit", True, "bool")
            if text.upper() == "FALSE":
                return N("lit", False, "bool")
            return N("col", text)
        raise Untranslated(f"unexpected {text!r}")


def _unescape(s: str) -> str:
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t", "r": "\r", "0": "\0"}.get(m.group(1), m.group(1)), s)


def parse(text: str) -> N:
    p = _Parser(_tokens(text))
    node = p.expr()
    if not p.done():
        raise Untranslated(f"unexpected {p.peek()[1]!r}")
    return node


# ---- types -----------------------------------------------------------------------------------------------------------------------------------
_STR_FUNCS = {"UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM", "SUBSTRING", "REPLACE", "LEFT", "RIGHT"}
_CASTS = {"DT_WSTR": "str", "DT_STR": "str", "DT_I1": "num", "DT_I2": "num", "DT_I4": "num", "DT_I8": "num", "DT_UI1": "num", "DT_UI2": "num",
          "DT_UI4": "num", "DT_UI8": "num", "DT_R4": "num", "DT_R8": "num", "DT_BOOL": "bool", "DT_DBTIMESTAMP": "date", "DT_DATE": "date",
          "DT_DBDATE": "date", "DT_NUMERIC": "num", "DT_DECIMAL": "num", "DT_CY": "num"}


def _cast_name(text: str) -> tuple[str, list[str]]:
    inner = text.strip()[1:-1]
    parts = [x.strip() for x in inner.split(",")]
    return parts[0].upper(), parts[1:]


def type_of(n: N, env: Env) -> Optional[str]:
    if n.kind == "lit":
        return n.b
    if n.kind == "col":
        return env.cols.get(n.a)
    if n.kind == "var":
        return env.vars.get(n.a, ("", None))[1]
    if n.kind == "cast":
        return _CASTS.get(_cast_name(n.a)[0])
    if n.kind == "call":
        return "str" if n.a in _STR_FUNCS else "bool" if n.a == "ISNULL" else "date" if n.a == "GETDATE" else "num"
    if n.kind in ("not",):
        return "bool"
    if n.kind == "neg":
        return "num"
    if n.kind == "cond":
        return type_of(n.b, env) or type_of(n.c, env)
    if n.kind == "op":
        if n.a in ("==", "!=", "<", "<=", ">", ">=", "&&", "||"):
            return "bool"
        if n.a == "+":
            ta, tb = type_of(n.b, env), type_of(n.c, env)
            return "str" if "str" in (ta, tb) else ta or tb
        return "num"
    return None


# ---- column mode ----------------------------------------------------------------------------------------------------------------------------
_SPARK_CAST = {"DT_I1": "tinyint", "DT_I2": "smallint", "DT_I4": "int", "DT_I8": "bigint", "DT_UI1": "smallint", "DT_UI2": "int", "DT_UI4": "bigint",
               "DT_UI8": "decimal(20,0)", "DT_R4": "float", "DT_R8": "double", "DT_BOOL": "boolean", "DT_DBTIMESTAMP": "timestamp",
               "DT_DATE": "timestamp", "DT_DBDATE": "date", "DT_WSTR": "string", "DT_STR": "string", "DT_CY": "decimal(19,4)"}
_DATEPART = {"yy": "F.year", "yyyy": "F.year", "mm": "F.month", "m": "F.month", "dd": "F.dayofmonth", "d": "F.dayofmonth", "dw": "F.dayofweek",
             "hh": "F.hour", "mi": "F.minute", "ss": "F.second"}


def _lit_str(n: N) -> Optional[str]:
    return n.a if n.kind == "lit" and n.b == "str" else None


def col_code(n: N, env: Env) -> str:
    """A PySpark column expression for ``n``."""
    k = n.kind
    if k == "lit":
        return f"F.lit({n.a!r})"
    if k == "col":
        if env.cols and n.a not in env.cols:
            raise Untranslated(f"column '{n.a}' is not in the data flow's input")
        return f"F.col({n.a!r})"
    if k == "var":
        if n.a not in env.vars:
            raise Untranslated(f"variable {n.a} is not declared in the package")
        return f"F.lit({env.vars[n.a][0]})"
    if k == "not":
        return f"(~{col_code(n.a, env)})"
    if k == "neg":
        return f"(-{col_code(n.a, env)})"
    if k == "cond":
        return f"F.when({col_code(n.a, env)}, {col_code(n.b, env)}).otherwise({col_code(n.c, env)})"
    if k == "cast":
        name, args = _cast_name(n.a)
        if name in ("DT_NUMERIC", "DT_DECIMAL"):
            target = f"decimal({args[0] if args else 38},{args[1] if len(args) > 1 else 0})"
        elif name in _SPARK_CAST:
            target = _SPARK_CAST[name]
        else:
            raise Untranslated(f"cast to {name}")
        return f"{col_code(n.b, env)}.cast({target!r})"
    if k == "op":
        a, b = col_code(n.b, env), col_code(n.c, env)
        if n.a == "+":
            ta, tb = type_of(n.b, env), type_of(n.c, env)
            if "str" in (ta, tb):
                return f"F.concat({a}, {b})"
            if ta is None and tb is None:
                env.notes.append("a '+' between values of unknown type is numeric addition here; SSIS concatenates strings")
        if n.a == "/":
            env.notes.append("a '/' on integers is integer division in SSIS; Spark '/' gives a decimal: check the result")
        py = {"&&": "&", "||": "|"}.get(n.a, n.a)
        return f"({a} {py} {b})"
    if k == "call":
        return _col_call(n, env)
    raise Untranslated(f"expression kind {k}")


def _col_call(n: N, env: Env) -> str:
    f, args = n.a, n.b
    c = [col_code(x, env) for x in args if not (x.kind == "lit" and x.b == "str" and f in ("DATEPART", "DATEDIFF") and x is args[0])]
    want = {"FINDSTRING": 3, "UPPER": 1, "LOWER": 1, "TRIM": 1, "LTRIM": 1, "RTRIM": 1, "LEN": 1, "ISNULL": 1, "ABS": 1, "FLOOR": 1, "CEILING": 1, "YEAR": 1, "MONTH": 1,
            "DAY": 1, "GETDATE": 0, "REPLACENULL": 2, "ROUND": 2, "LEFT": 2, "RIGHT": 2, "SUBSTRING": 3, "REPLACE": 3, "DATEPART": 2, "DATEDIFF": 3}
    if f not in want:
        raise Untranslated(f"function {f}()")
    if len(args) != want[f]:
        raise Untranslated(f"{f}() with {len(args)} arguments")
    simple = {"UPPER": "F.upper", "LOWER": "F.lower", "TRIM": "F.trim", "LTRIM": "F.ltrim", "RTRIM": "F.rtrim", "LEN": "F.length", "ABS": "F.abs",
              "FLOOR": "F.floor", "CEILING": "F.ceil", "YEAR": "F.year", "MONTH": "F.month", "DAY": "F.dayofmonth"}
    if f in simple:
        return f"{simple[f]}({c[0]})"
    if f == "FINDSTRING":  # 1-based position of the first occurrence, 0 when absent (the same as Spark's locate)
        find = _lit_str(args[1])
        if find is None or args[2].kind != "lit" or args[2].a != 1:
            raise Untranslated("FINDSTRING() with a computed search text or an occurrence other than 1")
        return f"F.locate({find!r}, {c[0]})"
    if f == "GETDATE":
        return "F.current_timestamp()"
    if f == "ISNULL":
        return f"{c[0]}.isNull()"
    if f == "REPLACENULL":
        return f"F.coalesce({c[0]}, {c[1]})"
    if f == "ROUND":
        return f"F.round({c[0]}, {int(args[1].a) if args[1].kind == 'lit' else 0})"
    if f == "LEFT":
        return f"F.substring({c[0]}, 1, {int(args[1].a)})" if args[1].kind == "lit" else _bad(f, "a literal length")
    if f == "RIGHT":
        return f"F.substring({c[0]}, -{int(args[1].a)}, {int(args[1].a)})" if args[1].kind == "lit" else _bad(f, "a literal length")
    if f == "SUBSTRING":
        return f"F.substring({c[0]}, {c[1]}, {c[2]})" if all(x.kind == "lit" for x in args[1:]) else f"{c[0]}.substr({c[1]}, {c[2]})"
    if f == "REPLACE":
        find, rep = _lit_str(args[1]), _lit_str(args[2])
        if find is None or rep is None:
            raise Untranslated("REPLACE() with a computed search or replacement text")
        return f"F.regexp_replace({c[0]}, {re.escape(find)!r}, {rep.replace(chr(92), chr(92) * 2).replace('$', chr(92) + '$')!r})"
    if f == "DATEPART":
        part = _lit_str(args[0])
        if part is None or part.lower() not in _DATEPART:
            raise Untranslated(f"DATEPART({part!r}, ...)")
        return f"{_DATEPART[part.lower()]}({c[0]})"
    if f == "DATEDIFF":
        if (_lit_str(args[0]) or "").lower() not in ("d", "dd"):
            raise Untranslated(f"DATEDIFF({_lit_str(args[0])!r}, ...)")
        return f"F.datediff({c[1]}, {c[0]})"
    raise Untranslated(f"function {f}()")


def _bad(f: str, what: str) -> str:
    raise Untranslated(f"{f}() with something other than {what}")


# ---- python (scalar) mode ---------------------------------------------------------------------------------------------------------------------
def py_code(n: N, env: Env) -> str:
    """A Python expression for ``n`` (variables, literals, functions of those). A column is an error: there is no row here."""
    k = n.kind
    if k == "lit":
        return repr(n.a)
    if k == "col":
        raise Untranslated(f"'{n.a}' is read as a column, but this expression has no row (a variable is written @[User::name])")
    if k == "var":
        if n.a not in env.vars:
            raise Untranslated(f"variable {n.a} is not declared in the package")
        return env.vars[n.a][0]
    if k == "not":
        return f"(not {py_code(n.a, env)})"
    if k == "neg":
        return f"(-{py_code(n.a, env)})"
    if k == "cond":
        return f"({py_code(n.b, env)} if {py_code(n.a, env)} else {py_code(n.c, env)})"
    if k == "cast":
        name, args = _cast_name(n.a)
        inner = py_code(n.b, env)
        if name in ("DT_WSTR", "DT_STR"):
            return f"str({inner})"
        if name in ("DT_I1", "DT_I2", "DT_I4", "DT_I8", "DT_UI1", "DT_UI2", "DT_UI4", "DT_UI8"):
            return f"int({inner})"
        if name in ("DT_R4", "DT_R8", "DT_NUMERIC", "DT_DECIMAL", "DT_CY"):
            return f"float({inner})"
        if name == "DT_BOOL":
            return f"bool({inner})"
        if name in ("DT_DBTIMESTAMP", "DT_DATE", "DT_DBDATE"):
            return f"_to_dt({inner})"
        raise Untranslated(f"cast to {name}")
    if k == "op":
        a, b = py_code(n.b, env), py_code(n.c, env)
        if n.a == "+" and "str" in (type_of(n.b, env), type_of(n.c, env)):
            return f"(str({a}) + str({b}))"
        py = {"&&": "and", "||": "or"}.get(n.a, n.a)
        return f"({a} {py} {b})"
    if k == "call":
        f, args = n.a, n.b
        c = [py_code(x, env) for x in args if not (f in ("DATEPART", "DATEDIFF") and x is args[0])]
        simple = {"UPPER": "str({0}).upper()", "LOWER": "str({0}).lower()", "TRIM": "str({0}).strip()", "LTRIM": "str({0}).lstrip()",
                  "RTRIM": "str({0}).rstrip()", "LEN": "len(str({0}))", "ISNULL": "({0} is None)", "ABS": "abs({0})", "YEAR": "{0}.year",
                  "MONTH": "{0}.month", "DAY": "{0}.day", "FLOOR": "math.floor({0})", "CEILING": "math.ceil({0})"}
        if f in simple and len(args) == 1:
            return simple[f].format(c[0])
        if f == "FINDSTRING" and len(args) == 3 and args[2].kind == "lit" and args[2].a == 1:
            return f"(str({c[0]}).find({c[1]}) + 1)"
        if f == "GETDATE" and not args:
            return "datetime.now()"
        if f == "REPLACENULL" and len(args) == 2:
            return f"({c[1]} if {c[0]} is None else {c[0]})"
        if f == "ROUND" and len(args) == 2:
            return f"round({c[0]}, {c[1]})"
        if f == "SUBSTRING" and len(args) == 3:
            return f"str({c[0]})[int({c[1]}) - 1:int({c[1]}) - 1 + int({c[2]})]"
        if f == "REPLACE" and len(args) == 3:
            return f"str({c[0]}).replace({c[1]}, {c[2]})"
        if f == "LEFT" and len(args) == 2:
            return f"str({c[0]})[:int({c[1]})]"
        if f == "RIGHT" and len(args) == 2:
            return f"str({c[0]})[-int({c[1]}):]"
        if f == "DATEPART" and len(args) == 2:
            part = (_lit_str(args[0]) or "").lower()
            tbl = {"yy": "{0}.year", "yyyy": "{0}.year", "mm": "{0}.month", "m": "{0}.month", "dd": "{0}.day", "d": "{0}.day",
                   "dw": "(({0}.isoweekday() % 7) + 1)", "hh": "{0}.hour", "mi": "{0}.minute", "ss": "{0}.second"}
            if part in tbl:
                return tbl[part].format(c[0])
            raise Untranslated(f"DATEPART({part!r}, ...)")
        if f == "DATEDIFF" and len(args) == 3 and (_lit_str(args[0]) or "").lower() in ("d", "dd"):
            return f"({c[1]}.date() - {c[0]}.date()).days"
        raise Untranslated(f"function {f}()")
    raise Untranslated(f"expression kind {k}")


# ---- public helpers --------------------------------------------------------------------------------------------------------------------------
def column_expr(text: str, env: Env) -> tuple[str, Optional[str]]:
    node = parse(text)
    return col_code(node, env), type_of(node, env)


def python_expr(text: str, env: Env) -> tuple[str, Optional[str]]:
    node = parse(text)
    return py_code(node, env), type_of(node, env)


def assignment(text: str, env: Env) -> tuple[str, str]:
    """``@[User::x] = expr`` (an Expression Task, a For Loop's init / assign) -> (python variable, python expression)."""
    m = re.match(r"\s*(@\[[^\]]+\])\s*=(?!=)(.*)$", text, re.S)
    if not m:
        raise Untranslated("an expression that is not '@[User::name] = expression'")
    name = m.group(1)[2:-1]
    if name not in env.vars:
        raise Untranslated(f"variable {name} is not declared in the package")
    return env.vars[name][0], python_expr(m.group(2), env)[0]
