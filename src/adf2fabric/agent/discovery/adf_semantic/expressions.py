"""Deterministic parser + analyzer for the ADF expression language.

ADF "dynamic content" is stored as ``{"type": "Expression", "value": "<text>"}``.
The text takes one of these forms:

* ``@<expr>``            a full expression (typed result)
* ``text @{<expr>} ...`` string interpolation (any number of ``@{...}`` holes)
* ``@@...``              an escaped literal ``@``
* anything else          a literal string

This module tokenizes and parses ``<expr>`` with a small recursive-descent parser
(literals, function calls, ``.`` / ``?.`` member access and ``[...]`` / ``?[...]``
indexing — the whole documented grammar) and then WALKS the syntax tree to report
what the expression refers to and which functions it uses. It never evaluates
anything, never substitutes a value, and never rewrites the original text: a
construct it cannot parse is reported as ``unparsed`` with the raw text intact.

Pure functions only: no I/O, no randomness, no clock — same input, same output.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from adf2fabric.project.discovery import AdfExpression, AdfExpressionRef

# --- documented function set -------------------------------------------------------

_FUNCTION_GROUPS: dict[str, tuple[str, ...]] = {
    "string": (
        "concat", "endsWith", "guid", "indexOf", "lastIndexOf", "replace", "split",
        "startsWith", "substring", "toLower", "toUpper", "trim",
    ),
    "collection": (
        "contains", "empty", "first", "intersection", "join", "last", "length",
        "skip", "take", "union",
    ),
    "logical": (
        "and", "equals", "greater", "greaterOrEquals", "if", "less", "lessOrEquals",
        "not", "or", "true", "false", "null",
    ),
    "conversion": (
        "array", "base64", "base64ToBinary", "base64ToString", "binary", "bool",
        "coalesce", "createArray", "dataUri", "dataUriToBinary", "dataUriToString",
        "decimal", "decodeBase64", "decodeDataUri", "decodeUriComponent",
        "encodeUriComponent", "float", "formatNumber", "int", "json", "string",
        "uriComponent", "uriComponentToBinary", "uriComponentToString", "xml", "xpath",
    ),
    "math": ("add", "div", "max", "min", "mod", "mul", "rand", "range", "sub"),
    "datetime": (
        "addDays", "addHours", "addMinutes", "addSeconds", "addToTime", "convertFromUtc",
        "convertTimeZone", "convertToUtc", "dayOfMonth", "dayOfWeek", "dayOfYear",
        "formatDateTime", "getFutureTime", "getPastTime", "startOfDay", "startOfHour",
        "startOfMonth", "subtractFromTime", "ticks", "utcNow",
    ),
    "reference": (
        "activity", "dataset", "item", "linkedService", "pipeline", "trigger",
        "triggerBody", "triggerOutputs", "variables",
    ),
}
# lowercase name -> (documented spelling, group). ADF function names are case-insensitive.
FUNCTIONS: dict[str, tuple[str, str]] = {
    fn.lower(): (fn, group) for group, names in _FUNCTION_GROUPS.items() for fn in names
}

_PIPELINE_SYSTEM = {
    "datafactory", "pipeline", "runid", "triggertype", "triggerid", "triggername",
    "triggertime", "groupid",
}
_MAX_DEPTH = 64


class ExpressionSyntaxError(Exception):
    def __init__(self, message: str, pos: int):
        super().__init__(f"{message} (at position {pos})")
        self.pos = pos


# --- syntax tree ---------------------------------------------------------------------


@dataclass(frozen=True)
class Literal:
    value: object
    kind: str  # string | number | bool | null


@dataclass(frozen=True)
class Call:
    name: str
    args: tuple


@dataclass(frozen=True)
class Member:
    obj: object
    prop: str
    optional: bool = False


@dataclass(frozen=True)
class Index:
    obj: object
    index: object
    optional: bool = False


# --- tokenizer -------------------------------------------------------------------------


@dataclass(frozen=True)
class Token:
    kind: str  # string | number | ident | punct | eof
    value: str
    pos: int


def tokenize(text: str) -> list[Token]:
    tokens: list[Token] = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c.isspace():
            i += 1
        elif c == "'":
            j, buf = i + 1, []
            while True:
                if j >= n:
                    raise ExpressionSyntaxError("unterminated string literal", i)
                if text[j] == "'":
                    if j + 1 < n and text[j + 1] == "'":  # '' escapes a quote
                        buf.append("'")
                        j += 2
                        continue
                    break
                buf.append(text[j])
                j += 1
            tokens.append(Token("string", "".join(buf), i))
            i = j + 1
        elif c.isdigit() or (c == "-" and i + 1 < n and text[i + 1].isdigit()):
            j = i + 1
            while j < n and text[j].isdigit():
                j += 1
            if j + 1 < n and text[j] == "." and text[j + 1].isdigit():
                j += 1
                while j < n and text[j].isdigit():
                    j += 1
            tokens.append(Token("number", text[i:j], i))
            i = j
        elif c.isalpha() or c == "_":
            j = i + 1
            while j < n and (text[j].isalnum() or text[j] == "_"):
                j += 1
            tokens.append(Token("ident", text[i:j], i))
            i = j
        elif c == "?" and i + 1 < n and text[i + 1] in ".[":
            tokens.append(Token("punct", "?" + text[i + 1], i))
            i += 2
        elif c in "(),.[]":
            tokens.append(Token("punct", c, i))
            i += 1
        else:
            raise ExpressionSyntaxError(f"unexpected character {c!r}", i)
    tokens.append(Token("eof", "", n))
    return tokens


# --- parser ------------------------------------------------------------------------------


class _Parser:
    def __init__(self, tokens: list[Token]):
        self.t = tokens
        self.i = 0

    def peek(self) -> Token:
        return self.t[self.i]

    def take(self) -> Token:
        tok = self.t[self.i]
        self.i += 1
        return tok

    def expect(self, value: str) -> Token:
        tok = self.take()
        if tok.kind != "punct" or tok.value != value:
            raise ExpressionSyntaxError(f"expected {value!r}", tok.pos)
        return tok

    def parse(self):
        node = self.expression(0)
        tok = self.peek()
        if tok.kind != "eof":
            raise ExpressionSyntaxError(f"unexpected {tok.value!r}", tok.pos)
        return node

    def expression(self, depth: int):
        if depth > _MAX_DEPTH:
            raise ExpressionSyntaxError("expression nested too deeply", self.peek().pos)
        node = self.primary(depth)
        while True:
            tok = self.peek()
            if tok.kind != "punct":
                return node
            if tok.value in (".", "?."):
                self.take()
                name = self.take()
                if name.kind != "ident":
                    raise ExpressionSyntaxError("expected a property name", name.pos)
                node = Member(node, name.value, tok.value == "?.")
            elif tok.value in ("[", "?["):
                self.take()
                idx = self.expression(depth + 1)
                self.expect("]")
                node = Index(node, idx, tok.value == "?[")
            else:
                return node

    def primary(self, depth: int):
        tok = self.take()
        if tok.kind == "string":
            return Literal(tok.value, "string")
        if tok.kind == "number":
            return Literal(float(tok.value) if "." in tok.value else int(tok.value), "number")
        if tok.kind == "ident":
            nxt = self.peek()
            if nxt.kind == "punct" and nxt.value == "(":
                self.take()
                args = []
                if not (self.peek().kind == "punct" and self.peek().value == ")"):
                    while True:
                        args.append(self.expression(depth + 1))
                        if self.peek().kind == "punct" and self.peek().value == ",":
                            self.take()
                            continue
                        break
                self.expect(")")
                return Call(tok.value, tuple(args))
            low = tok.value.lower()
            if low in ("true", "false"):
                return Literal(low == "true", "bool")
            if low == "null":
                return Literal(None, "null")
            raise ExpressionSyntaxError(
                f"bare identifier {tok.value!r} (a function call or reference was expected)", tok.pos)
        raise ExpressionSyntaxError(
            "unexpected end of expression" if tok.kind == "eof" else f"unexpected {tok.value!r}",
            tok.pos)


def parse_expression(text: str):
    """Parse an expression body (without the leading ``@``) into a syntax tree."""
    if not text.strip():
        raise ExpressionSyntaxError("empty expression", 0)
    return _Parser(tokenize(text)).parse()


# --- templates ---------------------------------------------------------------------------


def split_template(raw: str) -> tuple[str, list[tuple[str, str]]]:
    """Classify ``raw`` and split it into ``('text'|'expr', value)`` parts.

    Returns ``(form, parts)`` where form is expression | interpolated | literal |
    escaped_literal. Raises :class:`ExpressionSyntaxError` for an unterminated ``@{``.
    """
    if raw.startswith("@") and not raw.startswith(("@@", "@{")):
        return "expression", [("expr", raw[1:])]

    parts: list[tuple[str, str]] = []
    buf: list[str] = []
    escaped = False
    i, n = 0, len(raw)
    while i < n:
        if raw.startswith("@@", i):
            buf.append("@")
            escaped = True
            i += 2
        elif raw.startswith("@{", i):
            end = _find_closing_brace(raw, i + 2)
            if buf:
                parts.append(("text", "".join(buf)))
                buf = []
            parts.append(("expr", raw[i + 2:end]))
            i = end + 1
        else:
            buf.append(raw[i])
            i += 1
    if buf:
        parts.append(("text", "".join(buf)))
    if any(kind == "expr" for kind, _ in parts):
        return "interpolated", parts
    return ("escaped_literal" if escaped else "literal"), parts


def _find_closing_brace(raw: str, start: int) -> int:
    i, n = start, len(raw)
    while i < n:
        c = raw[i]
        if c == "'":  # skip a quoted string ('' is an escaped quote)
            i += 1
            while i < n:
                if raw[i] == "'":
                    if i + 1 < n and raw[i + 1] == "'":
                        i += 2
                        continue
                    break
                i += 1
        elif c == "}":
            return i
        i += 1
    raise ExpressionSyntaxError("unterminated '@{' interpolation", start - 2)


# --- analysis ----------------------------------------------------------------------------


def render(node) -> str:
    """Canonical text of a syntax node (used for reference paths only)."""
    if isinstance(node, Literal):
        if node.kind == "string":
            return "'" + str(node.value).replace("'", "''") + "'"
        if node.kind == "null":
            return "null"
        if node.kind == "bool":
            return "true" if node.value else "false"
        return str(node.value)
    if isinstance(node, Call):
        return f"{node.name}({', '.join(render(a) for a in node.args)})"
    if isinstance(node, Member):
        return f"{render(node.obj)}{'?.' if node.optional else '.'}{node.prop}"
    if isinstance(node, Index):
        return f"{render(node.obj)}{'?[' if node.optional else '['}{render(node.index)}]"
    return "?"


def _flatten(node):
    """``a.b[c].d`` -> (root, [segment, ...]); segment = ('prop', name) | ('index', node)."""
    segs: list[tuple[str, object]] = []
    while True:
        if isinstance(node, Member):
            segs.append(("prop", node.prop))
            node = node.obj
        elif isinstance(node, Index):
            segs.append(("index", node.index))
            node = node.obj
        else:
            break
    segs.reverse()
    return node, segs


def _path(segs) -> str:
    out = []
    for kind, val in segs:
        out.append(f".{val}" if kind == "prop" else f"[{render(val)}]")
    return "".join(out)


class _Collector:
    def __init__(self) -> None:
        self.refs: list[AdfExpressionRef] = []
        self.functions: dict[str, str] = {}  # lower -> spelling
        self.unknown: dict[str, str] = {}

    def note_function(self, name: str) -> None:
        low = name.lower()
        if low in FUNCTIONS:
            self.functions.setdefault(low, FUNCTIONS[low][0])
        else:
            self.functions.setdefault(low, name)
            self.unknown.setdefault(low, name)

    def visit(self, node, depth: int = 0) -> None:
        if depth > _MAX_DEPTH * 2:
            return
        root, segs = _flatten(node)
        if isinstance(root, Call):
            self.note_function(root.name)
            ref = _make_ref(root, segs, render(node))
            if ref is not None:
                self.refs.append(ref)
            for arg in root.args:
                self.visit(arg, depth + 1)
        for kind, val in segs:
            if kind == "index":
                self.visit(val, depth + 1)


def _literal_arg(call: Call) -> Optional[str]:
    if call.args and isinstance(call.args[0], Literal) and call.args[0].kind == "string":
        return str(call.args[0].value)
    return None


def _make_ref(root: Call, segs, raw: str) -> Optional[AdfExpressionRef]:
    fn = root.name.lower()
    if fn == "variables":
        return AdfExpressionRef(kind="variable", name=_literal_arg(root), path=_path(segs) or None, raw=raw)
    if fn == "activity":
        return AdfExpressionRef(kind="activity_output", name=_literal_arg(root), path=_path(segs) or None, raw=raw)
    if fn == "item":
        return AdfExpressionRef(kind="loop_item", path=_path(segs) or None, raw=raw)
    if fn == "pipeline":
        if segs and segs[0][0] == "prop":
            head = str(segs[0][1])
            low = head.lower()
            if low in ("parameters", "globalparameters") and len(segs) > 1 and segs[1][0] == "prop":
                kind = "pipeline_parameter" if low == "parameters" else "global_parameter"
                return AdfExpressionRef(kind=kind, name=str(segs[1][1]), path=_path(segs[2:]) or None, raw=raw)
            if low in _PIPELINE_SYSTEM:
                return AdfExpressionRef(kind="pipeline_system", name=head, path=_path(segs[1:]) or None, raw=raw)
            return AdfExpressionRef(kind="pipeline_property", name=head, path=_path(segs[1:]) or None, raw=raw)
        return AdfExpressionRef(kind="pipeline_property", raw=raw)
    if fn in ("dataset", "linkedservice"):
        kind = "dataset_parameter" if fn == "dataset" else "linked_service_parameter"
        name = str(segs[0][1]) if segs and segs[0][0] == "prop" else None
        return AdfExpressionRef(kind=kind, name=name, path=_path(segs[1:]) or None, raw=raw)
    if fn in ("trigger", "triggerbody", "triggeroutputs"):
        name = str(segs[0][1]) if segs and segs[0][0] == "prop" else None
        return AdfExpressionRef(kind="trigger", name=name or root.name, path=_path(segs[1:] if name else segs) or None, raw=raw)
    return None


def analyze_expression(raw: str, property_path: str, role: str = "general") -> AdfExpression:
    """Analyze one dynamic-content string. Never raises; never alters ``raw``."""
    try:
        form, parts = split_template(raw)
    except ExpressionSyntaxError as exc:
        return AdfExpression(property_path=property_path, role=role, raw=raw, form="interpolated",
                             parse_status="unparsed", error=str(exc))

    literal_text = "".join(v for k, v in parts if k == "text")
    if form in ("literal", "escaped_literal"):
        return AdfExpression(property_path=property_path, role=role, raw=raw, form=form,
                             is_dynamic=False, literal_text=literal_text)

    collector = _Collector()
    try:
        for kind, text in parts:
            if kind == "expr":
                collector.visit(parse_expression(text))
    except ExpressionSyntaxError as exc:
        return AdfExpression(property_path=property_path, role=role, raw=raw, form=form,
                             parse_status="unparsed", error=str(exc), literal_text=literal_text)

    groups = sorted({FUNCTIONS[f][1] for f in collector.functions if f in FUNCTIONS} - {"reference"})
    return AdfExpression(
        property_path=property_path, role=role, raw=raw, form=form,
        references=collector.refs,
        functions=sorted(collector.functions.values(), key=str.lower),
        unknown_functions=sorted(collector.unknown.values(), key=str.lower),
        function_groups=groups,
        literal_text=literal_text,
    )
