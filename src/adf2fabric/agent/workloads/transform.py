"""Deterministic source-to-Fabric code transformation.

Only rewrites whose Fabric target is documented AND whose meaning is unchanged are applied automatically:

* Databricks widgets become parameter-cell variables (``dbutils.widgets.get("x")`` -> ``x``; declarations are removed and
  their defaults move to the parameter cell, so a pipeline that passes ``x`` overrides it, as it overrode the widget).
* ``dbutils.notebook.exit`` -> ``notebookutils.notebook.exit``; ``dbutils.fs.{ls,mkdirs,cp,mv,rm,put,head,append}`` ->
  ``notebookutils.fs.*`` (``mv`` only with two arguments: the third positional argument means something else in Fabric).
* ``dbutils.secrets.get(scope, key)`` -> ``notebookutils.credentials.getSecret(<vault URL>, key)`` ONLY when the caller
  supplied a scope -> vault URL mapping (the URL is not guessable and is not a secret).
* ``mssparkutils.{fs,notebook,runtime}`` and ``credentials.{getToken,getSecret,putSecret,isValidToken}`` ->
  ``notebookutils`` (Microsoft keeps ``mssparkutils`` as an alias that "will be retired").
* ``spark.conf.set("spark.databricks.*", ...)`` statements are replaced by ``pass`` with a comment naming the setting.
* ``%md`` / ``%sql`` / ``%scala`` / ``%r`` cells become markdown / ``%%sql`` / ``%%spark`` / ``%%sparkr`` cells.

Anything else is left exactly as written and stays in the findings as work for a person. A cell that cannot work in Fabric
(``%%csharp``, ``%sh``, ``%fs``) is kept as comments followed by a ``raise NotImplementedError``, so it fails loudly
instead of looking finished.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from typing import Optional

from adf2fabric.agent.workloads.analyzer import Analysis, FABRIC_FS, _chain, _IDENT
from adf2fabric.agent.workloads.notebook_format import Cell
from adf2fabric.project.workloads import Rewrite

PARAMETERS_TAG = "parameters"  # cell tag; see docs/workload-migration.md for how it was checked
_CELL_MAGIC = {"python": None, "sql": "%%sql", "scala": "%%spark", "r": "%%sparkr"}
_SYNAPSE_CRED_OK = {"getToken", "getSecret", "putSecret", "isValidToken"}


@dataclass
class Transformed:
    cells: list[dict] = field(default_factory=list)  # ipynb cells
    rewrites: list[Rewrite] = field(default_factory=list)
    parameters: list[tuple[str, str]] = field(default_factory=list)  # (name, default) in the parameter cell
    source_parameters: set = field(default_factory=set)  # names the SOURCE's own tagged parameter cell declares
    unrewritten: set[str] = field(default_factory=set)  # finding codes the generator could not fix


class _Edits:
    """Collect replacements on one cell's text by (line, col) spans and apply them last-to-first."""

    def __init__(self, code: str):
        self.lines = code.split("\n")
        self.items: list[tuple[int, int, int, int, str]] = []

    def _col(self, line: int, byte_col: int) -> int:
        return len(self.lines[line - 1].encode("utf-8")[:byte_col].decode("utf-8", errors="ignore"))

    def replace(self, node: ast.AST, text: str) -> None:
        self.items.append((node.lineno, self._col(node.lineno, node.col_offset), node.end_lineno,
                           self._col(node.end_lineno, node.end_col_offset), text))

    def apply(self) -> str:
        lines = list(self.lines)
        for l1, c1, l2, c2, text in sorted(self.items, key=lambda x: (x[0], x[1]), reverse=True):
            head, tail = lines[l1 - 1][:c1], lines[l2 - 1][c2:]
            lines[l1 - 1:l2] = [head + text + tail]
        return "\n".join(lines)


def _span_overlaps(items: list, node: ast.AST) -> bool:
    return any(i[0] <= node.lineno <= i[2] and not (node.end_lineno < i[0]) for i in items)


class _Rewriter(ast.NodeVisitor):
    def __init__(self, code: str, analysis: Analysis, secret_map: dict[str, str], platform: str, widget_ok: set[str]):
        self.code, self.a, self.secret_map, self.platform, self.widget_ok = code, analysis, secret_map, platform, widget_ok
        self.edits = _Edits(code)
        self.counts: dict[str, int] = {}
        self.unrewritten: set[str] = set()

    def _count(self, key: str) -> None:
        self.counts[key] = self.counts.get(key, 0) + 1

    def visit_Expr(self, node: ast.Expr) -> None:
        v = node.value
        if isinstance(v, ast.Call):
            chain = _chain(v.func) or ""
            if chain.startswith("dbutils.widgets.") and chain.split(".")[-1] in ("text", "dropdown", "combobox", "multiselect"):
                name = v.args[0].value if v.args and isinstance(v.args[0], ast.Constant) else None
                if isinstance(name, str) and name in self.widget_ok and node.lineno == node.end_lineno:
                    self.edits.replace(node, "pass  # [adf2fabric] widget '%s' is now a parameter-cell variable" % name)
                    self._count("widget_declaration")
                    return
            if chain in ("spark.conf.set", "spark.sql.conf.set") and v.args and isinstance(v.args[0], ast.Constant) \
                    and isinstance(v.args[0].value, str) and v.args[0].value.lower().startswith(("spark.databricks.", "spark.synapse.", "spark.storage.synapse.")):
                self.edits.replace(node, "pass  # [adf2fabric] removed source-platform-only Spark setting: %s" % v.args[0].value)
                self._count("source_only_spark_conf")
                self.unrewritten.discard("databricks_spark_conf")
                return
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        chain = _chain(node.func) or ""
        parts = chain.split(".")
        if parts[0] == "dbutils" and len(parts) >= 3:
            sub, method = parts[1], parts[-1]
            if sub == "widgets" and method == "get" and len(parts) == 3 and node.args and isinstance(node.args[0], ast.Constant) \
                    and isinstance(node.args[0].value, str) and node.args[0].value in self.widget_ok and len(node.args) == 1:
                self.edits.replace(node, node.args[0].value)
                self._count("widget_get")
                return
            if sub == "notebook" and method == "exit" and len(parts) == 3:
                self.edits.replace(node.func, "notebookutils.notebook.exit")
                self._count("notebook_exit")
            elif sub == "fs" and method in FABRIC_FS and len(parts) == 3:
                if method == "mv" and (len(node.args) + len(node.keywords)) > 2:
                    self.unrewritten.add("dbutils_fs_mv_args")
                else:
                    self.edits.replace(node.func, f"notebookutils.fs.{method}")
                    self._count("fs_" + method)
            elif sub == "secrets" and method == "get" and len(parts) == 3 and len(node.args) == 2 + 0 and not node.keywords:
                scope = node.args[0].value if isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str) else None
                url = self.secret_map.get(scope) if scope else None
                if url:
                    key_src = ast.get_source_segment(self.code, node.args[1])
                    if key_src is not None and "\n" not in key_src and "dbutils" not in key_src:
                        self.edits.replace(node, f'notebookutils.credentials.getSecret("{url}", {key_src})')
                        self._count("secrets_get")
                        return
                self.unrewritten.add("dbutils_secrets")
            elif sub == "secrets":
                self.unrewritten.add("dbutils_secrets")
        elif parts[0] == "mssparkutils" and len(parts) >= 3:
            sub, method = parts[1], parts[-1]
            if (sub in ("fs", "notebook", "runtime")) or (sub == "credentials" and method in _SYNAPSE_CRED_OK):
                self.edits.replace(node.func if sub != "runtime" else node.func, chain.replace("mssparkutils", "notebookutils", 1))
                self._count("mssparkutils_alias")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        chain = _chain(node) or ""
        if chain.startswith("mssparkutils.runtime.") and not _span_overlaps(self.edits.items, node):
            self.edits.replace(node, chain.replace("mssparkutils", "notebookutils", 1))
            self._count("mssparkutils_alias")
            return
        self.generic_visit(node)


def _assigned_constants(code: str) -> list[tuple[str, str]]:
    """(name, literal default) for each top-level ``name = <literal>`` in a parameter cell."""
    out: list[tuple[str, str]] = []
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return out
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            try:
                out.append((node.targets[0].id, repr(ast.literal_eval(node.value))))
            except (ValueError, SyntaxError):
                out.append((node.targets[0].id, ""))
    return out


def _widgets_safe(a: Analysis) -> set[str]:
    """Widgets that can become variables: a valid identifier that nothing else in the code assigns."""
    return {w for w in a.widgets if _IDENT.match(w) and w not in a.assigned_names}


def _code_cell(source: str, tags: Optional[list[str]] = None) -> dict:
    meta: dict = {"tags": tags} if tags else {}
    return {"cell_type": "code", "source": source.splitlines(keepends=True), "execution_count": None, "outputs": [], "metadata": meta}


def _md_cell(source: str) -> dict:
    return {"cell_type": "markdown", "source": source.splitlines(keepends=True), "metadata": {}}


def _fail_loud(cell: Cell, why: str) -> str:
    quoted = "\n".join("# " + l for l in cell.code.split("\n"))
    return f"# [adf2fabric] this cell cannot run in Fabric: {why}\n{quoted}\nraise NotImplementedError({why!r})"


def transform(cells: list[Cell], analysis: Analysis, platform: str, secret_map: Optional[dict[str, str]] = None,
              passed_parameters: Optional[list[str]] = None) -> Transformed:
    """ipynb cells for the Fabric notebook, the rewrites applied, and the parameter-cell variables."""
    out = Transformed()
    secret_map = secret_map or {}
    widget_ok = _widgets_safe(analysis)
    totals: dict[str, int] = {}
    body: list[dict] = []
    source_param_cell = False
    for cell in cells:
        lang = cell.language
        if cell.source_type == "markdown" or lang == "markdown":
            body.append(_md_cell(cell.code))
            continue
        if lang == "python":
            code = cell.code
            first_magic = cell.magic
            stripped = "\n".join("" if l.lstrip().startswith(("%", "!")) else l for l in code.split("\n"))
            try:
                tree = ast.parse(stripped)
            except SyntaxError:
                body.append(_code_cell(code))  # left as written; the analysis already flagged the syntax error
                continue
            rw = _Rewriter(stripped, analysis, secret_map, platform, widget_ok)
            rw.visit(tree)
            new = rw.edits.apply() if rw.edits.items else code
            if rw.edits.items:  # restore magic/command lines that were blanked for parsing
                orig, edited = code.split("\n"), new.split("\n")
                if len(orig) == len(edited):
                    new = "\n".join(o if o.lstrip().startswith(("%", "!")) else e for o, e in zip(orig, edited))
            for k, v in rw.counts.items():
                totals[k] = totals.get(k, 0) + v
            out.unrewritten |= rw.unrewritten
            tags = cell.extra.get("tags") or None
            if tags and PARAMETERS_TAG in tags:  # the source already marks its own parameter cell: keep it, add none
                source_param_cell = True
                for n in _assigned_constants(stripped):
                    out.parameters.append(n)
                    out.source_parameters.add(n[0])
            body.append(_code_cell(new, tags))
        elif lang in ("sql", "scala", "r"):
            body.append(_code_cell(f"{_CELL_MAGIC[lang]}\n{cell.code}"))
        elif lang == "pip":
            body.append(_code_cell(cell.code if cell.code.startswith("%") else "%" + cell.code))
        elif lang == "run":
            body.append(_code_cell(cell.code))
        elif lang == "configure":
            body.append(_code_cell(cell.code if cell.code.startswith("%") else "%%" + cell.code))
        elif lang == "csharp":
            body.append(_code_cell(_fail_loud(cell, ".NET for Spark is not supported in Fabric Spark")))
        elif lang == "shell":
            body.append(_code_cell(_fail_loud(cell, "%sh has no documented Fabric equivalent")))
        elif lang == "fs":
            body.append(_code_cell(_fail_loud(cell, "%fs works on DBFS; use notebookutils.fs in Python")))
        else:
            body.append(_code_cell(cell.code))
    # parameter cell: pipeline-passed names and declared widgets that were turned into variables
    names = [w for w in analysis.widgets if w in widget_ok]
    if not source_param_cell:
        for n in names:
            default = analysis.widgets.get(n)
            out.parameters.append((n, default if default is not None else ""))
    if out.parameters and not source_param_cell:
        lines = ["# Parameters: the pipeline's notebook activity overrides these values by name.\n"] + \
                [f"{n} = {d!r}\n" for n, d in out.parameters]
        out.cells.append(_code_cell("".join(lines), [PARAMETERS_TAG]))
    out.cells += body
    describe = {
        "widget_declaration": "dbutils.widgets declarations removed; their defaults are in the parameter cell",
        "widget_get": "dbutils.widgets.get(name) replaced by the parameter-cell variable",
        "notebook_exit": "dbutils.notebook.exit -> notebookutils.notebook.exit",
        "secrets_get": "dbutils.secrets.get -> notebookutils.credentials.getSecret with the supplied vault URL",
        "source_only_spark_conf": "source-platform-only spark.conf.set statements replaced by pass",
        "mssparkutils_alias": "mssparkutils -> notebookutils (same names)",
    }
    for key, n in sorted(totals.items()):
        out.rewrites.append(Rewrite(code=key, count=n, description=describe.get(key, f"dbutils.fs.{key[3:]} -> notebookutils.fs.{key[3:]}")))
    return out


def to_ipynb(cells: list[dict], language: str = "python") -> dict:
    return {"nbformat": 4, "nbformat_minor": 5, "cells": cells, "metadata": {"language_info": {"name": language}}}
