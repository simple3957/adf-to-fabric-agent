"""Read notebook source of every shape the source platforms produce into one list of cells.

Shapes handled:

* Databricks SOURCE export (``.py`` / ``.scala`` / ``.sql`` / ``.r``): first line ``# Databricks notebook source`` (comment
  marker per language), cells separated by ``COMMAND ----------``, magic lines written ``# MAGIC <text>``.
* Jupyter ``.ipynb`` (a Databricks ipynb export, or a Synapse notebook definition under ``properties``).

Everything becomes ``Cell(index, language, code, start_line)``: ``language`` is the language the cell runs in
(``python | sql | scala | r | markdown | shell | csharp``), ``code`` has the magic prefix removed, and ``start_line`` is the
1-based line of the cell's first line in the ORIGINAL text so findings point at real lines. Nothing here executes or
interprets the code beyond recognising magic commands.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Optional

_COMMENT = {"python": "#", "scala": "//", "sql": "--", "r": "#"}
_SEPARATOR = re.compile(r"^\s*(?:#|//|--)\s*COMMAND\s*-{3,}\s*$")
_HEADER = re.compile(r"^\s*(?:#|//|--)\s*(?:Databricks|Synapse)\s+notebook\s+source\s*$", re.I)
_MAGIC_PREFIX = re.compile(r"^\s*(?:#|//|--)\s*MAGIC ?")
# line magics (Databricks) and cell magics (Synapse / Fabric)
_MAGIC_LANG = {"python": "python", "pyspark": "python", "py": "python", "sql": "sql", "scala": "scala", "spark": "scala",
               "r": "r", "sparkr": "r", "md": "markdown", "sh": "shell", "csharp": "csharp", "dotnet": "csharp",
               "pip": "pip", "conda": "pip", "run": "run", "fs": "fs", "configure": "configure", "lsmagic": "other",
               "scalaspark": "scala"}
_MAGIC_RE = re.compile(r"^\s*(%%?)\s*([A-Za-z]+)\b(.*)$")


@dataclass
class Cell:
    index: int
    language: str
    code: str
    start_line: int
    magic: Optional[str] = None  # the magic that set the language, as written (without the % signs)
    source_type: str = "code"  # code | markdown
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def lines(self) -> int:
        return self.code.count("\n") + 1 if self.code else 0


def _split_magic(code: str, default: str) -> tuple[str, str, Optional[str]]:
    """(language, code without the magic line, magic name) for a cell body."""
    lines = code.split("\n")
    first = next((i for i, l in enumerate(lines) if l.strip()), None)
    if first is None:
        return default, code, None
    m = _MAGIC_RE.match(lines[first])
    if not m:
        return default, code, None
    name = m.group(2).lower()
    lang = _MAGIC_LANG.get(name)
    if lang is None:
        return default, code, None
    rest = m.group(3).strip()
    body = lines[:first] + ([rest] if rest and lang not in ("pip", "run", "fs", "configure", "shell") else []) + lines[first + 1:]
    if lang in ("pip", "run", "fs", "configure", "shell"):  # keep the command text: the analyzer reads it
        body = lines[:first] + [f"{m.group(1)}{name} {rest}".rstrip()] + lines[first + 1:]
    return lang, "\n".join(body), name


def _default_language(first_line: str, filename: str) -> str:
    low = filename.lower()
    if low.endswith(".scala") or first_line.lstrip().startswith("//"):
        return "scala"
    if low.endswith(".sql") or first_line.lstrip().startswith("--"):
        return "sql"
    if low.endswith(".r"):
        return "r"
    return "python"


def parse_databricks_source(text: str, filename: str = "") -> tuple[list[Cell], str]:
    """Cells of a Databricks SOURCE export, and the notebook's default language."""
    lines = text.replace("\r\n", "\n").split("\n")
    default = _default_language(lines[0] if lines else "", filename)
    cells: list[Cell] = []
    cur: list[tuple[int, str]] = []

    def flush() -> None:
        if not cur:
            return
        start = cur[0][0]
        body = "\n".join(l for _, l in cur).strip("\n")
        magic_lines = [bool(_MAGIC_PREFIX.match(l)) for _, l in cur if l.strip()]
        if magic_lines and all(magic_lines):  # a cell written entirely as MAGIC lines: a non-default language cell
            body = "\n".join(_MAGIC_PREFIX.sub("", l, count=1) for _, l in cur).strip("\n")
            lang, code, magic = _split_magic(body, default)
            cells.append(Cell(len(cells), lang, code, start, magic, "markdown" if lang == "markdown" else "code"))
        elif body.strip():
            cells.append(Cell(len(cells), default, body, start))
        cur.clear()

    for number, line in enumerate(lines, 1):
        if number == 1 and _HEADER.match(line):
            continue
        if _SEPARATOR.match(line):
            flush()
            continue
        if not cur:
            cur.append((number, line)) if line.strip() else None
        else:
            cur.append((number, line))
    flush()
    return cells, default


def parse_ipynb(data: Any) -> tuple[list[Cell], str]:
    """Cells of a Jupyter notebook; also accepts a Synapse definition (the notebook under ``properties``)."""
    nb = data.get("properties") if isinstance(data, dict) and isinstance(data.get("properties"), dict) \
        and "cells" in data["properties"] else data
    if not isinstance(nb, dict) or not isinstance(nb.get("cells"), list):
        raise ValueError("not a notebook: no 'cells' list")
    meta = nb.get("metadata") if isinstance(nb.get("metadata"), dict) else {}
    lang_info = meta.get("language_info") if isinstance(meta.get("language_info"), dict) else {}
    default = {"python": "python", "scala": "scala", "sql": "sql", "r": "r", "csharp": "csharp"}.get(
        str(lang_info.get("name") or "").lower(), "python")
    cells: list[Cell] = []
    line = 1
    for raw in nb["cells"]:
        if not isinstance(raw, dict):
            continue
        src = raw.get("source", "")
        code = "".join(src) if isinstance(src, list) else str(src or "")
        n = code.count("\n") + 1
        if raw.get("cell_type") == "markdown":
            cells.append(Cell(len(cells), "markdown", code, line, None, "markdown"))
        elif raw.get("cell_type") == "code" and code.strip():
            lang, body, magic = _split_magic(code, default)
            meta = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
            tags = [t for t in meta.get("tags", []) if isinstance(t, str)] if isinstance(meta.get("tags"), list) else []
            cells.append(Cell(len(cells), lang, body, line, magic, "markdown" if lang == "markdown" else "code",
                              {"tags": tags} if tags else {}))
        line += n + 1
    return cells, default


def load(text: str, filename: str) -> tuple[list[Cell], str]:
    """Cells and default language of a notebook file's text (.ipynb / .json as Jupyter, anything else as SOURCE)."""
    if filename.lower().endswith((".ipynb", ".json")):
        try:
            return parse_ipynb(json.loads(text))
        except json.JSONDecodeError as exc:
            raise ValueError(f"not valid JSON ({exc.msg} at line {exc.lineno})") from None
    return parse_databricks_source(text, filename)
