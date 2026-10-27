"""The prompt templates: Markdown files with placeholders, rendered without a template library.

* ``{{name}}`` and ``{{a.b}}``: a value from the context, or nothing if it is absent;
* ``{{#if name}} ... {{/if}}`` and ``{{#if name}} ... {{else}} ... {{/if}}``: kept when the value is non-empty; blocks nest;
* ``<!-- ... -->``: a comment for whoever edits the template, removed before it is sent.

Values are inserted in one pass and never scanned again, so source text that happens to contain ``{{`` is sent exactly as it is.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping

_COMMENT = re.compile(r"<!--.*?-->\s*", re.S)
_NAME = r"[A-Za-z_][\w.]*"
_IF = re.compile(r"\{\{#if\s+(" + _NAME + r")\s*\}\}((?:(?!\{\{#if\s).)*?)\{\{/if\}\}", re.S)  # the innermost block: its body holds no other #if
_ELSE = re.compile(r"\{\{else\}\}")
_VAR = re.compile(r"\{\{\s*(" + _NAME + r")\s*\}\}")
_BLANK = re.compile(r"\n{3,}")


class TemplateError(Exception):
    """A template could not be read or is malformed."""


def lookup(context: Mapping[str, Any], name: str) -> Any:
    value: Any = context
    for part in name.split("."):
        if not isinstance(value, Mapping) or part not in value:
            return None
        value = value[part]
    return value


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "\n".join(f"- {v}" for v in value)
    return str(value)


def render(template: str, context: Mapping[str, Any]) -> str:
    text = _COMMENT.sub("", template)

    def branch(m: "re.Match[str]") -> str:
        parts = _ELSE.split(m.group(2), maxsplit=1)
        return parts[0] if lookup(context, m.group(1)) else (parts[1] if len(parts) > 1 else "")

    while True:
        text, count = _IF.subn(branch, text)
        if not count:
            break
    if "{{#if" in text or "{{/if}}" in text:
        raise TemplateError("An {{#if}} block is not closed, or an {{/if}} has no {{#if}}.")
    return _BLANK.sub("\n\n", _VAR.sub(lambda m: _text(lookup(context, m.group(1))), text)).strip() + "\n"


def read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""
    except OSError as exc:
        raise TemplateError(f"The template {path.name} could not be read: {exc.strerror or exc}.") from exc
