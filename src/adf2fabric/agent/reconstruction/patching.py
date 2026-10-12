"""A small, strict JSON-patch engine (``add`` / ``replace`` over JSON pointers).

Proposals never edit anything. They carry patches against the ORIGINAL file; patches are applied
to deep COPIES held in memory (and written only to ``reconstruction/completed/``), so the original
ADF JSON on disk is physically never touched.
"""

from __future__ import annotations

import copy
import re
from typing import Any, Union

from adf2fabric.project.reconstruction import Patch, PatchOp

Token = Union[str, int]
_TOKEN = re.compile(r"([^.\[\]]+)|\[(\d+)\]")


class PatchError(Exception):
    pass


def ir_pointer_tokens(pointer: str) -> list[Token]:
    """``properties.activities[2].typeProperties.ifTrueActivities[0]`` -> path tokens."""
    out: list[Token] = []
    for name, idx in _TOKEN.findall(pointer):
        out.append(int(idx) if idx != "" else name)
    return out


def to_pointer(tokens: list[Token]) -> str:
    return "".join("/" + str(t).replace("~", "~0").replace("/", "~1") for t in tokens)


def from_pointer(pointer: str) -> list[Token]:
    if pointer in ("", "/"):
        return []
    out: list[Token] = []
    for part in pointer.split("/")[1:]:
        part = part.replace("~1", "/").replace("~0", "~")
        out.append(int(part) if part.isdigit() else part)
    return out


def get(doc: Any, tokens: list[Token]) -> Any:
    cur = doc
    for t in tokens:
        if isinstance(cur, dict) and isinstance(t, str) and t in cur:
            cur = cur[t]
        elif isinstance(cur, list) and isinstance(t, int) and 0 <= t < len(cur):
            cur = cur[t]
        else:
            raise PatchError(f"path {to_pointer(tokens)!r} does not exist")
    return cur


def exists(doc: Any, tokens: list[Token]) -> bool:
    try:
        get(doc, tokens)
        return True
    except PatchError:
        return False


def set_ops(doc: Any, tokens: list[Token], value: Any) -> list[PatchOp]:
    """The minimal ops that make ``tokens`` hold ``value`` in ``doc`` (creating missing parents)."""
    cur = doc
    for i, t in enumerate(tokens):
        if isinstance(cur, dict) and isinstance(t, str):
            if t not in cur:
                nested: Any = value
                for rest in reversed(tokens[i + 1:]):
                    nested = {str(rest): nested}
                return [PatchOp(op="add", path=to_pointer(tokens[: i + 1]), value=nested)]
            cur = cur[t]
        elif isinstance(cur, list) and isinstance(t, int) and 0 <= t < len(cur):
            cur = cur[t]
        else:
            raise PatchError(f"cannot descend into {to_pointer(tokens[: i + 1])!r}")
    return [PatchOp(op="replace", path=to_pointer(tokens), value=value)]


def append_ops(doc: Any, list_tokens: list[Token], value: Any) -> list[PatchOp]:
    """Append ``value`` to the list at ``list_tokens`` (creating the list when absent)."""
    if exists(doc, list_tokens):
        target = get(doc, list_tokens)
        if not isinstance(target, list):
            raise PatchError(f"{to_pointer(list_tokens)!r} is not a list")
        return [PatchOp(op="add", path=to_pointer([*list_tokens, "-"]), value=value)]
    return set_ops(doc, list_tokens, [value])


def apply_ops(doc: Any, ops: list[PatchOp]) -> Any:
    """A patched deep copy of ``doc``. ``doc`` itself is never modified."""
    out = copy.deepcopy(doc)
    for op in ops:
        tokens = from_pointer(op.path)
        if not tokens:
            raise PatchError("cannot patch the document root")
        parent = get(out, tokens[:-1])
        last = tokens[-1]
        value = copy.deepcopy(op.value)
        if isinstance(parent, dict):
            if op.op == "replace" and str(last) not in parent:
                raise PatchError(f"replace target {op.path!r} does not exist")
            parent[str(last)] = value
        elif isinstance(parent, list):
            if last == "-":
                parent.append(value)
            elif isinstance(last, int) and op.op == "add" and 0 <= last <= len(parent):
                parent.insert(last, value)
            elif isinstance(last, int) and op.op == "replace" and 0 <= last < len(parent):
                parent[last] = value
            else:
                raise PatchError(f"bad list index in {op.path!r}")
        else:
            raise PatchError(f"cannot patch inside a scalar at {op.path!r}")
    return out


def apply_patch(doc: Any, patch: Patch) -> Any:
    return apply_ops(doc, patch.ops)
