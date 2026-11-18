"""ADF global parameters -> one Fabric Variable Library per factory (2026-11-16).

Fabric has no global parameters; Microsoft's ADF-to-Fabric guidance moves them to a Variable Library, which a pipeline reads
through ``properties.libraryVariables`` (``{"<alias>": {"type", "variableName", "libraryName"}}``) and the expression
``@pipeline().libraryVariables.<alias>``. The shape was verified live on 2026-11-16: a pipeline reading String and Integer variables
this way (a probe, not a converted pipeline) ran and returned the library's values.

A global parameter goes to the library only when a pipeline can read it from there and its value is a plain literal:
String, Int (library type Integer, read as Int) and Bool (library Boolean, read as Bool) as themselves. Float has no
pipeline-readable library type (Microsoft, "Variable library integration with pipelines", 2026-06-22: "Number types aren't supported
in pipelines"), and Array and Object have no library type at all, so these three are stored as a String holding their compact JSON
text (``0.75``, ``["EU","US"]``) and every reference is read back through ``float(...)`` or ``json(...)`` (expr_convert.py; ``ENCODED``).
An expression default has no literal value and stays a ``gp_`` pipeline parameter. (ADF itself refuses a SecureString global
parameter: "UnsupportedTypeSecureString", seen live 2026-11-18.)

The definition parts below (``variables.json``, ``settings.json``) are the ones Fabric's getDefinition returned for a library
created through the REST API.
"""

from __future__ import annotations

import json
import math
import re
from typing import Any, Iterable, Optional

from adf2fabric.agent.conversion.expr_convert import LIBRARY_TYPES

VARIABLES_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/variables/1.0.0/schema.json"
SETTINGS_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/settings/1.0.0/schema.json"
FOLDER = "variable-library"


def library_name(factory: Optional[str]) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", f"{factory or 'factory'}_globals")


def _value(kind: str, raw: Any) -> tuple[bool, Any]:
    """(usable, value) of a global parameter's authored default for library type ``kind``."""
    if kind == "String":
        return isinstance(raw, str), raw
    if kind == "Integer":
        return isinstance(raw, int) and not isinstance(raw, bool), raw
    if kind == "Boolean":
        return isinstance(raw, bool), raw
    return False, None


# ADF type -> the function every reference reads the String variable back with
ENCODED = {"float": "float", "array": "json", "object": "json"}


def _encodable(adf_type: str, raw: Any) -> bool:
    if adf_type == "float":
        return isinstance(raw, (int, float)) and not isinstance(raw, bool) and math.isfinite(raw)
    return isinstance(raw, list) if adf_type == "array" else isinstance(raw, dict)


def eligible(gp) -> Optional[tuple[str, str]]:
    """(library type, pipeline type) when global parameter ``gp`` can live in the library, else None."""
    t = (gp.type or "").lower()
    if gp.is_expression or not gp.has_default:
        return None
    if t in ENCODED:
        return ("String", "String") if _encodable(t, gp.default_raw) else None
    kinds = LIBRARY_TYPES.get(t)
    if not kinds:
        return None
    ok, _ = _value(kinds[0], gp.default_raw)
    return kinds if ok else None


def decoder(gp) -> Optional[str]:
    """``float`` / ``json`` when ``gp`` is held in the library as JSON text, else None."""
    return ENCODED.get((gp.type or "").lower()) if eligible(gp) else None


def stored_value(gp) -> Any:
    """The library value: the literal itself, or the compact JSON text of a Float / Array / Object (what ADF's string() prints)."""
    if decoder(gp):
        return json.dumps(gp.default_raw, separators=(",", ":"), ensure_ascii=False)
    return gp.default_raw


def variables(globals_: Iterable) -> list[dict]:
    out = []
    for gp in sorted(globals_, key=lambda g: g.name.lower()):
        kinds = eligible(gp)
        if kinds:
            fn = decoder(gp)
            note = f"ADF global parameter {gp.name}" + (f" ({gp.type}, stored as JSON text; pipelines read it with {fn}())" if fn else "")
            out.append({"name": gp.name, "note": note, "type": kinds[0], "value": stored_value(gp)})
    return out


def definition(globals_: Iterable) -> dict[str, str]:
    """The library's definition parts (path -> text), without ``.platform`` (the deploy and the package add their own)."""
    return {
        "variables.json": json.dumps({"$schema": VARIABLES_SCHEMA, "variables": variables(globals_)}, indent=2, ensure_ascii=False) + "\n",
        "settings.json": json.dumps({"$schema": SETTINGS_SCHEMA, "valueSetsOrder": []}, indent=2) + "\n",
    }


def referenced(contents: Iterable[dict]) -> dict[str, set[str]]:
    """Library name -> the variable names the given pipeline contents read from it."""
    out: dict[str, set[str]] = {}
    for c in contents:
        for ref in ((c or {}).get("properties") or {}).get("libraryVariables", {}).values():
            if isinstance(ref, dict) and ref.get("libraryName") and ref.get("variableName"):
                out.setdefault(ref["libraryName"], set()).add(ref["variableName"])
    return out


def load(conversion_dir) -> dict[str, dict[str, str]]:
    """Library name -> definition parts written by the last conversion (``conversion/variable-library/<name>.VariableLibrary/``)."""
    from pathlib import Path

    root = Path(conversion_dir) / FOLDER
    out: dict[str, dict[str, str]] = {}
    if root.is_dir():
        for d in sorted(root.glob("*.VariableLibrary")):
            out[d.name[: -len(".VariableLibrary")]] = {f.name: f.read_text(encoding="utf-8") for f in sorted(d.iterdir()) if f.is_file()}
    return out
