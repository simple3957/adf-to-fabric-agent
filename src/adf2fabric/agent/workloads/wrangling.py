"""ADF wrangling data flow (Power Query) -> a Fabric Dataflow Gen2 definition (2026-11-17).

ADF stores a wrangling data flow as ``typeProperties.script`` (the Power Query mashup document), ``documentLocale`` and ``sources``
(Microsoft.DataFactory/factories/dataflows, type WranglingDataFlow). A Fabric Dataflow Gen2 is created with a definition of two parts
(Fabric REST "Dataflow definition", page updated 2026-06-16): ``queryMetadata.json`` (``formatVersion`` "202502", ``name``,
``documentLocale``, ``queriesMetadata`` keyed by query name with ``queryId`` / ``queryName`` / ``loadEnabled``) and ``mashup.pq`` (a section
document of ``shared <query> = ...;`` queries). The script is carried as the mashup unchanged; nothing in it is rewritten.

Not generated, because Microsoft documents neither for Power Query queries: the connections the queries sign in with
(``queryMetadata.connections`` holds ids only Fabric knows) and the output destination (the ADF sinks). Both are manual steps on the
workload and on the converted activity, never guessed.
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any

FORMAT_VERSION = "202502"
_NS = uuid.UUID("7b3c1f0e-2a54-4e5b-9d3f-0adf2fab1d01")
_SHARED = re.compile(r'^\s*shared\s+(#"(?:[^"]|"")+"|[A-Za-z_][\w.]*)\s*=', re.M)


def query_names(script: str) -> list[str]:
    """The ``shared`` queries of a section document, in order (a quoted name keeps its quotes removed)."""
    out = []
    for m in _SHARED.finditer(script or ""):
        n = m.group(1)
        out.append(n[2:-1].replace('""', '"') if n.startswith('#"') else n)
    return out


def mashup(script: str) -> str:
    """The mashup document: the ADF script itself, with a section header added only when the script has none."""
    text = (script or "").strip()
    return text if re.match(r"^\s*(\[[^\]]*\]\s*)?section\s", text) else f"section Section1;\n{text}"


def definition(name: str, type_properties: dict[str, Any]) -> tuple[dict[str, str], list[str]]:
    """(the definition parts as text, what a person must still set). Raises ValueError when the data flow has no script."""
    script = type_properties.get("script")
    if not isinstance(script, str) or not script.strip():
        raise ValueError("the wrangling data flow has no Power Query script")
    queries = query_names(script)
    if not queries:
        raise ValueError("the Power Query script defines no shared query")
    sources = {str(s.get("name")) for s in type_properties.get("sources") or [] if isinstance(s, dict)}
    meta = {
        "formatVersion": FORMAT_VERSION, "name": name, "documentLocale": type_properties.get("documentLocale") or "en-US",
        # every query keeps loading as in ADF, where the sink decides what is written; the source queries are what ADF generated
        "queriesMetadata": {q: {"queryId": str(uuid.uuid5(_NS, f"{name}/{q}")), "queryName": q, "loadEnabled": q not in sources} for q in queries},
    }
    todo = ["Sign in each source query's connection in the Dataflow Gen2 (Fabric asks when it is opened); connections are not in the definition.",
            "Set the output destination of the query the ADF sink wrote (Microsoft documents no destination format for Power Query queries)."]
    return {"queryMetadata.json": json.dumps(meta, indent=2, ensure_ascii=False) + "\n", "mashup.pq": mashup(script) + "\n"}, todo
