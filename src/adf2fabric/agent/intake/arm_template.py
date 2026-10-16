"""Turn an ``adf_publish`` ARM template into the ADF Git folder layout.

The ``adf_publish`` branch holds ``ARMTemplateForFactory.json`` (and, for large factories, ``linkedTemplates/*.json``)
instead of ``pipeline/``, ``dataset/`` ... folders. Every later stage reads the Git layout, so this module writes that
layout next to the template, in a generated folder. The cloned files themselves are never modified.

Only literal content is carried over. A value that is exactly ``[parameters('x')]`` is replaced by the template's
default for ``x`` when there is one (never for secure strings); every other ARM expression is left as written and
counted, so the caller can say so.
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

EXTRACT_DIR = "_adf_publish_extracted"

_FACTORY_PREFIX = "microsoft.datafactory/factories/"
# ARM ``factories/<subtype>`` -> folder name in the ADF Git layout.
_SUBTYPE_DIR = {
    "pipelines": "pipeline",
    "datasets": "dataset",
    "linkedservices": "linkedService",
    "triggers": "trigger",
    "integrationruntimes": "integrationRuntime",
    "dataflows": "dataflow",
    "managedvirtualnetworks": "managedVirtualNetwork",
    "credentials": "credential",
    "adfcdcs": "changedatacapture",
}
_NAME_RE = re.compile(r"'/([^']+)'\)\]$")
_PARAM_RE = re.compile(r"^\[parameters\('([^']+)'\)\]$")
_BAD_NAME = re.compile(r'[<>:"|?*\\/\x00-\x1f]')
_MAX_DEPTH = 60


@dataclass
class ArmExtraction:
    root: Path  # the generated Git-layout folder
    template: str  # the ARM template it came from, relative to the scanned root
    counts: dict[str, int] = field(default_factory=dict)
    skipped: list[str] = field(default_factory=list)
    unresolved_expressions: int = 0
    other_templates: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return sum(self.counts.values())


def _resource_name(raw: Any) -> Optional[str]:
    if not isinstance(raw, str):
        return None
    match = _NAME_RE.search(raw)
    if match:
        return match.group(1)
    if "[" not in raw and "/" in raw:  # a literal "factory/Name"
        return raw.rsplit("/", 1)[-1]
    return None


def _defaults(template: dict) -> dict[str, Any]:
    out: dict[str, Any] = {}
    params = template.get("parameters")
    if isinstance(params, dict):
        for name, spec in params.items():
            if (isinstance(spec, dict) and "defaultValue" in spec
                    and str(spec.get("type", "")).lower() != "securestring"):
                out[name] = spec["defaultValue"]
    return out


class _Resolver:
    def __init__(self, defaults: dict[str, Any]):
        self.defaults = defaults
        self.unresolved = 0

    def walk(self, node: Any, depth: int = 0) -> Any:
        if depth > _MAX_DEPTH:
            return node
        if isinstance(node, dict):
            return {k: self.walk(v, depth + 1) for k, v in node.items()}
        if isinstance(node, list):
            return [self.walk(v, depth + 1) for v in node]
        if isinstance(node, str) and node.startswith("["):
            if node.startswith("[["):  # ARM escape for a literal leading "["
                return node[1:]
            match = _PARAM_RE.match(node)
            if match and match.group(1) in self.defaults:
                return self.defaults[match.group(1)]
            if node.endswith("]"):
                self.unresolved += 1
        return node


def _read_json(path: Path) -> Optional[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _template_files(template: Path) -> list[Path]:
    files = [template]
    linked = template.parent / "linkedTemplates"
    if linked.is_dir():
        files += sorted(p for p in linked.glob("*.json") if p.is_file())
    return files


def extract(root: Path, arm_templates: list[str]) -> Optional[ArmExtraction]:
    """Write the Git layout for the first usable template in ``arm_templates`` (paths relative to ``root``).

    Returns None when no template holds any factory resource. The output folder is generated, so it is replaced on
    every call.
    """
    for index, rel in enumerate(arm_templates):
        template = root / rel
        out = template.parent / EXTRACT_DIR
        result = _extract_one(template, out, rel)
        if result is not None:
            result.other_templates = [t for i, t in enumerate(arm_templates) if i != index]
            return result
    return None


def _extract_one(template: Path, out: Path, rel: str) -> Optional[ArmExtraction]:
    main = _read_json(template)
    if main is None:
        return None
    resolver = _Resolver(_defaults(main))
    found: list[tuple[str, str, dict]] = []
    skipped: list[str] = []
    for file in _template_files(template):
        data = main if file == template else _read_json(file)
        if data is None:
            skipped.append(f"{file.name} (not readable JSON)")
            continue
        if file != template:
            resolver.defaults.update(_defaults(data))
        for res in data.get("resources") or []:
            if not isinstance(res, dict):
                continue
            rtype = str(res.get("type", "")).lower()
            if not rtype.startswith(_FACTORY_PREFIX):
                continue
            subtype = rtype[len(_FACTORY_PREFIX):]
            folder = _SUBTYPE_DIR.get(subtype)
            name = _resource_name(res.get("name"))
            if folder is None:
                skipped.append(f"{subtype} {name or ''}".strip())
                continue
            if not name or _BAD_NAME.search(name) or name.startswith("."):
                skipped.append(f"{subtype} {name or str(res.get('name'))!r} (name not usable)")
                continue
            props = res.get("properties")
            if not isinstance(props, dict):
                skipped.append(f"{subtype} {name} (no properties)")
                continue
            found.append((folder, name, props))
    if not found:
        return None
    if out.exists():
        shutil.rmtree(out)
    counts: dict[str, int] = {}
    seen: set[tuple[str, str]] = set()
    for folder, name, props in found:
        key = (folder, name.lower())
        if key in seen:
            skipped.append(f"{folder} {name} (defined more than once)")
            continue
        seen.add(key)
        target = out / folder
        target.mkdir(parents=True, exist_ok=True)
        body = {"name": name, "properties": resolver.walk(props)}
        (target / f"{name}.json").write_text(json.dumps(body, indent=2), encoding="utf-8")
        counts[folder] = counts.get(folder, 0) + 1
    return ArmExtraction(root=out, template=rel, counts=dict(sorted(counts.items())), skipped=skipped,
                         unresolved_expressions=resolver.unresolved)
