"""The ADF pipeline and its deployed Fabric twin, side by side: do they consist of the same things?

Compared, by activity name (ADF requires names to be unique within a pipeline): that every activity is present, where it sits
(its parent container), its dependency edges with conditions, which pipelines it starts, how many datasets it uses (ADF names them,
Fabric inlines them), that its Fabric type is the expected one, and that every connection is a real id. Differences the converter
documents (``ExecutePipeline -> InvokePipeline``, a dataset inlined, a linked service that became a connection) are listed as
``documented``; anything else is a ``difference``. Nothing here is a runtime claim: it says the structure is carried, not that it runs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from adf2fabric.agent.structure.tree import Structure, is_bound

# Fabric type each ADF type becomes when the conversion record does not say otherwise (the documented ones only).
DOCUMENTED_TYPES: dict[str, tuple[str, ...]] = {
    "ExecutePipeline": ("InvokePipeline",), "SynapseNotebook": ("TridentNotebook",), "DatabricksNotebook": ("DatabricksNotebook", "TridentNotebook"),
}


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class Parity:
    pipeline: str
    adf_activities: int
    fabric_activities: int
    checks: list[Check] = field(default_factory=list)
    differences: list[str] = field(default_factory=list)  # not documented: needs a person
    documented: list[str] = field(default_factory=list)  # carried differently, as the converter documents

    @property
    def verdict(self) -> str:
        if self.differences:
            return "differs"
        return "equivalent_with_documented_differences" if self.documented else "equivalent"


def _edges(s: Structure) -> dict[str, set[tuple[str, tuple[str, ...]]]]:
    return {n.name.lower(): {(p.lower(), c) for p, c in n.depends} for n in s.nodes}


def compare(adf: Structure, fabric: Structure, expected_types: Optional[dict[str, tuple[str, ...]]] = None) -> Parity:
    """``expected_types``: ADF activity name (lower) -> the Fabric types the conversion record says it became."""
    p = Parity(adf.pipeline, len(adf.nodes), len(fabric.nodes))
    a = {n.name.lower(): n for n in adf.nodes}
    f = {n.name.lower(): n for n in fabric.nodes}

    missing = sorted(a[k].name for k in a if k not in f)
    extra = sorted(f[k].name for k in f if k not in a)
    p.checks.append(Check("activities present", not missing, f"missing in Fabric: {', '.join(missing)}" if missing else f"{len(a)} of {len(a)} present"))
    if missing:
        p.differences.append(f"activities missing in Fabric: {', '.join(missing)}")
    if extra:
        p.documented.append(f"activities added by the conversion: {', '.join(extra)}")
    both = sorted(set(a) & set(f))

    moved = [a[k].name for k in both if (a[k].parent or "").lower() != (f[k].parent or "").lower()]
    p.checks.append(Check("nesting", not moved, f"sits under a different container: {', '.join(moved)}" if moved else f"{adf.max_depth} level(s) carried"))
    if moved:
        p.differences.append(f"nesting differs for: {', '.join(moved)}")

    ea, ef = _edges(adf), _edges(fabric)
    wrong = []
    for k in both:
        mine = {e for e in ea[k] if e[0] in f}  # an edge to an activity that is missing is already reported above
        if mine != ef[k]:
            wrong.append(a[k].name)
    p.checks.append(Check("dependencies", not wrong, f"dependsOn differs for: {', '.join(wrong)}" if wrong else f"{adf.edges} edge(s), conditions included"))
    if wrong:
        p.differences.append(f"dependency edges differ for: {', '.join(wrong)}")

    ca, cf = adf.called, fabric.called
    same_calls = [c.lower() for c in ca] == [c.lower() for c in cf]
    p.checks.append(Check("pipelines started", same_calls, f"ADF starts {ca or 'none'}; Fabric starts {cf or 'none'}" if not same_calls else
                          (f"starts {', '.join(ca)}" if ca else "starts no other pipeline")))
    if not same_calls:
        p.differences.append(f"pipelines started differ: ADF {ca or 'none'}, Fabric {cf or 'none'}")

    typed = []
    for k in both:
        want = (expected_types or {}).get(k) or DOCUMENTED_TYPES.get(a[k].type, (a[k].type,))
        if f[k].type not in want:
            typed.append(f"{a[k].name} ({a[k].type} -> {f[k].type}, expected {'/'.join(want)})")
        elif f[k].type != a[k].type:
            p.documented.append(f"{a[k].name}: {a[k].type} became {f[k].type}")
    p.checks.append(Check("activity types", not typed, "; ".join(typed) if typed else "every type is the expected one"))
    if typed:
        p.differences.append("types differ: " + "; ".join(typed))

    fewer = [f"{a[k].name} ({len(a[k].datasets)} in ADF, {len(f[k].datasets)} inline in Fabric)" for k in both if a[k].datasets and len(f[k].datasets) < len(set(a[k].datasets))]
    p.checks.append(Check("datasets inlined", not fewer, "; ".join(fewer) if fewer else "every dataset an activity used is inline in Fabric"))
    if fewer:
        p.differences.append("datasets not carried inline: " + "; ".join(fewer))
    if any(n.datasets for n in adf.nodes):
        p.documented.append("datasets became inline datasetSettings (Fabric has no dataset item)")

    loose = sorted({c for n in fabric.nodes for c in n.connections if not is_bound(c)})
    p.checks.append(Check("connections bound", not loose, f"unbound: {', '.join(loose)}" if loose else
                          f"{len({c for n in fabric.nodes for c in n.connections})} connection(s), all real ids"))
    if loose:
        p.differences.append(f"unbound connections in the deployed pipeline: {', '.join(loose)}")
    if any(n.connections for n in adf.nodes):
        p.documented.append("linked services became Fabric connections")

    pa, pf = set(adf.parameters), set(fabric.parameters)
    lost = sorted(pa - pf)
    p.checks.append(Check("parameters", not lost, f"missing in Fabric: {', '.join(lost)}" if lost else f"{len(pa)} parameter(s), {len(adf.variables)} variable(s)"))
    if lost:
        p.differences.append(f"parameters missing in Fabric: {', '.join(lost)}")
    return p
