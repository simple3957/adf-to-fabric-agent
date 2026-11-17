"""Every way each ADF activity type can be converted, read from the master compatibility catalog (``adf2fabric/catalog``).

Each option names the condition the handler tests (``when``), what it writes (``fabric``: the Fabric activity types, empty when the
activity is replaced by a fail-fast placeholder), the status the activity gets, and the code it is recorded under (``code``) when it is
not plainly converted. An option that belongs to a choice a person may switch per activity carries ``switch`` (``storage``, ``spark``,
``dataflow``; see ``master()["switches"]``). The project's activity mapping (``agent/conversion/project_mapping.py``) is built from these.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from adf2fabric.catalog import master

CONVERTED, DIFFERENCES, MANUAL, BLOCKED = "converted", "converted_with_differences", "manual_required", "blocked"


@dataclass(frozen=True)
class Path:
    when: str
    fabric: tuple[str, ...]
    status: str
    how: str
    code: str = ""
    switch: Optional[tuple[str, str]] = None  # (kind, value) when a person selects this option per activity


@dataclass(frozen=True)
class Workaround:
    what: str
    source: str  # handler | compatibility | manual


@dataclass(frozen=True)
class Entry:
    paths: tuple[Path, ...]
    workarounds: tuple[Workaround, ...] = field(default_factory=tuple)


def _load() -> dict[str, Entry]:
    out: dict[str, Entry] = {}
    for cap in master()["capabilities"].values():
        for adf_type, e in cap["by_adf_type"].items():
            paths = tuple(Path(o["when"], tuple(o["fabric"]), o["status"], o["how"], o.get("code", ""),
                               (o["switch"]["kind"], o["switch"]["value"]) if o.get("switch") else None) for o in e["options"])
            out[adf_type] = Entry(paths, tuple(Workaround(w["what"], w["source"]) for w in e["workarounds"]))
    return out


CATALOG: dict[str, Entry] = _load()
GENERAL: tuple[dict[str, str], ...] = tuple(master()["rules_for_every_activity"])
STRATEGIES: dict[str, str] = master()["strategies"]
STATUSES: dict[str, str] = master()["statuses"]
SWITCHES: dict[str, dict] = master()["switches"]
