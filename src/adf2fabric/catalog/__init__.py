"""The master compatibility catalog (``compatibility.json``): the one place that says what each source component is, what it can
become on the target, under which condition, what is and is not carried, the workarounds, and which Microsoft page says so.

It is the intermediate layer of the migrator: a capability (``copy``, ``lookup``, ``execute_pipeline``, ``databricks_job`` ...) is
platform-neutral, and each platform names its own types for it (``adf_types``, ``fabric_activities``); ``directions`` lists the
conversions the code implements (``adf->fabric`` today). The converter's handlers still build the target JSON in code, but every
fact they rely on is read from here: the rule descriptions (``registry``), the conversion options and switches
(``mapping_catalog``), the lifecycle catalog (``compatibility``), connector verdicts (``connectors``) and the target schema
(``fabric_schema``). When Microsoft's documentation changes, edit the JSON (and the source's ``read_on`` date), not the code.
``tests/test_master_catalog.py`` checks the file's shape and that the code agrees with it.
"""

from __future__ import annotations

import json
from functools import lru_cache
from importlib import resources
from typing import Any, Optional


@lru_cache(maxsize=1)
def master() -> dict[str, Any]:
    return json.loads(resources.files("adf2fabric.catalog").joinpath("compatibility.json").read_text(encoding="utf-8"))


def capability(name: str) -> dict[str, Any]:
    return master()["capabilities"][name]


def capability_for(adf_type: Optional[str]) -> Optional[tuple[str, dict[str, Any]]]:
    """(capability id, its entry) for an ADF activity type, case-insensitively; None when no capability covers it."""
    low = (adf_type or "").lower()
    for name, cap in master()["capabilities"].items():
        if any(t.lower() == low for t in cap["adf_types"]):
            return name, cap
    return None


def by_adf_type(adf_type: Optional[str]) -> Optional[dict[str, Any]]:
    """The conversion entry (options, workarounds, lifecycle, Microsoft's comparison) for one ADF activity type."""
    hit = capability_for(adf_type)
    if hit is None:
        return None
    return next(v for k, v in hit[1]["by_adf_type"].items() if k.lower() == (adf_type or "").lower())


def source(source_id: str) -> dict[str, Any]:
    return master()["sources"][source_id]
