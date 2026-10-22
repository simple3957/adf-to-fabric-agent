"""Independent inventory of a pipeline's activities, read straight from the raw JSON.

The parser (``parser_pipelines``) walks activities with its own code. This module re-derives the same inventory from
the raw JSON by Microsoft's DOCUMENTED structure only, so a parser defect, a nesting limit or a malformed list shows up
as a difference instead of a silently missing activity:

* ``properties.activities`` holds the pipeline's activities;
* an activity's children live under ``typeProperties``: ``activities`` (ForEach, Until), ``ifTrueActivities`` /
  ``ifFalseActivities`` (IfCondition), ``defaultActivities`` and ``cases[].activities`` (Switch).

Free-form content (a Web activity ``body``, a parameter default, a Set Variable value) can contain objects that merely
LOOK like activities. Those are never counted as activities; a separate, wider scan lists them as ``suspect_pointers``
(informational), which is also how an unfamiliar container shape is surfaced.

Pointers use the form of ``AdfSourceLocation.pointer`` (``properties.activities[2].typeProperties.ifTrueActivities[0]``),
so the sets compare directly. Nothing here interprets a value.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

from adf2fabric.project.discovery import AdfActivity, AdfInventoryCheck

_CONTAINER_KEYS = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")
_ACTIVITY_LIST_KEY = re.compile(r"activities$", re.IGNORECASE)
_ACTIVITY_MARKERS = ("typeProperties", "dependsOn", "policy", "linkedServiceName")
_MAX_SCAN_DEPTH = 200  # JSON this deep is not a pipeline; stops a hostile document from exhausting the stack


def _looks_like_activity(obj: Any) -> bool:
    return (isinstance(obj, dict) and isinstance(obj.get("name"), str) and isinstance(obj.get("type"), str)
            and any(k in obj for k in _ACTIVITY_MARKERS))


def scan_documented_activities(data: Any) -> tuple[set[str], list[str]]:
    """(pointers of the activities in the documented lists, pointers of list entries that are not objects)."""
    found: set[str] = set()
    malformed: list[str] = []
    props = data.get("properties") if isinstance(data, dict) else None
    stack: list[tuple[Any, str, int]] = [(props.get("activities") if isinstance(props, dict) else None,
                                          "properties.activities", 0)]
    while stack:
        items, pointer, depth = stack.pop()
        if not isinstance(items, list) or depth > _MAX_SCAN_DEPTH:
            continue
        for i, item in enumerate(items):
            here = f"{pointer}[{i}]"
            if not isinstance(item, dict):
                malformed.append(here)
                continue
            found.add(here)
            tp = item.get("typeProperties")
            if not isinstance(tp, dict):
                continue
            for key in _CONTAINER_KEYS:
                stack.append((tp.get(key), f"{here}.typeProperties.{key}", depth + 1))
            cases = tp.get("cases")
            if isinstance(cases, list):
                for ci, case in enumerate(cases):
                    if isinstance(case, dict):
                        stack.append((case.get("activities"), f"{here}.typeProperties.cases[{ci}].activities", depth + 1))
    return found, sorted(malformed)


def scan_activity_shaped_objects(data: Any) -> set[str]:
    """Pointers of every object that looks like an activity ANYWHERE in the document (a deliberately wide net)."""
    found: set[str] = set()
    stack: list[tuple[Any, str, int]] = [(data, "", 0)]
    while stack:
        obj, path, depth = stack.pop()
        if depth > _MAX_SCAN_DEPTH:
            continue
        if isinstance(obj, dict):
            for key, value in obj.items():
                child = f"{path}.{key}" if path else str(key)
                if isinstance(value, list) and _ACTIVITY_LIST_KEY.search(str(key)):
                    found.update(f"{child}[{i}]" for i, item in enumerate(value) if isinstance(item, dict))
                stack.append((value, child, depth + 1))
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                child = f"{path}[{i}]"
                if _looks_like_activity(item):
                    found.add(child)
                stack.append((item, child, depth + 1))
    return found


def walk(activities: list[AdfActivity]) -> Iterator[AdfActivity]:
    for a in activities:
        yield a
        yield from walk(a.activities)


def reconcile(activities: list[AdfActivity], raw: Any) -> AdfInventoryCheck:
    """Compare the parsed activities with the independent scan of ``raw`` (the pipeline JSON)."""
    parsed = {a.source_location.pointer for a in walk(activities) if a.source_location is not None}
    source, malformed = scan_documented_activities(raw)
    missing = sorted(source - parsed)
    unexpected = sorted(parsed - source)
    suspects = sorted(scan_activity_shaped_objects(raw) - source - parsed)
    return AdfInventoryCheck(
        source_activity_count=len(source), parsed_activity_count=len(parsed),
        matched=not missing and not unexpected,
        missing_pointers=missing, unexpected_pointers=unexpected, malformed_pointers=malformed,
        suspect_pointers=suspects)
