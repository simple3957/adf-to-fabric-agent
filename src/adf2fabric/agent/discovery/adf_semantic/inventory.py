"""Independent inventory of a pipeline's activities, read straight from the raw JSON.

The parser (``parser_pipelines``) walks the documented containers. This module finds activities by a different,
deliberately more general rule, so that an activity the parser does not reach (an unfamiliar container shape, a
malformed list, a nesting limit) shows up as a difference instead of disappearing:

* every object in a list stored under any key that ends in ``activities`` is an activity, wherever that list is; and
* any other object in a list that carries ``name`` + ``type`` + one of ``typeProperties`` / ``dependsOn`` / ``policy`` /
  ``linkedServiceName`` is an activity too.

Pointers use the form of ``AdfSourceLocation.pointer`` (``properties.activities[2].typeProperties.ifTrueActivities[0]``),
so the two sets can be compared directly. Nothing here interprets a value.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

from adf2fabric.project.discovery import AdfActivity, AdfInventoryCheck

_ACTIVITY_LIST_KEY = re.compile(r"activities$", re.IGNORECASE)
_ACTIVITY_MARKERS = ("typeProperties", "dependsOn", "policy", "linkedServiceName")
_MAX_SCAN_DEPTH = 200  # JSON this deep is not a pipeline; stops a hostile document from exhausting the stack


def _looks_like_activity(obj: Any) -> bool:
    return (isinstance(obj, dict) and isinstance(obj.get("name"), str) and isinstance(obj.get("type"), str)
            and any(k in obj for k in _ACTIVITY_MARKERS))


def scan_source_activities(data: Any) -> tuple[set[str], list[str]]:
    """(pointers of every activity found in ``data``, pointers of list entries that are not objects)."""
    found: set[str] = set()
    malformed: list[str] = []
    stack: list[tuple[Any, str, int]] = [(data, "", 0)]  # iterative: no recursion limit to hit
    while stack:
        obj, path, depth = stack.pop()
        if depth > _MAX_SCAN_DEPTH:
            continue
        if isinstance(obj, dict):
            for key, value in obj.items():
                child = f"{path}.{key}" if path else str(key)
                if isinstance(value, list) and _ACTIVITY_LIST_KEY.search(str(key)):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            found.add(f"{child}[{i}]")
                        else:
                            malformed.append(f"{child}[{i}]")
                stack.append((value, child, depth + 1))
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                child = f"{path}[{i}]"
                if _looks_like_activity(item):
                    found.add(child)
                stack.append((item, child, depth + 1))
    return found, sorted(malformed)


def walk(activities: list[AdfActivity]) -> Iterator[AdfActivity]:
    for a in activities:
        yield a
        yield from walk(a.activities)


def reconcile(activities: list[AdfActivity], raw: Any) -> AdfInventoryCheck:
    """Compare the parsed activities with the independent scan of ``raw`` (the pipeline JSON)."""
    parsed = {a.source_location.pointer for a in walk(activities) if a.source_location is not None}
    source, malformed = scan_source_activities(raw)
    missing = sorted(source - parsed)
    unexpected = sorted(parsed - source)
    return AdfInventoryCheck(
        source_activity_count=len(source), parsed_activity_count=len(parsed),
        matched=not missing and not unexpected,
        missing_pointers=missing, unexpected_pointers=unexpected, malformed_pointers=malformed)
