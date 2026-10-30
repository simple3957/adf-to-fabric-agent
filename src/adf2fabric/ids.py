"""Identifiers as Fabric and Azure write them. One definition, so every module agrees on what a GUID is."""

from __future__ import annotations

import re
from typing import Optional

GUID = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")


def is_guid(value: Optional[str]) -> bool:
    return bool(value) and bool(GUID.match(value))


def normalize_guid(value: Optional[str]) -> str:
    """An id as Fabric reports it: no spaces, no {braces}, lower case. Anything that is not a GUID after that is returned trimmed and unchanged,
    so the caller's own check still says what is wrong with it."""
    v = (value or "").strip().strip("{}").strip()
    return v.lower() if GUID.match(v) else (value or "").strip()
