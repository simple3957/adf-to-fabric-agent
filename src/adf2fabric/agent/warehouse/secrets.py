"""Detect and mask secret material in source SQL before anything is stored.

Real source scripts contain secrets (master-key passwords, credential secrets, SAS tokens, connection-string
passwords). They are masked here, counted, and never written to a catalog, manifest, report or log.
"""

from __future__ import annotations

import re

MASK = "***MASKED***"

_PATTERNS = [
    # CREATE MASTER KEY ENCRYPTION BY PASSWORD = '...'
    re.compile(r"(PASSWORD\s*=\s*N?')([^']*)(')", re.I),
    # CREATE DATABASE SCOPED CREDENTIAL ... SECRET = '...'  /  COPY ... CREDENTIAL = (..., SECRET = '...')
    re.compile(r"(SECRET\s*=\s*N?')([^']*)(')", re.I),
    # connection-string style key=value secrets
    re.compile(r"((?:PWD|Password|AccountKey|SharedAccessSignature)\s*=\s*)([^;'\"\s*][^;'\"]*)", re.I),
    # SAS token query strings
    re.compile(r"((?:\?|&)sig=)([^&'\"\s]+)", re.I),
]


def mask(text: str) -> tuple[str, int]:
    """Return (masked text, number of secrets masked)."""
    total = 0
    for p in _PATTERNS:
        if p.groups == 3:
            text, n = p.subn(lambda m: f"{m.group(1)}{MASK}{m.group(3)}", text)
        else:
            text, n = p.subn(lambda m: f"{m.group(1)}{MASK}", text)
        total += n
    return text, total
