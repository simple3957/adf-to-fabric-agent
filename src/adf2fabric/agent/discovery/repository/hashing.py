"""Deterministic content hashing for Repository Discovery.

Streams file content in fixed-size chunks so large files are never fully loaded
into memory. Content-derived hashes make the inventory reproducible for a given
commit regardless of filesystem traversal order.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

_HASH_CHUNK = 65536


def sha256_file(path: Path) -> str:
    """Return the streamed SHA-256 hex digest of the file at ``path``."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()
