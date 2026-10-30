"""Normalised configuration diffs with redaction.

A diff is built from the FLATTENED, sorted property paths of two JSON values, so key order and formatting never show up as a
difference. Before anything is printed or stored:

* a value under a secret-named key (password, key, secret, token, connection string, SAS ...) is replaced by ``[redacted]``,
* a value that looks like a credential (bearer token, JWT, account key, SAS signature) is replaced by ``[redacted]``,
* long values are truncated.

The report therefore says WHERE two configurations differ without ever carrying a secret.
"""

from __future__ import annotations

import re
from typing import Any, Iterator

from adf2fabric.connections.redaction import redact

SECRET_KEY = re.compile(r"(?i)(password|passwd|pwd|secret|token|apikey|api_key|accountkey|account_key|sharedaccess|sas|connectionstring|"
                        r"credential|privatekey|private_key|authorization)")
_SECRET_VALUE = re.compile(r"(?i)(bearer\s+[\w\-.~+/=]+|eyJ[\w\-.]{20,}|AccountKey=[^;\s]+|SharedAccessSignature=[^;\s]+|sig=[^&\s]+|"
                           r"password=[^;&\s]+|[A-Za-z0-9+/]{40,}={0,2})")
_SAFE_KEYS = {"secretname", "secretversion", "secretpermissions", "secretbase", "tokenaudience"}  # names/references, not values
# Encrypted by ADF for its own factory (the integration runtime's credential manager): not plaintext, and unreadable anywhere else. Still redacted.
_ENCRYPTED_KEYS = {"encryptedcredential"}
MAX = 120


def redact_value(key: str, value: Any) -> str:
    """The display form of one leaf value: redacted when its key or its shape says it may be a secret."""
    last = re.split(r"[.\[\]]", key.rstrip("]"))[-1].lower() if key else ""
    text = "null" if value is None else str(value)
    if isinstance(value, str) and value.startswith(("@", "<")) or last in _SAFE_KEYS:
        pass
    elif SECRET_KEY.search(last) and value not in (None, "", False, True) and not str(value).startswith(("@", "<")):
        return "[redacted]"
    text = redact(_SECRET_VALUE.sub("[redacted]", text))
    return text if len(text) <= MAX else text[:MAX] + "..."


def flatten(node: Any, prefix: str = "") -> Iterator[tuple[str, Any]]:
    if isinstance(node, dict):
        for k in sorted(node, key=str):
            yield from flatten(node[k], f"{prefix}.{k}" if prefix else str(k))
    elif isinstance(node, list):
        if not node:
            yield prefix, []
        for i, v in enumerate(node):
            yield from flatten(v, f"{prefix}[{i}]")
    else:
        yield prefix, node


def diff(source: Any, target: Any, ignore: tuple[str, ...] = (), limit: int = 25) -> list[str]:
    """Redacted lines describing how two JSON values differ (``-`` only in source, ``+`` only in target, ``~`` changed)."""
    a = {k: v for k, v in flatten(source) if not any(k == i or k.startswith(i + ".") or k.startswith(i + "[") for i in ignore)}
    b = {k: v for k, v in flatten(target) if not any(k == i or k.startswith(i + ".") or k.startswith(i + "[") for i in ignore)}
    out: list[str] = []
    for k in sorted(set(a) | set(b)):
        if k in a and k not in b:
            out.append(f"- {k}: {redact_value(k, a[k])}")
        elif k in b and k not in a:
            out.append(f"+ {k}: {redact_value(k, b[k])}")
        elif a[k] != b[k]:
            out.append(f"~ {k}: {redact_value(k, a[k])} -> {redact_value(k, b[k])}")
    if len(out) > limit:
        out = out[:limit] + [f"... and {len(out) - limit} more difference(s)"]
    return out


def has_plaintext_secret(node: Any) -> list[str]:
    """Paths (never values) where a secret-named key holds a literal value, or a string holds a credential-shaped literal."""
    hits: list[str] = []
    for path, v in flatten(node):
        if not isinstance(v, str) or not v:
            continue
        last = re.split(r"[.\[\]]", path.rstrip("]"))[-1].lower()
        if v.startswith(("@", "<")) or last in _SAFE_KEYS or last in _ENCRYPTED_KEYS:
            continue
        if SECRET_KEY.search(last) and not re.fullmatch(r"[0-9a-fA-F-]{36}", v) and last not in ("tokenaudience",) and not _looks_like_reference(path, v):
            hits.append(path)
        elif re.search(r"(?i)(AccountKey=|SharedAccessSignature=|[;?&]sig=|Password=|bearer\s+\S{12,}|eyJ[\w\-.]{20,})", v):
            hits.append(path)
    return sorted(set(hits))


def _looks_like_reference(path: str, v: str) -> bool:
    """A Key Vault / secret REFERENCE (a name), not a secret."""
    return path.lower().endswith(("secretname", "secret.secretname", "store.referencename")) or v in ("SecureString", "AzureKeyVaultSecret")
