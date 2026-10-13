"""Redaction for anything that could reach a log, a diagnostic, an exception message or a report.

Two layers: EXACT secrets the caller knows (passwords, keys it just used), and PATTERNS for secret-shaped text that
a service or CLI may echo back (bearer tokens, JWTs, SAS signatures, connection-string keys, ``password=`` pairs).
"""

from __future__ import annotations

import re

MASK = "***"

_JWT = re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*")
_BEARER = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
# key=value / key: value pairs whose KEY names a secret. The value is masked, the key is kept so the text stays useful.
_KEYED = re.compile(
    r"(?i)(\"?(?:password|pwd|passwd|accountkey|account_key|sharedaccesskey|sharedaccesssignature|secret|"
    r"client_secret|clientsecret|servicePrincipalSecret|access_?token|refresh_?token|token|sas|sig|apikey|api_key|"
    r"key|credential)s?\"?\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s;,&\"'}]+)")
_URL_USERINFO = re.compile(r"(https?://)[^/\s:@]+:[^/\s@]+@")


def redact(text: object, *secrets: str) -> str:
    """``text`` with every known secret and every secret-shaped fragment masked."""
    out = "" if text is None else str(text)
    for s in secrets:
        if s and len(s) >= 4:
            out = out.replace(s, MASK)
    out = _URL_USERINFO.sub(r"\1" + MASK + "@", out)
    out = _JWT.sub(MASK, out)
    out = _BEARER.sub(lambda m: f"{m.group(1)} {MASK}", out)

    def keyed(m: re.Match) -> str:
        quote = '"' if m.group(2).startswith('"') else "'" if m.group(2).startswith("'") else ""
        return f"{m.group(1)}{quote}{MASK}{quote}"

    return _KEYED.sub(keyed, out)


def redact_mapping(data: object, *secrets: str) -> object:
    """A deep copy of ``data`` with every string redacted and secret-named keys masked."""
    if isinstance(data, dict):
        return {k: (MASK if _is_secret_key(str(k)) and isinstance(v, str) else redact_mapping(v, *secrets))
                for k, v in data.items()}
    if isinstance(data, list):
        return [redact_mapping(v, *secrets) for v in data]
    if isinstance(data, str):
        return redact(data, *secrets)
    return data


def _is_secret_key(key: str) -> bool:
    k = key.lower().replace("-", "").replace("_", "")
    return any(h in k for h in ("password", "secret", "token", "accountkey", "sastoken", "apikey", "privatekey",
                                "connectionstring"))
