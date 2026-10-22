"""Security settings, all from the environment (names only here; values are never in source control).

| Variable                          | Meaning                                                                                      | Default |
|-----------------------------------|----------------------------------------------------------------------------------------------|---------|
| ADF2FABRIC_AUTH_MODE              | ``disabled`` (one local user, loopback only) or ``token`` (every request authenticated)       | ``token`` when ADF2FABRIC_API_TOKEN is set or users exist, else ``disabled`` |
| ADF2FABRIC_API_TOKEN              | legacy single shared token (maps to one admin principal; prefer named users)                  | unset |
| ADF2FABRIC_ALLOWED_ORIGINS        | browser origins allowed by CORS and accepted on state-changing requests                      | the local Streamlit UI |
| ADF2FABRIC_ALLOWED_HOSTS          | Host header values accepted (defends against DNS rebinding)                                   | localhost, 127.0.0.1, [::1] |
| ADF2FABRIC_ALLOWED_GIT_HOSTS      | if set, the only hosts a Git source may use (comma separated); otherwise any public host      | unset |
| ADF2FABRIC_LEGACY_ORG             | the organisation that owns projects created before ownership was recorded                     | ``default`` |
| ADF2FABRIC_RATE_LIMIT_PER_MIN     | requests per minute per principal                                                             | 300 |
| ADF2FABRIC_SENSITIVE_RATE_PER_MIN | requests per minute per principal for sign-in, import, upload and apply operations            | 20 |
| ADF2FABRIC_MAX_JSON_BYTES         | largest JSON request body                                                                     | 1 MiB |
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1", "[::1]")


def _list(name: str, default: str = "") -> list[str]:
    return [x.strip() for x in (os.environ.get(name) or default).split(",") if x.strip()]


def _int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, "")))
    except ValueError:
        return default


@dataclass
class SecuritySettings:
    root: Optional[Path] = None  # the projects root: where _auth/ and _audit/ live
    mode: Optional[str] = None  # None = decide from the environment (see above)
    legacy_token_env: str = "ADF2FABRIC_API_TOKEN"
    allowed_hosts: list[str] = field(default_factory=list)
    allowed_origins: list[str] = field(default_factory=list)
    allowed_git_hosts: list[str] = field(default_factory=list)
    legacy_org: str = ""
    rate_per_min: int = 0
    sensitive_per_min: int = 0
    max_json_bytes: int = 0
    auth_fail_limit: int = 8  # failed sign-ins per client per window before it is locked out
    auth_fail_window: int = 300  # seconds

    def __post_init__(self) -> None:
        self.allowed_hosts = self.allowed_hosts or _list("ADF2FABRIC_ALLOWED_HOSTS", ",".join(LOOPBACK_HOSTS))
        self.allowed_origins = self.allowed_origins or _list("ADF2FABRIC_ALLOWED_ORIGINS", "http://localhost:8501,http://127.0.0.1:8501")
        self.allowed_git_hosts = self.allowed_git_hosts or _list("ADF2FABRIC_ALLOWED_GIT_HOSTS")
        self.legacy_org = self.legacy_org or os.environ.get("ADF2FABRIC_LEGACY_ORG") or "default"
        self.rate_per_min = self.rate_per_min or _int("ADF2FABRIC_RATE_LIMIT_PER_MIN", 300)
        self.sensitive_per_min = self.sensitive_per_min or _int("ADF2FABRIC_SENSITIVE_RATE_PER_MIN", 20)
        self.max_json_bytes = self.max_json_bytes or _int("ADF2FABRIC_MAX_JSON_BYTES", 1024 * 1024)

    @property
    def legacy_token(self) -> Optional[str]:
        return os.environ.get(self.legacy_token_env) or None

    def effective_mode(self, has_users: bool) -> str:
        explicit = (self.mode or os.environ.get("ADF2FABRIC_AUTH_MODE") or "").strip().lower()
        if explicit in ("disabled", "token"):
            return explicit
        return "token" if (self.legacy_token or has_users) else "disabled"
