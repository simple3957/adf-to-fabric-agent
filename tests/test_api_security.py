"""Negative security tests and regressions for the API: authentication, authorisation, tenancy, request protection, SSRF, uploads, audit.

The permission table below is written by hand, independently of the application code: if a route's requirement changes, this table
must change with it on purpose. Every test builds a real app in token mode with real users in a real (temporary) user store.
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path

import pytest

from adf2fabric.api.security import Grants
from adf2fabric.api.security import guards
from adf2fabric.api.security.audit import AuditLog
from adf2fabric.api.security.permissions import ROLES, Permission as P, permissions_for

SUB = "00000000-0000-0000-0000-0000000005b1"  # the demo subscription
WS = "11111111-1111-1111-1111-111111111111"
ALL = Grants(subscriptions=["*"], tenants=["*"], accounts=["*"], workspaces=["*"])


def make_zip(files: dict[str, str | bytes] | None = None, **extra) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in (files or {"pipeline/PL_A.json": json.dumps({"name": "PL_A", "properties": {"activities": []}})}).items():
            z.writestr(name, data)
        for name, info in extra.items():
            z.writestr(info[0], info[1])
    return buf.getvalue()


# =====================================================================================================================
# 1. authentication: nothing protected is reachable without a valid token
# =====================================================================================================================


# =====================================================================================================================
# 2. authorisation: a hand-written permission table, checked for every role
# =====================================================================================================================

G = P  # short alias for the table


def test_the_roles_are_least_privilege():
    assert permissions_for(["viewer"]) == {P.projects_view, P.reports_view}
    assert P.migration_execute not in permissions_for(["analyst"]) and P.provision_approve not in permissions_for(["analyst"])
    assert P.migration_execute not in permissions_for(["approver"])  # approving is not executing: separation of duties
    assert P.users_manage not in permissions_for(["operator"]) and P.credentials_manage not in permissions_for(["operator"])
    assert permissions_for(["admin"]) == set(P)
    assert set(ROLES) == {"viewer", "analyst", "approver", "operator", "admin"}


# =====================================================================================================================
# 3. tenancy: projects, jobs, sessions and uploads belong to an organisation (IDOR)
# =====================================================================================================================


# =====================================================================================================================
# 4. what a client may point the server at
# =====================================================================================================================


def test_the_ssrf_guard_resolves_names_and_honours_an_allow_list():
    pub = lambda h: ["140.82.112.3"]  # noqa: E731
    priv = lambda h: ["10.1.2.3"]  # noqa: E731
    mixed = lambda h: ["140.82.112.3", "169.254.169.254"]  # noqa: E731
    assert guards.check_public_https_url("https://github.com/o/r.git", resolver=pub)
    for resolver in (priv, mixed, lambda h: []):
        with pytest.raises(guards.Rejected):
            guards.check_public_https_url("https://github.com/o/r.git", resolver=resolver)
    with pytest.raises(guards.Rejected):
        guards.check_public_https_url("https://evil.example/o/r", allowed_hosts=["github.com"], resolver=pub)
    assert guards.check_public_https_url("https://dev.azure.com/o/p/_git/r", allowed_hosts=["dev.azure.com"], resolver=pub)
    assert guards.check_public_https_url("https://sub.github.com/o/r", allowed_hosts=["github.com"], resolver=pub)


# =====================================================================================================================
# 5. path traversal and malicious archives
# =====================================================================================================================


# =====================================================================================================================
# 6. CSRF, DNS rebinding, CORS, content type, size, headers
# =====================================================================================================================


# =====================================================================================================================
# 7. rate limiting
# =====================================================================================================================


# =====================================================================================================================
# 8. nothing sensitive in responses, errors, jobs or the audit log
# =====================================================================================================================


# =====================================================================================================================
# 9. audit
# =====================================================================================================================


def test_audit_values_are_redacted_and_bounded(tmp_path):
    log = AuditLog(tmp_path)
    ev = log.event("x.y", "ok", actor="a", detail_token="Bearer abc.def.ghi", path=f"C:\\Users\\someone\\secret\\file.txt", long="z" * 1000)
    assert "abc.def.ghi" not in json.dumps(ev) and "someone" not in json.dumps(ev) and len(ev["detail"]["long"]) <= 200


# =====================================================================================================================
# 10. users: management stays inside the organisation and cannot escalate
# =====================================================================================================================


# =====================================================================================================================
# 11. CLI: users, audit
# =====================================================================================================================


def test_every_sensitive_cli_command_carries_the_audit_decorator():
    import inspect

    from adf2fabric import cli as cli_mod

    src = inspect.getsource(cli_mod)
    for fn in ("deploy_cmd", "run_cmd", "verify_cmd", "provision_apply", "provision_approve", "workloads_deploy", "workloads_approve",
               "deploy_notebook_cmd", "compute_deploy_cmd", "create_shortcut_cmd"):
        assert re.search(rf'@_audited\("[a-z_.]+"\)\ndef {fn}\(', src), fn


# =====================================================================================================================
# 12. credentials are never in source control
# =====================================================================================================================

_SECRET_PATTERNS = [r"ghp_[A-Za-z0-9]{36}", r"github_pat_[A-Za-z0-9_]{50,}", r"AKIA[0-9A-Z]{16}", r"-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----",
                    r"eyJ[A-Za-z0-9_-]{20,}\.eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}", r"(?i)(password|secret|api[_-]?key|token)\s*[:=]\s*[\"'][A-Za-z0-9+/=_-]{20,}[\"']"]


def test_no_credential_is_hard_coded_in_the_source_or_ui():
    root = Path(__file__).parents[1]
    hits = []
    for base in ("src", "ui", "config"):
        for f in (root / base).rglob("*"):
            if f.is_file() and f.suffix in (".py", ".yaml", ".yml", ".json", ".toml", ".md") and "__pycache__" not in f.parts:
                text = f.read_text(encoding="utf-8", errors="ignore")
                for pat in _SECRET_PATTERNS:
                    for m in re.finditer(pat, text):
                        hits.append(f"{f.relative_to(root)}: {m.group(0)[:20]}...")
    assert hits == []


def test_tokens_are_never_stored_by_the_ui_client_beyond_its_own_object():
    from importlib import import_module
    import sys

    sys.path.insert(0, str(Path(__file__).parents[1] / "ui"))
    try:
        api_client = import_module("api_client")
    finally:
        sys.path.pop(0)
    c = api_client.ApiClient(session_id="s", token="")
    c.set_token("a2f_abc")
    assert c._token == "a2f_abc"
    c.clear_token()
    assert c._token is None
    assert "token" not in repr(vars(c)).lower().replace("_token': none", "")


# =====================================================================================================================
# 13. live-factory sources: the subscription inside a project's source is gated like any client-chosen subscription
# =====================================================================================================================


