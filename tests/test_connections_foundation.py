"""Connection foundations: redaction, the secret vault, Key Vault reads, both CLI providers and the Fabric service.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import json

import pytest

from adf2fabric.connections import keyvault as kv
from adf2fabric.connections.providers._login import FAILED, LoginManager
from adf2fabric.connections.redaction import MASK, redact, redact_mapping
from adf2fabric.connections.runner import CliResult, run_cli

JWT = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"


def ok(stdout="", **kw):
    return CliResult(found=True, returncode=0, stdout=stdout, **kw)


def bad(stderr="", code=1):
    return CliResult(found=True, returncode=code, stderr=stderr)


# --- redaction ------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("text, leaked", [
    (f"Authorization: Bearer {JWT}", JWT),
    (f"token {JWT} expired", JWT),
    ("Server=x;Database=y;Password=S3cr3t!Pass;Encrypt=yes", "S3cr3t!Pass"),
    ("DefaultEndpointsProtocol=https;AccountName=a;AccountKey=abc123XYZ+/=;EndpointSuffix=x", "abc123XYZ+/="),
    ("https://a.blob.core.windows.net/c?sv=2022-11-02&sig=Zm9vYmFyU0lH%2Fxx&se=2030", "Zm9vYmFyU0lH%2Fxx"),
    ('{"password": "hunter2hunter2", "user": "bob"}', "hunter2hunter2"),
    ("git clone https://user:ghp_abcdef123456@github.com/x/y.git", "ghp_abcdef123456"),
    ("client_secret=abc~DEF.ghi123", "abc~DEF.ghi123"),
])
def test_redaction_masks_secret_shaped_text_but_keeps_context(text, leaked):
    out = redact(text)
    assert leaked not in out and MASK in out


def test_redaction_masks_exact_secrets_and_keeps_ordinary_text():
    assert redact("login failed for hunter22 on server s1", "hunter22") == f"login failed for {MASK} on server s1"
    assert redact("Subscription 'Azure subscription 1' not found") == "Subscription 'Azure subscription 1' not found"
    assert redact(None) == "" and redact("ab", "ab") == "ab"  # too short to be treated as a secret


def test_redact_mapping_masks_secret_keys_and_nested_strings():
    out = redact_mapping({"password": "p", "nested": {"accountKey": "k", "url": f"x Bearer {JWT}"}, "n": 1})
    assert out["password"] == MASK and out["nested"]["accountKey"] == MASK and JWT not in json.dumps(out) and out["n"] == 1


# --- runner ---------------------------------------------------------------------------------------------------


def test_runner_reports_a_missing_executable_as_a_normal_result():
    r = run_cli(["definitely-not-a-real-program-xyz", "--version"])
    assert not r.found and not r.ok and "not found" in r.stderr


# --- secret vault ---------------------------------------------------------------------------------------------


# --- key vault ------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("url", ["https://evil.example.com", "http://v.vault.azure.net", "ftp://v.vault.azure.net", "",
                                 "https://vault.azure.net.evil.com", "https://169.254.169.254"])
def test_only_azure_key_vault_endpoints_are_trusted(url):
    with pytest.raises(kv.KeyVaultError):
        kv.normalize_vault_url(url)


def test_vault_url_forms_normalise():
    assert kv.normalize_vault_url("myvault.vault.azure.net") == "https://myvault.vault.azure.net"
    assert kv.normalize_vault_url("https://MyVault.vault.azure.net/secrets/x") == "https://myvault.vault.azure.net"
    assert kv.normalize_vault_url("https://v.vault.azure.cn") == "https://v.vault.azure.cn"


def test_bad_secret_names_are_rejected():
    for bad_name in ("", "a/b", "x" * 200, "a b"):
        with pytest.raises(kv.KeyVaultError):
            kv.validate_secret_name(bad_name)


# --- login manager --------------------------------------------------------------------------------------------


def test_login_that_cannot_start_is_a_failed_attempt_not_a_crash():
    def popen(*a, **k):
        raise OSError("nope")

    a = LoginManager(popen=popen).start(["az", "login"])
    assert a.state == FAILED and "Could not start" in a.lines[0]


# --- Azure CLI provider ---------------------------------------------------------------------------------------


# --- Fabric CLI provider --------------------------------------------------------------------------------------


# --- Fabric service (both transports) -------------------------------------------------------------------------


def ws(i, name, **kw):
    return {"id": f"id{i}", "displayName": name, "type": "Workspace", "capacityId": "cap", **kw}


