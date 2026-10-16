"""Connection foundations: redaction, the secret vault, Key Vault reads, both CLI providers and the Fabric service.

External CLIs and services are faked; the CLI output formats below are copied from the real installed tools
(az 2.90, fab 1.7.0), so the parsers are exercised on realistic text.
"""

from __future__ import annotations

import json
from pathlib import Path
import time

import pytest

from adf2fabric.agent.intake.azure_source import AdfRequestError
from adf2fabric.connections import keyvault as kv
from adf2fabric.connections.fabric import (
    ApiResponse,
    FabCliApi,
    FabricApiError,
    FabricService,
    RestFabricApi,
    unwrap_fab,
)
from adf2fabric.connections.models import CheckStatus, ConnectionState
from adf2fabric.connections.providers._login import FAILED, RUNNING, SUCCEEDED, TIMED_OUT, LoginManager
from adf2fabric.connections.providers.azure_cli import AzureCliProvider, ProviderError
from adf2fabric.connections.providers.fabric_cli import LOGIN_METHODS, FabricCliProvider
from adf2fabric.connections.redaction import MASK, redact, redact_mapping
from adf2fabric.connections.runner import CliResult, run_cli
from adf2fabric.connections.secret_vault import SecretVault

JWT = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"


class FakeRunner:
    """Maps the start of the argument list (joined) to a CliResult; records every call."""

    def __init__(self, table):
        self.table, self.calls = table, []

    def __call__(self, args, **kw):
        self.calls.append((list(args), kw))
        key = " ".join(args)
        for prefix, result in self.table.items():
            if key.startswith(prefix):
                return result(args, kw) if callable(result) else result
        return CliResult(found=True, returncode=1, stderr=f"unscripted: {key}")


def ok(stdout="", **kw):
    return CliResult(found=True, returncode=0, stdout=stdout, **kw)


def bad(stderr="", code=1):
    return CliResult(found=True, returncode=code, stderr=stderr)


MISSING = CliResult(found=False, returncode=127, stderr="'az' was not found on PATH")


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


def test_runner_redacts_output_and_only_raw_mode_keeps_a_token(monkeypatch):
    import subprocess

    class P:
        returncode, stdout, stderr = 0, f"{JWT}\n".encode(), b"password=abc12345"

    monkeypatch.setattr(subprocess, "run", lambda *a, **k: P())
    monkeypatch.setattr("adf2fabric.connections.runner.shutil.which", lambda n: "/bin/" + n)
    r = run_cli(["az", "x"])
    assert JWT not in r.stdout and "abc12345" not in r.stderr
    assert JWT in run_cli(["az", "x"], raw=True).stdout and "abc12345" not in run_cli(["az", "x"], raw=True).stderr


# --- secret vault ---------------------------------------------------------------------------------------------


def test_vault_holds_expires_wipes_and_never_reveals_itself():
    now = [100.0]
    v = SecretVault(ttl_seconds=10, clock=lambda: now[0])
    v.put("s", "ls", "LS_SQL", "password", "hunter2")
    assert v.get("s", "ls", "LS_SQL", "password") == "hunter2" and v.fields("s", "ls", "LS_SQL") == ["password"]
    assert "hunter2" not in repr(v) and "hunter2" not in str(v) and len(v) == 1
    with pytest.raises(TypeError):
        import copy
        copy.deepcopy(v)
    now[0] = 111.0
    assert v.get("s", "ls", "LS_SQL", "password") is None and len(v) == 0  # expired
    v.put("s", "ls", "A", "key", "k1")
    v.put("s", "ls", "B", "key", "k2")
    v.put("other", "ls", "A", "key", "k3")
    assert v.wipe("s", owner="A") == 1 and v.has("s", "ls", "B", "key")
    assert v.wipe("s") == 1 and v.has("other", "ls", "A", "key")


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


class Secret:
    value, properties = "s3cr3t-value", type("P", (), {"enabled": True, "content_type": "text/plain"})()


class Boom(Exception):
    def __init__(self, status=None, msg=""):
        super().__init__(msg)
        self.status_code = status


def test_read_secret_returns_value_and_safe_metadata():
    seen = {}

    def factory(url, cred):
        seen["url"] = url
        return type("C", (), {"get_secret": lambda self, n: Secret()})()

    value, info = kv.read_secret("v.vault.azure.net", "sql-pw", object(), factory)
    assert value == "s3cr3t-value" and info.name == "sql-pw" and info.enabled is True and seen["url"] == "https://v.vault.azure.net"
    assert "s3cr3t" not in repr(info)


@pytest.mark.parametrize("exc, words", [
    (Boom(403, "Forbidden Bearer " + JWT), "not allowed"), (Boom(404), "not found"), (Boom(401), "rejected"),
    (type("ClientAuthenticationError", (Exception,), {})("AADSTS700082 expired " + JWT), "rejected"),
    (type("ServiceRequestError", (Exception,), {})("x"), "reached")])
def test_key_vault_errors_are_actionable_and_never_echo_tokens(exc, words):
    def factory(url, cred):
        def get_secret(self, n):
            raise exc
        return type("C", (), {"get_secret": get_secret})()

    with pytest.raises(kv.KeyVaultError) as e:
        kv.read_secret("v.vault.azure.net", "x", object(), factory)
    assert words in str(e.value) and JWT not in str(e.value) and e.value.__cause__ is None


def test_bad_secret_names_are_rejected():
    for bad_name in ("", "a/b", "x" * 200, "a b"):
        with pytest.raises(kv.KeyVaultError):
            kv.validate_secret_name(bad_name)


# --- login manager --------------------------------------------------------------------------------------------


class FakeProc:
    def __init__(self, lines, rc=0, hang=False):
        self.stdout, self._rc, self._hang, self.killed = iter(lines), rc, hang, False

    def wait(self):
        while self._hang and not self.killed:
            time.sleep(0.01)
        return self._rc

    def kill(self):
        self.killed = True


def wait_for(attempt, state, secs=3):
    end = time.time() + secs
    while attempt.state == state and time.time() < end:
        time.sleep(0.01)


def test_login_collects_redacted_output_and_reports_the_outcome():
    mgr = LoginManager(popen=lambda *a, **k: FakeProc(["To sign in, open https://microsoft.com/devicelogin and enter ABC123\n",
                                                        f"token {JWT}\n"]))
    a = mgr.start(["az", "login", "--tenant", "t"])
    wait_for(a, RUNNING)
    assert a.state == SUCCEEDED and a.command == "az" and "t" not in a.public()["command"]
    assert any("devicelogin" in line for line in a.lines) and JWT not in json.dumps(a.public())


def test_login_failure_single_flight_and_timeout():
    mgr = LoginManager(popen=lambda *a, **k: FakeProc(["boom\n"], rc=1))
    a = mgr.start(["az", "login"])
    wait_for(a, RUNNING)
    assert a.state == FAILED
    procs = []
    hang = LoginManager(popen=lambda *a, **k: procs.append(FakeProc([], hang=True)) or procs[-1], timeout=0.2)
    b = hang.start(["az", "login"])
    c = hang.start(["az", "login"])  # one sign-in at a time: the newest replaces an unfinished one
    assert b.state == FAILED and "Replaced by a newer sign-in." in b.lines and procs[0].killed
    wait_for(c, RUNNING, 5)
    assert c.state == TIMED_OUT and procs[1].killed


def test_login_that_cannot_start_is_a_failed_attempt_not_a_crash():
    def popen(*a, **k):
        raise OSError("nope")

    a = LoginManager(popen=popen).start(["az", "login"])
    assert a.state == FAILED and "Could not start" in a.lines[0]


# --- Azure CLI provider ---------------------------------------------------------------------------------------

ACCOUNT = json.dumps({"id": "sub-1", "tenantId": "ten-1", "name": "Sub One", "user": {"name": "me@pal.tech", "type": "user"}})
SUBS = json.dumps([{"id": "s1", "name": "Azure subscription 1", "tenantId": "t1", "state": "Enabled", "isDefault": True,
                    "user": {"name": "a@b.c"}}, {"id": "s2", "name": "Other", "tenantId": "t2", "state": "Disabled"}])


def az(table):
    return AzureCliProvider(runner=FakeRunner(table))


def test_azure_cli_not_installed():
    st = az({"az account show": MISSING}).status()
    assert st.state is ConnectionState.not_configured and not st.available and "Install the Azure CLI" in st.actions[0]


def test_azure_cli_signed_out_expired_and_ok_are_distinct_states():
    out = az({"az account show": bad("ERROR: Please run 'az login' to setup account.")}).status()
    assert out.state is ConnectionState.authentication_required and out.available
    expired = az({"az account show": ok(ACCOUNT),
                  "az account get-access-token": bad("AADSTS700082: The refresh token has expired due to inactivity.")}).status()
    assert expired.state is ConnectionState.authentication_required and "expired" in expired.error
    assert [c.status for c in expired.checks] == [CheckStatus.passed, CheckStatus.passed, CheckStatus.failed]
    good = az({"az account show": ok(ACCOUNT), "az account get-access-token": ok("2026-10-13 12:00:00"),
               "az version": ok('{"azure-cli": "2.90.0"}')}).status()
    assert good.authenticated and good.state is ConnectionState.authenticated and good.account == "me@pal.tech"
    assert good.version == "2.90.0" and "tenantId" not in good.model_dump_json()


def test_azure_status_asks_only_for_the_token_expiry_never_the_token():
    r = FakeRunner({"az account show": ok(ACCOUNT), "az account get-access-token": ok("2026-10-13"), "az version": ok("{}")})
    AzureCliProvider(runner=r).status()
    tok = next(c for c, _ in r.calls if c[:3] == ["az", "account", "get-access-token"])
    assert "expiresOn" in tok and "accessToken" not in tok


def test_subscriptions_and_login_arguments():
    p = az({"az account list": ok(SUBS), "az --version": ok("azure-cli 2.90"), "az account tenant list": ok(
        json.dumps([{"tenantId": "t1", "displayName": "Default"}]))})
    subs = p.subscriptions()
    assert [(s.id, s.is_default, s.state) for s in subs] == [("s1", True, "Enabled"), ("s2", False, "Disabled")]
    assert p.tenants() == [{"id": "t1", "name": "Default"}]
    with pytest.raises(ProviderError):
        az({"az account list": bad("Please run 'az login'")}).subscriptions()
    captured = {}
    p.logins = LoginManager(popen=lambda argv, **k: captured.setdefault("argv", argv) and FakeProc([]))
    p.start_login(tenant="t1", device_code=True)
    # The program is resolved on PATH (on Windows 'az' is az.cmd), so compare its name, then the arguments.
    assert Path(captured["argv"][0]).stem.lower() == "az"
    assert captured["argv"][1:] == ["login", "--allow-no-subscriptions", "--tenant", "t1", "--use-device-code"]
    with pytest.raises(ProviderError):
        az({"az --version": MISSING}).start_login()


def test_logout_signs_out_one_account_and_reports_the_new_state():
    r = FakeRunner({"az logout": ok(""), "az version": ok('{"azure-cli": "2.90"}'), "az account show": bad("Please run 'az login'")})
    st = AzureCliProvider(runner=r).logout("someone@contoso.example")
    assert r.calls[0][0] == ["az", "logout", "--username", "someone@contoso.example"]
    assert st.authenticated is False  # no account left: the UI asks the user to sign in again
    with pytest.raises(ProviderError):
        az({"az logout": bad("boom")}).logout()
    with pytest.raises(ProviderError):
        az({"az logout": MISSING}).logout()


def test_token_is_fetched_raw_for_local_use_and_failures_are_actionable():
    r = FakeRunner({"az account get-access-token": ok(JWT + "\n")})
    assert AzureCliProvider(runner=r).token(subscription="s1") == JWT
    assert r.calls[0][1].get("raw") is True and "--subscription" in r.calls[0][0]
    with pytest.raises(ProviderError) as e:
        az({"az account get-access-token": bad("AADSTS700082 expired")}).token()
    assert "expired" in str(e.value) and e.value.actions


class FakeFetcher:
    def __init__(self, pages=None, fail=None):
        self.pages, self.fail, self.urls = pages or [], fail, []

    def get(self, url):
        self.urls.append(url)
        if self.fail:
            raise self.fail
        return {"id": "x", "properties": {"connectivityEndpoints": {"dev": "https://w.dev.azuresynapse.net"}}}

    def list_all(self, url):
        self.urls.append(url)
        if self.fail:
            raise self.fail
        return self.pages


def provider(fetcher):
    return AzureCliProvider(runner=FakeRunner({"az account get-access-token": ok("tok")}), fetcher_factory=lambda t: fetcher)


def test_resource_discovery_filters_by_type_and_extracts_resource_groups():
    f = FakeFetcher([{"id": "/subscriptions/s/resourceGroups/RG-B/providers/Microsoft.DataFactory/factories/zeta",
                      "name": "zeta", "type": "Microsoft.DataFactory/factories", "location": "centralindia"},
                     {"id": "/subscriptions/s/resourceGroups/rg-a/providers/Microsoft.DataFactory/factories/alpha",
                      "name": "alpha", "type": "Microsoft.DataFactory/factories"}])
    res = provider(f).list_resources("s", "factories")
    assert [(r.name, r.resource_group) for r in res] == [("alpha", "rg-a"), ("zeta", "RG-B")]
    assert "resourceType%20eq%20%27Microsoft.DataFactory/factories%27" in f.urls[0] and f.urls[0].startswith(
        "https://management.azure.com/subscriptions/s/resources?")


@pytest.mark.parametrize("status, words", [(403, "Reader"), (404, "not found"), (401, "rejected"), (429, "throttled")])
def test_discovery_failures_are_actionable(status, words):
    p = provider(FakeFetcher(fail=AdfRequestError(status, "https://management.azure.com/x")))
    with pytest.raises(ProviderError) as e:
        p.list_resources("s", "storage_accounts")
    assert words in (str(e.value) + " ".join(e.value.actions)) or words in str(e.value).lower()


def test_discover_reports_a_failing_kind_without_losing_the_others():
    class Mixed(FakeFetcher):
        def list_all(self, url):
            if "KeyVault" in url:
                raise AdfRequestError(403, url)
            return []

    found = provider(Mixed()).discover("s")
    assert found["factories"] == [] and any(r.name for r in found["_errors"])


def test_validate_factory_reports_exactly_what_was_proved_and_actionable_failures():
    f = FakeFetcher([{"name": "p1"}, {"name": "p2"}])
    rep = provider(f).validate_factory("s", "rg", "adf1")
    assert rep.state is ConnectionState.connection_tested and "2 pipeline(s) can be listed" in rep.verified
    assert rep.not_verified and "linked-service credentials" in rep.not_verified[0]
    bad_rep = provider(FakeFetcher(fail=AdfRequestError(403, "https://management.azure.com/x"))).validate_factory("s", "rg", "adf1")
    assert bad_rep.state is ConnectionState.validation_failed and "Reader" in bad_rep.actions[0]
    signed_out = AzureCliProvider(runner=FakeRunner({"az account get-access-token": bad("Please run 'az login'")}))
    assert signed_out.validate_factory("s", "rg", "f").state is ConnectionState.authentication_required


def test_validate_synapse_workspace():
    rep = provider(FakeFetcher()).validate_synapse_workspace("s", "rg", "w")
    assert rep.state is ConnectionState.connection_tested and any("development endpoint" in v for v in rep.verified)
    assert any("Git export or ZIP" in n for n in rep.not_verified)


# --- Fabric CLI provider --------------------------------------------------------------------------------------

FAB_HELP = """Work seamlessly with Fabric from the command line.

Core Commands:
  assign     : Assign a capacity or resource to a workspace.
  dir        : List workspaces, items, and files.

Util Commands:
  api        : Make authenticated API requests.
  auth       : Authenticate with Fabric.
"""
FAB_OUT = "✗ Not logged in to app.fabric.microsoft.com\n" + json.dumps({
    "timestamp": "2026-10-13T05:56:56Z", "status": "Success", "command": "auth", "result": {"data": [{
        "logged_in": False, "account": "N/A", "principal_id": "N/A", "tenant_id": "N/A", "app_id": "N/A",
        "token_fabric_powerbi": "N/A", "token_storage": "N/A", "token_azure": "N/A"}]}})
FAB_IN = FAB_OUT.replace('"logged_in": false', '"logged_in": true').replace('"account": "N/A"', '"account": "me@pal.tech"').replace(
    '"tenant_id": "N/A"', '"tenant_id": "ten-1"').replace('"token_azure": "N/A"', '"token_azure": "SECRETTOKENVALUE123"')


def fab(table):
    return FabricCliProvider(runner=FakeRunner(table))


def test_fabric_cli_states():
    assert fab({"fab --version": MISSING}).status().state is ConnectionState.not_configured
    no_cmds = fab({"fab --version": ok("fab version 1.7.0"), "fab --help": ok("Core Commands:\n  dir : List.\n")}).status()
    assert no_cmds.state is ConnectionState.manual_action_required and "auth" in no_cmds.error
    base = {"fab --version": ok("fab version 1.7.0\nhttps://aka.ms/x"), "fab --help": ok(FAB_HELP)}
    out = fab({**base, "fab auth status": ok(FAB_OUT)}).status()
    assert out.state is ConnectionState.authentication_required and out.available and out.version == "1.7.0"
    ins = fab({**base, "fab auth status": ok(FAB_IN)}).status()
    assert ins.authenticated and ins.account == "me@pal.tech" and ins.tenant_id == "ten-1"
    assert "SECRETTOKENVALUE123" not in ins.model_dump_json()  # token_* fields are never read
    junk = fab({**base, "fab auth status": ok("garbage")}).status()
    assert junk.state is ConnectionState.validation_failed


def test_fabric_cli_capabilities_come_from_its_own_help():
    p = fab({"fab --help": ok(FAB_HELP)})
    assert p.capabilities() == {"auth": True, "api": True}
    assert fab({"fab --help": ok("  dir : x\n")}).capabilities() == {"auth": False, "api": False}


def test_fabric_cli_sign_in_methods_and_the_one_it_refuses():
    assert set(LOGIN_METHODS) == {"interactive", "managed_identity", "service_principal_certificate"}
    p = fab({"fab --version": ok("fab version 1.7.0")})
    captured = []
    p.logins = LoginManager(popen=lambda argv, **k: captured.append(argv) or FakeProc([]))
    guid, guid2 = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
    wait_for(p.start_login("interactive"), RUNNING)
    wait_for(p.start_login("managed_identity", client_id=guid), RUNNING)
    wait_for(p.start_login("service_principal_certificate", tenant=guid, client_id=guid2, certificate_path="/c/app.pem"), RUNNING)
    assert captured[0] == ["fab", "auth", "login"]
    assert captured[1] == ["fab", "auth", "login", "--identity", "-u", guid]
    assert captured[2] == ["fab", "auth", "login", "-u", guid2, "--tenant", guid, "--certificate", "/c/app.pem"]
    with pytest.raises(ValueError, match="command-line argument"):
        p.start_login("service_principal_secret")
    with pytest.raises(ValueError):
        p.start_login("service_principal_certificate", tenant=guid)
    with pytest.raises(ValueError, match="GUID"):
        p.start_login("interactive", tenant="not-a-guid")
    with pytest.raises(ValueError, match="Unknown"):
        p.start_login("password")


# --- Fabric service (both transports) -------------------------------------------------------------------------


class FakeApi:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        for key, resp in self.routes.items():
            if f"{method} {path}".startswith(key):
                return resp.pop(0) if isinstance(resp, list) and len(resp) > 1 else (resp[0] if isinstance(resp, list) else resp)
        return ApiResponse(404, {"errorCode": "NotFound", "message": "no route"})


def ws(i, name, **kw):
    return {"id": f"id{i}", "displayName": name, "type": "Workspace", "capacityId": "cap", **kw}


def test_service_pages_filters_personal_and_sorts():
    api = FakeApi({"GET workspaces": [ApiResponse(200, {"value": [ws(1, "Zeta"), {"id": "p", "displayName": "My workspace",
                                                                                   "type": "Personal"}], "continuationToken": "n"}),
                                      ApiResponse(200, {"value": [ws(2, "alpha")]})]})
    out = FabricService(api).workspaces()
    assert [w.display_name for w in out] == ["alpha", "Zeta"] and "continuationToken=n" in api.calls[1][1]
    assert len(FabricService(FakeApi({"GET workspaces": ApiResponse(200, {"value": [{"id": "p", "displayName": "My workspace",
                                      "type": "Personal"}]})})).workspaces(include_personal=True)) == 1


def test_supported_types_and_connections():
    api = FakeApi({"GET connections/supportedConnectionTypes": ApiResponse(200, {"value": [
        {"type": "SQL", "supportedCredentialTypes": ["Basic", "WorkspaceIdentity"]}]}),
        "GET connections": ApiResponse(200, {"value": [{"id": "c1", "displayName": "X"}]})})
    s = FabricService(api)
    assert s.supported_connection_types() == {"SQL": ["Basic", "WorkspaceIdentity"]}
    assert s.connections() == [{"id": "c1", "displayName": "X"}]


@pytest.mark.parametrize("status, words", [(401, "rejected"), (403, "Permission denied"), (404, "Not found"), (500, "HTTP 500")])
def test_service_errors_are_actionable_and_redacted(status, words):
    api = FakeApi({"GET workspaces": ApiResponse(status, {"errorCode": "X", "message": f"bad Bearer {JWT}"})})
    with pytest.raises(FabricApiError) as e:
        FabricService(api).workspaces()
    assert words in str(e.value) and JWT not in str(e.value) and e.value.status == status


def test_validate_workspace_says_exactly_what_it_proved():
    good = FabricService(FakeApi({"GET workspaces/w/dataPipelines": ApiResponse(200, {"value": [{}, {}]}),
                                  "GET workspaces/w": ApiResponse(200, ws(1, "Dest"))})).validate_workspace("w")
    assert good.state is ConnectionState.connection_tested and good.ok
    assert "its data pipelines can be listed (2 present)" in good.verified and "CREATE items" in good.not_verified[0]
    personal = FabricService(FakeApi({"GET workspaces/w": ApiResponse(200, {"displayName": "My", "type": "Personal"})})
                             ).validate_workspace("w")
    assert personal.state is ConnectionState.validation_failed and "personal" in personal.error
    nocap = FabricService(FakeApi({"GET workspaces/w": ApiResponse(200, {"displayName": "D", "type": "Workspace"})})
                          ).validate_workspace("w")
    assert "capacity" in nocap.error and not nocap.ok
    denied = FabricService(FakeApi({"GET workspaces/w/dataPipelines": ApiResponse(403, {"errorCode": "Insufficient"}),
                                    "GET workspaces/w": ApiResponse(200, ws(1, "D"))})).validate_workspace("w")
    assert denied.state is ConnectionState.validation_failed and "Contributor" in denied.actions[0]
    gone = FabricService(FakeApi({})).validate_workspace("w")
    assert gone.state is ConnectionState.validation_failed and gone.checks[0].status is CheckStatus.failed


def test_create_connection_and_identity_provisioning():
    api = FakeApi({"POST connections": ApiResponse(201, {"id": "new"}), "POST workspaces/w/provisionIdentity": ApiResponse(
        202, None)})
    s = FabricService(api)
    assert s.create_connection({"displayName": "x"})["id"] == "new" and api.calls[0][2] == {"displayName": "x"}
    assert s.provision_identity("w") == {"accepted": True}
    with pytest.raises(FabricApiError):
        FabricService(FakeApi({"POST connections": ApiResponse(400, {"errorCode": "Bad"})})).create_connection({})


def test_rest_transport_pins_the_host_retries_429_and_never_leaks_the_token_in_errors():
    class Sess:
        def __init__(self, resp):
            self.resp, self.calls = resp, []

        def request(self, method, url, **kw):
            self.calls.append((url, kw["headers"]))
            if isinstance(self.resp, Exception):
                raise self.resp
            return self.resp.pop(0)

    class R:
        def __init__(self, code, body=None, headers=None):
            self.status_code, self._b, self.headers, self.content = code, body, headers or {}, b"x" if body is not None else b""

        def json(self):
            return self._b

    sleeps = []
    s = Sess([R(429, {}, {"Retry-After": "2"}), R(200, {"value": []})])
    api = RestFabricApi(lambda: "TOK", session=s, sleep=sleeps.append)
    assert api.request("GET", "workspaces").status == 200 and sleeps == [2.0] and s.calls[0][1]["Authorization"] == "Bearer TOK"
    with pytest.raises(FabricApiError, match="other than the Fabric API"):
        api.request("GET", "https://evil.example.com/v1/x")
    with pytest.raises(FabricApiError) as e:
        RestFabricApi(lambda: "TOK", session=Sess(ConnectionError("boom TOK"))).request("GET", "workspaces")
    assert "TOK" not in str(e.value)


def test_fab_cli_transport_unwraps_the_documented_shapes_and_reports_a_missing_cli():
    body = {"value": [{"id": "1"}]}
    for out in (json.dumps({"result": {"data": [{"text": body}]}}), json.dumps({"result": {"data": [{"text": json.dumps(body)}]}}),
                json.dumps({"result": {"data": [body]}}), "header line\n" + json.dumps(body)):
        r = FakeRunner({"fab api": ok(out)})
        assert FabCliApi(r).request("GET", "workspaces").body == body, out
    assert r.calls[0][0][:5] == ["fab", "api", "workspaces", "-X", "get"]
    with pytest.raises(FabricApiError, match="not installed"):
        FabCliApi(FakeRunner({"fab api": MISSING})).request("GET", "workspaces")
    with pytest.raises(FabricApiError):
        FabCliApi(FakeRunner({"fab api": bad("Not authenticated")})).request("GET", "workspaces")
    assert unwrap_fab({"a": 1}) == {"a": 1}
