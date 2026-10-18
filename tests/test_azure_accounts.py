"""Several signed-in accounts at once: ADF can use one, Fabric another, neither changes the CLI's default."""

from __future__ import annotations

import json

import pytest

from adf2fabric.connections.providers.azure_cli import AzureCliProvider, ProviderError
from tests.test_connections_foundation import FakeRunner, bad, ok

PAL, GMAIL, OTHER = "sathvik.kamegaonkar@pal.tech", "sathvik7593@gmail.com", "someone@example.com"
ROWS = [
    {"id": "t-pal", "name": "N/A(tenant level account)", "tenantId": "t-pal", "state": "Enabled", "isDefault": True,
     "user": {"name": PAL}},
    {"id": "s-adf", "name": "Azure subscription 1", "tenantId": "t-adf", "state": "Enabled", "user": {"name": GMAIL}},
    {"id": "s-old", "name": "Azure subscription 1", "tenantId": "t-old", "state": "Disabled", "user": {"name": GMAIL}},
    {"id": "s-other", "name": "Azure subscription 1", "tenantId": "t-other", "state": "Enabled", "user": {"name": OTHER}},
]
LIST = "az account list"


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def provider(table=None, ttl=20.0, clock=None):
    runner = FakeRunner({LIST: ok(json.dumps(ROWS)), "az account get-access-token": ok("TOKEN\n"),
                         "az account show": ok(json.dumps({"id": "t-pal", "tenantId": "t-pal", "user": {"name": PAL}})),
                         "az logout": ok(), "az --version": ok("azure-cli 2.0"), "az version": ok('{"azure-cli": "2.0"}'),
                         **(table or {})})
    return AzureCliProvider(runner=runner, cache_seconds=ttl, clock=clock or Clock()), runner


def count(runner, prefix):
    return sum(1 for args, _ in runner.calls if " ".join(args).startswith(prefix))


# --- who is signed in ---------------------------------------------------------------------------------------------


def test_accounts_lists_each_person_once_default_first():
    p, _ = provider()
    got = p.accounts()
    assert [a.username for a in got] == [PAL, GMAIL, OTHER]
    assert got[0].is_default and not got[1].is_default
    gmail = got[1]
    assert gmail.tenants == ["t-adf", "t-old"] and gmail.subscription_count == 2


def test_a_sign_in_with_no_subscriptions_counts_none_but_still_appears():
    p, _ = provider()
    pal = p.accounts()[0]
    assert pal.username == PAL and pal.subscription_count == 0 and pal.tenants == ["t-pal"]


def test_subscriptions_can_be_limited_to_one_account_ignoring_case():
    p, _ = provider()
    assert [s.id for s in p.subscriptions(account=GMAIL.upper())] == ["s-adf", "s-old"]
    assert len(p.subscriptions()) == 4
    assert p.subscriptions(account="nobody@example.com") == []


# --- picking an account for a token -------------------------------------------------------------------------------


def test_subscription_for_prefers_the_tenant_then_an_enabled_real_subscription():
    p, _ = provider()
    assert p.subscription_for(GMAIL) == "s-adf"  # enabled beats disabled
    assert p.subscription_for(GMAIL, "t-old") == "s-old"  # a named tenant wins
    assert p.subscription_for(PAL) == "t-pal"  # only the tenant-level entry exists
    assert p.subscription_for("nobody@example.com") is None


def test_a_token_for_an_account_asks_the_cli_for_that_accounts_subscription_and_never_changes_the_default():
    p, runner = provider()
    assert p.token(account=GMAIL, resource="https://api.fabric.microsoft.com") == "TOKEN"
    argv = next(a for a, _ in runner.calls if a[:3] == ["az", "account", "get-access-token"])
    assert argv[argv.index("--subscription") + 1] == "s-adf" and "--tenant" not in argv
    assert not any(a[:3] == ["az", "account", "set"] for a, _ in runner.calls)


def test_an_explicit_subscription_beats_an_account():
    p, runner = provider()
    p.token(subscription="s-other", account=GMAIL)
    argv = next(a for a, _ in runner.calls if a[:3] == ["az", "account", "get-access-token"])
    assert argv[argv.index("--subscription") + 1] == "s-other"


def test_an_account_that_is_not_signed_in_is_a_clear_error_not_a_default_token():
    p, runner = provider()
    with pytest.raises(ProviderError, match="not signed in") as e:
        p.token(account="nobody@example.com")
    assert e.value.actions and not any(a[:3] == ["az", "account", "get-access-token"] for a, _ in runner.calls)


def test_without_an_account_the_default_is_used_as_before():
    p, runner = provider()
    p.token(tenant="t-adf")
    argv = next(a for a, _ in runner.calls if a[:3] == ["az", "account", "get-access-token"])
    assert "--subscription" not in argv and argv[argv.index("--tenant") + 1] == "t-adf"


# --- the cache ----------------------------------------------------------------------------------------------------


def test_the_account_list_is_fetched_once_for_many_calls_until_it_expires():
    clock = Clock()
    p, runner = provider(clock=clock)
    p.accounts(), p.subscriptions(), p.subscription_for(GMAIL), p.accounts()
    assert count(runner, LIST) == 1
    clock.now = 21.0
    p.accounts()
    assert count(runner, LIST) == 2


def test_refresh_bypasses_the_cache():
    p, runner = provider()
    p.accounts()
    p.accounts(refresh=True)
    p.subscriptions(refresh=True)
    assert count(runner, LIST) == 3


def test_a_sign_out_drops_the_cache_so_the_next_call_sees_the_change():
    p, runner = provider()
    p.accounts()
    p.logout(GMAIL)
    p.accounts()
    assert count(runner, LIST) == 2


def test_starting_a_sign_in_drops_the_cache():
    class Logins:
        def start(self, argv, env_extra=None):
            return "attempt"

    p, runner = provider()
    p.logins = Logins()
    p.accounts()
    assert p.start_login(device_code=True) == "attempt"
    p.accounts()
    assert count(runner, LIST) == 2


def test_status_is_cached_too_and_a_missing_cli_is_never_cached():
    p, runner = provider()
    first = p.status()
    p.status(), p.status()
    assert first.authenticated and count(runner, "az account show") == 1
    p.status(refresh=True)
    assert count(runner, "az account show") == 2

    gone, runner2 = provider({"az account show": FakeRunnerMissing()})
    assert not gone.status().available
    gone.status()
    assert count(runner2, "az account show") == 2  # installing the CLI is what the user does next: no stale answer


def FakeRunnerMissing():
    from adf2fabric.connections.runner import CliResult

    return CliResult(found=False, returncode=127, stderr="'az' was not found on PATH")


def test_a_cached_status_cannot_be_changed_by_a_caller():
    p, _ = provider()
    p.status().account = "mutated"
    assert p.status().account == PAL


def test_a_failed_account_list_is_an_error_and_is_not_cached():
    p, runner = provider({LIST: bad("ERROR: Please run 'az login'")})
    with pytest.raises(ProviderError):
        p.accounts()
    with pytest.raises(ProviderError):
        p.accounts()
    assert count(runner, LIST) == 2


# --- the API ------------------------------------------------------------------------------------------------------


def _client():
    from fastapi.testclient import TestClient

    from adf2fabric.api.app import create_app
    from adf2fabric.api.container import Settings
    from adf2fabric.api.demo import build_demo_container

    demo = build_demo_container()
    return TestClient(create_app(Settings(), live=demo, demo=demo), raise_server_exceptions=False)


def test_the_accounts_route_lists_who_is_signed_in():
    tc = _client()
    tc.post("/mode", json={"mode": "demo"}, headers={"X-Session-Id": "a1"})
    r = tc.get("/auth/azure_cli/accounts", headers={"X-Session-Id": "a1"})
    assert r.status_code == 200
    assert [a["username"] for a in r.json()["accounts"]] == ["demo.user@contoso.example"]
    assert tc.get("/azure/subscriptions", params={"account": "demo.user@contoso.example"},
                  headers={"X-Session-Id": "a1"}).status_code == 200


def test_a_destination_remembers_which_account_it_uses():
    tc = _client()
    h = {"X-Session-Id": "a2"}
    tc.post("/mode", json={"mode": "demo"}, headers=h)
    r = tc.put("/session/destination", json={"provider": "azure_cli", "account": PAL, "workspace_id": "w"}, headers=h)
    assert r.status_code == 200 and r.json()["destination"]["account"] == PAL
    assert tc.get("/session", headers=h).json()["destination"]["account"] == PAL


def test_fabric_is_reached_as_the_chosen_account_even_when_another_is_the_default():
    from adf2fabric.api.container import Container, Session
    from adf2fabric.api.demo import build_demo_container

    seen = []

    class Azure:
        def token(self, **kw):
            seen.append(kw)
            return "t"

    c = Container(azure=Azure(), store=build_demo_container().store)
    sess = Session(id="x", destination={"provider": "azure_cli", "account": PAL, "tenant_id": "t-pal"})
    c.fabric(sess).api._token()  # the saved account
    c.fabric(sess, account=GMAIL).api._token()  # an explicit one wins
    assert [k["account"] for k in seen] == [PAL, GMAIL] and all(k["tenant"] == "t-pal" for k in seen)
