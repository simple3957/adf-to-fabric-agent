"""Creating a lakehouse and an ADLS Gen2 shortcut (fake Fabric, no network)."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from adf2fabric.agent.deployment import fabric_deploy as fd
from adf2fabric.agent.deployment import shortcut_deploy as sd
from tests.test_fabric_deploy import WS, Resp, client

LH = "66666666-6666-6666-6666-666666666666"
CONN = "6c1485aa-525f-490f-b2a3-737d4421fd4d"
LH_LIST = ("GET", f"/workspaces/{WS}/lakehouses")
LH_CREATE = ("POST", f"/workspaces/{WS}/lakehouses")
SC = ("GET", f"/workspaces/{WS}/items/{LH}/shortcuts")
SC_CREATE = ("POST", f"/workspaces/{WS}/items/{LH}/shortcuts")
SPEC = sd.ShortcutSpec("migrationcontainer", "Files", "https://datamigrationadftofabric.dfs.core.windows.net",
                       "/migrationcontainer", CONN)


def lakehouses(*pairs):
    return Resp(200, {"value": [{"id": i, "displayName": n} for i, n in pairs]})


def existing_shortcut(**over):
    t = {"location": SPEC.location, "subpath": SPEC.subpath, "connectionId": CONN, **over}
    return {"name": "migrationcontainer", "path": "/Files", "target": {"type": "AdlsGen2", "adlsGen2": t}}


def test_dry_run_with_nothing_there_only_reads():
    c, s = client({LH_LIST: [lakehouses()]})
    r = sd.create_lakehouse_shortcut(c, WS, "LH_Migration", SPEC, apply=False)
    assert (r.lakehouse_action, r.shortcut_action, r.lakehouse_id) == ("create", "create", None)
    assert {m for m, *_ in s.calls} == {"GET"} and "dry run" in r.detail


def test_apply_creates_the_lakehouse_then_the_shortcut_with_the_documented_body():
    c, s = client({LH_LIST: [lakehouses()], LH_CREATE: [Resp(201, {"id": LH, "displayName": "LH_Migration"})],
                   SC: [Resp(200, {"value": []})], SC_CREATE: [Resp(201, {})]})
    r = sd.create_lakehouse_shortcut(c, WS, "LH_Migration", SPEC, apply=True)
    assert (r.lakehouse_action, r.shortcut_action, r.lakehouse_id) == ("create", "create", LH)
    assert r.onelake_path == f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{LH}/Files/migrationcontainer"
    posts = [x for x in s.calls if x[0] == "POST"]
    assert posts[0][2] == {"displayName": "LH_Migration"}
    assert posts[1][2] == {"name": "migrationcontainer", "path": "Files", "target": {"adlsGen2": {
        "location": SPEC.location, "subpath": "/migrationcontainer", "connectionId": CONN}}}


def test_a_lakehouse_created_through_a_long_running_operation_is_looked_up_afterwards():
    op = f"{fd.FABRIC_API}/operations/9d899865-241b-4694-8232-74ec47124d24"
    c, _ = client({LH_LIST: [lakehouses(), lakehouses((LH, "LH_Migration"))],
                   LH_CREATE: [Resp(202, {}, {"Location": op, "Retry-After": "1"})],
                   ("GET", "/operations/9d899865-241b-4694-8232-74ec47124d24"): [Resp(200, {"status": "Succeeded"})],
                   SC: [Resp(200, {"value": []})], SC_CREATE: [Resp(201, {})]})
    assert sd.create_lakehouse_shortcut(c, WS, "LH_Migration", SPEC, apply=True).lakehouse_id == LH


def test_an_identical_shortcut_is_left_alone_and_a_rerun_creates_nothing():
    c, s = client({LH_LIST: [lakehouses((LH, "lh_migration"))], SC: [Resp(200, {"value": [existing_shortcut()]})]})
    r = sd.create_lakehouse_shortcut(c, WS, "LH_Migration", SPEC, apply=True)
    assert (r.lakehouse_action, r.shortcut_action) == ("exists", "exists")
    assert {m for m, *_ in s.calls} == {"GET"}


def test_a_shortcut_that_points_elsewhere_is_reported_and_never_overwritten():
    c, s = client({LH_LIST: [lakehouses((LH, "LH_Migration"))],
                   SC: [Resp(200, {"value": [existing_shortcut(subpath="/other")]})]})
    r = sd.create_lakehouse_shortcut(c, WS, "LH_Migration", SPEC, apply=True)
    assert r.shortcut_action == "differs" and "NOT replaced" in r.detail
    assert {m for m, *_ in s.calls} == {"GET"}


def test_a_refused_shortcut_is_an_error():
    c, _ = client({LH_LIST: [lakehouses((LH, "LH_Migration"))], SC: [Resp(200, {"value": []})],
                   SC_CREATE: [Resp(403, {"errorCode": "InsufficientPrivileges", "message": "no"})]})
    with pytest.raises(fd.DeployError, match="HTTP 403"):
        sd.create_lakehouse_shortcut(c, WS, "LH_Migration", SPEC, apply=True)


@pytest.mark.parametrize("field,value,why", [
    ("name", "bad/name", "shortcut name"),
    ("path", "Other/x", "start with Files or Tables"),
    ("location", "https://evil.example.com", "dfs.core.windows.net"),
    ("location", "http://acct.dfs.core.windows.net", "dfs.core.windows.net"),
    ("subpath", "container", "start with /"),
    ("subpath", "/a/../b", "start with /"),
    ("connection_id", "not-a-guid", "GUID"),
])
def test_bad_input_is_refused_before_any_request(field, value, why):
    c, s = client({})
    spec = sd.ShortcutSpec(**{**SPEC.__dict__, field: value})
    with pytest.raises(fd.DeployError, match=why):
        sd.create_lakehouse_shortcut(c, WS, "LH_Migration", spec, apply=True)
    assert not s.calls


def test_a_bad_workspace_or_lakehouse_name_is_refused():
    c, _ = client({})
    with pytest.raises(fd.DeployError, match="GUID"):
        sd.create_lakehouse_shortcut(c, "nope", "LH", SPEC, apply=False)
    with pytest.raises(fd.DeployError, match="lakehouse name"):
        sd.create_lakehouse_shortcut(c, WS, "bad/name", SPEC, apply=False)


def test_listing_follows_continuation_tokens():
    c, _ = client({LH_LIST: [Resp(200, {"value": [{"id": "a", "displayName": "A"}], "continuationToken": "t"}),
                             Resp(200, {"value": [{"id": "b", "displayName": "B"}]})]})
    assert sd.list_lakehouses(c, WS) == {"a": "a", "b": "b"}


def test_cli_dry_run_prints_the_plan_and_rejects_a_connection_that_is_not_a_guid(monkeypatch):
    from adf2fabric import cli as cli_mod

    c, s = client({LH_LIST: [lakehouses((LH, "LH_Migration"))], SC: [Resp(200, {"value": []})]})
    monkeypatch.setattr(fd, "FabricClient", lambda token: c)

    class Cred:
        def get_token(self, scope):
            return type("T", (), {"token": "TOKEN"})()

    monkeypatch.setattr("adf2fabric.connectivity._azure.build_credential", lambda ref: (Cred(), None))
    args = ["create-shortcut", "--workspace-id", WS, "--lakehouse", "LH_Migration", "--name", "migrationcontainer",
            "--storage-account", SPEC.location, "--subpath", "/migrationcontainer", "--connection-id", CONN]
    out = CliRunner().invoke(cli_mod.cli, args)
    assert out.exit_code == 0, out.output
    assert "dry run" in out.output and "Files/migrationcontainer: create" in out.output
    assert all(call[0] == "GET" for call in s.calls)
    bad = CliRunner().invoke(cli_mod.cli, args[:-1] + ["nope"])
    assert bad.exit_code == 1 and "GUID" in bad.output
