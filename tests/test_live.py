"""Real calls only: the tool reaches Azure Data Factory and Microsoft Fabric with the machine's `az login` and validates the real project.

Nothing is faked and nothing is written: every call is a read (ARM GETs, Fabric GETs and getDefinition), and the validation report is
built in memory, never saved over the project's own. These tests run on every `pytest` and fail when a sign-in, the network or the
real state is not what the project says.

Which project and workspace (no tenant, workspace or subscription id is written in this file):
* ``ADF2FABRIC_LIVE_PROJECT``    the project under ``projects/`` (default ``adf2fabricv1``)
* ``ADF2FABRIC_LIVE_WORKSPACE``  the Fabric workspace id (default: the workspace of the project's last deployment)
The Fabric account is the signed-in `az` account that can read that workspace; the ADF factory is the one the project was read from.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from adf2fabric import cloud
from adf2fabric.agent.provisioning.transport import FabricGateway
from adf2fabric.agent.validation import engine
from adf2fabric.connections.providers.azure_cli import AzureCliProvider, ProviderError
from adf2fabric.config import load_config, projects_root
from adf2fabric.project.store import ProjectStore
from adf2fabric.project.validation import Result as R, Stage

ROOT = Path(__file__).resolve().parents[1]
FACTORY = re.compile(r"/subscriptions/([^/]+)/resourceGroups/([^/]+)/providers/Microsoft\.DataFactory/factories/([^/?#\s]+)", re.I)


@pytest.fixture(scope="module")
def live():
    """The real project, the workspace it was deployed to, and a Fabric gateway signed in as the `az` account that can read it."""
    os.chdir(ROOT)  # projects_root in config/config.yaml is relative to the repository
    store = ProjectStore(projects_root(load_config()))
    pid = os.environ.get("ADF2FABRIC_LIVE_PROJECT", "adf2fabricv1")
    assert store.exists(pid), f"Project '{pid}' is not under {store.projects_root}: run the migration once, or set ADF2FABRIC_LIVE_PROJECT."
    ws = os.environ.get("ADF2FABRIC_LIVE_WORKSPACE")
    if not ws:
        rec = json.loads((store.project_dir(pid) / "deployment" / "deployment-record.json").read_text(encoding="utf-8-sig"))
        latest = max(rec["pipelines"].values(), key=lambda v: v.get("at", 0))
        ws = latest["workspace"]
    az = AzureCliProvider()
    tried = []
    for account in az.accounts(refresh=True):
        try:
            token = az.token(account=account.username, resource=cloud.FABRIC_ENDPOINT)
        except ProviderError as err:
            tried.append(f"{account.username}: {err}")
            continue
        fab = FabricGateway(lambda t=token: t)
        r = fab.request("GET", f"workspaces/{ws}")
        if r.status == 200:
            return {"store": store, "pid": pid, "ws": ws, "fab": fab, "workspace": r.body or {}, "az": az}
        tried.append(f"{account.username}: HTTP {r.status}")
    pytest.fail("No signed-in `az` account can read the Fabric workspace. Sign in with `az login` as a member of it. Tried: " + "; ".join(tried))


def test_az_login_reaches_the_fabric_workspace_and_it_is_on_a_capacity(live):
    assert live["workspace"].get("displayName")
    assert live["workspace"].get("capacityId"), "The workspace is not on a capacity, so nothing in it can run."


def test_the_real_migration_validates_static_api_and_deployment_with_no_failure(live):
    r = engine.validate(live["store"], live["pid"], stages={"static", "api", "deployment"}, fab=live["fab"], workspace=live["ws"])
    open_ = [f"{c.stage.value} {c.id} {c.component}: {c.summary}" for c in r.checks if c.required and c.result in (R.FAIL, R.BLOCKED)]
    assert not open_, "\n".join(open_)
    status = {s.stage: s.status for s in r.stages}
    assert status[Stage.static] is R.PASS and status[Stage.api] is R.PASS and status[Stage.deployment] is R.PASS
    assert not r.full_success_claimed  # no runtime evidence was asked for, so the overall verdict cannot be "verified"


def test_every_item_the_pipelines_use_exists_in_the_workspace(live):
    r = engine.validate(live["store"], live["pid"], stages={"api"}, fab=live["fab"], workspace=live["ws"])
    items = [c for c in r.checks if c.id == "deployment.items_exist"]
    assert items and items[0].result in (R.PASS, R.NOT_RUN), items[0].summary + " " + "; ".join(items[0].evidence)
    conns = [c for c in r.checks if c.id == "deployment.connections_exist"]
    assert conns and conns[0].result in (R.PASS, R.NOT_RUN), conns[0].summary + " " + "; ".join(conns[0].evidence)


def test_runtime_is_claimed_only_from_a_real_run_in_this_workspace(live):
    r = engine.validate(live["store"], live["pid"], stages={"static", "api", "deployment", "runtime"}, fab=live["fab"], workspace=live["ws"])
    runtime = [c for c in r.checks if c.stage is Stage.runtime]
    assert runtime
    for c in runtime:  # a run recorded in another workspace never counts for this one
        if c.result is R.PASS:
            assert "another Fabric workspace" not in c.summary
    passed = all(c.result is R.PASS for c in runtime if c.required)
    assert r.full_success_claimed == (r.overall == "MIGRATION_VERIFIED")
    if not passed:
        assert r.overall != "MIGRATION_VERIFIED"


def test_the_adf_factory_is_read_with_az_login_and_matches_the_project_source(live):
    project = (live["store"].project_dir(live["pid"]) / "project.yaml").read_text(encoding="utf-8")
    m = FACTORY.search(project)
    assert m, "The project was not read from a live factory, so there is no factory to compare with."
    sub, rg, factory = m.groups()
    rep = live["az"].validate_factory(sub, rg, factory)
    assert rep.state.value == "connection_tested", f"{rep.error} {' '.join(rep.actions)}"
    arm = live["az"].get_resource(sub, f"/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factory}/pipelines",
                                  "2018-06-01")
    in_adf = {p["name"].lower() for p in arm.get("value", [])}
    in_project = {p.stem.lower() for p in (live["store"].source_dir(live["pid"]) / "pipeline").glob("*.json")}
    assert in_adf == in_project, (f"ADF and the project's source differ: only in ADF {sorted(in_adf - in_project)}, "
                                  f"only in the project {sorted(in_project - in_adf)}. Import the source again (Discover).")


def test_the_deploy_read_back_lists_what_is_really_in_the_workspace(live, tmp_path):
    """What the Deploy step shows after a deploy: the workspace read back from Fabric. Recorded into a copy of the project, never the real one."""
    import shutil
    import stat

    from adf2fabric.api.views import validation_summary

    src = live["store"].project_dir(live["pid"])
    shutil.copytree(src, tmp_path / live["pid"], ignore=shutil.ignore_patterns(".git"))
    store = ProjectStore(tmp_path)
    try:
        engine.record(store, live["pid"], readback=True, fab=live["fab"], workspace=live["ws"])
        v = validation_summary(store, live["pid"], workspace=live["ws"])
        rb = v["readback"]
        assert rb["current"] and rb["items"]
        wrong = [f"{r['kind']} {r['name']}: {r['state']} {r['detail']}" for r in rb["items"] if not r["ok"]]
        assert not wrong, "\n".join(wrong)
        generated = {p.parent.name[: -len(".DataPipeline")].lower() for p in (store.conversion_dir(live["pid"]) / "pipelines").glob("*/pipeline-content.json")}
        listed = {r["name"].lower() for r in rb["items"] if r["kind"] == "Pipeline"}
        assert generated <= listed, f"not read back: {sorted(generated - listed)}"
        engine.record(store, live["pid"], workspace=live["ws"])  # the Validate step afterwards keeps the read-back while it is current
        assert validation_summary(store, live["pid"], workspace=live["ws"])["readback"]["current"]
    finally:
        shutil.rmtree(tmp_path / live["pid"], onexc=lambda f, p, e: (os.chmod(p, stat.S_IWRITE), f(p)))
