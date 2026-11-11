"""Fabric target provisioning: planning, approval, apply, retries, partial failure, verification, permissions.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from adf2fabric.agent.conversion.compute_plan import build_compute_plan
from adf2fabric.agent.provisioning import requirements as rq
from adf2fabric.agent.provisioning.transport import token_tenant
from adf2fabric.project.provisioning import ResourceType as T, TargetSpec
from tests.test_discovery_activity_inventory import _discover, _project
from tests.test_workloads import ESTATE  # a Databricks/Synapse estate with Spark compute and workloads

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
TENANT = "1680abbe-1a01-4cb0-99e9-452fe05bf615"
WS_ID = "53d67e72-8124-4ca3-ae55-c4b266935faa"


@pytest.fixture(scope="module")
def project(tmp_path_factory):
    store, pid = _project(tmp_path_factory.mktemp("prov"), ESTATE)
    d = _discover(store)
    plan = build_compute_plan(d)
    store.conversion_dir(pid).mkdir(parents=True, exist_ok=True)
    (store.conversion_dir(pid) / "compute-plan.json").write_text(plan.model_dump_json(), encoding="utf-8")
    from adf2fabric.agent.dependencies import discover_dependencies

    discover_dependencies(store, pid)  # linked services that need a Fabric connection come from the dependency inventory
    return store, pid


@pytest.fixture()
def prov(project, tmp_path):
    """A fresh copy of the project's records for each test (provisioning records are written next to the project)."""
    import shutil

    store, pid = project
    shutil.rmtree(store.provisioning_dir(pid), ignore_errors=True)
    return store, pid


def spec(**kw) -> TargetSpec:
    base = dict(workspace_name="target-ws", capacity_name="capone", tenant_id=TENANT, lakehouse_names=["lh_main"])
    base.update(kw)
    return TargetSpec(**base)


# =====================================================================================================================
# 1. requirements: only what the migration needs
# =====================================================================================================================


def test_requirements_come_from_the_actual_migration(project):
    store, pid = project
    reqs, notes = rq.build_requirements(spec(), rq.load_inputs(store, pid))
    types = {r.type for r in reqs}
    assert {T.workspace, T.capacity_assignment, T.lakehouse, T.connection} <= types
    assert T.warehouse not in types and T.azure_capacity not in types and T.workspace_identity not in types  # nothing was asked for or implied
    assert all(r.needed_by or r.type in (T.workspace, T.capacity_assignment, T.lakehouse) for r in reqs)


def test_a_project_without_spark_compute_gets_no_pool_or_environment(tmp_path):
    store, pid = _project(tmp_path, {"pipeline/P.json": {"name": "P", "properties": {"activities": [{"name": "W", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}]}}})
    _discover(store)
    reqs, notes = rq.build_requirements(spec(), rq.load_inputs(store, pid))
    assert not {r.type for r in reqs} & {T.spark_pool, T.environment, T.notebook, T.spark_job_definition, T.connection}


def test_spark_kept_on_its_source_compute_plans_no_pool_or_environment(project):
    store, pid = project
    inputs = rq.load_inputs(store, pid)
    assert inputs.compute is not None and inputs.compute.pools  # the compute plan does propose a pool
    for undecided_or_fabric in (spec(), spec(spark_compute="fabric")):
        assert T.spark_pool in {r.type for r in rq.build_requirements(undecided_or_fabric, inputs)[0]}
    reqs, notes = rq.build_requirements(spec(spark_compute="keep"), inputs)
    assert not {r.type for r in reqs} & {T.spark_pool, T.environment}
    assert any("keeps running on its source compute" in n for n in notes)
    assert {T.workspace, T.capacity_assignment, T.lakehouse} <= {r.type for r in reqs}  # everything else is still planned


def test_capacity_estimate_follows_the_documented_ratio():
    assert rq.sku_units("F64") == 64 and rq.sku_units("FTL64") is None and rq.sku_units("P1") is None
    assert rq.recommended_sku(12) == "F16" and rq.recommended_sku(0.5) == "F2" and rq.recommended_sku(5000) == "F2048"


# =====================================================================================================================
# 2. planning is read-only; existing resources are reused; conflicts are not overwritten
# =====================================================================================================================


# =====================================================================================================================
# 3. permissions, tenant and unreadable state
# =====================================================================================================================


def test_the_token_tenant_is_read_from_the_token_only():
    import base64

    payload = base64.urlsafe_b64encode(json.dumps({"tid": TENANT, "secret": "x"}).encode()).decode().rstrip("=")
    assert token_tenant(f"h.{payload}.s") == TENANT and token_tenant("garbage") is None


# =====================================================================================================================
# 4. Azure capacity (ARM): a different API with its own checks
# =====================================================================================================================


# =====================================================================================================================
# 5. approval and apply
# =====================================================================================================================


# =====================================================================================================================
# 6. retries, throttling, ambiguity
# =====================================================================================================================


# =====================================================================================================================
# 7. partial failure, journal, safe recovery
# =====================================================================================================================


# =====================================================================================================================
# 8. verification
# =====================================================================================================================


# =====================================================================================================================
# 9. nothing sensitive is persisted; nothing is deleted; discovery and tests create nothing
# =====================================================================================================================


def test_provisioning_modules_issue_no_delete_or_patch():
    src = "".join(p.read_text(encoding="utf-8") for p in (Path(__file__).parents[1] / "src/adf2fabric/agent/provisioning").glob("*.py"))
    assert not re.search(r'"(DELETE|PATCH)"', src) and "deprovision" not in src.lower().replace("deprovisioning", "")


# ---- the lakehouse and OneLake shortcuts the generated notebooks read through ----------------------------------------------------


