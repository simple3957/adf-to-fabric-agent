"""Fabric deployment.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations


from adf2fabric.agent.deployment import fabric_deploy as fd

WS = "11111111-1111-1111-1111-111111111111"
GOOD = {"properties": {"activities": [
    {"name": "W", "type": "Wait", "dependsOn": [], "typeProperties": {"waitTimeInSeconds": 1}}]}}
LIST = ("GET", f"/workspaces/{WS}/dataPipelines")
CREATE = ("POST", f"/workspaces/{WS}/dataPipelines")


# --- child pipelines: built first, ids substituted into the parent ------------------------------------------


def parent_of(*children, name="Run"):
    acts = [{"name": f"{name}_{c}", "type": "InvokePipeline", "dependsOn": [],
             "typeProperties": {"pipelineId": f"<item:pipeline:{c}>", "workspaceId": "<workspace-id>",
                                "waitOnCompletion": True}} for c in children]
    return {"properties": {"activities": acts}}


def test_grandchildren_are_ordered_transitively():
    ordered, cyc = fd.order_children_first([
        fd.DeployItem("Top", parent_of("Mid")), fd.DeployItem("Mid", parent_of("Leaf")), fd.DeployItem("Leaf", GOOD)])
    assert [i.name for i in ordered] == ["Leaf", "Mid", "Top"] and cyc == {}


