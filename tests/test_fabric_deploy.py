"""Fabric deployment against a fake REST session: no network, no credentials."""

from __future__ import annotations

import base64
import json

import pytest

from adf2fabric.agent.deployment import fabric_deploy as fd

WS = "11111111-1111-1111-1111-111111111111"
GOOD = {"properties": {"activities": [
    {"name": "W", "type": "Wait", "dependsOn": [], "typeProperties": {"waitTimeInSeconds": 1}}]}}
UNBOUND = {"properties": {"activities": [{"name": "C", "type": "Copy", "typeProperties": {
    "source": {"datasetSettings": {"externalReferences": {"connection": "<connection:LS>"}}}}}]}}
LIST = ("GET", f"/workspaces/{WS}/dataPipelines")
CREATE = ("POST", f"/workspaces/{WS}/dataPipelines")


class Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}
        self.content = b"x" if body is not None else b""
        self.text = json.dumps(body or {})

    def json(self):
        return self._body


class Fake:
    """Records calls; `script` maps (method, url-suffix) -> responses handed out in order (last one repeats)."""

    def __init__(self, script):
        self.script, self.calls = script, []

    def request(self, method, url, headers=None, json=None, timeout=None):
        self.calls.append((method, url, json, headers))
        for (m, suffix), queue in self.script.items():
            if m == method and url.split("?")[0].endswith(suffix):
                return queue.pop(0) if len(queue) > 1 else queue[0]
        raise AssertionError(f"unscripted call {method} {url}")


def client(script):
    s = Fake(script)
    return fd.FabricClient("TOKEN", session=s, sleep=lambda _: None), s


def pipelines(*pairs):
    return Resp(200, {"value": [{"id": i, "displayName": n} for i, n in pairs]})


def test_dry_run_only_lists_and_never_writes():
    c, s = client({LIST: [pipelines(("abc", "Existing"))]})
    res = fd.deploy(c, WS, [fd.DeployItem("Existing", GOOD), fd.DeployItem("New", GOOD)], apply=False)
    assert [(r.name, r.action) for r in res] == [("Existing", "update"), ("New", "create")]
    assert {m for m, *_ in s.calls} == {"GET"}


def test_apply_creates_with_documented_body_and_updates_existing():
    c, s = client({
        LIST: [pipelines(("abc", "Old")), pipelines(("abc", "Old"), ("new1", "New"))],
        CREATE: [Resp(201, {"id": "new1"})],
        ("POST", f"/workspaces/{WS}/dataPipelines/abc/updateDefinition"): [Resp(200, {})],
    })
    res = fd.deploy(c, WS, [fd.DeployItem("Old", GOOD), fd.DeployItem("New", GOOD)], apply=True)
    assert [(r.name, r.action, r.item_id) for r in res] == [("Old", "update", "abc"), ("New", "create", "new1")]
    create = next(b for m, u, b, _ in s.calls if m == "POST" and u.endswith("/dataPipelines"))
    assert create["displayName"] == "New"
    part = create["definition"]["parts"][0]
    assert part["path"] == "pipeline-content.json" and part["payloadType"] == "InlineBase64"
    assert json.loads(base64.b64decode(part["payload"])) == GOOD
    assert all(h["Authorization"] == "Bearer TOKEN" for *_, h in s.calls)


def test_unbound_or_invalid_pipelines_are_refused_and_nothing_is_written():
    c, s = client({LIST: [pipelines()]})
    res = fd.deploy(c, WS, [fd.DeployItem("U", UNBOUND), fd.DeployItem("Bad", GOOD, structural_valid=False),
                            fd.DeployItem("Empty", {"properties": {"activities": []}})], apply=True)
    assert [r.action for r in res] == ["refused"] * 3
    assert "<connection:LS>" in res[0].detail
    assert {m for m, *_ in s.calls} == {"GET"}


def test_allow_unbound_pushes_it():
    c, _ = client({LIST: [pipelines(), pipelines(("i", "U"))], CREATE: [Resp(201, {"id": "i"})]})
    assert fd.deploy(c, WS, [fd.DeployItem("U", UNBOUND)], apply=True, allow_unbound=True)[0].action == "create"


def test_long_running_create_is_polled_to_success_and_failure_is_reported():
    op = f"{fd.FABRIC_API}/operations/op1"
    c, _ = client({
        LIST: [pipelines(), pipelines(("i", "P"))],
        CREATE: [Resp(202, {}, {"Location": op, "Retry-After": "1"})],
        ("GET", "/operations/op1"): [Resp(200, {"status": "Running"}), Resp(200, {"status": "Succeeded"})],
    })
    assert fd.deploy(c, WS, [fd.DeployItem("P", GOOD)], apply=True)[0].item_id == "i"
    c, _ = client({
        LIST: [pipelines()],
        CREATE: [Resp(202, {}, {"Location": op})],
        ("GET", "/operations/op1"): [Resp(200, {"status": "Failed",
                                               "error": {"errorCode": "CorruptedPayload", "message": "bad"}})],
    })
    r = fd.deploy(c, WS, [fd.DeployItem("P", GOOD)], apply=True)[0]
    assert r.action == "failed" and "CorruptedPayload" in r.detail


def test_one_failure_does_not_stop_the_other_pipelines():
    c, _ = client({
        LIST: [pipelines(), pipelines(("g", "Good"))],
        CREATE: [Resp(400, {"errorCode": "ItemDisplayNameAlreadyInUse", "message": "dup"}), Resp(201, {"id": "g"})],
    })
    res = fd.deploy(c, WS, [fd.DeployItem("Bad", GOOD), fd.DeployItem("Good", GOOD)], apply=True)
    assert [r.action for r in res] == ["failed", "create"]
    assert "ItemDisplayNameAlreadyInUse" in res[0].detail


def test_pagination_and_rate_limit_are_handled():
    first = Resp(200, {"value": [{"id": "1", "displayName": "A"}], "continuationToken": "t"})
    c, _ = client({LIST: [first, Resp(429, {}, {"Retry-After": "1"}), pipelines(("2", "B"))]})
    assert c.list_pipelines(WS) == {"a": "1", "b": "2"}


def test_bad_workspace_id_and_foreign_urls_are_rejected():
    c, _ = client({})
    with pytest.raises(fd.DeployError):
        fd.deploy(c, "not-a-guid", [], apply=False)
    with pytest.raises(fd.DeployError):
        c._call("GET", "https://evil.example.com/x")


# --- child pipelines: built first, ids substituted into the parent ------------------------------------------


def parent_of(*children, name="Run"):
    acts = [{"name": f"{name}_{c}", "type": "InvokePipeline", "dependsOn": [],
             "typeProperties": {"pipelineId": f"<item:pipeline:{c}>", "workspaceId": "<workspace-id>",
                                "waitOnCompletion": True}} for c in children]
    return {"properties": {"activities": acts}}


def payloads(session):
    """{displayName: decoded pipeline content} for every create the fake saw, in call order."""
    out = {}
    for m, u, body, _ in session.calls:
        if m == "POST" and u.endswith("/dataPipelines"):
            part = body["definition"]["parts"][0]
            out[body["displayName"]] = json.loads(base64.b64decode(part["payload"]))
    return out


def test_children_are_created_before_parents_and_ids_are_filled_in():
    c, s = client({
        LIST: [pipelines(), pipelines(("c-id", "Child")), pipelines(("c-id", "Child"), ("p-id", "Parent"))],
        CREATE: [Resp(201, {"id": "x"})],
    })
    # Parent listed first on purpose: the deploy must still build the child first.
    res = fd.deploy(c, WS, [fd.DeployItem("Parent", parent_of("Child")), fd.DeployItem("Child", GOOD)], apply=True)
    assert [(r.name, r.action) for r in res] == [("Child", "create"), ("Parent", "create")]
    assert list(payloads(s)) == ["Child", "Parent"]
    inv = payloads(s)["Parent"]["properties"]["activities"][0]["typeProperties"]
    assert inv["pipelineId"] == "c-id" and inv["workspaceId"] == WS
    assert res[1].children == ["Child"]


def test_grandchildren_are_ordered_transitively():
    ordered, cyc = fd.order_children_first([
        fd.DeployItem("Top", parent_of("Mid")), fd.DeployItem("Mid", parent_of("Leaf")), fd.DeployItem("Leaf", GOOD)])
    assert [i.name for i in ordered] == ["Leaf", "Mid", "Top"] and cyc == {}


def test_dry_run_plans_children_first_without_writing():
    c, s = client({LIST: [pipelines()]})
    res = fd.deploy(c, WS, [fd.DeployItem("Parent", parent_of("Child")), fd.DeployItem("Child", GOOD)], apply=False)
    assert [(r.name, r.action) for r in res] == [("Child", "create"), ("Parent", "create")]
    assert {m for m, *_ in s.calls} == {"GET"}


def test_child_already_in_the_workspace_is_linked_without_being_redeployed():
    c, s = client({
        LIST: [pipelines(("c-id", "Child")), pipelines(("c-id", "Child"), ("p-id", "Parent"))],
        CREATE: [Resp(201, {})],
    })
    res = fd.deploy(c, WS, [fd.DeployItem("Parent", parent_of("Child"))], apply=True)
    assert [(r.name, r.action) for r in res] == [("Parent", "create")]
    assert list(payloads(s)) == ["Parent"]  # the existing child is referenced, never rewritten
    assert payloads(s)["Parent"]["properties"]["activities"][0]["typeProperties"]["pipelineId"] == "c-id"


def test_missing_child_refuses_the_parent_and_writes_nothing():
    c, s = client({LIST: [pipelines()]})
    res = fd.deploy(c, WS, [fd.DeployItem("Parent", parent_of("Ghost"))], apply=True)
    assert res[0].action == "refused" and "Ghost" in res[0].detail and "neither selected" in res[0].detail
    assert {m for m, *_ in s.calls} == {"GET"}


def test_failed_child_stops_its_parent_but_not_unrelated_pipelines():
    c, s = client({
        LIST: [pipelines(), pipelines(("o-id", "Other"))],
        CREATE: [Resp(400, {"errorCode": "CorruptedPayload", "message": "bad"}), Resp(201, {})],
    })
    res = fd.deploy(c, WS, [fd.DeployItem("Parent", parent_of("Child")), fd.DeployItem("Child", GOOD),
                            fd.DeployItem("Other", GOOD)], apply=True)
    by = {r.name: r for r in res}
    assert by["Child"].action == "failed"
    assert by["Parent"].action == "refused" and "was not deployed" in by["Parent"].detail
    assert by["Other"].action == "create"
    assert list(payloads(s)) == ["Child", "Other"]  # the parent was never sent


def test_a_child_that_was_refused_blocks_the_parent_with_a_clear_reason():
    c, s = client({LIST: [pipelines()]})
    res = fd.deploy(c, WS, [fd.DeployItem("Parent", parent_of("Child")), fd.DeployItem("Child", UNBOUND)], apply=True)
    by = {r.name: r for r in res}
    assert by["Child"].action == "refused" and by["Parent"].action == "refused"
    assert "was not deployed" in by["Parent"].detail
    assert {m for m, *_ in s.calls} == {"GET"}


def test_cycles_are_refused_accurately():
    items = [fd.DeployItem("A", parent_of("B")), fd.DeployItem("B", parent_of("A")),
             fd.DeployItem("Caller", parent_of("A")), fd.DeployItem("Free", GOOD)]
    ordered, cyc = fd.order_children_first(items)
    assert [i.name for i in ordered] == ["Free"]
    assert cyc["A"] == cyc["B"] == "part of a pipeline-invocation cycle"
    assert cyc["Caller"] == "invokes a pipeline that is part of a cycle"
    c, s = client({LIST: [pipelines()]})
    res = {r.name: r for r in fd.deploy(c, WS, items, apply=False)}
    assert res["A"].action == res["B"].action == res["Caller"].action == "refused" and res["Free"].action == "create"
    assert fd.order_children_first([fd.DeployItem("Me", parent_of("Me"))])[1] == {
        "Me": "part of a pipeline-invocation cycle"}


def test_other_placeholders_are_left_for_preflight():
    both = parent_of("Child")
    both["properties"]["activities"][0]["externalReferences"] = {"connection": "<connection:invoke-pipeline>"}
    c, _ = client({LIST: [pipelines(("c-id", "Child"))]})
    res = fd.deploy(c, WS, [fd.DeployItem("Parent", both)], apply=False)
    assert res[0].action == "refused" and "<connection:invoke-pipeline>" in res[0].detail
    assert "<item:pipeline:Child>" not in res[0].detail  # the child id was resolvable, so only the connection blocks
