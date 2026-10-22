"""Demo mode's stand-in for the Fabric REST API: a small stateful in-memory service behind the ``requests``-style session interface.

Only demo mode uses it, and demo mode is labelled everywhere it appears. It exists so the whole guided flow (deploy, validation,
provisioning plan / approval / apply / verify) can be shown on the bundled sample without a tenant. It never contacts a network, holds
no credential, and production mode (``ADF2FABRIC_ENV=production``) refuses demo mode entirely, so nothing it returns can be mistaken for a
real result in a real deployment.
"""

from __future__ import annotations

import base64
import json
import re
import uuid
from typing import Any, Optional

DEMO_WS = "00000000-0000-0000-0000-00000000aa01"
DEMO_CAP = "00000000-0000-0000-0000-00000000cc01"
FABRIC = "https://api.fabric.microsoft.com/v1/"


class _Resp:
    def __init__(self, status: int, body: Any = None, headers: Optional[dict] = None):
        self.status_code, self._b, self.headers = status, body, headers or {}
        self.content = b"x" if body is not None else b""
        self.text = ""

    def json(self):
        return self._b


class DemoFabricHttp:
    def __init__(self) -> None:
        self.pipes: dict[str, dict] = {}  # id -> {displayName, definition}
        self.items: list[dict] = []
        self.pools: list[dict] = []
        self.roles: list[dict] = []
        self.workspace: dict[str, Any] = {"id": DEMO_WS, "displayName": "Demo Fabric workspace", "type": "Workspace", "capacityId": DEMO_CAP,
                                          "capacityRegion": "West Europe"}

    def request(self, method: str, url: str, headers=None, json=None, timeout=None, **_: Any):  # noqa: A002 - the requests signature
        if not url.startswith(FABRIC):
            raise AssertionError("demo target only answers Fabric URLs")
        path = url[len(FABRIC):]
        p, _, q = path.partition("?")
        body = json
        if p == "capacities":
            return _Resp(200, {"value": [{"id": DEMO_CAP, "displayName": "Demo capacity", "sku": "F64", "region": "West Europe", "state": "Active"}]})
        if p == "connections":
            return _Resp(200, {"value": [{"id": "00000000-0000-0000-0000-0000000000c1", "displayName": "Sales SQL (existing)"}]})
        if p == "workspaces":
            if method == "GET":
                m = re.search(r"roles=(\w+)", q)
                rows = [] if (m and m.group(1) not in ("Admin", "Member", "Contributor", "Viewer")) else [self.workspace]
                return _Resp(200, {"value": [{k: v for k, v in w.items() if k != "workspaceIdentity"} for w in rows]})
            return _Resp(409, {"errorCode": "DemoReadOnly", "message": "The demo does not create workspaces."})
        if p == f"workspaces/{DEMO_WS}":
            return _Resp(200, dict(self.workspace))
        m = re.fullmatch(rf"workspaces/{DEMO_WS}/(\w+)(?:/([^/]+))?(?:/(.*))?", p)
        if not m:
            return _Resp(404, {"errorCode": "NotFound"})
        sub, a, rest = m.groups()
        if sub == "dataPipelines":
            if method == "GET" and a is None:
                return _Resp(200, {"value": [{"id": i, "displayName": v["displayName"]} for i, v in self.pipes.items()]})
            if method == "POST" and a is None:
                pid = str(uuid.uuid4())
                self.pipes[pid] = {"displayName": body["displayName"], "definition": body.get("definition")}
                return _Resp(201, {"id": pid, "displayName": body["displayName"]})
            if method == "POST" and rest == "updateDefinition" and a in self.pipes:
                self.pipes[a]["definition"] = body.get("definition")
                return _Resp(200, {})
            if method == "POST" and rest == "getDefinition" and a in self.pipes:
                return _Resp(200, {"definition": self.pipes[a]["definition"]})
        if sub == "items":
            if method == "GET" and a is None:
                t = re.search(r"type=(\w+)", q)
                rows = [{"id": i, "displayName": v["displayName"], "type": "DataPipeline"} for i, v in self.pipes.items()] + self.items
                return _Resp(200, {"value": [r for r in rows if not t or r["type"] == t.group(1)]})
            if a in self.pipes and rest == "jobs/Pipeline/schedules":
                return _Resp(200, {"value": []})
        if sub == "spark" and a == "pools":
            if method == "GET":
                return _Resp(200, {"value": self.pools})
            pool = {"id": str(uuid.uuid4()), **(body or {})}
            self.pools.append(pool)
            return _Resp(201, pool)
        if sub == "roleAssignments":
            if method == "GET":
                return _Resp(200, {"value": self.roles})
            r = {"id": str(uuid.uuid4()), **(body or {})}
            self.roles.append(r)
            return _Resp(201, r)
        kinds = {"lakehouses": "Lakehouse", "warehouses": "Warehouse", "environments": "Environment"}
        if sub in kinds and method == "POST":
            if any(i["displayName"].lower() == body["displayName"].lower() for i in self.items):
                return _Resp(409, {"errorCode": "ItemDisplayNameAlreadyInUse"})
            item = {"id": str(uuid.uuid4()), "displayName": body["displayName"], "type": kinds[sub]}
            self.items.append(item)
            return _Resp(201, item)
        if sub == "provisionIdentity" and method == "POST":
            self.workspace["workspaceIdentity"] = {"applicationId": "demo", "servicePrincipalId": "demo"}
            return _Resp(200, {"applicationId": "demo", "servicePrincipalId": "demo"})
        return _Resp(404, {"errorCode": "NotFound"})
