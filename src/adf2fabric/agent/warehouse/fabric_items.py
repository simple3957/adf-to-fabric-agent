"""Fabric REST for Warehouse items: find the workspace and Warehouse by name, create a Warehouse (only on request).

Built on the deployment ``FabricClient`` (429 retry, long-running operations, token only sent to Fabric).
"""

from __future__ import annotations

from typing import Optional

from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, DeployError, FabricClient, _error_text

FABRIC_RESOURCE = "https://api.fabric.microsoft.com"


class WarehouseClient(FabricClient):
    def _get_all(self, url: str) -> list[dict]:
        out, token = [], None
        while True:
            resp = self._call("GET", url + (f"?continuationToken={token}" if token else ""))
            if resp.status_code != 200:
                raise DeployError(f"GET {url.replace(FABRIC_API, '')} failed (HTTP {resp.status_code}): {_error_text(resp)}")
            data = resp.json()
            out += data.get("value", [])
            token = data.get("continuationToken")
            if not token:
                return out

    def workspace_id(self, name_or_id: str) -> str:
        for w in self._get_all(f"{FABRIC_API}/workspaces"):
            if name_or_id.lower() in (w["id"].lower(), w["displayName"].lower()):
                return w["id"]
        raise DeployError(f"Workspace '{name_or_id}' not found or not accessible.")

    def find_warehouse(self, workspace: str, name: str) -> Optional[dict]:
        for w in self._get_all(f"{FABRIC_API}/workspaces/{workspace}/warehouses"):
            if w["displayName"].lower() == name.lower():
                return self.get_warehouse(workspace, w["id"])
        return None

    def get_warehouse(self, workspace: str, warehouse_id: str) -> dict:
        resp = self._call("GET", f"{FABRIC_API}/workspaces/{workspace}/warehouses/{warehouse_id}")
        if resp.status_code != 200:
            raise DeployError(f"Reading the Warehouse failed (HTTP {resp.status_code}): {_error_text(resp)}")
        return resp.json()

    def create_warehouse(self, workspace: str, name: str, description: str = "", collation: Optional[str] = None,
                         polls: int = 40) -> dict:
        """Create and wait until it is listed with a SQL endpoint. Progress is checked by listing the workspace,
        not by following the operation URL (which can point outside the Fabric API host)."""
        body: dict = {"displayName": name, "description": description}
        if collation:
            body["creationPayload"] = {"defaultCollation": collation}
        resp = self._call("POST", f"{FABRIC_API}/workspaces/{workspace}/warehouses", body)
        if resp.status_code not in (200, 201, 202):
            raise DeployError(f"Creating Warehouse '{name}' failed (HTTP {resp.status_code}): {_error_text(resp)}")
        for _ in range(polls):
            wh = self.find_warehouse(workspace, name)
            if wh and sql_endpoint(wh):
                return wh
            self._sleep(float(resp.headers.get("Retry-After", 5)))
        raise DeployError(f"Warehouse '{name}' was requested but is not ready yet; check the workspace and retry.")


    def pipeline_definition(self, workspace: str, item_id: str) -> dict:
        """The pipeline-content.json of a data pipeline."""
        import base64
        import json

        resp = self._call("POST", f"{FABRIC_API}/workspaces/{workspace}/items/{item_id}/getDefinition")
        if resp.status_code == 202:
            op = self._finish(resp, "Reading the pipeline definition")["operation"]
            resp = self._call("GET", op.rstrip("/") + "/result")
        if resp.status_code != 200:
            raise DeployError(f"Reading the pipeline definition failed (HTTP {resp.status_code}): {_error_text(resp)}")
        part = next(p for p in resp.json()["definition"]["parts"] if p["path"] == "pipeline-content.json")
        return json.loads(base64.b64decode(part["payload"]).decode("utf-8"))


def sql_endpoint(warehouse: dict) -> Optional[str]:
    return (warehouse.get("properties") or {}).get("connectionString")
