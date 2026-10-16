"""Create a lakehouse and an ADLS Gen2 OneLake shortcut in it, so a Fabric notebook can read storage it has no direct access to.

Why this exists: a Databricks notebook often reaches ADLS through a Unity Catalog external location. Run as a Fabric notebook,
the same ``abfss://`` path is read with the running identity's own access and fails with HTTP 401 (seen live on 2026-10-16).
Reading the data through a OneLake shortcut that uses a Fabric connection fixes that.

Fabric REST (verified against Microsoft Learn and live, 2026-10-16):

* ``POST /v1/workspaces/{ws}/lakehouses`` ``{displayName}`` - 201, or 202 (long running operation).
* ``GET  /v1/workspaces/{ws}/items/{lakehouse}/shortcuts`` - the existing shortcuts.
* ``POST /v1/workspaces/{ws}/items/{lakehouse}/shortcuts`` ``{name, path, target:{adlsGen2:{location, subpath, connectionId}}}`` - 201.

The connection is one that already exists in Fabric; this module never handles a storage key. Nothing is deleted or
overwritten: a shortcut with the same name and path that points elsewhere is reported, not replaced. Without ``apply`` it
only reads.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, DeployError, FabricClient, _GUID, _error_text

_ACCOUNT_URL = re.compile(r"^https://[a-z0-9]{3,24}\.dfs\.core\.windows\.net$")
_NAME = re.compile(r"^[A-Za-z0-9 _.\-]{1,128}$")


@dataclass
class ShortcutSpec:
    name: str  # shortcut name, e.g. migrationcontainer
    path: str  # where it is created inside the lakehouse, "Files" or "Files/sub"
    location: str  # https://<account>.dfs.core.windows.net
    subpath: str  # /<container>[/<folder>]
    connection_id: str


@dataclass
class ShortcutResult:
    lakehouse: str
    lakehouse_action: str  # create | exists
    lakehouse_id: Optional[str]
    shortcut: str
    shortcut_action: str  # create | exists | differs
    detail: str = ""
    onelake_path: Optional[str] = None


def validate_spec(spec: ShortcutSpec) -> None:
    if not _NAME.match(spec.name or ""):
        raise DeployError("The shortcut name may use letters, digits, space, _ . - (1..128 characters).")
    if not re.match(r"^(Files|Tables)(/[^/\\]+)*$", spec.path or ""):
        raise DeployError("The shortcut path must start with Files or Tables, for example 'Files'.")
    if not _ACCOUNT_URL.match((spec.location or "").lower()):
        raise DeployError("The storage location must look like https://<account>.dfs.core.windows.net.")
    if not (spec.subpath or "").startswith("/") or len(spec.subpath) < 2 or ".." in spec.subpath:
        raise DeployError("The subpath must start with / and name a container, for example /mycontainer.")
    if not _GUID.match(spec.connection_id or ""):
        raise DeployError("The connection id must be the GUID of an existing Fabric connection to the storage account.")


def _paged(client: FabricClient, url: str, what: str) -> list[dict]:
    out: list[dict] = []
    token: Optional[str] = None
    while True:
        resp = client._call("GET", url + (f"?continuationToken={token}" if token else ""))
        if resp.status_code != 200:
            raise DeployError(f"Listing {what} failed (HTTP {resp.status_code}): {_error_text(resp)}")
        data = resp.json()
        out += data.get("value", [])
        token = data.get("continuationToken")
        if not token:
            return out


def list_lakehouses(client: FabricClient, workspace: str) -> dict[str, str]:
    return {i["displayName"].lower(): i["id"] for i in _paged(client, f"{FABRIC_API}/workspaces/{workspace}/lakehouses", "lakehouses")}


def list_shortcuts(client: FabricClient, workspace: str, lakehouse: str) -> list[dict]:
    return _paged(client, f"{FABRIC_API}/workspaces/{workspace}/items/{lakehouse}/shortcuts", "shortcuts")


def onelake_path(workspace: str, lakehouse: str, shortcut_path: str, name: str) -> str:
    """The abfss path a notebook uses to read through the shortcut."""
    return f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/{shortcut_path.strip('/')}/{name}"


def _same_target(existing: dict, spec: ShortcutSpec) -> bool:
    t = (existing.get("target") or {}).get("adlsGen2") or {}
    return (str(t.get("location", "")).lower().rstrip("/") == spec.location.lower().rstrip("/")
            and str(t.get("subpath", "")).rstrip("/") == spec.subpath.rstrip("/")
            and str(t.get("connectionId", "")).lower() == spec.connection_id.lower())


def create_lakehouse_shortcut(client: FabricClient, workspace: str, lakehouse_name: str, spec: ShortcutSpec,
                              *, apply: bool) -> ShortcutResult:
    """Make sure the lakehouse and the shortcut exist. Dry run unless ``apply``."""
    if not _GUID.match(workspace or ""):
        raise DeployError("The workspace id must be a GUID (Fabric workspace settings > About).")
    if not _NAME.match(lakehouse_name or ""):
        raise DeployError("The lakehouse name may use letters, digits, space, _ . - (1..128 characters).")
    validate_spec(spec)
    lakehouses = list_lakehouses(client, workspace)
    lh_id = lakehouses.get(lakehouse_name.lower())
    lh_action = "exists" if lh_id else "create"
    if lh_id is None and apply:
        resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/lakehouses", {"displayName": lakehouse_name})
        body = client._finish(resp, f"Creating lakehouse '{lakehouse_name}'") or {}
        lh_id = body.get("id") if isinstance(body.get("id"), str) else list_lakehouses(client, workspace).get(lakehouse_name.lower())
        if not lh_id:
            raise DeployError(f"Lakehouse '{lakehouse_name}' was created but its id could not be read; check the workspace.")
    result = ShortcutResult(lakehouse_name, lh_action, lh_id, spec.name, "create")
    if lh_id is None:  # dry run, lakehouse not there yet: nothing to compare against
        result.detail = "dry run: the lakehouse and the shortcut would be created"
        return result
    result.onelake_path = onelake_path(workspace, lh_id, spec.path, spec.name)
    here = [s for s in list_shortcuts(client, workspace, lh_id)
            if s.get("name", "").lower() == spec.name.lower() and str(s.get("path", "")).strip("/").lower() == spec.path.strip("/").lower()]
    if here:
        if _same_target(here[0], spec):
            result.shortcut_action, result.detail = "exists", "an identical shortcut already exists; left unchanged"
        else:
            result.shortcut_action = "differs"
            result.detail = "a shortcut with this name and path points somewhere else; it was NOT replaced"
        return result
    if not apply:
        result.detail = "dry run: nothing changed"
        return result
    body = {"name": spec.name, "path": spec.path,
            "target": {"adlsGen2": {"location": spec.location, "subpath": spec.subpath, "connectionId": spec.connection_id}}}
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/items/{lh_id}/shortcuts", body)
    if resp.status_code not in (200, 201):
        raise DeployError(f"Creating the shortcut failed (HTTP {resp.status_code}): {_error_text(resp)}")
    result.detail = "created"
    return result
