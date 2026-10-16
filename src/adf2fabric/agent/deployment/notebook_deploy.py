"""Create or update a Fabric notebook from a local file, and record its id so converted pipelines can bind to it.

Fabric REST (verified against Microsoft Learn, 2026-10-16):

* ``POST /v1/workspaces/{ws}/notebooks`` with ``{displayName, definition:{format, parts}}`` - 201, or 202 (long
  running operation). Needs a contributor role and a workspace on a Fabric capacity.
* ``POST /v1/workspaces/{ws}/notebooks/{id}/updateDefinition`` - 200, or 202.
* ``GET  /v1/workspaces/{ws}/notebooks`` - paged with ``continuationToken``.

The definition is sent in the ``ipynb`` format (a part named ``notebook-content.ipynb``). A plain ``.py`` file becomes
one code cell; an ``.ipynb`` file is sent as it is. Notebook *content* is never changed or generated here: whatever the
file holds is what runs, so a notebook migrated from Synapse still needs its own storage paths and lakehouse attached.

Nothing is ever deleted. Without ``apply`` this only reads.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, DeployError, FabricClient, _GUID, _error_text

NOTEBOOK_PART = "notebook-content.ipynb"
_MAX_BYTES = 8 * 1024 * 1024


@dataclass
class NotebookResult:
    name: str
    action: str  # create | update
    item_id: Optional[str] = None
    detail: str = ""


def build_definition(path: Path) -> dict:
    """The ``definition`` object for a notebook file (``.py`` -> one code cell, ``.ipynb`` -> as is)."""
    path = Path(path)
    if not path.is_file():
        raise DeployError(f"Notebook file not found: {path}")
    if path.stat().st_size > _MAX_BYTES:
        raise DeployError("The notebook file is larger than 8 MB.")
    suffix = path.suffix.lower()
    if suffix == ".ipynb":
        try:
            nb = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeployError(f"The notebook is not valid JSON ({type(exc).__name__}).") from None
        if not isinstance(nb, dict) or not isinstance(nb.get("cells"), list):
            raise DeployError("The .ipynb file has no 'cells' list.")
    elif suffix == ".py":
        try:
            code = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise DeployError(f"The notebook file cannot be read ({type(exc).__name__}).") from None
        nb = {
            "nbformat": 4, "nbformat_minor": 5,
            "cells": [{"cell_type": "code", "source": code.splitlines(keepends=True), "execution_count": None,
                       "outputs": [], "metadata": {}}],
            "metadata": {"language_info": {"name": "python"}},
        }
    else:
        raise DeployError(f"Only .py and .ipynb notebook files are supported here, not '{suffix or 'no extension'}'.")
    raw = json.dumps(nb, ensure_ascii=False).encode("utf-8")
    return {"format": "ipynb", "parts": [{"path": NOTEBOOK_PART, "payload": base64.b64encode(raw).decode("ascii"),
                                          "payloadType": "InlineBase64"}]}


def list_notebooks(client: FabricClient, workspace: str) -> dict[str, str]:
    """Display name (lowercased) -> item id for every notebook in the workspace."""
    found: dict[str, str] = {}
    url = f"{FABRIC_API}/workspaces/{workspace}/notebooks"
    token: Optional[str] = None
    while True:
        resp = client._call("GET", url + (f"?continuationToken={token}" if token else ""))
        if resp.status_code != 200:
            raise DeployError(f"Listing notebooks failed (HTTP {resp.status_code}): {_error_text(resp)}")
        data = resp.json()
        for it in data.get("value", []):
            found[it["displayName"].lower()] = it["id"]
        token = data.get("continuationToken")
        if not token:
            return found


def deploy_notebook(client: FabricClient, workspace: str, name: str, definition: dict, *, apply: bool) -> NotebookResult:
    """Create the notebook, or replace its content when one with this name exists. Dry run unless ``apply``."""
    if not _GUID.match(workspace or ""):
        raise DeployError("The workspace id must be a GUID (Fabric workspace settings > About).")
    if not name or len(name) > 256:
        raise DeployError("The notebook name must be 1..256 characters.")
    existing = list_notebooks(client, workspace)
    item_id = existing.get(name.lower())
    action = "update" if item_id else "create"
    if not apply:
        return NotebookResult(name, action, item_id, "dry run: nothing changed")
    if item_id:
        resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/notebooks/{item_id}/updateDefinition",
                            {"definition": definition})
        client._finish(resp, f"Updating notebook '{name}'")
        return NotebookResult(name, action, item_id, "content replaced")
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/notebooks",
                        {"displayName": name, "definition": definition})
    body = client._finish(resp, f"Creating notebook '{name}'") or {}
    item_id = body.get("id") if isinstance(body.get("id"), str) else list_notebooks(client, workspace).get(name.lower())
    if not item_id:
        raise DeployError(f"Notebook '{name}' was created but its id could not be read; check the workspace.")
    return NotebookResult(name, action, item_id, "created")


def bind_resolutions(path: Path, source_name: str, item_id: str, workspace: str) -> list[str]:
    """Upsert ``ItemToId notebook:<source_name>`` and ``WorkspaceId *`` in a resolutions file, keeping every other entry.

    Returns what changed, for display. The file is created if it does not exist.
    """
    path = Path(path)
    entries: list[Any] = []
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DeployError(f"The resolutions file cannot be read ({type(exc).__name__}); not changed.") from None
        if not isinstance(loaded, list):
            raise DeployError("The resolutions file is not a list; not changed.")
        entries = loaded
    changes: list[str] = []

    def upsert(kind: str, key: str, value: str) -> None:
        for e in entries:
            if isinstance(e, dict) and e.get("type") == kind and str(e.get("key", "")).lower() == key.lower():
                if e.get("value") != value:
                    e["value"] = value
                    changes.append(f"{kind} {key}: updated")
                return
        entries.append({"type": kind, "key": key, "value": value})
        changes.append(f"{kind} {key}: added")

    upsert("ItemToId", f"notebook:{source_name}", item_id)
    upsert("WorkspaceId", "*", workspace)
    if changes:
        path.write_text(json.dumps(entries, indent=2), encoding="utf-8")
    return changes
