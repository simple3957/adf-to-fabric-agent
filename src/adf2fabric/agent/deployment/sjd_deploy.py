"""Build and deploy a Fabric Spark Job Definition (SJD).

Fabric REST (verified against Microsoft Learn, 2026-10-22):

* ``POST /v1/workspaces/{ws}/items`` with ``{displayName, type: "SparkJobDefinition", definition}`` creates one;
  ``POST /v1/workspaces/{ws}/items/{id}/updateDefinition`` replaces its definition; ``GET .../items?type=SparkJobDefinition`` lists.
* The definition is format ``SparkJobDefinitionV1`` with one part, ``SparkJobDefinitionV1.json``, holding ``executableFile``,
  ``mainClass``, ``commandLineArguments``, ``defaultLakehouseArtifactId``, ``additionalLakehouseIds``, ``additionalLibraryUris``,
  ``language``, ``environmentArtifactId``, ``retryPolicy``. V1 only stores references: the main file and libraries must already
  be in OneLake/ADLS (``abfss://``). Every Fabric SJD needs at least one lakehouse. ``language`` is documented with "Python" and
  "Scala" examples; no value for R is documented, so none is generated.

Nothing is deleted. Without ``apply`` this only reads.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Optional

from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, DeployError, FabricClient, _GUID, _error_text

SJD_PART = "SparkJobDefinitionV1.json"
LANGUAGES = {"python": "Python", "scala": "Scala", "java": "Scala"}


@dataclass
class SjdResult:
    name: str
    action: str  # create | update
    item_id: Optional[str] = None
    detail: str = ""


def build_properties(*, language: str, executable_file: str, main_class: Optional[str], arguments: list[str],
                     lakehouse_id: Optional[str], library_uris: list[str], environment_id: Optional[str] = None) -> dict:
    """The decoded ``SparkJobDefinitionV1.json``. Raises DeployError for what Fabric would reject, instead of sending it."""
    lang = LANGUAGES.get((language or "").lower())
    if lang is None:
        raise DeployError(f"No documented Fabric language value for '{language}'; only Python and Scala/Java are supported here.")
    if not executable_file.startswith(("abfss://", "https://")):
        raise DeployError("The main file must already be in OneLake/ADLS (an abfss:// URI): a Spark Job Definition only stores a reference.")
    if lang == "Scala" and not main_class:
        raise DeployError("A Scala/Java job needs its main class.")
    if lang == "Python" and not executable_file.lower().endswith(".py"):
        raise DeployError("A Python job's main file must be a .py file.")
    if any(any(ch.isspace() for ch in a) for a in arguments):
        raise DeployError("An argument contains whitespace; Fabric takes one command-line string, so it cannot be passed unambiguously.")
    if lakehouse_id is not None and not _GUID.match(lakehouse_id):
        raise DeployError("The lakehouse id must be a GUID.")
    return {"executableFile": executable_file, "defaultLakehouseArtifactId": lakehouse_id or "", "mainClass": main_class or "",
            "additionalLakehouseIds": [], "retryPolicy": None, "commandLineArguments": " ".join(arguments),
            "additionalLibraryUris": list(library_uris), "language": lang, "environmentArtifactId": environment_id}


def build_definition(properties: dict) -> dict:
    raw = json.dumps(properties, ensure_ascii=False).encode("utf-8")
    return {"format": "SparkJobDefinitionV1",
            "parts": [{"path": SJD_PART, "payload": base64.b64encode(raw).decode("ascii"), "payloadType": "InlineBase64"}]}


def list_spark_job_definitions(client: FabricClient, workspace: str) -> dict[str, str]:
    found: dict[str, str] = {}
    url = f"{FABRIC_API}/workspaces/{workspace}/items?type=SparkJobDefinition"
    token: Optional[str] = None
    while True:
        resp = client._call("GET", url + (f"&continuationToken={token}" if token else ""))
        if resp.status_code != 200:
            raise DeployError(f"Listing Spark job definitions failed (HTTP {resp.status_code}): {_error_text(resp)}")
        data = resp.json()
        for it in data.get("value", []):
            found[it["displayName"].lower()] = it["id"]
        token = data.get("continuationToken")
        if not token:
            return found


def deploy_spark_job_definition(client: FabricClient, workspace: str, name: str, definition: dict, *, apply: bool) -> SjdResult:
    if not _GUID.match(workspace or ""):
        raise DeployError("The workspace id must be a GUID.")
    if not name or len(name) > 256:
        raise DeployError("The Spark job definition name must be 1..256 characters.")
    existing = list_spark_job_definitions(client, workspace)
    item_id = existing.get(name.lower())
    action = "update" if item_id else "create"
    if not apply:
        return SjdResult(name, action, item_id, "dry run: nothing changed")
    if item_id:
        resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/items/{item_id}/updateDefinition", {"definition": definition})
        client._finish(resp, f"Updating Spark job definition '{name}'")
        return SjdResult(name, action, item_id, "definition replaced")
    resp = client._call("POST", f"{FABRIC_API}/workspaces/{workspace}/items",
                        {"displayName": name, "type": "SparkJobDefinition", "definition": definition})
    body = client._finish(resp, f"Creating Spark job definition '{name}'") or {}
    item_id = body.get("id") if isinstance(body.get("id"), str) else list_spark_job_definitions(client, workspace).get(name.lower())
    if not item_id:
        raise DeployError(f"Spark job definition '{name}' was created but its id could not be read; check the workspace.")
    return SjdResult(name, action, item_id, "created")
