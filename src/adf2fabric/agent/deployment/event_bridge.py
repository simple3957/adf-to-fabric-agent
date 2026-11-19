"""Write the Event Grid -> Logic App -> Fabric bridge of each ADF event trigger for one workspace (agent/conversion/event_triggers.py).

Files only: ``deployment/event-bridges/<workspace>/<trigger>/template.json`` and ``parameters.json``, plus the ``az`` command to deploy
them. Nothing is created in Azure or Fabric: the Event Grid subscription and the Logic App belong in the client's Azure subscription
(the storage account's or topic's resource group) and a person deploys them. Pipeline ids are read from the workspace (read-only);
a pipeline that is not there yet is reported and its bridge is not written.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from adf2fabric.agent.conversion import event_triggers as ET


@dataclass
class BridgeResult:
    trigger: str
    action: str  # written | skipped | manual
    detail: str = ""
    folder: Optional[str] = None


def pipeline_param_types(conversion_dir: Path, pipelines: list[str]) -> dict[str, dict[str, str]]:
    """Pipeline (lower) -> {parameter: type}, from the converted pipeline definitions."""
    out: dict[str, dict[str, str]] = {}
    for name in pipelines:
        f = conversion_dir / "pipelines" / f"{name}.DataPipeline" / "pipeline-content.json"
        try:
            props = json.loads(f.read_text(encoding="utf-8")).get("properties") or {}
        except (OSError, ValueError):
            continue
        out[name.lower()] = {k: str((v or {}).get("type") or "") for k, v in (props.get("parameters") or {}).items()}
    return out


def write_bridges(out_dir: Path, conversion_dir: Path, plans: list[ET.EventBridgePlan], *, workspace_id: str,
                  pipeline_ids: dict[str, str], identity: str, tenant_id: Optional[str], client_id: Optional[str]) -> list[BridgeResult]:
    results: list[BridgeResult] = []
    for p in plans:
        if p.status not in ("generated", "generated_with_differences"):
            results.append(BridgeResult(p.trigger, "manual", " ".join(p.notes) or "no generated bridge"))
            continue
        try:
            types = pipeline_param_types(conversion_dir, [pl["name"] for pl in p.pipelines])
            template, params = ET.arm_template(p, workspace_id=workspace_id, pipeline_ids=pipeline_ids, param_types=types, identity=identity,
                                               tenant_id=tenant_id, client_id=client_id)
        except ValueError as exc:
            results.append(BridgeResult(p.trigger, "skipped", str(exc)))
            continue
        folder = out_dir / workspace_id / re.sub(r"[^A-Za-z0-9._-]+", "_", p.trigger)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "template.json").write_text(json.dumps(template, indent=2) + "\n", encoding="utf-8")
        (folder / "parameters.json").write_text(json.dumps(params, indent=2) + "\n", encoding="utf-8")
        rg = re.search(r"/resourceGroups/([^/]+)", p.scope or "", re.I)
        sub = re.search(r"/subscriptions/([^/]+)", p.scope or "", re.I)
        cmd = (f"az deployment group create --subscription {sub.group(1) if sub else '<subscription>'} "
               f"--resource-group {rg.group(1) if rg else '<resource group>'} --template-file template.json --parameters @parameters.json")
        (folder / "README.md").write_text("\n".join([
            f"# Event bridge for ADF trigger `{p.trigger}`", "",
            "Event Grid subscription (the ADF trigger's filters) -> Logic App -> Fabric Run On Demand Item Job.", "",
            "1. " + ("Fill the Key Vault reference for `fabricClientSecret` in parameters.json." if identity == "service_principal"
                     else "No secret: the Logic App signs in with its managed identity."),
            f"2. Deploy (from this folder): `{cmd}`",
            "3. Give the " + ("service principal" if identity == "service_principal" else "Logic App's managed identity (output logicAppPrincipalId)")
            + " Contributor access to the Fabric workspace.",
            "4. At cutover deploy again with `runPipelines=true`, and stop the ADF trigger.", "", *[f"- {n}" for n in p.notes], ""]),
            encoding="utf-8")
        results.append(BridgeResult(p.trigger, "written", f"runPipelines=false; deploy with: {cmd}", str(folder)))
    return results
