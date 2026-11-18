"""Deploy the Activator (Reflex) rules generated from ADF storage event triggers (agent/conversion/event_triggers.py).

A ``onelake`` plan becomes one Activator item per trigger (Create Item, type Reflex, part ``ReflexEntities.json``; shape checked live on
2026-11-18). Dry run unless ``apply``. An Activator with the same name is never modified: it is reported as ``exists``. Its rules are
created switched off unless ``enable`` and the ADF trigger was started. An ``azure_blob`` plan is not created (its source definition
was never read); it is reported as ``manual`` with read-only readiness checks:

* the storage account's subscription is in the Fabric workspace's Entra tenant (the Fabric wizard lists no other);
* the account kind is StorageV2, BlockBlobStorage or BlobStorage (Microsoft: general-purpose v1 is not supported);
* the ``Microsoft.EventGrid`` resource provider is registered (the wizard creates an Event Grid subscription);
* the workspace's capacity is not in a region where Azure Blob Storage events are unavailable.

A read that fails is ``unknown``, never a pass.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from adf2fabric.agent.conversion import event_triggers as ET
from adf2fabric.agent.deployment.fabric_deploy import FABRIC_API, DeployError, FabricClient

_KINDS = {"storagev2", "blockblobstorage", "blobstorage"}


@dataclass
class ActivatorResult:
    trigger: str
    action: str  # create | exists | skipped | manual | failed
    item_id: Optional[str] = None
    detail: str = ""
    checks: list[tuple[str, str, str]] = field(default_factory=list)  # (check, ok | fail | unknown, detail)


def plan_from_file(data: dict) -> ET.EventTriggerPlan:
    keys = ET.EventTriggerPlan.__dataclass_fields__
    return ET.EventTriggerPlan(**{k: v for k, v in data.items() if k in keys})


def deploy_activators(client: FabricClient, workspace: str, plans: list[ET.EventTriggerPlan], *, tenant_id: Optional[str], apply: bool,
                      enable: bool = False, arm: Any = None) -> list[ActivatorResult]:
    """``arm``: an ArmGateway for the readiness checks of ``azure_blob`` plans (None: those checks are ``unknown``)."""
    results: list[ActivatorResult] = []
    onelake = [p for p in plans if p.target == "onelake"]
    existing = client.list_items(workspace, "Reflex") if onelake else {}
    lakehouses = client.list_items(workspace, "Lakehouse") if onelake else {}
    pipelines = client.list_pipelines(workspace) if onelake else {}
    for p in plans:
        if p.target == "azure_blob":
            results.append(ActivatorResult(p.trigger, "manual", detail="create it in the Fabric Trigger wizard (steps in the conversion report)",
                                           checks=azure_readiness(client, workspace, p, tenant_id, arm)))
            continue
        if p.target == "custom_event":
            results.append(ActivatorResult(p.trigger, "manual", detail="an Eventstream and an Activator rule created by a person (steps in the "
                                                                        "conversion report); nothing is created by the tool"))
            continue
        if p.target != "onelake":
            results.append(ActivatorResult(p.trigger, "skipped", detail=" ".join(p.notes) or "no Fabric form"))
            continue
        name = ET.item_name(p)
        if name.lower() in existing:
            results.append(ActivatorResult(p.trigger, "exists", existing[name.lower()], "an Activator with this name exists; left unchanged"))
            continue
        lh = lakehouses.get((p.lakehouse or "").lower())
        missing = [n for n in p.pipelines if n.lower() not in pipelines]
        problems = ([f"lakehouse '{p.lakehouse}' is not in the workspace"] if not lh else []) + (
            [f"pipeline(s) not in the workspace: {', '.join(missing)}"] if missing else []) + ([] if tenant_id else ["the tenant id could not be read"])
        if problems:
            results.append(ActivatorResult(p.trigger, "skipped" if apply else "create", detail=("not created: " if apply else "dry run, but: ")
                                           + "; ".join(problems)))
            continue
        try:
            entities = ET.reflex_entities(p, tenant_id=tenant_id or "", workspace_id=workspace, lakehouse_id=lh or "", pipeline_ids=pipelines,
                                          enable=enable)
        except ValueError as exc:
            results.append(ActivatorResult(p.trigger, "failed", detail=str(exc)))
            continue
        on = ET.rules_running(entities)
        state = f"rule(s) switched on: {', '.join(on)}" if on else "rules switched off"
        if not apply:
            results.append(ActivatorResult(p.trigger, "create", detail=f"dry run: would create Activator '{name}' ({state})"))
            continue
        try:
            client.create_item(workspace, name, "Reflex", ET.definition_parts(entities))
            item_id = client.list_items(workspace, "Reflex").get(name.lower())
            existing[name.lower()] = item_id or ""
            results.append(ActivatorResult(p.trigger, "create", item_id, f"created Activator '{name}' ({state})"))
        except DeployError as exc:
            results.append(ActivatorResult(p.trigger, "failed", detail=str(exc)))
    return results


def _arm_read(arm: Any, path: str) -> tuple[Optional[dict], str]:
    try:
        r = arm.request("GET", path)
    except Exception as exc:  # noqa: BLE001 - reported as unknown, never as a pass
        return None, f"could not be read ({type(exc).__name__})"
    if r.status != 200 or not isinstance(r.body, dict):
        return None, {403: "permission denied", 404: "not found with this sign-in (it may be in another tenant)"}.get(r.status, f"HTTP {r.status}")
    return r.body, ""


def azure_readiness(client: FabricClient, workspace: str, p: ET.EventTriggerPlan, fabric_tenant: Optional[str], arm: Any) -> list[tuple[str, str, str]]:
    """Read-only checks for a trigger that keeps watching Azure storage (see the module docstring)."""
    out: list[tuple[str, str, str]] = []
    m = re.search(r"/subscriptions/([^/]+)", p.scope or "", re.I)
    acct = re.search(r"/storageAccounts/([^/]+)", p.scope or "", re.I)
    sub = m.group(1) if m else None
    if arm is None or sub is None:
        out.append(("same tenant as Fabric", "unknown", "no Azure sign-in for this check" if arm is None else "the trigger scope names no subscription"))
    else:
        body, why = _arm_read(arm, f"subscriptions/{sub}?api-version=2022-12-01")
        if body is None:
            out.append(("same tenant as Fabric", "unknown", f"the storage subscription {why}"))
        else:
            tid = str(body.get("tenantId") or "")
            ok = bool(fabric_tenant) and tid.lower() == fabric_tenant.lower()
            out.append(("same tenant as Fabric", "ok" if ok else ("fail" if fabric_tenant and tid else "unknown"),
                        "the storage subscription is in the workspace's tenant" if ok else
                        "the storage subscription is in another tenant: Fabric's wizard cannot list it"))
        prov, why = _arm_read(arm, f"subscriptions/{sub}/providers/Microsoft.EventGrid?api-version=2021-04-01")
        state = str((prov or {}).get("registrationState") or "")
        out.append(("Microsoft.EventGrid registered", "ok" if state.lower() == "registered" else ("unknown" if prov is None else "fail"),
                    state or why))
        if acct:
            found, why = _arm_read(arm, f"subscriptions/{sub}/providers/Microsoft.Storage/storageAccounts?api-version=2023-05-01")
            row = next((a for a in (found or {}).get("value", []) if str(a.get("name", "")).lower() == acct.group(1).lower()), None)
            kind = str((row or {}).get("kind") or "")
            out.append(("storage account kind", "ok" if kind.lower() in _KINDS else ("unknown" if row is None else "fail"),
                        kind or why or "the account was not listed"))
    out.append(_capacity_region(client, workspace))
    return out


def _capacity_region(client: FabricClient, workspace: str) -> tuple[str, str, str]:
    check = "capacity region supports Azure Blob events"
    try:
        ws = client._call("GET", f"{FABRIC_API}/workspaces/{workspace}")
        cap_id = (ws.json() or {}).get("capacityId") if ws.status_code == 200 else None
        caps = client._call("GET", f"{FABRIC_API}/capacities")
        rows = (caps.json() or {}).get("value", []) if caps.status_code == 200 else []
    except Exception as exc:  # noqa: BLE001
        return check, "unknown", f"could not be read ({type(exc).__name__})"
    region = next((str(c.get("region") or "") for c in rows if str(c.get("id", "")).lower() == str(cap_id or "").lower()), "")
    if not region:
        return check, "unknown", "the workspace's capacity region could not be read"
    bad = region.replace(" ", "").lower() in {r.replace(" ", "").lower() for r in ET.UNSUPPORTED_CAPACITY_REGIONS}
    return check, "fail" if bad else "ok", region
