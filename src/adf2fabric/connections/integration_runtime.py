"""Integration runtime + private connectivity analysis for one linked service.

For every linked service this resolves ``connectVia``, identifies the runtime, collects EVIDENCE about whether the endpoint is
public or private, and recommends a Fabric connectivity path with the prerequisites it implies. Rules it follows:

* evidence, not assumption: an unknown stays ``unknown`` and appears under ``missing_info``; no server, network or gateway value
  is ever invented;
* an ADF self-hosted runtime is NOT reusable by Fabric: the equivalent is an on-premises data gateway on a machine that can
  reach the source, and credentials stored on the runtime's nodes cannot be exported (nothing secret is read or copied here);
* ADF managed virtual networks / managed private endpoints have no direct Fabric equivalent (Microsoft lists them as "to be
  determined"); the supported path is a virtual network data gateway in a virtual network that can reach the resource;
* Azure-SSIS runtimes have no Fabric equivalent;
* gateway connections cannot be created through the Fabric API by this tool (their credentials need the gateway's public key),
  so those paths end in "create the connection in Fabric, then reuse it".

Sources: Microsoft's Fabric vs ADF comparison (runtime and private-networking rows) and the Fabric "List Gateways" API.
"""

from __future__ import annotations

import ipaddress
from typing import Any, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field

from adf2fabric.project.discovery import AdfIntegrationRuntime, AdfLinkedService

AUTO_RESOLVE = "autoresolveintegrationruntime"
_PRIVATE_SUFFIXES = (".local", ".internal", ".corp", ".lan", ".intranet", ".home", ".localdomain", ".private")
_PUBLIC_AZURE_SUFFIXES = (
    ".database.windows.net", ".sql.azuresynapse.net", ".dev.azuresynapse.net", ".azuresynapse.net", ".dfs.core.windows.net",
    ".blob.core.windows.net", ".file.core.windows.net", ".table.core.windows.net", ".queue.core.windows.net",
    ".vault.azure.net", ".azuredatabricks.net", ".azurewebsites.net", ".documents.azure.com", ".database.azure.com",
    ".mysql.database.azure.com", ".postgres.database.azure.com", ".servicebus.windows.net")


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RuntimeAnalysis(_M):
    linked_service: str
    runtime_name: Optional[str] = None  # None = no connectVia (the implicit Azure runtime)
    runtime_kind: str  # azure | managed_vnet | self_hosted | ssis | unresolved | unclassified
    runtime_type: Optional[str] = None  # Managed | SelfHosted, as authored
    resolved: bool = True  # the runtime definition was found in the source (or none was needed)
    managed_virtual_network: Optional[str] = None
    shared_from: Optional[str] = None
    host: Optional[str] = None
    exposure: str = "unknown"  # public | public_restricted | private | unknown
    evidence: list[str] = Field(default_factory=list)
    fabric_path: str  # cloud_connection | on_premises_gateway | virtual_network_gateway | unsupported | manual
    api_creatable: bool = True  # False when the connection must be created in the Fabric UI
    recommendation: str = ""
    requirements: list[str] = Field(default_factory=list)  # network / identity / permission / runtime changes
    missing_info: list[str] = Field(default_factory=list)
    gateway_candidates: list[dict[str, str]] = Field(default_factory=list)  # existing Fabric gateways that fit
    warnings: list[str] = Field(default_factory=list)


def host_of(ls: AdfLinkedService) -> Optional[str]:
    """The data store host a linked service addresses, from explicitly authored properties only."""
    raw = ls.endpoint or ls.connection.server
    if not raw or "@{" in raw or raw.startswith("@"):
        return None  # dynamic content: no single host
    raw = raw.strip()
    if "://" not in raw:
        raw = f"//{raw}"
    h = urlparse(raw).hostname
    return h.lower() if h else None


def classify_host(host: Optional[str]) -> tuple[str, list[str]]:
    """``(exposure, evidence)`` from the host name alone."""
    if not host:
        return "unknown", ["The linked service gives no single host name (dynamic, parameterized, or a connection string)."]
    try:
        ip = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        ip = None
    if ip is not None:
        if ip.is_private or ip.is_loopback or ip.is_link_local:
            return "private", [f"{host} is a private-range IP address."]
        return "public", [f"{host} is a public IP address."]
    if "privatelink." in host:
        return "private", [f"{host} is a Private Link DNS name."]
    if "." not in host:
        return "private", [f"'{host}' is a single-label name, which only resolves inside a network."]
    if host.endswith(_PRIVATE_SUFFIXES):
        return "private", [f"{host} ends with a private-network suffix."]
    if host.endswith(_PUBLIC_AZURE_SUFFIXES):
        return "public", [f"{host} is a public Azure endpoint name (its firewall and private endpoints were not checked)."]
    return "unknown", [f"{host} is not a recognised public Azure endpoint, so whether it is reachable from the internet is unknown."]


def apply_network_hint(exposure: str, evidence: list[str], hint: Optional[dict[str, Any]]) -> tuple[str, list[str]]:
    """Refine the exposure with what Azure Resource Manager says about the resource (``public_network_access``,
    ``default_action``, ``private_endpoints``). Pure function; the caller fetches the hint with the user's own sign-in."""
    if not hint:
        return exposure, evidence
    out = list(evidence)
    pna = str(hint.get("public_network_access") or "").lower()
    action = str(hint.get("default_action") or "").lower()
    pe = hint.get("private_endpoints")
    if pna == "disabled":
        out.append("Azure reports public network access is DISABLED on the resource.")
        exposure = "private"
    elif pna in ("enabled", "") and action == "deny":
        out.append("Azure reports public access is enabled but the firewall default action is Deny (selected networks only).")
        exposure = "public_restricted"
    elif pna == "enabled":
        out.append("Azure reports public network access is enabled" + (" with no default-deny firewall." if action == "allow" else "."))
        exposure = "public"
    if isinstance(pe, int) and pe > 0:
        out.append(f"The resource has {pe} private endpoint connection(s).")
    return exposure, out


def analyze(ls: AdfLinkedService, runtimes: dict[str, AdfIntegrationRuntime],
            gateways: Optional[list[dict[str, Any]]] = None, network_hint: Optional[dict[str, Any]] = None) -> RuntimeAnalysis:
    name = ls.integration_runtime
    host = host_of(ls)
    exposure, evidence = apply_network_hint(*classify_host(host), network_hint)
    a = RuntimeAnalysis(linked_service=ls.name, runtime_name=name, runtime_kind="azure", host=host, exposure=exposure,
                        evidence=evidence, fabric_path="cloud_connection")
    gws = gateways or []
    on_prem = [{"id": str(g.get("id")), "name": str(g.get("displayName") or "(unnamed)"), "type": str(g.get("type"))}
               for g in gws if g.get("type") == "OnPremises"]
    vnet = [{"id": str(g.get("id")), "name": str(g.get("displayName") or "(unnamed)"), "type": str(g.get("type"))}
            for g in gws if g.get("type") == "VirtualNetwork"]

    rt = runtimes.get((name or "").lower()) if name else None
    if name and name.lower() != AUTO_RESOLVE and rt is None:
        a.runtime_kind, a.resolved, a.fabric_path = "unresolved", False, "manual"
        a.missing_info.append(f"The integration runtime '{name}' is referenced but its definition is not in the source; export "
                              f"integrationRuntime/{name}.json (or re-run the import with runtimes included).")
        a.recommendation = "The runtime type is unknown, so no Fabric path can be recommended yet."
        return a
    if rt is not None:
        a.runtime_type, a.managed_virtual_network, a.shared_from = rt.type, rt.managed_virtual_network, rt.shared_from
        a.runtime_kind = rt.kind or ("ssis" if rt.is_ssis else "unclassified")

    kind = a.runtime_kind
    if kind == "ssis":
        a.fabric_path = "unsupported"
        a.recommendation = ("An Azure-SSIS runtime has no Fabric equivalent. Keep the SSIS packages in ADF or re-implement them "
                            "with Copy, Dataflow Gen2 or notebooks.")
        return a
    if kind == "unclassified":
        a.fabric_path = "manual"
        a.missing_info.append(f"The runtime type '{rt.type if rt else None}' is not one this tool classifies.")
        a.recommendation = "Review the runtime by hand."
        return a

    if kind == "self_hosted":
        a.fabric_path, a.api_creatable = "on_premises_gateway", False
        a.gateway_candidates = on_prem
        a.recommendation = ("Use an on-premises data gateway. An ADF self-hosted runtime cannot be reused by Fabric; the gateway "
                            "must run on a machine that can reach the data source.")
        a.requirements += ["Install an on-premises data gateway on a machine with network access to the source, and register "
                           "it with a Microsoft Entra account that can use Fabric.",
                           "Create the Fabric connection against that gateway (Manage connections and gateways), then reuse it here.",
                           "Credentials stored on the runtime's nodes cannot be exported; re-enter them on the Fabric connection."]
        if a.shared_from:
            a.warnings.append("This runtime is shared from another factory; the machine that hosts it is outside this source.")
        if not on_prem:
            a.missing_info.append("No on-premises data gateway is visible to the signed-in Fabric account.")
        if exposure == "public":
            a.warnings.append("The endpoint looks publicly reachable; a cloud connection may work without a gateway if the "
                              "source firewall allows Fabric.")
        return a

    if kind == "managed_vnet":
        a.fabric_path, a.api_creatable = "virtual_network_gateway", False
        a.gateway_candidates = vnet
        a.recommendation = ("Use a virtual network data gateway in a virtual network that can reach the resource. ADF managed "
                            "virtual networks and managed private endpoints have no direct Fabric equivalent yet.")
        a.requirements += ["Create a virtual network data gateway in an Azure virtual network with a delegated subnet and "
                           "network line of sight (for example a private endpoint) to the data source.",
                           "Recreate the private endpoint approval on the target resource; managed private endpoints in ADF are "
                           "not migrated.",
                           "Create the Fabric connection against that gateway, then reuse it here."]
        if a.managed_virtual_network:
            a.evidence.append(f"The runtime is attached to the managed virtual network '{a.managed_virtual_network}'.")
        if not vnet:
            a.missing_info.append("No virtual network data gateway is visible to the signed-in Fabric account.")
        return a

    # Azure runtime (or the implicit auto-resolve runtime)
    if exposure == "private":
        a.fabric_path, a.api_creatable = "virtual_network_gateway", False
        a.gateway_candidates = vnet or on_prem
        a.recommendation = ("The Azure runtime reaches this endpoint, but the evidence says it is private; a cloud connection "
                            "will not reach it. Use a virtual network data gateway (Azure private endpoint) or an on-premises "
                            "data gateway.")
        a.requirements += ["Provide a network path from Fabric to the endpoint through a gateway, then create the connection "
                           "against it in Fabric and reuse it here."]
        a.warnings.append("An Azure integration runtime cannot reach a private address on its own; check how this linked service "
                          "worked in ADF (a managed private endpoint, a trusted-services exception, or an incorrect runtime).")
    elif exposure == "public_restricted":
        a.recommendation = ("A cloud connection can work, but the firewall restricts public access: allow Fabric's outbound "
                            "addresses, or use a virtual network data gateway.")
        a.requirements.append("Add a firewall rule or network exception for Fabric (see Microsoft's Fabric network documentation), "
                              "or use a gateway.")
    elif exposure == "public":
        a.recommendation = "A Fabric cloud connection (no gateway) is the equivalent of the Azure runtime."
    else:
        a.recommendation = ("A Fabric cloud connection is the equivalent of the Azure runtime if the endpoint is reachable from the "
                            "internet; whether it is could not be determined.")
        a.missing_info.append("Confirm whether the data source accepts connections from the internet or needs a gateway.")
    return a
