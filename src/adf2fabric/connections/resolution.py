"""Linked services -> Fabric connection plan.

``build_cards`` turns the semantic record into one card per linked service carrying everything the credential screen shows:
usage, endpoint metadata, parameters and their possible values, the integration runtime analysis, existing Fabric connections
that match, the authentication options this connector and the target tenant actually support, the state, and what is missing.
It never invents a server, database or container: a value is either authored in the source, derived from an authored URL, offered
as one of several candidates found in the pipelines, or reported missing.

``ConnectionChoice`` is the user's answer for one linked service. It holds NO secret: secret fields live in the in-memory vault and
Key Vault references are only an address (vault URL + secret name).
"""

from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field

from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.connections import catalog
from adf2fabric.connections.integration_runtime import RuntimeAnalysis, analyze
from adf2fabric.connections.models import ConnectionState
from adf2fabric.project.discovery import AdfLinkedService, AdfSemanticDiscovery


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ConnectionChoice(_M):
    """A user's (or an automatic) decision for one linked service. Safe to persist: it holds no secret value."""

    linked_service: str
    method: str  # an AuthMethod id of the connector
    endpoint: dict[str, str] = Field(default_factory=dict)  # field name -> value (server, database, account, container, url ...)
    values: dict[str, str] = Field(default_factory=dict)  # non-secret method fields (username, tenant_id, vault_url, ...)
    display_name: Optional[str] = None  # the Fabric connection name to create
    privacy_level: str = "Organizational"
    origin: str = "user"  # user | auto_matched
    variants: list[dict[str, str]] = Field(default_factory=list)  # extra endpoint values for a parameterized linked service


class EndpointField(_M):
    name: str
    label: str
    value: Optional[str] = None
    source: str = "missing"  # source | derived | candidate | choice | missing
    candidates: list[str] = Field(default_factory=list)
    required: bool = True


class ExistingMatch(_M):
    id: str
    display_name: str
    match: str  # exact | partial
    credential_type: Optional[str] = None
    reusable_with_method: Optional[bool] = None  # None = no method chosen yet
    note: str = ""


class LinkedServiceCard(_M):
    name: str
    adf_type: Optional[str] = None
    connector: Optional[str] = None
    connector_label: Optional[str] = None
    supported: bool = False
    fabric_connection_type: Optional[str] = None
    authentication_type: Optional[str] = None  # as authored in ADF
    user_name: Optional[str] = None
    key_vault_refs: list[dict[str, str]] = Field(default_factory=list)
    used_by_datasets: list[str] = Field(default_factory=list)
    used_by_activities: list[dict[str, str]] = Field(default_factory=list)
    parameters: list[dict[str, Any]] = Field(default_factory=list)
    parameterized: bool = False
    parameter_candidates: dict[str, list[str]] = Field(default_factory=dict)
    endpoint_fields: list[EndpointField] = Field(default_factory=list)
    runtime: Optional[RuntimeAnalysis] = None
    auth_options: list[dict[str, Any]] = Field(default_factory=list)
    existing_matches: list[ExistingMatch] = Field(default_factory=list)
    choice: Optional[ConnectionChoice] = None
    secrets_held: list[str] = Field(default_factory=list)  # field NAMES held in the vault, never values
    state: ConnectionState = ConnectionState.resource_discovered
    missing: list[str] = Field(default_factory=list)
    next_steps: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# ---- helpers ---------------------------------------------------------------------------------------------------------


def _account_from_host(host: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not host or "." not in host:
        return None, None
    account, _, domain = host.partition(".")
    return account, domain


def _literals(v: Any) -> list[str]:
    """Plain string values in an authored argument (an Expression yields nothing)."""
    if isinstance(v, str) and not v.startswith("@"):
        return [v]
    if isinstance(v, dict) and v.get("type") == "Expression":
        return []
    return []


def usage(d: AdfSemanticDiscovery, ls_name: str) -> tuple[list[str], list[dict[str, str]], dict[str, set[str]]]:
    """(datasets using it, activities using it, literal LS-parameter arguments seen in datasets and activity IO)."""
    low = ls_name.lower()
    ds_names, acts, args = [], [], {}
    ds_by = {x.name.lower(): x for x in d.datasets}
    for ds in d.datasets:
        if ds.linked_service_ref and ds.linked_service_ref.name.lower() == low:
            ds_names.append(ds.name)
            for k, v in ds.linked_service_arguments.items():
                for lit in _literals(v):
                    args.setdefault(k, set()).add(lit)
    used_ds = {n.lower() for n in ds_names}
    for p in d.pipelines:
        for a in walk(p.activities):
            io_ls = {io.linked_service.lower() for io in [*a.inputs, *a.outputs] if io.linked_service}
            io_ds = {(io.name or "").lower() for io in [*a.inputs, *a.outputs]}
            direct = {n.lower() for n in a.linked_service_refs}
            if low in io_ls or low in direct or io_ds & used_ds:
                acts.append({"pipeline": p.name, "activity": a.name, "type": a.type or ""})
    return sorted(ds_names), acts, args


def _containers(d: AdfSemanticDiscovery, ds_names: list[str]) -> list[str]:
    out: set[str] = set()
    low = {n.lower() for n in ds_names}
    for ds in d.datasets:
        if ds.name.lower() in low:
            loc = ds.type_properties.get("location") if isinstance(ds.type_properties, dict) else None
            fs = (loc or {}).get("fileSystem") or (loc or {}).get("container")
            if isinstance(fs, str) and not fs.startswith("@"):
                out.add(fs)
            elif isinstance(fs, dict) and isinstance(fs.get("value"), str):
                pass  # an expression: not a literal candidate
    return sorted(out)


def endpoint_fields(spec: Optional[catalog.ConnectorSpec], ls: AdfLinkedService, d: AdfSemanticDiscovery,
                    ds_names: list[str], choice: Optional[ConnectionChoice]) -> list[EndpointField]:
    if spec is None:
        return []
    from adf2fabric.connections.integration_runtime import host_of

    out: list[EndpointField] = []
    host = host_of(ls)
    account, domain = _account_from_host(host)
    for ep in spec.endpoint:
        f = EndpointField(name=ep.field.name, label=ep.field.label, required=ep.field.required)
        n = ep.field.name
        if spec.id == "sql":
            val = {"server": ls.connection.server, "database": ls.connection.database}.get(n)
            if val and "@{" not in val and not val.startswith("@"):
                f.value, f.source = val, "source"
        elif spec.id in ("adls_gen2", "blob"):
            if n == "account" and account:
                f.value, f.source = account, "derived"
            elif n == "domain" and domain:
                f.value, f.source = domain, "derived"
            elif n == "container":
                f.candidates = _containers(d, ds_names)
                if len(f.candidates) == 1:
                    f.value, f.source = f.candidates[0], "derived"
                elif f.candidates:
                    f.source = "candidate"
        elif spec.id in ("rest", "databricks") and ls.endpoint and "@" not in ls.endpoint:
            f.value, f.source = ls.endpoint.rstrip("/"), "source"
        if choice and (choice.endpoint.get(n) or "").strip():
            f.value, f.source = choice.endpoint[n].strip(), "choice"
        out.append(f)
    return out


def match_key(spec: catalog.ConnectorSpec, endpoint: dict[str, str]) -> str:
    g = lambda k: (endpoint.get(k) or "").strip().lower().rstrip("/.")  # noqa: E731
    if spec.id == "sql":
        return f"{g('server')};{g('database')}"
    if spec.id == "adls_gen2":
        acct = g("account")
        host = acct if "." in acct else (f"{acct}.dfs.core.windows.net" if acct else "")
        return f"{host}/{g('container')}"
    if spec.id == "blob":
        return f"{g('account')}.{g('domain')}"
    return g("base_url") or g("url")


def existing_key(conn: dict) -> tuple[str, str]:
    """(fabric type, normalised address) of an existing Fabric connection."""
    cd = conn.get("connectionDetails") or {}
    t, path = str(cd.get("type") or ""), str(cd.get("path") or "").strip().lower()
    if t == "AzureDataLakeStorage":
        u = urlparse(path if "://" in path else f"https://{path}")
        return t, f"{u.hostname or ''}/{u.path.strip('/')}"
    return t, path.rstrip("/")


def match_existing(spec: Optional[catalog.ConnectorSpec], fields: list[EndpointField], connections: list[dict],
                   method: Optional[catalog.AuthMethod]) -> list[ExistingMatch]:
    if spec is None or not connections:
        return []
    ep = {f.name: f.value or "" for f in fields}
    want = match_key(spec, ep)
    want_host = want.split(";")[0].split("/")[0]
    out = []
    for c in connections:
        t, key = existing_key(c)
        if t != spec.fabric_type or not key:
            continue
        cred = (c.get("credentialDetails") or {}).get("credentialType")
        if want and key == want:
            kind = "exact"
        elif want_host and key.split(";")[0].split("/")[0] == want_host:
            kind = "partial"
        else:
            continue
        reusable = None
        note = ""
        if method is not None and method.fabric_credential:
            reusable = cred == method.fabric_credential
            if not reusable:
                note = f"It uses {cred or 'unknown'} credentials, but the chosen method is {method.fabric_credential}."
        if kind == "partial":
            note = (note + " " if note else "") + "Same server, different database/container: reuse only if its credentials cover this one."
        out.append(ExistingMatch(id=str(c.get("id")), display_name=str(c.get("displayName") or ""), match=kind,
                                 credential_type=cred, reusable_with_method=reusable, note=note.strip()))
    return sorted(out, key=lambda m: (m.match != "exact", m.display_name.lower()))


# ---- the cards -------------------------------------------------------------------------------------------------------------


def build_cards(d: AdfSemanticDiscovery, *, choices: Optional[dict[str, ConnectionChoice]] = None,
                supported_types: Optional[dict[str, list[str]]] = None, connections: Optional[list[dict]] = None,
                gateways: Optional[list[dict]] = None, secrets_held: Optional[dict[str, list[str]]] = None,
                network_hints: Optional[dict[str, dict]] = None,
                states: Optional[dict[str, ConnectionState]] = None) -> list[LinkedServiceCard]:
    choices = choices or {}
    runtimes = {r.name.lower(): r for r in d.integration_runtimes}
    cards = []
    for ls in sorted(d.linked_services, key=lambda x: x.name.lower()):
        spec = catalog.connector_for(ls.type)
        choice = choices.get(ls.name.lower())
        ds_names, acts, args = usage(d, ls.name)
        fields = endpoint_fields(spec, ls, d, ds_names, choice)
        supported_cred = (supported_types or {}).get(spec.fabric_type) if (spec and supported_types) else None
        avail = catalog.methods_for(spec, supported_cred)
        chosen = next((a.method for a in avail if choice and a.method.id == choice.method), None)
        card = LinkedServiceCard(
            name=ls.name, adf_type=ls.type, connector=spec.id if spec else None, connector_label=spec.label if spec else None,
            supported=spec is not None, fabric_connection_type=spec.fabric_type if spec else None,
            authentication_type=ls.authentication_type, user_name=ls.connection.user_name,
            key_vault_refs=[{"store_linked_service": k.store_linked_service, "secret_name": k.secret_name} for k in ls.key_vault_refs],
            used_by_datasets=ds_names, used_by_activities=acts, endpoint_fields=fields, choice=choice,
            secrets_held=(secrets_held or {}).get(ls.name, []),
            auth_options=[{**a.method.public(), "available": a.available, "reason": a.reason} for a in avail],
            existing_matches=match_existing(spec, fields, connections or [], chosen),
            runtime=analyze(ls, runtimes, gateways, (network_hints or {}).get(ls.name)))
        # parameters: a linked service that takes parameters has no single address unless every parameter has a default
        card.parameters = [{"name": p.name, "type": p.type, "has_default": p.has_default,
                            "default": p.default_value} for p in ls.parameters]
        card.parameterized = bool(ls.parameters or ls.connection.parameterized_fields)
        for p in ls.parameters:
            vals = set(args.get(p.name, set()))
            if p.has_default and p.default_value:
                vals.add(p.default_value)
            if vals:
                card.parameter_candidates[p.name] = sorted(vals)
        _state_and_guidance(card, ls, spec, chosen, (states or {}).get(ls.name))
        cards.append(card)
    return cards


def _state_and_guidance(card: LinkedServiceCard, ls: AdfLinkedService, spec: Optional[catalog.ConnectorSpec],
                        chosen: Optional[catalog.AuthMethod], tested: Optional[ConnectionState]) -> None:
    rt = card.runtime
    if spec is None:
        card.state = ConnectionState.manual_action_required
        card.missing.append("No connector declaration exists for this linked-service type.")
        card.next_steps.append(catalog.UNSUPPORTED_NOTE)
        return
    unresolved = [f.label for f in card.endpoint_fields if f.required and not f.value]
    if unresolved:
        card.missing.append("Endpoint value(s) not found in the source: " + ", ".join(unresolved) + ".")
        card.next_steps.append("Enter " + " and ".join(unresolved).lower() + " (they are not authored in the linked service"
                               + (" - it uses a connection string, which this tool never reads" if ls.has_connection_string else "")
                               + ").")
    if card.parameterized:
        multi = {k: v for k, v in card.parameter_candidates.items() if len(v) > 1}
        none = [p["name"] for p in card.parameters if not p["has_default"] and p["name"] not in card.parameter_candidates]
        if none:
            card.warnings.append("Parameter(s) " + ", ".join(none) + " have no default and no value in any pipeline: no single "
                                 "address can be resolved; do not guess one.")
            card.next_steps.append("Enter the value(s) for " + ", ".join(none) + ", or configure one Fabric connection per value.")
        if multi:
            card.warnings.append("Several values are used for " + ", ".join(multi) + ": " + "; ".join(
                f"{k}={sorted(v)}" for k, v in multi.items()) + ". Create one Fabric connection per value; only a single "
                "connection can be bound automatically.")
    if rt is not None and rt.fabric_path in ("on_premises_gateway", "virtual_network_gateway", "manual", "unsupported") \
            and rt.runtime_kind != "azure":
        card.next_steps.append(rt.recommendation)
    if card.choice is None:
        card.state = ConnectionState.manual_action_required if (rt and rt.fabric_path == "unsupported") else \
            ConnectionState.resource_discovered
        card.next_steps.append("Choose how Fabric signs in to this data source.")
    else:
        if chosen is None:
            card.state = ConnectionState.validation_failed
            card.missing.append(f"The chosen method '{card.choice.method}' is not available for this connector.")
        else:
            problems = catalog.validate_choice(spec, chosen, {f.name: f.value or "" for f in card.endpoint_fields},
                                               card.choice.values, set(card.secrets_held))
            card.missing += problems
            if problems:
                card.state = ConnectionState.validation_failed
            elif not chosen.api_creatable and chosen.id != "existing_connection":
                card.state = ConnectionState.manual_action_required
                card.next_steps.append(chosen.notes)
            else:
                card.state = ConnectionState.connection_configured
            if chosen.needs_workspace_identity:
                card.next_steps.append("Provision the destination workspace identity and grant it access to the data source.")
    if tested == ConnectionState.connection_tested and card.state == ConnectionState.connection_configured:
        card.state = ConnectionState.connection_tested
    elif tested == ConnectionState.validation_failed:
        card.state = ConnectionState.validation_failed
    if rt is not None and rt.fabric_path in ("on_premises_gateway", "virtual_network_gateway") and rt.api_creatable is False \
            and (card.choice is None or card.choice.method != "existing_connection") and card.state == ConnectionState.connection_configured:
        card.state = ConnectionState.manual_action_required
        card.next_steps.append("This linked service needs a gateway connection: create it in Fabric against the gateway, then choose "
                               "'Use an existing Fabric connection'.")
