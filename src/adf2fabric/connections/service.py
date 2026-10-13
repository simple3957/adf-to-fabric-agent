"""Connection planning for a project: choices, secrets, tests, applying them in Fabric, and the connection migration report.

State kept per project on disk is NON-SECRET only (``projects/<id>/connections/choices.yaml`` and ``apply.yaml``). Secrets live
in the in-memory ``SecretVault`` for the session and are used once: to test a credential, or to create the Fabric connection
(Fabric then validates them itself). The result feeds the existing converter through ``conversion/resolutions.json``
(``LinkedServiceToConnectionId`` entries), merged with whatever is already there.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field

from adf2fabric.connections import catalog
from adf2fabric.connections.fabric import FabricApiError, FabricService
from adf2fabric.connections.keyvault import KeyVaultError, normalize_vault_url
from adf2fabric.connections.models import ConnectionState, TestReport
from adf2fabric.connections.redaction import redact
from adf2fabric.connections.resolution import ConnectionChoice, LinkedServiceCard, build_cards
from adf2fabric.connections.secret_vault import SecretVault
from adf2fabric.connections.testing import ConnectorTester
from adf2fabric.project.discovery import AdfSemanticDiscovery
from adf2fabric.project.store import ProjectStore

SCOPE = "linked_service"
_NAME_BAD = re.compile(r"[^A-Za-z0-9 ._()-]+")


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ApplyItem(_M):
    linked_service: str
    action: str  # created | reused | would_create | manual | skipped | failed
    origin: str = "user"  # user | auto_matched
    connection_ids: list[str] = Field(default_factory=list)
    bound: bool = False  # written to resolutions.json (exactly one connection)
    fabric_validated: bool = False  # Fabric tested the credentials when it created the connection
    detail: str = ""
    actions: list[str] = Field(default_factory=list)
    state: ConnectionState = ConnectionState.not_configured


class ApplyResult(_M):
    applied_at: datetime
    workspace_id: Optional[str] = None
    dry_run: bool = False
    items: list[ApplyItem] = Field(default_factory=list)
    resolutions_file: Optional[str] = None


class ConnectionService:
    def __init__(self, store: ProjectStore, vault: Optional[SecretVault] = None, tester: Optional[ConnectorTester] = None,
                 secret_reader: Optional[Callable[[str, str], str]] = None):
        self.store = store
        self.vault = vault if vault is not None else SecretVault()
        self.secret_reader = secret_reader
        self.tester = tester or ConnectorTester(secret_reader=secret_reader)
        self._states: dict[tuple[str, str], ConnectionState] = {}
        self.last_tests: dict[tuple[str, str], TestReport] = {}

    # ---- paths + persistence ---------------------------------------------------------------------------------------
    def _dir(self, pid: str) -> Path:
        d = self.store.project_dir(pid) / "connections"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def load_choices(self, pid: str) -> dict[str, ConnectionChoice]:
        f = self._dir(pid) / "choices.yaml"
        if not f.exists():
            return {}
        raw = yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        return {k.lower(): ConnectionChoice.model_validate(v) for k, v in raw.items()}

    def _save_choices(self, pid: str, choices: dict[str, ConnectionChoice]) -> None:
        data = {c.linked_service: json.loads(c.model_dump_json()) for c in choices.values()}
        (self._dir(pid) / "choices.yaml").write_text(yaml.safe_dump(data, sort_keys=True), encoding="utf-8")

    def semantic(self, pid: str) -> AdfSemanticDiscovery:
        return self.store.load_adf_semantic_discovery(pid)

    # ---- choices + secrets -----------------------------------------------------------------------------------------
    def set_choice(self, pid: str, choice: ConnectionChoice) -> ConnectionChoice:
        d = self.semantic(pid)
        ls = next((x for x in d.linked_services if x.name.lower() == choice.linked_service.lower()), None)
        if ls is None:
            raise catalog.ChoiceError([f"Linked service '{choice.linked_service}' is not in this project."])
        spec = catalog.connector_for(ls.type)
        if spec is None:
            if choice.method != "existing_connection":
                raise catalog.ChoiceError([catalog.UNSUPPORTED_NOTE])
        else:
            catalog.method_by_id(spec, choice.method)
        for k in list(choice.values):  # a secret field must never arrive through the non-secret values
            if spec and any(f.name == k and f.kind == "secret" for m in spec.methods for f in m.fields):
                raise catalog.ChoiceError([f"'{k}' is a secret: send it through the secrets endpoint, not as a value."])
        choice = choice.model_copy(update={"linked_service": ls.name})
        choices = self.load_choices(pid)
        choices[ls.name.lower()] = choice
        self._save_choices(pid, choices)
        self._states.pop((pid, ls.name), None)
        return choice

    def put_secret(self, session: str, pid: str, linked_service: str, field: str, value: str) -> None:
        """Hold a secret for one linked service field. It is never persisted, logged or returned."""
        d = self.semantic(pid)
        ls = next((x for x in d.linked_services if x.name.lower() == linked_service.lower()), None)
        spec = catalog.connector_for(ls.type) if ls else None
        allowed = {f.name for m in (spec.methods if spec else ()) for f in m.fields if f.kind == "secret"}
        if ls is None or field not in allowed:
            raise catalog.ChoiceError([f"'{field}' is not a secret field of this linked service's connector."])
        if not value:
            raise catalog.ChoiceError([f"The value for '{field}' is empty."])
        self.vault.put(session, SCOPE, ls.name, field, value)
        self._states.pop((pid, ls.name), None)

    def clear_secrets(self, session: str, linked_service: Optional[str] = None) -> int:
        return self.vault.wipe(session, SCOPE, linked_service)

    def _held(self, session: str, d: AdfSemanticDiscovery) -> dict[str, list[str]]:
        return {ls.name: self.vault.fields(session, SCOPE, ls.name) for ls in d.linked_services}

    # ---- cards ---------------------------------------------------------------------------------------------------
    def cards(self, pid: str, session: str, fabric: Optional[FabricService] = None,
              network_hints: Optional[dict[str, dict]] = None) -> list[LinkedServiceCard]:
        d = self.semantic(pid)
        supported = conns = gws = None
        if fabric is not None:
            supported = _soft(fabric.supported_connection_types)
            conns = _soft(fabric.connections)
            gws = _soft(fabric.gateways)
        states = {name: s for (p, name), s in self._states.items() if p == pid}
        return build_cards(d, choices=self.load_choices(pid), supported_types=supported, connections=conns, gateways=gws,
                           secrets_held=self._held(session, d), network_hints=network_hints, states=states)

    # ---- testing -------------------------------------------------------------------------------------------------
    def test(self, pid: str, session: str, linked_service: str, fabric: Optional[FabricService] = None) -> TestReport:
        cards = {c.name.lower(): c for c in self.cards(pid, session, fabric)}
        card = cards.get(linked_service.lower())
        if card is None:
            raise catalog.ChoiceError([f"Linked service '{linked_service}' is not in this project."])
        if card.choice is None or card.connector is None:
            rep = TestReport(target=card.name, state=ConnectionState.not_configured,
                             error="No authentication method has been chosen for this linked service.",
                             actions=["Choose how Fabric signs in to it, then test."])
            return rep
        spec = catalog.connector_for(card.adf_type)
        method = catalog.method_by_id(spec, card.choice.method)
        endpoint = {f.name: f.value or "" for f in card.endpoint_fields}
        secrets = {f: self.vault.get(session, SCOPE, card.name, f) or "" for f in method.secret_fields}
        rep = self.tester.test(spec, method, endpoint, card.choice.values, secrets)
        if card.runtime and card.runtime.fabric_path in ("on_premises_gateway", "virtual_network_gateway") \
                and card.runtime.runtime_kind != "azure":
            rep.not_verified.append("this data source is reached through a gateway in ADF; a test from this machine does not "
                                    "prove the gateway path")
        self._states[(pid, card.name)] = rep.state
        self.last_tests[(pid, card.name)] = rep
        return rep

    # ---- applying ------------------------------------------------------------------------------------------------
    def apply(self, pid: str, session: str, fabric: FabricService, *, workspace_id: Optional[str] = None,
              names: Optional[list[str]] = None, auto_match: bool = False, dry_run: bool = False) -> ApplyResult:
        want = {n.lower() for n in names} if names else None
        cards = self.cards(pid, session, fabric)
        existing = {str(c.get("id")): c for c in _soft(fabric.connections) or []}
        result = ApplyResult(applied_at=datetime.now(timezone.utc), workspace_id=workspace_id, dry_run=dry_run)
        bindings: dict[str, str] = {}
        for card in cards:
            if want is not None and card.name.lower() not in want:
                continue
            item = self._apply_one(pid, session, fabric, card, existing, auto_match, dry_run)
            result.items.append(item)
            if item.bound and not dry_run:
                bindings[card.name] = item.connection_ids[0]
            self._states[(pid, card.name)] = item.state
        if bindings and not dry_run:
            result.resolutions_file = self._merge_resolutions(pid, bindings)
        if not dry_run:
            (self._dir(pid) / "apply.yaml").write_text(yaml.safe_dump(json.loads(result.model_dump_json()), sort_keys=False),
                                                      encoding="utf-8")
        return result

    def _apply_one(self, pid, session, fabric, card: LinkedServiceCard, existing: dict, auto_match: bool, dry_run: bool) -> ApplyItem:
        item = ApplyItem(linked_service=card.name, action="skipped", state=card.state)
        choice, origin = card.choice, "user"
        if choice is None and auto_match:
            exact = [m for m in card.existing_matches if m.match == "exact"]
            if len(exact) == 1 and card.connector:
                choice = ConnectionChoice(linked_service=card.name, method="existing_connection",
                                          values={"connection_id": exact[0].id}, origin="auto_matched")
                origin = "auto_matched"
        item.origin = origin
        if card.connector is None and choice is None:
            item.action, item.state = "manual", ConnectionState.manual_action_required
            item.detail = catalog.UNSUPPORTED_NOTE
            return item
        if choice is None:
            item.detail = "No authentication method was chosen."
            item.actions = ["Choose how Fabric signs in to this linked service."]
            item.state = ConnectionState.resource_discovered
            return item
        if card.runtime and card.runtime.fabric_path == "unsupported":
            item.action, item.state = "manual", ConnectionState.manual_action_required
            item.detail = card.runtime.recommendation
            return item
        spec = catalog.connector_for(card.adf_type)
        if choice.method == "existing_connection":
            cid = (choice.values.get("connection_id") or "").strip()
            if cid not in existing:
                item.action, item.state = "failed", ConnectionState.validation_failed
                item.detail = f"Connection '{cid}' was not found in Fabric for this account."
                item.actions = ["Check the connection id, and that this account can see it."]
                return item
            item.action, item.connection_ids, item.bound = "reused", [cid], True
            item.state = ConnectionState.connection_configured
            item.detail = (f"Reused '{existing[cid].get('displayName')}'"
                           + (" (matched automatically by its endpoint)" if origin == "auto_matched" else "")
                           + "; its credentials were validated when it was created in Fabric, not by this tool.")
            return item
        method = catalog.method_by_id(spec, choice.method)
        endpoint = {f.name: f.value or "" for f in card.endpoint_fields}
        problems = [p for p in card.missing]
        if problems:
            item.action, item.state, item.detail = "failed", ConnectionState.validation_failed, "; ".join(problems)
            item.actions = problems
            return item
        if not method.api_creatable or (card.runtime and card.runtime.fabric_path in ("on_premises_gateway", "virtual_network_gateway")
                                        and card.runtime.runtime_kind != "azure"):
            item.action, item.state = "manual", ConnectionState.manual_action_required
            item.detail = (method.notes if not method.api_creatable else card.runtime.recommendation)
            item.actions = (card.runtime.requirements if card.runtime and card.runtime.fabric_path != "cloud_connection" else [])
            return item
        variants = [endpoint] + [{**endpoint, **v} for v in choice.variants]
        name = _name(choice.display_name or card.name)
        if dry_run:
            item.action, item.state = "would_create", ConnectionState.connection_configured
            item.detail = f"Would create {len(variants)} connection(s) of type {spec.fabric_type} using '{method.label}'."
            return item
        try:
            secrets = self._secrets_for(session, card.name, method, choice)
        except KeyVaultError as exc:
            item.action, item.state, item.detail, item.actions = "failed", ConnectionState.validation_failed, str(exc), exc.actions
            return item
        ids: list[str] = []
        for i, ep in enumerate(variants):
            body = catalog.build_connection_body(spec, method, ep, choice.values, secrets,
                                                 name if i == 0 else f"{name}-{i + 1}", choice.privacy_level)
            try:
                made = fabric.create_connection(body)
            except FabricApiError as exc:
                item.action, item.state = "failed", ConnectionState.validation_failed
                item.detail = redact(str(exc), *secrets.values())
                item.actions = exc.actions + _hint(str(exc), method)
                item.connection_ids = ids
                return item
            ids.append(str(made.get("id")))
        item.action, item.connection_ids, item.fabric_validated = "created", ids, True
        item.bound = len(ids) == 1
        item.state = ConnectionState.connection_tested
        item.detail = (f"Created {len(ids)} Fabric connection(s); Fabric tested the credentials when it created them."
                       + ("" if item.bound else " More than one connection exists for this parameterized linked service, so none "
                          "was bound automatically: choose per pipeline in Fabric."))
        return item

    def _secrets_for(self, session: str, ls: str, method: catalog.AuthMethod, choice: ConnectionChoice) -> dict[str, str]:
        """Secret values for one create call: typed secrets from the vault, or the Key Vault secret named by the choice."""
        out = {f: self.vault.get(session, SCOPE, ls, f) or "" for f in method.secret_fields}
        if method.key_vault:
            if self.secret_reader is None:
                raise KeyVaultError("Key Vault access is not configured on this server.")
            value = self.secret_reader(normalize_vault_url(choice.values.get("vault_url", "")), choice.values.get("secret_name", ""))
            target = {"Basic": "password", "Key": "account_key" if "account_key" in method.id else "token",
                      "SharedAccessSignature": "sas_token", "ServicePrincipal": "client_secret"}[method.fabric_credential or "Basic"]
            out[target] = value
        missing = [f.label for f in method.fields if f.kind == "secret" and not out.get(f.name)]
        if missing:
            raise KeyVaultError("Not supplied: " + ", ".join(missing) + ".", ["Send the secret before applying."])
        return out

    def _merge_resolutions(self, pid: str, bindings: dict[str, str]) -> str:
        path = self.store.conversion_dir(pid) / "resolutions.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        entries: list[dict] = []
        if path.exists():
            try:
                entries = json.loads(path.read_text(encoding="utf-8"))
            except ValueError:
                entries = []
        keep = [e for e in entries if not (e.get("type") == "LinkedServiceToConnectionId"
                                           and str(e.get("key", "")).lower() in {k.lower() for k in bindings})]
        keep += [{"type": "LinkedServiceToConnectionId", "key": k, "value": v} for k, v in sorted(bindings.items())]
        path.write_text(json.dumps(keep, indent=2) + "\n", encoding="utf-8")
        return "resolutions.json"

    def provision_identity(self, fabric: FabricService, workspace_id: str) -> dict:
        try:
            r = fabric.provision_identity(workspace_id)
        except FabricApiError as exc:
            return {"provisioned": False, "error": str(exc), "actions": exc.actions + [
                "You need the Admin role in the workspace, and the tenant must allow workspace identities."]}
        return {"provisioned": True, "accepted": bool(r.get("accepted")), "application_id": r.get("applicationId"),
                "service_principal_id": r.get("servicePrincipalId"),
                "next": "Grant this identity a role on each data source that uses 'Workspace identity'."}

    # ---- report --------------------------------------------------------------------------------------------------
    def last_apply(self, pid: str) -> Optional[ApplyResult]:
        f = self._dir(pid) / "apply.yaml"
        return ApplyResult.model_validate(yaml.safe_load(f.read_text(encoding="utf-8"))) if f.exists() else None

    def write_report(self, pid: str, session: str, fabric: Optional[FabricService] = None) -> dict[str, str]:
        """``conversion/connections-report.{md,json}``: how each linked service maps, and what is still open."""
        cards = self.cards(pid, session, fabric)
        applied = {i.linked_service: i for i in (self.last_apply(pid).items if self.last_apply(pid) else [])}
        rows = []
        for c in cards:
            a = applied.get(c.name)
            resolution = ("user-configured" if (a and a.action == "created") else
                          "automatic (matched an existing connection)" if (a and a.origin == "auto_matched" and a.bound) else
                          "user-configured (existing connection)" if (a and a.action == "reused") else
                          "manual action required" if (a and a.action == "manual") or c.state == ConnectionState.manual_action_required
                          else "unresolved")
            rows.append({"linked_service": c.name, "adf_type": c.adf_type, "connector": c.connector_label,
                         "fabric_type": c.fabric_connection_type, "method": c.choice.method if c.choice else None,
                         "resolution": resolution, "connection_ids": a.connection_ids if a else [], "state": c.state.value,
                         "runtime": c.runtime.model_dump(include={"runtime_name", "runtime_kind", "exposure", "fabric_path",
                                                                  "api_creatable"}) if c.runtime else None,
                         "missing": c.missing, "next_steps": c.next_steps, "warnings": c.warnings})
        out_dir = self.store.conversion_dir(pid)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "connections-report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
        (out_dir / "connections-report.md").write_text(_render_md(rows), encoding="utf-8")
        return {"markdown": "connections-report.md", "json": "connections-report.json"}


def _soft(fn: Callable[[], Any]) -> Any:
    """Optional Fabric context: a failure leaves the cards without it (matches/options) instead of breaking the screen."""
    try:
        return fn()
    except (FabricApiError, Exception):  # noqa: BLE001
        return None


def _name(raw: str) -> str:
    return (_NAME_BAD.sub("_", raw).strip() or "adf-connection")[:100]


def _hint(message: str, method: catalog.AuthMethod) -> list[str]:
    m = message.lower()
    if method.needs_workspace_identity and ("identity" in m or "400" in m):
        return ["Provision the destination workspace identity first (it needs the Admin role in the workspace)."]
    if "duplicate" in m or "already" in m:
        return ["Rename the connection (a Fabric connection with this name already exists)."]
    return []


def _render_md(rows: list[dict]) -> str:
    n = len(rows)
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["resolution"]] = counts.get(r["resolution"], 0) + 1
    lines = ["# Connection migration report", "",
             f"{n} linked service(s). " + "; ".join(f"{v} {k}" for k, v in sorted(counts.items())) + ".", "",
             "*Resolution* says who decided: **automatic** means an existing Fabric connection matched the endpoint exactly and was "
             "reused; **user-configured** means a person chose the method (or an existing connection) and, for created connections, "
             "Fabric tested the credentials when it created them. A reused connection's credentials are not validated by this tool. "
             "No secret appears in this report.", "",
             "| Linked service | Type -> Fabric | Method | Resolution | Connection | State | Runtime -> path |",
             "| --- | --- | --- | --- | --- | --- | --- |"]
    for r in rows:
        rt = r["runtime"] or {}
        lines.append(f"| `{r['linked_service']}` | {r['adf_type']} -> {r['fabric_type'] or 'none'} | {r['method'] or '-'} | "
                     f"{r['resolution']} | {', '.join(r['connection_ids']) or '-'} | {r['state']} | "
                     f"{rt.get('runtime_kind', '-')} ({rt.get('exposure', '-')}) -> {rt.get('fabric_path', '-')} |")
    open_items = [r for r in rows if r["missing"] or r["next_steps"] or r["warnings"]]
    if open_items:
        lines += ["", "## Open items"]
        for r in open_items:
            lines += ["", f"### `{r['linked_service']}`"]
            lines += [f"- Missing: {m}" for m in r["missing"]]
            lines += [f"- Next: {m}" for m in r["next_steps"]]
            lines += [f"- Warning: {m}" for m in r["warnings"]]
    return "\n".join(lines) + "\n"
