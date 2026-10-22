"""Conversion state: project/pipeline context and the per-activity builder handlers use.

Handlers never touch shared state directly. They receive an :class:`ActivityBuilder`, read the
semantic record (``cx.act``), build Fabric JSON, and RECORD every decision through it — property
mappings, expression conversions, differences, manual steps, alternatives, prerequisites, and
which generated component covers which source operation. The engine turns those records into
the persisted :class:`ActivityConversion`; nothing is inferred from "JSON was produced".
"""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from adf2fabric.agent.conversion.expr_convert import ExprEnv, convert_tree, convert_value
from adf2fabric.agent.conversion.fabric_schema import (
    POLICY_KEYS,
    RETRY_INTERVAL_MAX,
    RETRY_INTERVAL_MIN,
    SECURE_POLICY_KEYS,
)
from adf2fabric.agent.conversion import compatibility as compat
from adf2fabric.project.conversion import (
    ConversionIssue,
    ConversionStrategy,
    ExpressionConversion,
    FabricComponent,
    Impact,
    IssueSeverity,
    OperationCoverage,
    Prerequisite,
    PropertyMapping,
)
from adf2fabric.project.discovery import (
    AdfActivity,
    AdfDataset,
    AdfLinkedService,
    AdfOperationKind,
    AdfParameter,
    AdfPipeline,
    AdfTrigger,
)

_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
NESTED_KEYS = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")
AUTO_RESOLVE_IR = "autoresolveintegrationruntime"


def gateway_need(ir_name: Optional[str], kind: Optional[str]) -> Optional[str]:
    """Why a Fabric gateway is needed for an ADF integration runtime, or None when it is not.

    ``kind`` comes from the runtime's own definition: a plain Azure runtime needs no gateway (a
    Fabric cloud connection replaces it); a self-hosted runtime maps to an on-premises data
    gateway; one in a managed virtual network maps to a virtual network data gateway. An
    unclassified runtime (definition missing, or a type we do not classify) stays a cautious
    "verify" prerequisite rather than being silently ignored.
    """
    if not ir_name or ir_name.lower() == AUTO_RESOLVE_IR or kind == "azure":
        return None
    if kind == "self_hosted":
        return f"Integration runtime '{ir_name}' is self-hosted: create the Fabric connection on an on-premises data gateway."
    if kind == "managed_vnet":
        return (f"Integration runtime '{ir_name}' runs in a managed virtual network: create the Fabric connection on a "
                "virtual network data gateway (or use managed private endpoints).")
    return (f"Integration runtime '{ir_name}' could not be classified (its definition is missing or of an unclassified "
            "type). Verify whether the source needs an on-premises or virtual network data gateway.")


# --- resolutions (token -> real id) ------------------------------------------------------------------


class Resolutions:
    """Optional id bindings supplied by the user.

    The file format extends Microsoft's documented upgrader "resolution file"
    (``LinkedServiceToConnectionId``: ADF linked-service name -> Fabric connection GUID) with
    two types of this tool's own:

    * ``ItemToId``      key ``"<kind>:<name>"`` (kind: pipeline|notebook|dataflow|sparkjob|connection)
    * ``WorkspaceId``   key ``"*"``
    """

    def __init__(self, entries: Optional[list[dict]] = None):
        self.connections: dict[str, str] = {}
        self.items: dict[str, str] = {}
        self.workspace: Optional[str] = None
        self.problems: list[str] = []
        for i, e in enumerate(entries or []):
            if not isinstance(e, dict) or not all(k in e for k in ("type", "key", "value")):
                self.problems.append(f"entry {i}: expected an object with type, key and value")
                continue
            t, key, value = e["type"], str(e["key"]), str(e["value"])
            if not _GUID.match(value):
                self.problems.append(f"entry {i} ({t} '{key}'): '{value}' is not a GUID; ignored")
                continue
            if t == "LinkedServiceToConnectionId":
                self.connections[key.lower()] = value
            elif t == "ItemToId":
                self.items[key.lower()] = value
            elif t == "WorkspaceId":
                self.workspace = value
            else:
                self.problems.append(f"entry {i}: unknown resolution type '{t}'; ignored")

    @classmethod
    def load(cls, path: Optional[Path]) -> "Resolutions":
        if path is None:
            return cls()
        try:
            data = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            r = cls()
            r.problems.append(f"could not read resolutions file: {type(exc).__name__}")
            return r
        return cls(data if isinstance(data, list) else [])


# --- project / pipeline context ---------------------------------------------------------------------


@dataclass
class ProjectContext:
    datasets: dict[str, AdfDataset]
    linked_services: dict[str, AdfLinkedService]
    pipelines: dict[str, AdfPipeline]
    global_params: dict[str, AdfParameter]
    triggers: list[AdfTrigger]
    resolutions: Resolutions
    globals_needed: dict[str, list[str]] = field(default_factory=dict)  # pipeline(lower) -> global names
    ir_kinds: dict[str, Optional[str]] = field(default_factory=dict)  # integration runtime (lower) -> kind
    prereqs: dict[tuple[str, str], Prerequisite] = field(default_factory=dict)
    # Spark workloads that were approved AND deployed to Fabric (workloads-deploy): source item (lower) -> manifest entry.
    # Empty unless the user opted in; conversion then still defaults to running the source compute.
    workloads: dict[str, Any] = field(default_factory=dict)

    # -- bindings ------------------------------------------------------------------------------------
    def _prereq(self, kind: str, name: str, detail: str, token: str, bound: bool, needed_by: str) -> Prerequisite:
        key = (kind, name.lower())
        p = self.prereqs.get(key)
        if p is None:
            p = Prerequisite(kind=kind, name=name, detail=detail, binding=None if bound else token, bound=bound)
            self.prereqs[key] = p
        if needed_by and needed_by not in p.needed_by:
            p.needed_by.append(needed_by)
        return p

    def connection_ref(self, ls_name: str, needed_by: str, detail_extra: str = "") -> str:
        """The connection id for an ADF linked service (a real GUID when resolved, else a token)."""
        ls = self.linked_services.get(ls_name.lower())
        parts = [f"ADF linked service type {ls.type}" if ls and ls.type else "ADF linked service"]
        if ls:
            if ls.endpoint:
                parts.append(f"endpoint {ls.endpoint}")
            if ls.authentication_type:
                parts.append(f"authentication {ls.authentication_type}")
            if ls.key_vault_refs:
                parts.append("secrets come from Azure Key Vault (recreate the credential in the Fabric connection)")
        if detail_extra:
            parts.append(detail_extra)
        guid = self.resolutions.connections.get(ls_name.lower())
        token = f"<connection:{ls_name}>"
        self._prereq("connection", ls_name, "; ".join(parts), token, guid is not None, needed_by)
        why = gateway_need(ls.integration_runtime, ls.integration_runtime_kind) if ls else None
        if why:
            self._prereq("gateway", ls.integration_runtime, f"Linked service '{ls_name}': {why}",
                         f"<gateway:{ls.integration_runtime}>", False, needed_by)
        return guid or token

    def free_connection_ref(self, key: str, detail: str, needed_by: str) -> str:
        """A connection with no ADF linked service behind it (Web base URL, Invoke pipeline, ...)."""
        guid = self.resolutions.items.get(f"connection:{key}".lower())
        token = f"<connection:{key}>"
        self._prereq("connection", key, detail, token, guid is not None, needed_by)
        return guid or token

    def item_ref(self, kind: str, name: str, needed_by: str, detail: str) -> str:
        guid = self.resolutions.items.get(f"{kind}:{name}".lower())
        token = f"<item:{kind}:{name}>"
        self._prereq("workspace_item", f"{kind}:{name}", detail, token, guid is not None, needed_by)
        return guid or token

    def workspace_ref(self, needed_by: str) -> str:
        guid = self.resolutions.workspace
        self._prereq("workspace_item", "workspace",
                     "The Fabric workspace that holds the target items.", "<workspace-id>", guid is not None, needed_by)
        return guid or "<workspace-id>"

    def placeholder_ref(self, kind: str, name: str, token: str, detail: str, needed_by: str) -> str:
        self._prereq(kind, name, detail, token, False, needed_by)
        return token


@dataclass
class PipelineContext:
    project: ProjectContext
    pipeline: AdfPipeline
    names: set[str] = field(default_factory=set)  # lowercase component names in use
    used_globals: set[str] = field(default_factory=set)
    rename: dict[str, str] = field(default_factory=dict)  # lowercase source activity -> output component
    registry: list[Any] = field(default_factory=list)  # ActivityConversion list, flat, source order
    param_names: set[str] = field(default_factory=set)  # lowercase

    def alloc(self, base: str) -> str:
        """A pipeline-unique component name (deterministic)."""
        name, n = base, 1
        while name.lower() in self.names:
            n += 1
            name = f"{base}_{n}"
        self.names.add(name.lower())
        return name

    def reserve(self, name: str) -> None:
        self.names.add(name.lower())


# --- the builder --------------------------------------------------------------------------------------


def strip_nested(config: dict) -> tuple[dict, list[str]]:
    """A copy of ``config`` without nested activity lists, plus the names of the nested activities."""
    cfg = copy.deepcopy(config)
    names: list[str] = []
    tp = cfg.get("typeProperties")
    if isinstance(tp, dict):
        for k in NESTED_KEYS:
            if isinstance(tp.get(k), list):
                names.extend(a.get("name", "") for a in tp[k] if isinstance(a, dict))
                tp.pop(k)
        if isinstance(tp.get("cases"), list):
            for c in tp["cases"]:
                if isinstance(c, dict) and isinstance(c.get("activities"), list):
                    names.extend(a.get("name", "") for a in c["activities"] if isinstance(a, dict))
                    c.pop("activities")
    return cfg, names


class ActivityBuilder:
    """Everything a handler needs, and the record of everything it decides."""

    def __init__(self, act: AdfActivity, pc: PipelineContext,
                 children_fn: Callable[[str], list[dict]]):
        self.act = act
        self.pc = pc
        self._children_fn = children_fn
        self.components: list[FabricComponent] = []
        self.full: dict[str, dict] = {}  # component name -> complete JSON (nested activities included)
        self.strategy: ConversionStrategy = ConversionStrategy.transformed
        self.disposition: Optional[str] = None
        self.property_mappings: list[PropertyMapping] = []
        self.expression_conversions: list[ExpressionConversion] = []
        self.issues: list[ConversionIssue] = []
        self.limitations: list[str] = []
        self.manual_steps: list[str] = []
        self.alternatives: list[str] = []
        self.coverage: list[OperationCoverage] = []
        self.prerequisites: list[Prerequisite] = []
        self.output_notes: list[str] = []
        self.entry: Optional[list[str]] = None
        self.exit: Optional[list[str]] = None
        self.output_component: Optional[str] = None
        self.summary = ""
        self.child_names: list[str] = []

    # -- environments --------------------------------------------------------------------------------
    def env(self, **kw) -> ExprEnv:
        return ExprEnv(activity_rename=self.pc.rename, used_globals=self.pc.used_globals,
                       existing_params=self.pc.param_names, **kw)

    # -- recording -----------------------------------------------------------------------------------
    def mapping(self, source: str, target: Optional[str], rule: str, note: str = "", preserved: bool = True) -> None:
        self.property_mappings.append(PropertyMapping(
            source=source, target=target, rule=rule, note=note, preserved=preserved))

    def dropped(self, source: str, why: str, *, manual: Optional[str] = None) -> None:
        """A source property that is NOT carried to Fabric. Always visible; never silent."""
        self.mapping(source, None, "not_carried_over", why, preserved=False)
        self.difference("property_not_carried_over", f"'{source}' is not carried over: {why}",
                        manual or "Reproduce this setting manually in Fabric if it matters.")

    def issue(self, code: str, message: str, *, impact: Impact = Impact.none,
              severity: IssueSeverity = IssueSeverity.warning, action: str = "",
              source_path: Optional[str] = None, target_path: Optional[str] = None) -> None:
        self.issues.append(ConversionIssue(code=code, severity=severity, impact=impact, message=message,
                                           action=action, source_path=source_path, target_path=target_path))

    def info(self, code: str, message: str, action: str = "") -> None:
        self.issue(code, message, impact=Impact.none, severity=IssueSeverity.info, action=action)

    def difference(self, code: str, message: str, action: str = "") -> None:
        self.limitations.append(message)
        self.issue(code, message, impact=Impact.difference, action=action)

    def manual(self, code: str, message: str, action: str) -> None:
        self.manual_steps.append(action or message)
        self.issue(code, message, impact=Impact.manual, severity=IssueSeverity.warning, action=action)

    def blocker(self, code: str, message: str, action: str = "") -> None:
        self.strategy = ConversionStrategy.manual
        self.issue(code, message, impact=Impact.blocker, severity=IssueSeverity.error, action=action)

    def alternative(self, text: str) -> None:
        if text not in self.alternatives:
            self.alternatives.append(text)

    def cover(self, operation: "AdfOperationKind | str", *components: str, note: str = "") -> None:
        op = operation.value if isinstance(operation, AdfOperationKind) else str(operation)
        self.coverage.append(OperationCoverage(operation=op, covered_by=[c for c in components if c], note=note))

    def needed(self, p: Prerequisite) -> None:
        self.prerequisites.append(p)

    # -- expression helpers ----------------------------------------------------------------------------
    def conv(self, value: Any, source_path: str, target_path: str, env: Optional[ExprEnv] = None) -> Any:
        return convert_value(value, env or self.env(), source_path, target_path,
                             self.expression_conversions, self.issues)

    def tree(self, value: Any, source_path: str, target_path: str, env: Optional[ExprEnv] = None) -> Any:
        return convert_tree(value, env or self.env(), source_path, target_path,
                            self.expression_conversions, self.issues)

    # -- bindings ----------------------------------------------------------------------------------------
    def _bind(self, kind: str, name: str) -> None:
        p = self.pc.project.prereqs.get((kind, name.lower()))
        if p is not None and not any(q.kind == kind and q.name == name for q in self.prerequisites):
            self.prerequisites.append(p)

    def connection(self, ls_name: str, detail: str = "") -> str:
        ref = self.pc.project.connection_ref(ls_name, self.act.name, detail)
        self._bind("connection", ls_name)
        ls = self.pc.project.linked_services.get(ls_name.lower())
        if ls and gateway_need(ls.integration_runtime, ls.integration_runtime_kind):
            self._bind("gateway", ls.integration_runtime)
        if ls is not None:
            for e in compat.for_linked_service(ls.type, ls.connector_version):
                if not e.emit:
                    continue
                code = compat.issue_code(e)
                msg = compat.issue_message(e, ls.connector_version, prefix=f"Linked service '{ls_name}' ({ls.type}): ")
                if not any(i.code == code and i.message == msg for i in self.issues):  # one per activity, not per dataset
                    self.issue(code, msg, impact=e.impact, action=e.alternatives[0] if e.alternatives else "")
        if ls is None:
            self.issue("linked_service_not_found",
                       f"Linked service '{ls_name}' was not found among the discovered artifacts; the connection "
                       "cannot be described.", impact=Impact.manual,
                       action="Re-run discovery on the full factory, or create the Fabric connection by hand.")
        return ref

    def free_connection(self, key: str, detail: str) -> str:
        ref = self.pc.project.free_connection_ref(key, detail, self.act.name)
        self._bind("connection", key)
        return ref

    def item(self, kind: str, name: str, detail: str) -> str:
        ref = self.pc.project.item_ref(kind, name, self.act.name, detail)
        self._bind("workspace_item", f"{kind}:{name}")
        return ref

    def workspace(self) -> str:
        ref = self.pc.project.workspace_ref(self.act.name)
        self._bind("workspace_item", "workspace")
        return ref

    def placeholder(self, kind: str, name: str, token: str, detail: str) -> str:
        ref = self.pc.project.placeholder_ref(kind, name, token, detail, self.act.name)
        self._bind(kind, name)
        return ref

    # -- policy / envelope --------------------------------------------------------------------------------
    def policy(self, *, secure_only: bool = False) -> Optional[dict]:
        p = self.act.policy
        if p is None:
            return None
        out: dict[str, Any] = {}
        for key, value in p.raw.items():
            allowed = SECURE_POLICY_KEYS if secure_only else POLICY_KEYS
            if key not in allowed:
                self.dropped(f"policy.{key}", "not part of the documented Fabric activity policy"
                             + (" for this activity type" if secure_only else ""))
                continue
            out[key] = self.conv(value, f"policy.{key}", f"policy.{key}")
            self.mapping(f"policy.{key}", f"policy.{key}", "policy_passthrough")
        rv = out.get("retryIntervalInSeconds")
        if isinstance(rv, int) and not (RETRY_INTERVAL_MIN <= rv <= RETRY_INTERVAL_MAX):
            self.difference("retry_interval_out_of_range",
                            f"retryIntervalInSeconds={rv} is outside Fabric's documented range "
                            f"{RETRY_INTERVAL_MIN}-{RETRY_INTERVAL_MAX}; it is kept as authored.",
                            f"Set it between {RETRY_INTERVAL_MIN} and {RETRY_INTERVAL_MAX} seconds.")
        return out or None

    def envelope(self, name: str, fabric_type: str, type_properties: dict, *, primary: bool = True,
                 connection: Optional[str] = None, extra: Optional[dict] = None,
                 secure_policy_only: bool = False, policy: bool = True) -> dict:
        """The Fabric activity envelope. Only the primary component carries the source's state/policy."""
        d: dict[str, Any] = {"name": name, "type": fabric_type, "dependsOn": [], "typeProperties": type_properties}
        if primary:
            act = self.act
            if policy:
                pol = self.policy(secure_only=secure_policy_only)
                if pol:
                    d["policy"] = pol
            if act.state:
                state = "InActive" if act.state.lower() == "inactive" else "Active"
                d["state"] = state
                self.mapping("state", "state", "state_spelling",
                             "Fabric spells the inactive state 'InActive'" if state == "InActive" else "")
                mark = act.configuration.get("on_inactive_mark_as")
                if mark:
                    d["onInactiveMarkAs"] = mark
                    self.mapping("onInactiveMarkAs", "onInactiveMarkAs", "passthrough")
            if act.description:
                d["description"] = act.description
                self.mapping("description", "description", "passthrough")
        if connection:
            d["externalReferences"] = {"connection": connection}
        if extra:
            d.update(extra)
        return d

    # -- components ---------------------------------------------------------------------------------------------
    def add(self, config: dict, role: str = "primary") -> str:
        name = config["name"]
        persisted, children = strip_nested(config)
        self.components.append(FabricComponent(
            name=name, fabric_type=config["type"], role=role, configuration=persisted, children=children))
        self.full[name] = config
        return name

    def children(self, branch: str) -> list[dict]:
        """The converted nested activities of one branch (engine-provided)."""
        kids = self._children_fn(branch)
        self.child_names.extend(k.get("name", "") for k in kids)
        return kids

    def placeholder_component(self, code: str, reason: str) -> str:
        """The fail-fast stand-in for an activity that could not be converted.

        A Fail activity keeps the dependency graph intact and makes any run of the unfinished pipeline
        fail loudly at this point instead of silently skipping the operation.
        """
        a = self.act
        text = f"ADF activity '{a.name}' ({a.type}) was not converted: {reason} See the conversion report."
        cfg = {"name": a.name, "type": "Fail", "dependsOn": [],
               "typeProperties": {"message": text, "errorCode": f"ADF2FABRIC_{code.upper()}"}}
        if a.description:
            cfg["description"] = a.description
        return self.add(cfg, "placeholder")
