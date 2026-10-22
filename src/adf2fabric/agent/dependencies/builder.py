"""Build the dependency inventory from the semantic discovery record and the source manifest.

A pure function: no network, no file access, no clock other than the ``now`` it is given. The same semantic record and
manifest always give the same inventory, whichever input adapter (live Azure, a Git repository, a ZIP) produced the source.
Access checks (``access.py``) are run separately and passed in; with none, every external target stays ``not_checked``.

What is read: the semantic record (artifacts, references, expressions, external references), the source manifest
(provenance, retrieval failures, validation findings). What is never read or stored: secret values, connection strings,
source data, notebook source.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable, Optional
from urllib.parse import urlparse

from adf2fabric.agent.conversion import connectors
from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import strongly_connected_components, walk
from adf2fabric.connections.catalog import connector_for
from adf2fabric.connections.integration_runtime import analyze as analyze_runtime
from adf2fabric.connections.integration_runtime import host_of
from adf2fabric.connections.resolution import usage as linked_service_usage
from adf2fabric.project.dependencies import (
    ARTIFACT_TYPES,
    AccessCheck,
    AccessStatus,
    AuthRequirement,
    Dependency,
    DependencyInventory,
    DependencyIssue,
    DependencyReference,
    DependencySummary,
    DependencyType as T,
    Finding,
    MigrationCategory as C,
    MigrationImplication,
    MigrationWave,
    PipelineOrder,
    Resolution as R,
    Severity,
    SourceGap,
    SourceProvenance,
)
from adf2fabric.project.discovery import (
    AdfActivity,
    AdfIntegrationRuntime,
    AdfLinkedService,
    AdfSemanticDiscovery,
    ExternalResourceKind,
    UNPROCESSED_LOSS_REASONS,
)
from adf2fabric.project.source import SourceManifest

AUTO_RESOLVE = "autoresolveintegrationruntime"
_SQL_PORT = 1433
_FAMILY = {"AzureSqlDatabase", "AzureSqlMI", "AzureSqlDW", "SqlServer", "AmazonRdsForSqlServer"}
_SQL_TYPES = {t.lower() for t in _FAMILY}
_ARM_COLLECTION = {"pipeline": "pipelines", "dataset": "datasets", "linkedservice": "linkedservices", "trigger": "triggers",
                   "integrationruntime": "integrationRuntimes", "dataflow": "dataflows", "factory": "factories"}
_KIND_TO_TYPE = {"pipeline": T.pipeline, "dataset": T.dataset, "linkedservice": T.linked_service, "trigger": T.trigger,
                 "integrationruntime": T.integration_runtime, "dataflow": T.data_flow}
_MI = {"msi", "managedidentity", "systemassignedmanagedidentity", "userassignedmanagedidentity"}
_SECRET_HINTS = ("password", "key", "secret", "token", "sas", "credential", "connectionstring", "pwd")


def dep_id(type_: T, name: str) -> str:
    return f"{type_.value}:{name.strip().lower()}"


def _impl(category: C, summary: str, actions: Iterable[str] = (), blocks: bool = False) -> MigrationImplication:
    return MigrationImplication(category=category, summary=summary, actions=list(actions), blocks=blocks)


# =====================================================================================================================
# authentication requirements (modes and field NAMES only)
# =====================================================================================================================


def classify_authentication(ls: AdfLinkedService) -> AuthRequirement:
    """How the linked service authenticates, from what the source states. Never reads or stores a value."""
    fields = [f for f in ls.connection_field_names]
    low = {f.lower() for f in fields}
    at = (ls.authentication_type or "").lower().replace(" ", "")
    secret_fields = sorted(f for f in fields if any(h in f.lower() for h in _SECRET_HINTS))
    vault = sorted(f"{k.store_linked_service}/{k.secret_name}" for k in ls.key_vault_refs)
    notes: list[str] = []
    if at in _MI:
        mode = "managed_identity"
        notes.append("Fabric connections can use the workspace identity; grant it access to the data source.")
    elif "serviceprincipal" in at or "serviceprincipalid" in low or "servicePrincipalKey".lower() in low:
        mode = "service_principal"
    elif vault and not [f for f in secret_fields if f.lower() not in ("password", "connectionstring")] and "encryptedcredential" not in low:
        mode = "key_vault"
    elif "accountkey" in low:
        mode = "account_key"
    elif any(f in low for f in ("sastoken", "sasuri")):
        mode = "sas"
    elif "password" in low or at in ("basic", "sql", "windows"):
        mode = "password"
    elif "accesstoken" in low or "oauth" in at:
        mode = "oauth"
    elif "encryptedcredential" in low:
        mode = "encrypted"
    elif at == "anonymous":
        mode = "none"
    else:
        mode = "unknown"
        notes.append("The source does not state how this linked service authenticates.")
    if "encryptedcredential" in low:
        notes.append("ADF stored a credential encrypted for the factory (encryptedCredential): it cannot be exported or "
                     "reused, so it must be entered again when the Fabric connection is created.")
    if "connectionstring" in low:
        notes.append("A connection string is present; this tool never reads its value, so server and database are not known "
                     "from the source.")
    if vault:
        notes.append("Secrets are held in Key Vault; the values are not read here.")
    needs_input = mode not in ("managed_identity", "none") and (bool(secret_fields) or "encryptedcredential" in low or mode == "unknown")
    return AuthRequirement(mode=mode, secret_fields=secret_fields, key_vault_secrets=vault, needs_user_input=needs_input,
                           notes=notes)


# =====================================================================================================================
# the graph being built
# =====================================================================================================================


class _Graph:
    def __init__(self, unavailable_kinds: set[T], unprocessed: dict[tuple[T, str], str]):
        self.deps: dict[str, Dependency] = {}
        self.edges: dict[str, set[str]] = defaultdict(set)  # dependency id -> dependency ids it depends on
        self.unavailable_kinds = unavailable_kinds
        self.unprocessed = unprocessed  # (type, lower name) -> why it is missing from the record

    def define(self, type_: T, name: str, *, defined_in: Optional[str], resolution: R = R.resolved,
               attributes: Optional[dict[str, str]] = None) -> Dependency:
        did = dep_id(type_, name)
        dep = self.deps.get(did)
        if dep is None:
            dep = Dependency(id=did, type=type_, name=name, resolution=resolution, defined_in=defined_in,
                             migration=_impl(C.informational, ""), attributes=dict(attributes or {}))
            self.deps[did] = dep
        else:  # a reference created it first; the definition now fills it in
            dep.resolution, dep.defined_in = resolution, defined_in
            dep.attributes.update(attributes or {})
        return dep

    def ref(self, type_: T, name: str, consumer_kind: str, consumer: str, role: str, *, file: Optional[str] = None,
            pointer: Optional[str] = None, resolution: Optional[R] = None, attributes: Optional[dict[str, str]] = None) -> Dependency:
        """Record that ``consumer`` references the dependency, creating it when no definition has been seen."""
        did = dep_id(type_, name)
        dep = self.deps.get(did)
        if dep is None:
            res = resolution or self._missing(type_, name)
            dep = Dependency(id=did, type=type_, name=name, resolution=res, migration=_impl(C.informational, ""),
                             attributes=dict(attributes or {}))
            self.deps[did] = dep
        elif attributes:
            for k, v in attributes.items():
                dep.attributes.setdefault(k, v)
        ref = DependencyReference(consumer_kind=consumer_kind, consumer=consumer, role=role, file=file, pointer=pointer)
        if ref not in dep.references:
            dep.references.append(ref)
        consumer_type = {"dataset": T.dataset, "linked_service": T.linked_service, "data_flow": T.data_flow,
                         "integration_runtime": T.integration_runtime, "trigger": T.trigger, "pipeline": T.pipeline}.get(consumer_kind)
        if consumer_type is not None and consumer_kind != "pipeline":
            self.edges[dep_id(consumer_type, consumer)].add(did)
        return dep

    def _missing(self, type_: T, name: str) -> R:
        """Why a referenced ADF artifact has no definition here: it does not exist, or the source could not be fully read."""
        if type_ in ARTIFACT_TYPES and ((type_, name.lower()) in self.unprocessed or type_ in self.unavailable_kinds):
            return R.unavailable
        return R.unresolved

    def issue(self, dep: Dependency, code: str, severity: Severity, message: str) -> None:
        item = DependencyIssue(code=code, severity=severity, message=message)
        if item not in dep.issues:
            dep.issues.append(item)


def _pointer(act: AdfActivity, path: Optional[str]) -> Optional[str]:
    if act.source_location is None:
        return None
    return f"{act.source_location.pointer}.{path}" if path else act.source_location.pointer


# =====================================================================================================================
# provenance and gaps (from the source manifest)
# =====================================================================================================================


def _provenance(d: AdfSemanticDiscovery, m: Optional[SourceManifest]) -> SourceProvenance:
    if m is None:
        return SourceProvenance(source_type="unknown", version=d.source_commit_sha, complete=True)
    return SourceProvenance(source_type=m.source_type.value, identifier=m.source_identifier, branch=m.branch,
                            version=m.version, retrieved_at=m.retrieved_at, complete=m.complete,
                            selection=len(m.selection.resolved_paths))


def _permission_for(manifest_type: str, kind: str) -> Optional[str]:
    if manifest_type == "azure_adf":
        collection = _ARM_COLLECTION.get(kind.lower().replace("_", ""), kind.lower() + "s")
        return f"Microsoft.DataFactory/factories/{collection}/read"
    return None


def _gaps(d: AdfSemanticDiscovery, m: Optional[SourceManifest]) -> list[SourceGap]:
    out: list[SourceGap] = []
    if m is not None:
        for f in m.failures:
            reason = "denied" if f.status_code in (401, 403) else "not_found" if f.status_code == 404 else "error"
            perm = _permission_for(m.source_type.value, f.kind) if reason == "denied" else None
            out.append(SourceGap(kind=f.kind, name=f.name, reason=reason, permission=perm,
                                 detail=f"{f.error_code or ''} {f.error}".strip()[:300], action=f.action))
        for fd in m.findings:
            if fd.code in ("missing_reference", "invalid_json", "duplicate_artifact"):
                out.append(SourceGap(kind="reference" if fd.code == "missing_reference" else "artifact", name=fd.artifact,
                                     reason=fd.code, detail=fd.message[:300], action=fd.action))
    for u in d.unprocessed_artifacts:
        if u.reason in UNPROCESSED_LOSS_REASONS:
            out.append(SourceGap(kind=u.kind, name=u.name or u.path, reason="unprocessed",
                                 detail=f"{u.path}: {u.reason}. {u.detail}".strip(), action="Fix or re-export this file, then run discovery again."))
    return sorted(out, key=lambda g: (g.kind, g.name or "", g.reason))


# =====================================================================================================================
# the build
# =====================================================================================================================


def build_inventory(d: AdfSemanticDiscovery, manifest: Optional[SourceManifest] = None,
                    checks: Optional[dict[str, AccessCheck]] = None, checks_requested: Iterable[str] = (),
                    now: Optional[datetime] = None, previous: Optional[DependencyInventory] = None) -> DependencyInventory:
    """The dependency inventory of ``d``. ``checks`` maps ``access_key(dep)`` to a completed probe."""
    now = now or datetime.now(timezone.utc)
    gaps = _gaps(d, manifest)
    unavailable_kinds: set[T] = set()
    for g in gaps:
        if g.reason in ("denied", "error") and g.name is None:
            t = _KIND_TO_TYPE.get(g.kind.lower().replace("_", ""))
            if t is not None:
                unavailable_kinds.add(t)
    unprocessed: dict[tuple[T, str], str] = {}
    for u in d.unprocessed_artifacts:
        t = _KIND_TO_TYPE.get(u.kind.lower())
        if t is not None and u.reason in UNPROCESSED_LOSS_REASONS:
            for n in {(u.name or "").lower(), u.path.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()} - {""}:
                unprocessed[(t, n)] = u.reason
    g = _Graph(unavailable_kinds, unprocessed)
    runtimes = {r.name.lower(): r for r in d.integration_runtimes}

    _define_artifacts(d, g)
    declared_globals = {p.name.lower(): (f.path) for f in d.factory for p in f.global_parameters}
    for f in d.factory:
        for p in f.global_parameters:
            g.define(T.global_parameter, p.name, defined_in=f.path)
    _pipeline_edges(d, g, declared_globals)
    _dataset_and_flow_edges(d, g)
    _linked_service_edges(d, g, runtimes)
    _trigger_edges(d, g)
    _external_dependencies(d, g)
    cycles = _mark_cycles(d, g)

    for dep in g.deps.values():
        _implication(dep, d, g, runtimes)
    _attach_checks(g, checks or {})
    findings = _findings(d, g, gaps, cycles)
    order, waves = _order_and_waves(d, g, cycles)

    deps = sorted(g.deps.values(), key=lambda x: x.id)
    for dep in deps:
        dep.references.sort(key=lambda r: (r.consumer_kind, r.consumer, r.role, r.pointer or ""))
        dep.consumers = sorted({f"{r.consumer_kind}:{r.consumer}" for r in dep.references})
    inv = DependencyInventory(
        project_id=d.project_id, source_commit_sha=d.source_commit_sha, created_at=now, updated_at=now,
        source=_provenance(d, manifest), dependencies=deps, findings=findings, source_gaps=gaps,
        cycles=[sorted(c) for c in cycles], pipeline_order=order, waves=waves,
        checks=sorted((c for c in (checks or {}).values()), key=lambda c: (c.provider, c.target)),
        checks_requested=sorted(set(checks_requested)))
    inv.summary = _summarize(inv)
    if previous is not None:  # keep timestamps stable when nothing changed
        inv.created_at = previous.created_at
        if _comparable(previous) == _comparable(inv):
            inv.updated_at = previous.updated_at
    return inv


def _comparable(inv: DependencyInventory) -> dict:
    import json

    data = json.loads(inv.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    for c in data.get("checks", []):
        c.pop("checked_at", None)
    for dep in data.get("dependencies", []):
        if dep.get("access"):
            dep["access"].pop("checked_at", None)
    return data


# --- 1. what the source defines ---------------------------------------------------------------------------------------


def _define_artifacts(d: AdfSemanticDiscovery, g: _Graph) -> None:
    for p in d.pipelines:
        g.define(T.pipeline, p.name, defined_in=p.path, attributes={"activities": str(p.activity_total)})
    for ds in d.datasets:
        g.define(T.dataset, ds.name, defined_in=ds.path, attributes={"type": ds.type or ""})
    for ls in d.linked_services:
        g.define(T.linked_service, ls.name, defined_in=ls.path, attributes={"type": ls.type or ""})
    for rt in d.integration_runtimes:
        g.define(T.integration_runtime, rt.name, defined_in=rt.path, attributes={"kind": rt.kind or "", "type": rt.type or ""})
    for f in d.dataflows:
        g.define(T.data_flow, f.name, defined_in=f.path, attributes={"flow_type": f.flow_type or ""})
    for t in d.triggers:
        g.define(T.trigger, t.name, defined_in=t.path, attributes={"type": t.type or "", "state": t.runtime_state or ""})


# --- 2. pipelines: activities, parameters, variables, expressions -------------------------------------------------------------


def _pipeline_edges(d: AdfSemanticDiscovery, g: _Graph, declared_globals: dict[str, str]) -> None:
    for p in d.pipelines:
        params = {x.name.lower() for x in p.parameters}
        variables = {x.name.lower() for x in p.variables}
        names = {a.name.lower() for a in walk(p.activities) if a.name}
        for act in walk(p.activities):
            file, ptr = p.path, _pointer(act, None)
            who = act.activity_id
            for name in act.pipeline_refs:
                g.ref(T.pipeline, name, "activity", who, "executePipeline", file=file, pointer=ptr)
            for name in act.dataset_refs:
                g.ref(T.dataset, name, "activity", who, "dataset", file=file, pointer=ptr)
            for name in act.linked_service_refs:
                g.ref(T.linked_service, name, "activity", who, "linkedServiceName", file=file, pointer=ptr)
            for r in act.references:
                if r.kind == "dataflow":
                    g.ref(T.data_flow, r.name, "activity", who, f"dataflow:{r.role}", file=file, pointer=_pointer(act, r.role))
                elif r.kind == "integrationRuntime":
                    _runtime_ref(g, r.name, "activity", who, r.role, file, _pointer(act, r.role))
                elif r.kind in ("notebook", "sparkJobDefinition", "bigDataPool", "sqlPool"):
                    g.ref(T.synapse_item, f"{r.kind}:{r.name}", "activity", who, r.role, file=file, pointer=_pointer(act, r.role),
                          resolution=R.external, attributes={"kind": r.kind})
            for e in act.expressions:
                for r in e.references:
                    where = _pointer(act, e.property_path)
                    role = f"expression:{e.property_path}"
                    if r.kind == "pipeline_parameter" and r.name:
                        g.ref(T.pipeline_parameter, f"{p.name}.{r.name}", "activity", who, role, file=file, pointer=where,
                              resolution=R.resolved if r.name.lower() in params else R.unresolved)
                    elif r.kind == "global_parameter" and r.name:
                        g.ref(T.global_parameter, r.name, "activity", who, role, file=file, pointer=where,
                              resolution=R.resolved if r.name.lower() in declared_globals else R.unresolved)
                    elif r.kind == "variable" and r.name:
                        g.ref(T.variable, f"{p.name}.{r.name}", "activity", who, role, file=file, pointer=where,
                              resolution=R.resolved if r.name.lower() in variables else R.unresolved)
                    elif r.kind == "activity_output" and r.name:
                        g.ref(T.activity_output, f"{p.name}/{r.name}", "activity", who, role, file=file, pointer=where,
                              resolution=R.resolved if r.name.lower() in names else R.unresolved)
            for v in act.variables_written:
                g.ref(T.variable, f"{p.name}.{v}", "activity", who, "writes", file=file, pointer=ptr,
                      resolution=R.resolved if v.lower() in variables else R.unresolved)


def _runtime_ref(g: _Graph, name: str, consumer_kind: str, consumer: str, role: str, file: Optional[str],
                 pointer: Optional[str]) -> None:
    if name.lower() == AUTO_RESOLVE:  # the implicit Azure runtime: nothing is defined in the source, and nothing is missing
        g.ref(T.integration_runtime, name, consumer_kind, consumer, role, file=file, pointer=pointer,
              resolution=R.resolved, attributes={"implicit": "true", "kind": "azure"})
    else:
        g.ref(T.integration_runtime, name, consumer_kind, consumer, role, file=file, pointer=pointer)


# --- 3. datasets and data flows ----------------------------------------------------------------------------------------------


def _dataset_and_flow_edges(d: AdfSemanticDiscovery, g: _Graph) -> None:
    for ds in d.datasets:
        if ds.linked_service_ref is not None:
            g.ref(T.linked_service, ds.linked_service_ref.name, "dataset", ds.name, "linkedServiceName", file=ds.path)
    for f in d.dataflows:
        for name in f.dataset_refs:
            g.ref(T.dataset, name, "data_flow", f.name, "dataset", file=f.path)
        for name in f.linked_service_refs:
            g.ref(T.linked_service, name, "data_flow", f.name, "linkedService", file=f.path)
        for name in f.flowlet_refs:
            g.ref(T.data_flow, name, "data_flow", f.name, "flowlet", file=f.path)


# --- 4. linked services: runtime, Key Vault, parameters -------------------------------------------------------------------------


def _linked_service_edges(d: AdfSemanticDiscovery, g: _Graph, runtimes: dict[str, AdfIntegrationRuntime]) -> None:
    for ls in d.linked_services:
        dep = g.deps[dep_id(T.linked_service, ls.name)]
        dep.authentication = classify_authentication(ls)
        dep.attributes.update({k: v for k, v in {
            "authentication_type": ls.authentication_type or "", "integration_runtime": ls.integration_runtime or "",
            "connector_version": ls.connector_version or "", "host": host_of(ls) or ""}.items() if v})
        host = dep.attributes.get("host")
        if host:  # a literal host gets a network probe (never a login)
            port = _SQL_PORT if (ls.type or "").lower() in _SQL_TYPES else 443
            dep.attributes.update({"access.provider": "network", "access.target": host, "access.port": str(port)})
        if ls.integration_runtime:
            _runtime_ref(g, ls.integration_runtime, "linked_service", ls.name, "connectVia", ls.path, None)
        for kv in ls.key_vault_refs:
            g.ref(T.linked_service, kv.store_linked_service, "linked_service", ls.name, "keyVaultStore", file=ls.path)
        if ls.parameters:
            ds_names, acts, args = linked_service_usage(d, ls.name)
            for p in ls.parameters:
                if (p.has_default and p.default_value) or args.get(p.name):
                    continue
                if ds_names or acts:
                    g.issue(dep, "parameter_without_value", Severity.warning,
                            f"Parameter '{p.name}' has no default and nothing passes a value: what this linked service "
                            "connects to is decided at run time.")


# --- 5. triggers ---------------------------------------------------------------------------------------------------------------


def _trigger_edges(d: AdfSemanticDiscovery, g: _Graph) -> None:
    declared = {p.name.lower(): {x.name.lower() for x in p.parameters} for p in d.pipelines}
    for t in d.triggers:
        for name in t.pipeline_refs:
            pipe = g.ref(T.pipeline, name, "trigger", t.name, "triggers", file=t.path)
            for arg in (t.pipeline_parameters.get(name) or {}):
                if name.lower() in declared and arg.lower() not in declared[name.lower()]:
                    g.issue(pipe, "trigger_argument_undeclared", Severity.warning,
                            f"Trigger '{t.name}' passes '{arg}', which the pipeline does not declare.")
        if t.event is not None and t.event.scope:
            host = _scope_name(t.event.scope)
            dep = g.ref(T.event_source, host, "trigger", t.name, "eventScope", file=t.path, resolution=R.external,
                        attributes={"events": ",".join(t.event.events)})
            dep.attributes["scope"] = t.event.scope.rsplit("/", 2)[-1] if "/" in t.event.scope else t.event.scope


def _scope_name(scope: str) -> str:
    return scope.rstrip("/").rsplit("/", 1)[-1] or scope


# --- 6. external systems -------------------------------------------------------------------------------------------------------


def _databricks_host(d: AdfSemanticDiscovery, ls_name: str) -> Optional[str]:
    ls = next((x for x in d.linked_services if x.name.lower() == ls_name.lower()), None)
    url = ((ls.compute.workspace if ls and ls.compute else None) or (ls.endpoint if ls else None)) or None
    if url and "@" in url:  # an expression: no single workspace
        return None
    if url and "://" not in url:
        url = f"https://{url}"
    if url and "://" in url:
        host = urlparse(url).hostname
        return host.lower() if host else None
    return None


def _external_dependencies(d: AdfSemanticDiscovery, g: _Graph) -> None:
    ls_by = {ls.name.lower(): ls for ls in d.linked_services}
    ds_by = {ds.name.lower(): ds for ds in d.datasets}
    # Databricks notebooks: the code lives in the workspace, not in ADF
    for p in d.pipelines:
        for act in walk(p.activities):
            if (act.type or "").lower() == "databricksnotebook":
                _notebook(d, g, p.path, act)
            elif (act.type or "").lower() in ("webactivity", "webhook"):
                _web(g, p.path, act)
    # Key Vault secrets named by linked services
    for ls in d.linked_services:
        for kv in ls.key_vault_refs:
            store = ls_by.get(kv.store_linked_service.lower())
            vault = _vault_of(store) if store else None
            label = vault or kv.store_linked_service
            dep = g.ref(T.key_vault_secret, f"{label}/{kv.secret_name}", "linked_service", ls.name, "secret", file=ls.path,
                        resolution=R.external, attributes={"secret": kv.secret_name, "vault_linked_service": kv.store_linked_service})
            if vault:
                dep.attributes.update({"vault_url": f"https://{vault}", "access.provider": "key_vault", "access.target": vault})
                dep.attributes["access.secret"] = kv.secret_name
    # references the semantic record already resolved to an external resource
    for er in d.external_references:
        who = er.source_artifact
        kind = {"dataset": "dataset", "linkedService": "linked_service", "pipeline": "pipeline", "trigger": "trigger"}.get(
            er.source_kind.value, "dataset")
        if er.resource_kind is ExternalResourceKind.adls_path and (er.adls_account or er.adls_filesystem):
            name = f"{er.adls_account or '(dynamic account)'}/{er.adls_filesystem or '(dynamic container)'}"
            dyn = er.is_expression_driven or not er.adls_account or not er.adls_filesystem
            dep = g.ref(T.storage_path, name, kind, who, "path", resolution=R.dynamic if dyn else R.external,
                        attributes={"account": er.adls_account or "", "filesystem": er.adls_filesystem or ""})
            if not dyn:
                dep.attributes.update({"access.provider": "storage", "access.target": f"{er.adls_account}/{er.adls_filesystem}"})
        elif er.resource_kind is ExternalResourceKind.sql_object:
            obj = er.sql_stored_procedure or ".".join(x for x in (er.sql_schema, er.sql_table) if x)
            if obj:
                ls_name = _linked_service_of(d, ds_by, er.source_artifact)
                dep = g.ref(T.sql_object, f"{ls_name or er.source_artifact}:{obj}", kind, who, "sql", resolution=R.external,
                            attributes={"object": obj, "stored_procedure": "true" if er.sql_stored_procedure else "false",
                                        "linked_service": ls_name or ""})
                if er.is_expression_driven:
                    dep.resolution = R.dynamic
        elif er.resource_kind is ExternalResourceKind.rest_endpoint and er.rest_base_url:
            host = (urlparse(er.rest_base_url).hostname or "").lower()
            if host:
                dep = g.ref(T.rest_endpoint, host, kind, who, "baseUrl", resolution=R.external, attributes={"host": host})
                dep.attributes.update({"access.provider": "network", "access.target": host, "access.port": "443"})


def _vault_of(ls: AdfLinkedService) -> Optional[str]:
    if ls.endpoint and "://" in ls.endpoint:
        host = urlparse(ls.endpoint).hostname
        return host.lower() if host else None
    return None


def _linked_service_of(d: AdfSemanticDiscovery, ds_by: dict, artifact: str) -> Optional[str]:
    ds = ds_by.get(artifact.lower())
    if ds is not None and ds.linked_service_ref is not None:
        return ds.linked_service_ref.name
    if any(ls.name.lower() == artifact.lower() for ls in d.linked_services):
        return artifact
    return None


def _notebook(d: AdfSemanticDiscovery, g: _Graph, file: str, act: AdfActivity) -> None:
    ls_name = act.linked_service_refs[0] if act.linked_service_refs else ""
    path = act.configuration.get("notebook_path")
    host = _databricks_host(d, ls_name) if ls_name else None
    who = act.activity_id
    if isinstance(path, dict) or path is None:  # an expression, or absent
        dep = g.ref(T.databricks_notebook, f"{ls_name or '(no linked service)'}:(dynamic: {who})", "activity", who, "notebookPath",
                    file=file, pointer=_pointer(act, "typeProperties.notebookPath"), resolution=R.dynamic,
                    attributes={"linked_service": ls_name})
        return
    dep = g.ref(T.databricks_notebook, f"{host or ls_name}:{path}", "activity", who, "notebookPath", file=file,
                pointer=_pointer(act, "typeProperties.notebookPath"), resolution=R.external,
                attributes={"path": str(path), "linked_service": ls_name, "workspace": host or ""})
    if host:
        dep.attributes.update({"access.provider": "databricks", "access.target": host, "access.path": str(path)})


def _web(g: _Graph, file: str, act: AdfActivity) -> None:
    host = act.configuration.get("host")
    dynamic = bool(act.configuration.get("url_is_dynamic"))
    who = act.activity_id
    if host and not dynamic:
        dep = g.ref(T.rest_endpoint, str(host).lower(), "activity", who, "url", file=file,
                    pointer=_pointer(act, "typeProperties.url"), resolution=R.external, attributes={"host": str(host).lower()})
        dep.attributes.update({"access.provider": "network", "access.target": str(host).lower(), "access.port": "443"})
    elif dynamic:
        g.ref(T.rest_endpoint, f"(dynamic: {who})", "activity", who, "url", file=file,
              pointer=_pointer(act, "typeProperties.url"), resolution=R.dynamic)


# --- 7. cycles -------------------------------------------------------------------------------------------------------------------


def _cycles_of(nodes: list[str], succ: dict[str, set[str]]) -> list[list[str]]:
    index = {n: i for i, n in enumerate(sorted(nodes))}
    back = {i: n for n, i in index.items()}
    graph = {i: {index[m] for m in succ.get(n, ()) if m in index} for n, i in index.items()}
    out = []
    for comp in strongly_connected_components(graph):
        if len(comp) > 1 or any(i in graph[i] for i in comp):
            out.append(sorted(back[i] for i in comp))
    return sorted(out)


def _pipeline_call_graph(d: AdfSemanticDiscovery, g: _Graph) -> dict[str, set[str]]:
    succ: dict[str, set[str]] = defaultdict(set)
    for dep in g.deps.values():
        if dep.type is T.pipeline:
            for r in dep.references:
                if r.consumer_kind == "activity" and r.role == "executePipeline":
                    parent = dep_id(T.pipeline, r.consumer.split("/", 1)[0])
                    succ[parent].add(dep.id)
    return succ


def _mark_cycles(d: AdfSemanticDiscovery, g: _Graph) -> list[list[str]]:
    cycles: list[list[str]] = []
    pipelines = [x.id for x in g.deps.values() if x.type is T.pipeline]
    cycles += _cycles_of(pipelines, _pipeline_call_graph(d, g))
    flows = [x.id for x in g.deps.values() if x.type is T.data_flow]
    flow_succ: dict[str, set[str]] = defaultdict(set)
    for dep in g.deps.values():
        if dep.type is T.data_flow:
            for r in dep.references:
                if r.consumer_kind == "data_flow" and r.role == "flowlet":
                    flow_succ[dep_id(T.data_flow, r.consumer)].add(dep.id)
    cycles += _cycles_of(flows, flow_succ)
    for comp in cycles:
        for did in comp:
            dep = g.deps[did]
            dep.cycle = comp
            if dep.resolution is R.resolved:
                dep.resolution = R.circular
    return cycles


# =====================================================================================================================
# migration implications
# =====================================================================================================================

_EVENT_TRIGGERS = {"blobeventstrigger", "customeventstrigger"}


def _implication(dep: Dependency, d: AdfSemanticDiscovery, g: _Graph, runtimes: dict[str, AdfIntegrationRuntime]) -> None:
    t = dep.type
    if dep.resolution in (R.unresolved, R.unavailable) and t in ARTIFACT_TYPES:
        why = ("the source could not be fully retrieved, so it may exist" if dep.resolution is R.unavailable
               else "no definition of that name is in the source")
        dep.migration = _impl(C.missing, f"Referenced, but {why}.", [
            f"Supply {t.value.replace('_', ' ')} '{dep.name}' (expected at {_expected_path(dep)}) and run discovery again."]
            if dep.resolution is R.unresolved else
            ["Fix the retrieval problem listed under the source gaps, then run discovery again."], blocks=True)
        g.issue(dep, "missing_definition" if dep.resolution is R.unresolved else "source_unavailable",
                Severity.error if dep.resolution is R.unresolved else Severity.warning,
                f"No definition of {t.value.replace('_', ' ')} '{dep.name}' is available: {why}.")
        return
    if t is T.linked_service:
        _linked_service_implication(dep, d, runtimes)
    elif t is T.dataset:
        dep.migration = _impl(C.informational, "Fabric has no datasets: each activity carries its dataset inline.")
    elif t is T.integration_runtime:
        _runtime_implication(dep, d)
    elif t is T.data_flow:
        dep.migration = _impl(C.recreate, "Fabric has no Mapping Data Flow runtime; the flow is re-authored.", [
            "Upgrade it with Fabric's Mapping Data Flow upgrade (Dataflow Gen2, preview) or re-author it, then bind the "
            "Dataflow item id."])
    elif t is T.pipeline:
        dep.migration = _impl(C.ready, "Converted by the normal conversion.")
    elif t is T.trigger:
        if (dep.attributes.get("type", "")).lower() in _EVENT_TRIGGERS:
            dep.migration = _impl(C.manual, "Event triggers have no direct Fabric pipeline trigger.",
                                  ["Recreate it as a Fabric Activator rule on the same event."])
        elif (dep.attributes.get("type", "")).lower() in ("scheduletrigger", "tumblingwindowtrigger"):
            dep.migration = _impl(C.ready, "Converted to a Fabric schedule, created disabled.")
        else:
            dep.migration = _impl(C.manual, f"Trigger type '{dep.attributes.get('type') or 'unknown'}' is not converted automatically.",
                                  ["Review it and recreate the schedule by hand."])
    elif t is T.global_parameter:
        if dep.resolution is R.unresolved:
            dep.migration = _impl(C.missing, "A global parameter is referenced but not declared in the factory.",
                                  ["Declare it, or remove the reference."])
            g.issue(dep, "undeclared_global_parameter", Severity.error, f"Global parameter '{dep.name}' is not declared.")
        else:
            dep.migration = _impl(C.ready, f"Fabric has no global parameters: it becomes the pipeline parameter gp_{dep.name}.")
    elif t in (T.pipeline_parameter, T.variable, T.activity_output):
        if dep.resolution is R.unresolved:
            noun = t.value.replace("_", " ")
            dep.migration = _impl(C.missing, f"A {noun} is referenced but not declared or defined.", ["Fix the reference in the source."])
            g.issue(dep, "undeclared_reference", Severity.error, f"{noun.capitalize()} '{dep.name}' is not declared.")
        else:
            dep.migration = _impl(C.informational, "Resolved inside the pipeline.")
    elif t is T.key_vault_secret:
        dep.migration = _impl(C.manual, "The secret value is not read by this tool.", [
            "Supply the credential when creating the Fabric connection that uses it."])
    elif t is T.databricks_notebook:
        if dep.resolution is R.dynamic:
            dep.migration = _impl(C.manual, "The notebook path is computed at run time, so which notebooks run is not known.",
                                  ["List the notebooks this activity can run and migrate each."])
        else:
            dep.migration = _impl(C.recreate, "The notebook lives in the Databricks workspace; it is exported and imported as a Fabric notebook.",
                                  ["Export it (fetch-databricks-notebooks), review the compatibility findings, then deploy it as a Fabric notebook."])
    elif t is T.synapse_item:
        dep.migration = _impl(C.recreate, f"A Synapse {dep.attributes.get('kind', 'item')} becomes a Fabric item.",
                              ["Re-create it in Fabric and bind its item id."])
    elif t is T.sql_object:
        dep.migration = _impl(C.manual if dep.resolution is not R.dynamic else C.manual,
                              "A database object outside ADF; it must exist where the Fabric connection points.",
                              ["Confirm it exists in the target database (or the Warehouse its data moves to)."])
    elif t is T.storage_path:
        dep.migration = _impl(C.manual, "Storage outside ADF; the identity that runs in Fabric needs access.", [
            "Grant that identity read/write on the container, or expose it through a OneLake shortcut."])
    elif t is T.rest_endpoint:
        dep.migration = _impl(C.manual if dep.resolution is R.dynamic else C.informational,
                              "A web endpoint called at run time." if dep.resolution is not R.dynamic else
                              "The endpoint is computed at run time.",
                              ["Confirm Fabric can reach it (network, authentication)."])
    elif t is T.event_source:
        dep.migration = _impl(C.manual, "An event source that triggers pipelines.", ["Recreate the subscription as a Fabric Activator rule."])
    if dep.resolution is R.dynamic:
        g.issue(dep, "dynamic_target", Severity.warning, "Which target this is depends on an expression: it is resolved only at run time.")
    if dep.resolution is R.circular:
        g.issue(dep, "circular_reference", Severity.error, f"Part of a reference cycle: {' -> '.join(dep.cycle)}.")
        dep.migration = _impl(C.manual, "Part of a reference cycle that cannot be ordered.",
                              ["Break the cycle in the source, or migrate the members together by hand."], blocks=True)


def _expected_path(dep: Dependency) -> str:
    folder = {T.pipeline: "pipeline", T.dataset: "dataset", T.linked_service: "linkedService",
              T.integration_runtime: "integrationRuntime", T.data_flow: "dataflow", T.trigger: "trigger"}[dep.type]
    return f"{folder}/{dep.name}.json"


def _linked_service_implication(dep: Dependency, d: AdfSemanticDiscovery, runtimes: dict[str, AdfIntegrationRuntime]) -> None:
    ls = next(x for x in d.linked_services if x.name.lower() == dep.name.lower())
    kind, why = connectors.verdict(ls.type)
    ra = analyze_runtime(ls, runtimes)
    actions: list[str] = []
    auth = dep.authentication
    if (ls.type or "").lower() == "azuresynapseartifacts":
        dep.migration = _impl(C.informational, "Used by Synapse notebook and Spark job activities, which become Fabric items: no connection is created for it.")
        return
    if kind == "unsupported":
        dep.migration = _impl(C.unsupported, why, ["Move the data to a supported store first, or keep this part outside Fabric."], blocks=True)
        return
    if ra.fabric_path == "unsupported":
        dep.migration = _impl(C.unsupported, ra.recommendation or "The integration runtime has no Fabric equivalent.", ra.requirements, blocks=True)
        return
    can_create = connector_for((ls.type or "").lower()) is not None
    if ra.fabric_path in ("on_premises_gateway", "virtual_network_gateway"):
        gateway = "an on-premises data gateway" if ra.fabric_path == "on_premises_gateway" else "a virtual network data gateway"
        dep.migration = _impl(C.needs_gateway, f"Reached through {gateway} in Fabric.", [ra.recommendation, *ra.requirements][:6])
        dep.migration.actions = [a for a in dep.migration.actions if a]
        return
    verb = ("create it with this tool or in Fabric" if can_create else "create it in Fabric and bind it")
    summary = (f"A Fabric connection is needed ({verb})." if kind == "supported"
               else f"A Fabric connection is needed; '{ls.type}' is not named in the Fabric connector documentation consulted, so check "
                    "that a connector exists.")
    actions.append(f"Create the Fabric connection for '{ls.name}' ({ls.type}).")
    if auth is not None and auth.needs_user_input:
        actions.append("Enter the credential again: it cannot be exported from ADF.")
    if ra.exposure in ("private", "unknown") and ra.host:
        actions.append(f"Confirm Fabric can reach {ra.host} (private endpoint or firewall).")
    dep.migration = _impl(C.needs_connection, summary, actions)


def _runtime_implication(dep: Dependency, d: AdfSemanticDiscovery) -> None:
    rt = next((x for x in d.integration_runtimes if x.name.lower() == dep.name.lower()), None)
    if rt is None:  # the implicit Azure runtime
        dep.migration = _impl(C.informational, "The default Azure runtime: Fabric runs on its own capacity.")
    elif rt.is_ssis:
        dep.migration = _impl(C.unsupported, "Azure-SSIS runtimes have no Fabric equivalent.", ["Move the packages to another service."], blocks=True)
    elif rt.kind == "self_hosted":
        dep.migration = _impl(C.needs_gateway, "A self-hosted runtime is not reusable by Fabric.", [
            "Install an on-premises data gateway on a machine that can reach the source, and create the connection through it."])
    elif rt.kind == "managed_vnet":
        dep.migration = _impl(C.needs_gateway, "Managed virtual networks and managed private endpoints have no direct Fabric equivalent.",
                              ["Use a virtual network data gateway in a network that can reach the resource."])
    else:
        dep.migration = _impl(C.informational, "An Azure runtime: Fabric runs on its own capacity.")


# =====================================================================================================================
# access checks, findings, ordering, summary
# =====================================================================================================================


def access_key(dep: Dependency) -> Optional[str]:
    """The key a probe result is filed under, or None when the dependency has nothing to check."""
    provider, target = dep.attributes.get("access.provider"), dep.attributes.get("access.target")
    if not provider or not target:
        return None
    extra = dep.attributes.get("access.path") or dep.attributes.get("access.secret") or dep.attributes.get("access.port") or ""
    return f"{provider}|{target}|{extra}"


def _attach_checks(g: _Graph, checks: dict[str, AccessCheck]) -> None:
    for dep in g.deps.values():
        key = access_key(dep)
        check = checks.get(key) if key else None
        if check is None:
            if key:
                dep.access = AccessCheck(provider=dep.attributes["access.provider"], target=dep.attributes["access.target"],
                                         status=AccessStatus.not_checked, probe="No check was requested; nothing external was contacted.")
            continue
        dep.access = check
        if check.status not in (AccessStatus.accessible, AccessStatus.not_checked):
            g.issue(dep, "inaccessible_external", Severity.warning,
                    f"{check.provider} check: {check.status.value}" + (f" (missing: {check.missing})" if check.missing else ""))
            if dep.type is T.databricks_notebook:
                dep.migration = _impl(C.needs_access, "The notebook's code cannot be retrieved with the identity used for this check.",
                                      [f"Grant: {check.missing or check.permission}", *dep.migration.actions])
            elif dep.migration.category in (C.manual, C.informational, C.needs_connection):
                dep.migration.actions.append(f"Access: {check.missing or check.status.value} ({check.provider} {check.target}).")


def _findings(d: AdfSemanticDiscovery, g: _Graph, gaps: list[SourceGap], cycles: list[list[str]]) -> list[Finding]:
    out: list[Finding] = []

    def add(code, sev, subject, message, dep=None, action=""):
        f = Finding(code=code, severity=sev, subject=subject, message=message, dependency=dep, action=action)
        if f not in out:
            out.append(f)

    for dep in g.deps.values():
        who = ", ".join(sorted({r.consumer for r in dep.references})[:3]) or "(none)"
        if dep.resolution is R.unresolved and dep.type in ARTIFACT_TYPES:
            add("missing_file", Severity.error, dep.name, f"{dep.type.value.replace('_', ' ').capitalize()} '{dep.name}' is referenced "
                f"(by {who}) but its definition file is not in the source.", dep.id, f"Add {_expected_path(dep)} to the source.")
        elif dep.resolution is R.unresolved:
            add("broken_reference", Severity.error, dep.name, f"{dep.type.value.replace('_', ' ').capitalize()} '{dep.name}' is "
                f"referenced (by {who}) but not declared.", dep.id, "Fix the reference or declare it.")
        elif dep.resolution is R.unavailable:
            add("source_unavailable", Severity.warning, dep.name, f"'{dep.name}' is referenced (by {who}); the source could not be "
                "fully retrieved, so it may exist.", dep.id, "Resolve the source gaps and run discovery again.")
        elif dep.resolution is R.dynamic:
            add("unresolved_expression", Severity.warning, dep.name, f"The target of {who} is computed at run time: "
                f"{dep.type.value.replace('_', ' ')} cannot be resolved statically.", dep.id, "List the possible values and migrate each.")
        if dep.type is T.linked_service and dep.migration.category is C.unsupported:
            add("unsupported_connection_type", Severity.error, dep.name, dep.migration.summary, dep.id,
                dep.migration.actions[0] if dep.migration.actions else "")
        if dep.type is T.integration_runtime and dep.migration.category is C.unsupported:
            add("unsupported_connection_type", Severity.error, dep.name, dep.migration.summary, dep.id,
                dep.migration.actions[0] if dep.migration.actions else "")
        for i in dep.issues:
            if i.code == "parameter_without_value":
                add("unresolved_expression", Severity.warning, dep.name, i.message, dep.id, "Give the parameter a default or pass a value.")
        if dep.access is not None and dep.access.status not in (AccessStatus.accessible, AccessStatus.not_checked):
            add("inaccessible_external", Severity.warning, dep.name,
                f"{dep.access.provider} {dep.access.target}: {dep.access.status.value}. {dep.access.detail}".strip(), dep.id,
                f"Missing: {dep.access.missing}" if dep.access.missing else "")
    for comp in cycles:
        add("circular_dependency", Severity.error, " -> ".join(comp), f"Circular reference among {len(comp)} item(s): {', '.join(comp)}.",
            comp[0], "Break the cycle in the source.")
    for p in d.pipelines:
        if p.dependency_graph is not None:
            for cyc in p.dependency_graph.cycles:
                add("circular_dependency", Severity.error, p.name, f"Activities depend on each other in a cycle: {', '.join(cyc)}.",
                    dep_id(T.pipeline, p.name), "Remove the circular dependsOn.")
        for act in walk(p.activities):
            for e in act.expressions:
                if e.parse_status == "unparsed":
                    add("unresolved_expression", Severity.warning, act.activity_id,
                        f"An expression at {e.property_path} could not be parsed and is carried as written.", dep_id(T.pipeline, p.name))
                for fn in e.unknown_functions:
                    add("unresolved_expression", Severity.warning, act.activity_id,
                        f"Function '{fn}' at {e.property_path} is not in the documented expression set.", dep_id(T.pipeline, p.name))
    for gap in gaps:
        add("source_gap", Severity.error if gap.reason in ("denied", "unprocessed") else Severity.warning,
            gap.name or gap.kind, f"{gap.kind}: {gap.reason}" + (f" (needs {gap.permission})" if gap.permission else "") +
            (f". {gap.detail}" if gap.detail else ""), None, gap.action)
    rank = {Severity.error: 0, Severity.warning: 1, Severity.info: 2}
    return sorted(out, key=lambda f: (rank[f.severity], f.code, f.subject, f.message))


def _depth(nodes: list[str], succ: dict[str, set[str]]) -> dict[str, int]:
    """Children-first depth: a node with no children is 0; a cycle shares one depth. ``succ`` maps parent -> children."""
    index = {n: i for i, n in enumerate(sorted(nodes))}
    graph = {i: {index[m] for m in succ.get(n, ()) if m in index} for n, i in index.items()}
    comps = strongly_connected_components(graph)
    comp_of = {i: c for c, comp in enumerate(comps) for i in comp}
    dag: dict[int, set[int]] = defaultdict(set)
    for i, outs in graph.items():
        for j in outs:
            if comp_of[i] != comp_of[j]:
                dag[comp_of[i]].add(comp_of[j])
    memo: dict[int, int] = {}

    def depth(c: int) -> int:
        if c in memo:
            return memo[c]
        memo[c] = 0 if not dag.get(c) else 1 + max(depth(x) for x in dag[c])
        return memo[c]

    back = {i: n for n, i in index.items()}
    return {back[i]: depth(comp_of[i]) for i in graph}


def _blocking_closure(g: _Graph) -> dict[str, set[str]]:
    """For each dependency id: the blocking dependencies reachable through dataset / linked-service / data-flow edges."""
    blocking = {did for did, dep in g.deps.items() if dep.migration.blocks}
    memo: dict[str, set[str]] = {}

    def reach(did: str, seen: frozenset) -> set[str]:
        if did in memo:
            return memo[did]
        found = {did} if did in blocking else set()
        for nxt in g.edges.get(did, ()):
            if nxt not in seen:
                found |= reach(nxt, seen | {did})
        memo[did] = found
        return found

    return {did: reach(did, frozenset()) for did in g.deps}


def _order_and_waves(d: AdfSemanticDiscovery, g: _Graph, cycles: list[list[str]]) -> tuple[list[PipelineOrder], list[MigrationWave]]:
    pipes = sorted(p.name for p in d.pipelines)
    ids = {dep_id(T.pipeline, n): n for n in pipes}
    calls: dict[str, set[str]] = defaultdict(set)
    called_by: dict[str, set[str]] = defaultdict(set)
    for parent, children in _pipeline_call_graph(d, g).items():
        for child in children:
            if parent in ids and child in ids:
                calls[ids[parent]].add(ids[child])
                called_by[ids[child]].add(ids[parent])
    cyclic = {n for comp in cycles for did in comp if did in ids for n in [ids[did]]}
    succ = {dep_id(T.pipeline, n): {dep_id(T.pipeline, c) for c in calls.get(n, ())} for n in pipes}
    depths = _depth(list(ids), succ)
    closure = _blocking_closure(g)

    consumed: dict[str, set[str]] = defaultdict(set)  # pipeline name -> dependency ids its activities / itself consume
    for dep in g.deps.values():
        for r in dep.references:
            if r.consumer_kind in ("activity", "pipeline"):
                consumed[r.consumer.split("/", 1)[0]].add(dep.id)
    blocked_direct: dict[str, list[str]] = {}
    for n in pipes:
        blockers: set[str] = set()
        for did in consumed.get(n, ()):
            blockers |= closure.get(did, set())
        blockers.discard(dep_id(T.pipeline, n))
        blocked_direct[n] = sorted(blockers)

    ordered = sorted(pipes, key=lambda n: (depths[dep_id(T.pipeline, n)], n.lower()))
    ready: dict[str, bool] = {}

    def is_ready(n: str, seen: frozenset) -> bool:
        if n in ready:
            return ready[n]
        ok = not blocked_direct[n] and n not in cyclic
        for c in calls.get(n, ()):
            if c not in seen and not is_ready(c, seen | {n}):
                ok = False
        ready[n] = ok
        return ok

    for n in ordered:
        is_ready(n, frozenset())

    # waves
    by_type: dict[T, list[Dependency]] = defaultdict(list)
    for dep in sorted(g.deps.values(), key=lambda x: x.id):
        by_type[dep.type].append(dep)
    plan: list[tuple[str, str, list[str], str]] = []
    plan.append(("Foundation", "foundation", [x.id for t in (T.integration_runtime, T.global_parameter) for x in by_type[t]],
                 "Runtimes and factory-wide parameters other things depend on."))
    plan.append(("Prerequisites outside ADF", "foundation",
                 [x.id for t in (T.key_vault_secret, T.storage_path, T.sql_object, T.rest_endpoint, T.event_source) for x in by_type[t]],
                 "Systems ADF reaches; confirm Fabric will have access before connections are created."))
    plan.append(("Connections", "connections", [x.id for x in by_type[T.linked_service]],
                 "One Fabric connection per linked service (credentials are entered again)."))
    plan.append(("Definitions", "definitions", [x.id for x in by_type[T.dataset]], "Datasets are inlined into activities; nothing to migrate."))
    flow_depth = _depth([x.id for x in by_type[T.data_flow]], {
        did: {r.consumer for r in ()} for did in []} or _flowlet_succ(g))
    items = sorted((x for t in (T.data_flow,) for x in by_type[t]), key=lambda x: (flow_depth.get(x.id, 0), x.id))
    items_ids = [x.id for x in items] + [x.id for t in (T.databricks_notebook, T.synapse_item) for x in by_type[t]]
    plan.append(("Items to recreate", "items", items_ids, "Data flows (flowlets first), notebooks and Spark jobs to re-create in Fabric."))
    waves: list[MigrationWave] = []
    for name, kind, members, note in plan:
        if members:
            waves.append(MigrationWave(number=len(waves) + 1, name=name, kind=kind, items=members, note=note))
    by_depth: dict[int, list[str]] = defaultdict(list)
    for n in ordered:
        by_depth[depths[dep_id(T.pipeline, n)]].append(n)
    wave_of: dict[str, int] = {}
    for depth in sorted(by_depth):
        number = len(waves) + 1
        waves.append(MigrationWave(
            number=number, name=f"Pipelines (level {depth})", kind="pipelines", items=by_depth[depth],
            note="Callers of nothing first." if depth == 0 else "Call only pipelines from earlier levels (or each other: see cycles)."))
        for n in by_depth[depth]:
            wave_of[n] = number
    triggers = [x.id for x in by_type[T.trigger]]
    if triggers:
        waves.append(MigrationWave(number=len(waves) + 1, name="Triggers", kind="triggers", items=triggers,
                                   note="Created disabled, after the pipelines they start."))
    order = [PipelineOrder(pipeline=n, position=i + 1, wave=wave_of[n], calls=sorted(calls.get(n, ())),
                           called_by=sorted(called_by.get(n, ())), blocked_by=blocked_direct[n], ready=ready[n],
                           in_cycle=n in cyclic) for i, n in enumerate(ordered)]
    return order, waves


def _flowlet_succ(g: _Graph) -> dict[str, set[str]]:
    succ: dict[str, set[str]] = defaultdict(set)
    for dep in g.deps.values():
        if dep.type is T.data_flow:
            for r in dep.references:
                if r.consumer_kind == "data_flow" and r.role == "flowlet":
                    succ[dep_id(T.data_flow, r.consumer)].add(dep.id)
    return succ


def _summarize(inv: DependencyInventory) -> DependencySummary:
    s = DependencySummary(total=len(inv.dependencies))
    for dep in inv.dependencies:
        s.by_type[dep.type.value] = s.by_type.get(dep.type.value, 0) + 1
        s.by_resolution[dep.resolution.value] = s.by_resolution.get(dep.resolution.value, 0) + 1
        s.by_category[dep.migration.category.value] = s.by_category.get(dep.migration.category.value, 0) + 1
        if dep.access is not None:
            s.by_access[dep.access.status.value] = s.by_access.get(dep.access.status.value, 0) + 1
        s.blocking += 1 if dep.migration.blocks else 0
    s.findings_error = sum(1 for f in inv.findings if f.severity is Severity.error)
    s.findings_warning = sum(1 for f in inv.findings if f.severity is Severity.warning)
    for k in ("by_type", "by_resolution", "by_category", "by_access"):
        setattr(s, k, dict(sorted(getattr(s, k).items())))
    return s
