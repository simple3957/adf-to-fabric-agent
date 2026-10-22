"""Derive external references + typed relationship edges from parsed artifacts.

Everything here is a pure function of the already-parsed (secret-free) artifact
models. It records only relationships EXPLICITLY represented in the ADF JSON and
resolves references deterministically against the discovered artifact-name sets
(a ``resolved`` flag; never inferred). No new source reads, no Azure, no secrets.
"""

from __future__ import annotations

from urllib.parse import urlparse

from adf2fabric.project.discovery import (
    AdfActivity,
    AdfArtifactKind,
    AdfArtifactRef,
    AdfDataFlow,
    AdfDataset,
    AdfLinkedService,
    AdfPipeline,
    AdfRelationship,
    AdfRelationshipKind,
    AdfTrigger,
    ExternalReference,
    ExternalResourceKind,
)

# Linked-service type (lowercased substring) → how to record its endpoint.
_ADLS_LS_TYPES = ("azureblobfs", "azureblobstorage", "azuredatalakestore")
_KEYVAULT_LS_TYPES = ("azurekeyvault",)
_REST_LS_TYPES = ("restservice", "httpserver")


def _host(endpoint: str | None) -> str | None:
    if not endpoint:
        return None
    netloc = urlparse(endpoint).netloc
    return netloc or endpoint


def _walk_activities(activities: list[AdfActivity]):
    for a in activities:
        yield a
        yield from _walk_activities(a.activities)


def _pipeline_refs(pipeline: AdfPipeline) -> tuple[set[str], set[str]]:
    """(dataset names, linked-service names) referenced anywhere in the pipeline."""
    datasets: set[str] = set()
    linked: set[str] = set()
    for a in _walk_activities(pipeline.activities):
        datasets.update(a.dataset_refs)
        linked.update(a.linked_service_refs)
    return datasets, linked


def _ext_key(ref: ExternalReference) -> tuple:
    return (
        ref.resource_kind.value, ref.source_artifact, ref.source_kind.value,
        ref.sql_schema, ref.sql_table, ref.sql_stored_procedure,
        ref.adls_account, ref.adls_filesystem, ref.adls_path,
        ref.rest_base_url, ref.rest_relative_url,
        ref.key_vault_url, ref.key_vault_secret_name,
    )


def build_external_references(
    pipelines: list[AdfPipeline],
    datasets: list[AdfDataset],
    linked_services: list[AdfLinkedService],
) -> list[ExternalReference]:
    refs: list[ExternalReference] = []

    # Dataset asset identities (SQL table / ADLS path / REST relativeUrl).
    for d in datasets:
        if d.asset is not None:
            refs.append(d.asset)

    # Stored-procedure references from pipeline activities (SQL objects).
    for p in pipelines:
        procs: set[str] = set()
        for a in _walk_activities(p.activities):
            procs.update(a.stored_procedures)
        for sp in sorted(procs):
            refs.append(
                ExternalReference(
                    resource_kind=ExternalResourceKind.sql_object,
                    source_artifact=p.name,
                    source_kind=AdfArtifactKind.pipeline,
                    sql_stored_procedure=sp,
                )
            )

    # Linked-service endpoints (non-secret) + Key Vault references.
    for ls in linked_services:
        low = (ls.type or "").lower()
        if ls.endpoint and any(t in low for t in _ADLS_LS_TYPES):
            refs.append(
                ExternalReference(
                    resource_kind=ExternalResourceKind.adls_path,
                    source_artifact=ls.name,
                    source_kind=AdfArtifactKind.linked_service,
                    adls_account=_host(ls.endpoint),
                )
            )
        elif ls.endpoint and any(t in low for t in _KEYVAULT_LS_TYPES):
            refs.append(
                ExternalReference(
                    resource_kind=ExternalResourceKind.key_vault,
                    source_artifact=ls.name,
                    source_kind=AdfArtifactKind.linked_service,
                    key_vault_url=ls.endpoint,
                )
            )
        elif ls.endpoint and any(t in low for t in _REST_LS_TYPES):
            refs.append(
                ExternalReference(
                    resource_kind=ExternalResourceKind.rest_endpoint,
                    source_artifact=ls.name,
                    source_kind=AdfArtifactKind.linked_service,
                    rest_base_url=ls.endpoint,
                )
            )
        # Key Vault secret references declared on this linked service.
        for kv in ls.key_vault_refs:
            refs.append(
                ExternalReference(
                    resource_kind=ExternalResourceKind.key_vault,
                    source_artifact=ls.name,
                    source_kind=AdfArtifactKind.linked_service,
                    key_vault_secret_name=kv.secret_name,
                    note=f"via {kv.store_linked_service}",
                )
            )

    # Deduplicate + deterministic sort.
    seen: set[tuple] = set()
    unique: list[ExternalReference] = []
    for r in refs:
        k = _ext_key(r)
        if k not in seen:
            seen.add(k)
            unique.append(r)
    unique.sort(key=lambda r: tuple("" if v is None else str(v) for v in _ext_key(r)))
    return unique


def build_relationships(
    pipelines: list[AdfPipeline],
    datasets: list[AdfDataset],
    linked_services: list[AdfLinkedService],
    triggers: list[AdfTrigger],
    external_references: list[ExternalReference],
    dataflows: "list[AdfDataFlow] | tuple" = (),
) -> tuple[list[AdfRelationship], list[AdfArtifactRef]]:
    """Return (relationship edges, unresolved artifact references)."""
    pipeline_names = {p.name for p in pipelines}
    dataset_names = {d.name for d in datasets}
    ls_names = {ls.name for ls in linked_services}
    dataflow_names = {f.name for f in dataflows}

    edges: list[AdfRelationship] = []
    unresolved: list[AdfArtifactRef] = []

    def add(kind, s_kind, s_name, t_kind, t_name, target_names):
        resolved = t_name in target_names
        edges.append(
            AdfRelationship(
                relationship=kind, source_kind=s_kind, source_name=s_name,
                target_kind=t_kind, target_name=t_name, resolved=resolved,
            )
        )
        if not resolved and target_names is not None:
            unresolved.append(AdfArtifactRef(name=t_name, kind=t_kind, resolved=False))

    # trigger → pipeline
    for t in triggers:
        for pl in t.pipeline_refs:
            add(AdfRelationshipKind.trigger_triggers_pipeline, AdfArtifactKind.trigger, t.name,
                AdfArtifactKind.pipeline, pl, pipeline_names)

    # pipeline → pipeline / dataset / linked service
    for p in pipelines:
        for pl in p.pipeline_dependencies:
            add(AdfRelationshipKind.pipeline_executes_pipeline, AdfArtifactKind.pipeline, p.name,
                AdfArtifactKind.pipeline, pl, pipeline_names)
        ds_refs, ls_refs = _pipeline_refs(p)
        for ds in sorted(ds_refs):
            add(AdfRelationshipKind.pipeline_uses_dataset, AdfArtifactKind.pipeline, p.name,
                AdfArtifactKind.dataset, ds, dataset_names)
        for ls in sorted(ls_refs):
            add(AdfRelationshipKind.pipeline_uses_linked_service, AdfArtifactKind.pipeline, p.name,
                AdfArtifactKind.linked_service, ls, ls_names)

    # pipeline → data flow, and data flow → dataset / linked service / flowlet
    for p in pipelines:
        for name in sorted({r.name for a in _walk_activities(p.activities) for r in a.references if r.kind == "dataflow"}):
            add(AdfRelationshipKind.pipeline_executes_dataflow, AdfArtifactKind.pipeline, p.name,
                AdfArtifactKind.dataflow, name, dataflow_names)
    for f in dataflows:
        for name in f.dataset_refs:
            add(AdfRelationshipKind.dataflow_uses_dataset, AdfArtifactKind.dataflow, f.name,
                AdfArtifactKind.dataset, name, dataset_names)
        for name in f.linked_service_refs:
            add(AdfRelationshipKind.dataflow_uses_linked_service, AdfArtifactKind.dataflow, f.name,
                AdfArtifactKind.linked_service, name, ls_names)
        for name in f.flowlet_refs:
            add(AdfRelationshipKind.dataflow_uses_flowlet, AdfArtifactKind.dataflow, f.name,
                AdfArtifactKind.dataflow, name, dataflow_names)

    # dataset → linked service (also stamp the dataset ref's resolved flag)
    for d in datasets:
        if d.linked_service_ref is not None:
            name = d.linked_service_ref.name
            d.linked_service_ref.resolved = name in ls_names
            add(AdfRelationshipKind.dataset_uses_linked_service, AdfArtifactKind.dataset, d.name,
                AdfArtifactKind.linked_service, name, ls_names)

    # artifact → external asset (external targets are never "resolved")
    for r in external_references:
        edges.append(
            AdfRelationship(
                relationship=AdfRelationshipKind.artifact_references_external,
                source_kind=r.source_kind, source_name=r.source_artifact,
                target_kind=AdfArtifactKind.unknown, target_name=_external_label(r),
                resolved=False,
            )
        )

    edges.sort(key=lambda e: (e.relationship.value, e.source_name, e.target_name))
    # Dedup unresolved by (name, kind), deterministic order.
    seen: set[tuple] = set()
    unique_unresolved: list[AdfArtifactRef] = []
    for ref in sorted(unresolved, key=lambda r: (r.kind.value, r.name)):
        k = (ref.kind.value, ref.name)
        if k not in seen:
            seen.add(k)
            unique_unresolved.append(ref)
    return edges, unique_unresolved


def _external_label(r: ExternalReference) -> str:
    """A stable, secret-free label describing an external target."""
    if r.resource_kind is ExternalResourceKind.sql_object:
        if r.sql_stored_procedure:
            return r.sql_stored_procedure
        parts = [p for p in (r.sql_schema, r.sql_table) if p]
        return ".".join(parts) if parts else "sql_object"
    if r.resource_kind is ExternalResourceKind.adls_path:
        return r.adls_account or r.adls_path or r.adls_filesystem or "adls_path"
    if r.resource_kind is ExternalResourceKind.rest_endpoint:
        return r.rest_base_url or r.rest_relative_url or "rest_endpoint"
    if r.resource_kind is ExternalResourceKind.key_vault:
        return r.key_vault_url or r.key_vault_secret_name or "key_vault"
    return "external"
