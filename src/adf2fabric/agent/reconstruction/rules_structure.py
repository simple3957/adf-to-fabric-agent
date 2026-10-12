"""Reconstruction rules for structural gaps.

Each rule is a named, tested inference with an explicit evidence requirement:

    missing_artifact_reference   a dataset / linked service / pipeline / activity name that resolves
                                 to nothing  ->  a rename ONLY when exactly one existing artifact has
                                 the same name modulo separators/case AND is compatible with the use
    copy_endpoint_from_dataset   a Copy source/sink with no ``type`` (or no object) but a resolved
                                 dataset  ->  the source/sink type is UNIQUELY determined by the
                                 dataset type (and linked-service type for store settings)
    copy_endpoint_from_peers     a Copy with no dataset on one side  ->  the dataset other Copy
                                 activities pair with the known side
    dependency_from_reference    an expression reads activity('B').output but the activity does not
                                 depend on B  ->  add the dependency the reference implies
    lookup_shape_from_consumers  consumers read ``.output.value`` / ``.output.firstRow`` that the
                                 Lookup's firstRowOnly cannot deliver
    metadata_field_from_reads    consumers read a GetMetadata field that fieldList never requests
    argument_from_context        a child-pipeline / dataset parameter with no value  ->  the
                                 same-named pipeline parameter, a file-loop item, or peers' value

A rule proposes only what its evidence supports; anything else becomes a blocker that states the
exact information needed. Nothing is invented: no SQL, schema, table, credential or logic.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from adf2fabric.agent.reconstruction.context import PipelineCtx
from adf2fabric.agent.reconstruction.evidence import ev
from adf2fabric.agent.reconstruction.index import normalize
from adf2fabric.project.discovery import AdfActivity, AdfIO
from adf2fabric.project.reconstruction import (
    Alternative,
    EvidenceKind as K,
    IncompletenessClass as C,
    Severity,
)

_NESTED = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")

# --- uniquely determined type mappings (dataset type -> copy source / sink / settings types) --------------------

_SQL_ENDPOINTS = {
    "AzureSqlTable": ("AzureSqlSource", "AzureSqlSink"), "AzureSqlMITable": ("SqlMISource", "SqlMISink"),
    "AzureSqlDWTable": ("SqlDWSource", "SqlDWSink"), "SqlServerTable": ("SqlServerSource", "SqlServerSink"),
    "RestResource": ("RestSource", "RestSink"),
}
# format -> (source, sink, format-read-settings, format-write-settings)
_FORMAT_ENDPOINTS = {
    "DelimitedText": ("DelimitedTextSource", "DelimitedTextSink", "DelimitedTextReadSettings", "DelimitedTextWriteSettings"),
    "Parquet": ("ParquetSource", "ParquetSink", "ParquetReadSettings", "ParquetWriteSettings"),
    "Json": ("JsonSource", "JsonSink", "JsonReadSettings", "JsonWriteSettings"),
    "Orc": ("OrcSource", "OrcSink", None, "OrcWriteSettings"),
    "Avro": ("AvroSource", "AvroSink", None, "AvroWriteSettings"),
    "Binary": ("BinarySource", "BinarySink", "BinaryReadSettings", None),
}
_STORES = {
    "azureblobfs": "AzureBlobFS", "azureblobstorage": "AzureBlobStorage", "azurefilestorage": "AzureFileStorage",
    "sftp": "Sftp", "amazons3": "AmazonS3", "fileserver": "FileServer", "ftpserver": "FtpServer",
    "googlecloudstorage": "GoogleCloudStorage", "hdfs": "Hdfs",
}
# the dataset types each copy endpoint type is compatible with (for rename corroboration)
_COMPAT_SOURCE = {src: ds for ds, (src, _s) in _SQL_ENDPOINTS.items()}
_FILE_ISH = re.compile(r"^(file_?name|file|fname|blob_?name|source_?file(_?name)?|input_?file(_?name)?)$", re.I)


def endpoint_object(ds_type: str, ls_type: Optional[str], role: str) -> tuple[Optional[dict], list[str]]:
    """(object, information still required) for a Copy source/sink derived from a dataset type."""
    if ds_type in _SQL_ENDPOINTS:
        src, snk = _SQL_ENDPOINTS[ds_type]
        return {"type": src if role == "source" else snk}, []
    if ds_type in _FORMAT_ENDPOINTS:
        src, snk, rd, wr = _FORMAT_ENDPOINTS[ds_type]
        obj: dict[str, Any] = {"type": src if role == "source" else snk}
        info: list[str] = []
        store = _STORES.get((ls_type or "").lower())
        if store:
            obj["storeSettings"] = {"type": f"{store}{'Read' if role == 'source' else 'Write'}Settings"}
        else:
            info.append(f"the store settings type for linked service type '{ls_type or 'unknown'}'")
        fmt = rd if role == "source" else wr
        if fmt:
            obj["formatSettings"] = {"type": fmt}
        return obj, info
    return None, [f"the Copy {role} type for dataset type '{ds_type}'"]


# --- JSON helpers ---------------------------------------------------------------------------------------------

def find_ref_tokens(node: Any, kind: str, name: str, prefix: tuple = ()) -> list[list]:
    """Pointer tokens (relative to ``node``) of every ``<kind>Reference`` to ``name`` outside nested activities."""
    out: list[list] = []
    if isinstance(node, dict):
        if node.get("type") == f"{kind}Reference" and str(node.get("referenceName", "")).lower() == name.lower():
            out.append(list(prefix))
        for k, v in node.items():
            if k in _NESTED:
                continue
            if k == "cases" and isinstance(v, list):
                for i, c in enumerate(v):
                    if isinstance(c, dict):
                        for ck, cv in c.items():
                            if ck != "activities":
                                out += find_ref_tokens(cv, kind, name, (*prefix, k, i, ck))
                continue
            out += find_ref_tokens(v, kind, name, (*prefix, k))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            out += find_ref_tokens(v, kind, name, (*prefix, i))
    return out


def _dataset_ref_tokens(pc: PipelineCtx, a: AdfActivity, io: AdfIO) -> Optional[list]:
    node = pc.ev.act_json(pc.pipe, a)
    if node is None or not io.name:
        return None
    rels = find_ref_tokens(node, "Dataset", io.name)
    return [*pc.tokens(a), *rels[0]] if rels else None


def _is_copy(a: AdfActivity) -> bool:
    return (a.type or "").lower() == "copy"


def _kind_names(pc: PipelineCtx, kind: str) -> dict[str, str]:
    ev_ = pc.ev
    table = {"dataset": ev_.datasets, "linkedService": ev_.linked_services, "pipeline": ev_.pipelines}[kind]
    return {k: v.name for k, v in table.items()}


# =============================================================================================================
# missing artifact references
# =============================================================================================================


def rule_missing_artifacts(pc: PipelineCtx) -> None:
    ev_, p = pc.ev, pc.pipe
    broken = {normalize(c.name or c.path.rsplit("/", 1)[-1].rsplit(".", 1)[0]): c for c in ev_.broken_files()}
    for a in ev_.acts(p):
        node = ev_.act_json(p, a)
        if node is None:
            continue
        seen: set[tuple[str, str]] = set()
        refs: list[tuple[str, str, str]] = []  # (kind, name, where)
        for io in [*a.inputs, *a.outputs]:
            if io.kind == "dataset" and io.name and io.resolved is False:
                refs.append(("dataset", io.name, f"{io.role} dataset"))
        for r in a.references:
            if r.kind == "pipeline" and r.resolved is False:
                refs.append(("pipeline", r.name, "child pipeline"))
            elif r.kind == "linkedService" and r.resolved is False and r.role == "linkedServiceName":
                refs.append(("linkedService", r.name, "linked service"))
        for kind, name, where in refs:
            if (kind, name.lower()) in seen:
                continue
            seen.add((kind, name.lower()))
            _missing_one(pc, a, node, kind, name, where, broken)


def _compat_evidence(pc: PipelineCtx, a: AdfActivity, kind: str, cand_name: str) -> Optional[Any]:
    """Independent corroboration that the candidate fits how the reference is used (not just its name)."""
    ev_ = pc.ev
    if kind == "dataset":
        ds = ev_.datasets.get(cand_name.lower())
        if ds and _is_copy(a) and ds.type:
            expected: set[str] = set(_SQL_ENDPOINTS.get(ds.type, ())) | set(_FORMAT_ENDPOINTS.get(ds.type, ())[:2])
            used = {x for x in (a.source_type, a.sink_type) if x}
            if expected & used:
                return ev(K.referenced_artifact, f"dataset:{ds.name}",
                          f"dataset type {ds.type} matches the type this Copy reads or writes "
                          f"({', '.join(sorted(expected & used))})")
    if kind == "pipeline":
        child = ev_.pipelines.get(cand_name.lower())
        supplied = {k.lower() for k in (a.raw_properties.get("parameters") or {})}
        if child and supplied and supplied <= {x.name.lower() for x in child.parameters}:
            return ev(K.parent_child, f"pipeline:{child.name}",
                      f"declares every parameter this call passes ({', '.join(sorted(supplied))})")
    return None


def _missing_one(pc: PipelineCtx, a: AdfActivity, node: dict, kind: str, name: str, where: str, broken: dict) -> None:
    out, p = pc.out, pc.pipe
    key = normalize(name)
    if key in broken:
        c = broken[key]
        f = out.finding(C.invalid_json, "referenced_artifact_unreadable",
                        f"{a.type} '{a.name}' uses {kind} '{name}', but its file {c.path} could not be parsed.",
                        pipeline=p.name, activity=a, source_path=p.path, severity=Severity.error)
        out.blocker([f], pipeline=p.name, activity=a, reason=f"{kind} '{name}' exists but is unreadable",
                    information_required=[f"a valid copy of {c.path}"],
                    conversion_impact=f"{a.type} '{a.name}' cannot be converted without the {kind}.",
                    minimum_information=f"repair {c.path}", closest_safe_implementation="a fail-fast placeholder")
        return
    f = out.finding(C.missing_artifact, f"missing_{kind.lower()}",
                    f"{a.type} '{a.name}' references {where} '{name}', which is not among the discovered artifacts.",
                    pipeline=p.name, activity=a, source_path=p.path, severity=Severity.error)
    cands = [n for k, n in _kind_names(pc, kind).items() if normalize(n) == key and k != name.lower()]
    rels = find_ref_tokens(node, kind[0].upper() + kind[1:], name)
    if cands and rels:
        evidence = [ev(K.naming_convention, f"{kind}:{c}", f"'{c}' equals '{name}' ignoring case and separators")
                    for c in cands[:1]]
        corro = _compat_evidence(pc, a, kind, cands[0]) if len(cands) == 1 else None
        if corro:
            evidence.append(corro)
        tokens = [*pc.tokens(a), *rels[0], "referenceName"]
        unique = len(cands) == 1
        evidence = [ev(K.naming_convention, f"{kind}:{c}", f"'{c}' equals '{name}' ignoring case and separators")
                    for c in cands[: (1 if unique else len(cands))]] + (evidence[1:] if unique else [])
        prop = out.proposal(
            rule="missing_artifact_reference", finding=f, pipe=p, activity=a,
            missing_element=f"{kind} reference '{name}'", action="rename_reference",
            target_path=pc.pointer(tokens), proposed=cands[0] if unique else None, evidence=evidence,
            candidates=len(cands), patches=[pc.patch_set(tokens, cands[0])] if unique else [],
            assumptions=[f"'{cands[0]}' is the intended {kind}; the names differ only in case/separators"] if unique else [],
            alternatives=([Alternative(description=f"the reference is genuinely to a missing {kind}; add its file instead")]
                          if unique else [Alternative(description=f"use {kind} '{c}'", proposed=c) for c in cands]))
        if prop.safe_to_generate:
            return
    out.blocker([f], pipeline=p.name, activity=a, reason=f"{kind} '{name}' is not available and no safe substitute exists",
                information_required=[f"the definition of {kind} '{name}' (add its JSON file to the repository)"],
                conversion_impact=f"{a.type} '{a.name}' is converted to a fail-fast placeholder."
                if kind in ("dataset", "pipeline") else f"{a.type} '{a.name}' cannot be given a connection.",
                minimum_information=f"the {kind} definition file for '{name}'",
                closest_safe_implementation="a Fail placeholder that stops a run at this point")


# =============================================================================================================
# Copy endpoints
# =============================================================================================================


def rule_copy_endpoints(pc: PipelineCtx) -> None:
    ev_, out, p = pc.ev, pc.out, pc.pipe
    for a in ev_.acts(p):
        if not _is_copy(a):
            continue
        node = ev_.act_json(p, a)
        tp = node.get("typeProperties") if isinstance(node, dict) and isinstance(node.get("typeProperties"), dict) else {}
        for role, ios in (("source", a.inputs), ("sink", a.outputs)):
            cfg = tp.get(role)
            if isinstance(cfg, dict) and isinstance(cfg.get("type"), str) and cfg["type"]:
                continue
            path = f"typeProperties.{role}"
            f = out.finding(C.incomplete_configuration, f"copy_{role}_incomplete",
                            f"Copy '{a.name}' has no usable {role} definition ({path}).", pipeline=p.name, activity=a,
                            property_path=path, severity=Severity.error, source_path=p.path)
            io = ios[0] if ios and ios[0].kind == "dataset" and ios[0].name else None
            if io is None:
                _endpoint_from_peers(pc, a, f, role, cfg if isinstance(cfg, dict) else None)
            elif io.resolved is not True:
                out.blocker([f], pipeline=p.name, activity=a,
                            reason=f"the {role} dataset '{io.name}' is not available, so its type cannot determine the {role}",
                            information_required=[f"the definition of dataset '{io.name}'"],
                            conversion_impact="Copy cannot be converted.", minimum_information=f"dataset '{io.name}'",
                            closest_safe_implementation="a fail-fast placeholder")
            else:
                _endpoint_from_dataset(pc, a, f, role, io, cfg if isinstance(cfg, dict) else None)


def _store_type(pc: PipelineCtx, ds) -> Optional[str]:
    ls = pc.ev.linked_services.get(ds.linked_service_ref.name.lower()) if ds.linked_service_ref else None
    return ls.type if ls else None


def _endpoint_from_dataset(pc: PipelineCtx, a: AdfActivity, f, role: str, io: AdfIO, cfg: Optional[dict]) -> None:
    ds = pc.ev.datasets[io.name.lower()]
    obj, info = endpoint_object(ds.type or "", _store_type(pc, ds), role)
    if obj is None:
        pc.out.blocker([f], pipeline=pc.pipe.name, activity=a, reason=f"no known {role} type for dataset type {ds.type}",
                       information_required=info, conversion_impact="Copy cannot be converted faithfully.",
                       minimum_information=info[0], closest_safe_implementation="a fail-fast placeholder")
        return
    merged = {**(cfg or {}), **{k: v for k, v in obj.items() if k not in (cfg or {})}}
    tokens = pc.at(a, "typeProperties", role)
    evidence = [
        ev(K.explicit_property, f"{a.activity_id}.{'inputs' if role == 'source' else 'outputs'}[0]",
           f"the activity references dataset '{ds.name}' as its {role}"),
        ev(K.referenced_artifact, f"dataset:{ds.name}", f"dataset type {ds.type}"
           + (f" on linked service type {_store_type(pc, ds)}" if _store_type(pc, ds) else "")),
    ]
    pc.out.proposal(
        rule="copy_endpoint_from_dataset", finding=f, pipe=pc.pipe, activity=a,
        missing_element=f"Copy {role} definition", action="set_property", target_path=pc.pointer(tokens),
        proposed=merged, evidence=evidence, determined=True, information_required=info,
        patches=[pc.patch_set(tokens, merged)],
        assumptions=["only the connector types are inferred; query, partitioning and other settings stay as authored"])


def _endpoint_from_peers(pc: PipelineCtx, a: AdfActivity, f, role: str, cfg: Optional[dict]) -> None:
    other = (a.inputs if role == "sink" else a.outputs)
    anchor = other[0] if other and other[0].kind == "dataset" and other[0].name else None
    if anchor is None:
        pc.out.blocker([f], pipeline=pc.pipe.name, activity=a, reason="the Copy has no dataset on either side",
                       information_required=[f"the {role} dataset"], conversion_impact="Copy cannot be converted.",
                       minimum_information=f"a {role} dataset reference", closest_safe_implementation="a fail-fast placeholder")
        return
    peers: list[tuple[AdfActivity, str, AdfIO]] = []  # (activity, pipeline, its dataset on the missing side)
    for q in pc.ev.discovery.pipelines:
        for b in pc.ev.acts(q):
            if b is a or not _is_copy(b):
                continue
            known = (b.inputs if role == "sink" else b.outputs)
            wanted = (b.outputs if role == "sink" else b.inputs)
            if known and known[0].name and known[0].name.lower() == anchor.name.lower() and wanted and wanted[0].name:
                peers.append((b, q.name, wanted[0]))
    names = sorted({io.name for _, _, io in peers})
    evidence = [ev(K.repeated_pattern, f"{q}/{b.name}",
                   f"copies {'from' if role == 'sink' else 'into'} '{anchor.name}' paired with dataset '{io.name}'")
                for b, q, io in peers]
    info: list[str] = []
    proposed: Any = None
    patches = []
    if len(names) == 1:
        ds = pc.ev.datasets.get(names[0].lower())
        ref = {"referenceName": names[0], "type": "DatasetReference"}
        if ds and ds.parameters:
            args = [dict(io.parameters) for _, _, io in peers]
            if all(x == args[0] for x in args) and args[0]:
                ref["parameters"] = args[0]
            missing = [x.name for x in ds.parameters if not x.has_default and x.name not in (args[0] if args else {})]
            if missing:
                info.append(f"values for dataset parameter(s) {', '.join(missing)}")
        key = "outputs" if role == "sink" else "inputs"
        obj = None
        if ds and ds.type:
            obj, oinfo = endpoint_object(ds.type, _store_type(pc, ds), role)
            info += oinfo
        proposed = {key: [ref], f"typeProperties.{role}": obj}
        patches = [pc.patch_set(pc.at(a, key), [ref])]
        if obj is not None:
            patches.append(pc.patch_set(pc.at(a, "typeProperties", role), {**(cfg or {}), **obj}))
    pc.out.proposal(
        rule="copy_endpoint_from_peers", finding=f, pipe=pc.pipe, activity=a,
        missing_element=f"Copy {role} dataset", action="set_property",
        target_path=pc.pointer(pc.at(a, "outputs" if role == "sink" else "inputs")), proposed=proposed,
        evidence=evidence, candidates=len(names), information_required=info, patches=patches,
        assumptions=[f"this Copy writes where the other Copy activities reading '{anchor.name}' write"]
        if role == "sink" else [f"this Copy reads what the other Copy activities writing '{anchor.name}' read"],
        alternatives=[Alternative(description=f"use dataset '{n}'") for n in names] if len(names) > 1 else [])
    if not names:
        pc.out.blocker([f], pipeline=pc.pipe.name, activity=a,
                       reason=f"no other Copy activity pairs with '{anchor.name}', so the {role} cannot be inferred",
                       information_required=[f"the {role} dataset"], conversion_impact="Copy cannot be converted.",
                       minimum_information=f"a {role} dataset reference", closest_safe_implementation="a fail-fast placeholder")


# =============================================================================================================
# dependencies implied by output references
# =============================================================================================================


def rule_dependencies(pc: PipelineCtx) -> None:
    ev_, out, p = pc.ev, pc.out, pc.pipe
    acts = ev_.acts(p)
    names = {x.name.lower(): x for x in acts}
    handled: set[tuple[str, str]] = set()
    for a in acts:
        for dep in a.depends_on:
            if not dep.resolved:
                f = out.finding(C.incomplete_configuration, "dependency_undefined",
                                f"'{a.name}' depends on '{dep.activity}', which is not defined in the same activities list.",
                                pipeline=p.name, activity=a, property_path="dependsOn", severity=Severity.error,
                                source_path=p.path)
                out.blocker([f], pipeline=p.name, activity=a, reason=f"dependency '{dep.activity}' does not exist",
                            information_required=[f"the activity '{dep.activity}' or confirmation to remove the dependency"],
                            conversion_impact="The dependency cannot be mapped; ordering would silently change.",
                            minimum_information="define the activity or drop the dependency",
                            closest_safe_implementation="the dependency is reported and left unmapped")
        reads: dict[str, list[str]] = {}
        written: dict[str, str] = {}  # lowercase -> the name as the author wrote it
        for e in a.expressions:
            for r in e.references:
                if r.kind == "activity_output" and r.name:
                    reads.setdefault(r.name.lower(), []).append(r.path or "")
                    written.setdefault(r.name.lower(), r.name)
        for tname, paths in sorted(reads.items()):
            target = names.get(tname)
            if target is None:
                f = out.finding(C.incomplete_configuration, "activity_reference_unresolved",
                                f"'{a.name}' reads the output of '{written[tname]}', which is not an activity of this pipeline.",
                                pipeline=p.name, activity=a, severity=Severity.error, source_path=p.path)
                out.blocker([f], pipeline=p.name, activity=a, reason=f"activity '{written[tname]}' is not defined",
                            information_required=[f"the activity '{written[tname]}', or the intended activity name"],
                            conversion_impact="The expression cannot resolve; it is kept verbatim and fails validation.",
                            minimum_information="the correct activity name", closest_safe_implementation="expression preserved")
                continue
            if target is a or target in ev_.descendants_of(p, a) or tname in ev_.upstream(p, a):
                continue
            chain = [a, *ev_.ancestors(p, a)]
            owner = next((x for x in chain if x.parent_activity_id == target.parent_activity_id and x.branch == target.branch), None)
            f = out.finding(C.incomplete_configuration, "dependency_missing_for_reference",
                            f"'{a.name}' reads the output of '{target.name}' but does not depend on it.",
                            pipeline=p.name, activity=a, property_path="dependsOn", severity=Severity.error,
                            source_path=p.path)
            if owner is None:
                out.blocker([f], pipeline=p.name, activity=a, reason="the producer is in a different container scope",
                            information_required=["where the dependency should be expressed"],
                            conversion_impact="Fabric dependencies cannot cross container boundaries.",
                            minimum_information="restructure so both activities share a scope",
                            closest_safe_implementation="flagged; no dependency is added")
                continue
            if (owner.activity_id, target.name.lower()) in handled:
                continue
            handled.add((owner.activity_id, target.name.lower()))
            if owner.name.lower() in ev_.upstream(p, target):
                out.blocker([f], pipeline=p.name, activity=a, reason="adding the dependency would create a cycle",
                            information_required=["the intended execution order"],
                            conversion_impact="The dependency graph is circular.",
                            minimum_information="the order in which the two activities should run",
                            closest_safe_implementation="flagged; no dependency is added")
                continue
            all_paths = paths
            errs = any(x.startswith(".error") for x in all_paths)
            outs = any(x.startswith(".output") or x == "" for x in all_paths)
            cond = ["Completed"] if (errs and outs) else ["Failed"] if errs else ["Succeeded"]
            evidence = [ev(K.same_pipeline_reference, f"{a.activity_id}",
                           f"{e.property_path} reads activity('{target.name}'){pth}")
                        for e in a.expressions for r in e.references
                        if r.kind == "activity_output" and (r.name or "").lower() == tname
                        for pth in [r.path or ""]]
            dtoks = [*pc.tokens(owner), "dependsOn"]
            value = {"activity": target.name, "dependencyConditions": cond}
            out.proposal(
                rule="dependency_from_reference", finding=f, pipe=p, activity=owner,
                missing_element=f"dependency of '{owner.name}' on '{target.name}'", action="add_dependency",
                target_path=pc.pointer(dtoks), proposed=value, evidence=evidence, determined=True,
                patches=[pc.patch_append(dtoks, value)],
                assumptions=[f"condition {cond[0]} follows from reading "
                             f"{'the error' if cond == ['Failed'] else 'the output'} of '{target.name}'"],
                alternatives=[Alternative(description="condition Completed (run regardless of the outcome)",
                                          proposed={**value, "dependencyConditions": ["Completed"]})]
                if cond != ["Completed"] else [],
                affects=[owner.activity_id])


# =============================================================================================================
# output contracts
# =============================================================================================================


def _read_kind(path: str) -> Optional[str]:
    if path.startswith((".output.value", ".output.count")):
        return "all"
    if path.startswith(".output.firstRow"):
        return "first"
    return None


def rule_contracts(pc: PipelineCtx) -> None:
    ev_, p = pc.ev, pc.pipe
    by_id = ev_.by_id(p)
    for a in ev_.acts(p):
        t = (a.type or "").lower()
        if t == "lookup":
            _lookup_shape(pc, a, by_id)
        elif t == "getmetadata":
            _metadata_fields(pc, a, by_id)


def _reads(a: AdfActivity, by_id) -> list[tuple[AdfActivity, str, str]]:
    out = []
    for u in a.output_consumers:
        if u.via != "activity_output":
            continue
        c = by_id.get(u.activity_id)
        if c is not None:
            out.append((c, u.property_path, u.detail))
    return out


def _lookup_shape(pc: PipelineCtx, a: AdfActivity, by_id) -> None:
    reads = [(c, pth, d, _read_kind(d)) for c, pth, d in _reads(a, by_id)]
    needs_all = [r for r in reads if r[3] == "all"]
    needs_first = [r for r in reads if r[3] == "first"]
    if not (needs_all or needs_first):
        return
    explicit = a.raw_properties.get("firstRowOnly")
    explicit_bool = explicit if isinstance(explicit, bool) else None
    if isinstance(explicit, dict):  # dynamic: cannot judge
        return
    effective = True if explicit_bool is None else explicit_bool
    path = "typeProperties.firstRowOnly"
    ev_all = [ev(K.same_pipeline_reference, f"{c.activity_id}", f"{pth} reads {d}") for c, pth, d, _ in needs_all]
    ev_first = [ev(K.same_pipeline_reference, f"{c.activity_id}", f"{pth} reads {d}") for c, pth, d, _ in needs_first]
    p = pc.pipe
    if needs_all and needs_first:
        f = pc.out.finding(C.incomplete_configuration, "contract_lookup_shape",
                           f"Lookup '{a.name}' is read both as all rows (.output.value) and as a single row "
                           "(.output.firstRow); one Lookup cannot return both.", pipeline=p.name, activity=a,
                           property_path=path, severity=Severity.error, source_path=p.path)
        pc.out.blocker([f], pipeline=p.name, activity=a, reason="consumers disagree about the Lookup's output shape",
                       information_required=["whether the Lookup should return the first row or all rows"],
                       conversion_impact="A consumer will read a field that does not exist.",
                       minimum_information="choose one shape and align the consumers",
                       closest_safe_implementation="converted as authored; flagged")
        return
    if needs_all and effective:
        f = pc.out.finding(C.incomplete_configuration, "contract_lookup_shape",
                           f"Lookup '{a.name}' returns only the first row"
                           f"{' (default)' if explicit_bool is None else ''} but consumers read .output.value.",
                           pipeline=p.name, activity=a, property_path=path, severity=Severity.error, source_path=p.path)
        evidence = list(ev_all)
        if explicit_bool is not None:
            evidence.append(ev(K.explicit_property, a.activity_id, "firstRowOnly is explicitly true"))
        toks = pc.at(a, "typeProperties", "firstRowOnly")
        pc.out.proposal(
            rule="lookup_shape_from_consumers", finding=f, pipe=p, activity=a,
            missing_element="firstRowOnly=false", action="set_property", target_path=pc.pointer(toks), proposed=False,
            evidence=evidence, candidates=1 if explicit_bool is None else 2, determined=True,
            patches=[pc.patch_set(toks, False)],
            assumptions=["the consumers, not the (default) setting, express the intent"],
            alternatives=[Alternative(description="keep first-row-only and change the consumers to .output.firstRow")]
            if explicit_bool is not None else [])
        if explicit_bool is not None:
            pc.out.blocker([f], pipeline=p.name, activity=a, reason="the explicit setting and the consumers disagree",
                           information_required=["whether firstRowOnly or the consumers are correct"],
                           conversion_impact="Consumers read a field the Lookup will not return.",
                           minimum_information="confirm the intended shape", closest_safe_implementation="as authored; flagged")
    elif needs_first and not effective:
        f = pc.out.finding(C.incomplete_configuration, "contract_lookup_shape",
                           f"Lookup '{a.name}' has firstRowOnly=false but consumers read .output.firstRow.",
                           pipeline=p.name, activity=a, property_path=path, severity=Severity.error, source_path=p.path)
        toks = pc.at(a, "typeProperties", "firstRowOnly")
        pc.out.proposal(
            rule="lookup_shape_from_consumers", finding=f, pipe=p, activity=a, missing_element="firstRowOnly=true",
            action="set_property", target_path=pc.pointer(toks), proposed=True,
            evidence=[*ev_first, ev(K.explicit_property, a.activity_id, "firstRowOnly is explicitly false")],
            candidates=2, patches=[pc.patch_set(toks, True)],
            alternatives=[Alternative(description="keep all rows and change the consumers to .output.value[0]")])
        pc.out.blocker([f], pipeline=p.name, activity=a, reason="the explicit setting and the consumers disagree",
                       information_required=["whether firstRowOnly or the consumers are correct"],
                       conversion_impact="Consumers read a field the Lookup will not return.",
                       minimum_information="confirm the intended shape", closest_safe_implementation="as authored; flagged")


_DEFAULT_METADATA = {"itemname", "itemtype"}
_METADATA_FIELDS = {"exists", "childitems", "size", "lastmodified", "created", "structure", "columncount",
                    "contentmd5", "itemname", "itemtype"}


def _metadata_fields(pc: PipelineCtx, a: AdfActivity, by_id) -> None:
    fl = a.raw_properties.get("fieldList")
    if fl is not None and not isinstance(fl, list):
        return  # dynamic field list
    have = {str(x).lower() for x in (fl or []) if isinstance(x, str)}
    wanted: dict[str, list] = {}
    for c, pth, d in _reads(a, by_id):
        m = re.match(r"\.output\.(\w+)", d or "")
        if m and m.group(1).lower() in _METADATA_FIELDS:
            wanted.setdefault(m.group(1), []).append((c, pth, d))
    for field, users in sorted(wanted.items()):
        if field.lower() in have or field.lower() in _DEFAULT_METADATA:
            continue
        path = "typeProperties.fieldList"
        f = pc.out.finding(C.incomplete_configuration, "contract_metadata_field",
                           f"GetMetadata '{a.name}' does not request '{field}', but {users[0][0].name} reads it.",
                           pipeline=pc.pipe.name, activity=a, property_path=path, severity=Severity.error,
                           source_path=pc.pipe.path)
        lt = pc.at(a, "typeProperties", "fieldList")
        pc.out.proposal(
            rule="metadata_field_from_reads", finding=f, pipe=pc.pipe, activity=a,
            missing_element=f"fieldList entry '{field}'", action="set_property", target_path=pc.pointer(lt),
            proposed=field,
            evidence=[ev(K.same_pipeline_reference, c.activity_id, f"{pth} reads {d}") for c, pth, d in users],
            determined=True, patches=[pc.patch_append(lt, field)],
            assumptions=[], alternatives=[Alternative(description="remove the consumer's read of that field")])


# =============================================================================================================
# arguments
# =============================================================================================================


def rule_arguments(pc: PipelineCtx) -> None:
    ev_, p = pc.ev, pc.pipe
    for a in ev_.acts(p):
        if (a.type or "").lower() == "executepipeline":
            _child_arguments(pc, a)
        for io in [*a.inputs, *a.outputs]:
            if io.kind == "dataset" and io.resolved is True:
                _dataset_arguments(pc, a, io)


def _pipeline_param_candidates(pc: PipelineCtx, name: str, type_: Optional[str]) -> list:
    return [x for x in pc.pipe.parameters if x.name.lower() == name.lower()
            and (not x.type or not type_ or x.type.lower() == type_.lower())]


def _child_arguments(pc: PipelineCtx, a: AdfActivity) -> None:
    child_name = a.configuration.get("pipeline")
    child = pc.ev.pipelines.get(str(child_name).lower()) if child_name else None
    if child is None:
        return
    supplied = {k.lower() for k in (a.raw_properties.get("parameters") or {})}
    for prm in child.parameters:
        if prm.has_default or prm.name.lower() in supplied:
            continue
        path = f"typeProperties.parameters.{prm.name}"
        f = pc.out.finding(C.incomplete_configuration, "child_parameter_not_supplied",
                           f"ExecutePipeline '{a.name}' does not pass '{prm.name}' to '{child.name}', which requires it.",
                           pipeline=pc.pipe.name, activity=a, property_path=path, severity=Severity.error,
                           source_path=pc.pipe.path)
        cands = _pipeline_param_candidates(pc, prm.name, prm.type)
        if len(cands) == 1:
            toks = pc.at(a, "typeProperties", "parameters", prm.name)
            val = {"value": f"@pipeline().parameters.{cands[0].name}", "type": "Expression"}
            pc.out.proposal(
                rule="argument_from_context", finding=f, pipe=pc.pipe, activity=a,
                missing_element=f"argument '{prm.name}' for '{child.name}'", action="set_argument",
                target_path=pc.pointer(toks), proposed=val,
                evidence=[ev(K.parameter, f"pipeline:{pc.pipe.name}", f"declares parameter '{cands[0].name}' "
                             f"({cands[0].type}) with the same name and type"),
                          ev(K.parent_child, f"pipeline:{child.name}", f"requires parameter '{prm.name}' ({prm.type})")],
                patches=[pc.patch_set(toks, val)], material_assumptions=True,
                assumptions=["the parent's same-named parameter carries the value the child expects"],
                alternatives=[Alternative(description="a constant or an upstream activity's output is intended")])
        else:
            pc.out.blocker([f], pipeline=pc.pipe.name, activity=a,
                           reason=f"no parameter or value in the project supplies '{prm.name}'",
                           information_required=[f"the value (or expression) for child parameter '{prm.name}'"],
                           conversion_impact=f"'{child.name}' would run without a required parameter.",
                           minimum_information=f"an argument for '{prm.name}'",
                           closest_safe_implementation="InvokePipeline without that parameter; flagged")


def _file_loop_item(pc: PipelineCtx, a: AdfActivity):
    """(ForEach, GetMetadata, items-expression) when ``a`` runs inside a loop over a childItems file list.

    A Filter between the metadata and the loop is followed through (it passes items unchanged).
    """
    ev_, p = pc.ev, pc.pipe
    names = {x.name.lower(): x for x in ev_.acts(p)}
    for anc in ev_.ancestors(p, a):
        if (anc.type or "").lower() != "foreach":
            continue
        for e in anc.expressions:
            if e.role != "loop_collection":
                continue
            for r in e.references:
                if r.kind != "activity_output":
                    continue
                paths = [r.path or ""]
                src, hops = names.get((r.name or "").lower()), 0
                while src is not None and (src.type or "").lower() == "filter" and hops < 3:
                    nxt = None
                    for fe in src.expressions:
                        for fr in fe.references:
                            if fr.kind == "activity_output":
                                nxt = names.get((fr.name or "").lower())
                                paths.append(fr.path or "")
                    src, hops = nxt, hops + 1
                if src is not None and (src.type or "").lower() == "getmetadata" \
                        and any("childitems" in x.lower() for x in paths):
                    return anc, src, e
    return None


def _dataset_arguments(pc: PipelineCtx, a: AdfActivity, io: AdfIO) -> None:
    ds = pc.ev.datasets.get(io.name.lower())
    if ds is None:
        return
    supplied = {k.lower() for k in io.parameters}
    for prm in ds.parameters:
        if prm.has_default or prm.name.lower() in supplied:
            continue
        path = f"{io.role}:{io.name}.parameters.{prm.name}"
        f = pc.out.finding(C.incomplete_configuration, "dataset_parameter_not_supplied",
                           f"{a.type} '{a.name}' uses dataset '{ds.name}' without a value for parameter '{prm.name}'.",
                           pipeline=pc.pipe.name, activity=a, property_path=path, severity=Severity.error,
                           source_path=pc.pipe.path)
        ref_tokens = _dataset_ref_tokens(pc, a, io)
        toks = [*ref_tokens, "parameters", prm.name] if ref_tokens else None
        cands: list[tuple[str, Any, list, list, bool]] = []  # (label, value, evidence, assumptions, material)
        pcs = _pipeline_param_candidates(pc, prm.name, prm.type)
        if len(pcs) == 1:
            cands.append(("pipeline parameter", f"@pipeline().parameters.{pcs[0].name}",
                          [ev(K.parameter, f"pipeline:{pc.pipe.name}", f"declares parameter '{pcs[0].name}' with the same name"),
                           ev(K.referenced_artifact, f"dataset:{ds.name}", f"declares parameter '{prm.name}' without a default")],
                          ["the same-named pipeline parameter carries the value"], True))
        loop = _file_loop_item(pc, a) if _FILE_ISH.match(prm.name) else None
        if loop is not None and not cands:
            anc, md, e = loop
            cands.append(("loop item", "@item().name",
                          [ev(K.same_pipeline_reference, anc.activity_id, f"{e.property_path} iterates the childItems of "
                              f"GetMetadata '{md.name}'"),
                           ev(K.referenced_artifact, f"dataset:{ds.name}", f"parameter '{prm.name}' has no default"),
                           ev(K.naming_convention, f"dataset:{ds.name}", f"'{prm.name}' is named like a file name")],
                          [f"'{prm.name}' is the name of the file being processed (childItems entries expose 'name')"], True))
        conflict = False
        if not cands:  # peers: other uses of the same dataset that agree on this argument
            peers = []
            for q in pc.ev.discovery.pipelines:
                for b in pc.ev.acts(q):
                    if b is a:
                        continue
                    for bio in [*b.inputs, *b.outputs]:
                        if bio.name and bio.name.lower() == ds.name.lower() and prm.name in bio.parameters:
                            peers.append((q.name, b, bio.parameters[prm.name]))
            values = {repr(v) for _, _, v in peers}
            if len(values) == 1 and peers:
                cands.append(("peers", peers[0][2],
                              [ev(K.repeated_pattern, f"{q}/{b.name}", f"passes the same value for '{prm.name}'")
                               for q, b, _ in peers], ["other activities' constant applies here too"], True))
            conflict = len(values) > 1
        if len(cands) == 1 and toks and cands[0][1] is not None:
            label, value, evidence, assumptions, material = cands[0]
            val = {"value": value, "type": "Expression"} if isinstance(value, str) and value.startswith("@") else value
            pc.out.proposal(
                rule="argument_from_context", finding=f, pipe=pc.pipe, activity=a,
                missing_element=f"argument '{prm.name}' for dataset '{ds.name}'", action="set_argument",
                target_path=pc.pointer(toks), proposed=val, evidence=evidence, assumptions=assumptions,
                material_assumptions=material, patches=[pc.patch_set(toks, val)],
                alternatives=[Alternative(description="a different expression or constant is intended")])
        else:
            pc.out.blocker([f], pipeline=pc.pipe.name, activity=a,
                           reason=(f"other activities pass different values for '{prm.name}'" if conflict
                                   else f"nothing in the project determines dataset parameter '{prm.name}'"),
                           information_required=[f"the value (or expression) for '{prm.name}' on dataset '{ds.name}'"],
                           conversion_impact="The dataset path/table cannot be fully resolved when inlined.",
                           minimum_information=f"an argument for '{prm.name}'",
                           closest_safe_implementation="dataset inlined with the unresolved dataset() reference flagged")
