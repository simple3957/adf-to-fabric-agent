"""ADF artifact candidate detection + structural extraction for Repository Discovery.

Given the file inventory from :mod:`.scanner`, identifies which files are ADF
artifact candidates using deterministic structural evidence, and records the
directly-observable structural facts for each (identity + shape only). It does
NOT interpret meaning: no compatibility, complexity, risk, effort, target
mapping, or relationship graph. References are recorded as mere presence.

Structural inspection is intentionally shallow: only the approved top-level /
first-level JSON fields (name, type, properties.type, properties.activities,
properties.parameters, properties.variables, properties.annotations,
properties.schema, properties.linkedServiceName, properties.typeProperties keys)
are read. The only recursion is a bounded, value-free scan of KEY NAMES to flag
credential presence. No configuration values are ever read or stored.
"""

from __future__ import annotations

import json
from pathlib import Path

from adf2fabric.project.discovery import (
    AdfArtifactKind,
    AdfCandidate,
    DiscoveryStatus,
    FileRecord,
    KindEvidence,
)

# Directory name (lowercased) -> ADF artifact kind. Primary evidence in the ADF
# Git "code" layout, where the top-level ``type`` field is frequently absent.
_ADF_DIR_KIND = {
    "pipeline": AdfArtifactKind.pipeline,
    "dataset": AdfArtifactKind.dataset,
    "linkedservice": AdfArtifactKind.linked_service,
    "trigger": AdfArtifactKind.trigger,
    "integrationruntime": AdfArtifactKind.integration_runtime,
    "dataflow": AdfArtifactKind.dataflow,
    "factory": AdfArtifactKind.factory,
    "managedvirtualnetwork": AdfArtifactKind.managed_virtual_network,
    "credential": AdfArtifactKind.credential,
    # Folder the Azure intake writes CDC resources to (the kind's own name).
    "changedatacapture": AdfArtifactKind.change_data_capture,
}

# ARM ``.../factories/<subtype>`` -> kind. Stronger evidence when the top-level
# ``type`` field is present (common in ARM-exported artifacts).
_ADF_TYPE_SUBTYPE_KIND = {
    "pipelines": AdfArtifactKind.pipeline,
    "datasets": AdfArtifactKind.dataset,
    "linkedservices": AdfArtifactKind.linked_service,
    "triggers": AdfArtifactKind.trigger,
    "integrationruntimes": AdfArtifactKind.integration_runtime,
    "dataflows": AdfArtifactKind.dataflow,
    "managedvirtualnetworks": AdfArtifactKind.managed_virtual_network,
    "credentials": AdfArtifactKind.credential,
    # Microsoft.DataFactory/factories/adfcdcs (ARM template reference, verified 2026-10-13).
    "adfcdcs": AdfArtifactKind.change_data_capture,
}

# Exact file names (lowercased) that deterministically identify ARM artifacts.
_ARM_FILE_KIND = {
    "armtemplateforfactory.json": AdfArtifactKind.arm_template,
    "armtemplateparametersforfactory.json": AdfArtifactKind.arm_template_parameters,
    "arm-template-parameters-definition.json": AdfArtifactKind.arm_template_parameters,
}

# Credential-bearing JSON key names (lowercased). Presence -> credential_present;
# the VALUE is never read or stored. Conservative set to avoid over-flagging.
_SECRET_KEY_HINTS = frozenset(
    {
        "connectionstring",
        "accountkey",
        "accesskey",
        "secretaccesskey",
        "password",
        "sastoken",
        "sasuri",
        "serviceprincipalkey",
        "encryptedcredential",
        "clientsecret",
    }
)

# Files above this size are inventoried but not opened for structural inspection.
_MAX_STRUCTURAL_BYTES = 10 * 1024 * 1024


def extract_candidates(source_dir: Path, files: list[FileRecord]) -> list[AdfCandidate]:
    """Return the sorted list of ADF artifact candidates found in ``files``."""
    candidates: list[AdfCandidate] = []
    for rec in files:
        if rec.status != DiscoveryStatus.discovered:
            continue
        candidate = _classify(source_dir / rec.path, rec)
        if candidate is not None:
            candidates.append(candidate)
    candidates.sort(key=lambda c: c.path)
    return candidates


def _classify(full: Path, rec: FileRecord) -> AdfCandidate | None:
    ext = rec.extension
    fname = rec.name
    parent = full.parent.name.lower()
    dir_kind = _ADF_DIR_KIND.get(parent)
    arm_kind = _ARM_FILE_KIND.get(fname.lower())

    # Candidates are: .json under a known ADF directory, .json with an ADF type
    # field, or a known ARM file name. Everything else is a plain file.
    if ext != ".json" and arm_kind is None:
        return None

    name: str | None = None
    resource_type: str | None = None
    structural_type: str | None = None
    activity_count: int | None = None
    has_parameters = has_variables = has_annotations = has_schema = False
    parameter_names: list[str] = []
    connection_field_names: list[str] = []
    linked_service_ref_present = False
    credential_present = False
    parse_status = DiscoveryStatus.discovered
    note = ""
    type_kind: AdfArtifactKind | None = None

    if ext == ".json":
        if rec.size_bytes > _MAX_STRUCTURAL_BYTES:
            parse_status = DiscoveryStatus.skipped
            note = "file too large for structural inspection"
        else:
            try:
                with full.open(encoding="utf-8") as fh:
                    data = json.load(fh)
            except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
                parse_status = DiscoveryStatus.failed
                note = f"unparseable JSON ({type(exc).__name__})"
                data = None
            if isinstance(data, dict):
                if isinstance(data.get("name"), str) and data["name"]:
                    name = data["name"]
                raw_type = data.get("type")
                if isinstance(raw_type, str) and raw_type:
                    resource_type = raw_type
                    type_kind = _kind_from_type(raw_type)
                props = data.get("properties")
                if isinstance(props, dict):
                    if isinstance(props.get("type"), str) and props["type"]:
                        structural_type = props["type"]
                    acts = props.get("activities")
                    if isinstance(acts, list):
                        activity_count = len(acts)
                    params = props.get("parameters")
                    if isinstance(params, dict) and params:
                        has_parameters = True
                        parameter_names = sorted(str(k) for k in params)
                    variables = props.get("variables")
                    if isinstance(variables, dict) and variables:
                        has_variables = True
                    annotations = props.get("annotations")
                    if annotations:
                        has_annotations = True
                    if "schema" in props:
                        has_schema = True
                    if "linkedServiceName" in props:
                        linked_service_ref_present = True
                    tp = props.get("typeProperties")
                    if isinstance(tp, dict):
                        # Key NAMES only; values are never read or stored.
                        connection_field_names = sorted(str(k) for k in tp)
                credential_present = _has_secret_key(data)

    # Resolve kind + evidence deterministically (strongest signal wins).
    if type_kind is not None:
        kind, evidence = type_kind, KindEvidence.top_level_type_field
    elif dir_kind is not None:
        kind, evidence = dir_kind, KindEvidence.directory_name
    elif arm_kind is not None:
        kind, evidence = arm_kind, KindEvidence.file_name
    else:
        # A .json file not under a known ADF dir, with no ADF type field and no
        # known ARM name: not an ADF artifact candidate.
        return None

    # Activity count is only meaningful for pipelines; connection field names
    # only for linked services.
    if kind != AdfArtifactKind.pipeline:
        activity_count = None
    if kind != AdfArtifactKind.linked_service:
        connection_field_names = []

    return AdfCandidate(
        path=rec.path,
        name=name,
        artifact_kind=kind,
        kind_evidence=evidence,
        resource_type=resource_type,
        structural_type=structural_type,
        activity_count=activity_count,
        has_parameters=has_parameters,
        has_variables=has_variables,
        has_annotations=has_annotations,
        has_schema=has_schema,
        parameter_names=parameter_names,
        connection_field_names=connection_field_names,
        linked_service_ref_present=linked_service_ref_present,
        credential_present=credential_present,
        parse_status=parse_status,
        note=note,
    )


def _kind_from_type(raw_type: str) -> AdfArtifactKind | None:
    """Map an ADF ``type`` string to a kind, or None if it isn't an ADF type."""
    lowered = raw_type.strip().lower()
    prefix = "microsoft.datafactory/factories"
    if lowered == prefix:
        return AdfArtifactKind.factory
    if lowered.startswith(prefix + "/"):
        subtype = lowered[len(prefix) + 1 :]
        return _ADF_TYPE_SUBTYPE_KIND.get(subtype)
    return None


def _has_secret_key(data, _depth: int = 0) -> bool:
    """True if any credential-bearing key name appears anywhere in ``data``.

    Inspects KEY NAMES only (bounded depth); never reads or stores any value.
    """
    if _depth > 12:
        return False
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(k, str) and k.lower() in _SECRET_KEY_HINTS:
                return True
            if _has_secret_key(v, _depth + 1):
                return True
    elif isinstance(data, list):
        for item in data:
            if _has_secret_key(item, _depth + 1):
                return True
    return False
