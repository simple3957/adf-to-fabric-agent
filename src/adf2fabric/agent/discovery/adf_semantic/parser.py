"""Secret-safe structural extraction from ADF artifact JSON definitions.

Every function here reads ONLY an explicit positive allowlist of known-safe
fields; anything else (notably ``connectionString`` and any credential /
AzureKeyVaultSecret VALUE) is never read. As a secondary safeguard, every string
about to be emitted passes through :func:`safe_scalar`, which drops values that
look like secrets (connection strings, passwords, SAS signatures, keys).

The parser is purely a function of the immutable source JSON — no I/O beyond the
dict it is given, no Azure, no inference. Determinism: activity source order is
preserved (the file is immutable); derived collections are sorted by the caller.
"""

from __future__ import annotations

from typing import Any, Optional

from adf2fabric.agent.discovery.repository.artifacts import _SECRET_KEY_HINTS
from adf2fabric.project.discovery import (
    NOT_EXTRACTED,
    AdfActivity,
    AdfColumn,
    AdfArtifactKind,
    AdfArtifactRef,
    AdfDataset,
    AdfDependency,
    AdfFactoryConfig,
    AdfKeyVaultRef,
    AdfLinkedService,
    AdfParameter,
    AdfPipeline,
    AdfRecurrence,
    AdfTrigger,
    AdfVariable,
    ExternalReference,
    ExternalResourceKind,
)

# Reuse the Repository Discovery secret-key-name hint set (imported read-only).
SECRET_KEY_HINTS = frozenset(_SECRET_KEY_HINTS)

# Substrings (lowercased) that mark a VALUE as unsafe to emit. Conservative and
# deterministic — the positive allowlist remains authoritative; this is a net.
_SECRET_VALUE_MARKERS = (
    "password=",
    "pwd=",
    "accountkey=",
    "account key=",
    "sharedaccesssignature",
    "sig=",
    "secret=",
    "connectionstring",
    "authorization:",
    "bearer ",
    "-----begin",
)

# Endpoint fields that may be emitted verbatim (never connection strings).
_ENDPOINT_KEYS = ("url", "baseUrl", "endpoint")

# Depth guard for nested activity recursion + secret-key scans.
_MAX_DEPTH = 30


def safe_scalar(value: Any) -> Optional[str]:
    """Return ``value`` as a safe string, or ``None`` if it is not a scalar or
    looks like secret material.

    A connection-string shape (multiple ``key=value;`` pairs) is also rejected so
    an accidentally-allowlisted field can never leak one.
    """
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return str(value)
    if not isinstance(value, str):
        return None
    low = value.lower()
    for marker in _SECRET_VALUE_MARKERS:
        if marker in low:
            return None
    # Connection-string shape: >=2 ``=`` and at least one ``;`` joining pairs.
    if ";" in value and value.count("=") >= 2:
        return None
    return value


def _field_value(node: Any) -> tuple[Optional[str], bool]:
    """Extract a literal or expression value → (safe_string, is_expression).

    ADF represents an expression as ``{"value": "@...", "type": "Expression"}``.
    """
    if isinstance(node, dict):
        if node.get("type") == "Expression" and "value" in node:
            return safe_scalar(node.get("value")), True
        return None, False
    return safe_scalar(node), False


def _ref_name(node: Any) -> Optional[str]:
    """The ``referenceName`` of an ADF *Reference object, if present."""
    if isinstance(node, dict):
        name = node.get("referenceName")
        if isinstance(name, str) and name:
            return name
    return None


def _folder_name(props: dict) -> Optional[str]:
    folder = props.get("folder")
    if isinstance(folder, dict):
        name = folder.get("name")
        if isinstance(name, str) and name:
            return name
    return None


def _parse_param_map(node: Any) -> list[AdfParameter]:
    """Parse a ``{name: {type, defaultValue}}`` parameter/global-parameter map."""
    out: list[AdfParameter] = []
    if not isinstance(node, dict):
        return out
    for name in sorted(node):
        spec = node[name]
        ptype = spec.get("type") if isinstance(spec, dict) else None
        default = None
        is_expr = False
        if isinstance(spec, dict) and ("defaultValue" in spec or "value" in spec):
            raw = spec.get("defaultValue", spec.get("value"))
            default, is_expr = _field_value(raw)
        out.append(
            AdfParameter(
                name=str(name),
                type=ptype if isinstance(ptype, str) else None,
                is_expression=is_expr,
                default_value=default,
            )
        )
    return out


def _parse_variables(node: Any) -> list[AdfVariable]:
    out: list[AdfVariable] = []
    if not isinstance(node, dict):
        return out
    for name in sorted(node):
        spec = node[name]
        vtype = spec.get("type") if isinstance(spec, dict) else None
        default = None
        is_expr = False
        if isinstance(spec, dict) and "defaultValue" in spec:
            default, is_expr = _field_value(spec.get("defaultValue"))
        out.append(
            AdfVariable(
                name=str(name),
                type=vtype if isinstance(vtype, str) else None,
                is_expression=is_expr,
                default_value=default,
            )
        )
    return out


def _has_secret_key(data: Any, _depth: int = 0) -> bool:
    """True if any credential-bearing key NAME appears anywhere (names only)."""
    if _depth > _MAX_DEPTH:
        return False
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(k, str) and k.lower() in SECRET_KEY_HINTS:
                return True
            if _has_secret_key(v, _depth + 1):
                return True
    elif isinstance(data, list):
        for item in data:
            if _has_secret_key(item, _depth + 1):
                return True
    return False


# --- pipelines --------------------------------------------------------------

# Child-activity containers → the typeProperties keys holding nested activities.
_NESTED_ACTIVITY_KEYS = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")


def _parse_activity(node: dict, _depth: int = 0) -> AdfActivity:
    tp = node.get("typeProperties") if isinstance(node.get("typeProperties"), dict) else {}

    depends_on: list[AdfDependency] = []
    for dep in node.get("dependsOn", []) or []:
        if isinstance(dep, dict) and isinstance(dep.get("activity"), str):
            conds = [str(c) for c in (dep.get("dependencyConditions") or []) if isinstance(c, str)]
            depends_on.append(AdfDependency(activity=dep["activity"], conditions=conds))

    dataset_refs: list[str] = []
    for io_key in ("inputs", "outputs"):
        for ref in node.get(io_key, []) or []:
            name = _ref_name(ref)
            if name:
                dataset_refs.append(name)
    lookup_ds = _ref_name(tp.get("dataset"))
    if lookup_ds:
        dataset_refs.append(lookup_ds)

    linked_service_refs: list[str] = []
    ls_ref = _ref_name(node.get("linkedServiceName"))
    if ls_ref:
        linked_service_refs.append(ls_ref)

    pipeline_refs: list[str] = []
    pl_ref = _ref_name(tp.get("pipeline"))
    if pl_ref:
        pipeline_refs.append(pl_ref)

    source = tp.get("source") if isinstance(tp.get("source"), dict) else {}
    sink = tp.get("sink") if isinstance(tp.get("sink"), dict) else {}
    source_type = source.get("type") if isinstance(source.get("type"), str) else None
    sink_type = sink.get("type") if isinstance(sink.get("type"), str) else None

    stored_procedures: list[str] = []
    for sp in (tp.get("storedProcedureName"), source.get("sqlReaderStoredProcedureName")):
        if isinstance(sp, str) and sp:
            stored_procedures.append(sp)

    nested: list[AdfActivity] = []
    if _depth < _MAX_DEPTH:
        for key in _NESTED_ACTIVITY_KEYS:
            for child in tp.get(key, []) or []:
                if isinstance(child, dict):
                    nested.append(_parse_activity(child, _depth + 1))
        # Switch ``cases[].activities``
        for case in tp.get("cases", []) or []:
            if isinstance(case, dict):
                for child in case.get("activities", []) or []:
                    if isinstance(child, dict):
                        nested.append(_parse_activity(child, _depth + 1))

    return AdfActivity(
        name=str(node.get("name") or ""),
        type=node.get("type") if isinstance(node.get("type"), str) else None,
        depends_on=depends_on,
        dataset_refs=sorted(set(dataset_refs)),
        linked_service_refs=sorted(set(linked_service_refs)),
        pipeline_refs=sorted(set(pipeline_refs)),
        source_type=source_type,
        sink_type=sink_type,
        stored_procedures=sorted(set(stored_procedures)),
        activities=nested,
    )


def _collect_pipeline_refs(activities: list[AdfActivity]) -> set[str]:
    refs: set[str] = set()
    for a in activities:
        refs.update(a.pipeline_refs)
        refs.update(_collect_pipeline_refs(a.activities))
    return refs


def parse_pipeline(data: dict, path: str) -> AdfPipeline:
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    activities = [
        _parse_activity(a) for a in props.get("activities", []) or [] if isinstance(a, dict)
    ]
    return AdfPipeline(
        name=str(data.get("name") or ""),
        path=path,
        folder=_folder_name(props),
        parameters=_parse_param_map(props.get("parameters")),
        variables=_parse_variables(props.get("variables")),
        activities=activities,
        activity_count=len(activities),
        pipeline_dependencies=sorted(_collect_pipeline_refs(activities)),
    )


# --- datasets ---------------------------------------------------------------


def _dataset_columns(props: dict) -> list[AdfColumn]:
    schema = props.get("schema")
    out: list[AdfColumn] = []
    if isinstance(schema, list):
        for col in schema:
            if isinstance(col, dict) and isinstance(col.get("name"), str):
                ctype = col.get("type")
                out.append(
                    AdfColumn(name=col["name"], type=ctype if isinstance(ctype, str) else None)
                )
    return out


def _dataset_asset(name: str, dtype: Optional[str], tp: dict) -> Optional[ExternalReference]:
    """Build the source/sink asset identity from ALLOWLISTED structural fields."""
    low = (dtype or "").lower()
    # SQL table
    if "sql" in low and ("table" in low or "azuresqltable" in low or low.endswith("table")):
        schema_v, _ = _field_value(tp.get("schema"))
        table_v, exprs = _field_value(tp.get("table"))
        _, e2 = _field_value(tp.get("schema"))
        if schema_v or table_v:
            return ExternalReference(
                resource_kind=ExternalResourceKind.sql_object,
                source_artifact=name,
                source_kind=AdfArtifactKind.dataset,
                is_expression_driven=bool(exprs or e2),
                sql_schema=schema_v,
                sql_table=table_v,
            )
    # ADLS / blob location
    location = tp.get("location")
    if isinstance(location, dict):
        fs_v, e_fs = _field_value(location.get("fileSystem"))
        folder_v, e_fp = _field_value(location.get("folderPath"))
        file_v, e_fn = _field_value(location.get("fileName"))
        path_bits = [p for p in (folder_v, file_v) if p]
        if fs_v or path_bits:
            return ExternalReference(
                resource_kind=ExternalResourceKind.adls_path,
                source_artifact=name,
                source_kind=AdfArtifactKind.dataset,
                is_expression_driven=bool(e_fs or e_fp or e_fn),
                adls_filesystem=fs_v,
                adls_path="/".join(path_bits) if path_bits else None,
            )
    # REST resource
    rel = tp.get("relativeUrl")
    rel_v, e_rel = _field_value(rel)
    if rel_v is not None:
        return ExternalReference(
            resource_kind=ExternalResourceKind.rest_endpoint,
            source_artifact=name,
            source_kind=AdfArtifactKind.dataset,
            is_expression_driven=e_rel,
            rest_relative_url=rel_v,
        )
    return None


def parse_dataset(data: dict, path: str) -> AdfDataset:
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    tp = props.get("typeProperties") if isinstance(props.get("typeProperties"), dict) else {}
    dtype = props.get("type") if isinstance(props.get("type"), str) else None
    ls_ref = _ref_name(props.get("linkedServiceName"))
    name = str(data.get("name") or "")
    return AdfDataset(
        name=name,
        path=path,
        folder=_folder_name(props),
        type=dtype,
        linked_service_ref=(
            AdfArtifactRef(name=ls_ref, kind=AdfArtifactKind.linked_service)
            if ls_ref
            else None
        ),
        parameters=_parse_param_map(props.get("parameters")),
        columns=_dataset_columns(props),
        asset=_dataset_asset(name, dtype, tp),
    )


# --- linked services --------------------------------------------------------


def _find_key_vault_refs(node: Any, _depth: int = 0) -> list[AdfKeyVaultRef]:
    """Find AzureKeyVaultSecret references (store LS + secret NAME only)."""
    found: list[AdfKeyVaultRef] = []
    if _depth > _MAX_DEPTH:
        return found
    if isinstance(node, dict):
        if node.get("type") == "AzureKeyVaultSecret":
            store = _ref_name(node.get("store"))
            secret = node.get("secretName")
            secret_name = safe_scalar(secret) if secret is not None else None
            if store and secret_name:
                found.append(
                    AdfKeyVaultRef(store_linked_service=store, secret_name=secret_name)
                )
        for v in node.values():
            found.extend(_find_key_vault_refs(v, _depth + 1))
    elif isinstance(node, list):
        for item in node:
            found.extend(_find_key_vault_refs(item, _depth + 1))
    return found


def parse_linked_service(data: dict, path: str) -> AdfLinkedService:
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    tp = props.get("typeProperties") if isinstance(props.get("typeProperties"), dict) else {}

    endpoint = None
    for key in _ENDPOINT_KEYS:
        if key in tp:
            endpoint = safe_scalar(tp.get(key))
            if endpoint is not None:
                break

    auth = tp.get("authenticationType")
    return AdfLinkedService(
        name=str(data.get("name") or ""),
        path=path,
        type=props.get("type") if isinstance(props.get("type"), str) else None,
        endpoint=endpoint,
        has_connection_string=("connectionString" in tp),  # presence only; value NEVER read
        key_vault_refs=_find_key_vault_refs(tp),
        connection_field_names=sorted(str(k) for k in tp) if isinstance(tp, dict) else [],
        credential_present=_has_secret_key(props),
        authentication_type=safe_scalar(auth) if isinstance(auth, str) else None,
    )


# --- triggers ---------------------------------------------------------------


def parse_trigger(data: dict, path: str) -> AdfTrigger:
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    tp = props.get("typeProperties") if isinstance(props.get("typeProperties"), dict) else {}

    pipeline_refs: list[str] = []
    for pl in props.get("pipelines", []) or []:
        if isinstance(pl, dict):
            name = _ref_name(pl.get("pipelineReference"))
            if name:
                pipeline_refs.append(name)

    recurrence = None
    rec = tp.get("recurrence")
    if isinstance(rec, dict):
        interval = rec.get("interval")
        recurrence = AdfRecurrence(
            frequency=rec.get("frequency") if isinstance(rec.get("frequency"), str) else None,
            interval=interval if isinstance(interval, int) else None,
            time_zone=rec.get("timeZone") if isinstance(rec.get("timeZone"), str) else None,
            start_time=safe_scalar(rec.get("startTime")),
        )

    return AdfTrigger(
        name=str(data.get("name") or ""),
        path=path,
        type=props.get("type") if isinstance(props.get("type"), str) else None,
        runtime_state=(
            props.get("runtimeState") if isinstance(props.get("runtimeState"), str) else None
        ),
        pipeline_refs=sorted(set(pipeline_refs)),
        recurrence=recurrence,
    )


# --- factory ----------------------------------------------------------------


def parse_factory(data: dict, path: str) -> AdfFactoryConfig:
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    return AdfFactoryConfig(
        name=str(data.get("name") or ""),
        path=path,
        global_parameters=_parse_param_map(props.get("globalParameters")),
    )
