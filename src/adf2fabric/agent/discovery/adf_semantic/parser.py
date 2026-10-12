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
    AdfColumn,
    AdfArtifactKind,
    AdfArtifactRef,
    AdfDataset,
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


def _clean_json(node: Any, _depth: int = 0) -> Any:
    """A secret-masked copy of authored JSON (keys + structure preserved).

    Masks SecureString values, string values under credential-named keys, and strings
    carrying a secret-value marker. Dynamic content (Expression objects) is kept.
    """
    if _depth > _MAX_DEPTH * 2:
        return node
    if isinstance(node, dict):
        secure = node.get("type") == "SecureString"
        out: dict = {}
        for k, v in node.items():
            if isinstance(v, str) and (
                (secure and k == "value") or (isinstance(k, str) and k.lower() in SECRET_KEY_HINTS)
            ):
                out[k] = v if set(v) <= {"*"} and v else "[REDACTED]"
            else:
                out[k] = _clean_json(v, _depth + 1)
        return out
    if isinstance(node, list):
        return [_clean_json(v, _depth + 1) for v in node]
    if isinstance(node, str) and any(m in node.lower() for m in _SECRET_VALUE_MARKERS):
        return "[REDACTED]"
    return node


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
        has_default = False
        default_raw = None
        if isinstance(spec, dict) and ("defaultValue" in spec or "value" in spec):
            raw = spec.get("defaultValue", spec.get("value"))
            default, is_expr = _field_value(raw)
            has_default = True
            default_raw = _clean_json(raw)
        out.append(
            AdfParameter(
                name=str(name),
                type=ptype if isinstance(ptype, str) else None,
                is_expression=is_expr,
                default_value=default,
                has_default=has_default,
                default_raw=default_raw,
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
        has_default = False
        default_raw = None
        if isinstance(spec, dict) and "defaultValue" in spec:
            default, is_expr = _field_value(spec.get("defaultValue"))
            has_default = True
            default_raw = _clean_json(spec.get("defaultValue"))
        out.append(
            AdfVariable(
                name=str(name),
                type=vtype if isinstance(vtype, str) else None,
                is_expression=is_expr,
                default_value=default,
                has_default=has_default,
                default_raw=default_raw,
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


def parse_pipeline(data: dict, path: str) -> AdfPipeline:
    """Pipelines are parsed by :mod:`.parser_pipelines` — the single pipeline interface.

    Kept as a thin delegate so existing callers keep working; there is no second
    pipeline-parsing implementation.
    """
    from adf2fabric.agent.discovery.adf_semantic import parser_pipelines

    return parser_pipelines.parse_pipeline(data, path)


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
        type_properties=_clean_json(tp),
        annotations=[a for a in (props.get("annotations") or []) if isinstance(a, str)]
        if isinstance(props.get("annotations"), list) else [],
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
        integration_runtime=_ref_name(props.get("connectVia")),
    )


# --- integration runtimes ---------------------------------------------------


def classify_integration_runtime(data: dict) -> Optional[str]:
    """azure | self_hosted | managed_vnet from an integration runtime definition; None if not classifiable.

    ``SelfHosted`` is an on-premises runtime. ``Managed`` is an Azure runtime; one that carries a
    ``managedVirtualNetwork`` reference runs inside a managed virtual network. An Azure-SSIS runtime
    (``ssisProperties``) is deliberately left unclassified: it is not a data-store gateway question.
    """
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    kind = props.get("type")
    tp = props.get("typeProperties") if isinstance(props.get("typeProperties"), dict) else {}
    if kind == "SelfHosted":
        return "self_hosted"
    if kind == "Managed":
        if "ssisProperties" in tp:
            return None
        return "managed_vnet" if (props.get("managedVirtualNetwork") or tp.get("managedVirtualNetwork")) else "azure"
    return None


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
