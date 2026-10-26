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

import json

import re

from typing import Any, Optional

from adf2fabric.agent.discovery.repository.artifacts import _SECRET_KEY_HINTS
from adf2fabric.project.discovery import (
    NOT_EXTRACTED,
    AdfChangeDataCapture,
    AdfColumn,
    AdfArtifactKind,
    AdfArtifactRef,
    AdfDataset,
    AdfFactoryConfig,
    AdfIntegrationRuntime,
    AdfLinkedConnection,
    AdfKeyVaultRef,
    AdfLinkedService,
    AdfParameter,
    AdfPipeline,
    AdfRecurrence,
    AdfSparkCompute,
    AdfEventTrigger,
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
    elif "schema" not in props and isinstance(props.get("structure"), list):
        # The older 'structure' property: its type names (Int32, ...) are ADF's own and are not mapped, so only the names are kept.
        out = [AdfColumn(name=c["name"]) for c in props["structure"] if isinstance(c, dict) and isinstance(c.get("name"), str)]
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
        linked_service_arguments=_ls_arguments(props.get("linkedServiceName")),
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


def _ls_arguments(ref: Any) -> dict[str, Any]:
    """The arguments a LinkedServiceReference passes to the linked service's parameters (secret-masked, as authored)."""
    params = ref.get("parameters") if isinstance(ref, dict) else None
    return _clean_json(params) if isinstance(params, dict) else {}


def _text_of(v: Any) -> Optional[str]:
    """A scalar's text, or an Expression's text; secret-looking values are dropped."""
    if isinstance(v, dict) and isinstance(v.get("value"), str) and v.get("type") == "Expression":
        v = v["value"]
    return safe_scalar(v) if isinstance(v, (str, int, float)) and not isinstance(v, bool) else None


_LS_PARAM = re.compile(r"linkedService\(\)\.(\w+)", re.IGNORECASE)


def _connection_parts(tp: dict) -> AdfLinkedConnection:
    """The explicitly authored ``server`` / ``database`` / ``userName`` properties, and which fields use ``linkedService().X``.

    Connection strings are deliberately not read.
    """
    dynamic = sorted(k for k, v in tp.items()
                     if k != "password" and _LS_PARAM.search(v if isinstance(v, str) else json.dumps(v, default=str)))
    return AdfLinkedConnection(
        server=_text_of(tp.get("server")) or _text_of(tp.get("serverName")) or _text_of(tp.get("host")),
        database=_text_of(tp.get("database")) or _text_of(tp.get("databaseName")),
        user_name=_text_of(tp.get("userName")) or _text_of(tp.get("username")),
        parameterized_fields=dynamic)


# Spark setting names are dotted paths (spark.hadoop.fs.azure.account.key.<acct>), so the exact-name hint set above
# does not see them; match these fragments on the name with punctuation removed. Over-masking is the safe direction.
_SPARK_SECRET_FRAGMENTS = ("password", "passwd", "pwd", "secret", "token", "credential", "accountkey", "accesskey",
                           "sharedkey", "storagekey", "apikey", "privatekey", "encryptionkey", "sastoken")


def _secret_shaped_setting(name: str) -> bool:
    flat = re.sub(r"[^a-z]", "", name.lower())
    return name.lower() in SECRET_KEY_HINTS or any(f in flat for f in _SPARK_SECRET_FRAGMENTS)


def _worker_range(value: Any) -> tuple[Optional[int], Optional[int]]:
    """ADF writes ``newClusterNumOfWorker`` as ``"4"`` (fixed) or ``"2:8"`` (autoscale min:max)."""
    if isinstance(value, bool):
        return None, None
    if isinstance(value, int):
        return value, value
    if isinstance(value, str):
        m = re.fullmatch(r"\s*(\d+)\s*(?::\s*(\d+)\s*)?", value)
        if m:
            lo = int(m.group(1))
            return lo, int(m.group(2)) if m.group(2) else lo
    return None, None


def _databricks_compute(tp: dict) -> AdfSparkCompute:
    """Read the cluster settings of an ``AzureDatabricks`` linked service. Allowlisted, non-secret fields only."""
    unresolved: list[str] = []

    def lit(key: str) -> Optional[str]:
        v = tp.get(key)
        if v is None:
            return None
        s = safe_scalar(v) if isinstance(v, (str, int, float)) else None
        if s is None:  # an Expression object, a parameter, or something secret-shaped: never guessed
            unresolved.append(key)
        return s

    existing, pool = lit("existingClusterId"), lit("instancePoolId")
    node, workers = lit("newClusterNodeType"), tp.get("newClusterNumOfWorker")
    lo, hi = _worker_range(workers)
    if workers is not None and lo is None:
        unresolved.append("newClusterNumOfWorker")
    conf: dict[str, str] = {}
    raw_conf = tp.get("newClusterSparkConf")
    if isinstance(raw_conf, dict):
        for k, v in raw_conf.items():
            sv = safe_scalar(v) if isinstance(v, (str, int, float, bool)) else None
            conf[str(k)] = sv if sv is not None and not _secret_shaped_setting(str(k)) else "[masked]"
    elif raw_conf is not None:
        unresolved.append("newClusterSparkConf")
    env = tp.get("newClusterSparkEnvVars")
    tags = tp.get("newClusterCustomTags")
    scripts = tp.get("newClusterInitScripts")
    if existing:
        mode = "existing_cluster"
    elif node or workers is not None:
        mode = "new_cluster"
    elif pool:
        mode = "instance_pool"
    else:
        mode = "unspecified"
    return AdfSparkCompute(
        mode=mode, workspace=lit("domain"), existing_cluster_id=existing, instance_pool_id=pool,
        workspace_resource_id=lit("workspaceResourceId"),
        policy_id=lit("policyId"), node_type=node, driver_node_type=lit("newClusterDriverNodeType"),
        min_workers=lo, max_workers=hi, runtime_version=lit("newClusterVersion"), spark_conf=conf,
        spark_env_var_names=sorted(str(k) for k in env) if isinstance(env, dict) else [],
        init_script_count=len(scripts) if isinstance(scripts, list) else 0,
        custom_tag_names=sorted(str(k) for k in tags) if isinstance(tags, dict) else [],
        unresolved_fields=sorted(set(unresolved)))


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
    connection = _connection_parts(tp)
    ls_type = props.get("type") if isinstance(props.get("type"), str) else None
    return AdfLinkedService(
        name=str(data.get("name") or ""),
        path=path,
        type=ls_type,
        compute=_databricks_compute(tp) if ls_type == "AzureDatabricks" else None,
        endpoint=endpoint,
        has_connection_string=("connectionString" in tp),  # presence only; value NEVER read
        key_vault_refs=_find_key_vault_refs(tp),
        connection_field_names=sorted(str(k) for k in tp) if isinstance(tp, dict) else [],
        credential_present=_has_secret_key(props),
        authentication_type=safe_scalar(auth) if isinstance(auth, str) else None,
        integration_runtime=_ref_name(props.get("connectVia")),
        connection=connection,
        parameters=_parse_param_map(props.get("parameters")),
        connector_version=(str(tp.get("version")).strip() if isinstance(tp.get("version"), (str, int, float))
                           and not isinstance(tp.get("version"), bool) and str(tp.get("version")).strip() else None),
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


def parse_integration_runtime(data: dict, path: str) -> AdfIntegrationRuntime:
    """Describe a runtime definition. Keys and secrets are never read (a self-hosted runtime's auth key is not in the JSON)."""
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    tp = props.get("typeProperties") if isinstance(props.get("typeProperties"), dict) else {}
    vnet = props.get("managedVirtualNetwork") or tp.get("managedVirtualNetwork")
    compute = tp.get("computeProperties") if isinstance(tp.get("computeProperties"), dict) else {}
    linked = tp.get("linkedInfo") if isinstance(tp.get("linkedInfo"), dict) else {}
    return AdfIntegrationRuntime(
        name=str(data.get("name") or ""), path=path,
        type=props.get("type") if isinstance(props.get("type"), str) else None,
        kind=classify_integration_runtime(data), is_ssis="ssisProperties" in tp,
        managed_virtual_network=_ref_name(vnet) if isinstance(vnet, dict) else (vnet if isinstance(vnet, str) else None),
        compute_location=compute.get("location") if isinstance(compute.get("location"), str) else None,
        shared_from=safe_scalar(linked.get("resourceId")) if linked else None,
        description=safe_scalar(props.get("description")))


# --- triggers ---------------------------------------------------------------


def _trigger_parameters(raw: Any) -> dict[str, str]:
    """Parameter name -> literal or expression text. A value that looks like secret material is left out."""
    out: dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    for key, value in raw.items():
        if isinstance(value, dict) and value.get("type") == "Expression":
            value = value.get("value")
        text = safe_scalar(value)
        if text is not None:
            out[str(key)] = text
    return out


def _event_trigger(trigger_type: Any, tp: dict) -> Optional[AdfEventTrigger]:
    if not isinstance(trigger_type, str) or "event" not in trigger_type.lower():
        return None

    def text(key: str) -> Optional[str]:
        return tp.get(key) if isinstance(tp.get(key), str) else None

    ignore = tp.get("ignoreEmptyBlobs")
    return AdfEventTrigger(
        scope=text("scope"),
        events=[e for e in (tp.get("events") or []) if isinstance(e, str)],
        blob_path_begins_with=text("blobPathBeginsWith"),
        blob_path_ends_with=text("blobPathEndsWith"),
        ignore_empty_blobs=ignore if isinstance(ignore, bool) else None,
        subject_begins_with=text("subjectBeginsWith"),
        subject_ends_with=text("subjectEndsWith"),
    )


def parse_trigger(data: dict, path: str) -> AdfTrigger:
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    tp = props.get("typeProperties") if isinstance(props.get("typeProperties"), dict) else {}

    pipeline_refs: list[str] = []
    pipeline_parameters: dict[str, dict[str, str]] = {}
    for pl in props.get("pipelines", []) or []:
        if isinstance(pl, dict):
            name = _ref_name(pl.get("pipelineReference"))
            if name:
                pipeline_refs.append(name)
                params = _trigger_parameters(pl.get("parameters"))
                if params:
                    pipeline_parameters.setdefault(name, {}).update(params)

    recurrence = None
    rec = tp.get("recurrence")
    if isinstance(rec, dict):
        interval = rec.get("interval")
        sched = rec.get("schedule") if isinstance(rec.get("schedule"), dict) else {}

        def ints(v):
            return [x for x in (v if isinstance(v, list) else []) if isinstance(x, int) and not isinstance(x, bool)]

        recurrence = AdfRecurrence(
            frequency=rec.get("frequency") if isinstance(rec.get("frequency"), str) else None,
            interval=interval if isinstance(interval, int) else None,
            time_zone=rec.get("timeZone") if isinstance(rec.get("timeZone"), str) else None,
            start_time=safe_scalar(rec.get("startTime")),
            end_time=safe_scalar(rec.get("endTime")),
            minutes=ints(sched.get("minutes")),
            hours=ints(sched.get("hours")),
            week_days=[d for d in (sched.get("weekDays") or []) if isinstance(d, str)],
            month_days=ints(sched.get("monthDays")),
            monthly_occurrences=[
                {"day": o.get("day"), "occurrence": o.get("occurrence")}
                for o in (sched.get("monthlyOccurrences") or []) if isinstance(o, dict)],
        )

    return AdfTrigger(
        name=str(data.get("name") or ""),
        path=path,
        type=props.get("type") if isinstance(props.get("type"), str) else None,
        runtime_state=(
            props.get("runtimeState") if isinstance(props.get("runtimeState"), str) else None
        ),
        pipeline_refs=sorted(set(pipeline_refs)),
        pipeline_parameters=pipeline_parameters,
        event=_event_trigger(props.get("type"), tp),
        recurrence=recurrence,
    )


# --- change data capture ----------------------------------------------------


def parse_change_data_capture(data: dict, path: str) -> AdfChangeDataCapture:
    """Describe an ADF CDC resource: its linked services, tables and mode. Nothing is converted or invented."""
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}

    def side(key: str, entity_key: str) -> tuple[list[str], list[str]]:
        services: list[str] = []
        tables: list[str] = []
        for info in props.get(key) or []:
            if not isinstance(info, dict):
                continue
            conn = info.get("connection") if isinstance(info.get("connection"), dict) else {}
            name = _ref_name(conn.get("linkedService"))
            if name:
                services.append(name)
            for ent in info.get(entity_key) or []:
                if isinstance(ent, dict) and isinstance(ent.get("name"), str) and ent["name"]:
                    tables.append(ent["name"])
        return sorted(set(services)), sorted(set(tables))

    src_ls, src_tables = side("sourceConnectionsInfo", "sourceEntities")
    tgt_ls, tgt_tables = side("targetConnectionsInfo", "targetEntities")
    mappings = 0
    for info in props.get("targetConnectionsInfo") or []:
        if isinstance(info, dict):
            mappings += len([m for m in (info.get("dataMapperMappings") or []) if isinstance(m, dict)])
    policy = props.get("policy") if isinstance(props.get("policy"), dict) else {}
    rec = policy.get("recurrence") if isinstance(policy.get("recurrence"), dict) else {}
    interval = rec.get("interval")
    return AdfChangeDataCapture(
        name=str(data.get("name") or ""), path=path,
        status=props.get("status") if isinstance(props.get("status"), str) else None,
        mode=policy.get("mode") if isinstance(policy.get("mode"), str) else None,
        frequency=rec.get("frequency") if isinstance(rec.get("frequency"), str) else None,
        interval=interval if isinstance(interval, int) else None,
        source_linked_services=src_ls, target_linked_services=tgt_ls,
        source_tables=src_tables, target_tables=tgt_tables, mapping_count=mappings)


# --- factory ----------------------------------------------------------------


def parse_factory(data: dict, path: str) -> AdfFactoryConfig:
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    return AdfFactoryConfig(
        name=str(data.get("name") or ""),
        path=path,
        global_parameters=_parse_param_map(props.get("globalParameters")),
    )
