"""SQL execution and HTTP/Function conversions: StoredProcedure, Script, Web, WebHook, AzureFunction."""

from __future__ import annotations

from typing import Any, Optional

from adf2fabric.agent.conversion import connectors
from adf2fabric.agent.conversion.context import ActivityBuilder, gateway_need
from adf2fabric.agent.conversion.expr_convert import is_expression
from adf2fabric.agent.conversion.handlers._common import block, carry, split_url
from adf2fabric.agent.conversion.registry import ConversionRule, register
from adf2fabric.project.conversion import ConversionStrategy
from adf2fabric.project.discovery import AdfOperationKind as Op


def _block(cx: ActivityBuilder, code: str, reason: str, *alts: str) -> None:
    block(cx, code, reason, *alts)


def _sql_target(cx: ActivityBuilder, allowed: frozenset, what: str) -> Optional[str]:
    """The activity's linked-service name if its store is documented as supported, else None (blocked)."""
    a = cx.act
    ls = a.linked_service_refs[0] if a.linked_service_refs else None
    if not ls:
        cx.blocker(f"{what}_no_linked_service", f"The {a.type} activity names no linked service.")
        _block(cx, f"{what}_no_linked_service", "it has no linked service.")
        return None
    ls_ir = cx.pc.project.linked_services.get(ls.lower())
    ltype = ls_ir.type if ls_ir else None
    if ls_ir is None:  # the store type decides whether Fabric supports this activity, so a bound connection cannot stand in for the definition
        cx.blocker(f"{what}_linked_service_not_in_source",
                   f"Linked service '{ls}' is not in the source, so its store type is unknown and cannot be checked against the stores "
                   f"Fabric supports for this activity.",
                   "Add the linked service to the source (its linkedService/ file, the ARM template export, or intake from the live factory).")
        _block(cx, f"{what}_linked_service_not_in_source", "its linked service is not in the source, so its store type is unknown.")
        return None
    if (ltype or "").lower() not in allowed:
        verdict, why = connectors.verdict(ltype)
        cx.blocker(f"{what}_store_unsupported",
                   f"Linked service '{ls}' ({ltype or 'unknown type'}) is not a documented Fabric "
                   f"{'Stored procedure' if what == 'stored_procedure' else 'Script'} store ({why}).",
                   "Run the logic in a supported SQL store, a Fabric notebook, or keep it outside Fabric.")
        cx.alternative("Re-implement the logic as a Fabric notebook (Spark SQL / PySpark).")
        cx.alternative("Load the data into a Fabric Warehouse and run the equivalent T-SQL there.")
        _block(cx, f"{what}_store_unsupported", f"its store '{ltype}' is not supported by the Fabric activity.")
        return None
    return ls


# --- Stored procedure ------------------------------------------------------------------------------------------


def convert_stored_procedure(cx: ActivityBuilder) -> None:
    a = cx.act
    ls = _sql_target(cx, connectors.STORED_PROCEDURE_STORES, "stored_procedure")
    if ls is None:
        return
    tp = carry(cx, ("storedProcedureName", "storedProcedureParameters"))
    if "storedProcedureName" not in tp:
        cx.blocker("stored_procedure_name_missing", "No stored procedure name.")
        return _block(cx, "stored_procedure_name_missing", "it names no stored procedure.")
    name = cx.add(cx.envelope(a.name, "SqlServerStoredProcedure", tp, connection=cx.connection(ls)))
    cx.mapping("linkedServiceName", "externalReferences.connection", "linked_service_to_connection",
               f"linked service '{ls}' -> Fabric connection")
    cx.cover(Op.transform, name)
    cx.strategy = ConversionStrategy.transformed
    cx.info("sql_dialect", "The procedure is executed by the target database engine; T-SQL (SQL Server family) is "
            "assumed because the connection stays on the same store.",
            "If the data later moves into a Fabric Warehouse, re-create the procedure there and review T-SQL "
            "surface-area differences.")
    cx.output_notes.append("Stored procedure activities return no result set to the pipeline in either service.")
    cx.summary = f"Stored procedure '{a.name}' -> Fabric Stored procedure {tp['storedProcedureName']!r} on connection '{ls}'."


register(ConversionRule(
    name="stored_procedure", adf_types=("SqlServerStoredProcedure",), fabric_types=("SqlServerStoredProcedure",),
    variants=("with parameters", "dynamic procedure name"),
    strategy=ConversionStrategy.transformed, handler=convert_stored_procedure, runtime_disposition="replace",
    property_rules=("storedProcedureName, storedProcedureParameters (name/value/type) carried",
                    "linkedServiceName -> externalReferences.connection"),
    io_rules=("none (no dataset)",),
    expression_rules=("parameter value expressions converted; unchanged unless a named rewrite applies",),
    dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",),
    unsupported=("stores outside the documented list (Fabric SQL database/Warehouse, Azure SQL DB/MI, Synapse, "
                 "SQL Server, Amazon RDS for SQL Server) -> blocked with alternatives",),
    manual_configuration=("Fabric connection to the database", "gateway for on-premises SQL Server"),
    validation_rules=("storedProcedureName present", "connection present"),
))


# --- SQL pool stored procedure (Synapse) -------------------------------------------------------------------------


def convert_sql_pool_stored_procedure(cx: ActivityBuilder) -> None:
    """Synapse ``SqlPoolStoredProcedure`` -> Fabric Stored procedure activity.

    Microsoft's Fabric Stored procedure activity article lists Azure Synapse Analytics and Fabric Data Warehouse among its
    stores, so the activity can be carried. There is no linked service: the pool is an activity-level ``sqlPool``
    reference, so the Fabric connection is a free binding named after the pool (``connection:sqlpool:<pool>``).
    """
    a = cx.act
    pool = a.configuration.get("sql_pool")
    if not pool:
        cx.blocker("sql_pool_missing", "The SQL pool stored procedure names no SQL pool (sqlPool.referenceName).")
        return _block(cx, "sql_pool_missing", "it names no SQL pool.")
    tp = carry(cx, ("storedProcedureName", "storedProcedureParameters"))
    if "storedProcedureName" not in tp:
        cx.blocker("stored_procedure_name_missing", "No stored procedure name.")
        return _block(cx, "stored_procedure_name_missing", "it names no stored procedure.")
    conn = cx.free_connection(
        f"sqlpool:{pool}", f"A Fabric connection to the Synapse dedicated SQL pool '{pool}' (Azure Synapse Analytics "
                           "connection), or to the Fabric Warehouse its data was moved to.")
    name = cx.add(cx.envelope(a.name, "SqlServerStoredProcedure", tp, connection=conn))
    cx.mapping("sqlPool", "externalReferences.connection", "sql_pool_to_connection",
               f"SQL pool '{pool}' -> Fabric connection (bound by name in the resolutions file)")
    cx.cover(Op.transform, name)
    cx.strategy = ConversionStrategy.transformed
    cx.info("sql_pool_target", f"The procedure runs in whichever database the Fabric connection points to. The pool "
            f"'{pool}' itself does not move.",
            "Point the connection at the Synapse pool while it exists, or re-create the procedure in a Fabric Warehouse "
            "after migrating the data and point the connection there (review T-SQL differences).")
    cx.output_notes.append("Stored procedure activities return no result set to the pipeline in either service.")
    cx.summary = (f"SQL pool stored procedure '{a.name}' -> Fabric Stored procedure {tp['storedProcedureName']!r} on "
                  f"connection 'sqlpool:{pool}'.")


register(ConversionRule(
    name="sql_pool_stored_procedure", adf_types=("SqlPoolStoredProcedure",), fabric_types=("SqlServerStoredProcedure",),
    variants=("with parameters", "dynamic procedure name"),
    strategy=ConversionStrategy.transformed, handler=convert_sql_pool_stored_procedure, runtime_disposition="replace",
    property_rules=("storedProcedureName, storedProcedureParameters (name/value/type) carried",
                    "sqlPool (activity-level reference) -> externalReferences.connection, bound as connection:sqlpool:<pool>"),
    io_rules=("none (no dataset)",),
    expression_rules=("parameter value expressions converted; unchanged unless a named rewrite applies",),
    dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",),
    unsupported=("the dedicated SQL pool itself is not migrated by this activity",),
    manual_configuration=("Fabric connection to the Synapse pool or to the migrated Fabric Warehouse",),
    validation_rules=("sqlPool present", "storedProcedureName present", "connection present"),
))


# --- Script ----------------------------------------------------------------------------------------------------------


def convert_script(cx: ActivityBuilder) -> None:
    a, raw = cx.act, cx.act.raw_properties
    ls = _sql_target(cx, connectors.SCRIPT_STORES, "script")
    if ls is None:
        return
    blocks = raw.get("scripts")
    if not isinstance(blocks, list) or not blocks:
        cx.blocker("script_no_blocks", "The Script activity has no script blocks.")
        return _block(cx, "script_no_blocks", "it has no script blocks.")
    scripts = cx.tree(blocks, "typeProperties.scripts", "typeProperties.scripts")
    cx.mapping("typeProperties.scripts", "typeProperties.scripts", "script_blocks",
               f"{len(scripts)} block(s) carried verbatim (type Query/NonQuery, text, parameters)")
    tp: dict[str, Any] = {
        "database": cx.placeholder(
            "configuration", f"database:{ls}", f"<database:{ls}>",
            f"The database the script runs against. ADF takes it from linked service '{ls}', whose connection "
            "string is deliberately not read; enter it on the Fabric activity."),
        "scripts": scripts,
    }
    for key in ("scriptBlockExecutionTimeout",):
        if key in raw:
            tp[key] = cx.tree(raw[key], f"typeProperties.{key}", f"typeProperties.{key}")
            cx.mapping(f"typeProperties.{key}", f"typeProperties.{key}", "passthrough")
    if "logSettings" in raw:
        cx.dropped("typeProperties.logSettings", "references a linked service; its Fabric structure is undocumented",
                   manual="Re-enable script logging in the Fabric activity and bind its storage connection.")
    for key in raw:
        if key not in ("scripts", "scriptBlockExecutionTimeout", "logSettings"):
            cx.dropped(f"typeProperties.{key}", "not a documented Fabric Script property")
    name = cx.add(cx.envelope(a.name, "Script", tp, connection=cx.connection(ls),
                              ))
    for op in (a.operations or [Op.transform]):
        cx.cover(op, name)
    cx.strategy = ConversionStrategy.transformed
    kinds = ", ".join(a.configuration.get("statement_kinds") or []) or "unknown statements"
    cx.info("script_semantics", f"Script statements ({kinds}) run in the target engine unchanged. Transaction "
            "scope per block and the shape of the activity output are not documented for Fabric.",
            "Test multi-statement blocks and any downstream use of the script's result sets.")
    cx.output_notes.append("Script output (result sets, output parameters) shape is not documented for Fabric.")
    cx.summary = f"Script '{a.name}' -> Fabric Script with {len(scripts)} block(s) ({kinds}) on connection '{ls}'."


register(ConversionRule(
    name="script", adf_types=("Script",), fabric_types=("Script",),
    variants=("Query blocks", "NonQuery blocks", "parameterized blocks", "multi-block"),
    strategy=ConversionStrategy.transformed, handler=convert_script, runtime_disposition="replace",
    property_rules=("scripts[] (type, text, parameters) carried verbatim", "scriptBlockExecutionTimeout carried",
                    "linkedServiceName -> externalReferences.connection (inferred placement)"),
    io_rules=("none",), expression_rules=("script text / parameter expressions converted",),
    dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",),
    unsupported=("logSettings (linked-service reference, undocumented structure)",
                 "stores outside the documented Script list -> blocked with alternatives"),
    manual_configuration=("Fabric connection", "typeProperties.database (not read from the linked service)"),
    validation_rules=("scripts and database present",),
))


# --- Web / WebHook --------------------------------------------------------------------------------------------------------


def _web(cx: ActivityBuilder, ftype: str) -> None:
    a, raw = cx.act, cx.act.raw_properties
    url = raw.get("url")
    documented = {"WebActivity": ("method", "headers", "body", "disableCertValidation", "httpRequestTimeout",
                                  "turnOffAsync"),
                  "WebHook": ("method", "headers", "body", "timeout", "reportStatusOnCallBack",
                              "disableCertValidation")}[ftype]
    tp = carry(cx, documented, skip=("url", "authentication", "connectVia", "datasets", "linkedServices"))
    if isinstance(tp.get("method"), str):
        tp["method"] = tp["method"].upper()

    if is_expression(url):
        cx.manual("web_dynamic_url", "The URL is dynamic content. Fabric splits it into a connection (fixed base URL) "
                  "and a relativeUrl, so a fully dynamic URL cannot be converted safely.",
                  "Move the fixed base URL into the Fabric connection and keep only the variable part dynamic.")
        tp["relativeUrl"] = cx.conv(url, "typeProperties.url", "typeProperties.relativeUrl")
        base_key = "dynamic"
        base_note = "The base URL of the original dynamic URL must be set on this connection by hand."
        cx.mapping("typeProperties.url", "typeProperties.relativeUrl", "dynamic_url_kept",
                   "kept as the relative URL; base URL needs manual configuration")
    elif isinstance(url, str):
        base, rel = split_url(url)
        if base is None:
            cx.manual("web_url_not_absolute", f"The URL '{url}' is not an absolute URL.",
                      "Provide the base URL in the Fabric connection.")
            tp["relativeUrl"] = url
            base_key, base_note = "unknown", "Base URL unknown."
        else:
            tp["relativeUrl"] = rel
            base_key = base.split("://", 1)[-1]
            base_note = f"Base URL {base}"
            cx.mapping("typeProperties.url", "typeProperties.relativeUrl", "url_split",
                       f"'{base}' moves to the connection; '{rel or '(empty)'}' stays as relativeUrl")
    else:
        cx.blocker("web_no_url", "The activity has no URL.")
        return _block(cx, "web_no_url", "it has no URL.")

    auth = raw.get("authentication")
    detail = f"{base_note}."
    if isinstance(auth, dict):
        detail += f" The ADF activity authenticated with '{auth.get('type')}'; configure the equivalent credential " \
                  "on the connection."
        cx.dropped("typeProperties.authentication", "credentials belong to the Fabric connection, not the activity",
                   manual="Create the credential on the Fabric Web connection.")
        cx.alternative("Store the credential in the Fabric connection (or a Key Vault-backed connection), not in the pipeline.")
    ir = (raw.get("connectVia") or {}).get("referenceName") if isinstance(raw.get("connectVia"), dict) else None
    why = gateway_need(ir, cx.pc.project.ir_kinds.get((ir or "").lower()))
    if why:
        cx.placeholder("gateway", ir, f"<gateway:{ir}>", f"The activity ran on integration runtime '{ir}'. {why}")
    for key in ("datasets", "linkedServices"):
        if key in raw:
            cx.dropped(f"typeProperties.{key}", "datasets/linked services passed to the activity do not exist in Fabric")

    conn = cx.free_connection(f"web:{base_key}", detail)
    name = cx.add(cx.envelope(a.name, ftype, tp, connection=conn))
    cx.cover(Op.external_execution, name)
    cx.strategy = ConversionStrategy.transformed
    cx.output_notes.append("Web response payload is limited to 4 MB in Fabric.")
    cx.summary = f"{a.type} '{a.name}' -> Fabric {ftype} {tp.get('method')} {tp.get('relativeUrl')!r} via a connection."


def convert_web(cx: ActivityBuilder) -> None:
    _web(cx, "WebActivity")


def convert_webhook(cx: ActivityBuilder) -> None:
    _web(cx, "WebHook")


register(ConversionRule(
    name="web", adf_types=("WebActivity",), fabric_types=("WebActivity",),
    variants=("GET/POST/PUT/DELETE/PATCH", "literal URL", "dynamic URL", "with authentication", "self-hosted IR"),
    strategy=ConversionStrategy.transformed, handler=convert_web, runtime_disposition="replace",
    property_rules=("url -> connection base URL + typeProperties.relativeUrl (literal URLs only)",
                    "method upper-cased; headers, body, disableCertValidation, httpRequestTimeout, turnOffAsync carried"),
    io_rules=("datasets / linkedServices passed to the activity are not carried",),
    expression_rules=("header/body/relative-URL expressions converted",),
    dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",),
    unsupported=("authentication on the activity (moves to the connection)", "dynamic base URL",
                 "datasets / linkedServices arguments"),
    manual_configuration=("Fabric Web connection (base URL + credential)", "gateway for self-hosted integration runtimes"),
    validation_rules=("externalReferences.connection present", "relativeUrl and method present"),
))
register(ConversionRule(
    name="webhook", adf_types=("WebHook",), fabric_types=("WebHook",),
    variants=("callback timeout", "reportStatusOnCallBack"),
    strategy=ConversionStrategy.transformed, handler=convert_webhook, runtime_disposition="replace",
    property_rules=("url -> connection base URL + relativeUrl", "method, headers, body, timeout, "
                    "reportStatusOnCallBack, disableCertValidation carried"),
    io_rules=("none",), expression_rules=("converted like Web",), dependency_rules=("dependsOn mapped 1:1",),
    parameter_variable_rules=("unchanged",),
    unsupported=("authentication on the activity (moves to the connection)", "dynamic base URL"),
    manual_configuration=("Fabric Web connection",),
    validation_rules=("externalReferences.connection present",),
))


# --- Azure Function -----------------------------------------------------------------------------------------------------------


def convert_function(cx: ActivityBuilder) -> None:
    a = cx.act
    ls = a.linked_service_refs[0] if a.linked_service_refs else None
    if not ls:
        cx.blocker("function_no_linked_service", "The Azure Function activity names no linked service.")
        return _block(cx, "function_no_linked_service", "it has no linked service.")
    tp = carry(cx, ("method", "functionName", "headers", "body"))
    if isinstance(tp.get("method"), str):
        tp["method"] = tp["method"].upper()
    name = cx.add(cx.envelope(a.name, "AzureFunction", tp, connection=cx.connection(ls)))
    cx.mapping("linkedServiceName", "externalReferences.connection", "linked_service_to_connection")
    cx.cover(Op.external_execution, name)
    cx.strategy = ConversionStrategy.transformed
    cx.disposition = "reuse_externally"
    cx.info("function_runtime", "The Azure Function itself keeps running where it is; only the call moves to Fabric.",
            "Create a Fabric connection to the function app (function key via the connection).")
    cx.summary = f"AzureFunctionActivity '{a.name}' -> Fabric AzureFunction '{tp.get('functionName')}' via connection '{ls}'."


register(ConversionRule(
    name="azure_function", adf_types=("AzureFunctionActivity",), fabric_types=("AzureFunction",),
    variants=("GET/POST", "headers/body"),
    strategy=ConversionStrategy.transformed, handler=convert_function, runtime_disposition="reuse_externally",
    property_rules=("method, functionName, headers, body carried", "linkedServiceName -> externalReferences.connection"),
    io_rules=("none",), expression_rules=("converted like Web",), dependency_rules=("dependsOn mapped 1:1",),
    parameter_variable_rules=("unchanged",), unsupported=("function-app key (belongs to the connection)",),
    manual_configuration=("Fabric connection to the function app",),
    validation_rules=("externalReferences.connection present", "functionName and method present"),
))
