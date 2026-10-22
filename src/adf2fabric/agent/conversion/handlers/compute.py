"""Compute and transformation activity conversions.

For every runtime-bound activity the converter states its DISPOSITION:

* ``reuse_externally`` - the same external runtime (Databricks, HDInsight, Azure Batch, Azure ML,
  Azure Data Explorer) is called from Fabric through a Fabric activity + connection.
* ``replace``          - the work moves to a Fabric component (notebook, Spark job, Dataflow Gen2).
  The target item must be authored/migrated; the pipeline gets a bound scaffold.
* ``redesign``         - Fabric has no equivalent (SSIS, retired services); the activity is blocked
  with documented alternatives and a fail-fast placeholder.

Fabric items that do not exist yet (notebooks, dataflows, Spark job definitions) are referenced by
a placeholder id and listed as prerequisites; this tool never invents an item id.
"""

from __future__ import annotations

from typing import Any, Optional

from adf2fabric.agent.conversion.context import ActivityBuilder
from adf2fabric.agent.conversion.handlers._common import block, carry
from adf2fabric.agent.conversion.registry import ConversionRule, register
from adf2fabric.project.conversion import ConversionStrategy
from adf2fabric.project.discovery import AdfOperationKind as Op


def _block(cx: ActivityBuilder, code: str, reason: str, *alts: str, disposition: str = "redesign") -> None:
    block(cx, code, reason, *alts, disposition=disposition)


def _ref(v: Any) -> Optional[str]:
    return v.get("referenceName") if isinstance(v, dict) and isinstance(v.get("referenceName"), str) else None


def _ls(cx: ActivityBuilder) -> Optional[str]:
    return cx.act.linked_service_refs[0] if cx.act.linked_service_refs else None


# --- Databricks ----------------------------------------------------------------------------------------------------


def _bound_fabric_notebook(cx: ActivityBuilder) -> Optional[str]:
    """The notebook's name when it was deployed to Fabric and bound in the resolutions file (``notebook:<name>``), where
    ``<name>`` is the last part of the Databricks ``notebookPath``. None when it was not, so nothing is registered."""
    path = cx.act.raw_properties.get("notebookPath")
    name = path.rstrip("/").rsplit("/", 1)[-1] if isinstance(path, str) else ""
    resolutions = cx.pc.project.resolutions
    return name if name and f"notebook:{name}".lower() in resolutions.items else None


# Confirmed by a live Fabric run on 2026-10-22 (docs/workload-migration.md): the `parameters` cell tag, the {value, type} shape and
# an Expression value all reached the notebook. Set False to restore the warning if a later change breaks that.
PARAMETER_PASSING_VERIFIED = True


def _workload(cx: ActivityBuilder) -> Any:
    """The approved, deployed workload manifest entry for this activity's notebook, if the user opted in."""
    path = cx.act.raw_properties.get("notebookPath")
    return cx.pc.project.workloads.get(path.lower()) if isinstance(path, str) else None


def _parameters_mapped(entry: Any, passed: dict) -> bool:
    """True when the generated notebook reads every parameter this activity passes (so passing them keeps the contract)."""
    by = {p.name: p for p in entry.parameters}
    return all(k in by and (by[k].mapped or not by[k].declared_in_code) for k in passed)


def _databricks_notebook_as_fabric_notebook(cx: ActivityBuilder, name: str, entry: Any = None) -> None:
    """Run the same code as a Fabric notebook instead of calling Databricks.

    Only for a notebook with no ``baseParameters`` and no ``libraries``: the parameter and library settings of the two
    activities do not map one to one, and nothing is guessed.
    """
    a, raw = cx.act, cx.act.raw_properties
    tp = {"notebookId": cx.item("notebook", name, f"The Fabric notebook that runs the code of Databricks notebook '{name}'."),
          "workspaceId": cx.workspace()}
    cx.mapping("typeProperties.notebookPath", "typeProperties.notebookId", "notebook_bound")
    passed = raw.get("baseParameters")
    if isinstance(passed, dict) and passed:
        read = {p.name for p in (entry.parameters if entry is not None else []) if p.mapped}
        tp["parameters"] = {k: {"value": cx.tree(v, f"typeProperties.baseParameters.{k}", f"typeProperties.parameters.{k}.value"),
                                "type": "string"} for k, v in passed.items() if k in read}
        for k in passed:
            if k in read:
                cx.mapping(f"typeProperties.baseParameters.{k}", f"typeProperties.parameters.{k}", "base_parameter_to_parameter")
            else:
                cx.dropped(f"typeProperties.baseParameters.{k}", "the migrated notebook never reads this parameter")
        if not tp["parameters"]:
            del tp["parameters"]
        elif not PARAMETER_PASSING_VERIFIED:
            cx.manual("notebook_parameters_unverified",
                      "Microsoft documents the notebook activity's 'parameters' object only as 'Object', and does not name the cell tag that "
                      "marks a parameter cell, so passing values this way has not been confirmed by a Fabric run.",
                      "Run the pipeline once and check the notebook received the values (docs/workload-migration.md has the probe).")
    for o in (entry.outputs if entry is not None else []):
        cx.manual("notebook_output_contract", f"{o.consumer} reads {o.source_property}, which the Fabric notebook activity does not produce.",
                  o.action)
    for key in raw:
        if key not in ("notebookPath", "baseParameters"):
            cx.dropped(f"typeProperties.{key}", "a Databricks notebook setting with no Fabric notebook activity equivalent")
    out = cx.add(cx.envelope(a.name, "TridentNotebook", tp))
    cx.cover(Op.external_execution, out)
    cx.strategy = ConversionStrategy.alternative
    cx.disposition = "replace"
    cx.summary = (f"DatabricksNotebook '{a.name}' -> Fabric Notebook activity bound to the deployed notebook {tp['notebookId']}; "
                  "check its storage access, lakehouse and environment.")


def convert_databricks_notebook(cx: ActivityBuilder) -> None:
    a = cx.act
    bound = _bound_fabric_notebook(cx)
    entry = _workload(cx)
    passed = cx.act.raw_properties.get("baseParameters")
    params_ok = not passed or (entry is not None and isinstance(passed, dict) and _parameters_mapped(entry, passed))
    if bound and params_ok and not cx.act.raw_properties.get("libraries"):
        return _databricks_notebook_as_fabric_notebook(cx, bound, entry)
    ls = _ls(cx)
    if not ls:
        return _block(cx, "databricks_no_linked_service", "it has no Databricks linked service.",
                      "Add the Databricks linked service in the source and re-convert.", disposition="reuse_externally")
    tp = carry(cx, ("notebookPath", "baseParameters", "libraries"))
    name = cx.add(cx.envelope(a.name, "DatabricksNotebook", tp, connection=cx.connection(ls)))
    cx.mapping("linkedServiceName", "externalReferences.connection", "linked_service_to_connection")
    cx.cover(Op.external_execution, name)
    cx.strategy = ConversionStrategy.transformed
    cx.disposition = "reuse_externally"
    cx.manual("databricks_cluster",
              "The cluster (existing cluster id or new-cluster settings) is defined in the ADF linked service, which "
              "is not read.", "Set existingClusterId or newClusterSettings on the Fabric Azure Databricks activity.")
    cx.summary = (f"DatabricksNotebook '{a.name}' -> Fabric Azure Databricks activity running notebook "
                  f"{tp.get('notebookPath')!r} on the same workspace (reused externally).")


def convert_databricks_other(cx: ActivityBuilder) -> None:
    t = cx.act.type
    _block(cx, "databricks_jar_python",
           f"the structure of Fabric's Databricks {'Spark JAR' if 'Jar' in (t or '') else 'Python'} task settings is "
           "not documented, so a faithful configuration cannot be generated.",
           "Create the equivalent task on the Fabric Azure Databricks activity (it supports Notebook, Jar and Python) "
           "using the original main class / python file and parameters.",
           "Wrap the job as a Databricks notebook and use the notebook mapping.", disposition="reuse_externally")


register(ConversionRule(
    name="databricks_notebook", adf_types=("DatabricksNotebook",), fabric_types=("DatabricksNotebook",),
    variants=("existing cluster", "new job cluster", "libraries", "base parameters"),
    strategy=ConversionStrategy.transformed, handler=convert_databricks_notebook,
    runtime_disposition="reuse_externally",
    property_rules=("notebookPath, baseParameters, libraries carried", "linkedServiceName -> externalReferences.connection"),
    io_rules=("none",), expression_rules=("baseParameters expressions converted",),
    dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",),
    unsupported=("cluster settings held in the ADF linked service",),
    manual_configuration=("Fabric Databricks connection", "existingClusterId / newClusterSettings"),
    validation_rules=("notebookPath present", "connection present"),
))
def convert_databricks_job(cx: ActivityBuilder) -> None:
    _block(cx, "databricks_job",
           "Fabric's Azure Databricks activity has a Job type, but only its portal settings are documented (a job picked "
           "from a list, plus job parameters), not the JSON property names, so a faithful configuration cannot be "
           "generated.",
           "Create the Job task on the Fabric Azure Databricks activity (Settings > Job) with the original job and "
           "job parameters.", disposition="reuse_externally")


register(ConversionRule(
    name="databricks_job", adf_types=("DatabricksJob",), fabric_types=(), variants=("job id", "job parameters"),
    strategy=ConversionStrategy.manual, handler=convert_databricks_job, runtime_disposition="reuse_externally",
    property_rules=("not converted: the Fabric Job task's JSON structure is undocumented",), io_rules=("none",),
    expression_rules=("none",), dependency_rules=("a fail-fast placeholder keeps the dependency graph intact",),
    parameter_variable_rules=("none",), unsupported=("Job task settings (jobId, jobParameters)",),
    manual_configuration=("recreate the Job task on the Fabric Azure Databricks activity",),
    validation_rules=("placeholder Fail activity present",),
))
register(ConversionRule(
    name="databricks_jar_python", adf_types=("DatabricksSparkJar", "DatabricksSparkPython"),
    fabric_types=(), variants=("Spark JAR", "Python file"),
    strategy=ConversionStrategy.manual, handler=convert_databricks_other, runtime_disposition="reuse_externally",
    property_rules=("not converted: the Fabric task structure is undocumented",), io_rules=("none",),
    expression_rules=("none",), dependency_rules=("a fail-fast placeholder keeps the dependency graph intact",),
    parameter_variable_rules=("none",), unsupported=("Spark JAR / Python task settings",),
    manual_configuration=("recreate the task on the Fabric Azure Databricks activity",),
    validation_rules=("placeholder Fail activity present",),
))


# --- HDInsight -------------------------------------------------------------------------------------------------------


def convert_hdinsight(cx: ActivityBuilder) -> None:
    a = cx.act
    kind = {"hdinsighthive": "Hive", "hdinsightpig": "Pig", "hdinsightmapreduce": "MapReduce",
            "hdinsightstreaming": "Streaming", "hdinsightspark": "Spark"}[(a.type or "").lower()]
    ls = _ls(cx)
    if kind in ("MapReduce", "Streaming", "Spark") or not ls:
        return _block(
            cx, "hdinsight_program_settings",
            f"the {kind} program settings (jar/class, mapper/reducer, entry file) have no documented Fabric "
            "property, so the job cannot be configured faithfully." if kind != "Hive" and kind != "Pig"
            else "it has no HDInsight linked service.",
            "Recreate the job on the Fabric HDInsight activity using the original program settings.",
            "Move Spark workloads to a Fabric notebook or Spark job definition.", disposition="reuse_externally")
    raw = a.raw_properties
    tp: dict[str, Any] = {"hdiActivityType": kind}
    for key in ("arguments", "getDebugInfo", "defines", "variables", "queryTimeout"):
        if key in raw:
            tp[key] = cx.tree(raw[key], f"typeProperties.{key}", f"typeProperties.{key}")
            cx.mapping(f"typeProperties.{key}", f"typeProperties.{key}", "passthrough")
    script_ls = _ref(raw.get("scriptLinkedService"))
    if "scriptPath" in raw or script_ls:
        settings: dict[str, Any] = {}
        if "scriptPath" in raw:
            settings["scriptPath"] = cx.tree(raw["scriptPath"], "typeProperties.scriptPath",
                                             "typeProperties.scriptSettings.scriptPath")
        if script_ls:
            settings["externalReferences"] = {"connection": cx.connection(script_ls, "HDInsight script storage")}
        tp["scriptSettings"] = settings
        cx.mapping("typeProperties.scriptPath/scriptLinkedService", "typeProperties.scriptSettings", "script_settings")
    for key in raw:
        if key not in ("arguments", "getDebugInfo", "defines", "variables", "queryTimeout", "scriptPath",
                       "scriptLinkedService"):
            cx.dropped(f"typeProperties.{key}", "not a documented Fabric HDInsight property")
    name = cx.add(cx.envelope(a.name, "AzureHDInsight", tp, connection=cx.connection(ls)))
    cx.cover(Op.external_execution, name)
    cx.strategy = ConversionStrategy.transformed
    cx.disposition = "reuse_externally"
    cx.summary = f"HDInsight{kind} '{a.name}' -> Fabric HDInsight activity (type {kind}) on the same cluster."


register(ConversionRule(
    name="hdinsight", adf_types=("HDInsightHive", "HDInsightPig", "HDInsightMapReduce", "HDInsightStreaming",
                                 "HDInsightSpark"),
    fabric_types=("AzureHDInsight",), variants=("Hive", "Pig", "MapReduce", "Streaming", "Spark"),
    strategy=ConversionStrategy.transformed, handler=convert_hdinsight, runtime_disposition="reuse_externally",
    property_rules=("hdiActivityType derived from the ADF type", "arguments/getDebugInfo/defines/variables/"
                    "queryTimeout carried", "scriptPath + scriptLinkedService -> scriptSettings"),
    io_rules=("none",), expression_rules=("converted like Copy",), dependency_rules=("dependsOn mapped 1:1",),
    parameter_variable_rules=("unchanged",),
    unsupported=("MapReduce / Streaming / Spark program settings (undocumented in Fabric) -> blocked",
                 "storageLinkedServices"),
    manual_configuration=("Fabric HDInsight connection",), validation_rules=("hdiActivityType present",),
))


# --- Custom (Azure Batch) ----------------------------------------------------------------------------------------------


def convert_custom(cx: ActivityBuilder) -> None:
    a, raw = cx.act, cx.act.raw_properties
    batch = _ls(cx)
    if not batch or "command" not in raw:
        return _block(cx, "custom_incomplete", "it has no Azure Batch linked service or no command.",
                      "Complete the activity in the source and re-convert.", disposition="reuse_externally")
    conn = cx.connection(batch, "Azure Batch")
    tp = carry(cx, ("command", "folderPath", "extendedProperties", "retentionTimeInDays", "autoUserSpecification"),
               skip=("resourceLinkedService", "referenceObjects"))
    tp["externalReferences"] = {"connection": conn}
    extra: dict[str, Any] = {}
    res = _ref(raw.get("resourceLinkedService"))
    if res:
        extra["staging"] = {"externalReferences": {"connection": cx.connection(res, "Azure Batch resource storage")}}
        cx.mapping("typeProperties.resourceLinkedService", "staging.externalReferences.connection",
                   "linked_service_to_connection")
    if "referenceObjects" in raw:
        cx.dropped("typeProperties.referenceObjects", "linked services/datasets passed to the custom activity do not "
                   "exist in Fabric", manual="Pass the needed values through extendedProperties or parameters.")
    name = cx.add(cx.envelope(a.name, "Custom", tp, connection=conn, extra=extra))
    cx.cover(Op.external_execution, name)
    cx.strategy = ConversionStrategy.transformed
    cx.disposition = "reuse_externally"
    cx.summary = f"Custom '{a.name}' -> Fabric Azure Batch (Custom) activity running {tp.get('command')!r}."


register(ConversionRule(
    name="custom", adf_types=("Custom",), fabric_types=("Custom",), variants=("with resource storage", "extended properties"),
    strategy=ConversionStrategy.transformed, handler=convert_custom, runtime_disposition="reuse_externally",
    property_rules=("command, folderPath, extendedProperties, retentionTimeInDays, autoUserSpecification carried",
                    "linkedServiceName -> externalReferences (activity and typeProperties)",
                    "resourceLinkedService -> staging.externalReferences"),
    io_rules=("referenceObjects (linked services/datasets) are not carried",), expression_rules=("converted like Copy",),
    dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",),
    unsupported=("referenceObjects",), manual_configuration=("Fabric Azure Batch connection",),
    validation_rules=("command present", "externalReferences at both required levels"),
))


# --- Azure ML / Kusto ----------------------------------------------------------------------------------------------------


def convert_aml_pipeline(cx: ActivityBuilder) -> None:
    a = cx.act
    ls = _ls(cx)
    if not ls:
        return _block(cx, "aml_no_linked_service", "it has no Azure ML linked service.", disposition="reuse_externally")
    tp = carry(cx, ("mlPipelineId", "mlPipelineEndpointId", "version", "experimentName", "mlPipelineParameters",
                    "dataPathAssignments", "mlParentRunId", "continueOnStepFailure"))
    cfg = cx.envelope(a.name, "AzureMLExecutePipeline", tp, connection=cx.connection(ls))
    cfg.setdefault("policy", {})  # the Fabric table marks policy required for this type
    name = cx.add(cfg)
    cx.cover(Op.external_execution, name)
    cx.strategy = ConversionStrategy.transformed
    cx.disposition = "reuse_externally"
    cx.summary = f"AzureMLExecutePipeline '{a.name}' -> Fabric Azure Machine Learning activity (same published pipeline)."


def convert_aml_deprecated(cx: ActivityBuilder) -> None:
    _block(cx, "aml_deprecated", f"{cx.act.type} is deprecated in ADF and not available in Fabric.",
           "Publish the model as an Azure ML pipeline or batch endpoint and call it with the Fabric Azure Machine "
           "Learning activity.", disposition="redesign")


def convert_kusto(cx: ActivityBuilder) -> None:
    a = cx.act
    ls = _ls(cx)
    if not ls or "command" not in a.raw_properties:
        return _block(cx, "kusto_incomplete", "it has no linked service or no command.", disposition="reuse_externally")
    tp = carry(cx, ("command", "commandTimeout"))
    name = cx.add(cx.envelope(a.name, "KustoQueryLanguage", tp, connection=cx.connection(ls)))
    cx.cover(Op.transform, name)
    cx.strategy = ConversionStrategy.transformed
    cx.disposition = "reuse_externally"
    cx.manual("kusto_database", "The database is part of the ADF linked service, which is not read.",
              "Set the database on the Fabric KQL activity.")
    cx.summary = f"AzureDataExplorerCommand '{a.name}' -> Fabric KQL activity running the same control command."


register(ConversionRule(
    name="azure_ml", adf_types=("AzureMLExecutePipeline",), fabric_types=("AzureMLExecutePipeline",),
    variants=("published pipeline", "pipeline endpoint"), strategy=ConversionStrategy.transformed,
    handler=convert_aml_pipeline, runtime_disposition="reuse_externally",
    property_rules=("pipeline id/endpoint/version/experiment/parameters carried",
                    "linkedServiceName -> externalReferences.connection"),
    io_rules=("none",), expression_rules=("converted like Copy",), dependency_rules=("dependsOn mapped 1:1",),
    parameter_variable_rules=("unchanged",), unsupported=(), manual_configuration=("Fabric Azure ML connection",),
    validation_rules=("connection present", "policy present"),
))
register(ConversionRule(
    name="azure_ml_deprecated", adf_types=("AzureMLBatchExecution", "AzureMLUpdateResource"), fabric_types=(),
    variants=("batch execution", "update resource"), strategy=ConversionStrategy.manual,
    handler=convert_aml_deprecated, runtime_disposition="redesign",
    property_rules=("not converted: deprecated, unavailable in Fabric",), io_rules=("none",), expression_rules=("none",),
    dependency_rules=("a fail-fast placeholder keeps the dependency graph intact",), parameter_variable_rules=("none",),
    unsupported=("Azure ML Studio (classic) batch execution / update resource",),
    manual_configuration=("republish via Azure ML pipelines or batch endpoints",),
    validation_rules=("placeholder Fail activity present",),
))
register(ConversionRule(
    name="kusto", adf_types=("AzureDataExplorerCommand",), fabric_types=("KustoQueryLanguage",),
    variants=("control command",), strategy=ConversionStrategy.transformed, handler=convert_kusto,
    runtime_disposition="reuse_externally",
    property_rules=("command, commandTimeout carried",), io_rules=("none",), expression_rules=("converted like Copy",),
    dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",), unsupported=(),
    manual_configuration=("Fabric Kusto connection", "database"), validation_rules=("command present",),
))


# --- Fabric-item scaffolds: Synapse notebook, Spark job, data flows ------------------------------------------------------------


def convert_synapse_notebook(cx: ActivityBuilder) -> None:
    a, raw = cx.act, cx.act.raw_properties
    nb = _ref(raw.get("notebook")) or "notebook"
    params: dict[str, Any] = {}
    for k, v in (raw.get("parameters") or {}).items():
        params[k] = cx.tree(v, f"typeProperties.parameters.{k}", f"typeProperties.parameters.{k}")
        cx.mapping(f"typeProperties.parameters.{k}", f"typeProperties.parameters.{k}", "passthrough")
    notebook_id = cx.item("notebook", nb, f"The Fabric notebook migrated from Synapse notebook '{nb}'.")
    bound = not notebook_id.startswith("<")  # a real id came from the resolutions file: the notebook is already in Fabric
    tp: dict[str, Any] = {
        "notebookId": notebook_id,
        "workspaceId": cx.workspace(),
    }
    if params:
        tp["parameters"] = params
    for key in raw:
        if key not in ("notebook", "parameters"):
            cx.dropped(f"typeProperties.{key}", "Spark pool / executor / configuration settings do not map to the "
                       "Fabric notebook activity (they belong to a Fabric environment)")
    name = cx.add(cx.envelope(a.name, "TridentNotebook", tp))
    cx.cover(Op.external_execution, name)
    cx.strategy = ConversionStrategy.alternative
    cx.disposition = "replace"
    if bound:
        cx.summary = (f"SynapseNotebook '{a.name}' -> Fabric Notebook activity bound to the deployed notebook "
                      f"{notebook_id}; check its content, lakehouse and environment.")
        return
    cx.manual("synapse_notebook_migrate", f"Synapse notebook '{nb}' must be migrated to a Fabric notebook.",
              f"Import '{nb}' into the Fabric workspace, attach a lakehouse/environment, then bind its item id.")
    cx.alternative("Run the notebook content as a Spark job definition if it is a packaged job.")
    cx.summary = f"SynapseNotebook '{a.name}' -> Fabric Notebook activity (scaffold) for '{nb}'; the notebook must be migrated."


def convert_spark_job(cx: ActivityBuilder) -> None:
    a, raw = cx.act, cx.act.raw_properties
    job = _ref(raw.get("sparkJob")) or "sparkjob"
    tp: dict[str, Any] = {
        "sparkJobDefinitionId": cx.item("sparkjob", job, f"The Fabric Spark job definition migrated from '{job}'."),
        "workspaceId": cx.workspace(),
    }
    for src, dst in (("file", "executableFile"), ("className", "mainClass")):
        if src in raw:
            tp[dst] = cx.tree(raw[src], f"typeProperties.{src}", f"typeProperties.{dst}")
            cx.mapping(f"typeProperties.{src}", f"typeProperties.{dst}", "renamed")
    if isinstance(raw.get("args"), list):
        tp["commandLineArguments"] = " ".join(str(x) for x in raw["args"] if not isinstance(x, (dict, list)))
        cx.mapping("typeProperties.args", "typeProperties.commandLineArguments", "joined")
    for key in raw:
        if key not in ("sparkJob", "file", "className", "args"):
            cx.dropped(f"typeProperties.{key}", "pool/executor/configuration settings belong to the Fabric Spark job "
                       "definition and environment")
    name = cx.add(cx.envelope(a.name, "SparkJobDefinition", tp))
    cx.cover(Op.external_execution, name)
    cx.strategy = ConversionStrategy.alternative
    cx.disposition = "replace"
    cx.manual("spark_job_migrate", f"Spark job definition '{job}' must be recreated in Fabric.",
              "Create the Spark job definition in the workspace and bind its item id.")
    cx.summary = f"SparkJob '{a.name}' -> Fabric Spark job definition activity (scaffold) for '{job}'."


def convert_dataflow(cx: ActivityBuilder) -> None:
    a, raw = cx.act, cx.act.raw_properties
    df = a.configuration.get("dataflow") or _ref(raw.get("dataflow")) or "dataflow"
    params = {}
    dfraw = raw.get("dataflow") if isinstance(raw.get("dataflow"), dict) else {}
    for k, v in (dfraw.get("parameters") or {}).items():
        params[k] = cx.tree(v, f"typeProperties.dataflow.parameters.{k}", f"typeProperties.parameters.{k}")
        cx.mapping(f"typeProperties.dataflow.parameters.{k}", f"typeProperties.parameters.{k}", "passthrough")
    tp: dict[str, Any] = {
        "dataflowId": cx.item("dataflow", df, f"The Fabric Dataflow Gen2 that replaces data flow '{df}'."),
        "workspaceId": cx.workspace(),
    }
    if params:
        tp["parameters"] = params
    for key in raw:
        if key not in ("dataflow",):
            cx.dropped(f"typeProperties.{key}", "Spark compute/staging/trace settings of a mapping data flow do not "
                       "apply to a Dataflow Gen2 refresh")
    name = cx.add(cx.envelope(a.name, "RefreshDataFlow", tp))
    cx.cover(Op.transform, name)
    cx.strategy = ConversionStrategy.alternative
    cx.disposition = "replace"
    cx.manual("dataflow_reauthor",
              f"Data flow '{df}' is not converted automatically: its transformation graph is not part of the pipeline.",
              "Upgrade it with Fabric's built-in Mapping Data Flow upgrade (Dataflow Gen2 with MDF transforms, "
              "preview) or re-author it, then bind the Dataflow item id.")
    cx.alternative("Re-implement the transformation as a Fabric notebook (Spark).")
    cx.alternative("Use Power Query in Dataflow Gen2 as a redesign option.")
    cx.summary = f"{a.type} '{a.name}' -> Fabric Dataflow activity (scaffold) refreshing a Dataflow Gen2 for '{df}'."


def convert_ssis(cx: ActivityBuilder) -> None:
    _block(cx, "ssis_unsupported", "SSIS package execution is not available in Fabric Data Factory.",
           "Keep the package on the Azure-SSIS integration runtime in ADF and invoke the ADF pipeline from Fabric "
           "with the Invoke pipeline activity (it can call ADF pipelines).",
           "Re-implement the package's data flow as Copy / Dataflow Gen2 / notebook activities.",
           disposition="redesign")


def convert_usql(cx: ActivityBuilder) -> None:
    _block(cx, "usql_retired", "Azure Data Lake Analytics (U-SQL) is retired.",
           "Re-implement the U-SQL job as a Fabric notebook (Spark) or Dataflow Gen2.", disposition="redesign")


for _rule in (
    ConversionRule(
        name="synapse_notebook", adf_types=("SynapseNotebook",), fabric_types=("TridentNotebook",),
        variants=("with parameters",), strategy=ConversionStrategy.alternative, handler=convert_synapse_notebook,
        runtime_disposition="replace",
        property_rules=("parameters carried", "notebook reference -> notebookId placeholder"),
        io_rules=("none",), expression_rules=("parameter expressions converted",),
        dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",),
        unsupported=("Spark pool, executor and configuration settings",),
        manual_configuration=("migrate the notebook to Fabric", "bind notebookId and workspaceId"),
        validation_rules=("notebookId and workspaceId present",)),
    ConversionRule(
        name="spark_job", adf_types=("SparkJob",), fabric_types=("SparkJobDefinition",),
        variants=("with arguments",), strategy=ConversionStrategy.alternative, handler=convert_spark_job,
        runtime_disposition="replace",
        property_rules=("file -> executableFile", "className -> mainClass", "args -> commandLineArguments"),
        io_rules=("none",), expression_rules=("converted like Copy",), dependency_rules=("dependsOn mapped 1:1",),
        parameter_variable_rules=("unchanged",), unsupported=("pool / executor / configuration settings",),
        manual_configuration=("create the Spark job definition", "bind its id"),
        validation_rules=("sparkJobDefinitionId and workspaceId present",)),
    ConversionRule(
        name="dataflow", adf_types=("ExecuteDataFlow", "ExecuteWranglingDataflow"), fabric_types=("RefreshDataFlow",),
        variants=("mapping data flow", "wrangling data flow"), strategy=ConversionStrategy.alternative,
        handler=convert_dataflow, runtime_disposition="replace",
        property_rules=("dataflow parameters carried", "data flow reference -> dataflowId placeholder"),
        io_rules=("none",), expression_rules=("parameter expressions converted",),
        dependency_rules=("dependsOn mapped 1:1",), parameter_variable_rules=("unchanged",),
        unsupported=("Spark compute / staging / trace settings of mapping data flows",),
        manual_configuration=("upgrade or re-author the data flow as Dataflow Gen2", "bind the dataflow id"),
        validation_rules=("dataflowId and workspaceId present",)),
    ConversionRule(
        name="ssis", adf_types=("ExecuteSSISPackage",), fabric_types=(), variants=("SSISDB", "file system"),
        strategy=ConversionStrategy.manual, handler=convert_ssis, runtime_disposition="redesign",
        property_rules=("not converted: no Fabric equivalent",), io_rules=("none",), expression_rules=("none",),
        dependency_rules=("a fail-fast placeholder keeps the dependency graph intact",),
        parameter_variable_rules=("none",), unsupported=("SSIS package execution",),
        manual_configuration=("choose an alternative",), validation_rules=("placeholder Fail activity present",)),
    ConversionRule(
        name="usql", adf_types=("DataLakeAnalyticsU-SQL",), fabric_types=(), variants=("script",),
        strategy=ConversionStrategy.manual, handler=convert_usql, runtime_disposition="redesign",
        property_rules=("not converted: service retired",), io_rules=("none",), expression_rules=("none",),
        dependency_rules=("a fail-fast placeholder keeps the dependency graph intact",),
        parameter_variable_rules=("none",), unsupported=("U-SQL",), manual_configuration=("re-implement",),
        validation_rules=("placeholder Fail activity present",)),
):
    register(_rule)
