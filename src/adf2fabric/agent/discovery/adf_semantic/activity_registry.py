"""Explicit registry of the ADF activity types the parser understands.

One :class:`ActivitySpec` per documented activity ``type``. A spec states what the
activity IS (category + operations), which properties it must carry, and which
``typeProperties`` keys are known. Anything not listed in a spec is preserved under
``unknown_properties``; any activity ``type`` not in this registry is still parsed
structurally and reported as unknown — it is never dropped.

The registry is data. It contains no Fabric knowledge: it describes the SOURCE
activity so a separate converter can decide what to do with it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from adf2fabric.project.discovery import AdfOperationKind as Op
from adf2fabric.project.discovery import AdfSemanticCategory as Cat

# Keys an activity object may carry at its top level.
TOP_LEVEL_KEYS = frozenset({
    "name", "type", "description", "state", "onInactiveMarkAs", "dependsOn",
    "userProperties", "policy", "typeProperties", "inputs", "outputs", "linkedServiceName",
})

# typeProperties key -> the branch label its child activities live under.
NESTED_SLOTS = {
    "activities": "activities",
    "ifTrueActivities": "ifTrue",
    "ifFalseActivities": "ifFalse",
    "defaultActivities": "default",
}


@dataclass(frozen=True)
class ActivitySpec:
    type: str
    category: Cat
    operations: tuple[Op, ...]  # first entry is the primary operation
    required: tuple[str, ...] = ()  # property paths relative to the activity
    known: tuple[str, ...] = ()  # known typeProperties keys
    handler: str = ""  # specific interpretation in parser_pipelines ("" = generic)
    description: str = ""
    top_level: tuple[str, ...] = ()  # extra activity-level keys this type documents (e.g. a Synapse ``sqlPool``)
    # (canonical path, alternative spelling) pairs: Microsoft's own pages sometimes spell a key two ways, and either
    # is accepted without reporting the property as missing or unknown.
    aliases: tuple[tuple[str, str], ...] = ()


def _spec(type_: str, cat: Cat, ops: tuple[Op, ...], required=(), known=(), handler="", description="",
          top_level=(), aliases=()):
    return ActivitySpec(type_, cat, ops, tuple(required), tuple(known), handler, description,
                        tuple(top_level), tuple(aliases))


_tp = lambda *keys: tuple(f"typeProperties.{k}" for k in keys)  # noqa: E731

_SPECS: list[ActivitySpec] = [
    # --- data movement -------------------------------------------------------------
    _spec("Copy", Cat.data_movement, (Op.read, Op.write),
          _tp("source", "sink"),
          ("source", "sink", "translator", "enableStaging", "stagingSettings", "parallelCopies",
           "dataIntegrationUnits", "enableSkipIncompatibleRow", "redirectIncompatibleRowSettings",
           "logStorageSettings", "logSettings", "preserve", "preserveRules",
           "validateDataConsistency", "skipErrorFile", "sessionLog", "maxConcurrentConnections"),
          "copy", "Copies data from a source to a sink."),
    # --- data retrieval / validation ------------------------------------------------------
    _spec("Lookup", Cat.data_retrieval, (Op.read,), _tp("source", "dataset"),
          ("source", "dataset", "firstRowOnly"), "lookup", "Reads rows from a source into the pipeline."),
    _spec("GetMetadata", Cat.data_retrieval, (Op.read,), _tp("dataset"),
          ("dataset", "fieldList", "storeSettings", "formatSettings"), "dataset_only",
          "Reads metadata (existence, size, child items...) of a dataset."),
    _spec("Validation", Cat.validation, (Op.validate,), _tp("dataset"),
          ("dataset", "timeout", "sleep", "minimumSize", "childItems"), "dataset_only",
          "Waits until a dataset satisfies existence/size conditions."),
    _spec("Delete", Cat.data_transformation, (Op.delete,), _tp("dataset"),
          ("dataset", "recursive", "maxConcurrentConnections", "enableLogging", "logStorageSettings",
           "storeSettings"), "dataset_only", "Deletes files or folders from a store."),
    # --- data transformation / computation --------------------------------------------------
    _spec("ExecuteDataFlow", Cat.data_transformation, (Op.transform,), _tp("dataflow"),
          ("dataflow", "compute", "integrationRuntime", "staging", "traceLevel", "continueOnError",
           "runConcurrently", "sourceStagingConcurrency"), "dataflow", "Runs a Mapping Data Flow."),
    _spec("ExecuteWranglingDataflow", Cat.data_transformation, (Op.transform,), _tp("dataflow"),
          ("dataflow", "compute", "integrationRuntime", "staging", "traceLevel", "continueOnError",
           "runConcurrently", "sourceStagingConcurrency", "queries", "sinks"), "dataflow",
          "Runs a Power Query (wrangling) data flow."),
    _spec("SqlServerStoredProcedure", Cat.data_transformation, (Op.transform,), _tp("storedProcedureName"),
          ("storedProcedureName", "storedProcedureParameters"), "stored_procedure",
          "Executes a stored procedure."),
    # Synapse pipelines only (not in the Data Factory ARM schema). Source: the Synapse "SQL pool stored procedure
    # activity" page: ``sqlPool`` is an ACTIVITY-level reference (type SqlPoolReference), not a linked service.
    _spec("SqlPoolStoredProcedure", Cat.data_transformation, (Op.transform,), ("sqlPool", *_tp("storedProcedureName")),
          ("storedProcedureName", "storedProcedureParameters"), "sql_pool_stored_procedure",
          "Executes a stored procedure in a Synapse dedicated SQL pool.", top_level=("sqlPool",)),
    _spec("Script", Cat.data_transformation, (Op.transform,), _tp("scripts"),
          ("scripts", "scriptBlockExecutionTimeout", "logSettings"), "script",
          "Executes one or more SQL script blocks."),
    _spec("ExecuteSSISPackage", Cat.compute, (Op.external_execution,), _tp("packageLocation"),
          ("packageLocation", "runtime", "loggingLevel", "environmentPath", "executionCredential",
           "connectVia", "projectParameters", "packageParameters", "projectConnectionManagers",
           "packageConnectionManagers", "propertyOverrides", "logLocation"), "",
          "Runs an SSIS package on the Azure-SSIS integration runtime."),
    _spec("AzureDataExplorerCommand", Cat.data_transformation, (Op.transform,), _tp("command"),
          ("command", "commandTimeout"), "", "Runs a Kusto management command."),
    # --- compute ------------------------------------------------------------------------------
    _spec("DatabricksNotebook", Cat.compute, (Op.external_execution,), _tp("notebookPath"),
          ("notebookPath", "baseParameters", "libraries"), "", "Runs a Databricks notebook."),
    # Source: Microsoft's ARM schema (DatabricksJobActivity) and the Databricks Job activity page. That page's sample
    # JSON writes the key ``jobID`` while its property table and the ARM schema say ``jobId``; both are accepted.
    _spec("DatabricksJob", Cat.compute, (Op.external_execution,), _tp("jobId"),
          ("jobId", "jobParameters"), "databricks_job", "Runs a Databricks job (serverless jobs included).",
          aliases=(("typeProperties.jobId", "typeProperties.jobID"),)),
    _spec("DatabricksSparkJar", Cat.compute, (Op.external_execution,), _tp("mainClassName"),
          ("mainClassName", "parameters", "libraries"), "", "Runs a Databricks Spark JAR."),
    _spec("DatabricksSparkPython", Cat.compute, (Op.external_execution,), _tp("pythonFile"),
          ("pythonFile", "parameters", "libraries"), "", "Runs a Databricks Python file."),
    _spec("HDInsightHive", Cat.compute, (Op.external_execution,), (),
          ("scriptPath", "scriptLinkedService", "defines", "arguments", "getDebugInfo",
           "storageLinkedServices", "variables", "queryTimeout"), "", "Runs a Hive script on HDInsight."),
    _spec("HDInsightPig", Cat.compute, (Op.external_execution,), (),
          ("scriptPath", "scriptLinkedService", "defines", "arguments", "getDebugInfo",
           "storageLinkedServices"), "", "Runs a Pig script on HDInsight."),
    _spec("HDInsightMapReduce", Cat.compute, (Op.external_execution,), _tp("className", "jarFilePath"),
          ("className", "jarFilePath", "jarLinkedService", "jarLibs", "defines", "arguments",
           "getDebugInfo", "storageLinkedServices"), "", "Runs a MapReduce job on HDInsight."),
    _spec("HDInsightStreaming", Cat.compute, (Op.external_execution,), _tp("mapper", "reducer"),
          ("mapper", "reducer", "input", "output", "filePaths", "fileLinkedService", "combiner",
           "commandEnvironment", "defines", "arguments", "getDebugInfo", "storageLinkedServices"),
          "", "Runs a Hadoop streaming job on HDInsight."),
    _spec("HDInsightSpark", Cat.compute, (Op.external_execution,), _tp("rootPath", "entryFilePath"),
          ("rootPath", "entryFilePath", "arguments", "getDebugInfo", "sparkJobLinkedService",
           "className", "proxyUser", "sparkConfig"), "", "Runs a Spark program on HDInsight."),
    _spec("Custom", Cat.compute, (Op.external_execution,), _tp("command"),
          ("command", "resourceLinkedService", "folderPath", "referenceObjects", "extendedProperties",
           "retentionTimeInDays", "autoUserSpecification"), "",
          "Runs a custom command on Azure Batch."),
    _spec("AzureMLBatchExecution", Cat.compute, (Op.external_execution,), (),
          ("globalParameters", "webServiceInputs", "webServiceOutputs"), "", "Runs an Azure ML batch."),
    _spec("AzureMLUpdateResource", Cat.compute, (Op.external_execution,), (),
          ("trainedModelName", "trainedModelLinkedServiceName", "trainedModelFilePath"), "",
          "Updates an Azure ML model."),
    _spec("AzureMLExecutePipeline", Cat.compute, (Op.external_execution,), (),
          ("mlPipelineId", "mlPipelineEndpointId", "version", "experimentName", "mlPipelineParameters",
           "dataPathAssignments", "mlParentRunId", "continueOnStepFailure"), "",
          "Runs a published Azure ML pipeline."),
    _spec("DataLakeAnalyticsU-SQL", Cat.compute, (Op.external_execution,), _tp("scriptPath", "scriptLinkedService"),
          ("scriptPath", "scriptLinkedService", "degreeOfParallelism", "priority", "parameters",
           "runtimeVersion", "compilationMode"), "", "Runs a U-SQL script."),
    _spec("SynapseNotebook", Cat.compute, (Op.external_execution,), _tp("notebook"),
          ("notebook", "sparkPool", "parameters", "executorSize", "conf", "driverSize", "numExecutors",
           "configurationType", "targetSparkConfiguration", "sparkConfig"), "",
          "Runs a Synapse notebook."),
    _spec("SparkJob", Cat.compute, (Op.external_execution,), _tp("sparkJob"),
          ("sparkJob", "args", "targetBigDataPool", "executorSize", "conf", "driverSize",
           "numExecutors", "file", "className", "files", "pythonCodeReference", "filesV2",
           "targetSparkConfiguration", "sparkConfig", "scanFolder"), "",
          "Runs a Synapse Spark job definition."),
    # --- control flow -------------------------------------------------------------------------------
    _spec("IfCondition", Cat.control_flow, (Op.branch,), _tp("expression"),
          ("expression", "ifTrueActivities", "ifFalseActivities"), "if", "Branches on a boolean expression."),
    _spec("Switch", Cat.control_flow, (Op.branch,), _tp("on"),
          ("on", "cases", "defaultActivities"), "switch", "Branches on the value of an expression."),
    _spec("ForEach", Cat.control_flow, (Op.loop,), _tp("items", "activities"),
          ("items", "isSequential", "batchCount", "activities"), "foreach",
          "Runs its activities once per item of a collection."),
    _spec("Until", Cat.control_flow, (Op.loop,), _tp("expression", "activities"),
          ("expression", "timeout", "activities"), "until",
          "Repeats its activities until an expression is true."),
    _spec("Filter", Cat.control_flow, (Op.filter,), _tp("items", "condition"),
          ("items", "condition"), "filter", "Filters an array by a condition."),
    _spec("Wait", Cat.control_flow, (Op.wait,), _tp("waitTimeInSeconds"),
          ("waitTimeInSeconds",), "wait", "Pauses the pipeline."),
    _spec("Fail", Cat.control_flow, (Op.fail,), _tp("message", "errorCode"),
          ("message", "errorCode"), "", "Fails the pipeline with a custom message and code."),
    _spec("ExecutePipeline", Cat.orchestration, (Op.orchestrate,), _tp("pipeline"),
          ("pipeline", "parameters", "waitOnCompletion"), "execute_pipeline",
          "Invokes another pipeline."),
    # --- variables ------------------------------------------------------------------------------------
    _spec("SetVariable", Cat.variable_management, (Op.set_variable,), _tp("variableName", "value"),
          ("variableName", "value", "setSystemVariable"), "set_variable",
          "Assigns a pipeline variable (or the pipeline return value)."),
    _spec("AppendVariable", Cat.variable_management, (Op.append_variable,), _tp("variableName", "value"),
          ("variableName", "value"), "append_variable", "Appends a value to an array variable."),
    # --- external execution / integration --------------------------------------------------------------------
    _spec("WebActivity", Cat.external_execution, (Op.external_execution,), _tp("url", "method"),
          ("url", "method", "headers", "body", "authentication", "connectVia", "turnOffAsync",
           "disableCertValidation", "httpRequestTimeout", "datasets", "linkedServices"), "web",
          "Calls an HTTP endpoint."),
    _spec("WebHook", Cat.external_execution, (Op.external_execution,), _tp("url", "method"),
          ("url", "method", "headers", "body", "timeout", "authentication", "reportStatusOnCallBack"),
          "web", "Calls an HTTP endpoint and waits for a callback."),
    _spec("AzureFunctionActivity", Cat.external_execution, (Op.external_execution,), _tp("functionName", "method"),
          ("functionName", "method", "headers", "body"), "web", "Calls an Azure Function."),
]

ACTIVITY_REGISTRY: dict[str, ActivitySpec] = {s.type.lower(): s for s in _SPECS}

# Activity types that need an activity-level ``linkedServiceName``.
REQUIRES_LINKED_SERVICE = frozenset({
    "sqlserverstoredprocedure", "script", "azurefunctionactivity", "databricksnotebook", "databricksjob",
    "databrickssparkjar", "databrickssparkpython", "hdinsighthive", "hdinsightpig",
    "hdinsightmapreduce", "hdinsightstreaming", "hdinsightspark", "custom",
    "azuremlbatchexecution", "azuremlupdateresource", "datalakeanalyticsu-sql",
    "azuredataexplorercommand",
})


def get_spec(type_name: Optional[str]) -> Optional[ActivitySpec]:
    """The spec for an activity type (case-insensitive), or ``None`` if unknown."""
    return ACTIVITY_REGISTRY.get((type_name or "").strip().lower())


def snake_case(name: str) -> str:
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return s.replace("-", "_").lower()
