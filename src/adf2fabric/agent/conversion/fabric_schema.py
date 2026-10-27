"""The Fabric data pipeline schema the converter is allowed to emit.

This table is the single source of truth for what generated Fabric JSON may contain. It is
encoded from Microsoft's documentation (see ``SOURCES``):

* "DataPipeline definition" (Fabric REST API item definitions): the activity envelope, the
  policy limits, ``externalReferences``, and the ``typeProperties`` of each activity type.
* "Expression language" (Fabric Data Factory): system variables and the function set.
* "Activity overview": the 120-activity-per-pipeline limit.

Anything the converter emits that the documentation does NOT list is declared in ``INFERRED``
and surfaced in every conversion record as a schema note, so a reader knows exactly which
parts were validated against documentation and which were not. The converter must never
emit a Fabric activity type or property that is not in this module.

A table is not a Fabric service: passing validation here means "consistent with the documented
schema", not "the service accepted it". Runtime executability is tracked separately.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

SOURCES = (
    "https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/datapipeline-definition",
    "https://learn.microsoft.com/en-us/fabric/data-factory/expression-language",
    "https://learn.microsoft.com/en-us/fabric/data-factory/activity-overview",
    "https://learn.microsoft.com/en-us/fabric/data-factory/compare-fabric-data-factory-and-azure-data-factory",
    "https://learn.microsoft.com/en-us/fabric/data-factory/upgrade-pipelines-powershell-module-for-azure-data-factory-to-fabric",
)

ACTIVITY_LIMIT = 120  # per pipeline, nested activities included (Activity overview)
RETRY_INTERVAL_MIN, RETRY_INTERVAL_MAX = 30, 86400  # seconds (Activity Policy)
CONDITIONS = ("Succeeded", "Failed", "Skipped", "Completed")
ACTIVITY_STATES = ("Active", "InActive")
ON_INACTIVE_MARK_AS = ("Succeeded", "Failed", "Skipped")
POLICY_KEYS = frozenset({"timeout", "retry", "retryIntervalInSeconds", "secureInput", "secureOutput"})
SECURE_POLICY_KEYS = frozenset({"secureInput", "secureOutput"})  # SetVariable accepts only these

# Activity-level keys of the envelope (DataPipelineActivity) plus those documented per type.
ENVELOPE_KEYS = frozenset({
    "name", "type", "state", "onInactiveMarkAs", "dependsOn", "typeProperties", "policy",
    "externalReferences", "userProperties", "description",
})

# Pipeline-level properties. ``activities`` and ``description`` are documented; the rest are
# standard in exported pipelines but not in the definition page, so they are INFERRED.
PIPELINE_PROPERTY_KEYS = frozenset({"activities", "description", "parameters", "variables", "annotations"})
INFERRED_PIPELINE_PROPERTIES = frozenset({"parameters", "variables", "annotations"})


@dataclass(frozen=True)
class FabricActivitySchema:
    type: str
    required: tuple[str, ...] = ()  # required typeProperties keys
    allowed: frozenset = field(default_factory=frozenset)  # documented typeProperties keys
    envelope_required: tuple[str, ...] = ()  # e.g. externalReferences
    envelope_extra: frozenset = field(default_factory=frozenset)  # documented extra envelope keys
    containers: tuple[str, ...] = ()  # typeProperties keys holding nested activities
    secure_policy_only: bool = False
    inferred_envelope: frozenset = field(default_factory=frozenset)  # emitted but not documented
    inferred_type_properties: frozenset = field(default_factory=frozenset)


def _s(type_: str, required=(), allowed=(), envelope_required=(), envelope_extra=(), containers=(),
       secure_policy_only=False, inferred_envelope=(), inferred_tp=()):
    return FabricActivitySchema(
        type_, tuple(required), frozenset(allowed) | frozenset(required) | frozenset(inferred_tp),
        tuple(envelope_required), frozenset(envelope_extra), tuple(containers), secure_policy_only,
        frozenset(inferred_envelope), frozenset(inferred_tp))


_COPY_TP = (
    "source", "sink", "destination", "translator", "enableStaging", "stagingSettings", "scriptLines",
    "caller", "linkedIntegrationRuntime", "parallelCopies", "dataIntegrationUnits",
    "throughputOptimizationUnits", "enableSkipIncompatibleRow", "redirectIncompatibleRowSettings",
    "logStorageSettings", "logSettings", "preserveRules", "preserve", "resumeId",
    "validateDataConsistency", "skipErrorFile",
    "adaptivePerformanceTuning",  # documented in the Copy activity article's settings table
)

FABRIC_ACTIVITIES: dict[str, FabricActivitySchema] = {a.type: a for a in [
    _s("Wait", ["waitTimeInSeconds"]),
    _s("Copy", ["source"], _COPY_TP),
    _s("Lookup", ["source", "datasetSettings"], ["firstRowOnly"]),
    _s("GetMetadata", ["datasetSettings"], ["fieldList", "storeSettings", "formatSettings"]),
    _s("Delete", ["datasetSettings"], ["recursive", "maxConcurrentConnections", "enableLogging",
                                       "logStorageSettings", "storeSettings"]),
    _s("WebActivity", ["relativeUrl", "method"],
       ["headers", "body", "disableCertValidation", "httpRequestTimeout", "turnOffAsync"],
       envelope_required=("externalReferences",)),
    _s("WebHook", ["method", "relativeUrl"],
       ["timeout", "headers", "body", "reportStatusOnCallBack", "disableCertValidation"],
       envelope_extra=("externalReferences",)),
    _s("AzureFunction", ["method", "functionName"],
       ["headers", "body", "functionSetId", "workspaceId", "operationType", "parameters"],
       envelope_required=("externalReferences",)),
    _s("IfCondition", ["expression"], ["ifTrueActivities", "ifFalseActivities"],
       containers=("ifTrueActivities", "ifFalseActivities")),
    _s("ForEach", ["items", "activities"], ["isSequential", "batchCount"], containers=("activities",)),
    _s("Until", ["expression", "activities"], ["timeout"], containers=("activities",)),
    _s("Switch", ["on"], ["cases", "defaultActivities"], containers=("defaultActivities",)),
    _s("Filter", ["items", "condition"]),
    _s("Fail", ["message", "errorCode"]),
    _s("SetVariable", ["variableName", "value"], ["setSystemVariable"], secure_policy_only=True),
    _s("AppendVariable", ["variableName", "value"]),
    _s("InvokePipeline", [], ["parameters", "waitOnCompletion", "workspaceId", "pipelineId", "operationType"],
       envelope_required=("externalReferences",)),
    _s("SqlServerStoredProcedure", ["storedProcedureName"], ["database", "storedProcedureParameters"],
       envelope_extra=("externalReferences", "linkedService", "connectionSettings")),
    # Script's documented table lists no envelope externalReferences; the connection must be
    # carried somewhere, so the envelope reference is INFERRED (declared and reported).
    _s("Script", ["database", "scripts"], ["logSettings", "scriptBlockExecutionTimeout", "connectionVersion"],
       inferred_envelope=("externalReferences",)),
    _s("TridentNotebook", ["notebookId", "workspaceId"], ["parameters", "sessionTag"],
       envelope_extra=("externalReferences",)),
    _s("SparkJobDefinition", ["sparkJobDefinitionId", "workspaceId"],
       ["executableFile", "mainClass", "additionalLibraryUris", "commandLineArguments", "defaultLakehouse",
        "additionalLakehouses", "environmentId"]),
    _s("RefreshDataFlow", ["dataflowId", "workspaceId"], ["notifyOption", "dataflowType", "parameters"],
       envelope_extra=("externalReferences",)),
    # The documented newClusterSettings has no documented fields. A pipeline saved in the Fabric portal (2026-10-27)
    # stores the job cluster flat under the ADF linked-service names; the converter emits those, INFERRED.
    _s("DatabricksNotebook", ["notebookPath"],
       ["baseParameters", "libraries", "existingClusterId", "newClusterSettings", "pythonWheelTask",
        "sparkJarTask", "sparkPythonTask"], envelope_required=("externalReferences",),
       inferred_tp=("newClusterVersion", "newClusterNodeType", "newClusterNumOfWorker", "clusterOption",
                    "newClusterInitScripts", "newClusterDriverNodeType", "newClusterSparkConf", "policyId")),
    _s("AzureHDInsight", ["hdiActivityType"],
       ["arguments", "getDebugInfo", "scriptSettings", "defines", "variables", "queryTimeout"],
       envelope_extra=("externalReferences",)),
    _s("Custom", ["command"], ["externalReferences", "folderPath", "extendedProperties",
                                "retentionTimeInDays", "autoUserSpecification"],
       envelope_required=("externalReferences",), envelope_extra=("staging",)),
    _s("AzureMLExecutePipeline", [],
       ["mlExecutionType", "mlPipelineId", "mlPipelineEndpointId", "version", "experimentName",
        "mlPipelineParameters", "dataPathAssignments", "mlBatchEndpointName", "mlBatchDeploymentName",
        "jobSettings", "jobInputs", "jobOutputs", "mlParentRunId", "continueOnStepFailure"],
       envelope_required=("externalReferences",)),
    _s("KustoQueryLanguage", ["command"], ["commandTimeout", "database"],
       envelope_extra=("externalReferences", "linkedService", "connectionSettings")),
]}

# --- expression language (Fabric) -------------------------------------------------------------

FABRIC_FUNCTIONS = frozenset(f.lower() for f in (
    # date
    "addDays addHours addMinutes addSeconds addToTime convertFromUtc convertTimeZone convertToUtc "
    "dayOfMonth dayOfWeek dayOfYear formatDateTime getFutureTime getPastTime startOfDay startOfHour "
    "startOfMonth subtractFromTime ticks utcNow "
    # string
    "concat endsWith guid indexOf lastIndexOf replace split startsWith substring toLower toUpper trim "
    # collection
    "contains empty first intersection join last length skip take union "
    # logical
    "and equals greater greaterOrEquals if less lessOrEquals not or "
    # conversion
    "array base64 base64ToBinary base64ToString binary bool coalesce createArray dataUri dataUriToBinary "
    "dataUriToString decodeBase64 decodeDataUri decodeUriComponent encodeUriComponent float int json "
    "string uriComponent uriComponentToBinary uriComponentToString xml xpath "
    # math
    "add div max min mod mul rand range sub "
    # accessors
    "activity item pipeline variables"
).split())

# Documented Fabric system variables (Expression language, "Pipeline scope").
FABRIC_PIPELINE_SYSTEM = frozenset(v.lower() for v in (
    "DataFactory Pipeline PipelineName RunId TriggerId TriggerName TriggerTime GroupId "
    "TriggeredByPipelineName TriggeredByPipelineRunId").split())

# References with no documented Fabric equivalent. Never silently substituted.
UNSUPPORTED_ACCESSORS = {
    "dataset": "datasets do not exist in Fabric (settings are inline); a dataset() reference only makes "
               "sense inside a dataset definition, which the converter inlines",
    "linkedservice": "linkedService() parameters have no Fabric equivalent (connections are not parameterized "
                     "inside the pipeline)",
    "trigger": "trigger-scope values are not part of the documented Fabric expression language",
    "triggerbody": "triggerBody() is not part of the documented Fabric expression language",
    "triggeroutputs": "triggerOutputs() is not part of the documented Fabric expression language",
}

# Fabric inferred (not documented) parts the converter may emit — reported in every record.
INFERRED: tuple[str, ...] = (
    "pipeline properties 'parameters', 'variables' and 'annotations' (standard in exported pipelines, absent from "
    "the DataPipeline definition page)",
    "Script activity envelope 'externalReferences' (the documented Script table lists none)",
    "typeProperties of datasetSettings (shown in Microsoft's own example, absent from the DatasetSettings table)",
    "workspace item ids (notebook / dataflow / pipeline / Spark job) are placeholders until resolved",
    "envelope userProperties used to mark inferred components (shown in Microsoft's example, absent from the table)",
    "Azure Databricks activity job-cluster settings (newClusterVersion, newClusterNodeType, newClusterNumOfWorker, "
    "clusterOption, newClusterInitScripts read back from a pipeline saved in the Fabric portal; newClusterDriverNodeType, "
    "newClusterSparkConf and policyId follow the same naming, unconfirmed)",
)


def schema_for(fabric_type: str) -> Optional[FabricActivitySchema]:
    return FABRIC_ACTIVITIES.get(fabric_type)
