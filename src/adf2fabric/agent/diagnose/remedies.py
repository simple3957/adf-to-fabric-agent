"""What to do about each reason a pipeline or a service is not yet running in Fabric.

Three tables, all plain data so a test can require every entry to say something useful:

* ``CONVERSION``: the finding code the converter gives an activity it could not finish -> what a person does.
* ``BINDING``: an unbound prerequisite (a connection, a workspace item, a gateway) -> how to create and bind it.
* ``RUN_FAILURES``: words in a failed Fabric run's message -> the usual cause and the fix.

Nothing here is a guess dressed as a fact: an entry names the usual cause and the first thing to check, and the diagnosis always shows the raw message too.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Remedy:
    cause: str
    fix: str
    command: str = ""


# finding code (see ConversionIssue.code) -> remedy. Anything not listed falls back to the finding's own `action`.
CONVERSION: dict[str, Remedy] = {
    "linked_service_not_found": Remedy("The linked service is not in the source, so the connection cannot be described.",
        "Complete the source (a `linkedService/` folder, the factory's ARMTemplateForFactory.json export, or live intake with `--artifact-kind linkedservice`), or create the Fabric connection by hand and bind it.",
        "adf2fabric convert --project P --resolutions conversion/resolutions.json   # LinkedServiceToConnectionId: <linked service> -> <connection id>"),
    "dataset_not_found": Remedy("A dataset the pipeline uses is not in the source, so it cannot be inlined (Fabric has no dataset item).",
        "Add the `dataset/` folder to the source (the whole collaboration branch, a ZIP that includes it, or live intake), then re-run discovery and convert.",
        "adf2fabric intake --project P --factory F --resource-group RG   # or re-import the full source"),
    "dataset_parameter_unresolved": Remedy("A dataset parameter has no argument and no default.", "Give the parameter a default on the dataset, or pass it from the activity, then re-run discovery and convert."),
    "synapse_notebook_migrate": Remedy("A Synapse notebook must become a Fabric notebook first.", "Migrate the notebook (workloads), approve it, deploy it, then bind its item id and convert again.",
        "adf2fabric workloads-assess --project P && adf2fabric workloads-generate --project P && adf2fabric workloads-approve --project P && adf2fabric workloads-deploy --project P"),
    "spark_job_migrate": Remedy("A Spark job definition must become a Fabric item first.", "Migrate it with the workload commands, deploy it, bind its item id and convert again.",
        "adf2fabric workloads-assess --project P && adf2fabric workloads-generate --project P"),
    "databricks_cluster": Remedy("A Databricks activity names no cluster the converter can see.", "Either supply the cluster in a compute sources file, or move the notebook to Fabric (workloads) so no cluster is needed.",
        "adf2fabric convert --project P --compute-sources compute-sources.json"),
    "databricks_jar_python": Remedy("Databricks jar and python activities have no Fabric activity.", "Run the code as a Fabric notebook or Spark job definition (workloads), and call that from the pipeline."),
    "databricks_job": Remedy("A Databricks job has no Fabric equivalent.", "Recreate it as a Fabric notebook or Spark job definition, or keep it running in Databricks and call it with a Web activity."),
    "hdinsight_program_settings": Remedy("HDInsight program settings have no Fabric counterpart.", "Rewrite the program as a Fabric notebook or Spark job, or keep running HDInsight and call it from outside Fabric."),
    "ssis_unsupported": Remedy("SSIS packages cannot run in a Fabric pipeline.", "Keep the package in Azure-SSIS or rebuild it as a Fabric pipeline or dataflow; there is no automatic conversion."),
    "usql_retired": Remedy("U-SQL (Azure Data Lake Analytics) is retired.", "Rewrite the logic in Spark SQL or T-SQL (a Fabric notebook or warehouse); there is no automatic conversion."),
    "aml_deprecated": Remedy("Classic Azure ML activities are retired.", "Move to an Azure ML pipeline activity or a Fabric notebook that calls the model."),
    "aml_no_linked_service": Remedy("The Azure ML activity names no linked service.", "Add the linked service to the activity in the ADF source and reconvert."),
    "dataflow_reauthor": Remedy("Mapping and wrangling data flows are not converted.", "Rebuild the logic as a Dataflow Gen2 or a Fabric notebook; the pipeline keeps a scaffold activity where it was called."),
    "kusto_database": Remedy("The Kusto command has no database the converter can read.", "State the database in the linked service or the activity, then reconvert."),
    "web_dynamic_url": Remedy("The Web activity's URL is computed at run time, so no Fabric connection can be bound to it.", "Move the URL to a pipeline parameter with a default, or create a connection per base URL.",
        "adf2fabric assist-candidates --project P   # optional: a reviewed model-assisted draft can move the URL to a parameter"),
    "script_store_unsupported": Remedy("The Script activity's store is not one Fabric supports.", "Run the logic against a supported SQL store, or in a Fabric notebook."),
    "stored_procedure_store_unsupported": Remedy("The stored procedure's store is not one Fabric supports.", "Run it on a supported SQL store, or move the logic to a Fabric notebook or warehouse."),
    "stored_procedure_linked_service_not_in_source": Remedy("The linked service is not in the source, so its store type is unknown.", "Add the linked service to the source (a bound connection alone is not enough: the store type decides what Fabric supports)."),
    "script_linked_service_not_in_source": Remedy("The linked service is not in the source, so its store type is unknown.", "Add the linked service to the source."),
    "unsupported_trigger": Remedy("`trigger()` values do not exist in a Fabric schedule.", "Pass the value as a pipeline parameter with a default, or rewrite the expression.",
        "adf2fabric assist-candidates --project P   # optional: a reviewed model-assisted draft can do this"),
    "unsupported_triggerbody": Remedy("`triggerBody()` has no Fabric equivalent.", "Use a pipeline parameter that whatever starts the run supplies."),
    "unsupported_trigger_type": Remedy("`pipeline().TriggerType` takes different values in Fabric.", "Check what the expression compares it with and adjust it."),
    "unsupported_function": Remedy("The expression uses a function Fabric's expression language does not have.", "Rewrite it with documented functions (the converter's report names the function)."),
    "expression_unparsed": Remedy("An expression could not be read.", "Fix its syntax in the ADF source (quotes are single, a quote inside is doubled), then reconvert."),
    "unsupported_dataset_reference": Remedy("`@dataset()` is used outside a dataset's context.", "Remove it or move the logic into the dataset's parameters."),
    "unsupported_linkedservice": Remedy("`@linkedService()` is used outside a linked service.", "Pass the value as a dataset or activity argument; each distinct value needs its own Fabric connection."),
    "linked_service_argument_dynamic": Remedy("A linked-service argument is computed at run time, but a Fabric connection is fixed.", "Create one connection per value it can take, or split the pipeline by value."),
    "no_conversion_rule": Remedy("No conversion rule exists for this activity type.", "Rebuild the activity by hand in Fabric (the pipeline keeps a Fail placeholder where it was)."),
    "nesting_too_deep": Remedy("Its activities nest deeper than discovery reads.", "Move the inner activities into a child pipeline."),
    "connector_unsupported": Remedy("The data store has no Fabric connector.", "Move the data to a supported store first, or keep this step outside Fabric."),
    "secret_masked": Remedy("A secret was masked in the source and cannot be carried over.", "Re-enter it through a Fabric connection or a secure parameter; never in pipeline JSON."),
}

# prerequisite kind -> how to bind
BINDING: dict[str, Remedy] = {
    "connection": Remedy("The Fabric connection does not exist yet (or is not bound).",
        "Create it in Fabric (Settings > Manage connections and gateways; the tool can create SQL/Synapse, ADLS Gen2, Blob, REST and Databricks ones), enter the credential there, then bind its id.",
        "adf2fabric convert --project P --resolutions conversion/resolutions.json   # LinkedServiceToConnectionId: <linked service> -> <connection id>"),
    "gateway": Remedy("The source needs an on-premises data gateway (a self-hosted integration runtime in ADF).",
        "Install and register an on-premises data gateway on a machine that can reach the source, then create the connection through that gateway."),
    "workspace_item": Remedy("A Fabric item the pipeline calls does not exist yet (a notebook, a child pipeline, a lakehouse).",
        "Deploy it first (child pipelines are created before their callers by `deploy --with-children`), then bind its id with ItemToId.",
        "adf2fabric deploy --project P --workspace-id W --with-children --apply"),
    "credential": Remedy("A credential must be entered again in Fabric.", "Enter it in the Fabric connection; nothing is copied from ADF."),
}

# (pattern in a failed run's message, remedy), first match wins
RUN_FAILURES: list[tuple[re.Pattern, Remedy]] = [(re.compile(p, re.I), r) for p, r in (
    (r"capacity|throttl|CapacityNotActive|too many requests|\b429\b",
     Remedy("The Fabric capacity is paused, overloaded or throttled.", "Resume or scale the capacity (Azure portal or the capacity admin), wait a minute, run again.")),
    (r"\b401\b|\b403\b|unauthori[sz]ed|forbidden|AuthenticationFailed|login failed|invalid credential|permission denied|AuthorizationPermissionMismatch",
     Remedy("The connection's credential or permissions are wrong.", "Open the connection in Fabric, re-enter the credential and press Test; make sure the identity can read the source and write the target (for ADLS: Storage Blob Data Contributor).")),
    (r"connection.{0,40}(not found|does not exist|invalid)|invalid connection|ConnectionNotFound|unknown connection",
     Remedy("The pipeline points at a connection id that does not exist or is not shared with whoever runs it.", "Check the id in the resolutions file, and share the connection with the user or workspace identity (Manage connections > Manage users).")),
    (r"gateway",
     Remedy("The on-premises data gateway is offline or not shared with this workspace.", "Start the gateway service, check it is Online in Manage connections and gateways, and share it.")),
    (r"paused|ServiceUnavailable|is not accessible|server was not found|Cannot open server|connection refused|network.related|name or service not known|no such host|firewall",
     Remedy("The source is unreachable (a paused pool, a firewall, a private network).", "Resume the source, allow Fabric through its firewall (or use a gateway / managed private endpoint), then run again.")),
    (r"incorrect syntax near|invalid column|invalid object name|reserved|syntax error|ambiguous column",
     Remedy("The SQL does not run on the target as written.", "Fix the query in the ADF source (a reserved word used as an alias needs [brackets]; the table or column must exist on the target) and reconvert.")),
    (r"expression|InvalidTemplate|InvalidParameter|function .{0,40}(not|invalid)|cannot be evaluated|cannot be resolved|null reference",
     Remedy("An expression fails in Fabric.", "Open the failed activity in Monitor, find the expression, and rewrite it with functions Fabric documents (the conversion report lists rewrites).")),
    (r"notebook|livy|spark|session",
     Remedy("The notebook failed.", "Open the notebook run in Monitor. Common causes: reading ADLS through an external location (401) - route it through a OneLake shortcut or grant the running identity; a missing library; a cluster-only API.")),
    (r"not ?found|does not exist|path.{0,20}not|BlobNotFound|PathNotFound|\b404\b|no such file",
     Remedy("A file, folder or table the pipeline reads or writes is missing.", "Check the shortcut path and that the data exists at the source; run `storage-map`, create the shortcut, and test it.")),
    (r"cluster|databricks",
     Remedy("The Databricks activity has no usable cluster.", "Supply the cluster (compute sources), or move the notebook to Fabric so no cluster is needed.")),
    (r"timeout|timed out|did not complete",
     Remedy("The run exceeded its time limit.", "Check what was slow in Monitor, raise the activity's timeout, or reduce the data in the run.")),
)]


def for_run_failure(message: str) -> Optional[Remedy]:
    for pattern, remedy in RUN_FAILURES:
        if pattern.search(message or ""):
            return remedy
    return None
