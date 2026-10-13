# Compatibility playbook

What each compatibility finding means and what a person does about it. Every id below is an entry in
`src/adf2fabric/agent/conversion/compatibility.py` (a test keeps the two in sync). Dates and sources are
Microsoft's, verified on **2026-10-13**; re-verify before relying on a date.

Sources:
[Fabric vs ADF comparison](https://learn.microsoft.com/en-us/fabric/data-factory/compare-fabric-data-factory-and-azure-data-factory) |
[ADF connector release stages and timelines](https://learn.microsoft.com/en-us/azure/data-factory/connector-release-stages-and-timelines) |
[Mapping Data Flow transforms in Dataflow Gen2](https://learn.microsoft.com/en-us/fabric/data-factory/dataflow-gen2-mapping-data-flows-transforms)

How to read a finding: **status** (removed / deprecated / preview / not_available), **impact** (blocker = cannot convert;
manual = a person must act; difference = converted with a documented difference) and whether it is **emitted** (also an
issue on the activity) or listed only (the handler already blocks or redesigns it).

## Activities

### activity.ssis
`ExecuteSSISPackage`. **not_available**, blocker (listed only). Fabric has no SSIS execution and no Azure-SSIS runtime yet.
Do: keep the package on the Azure-SSIS runtime in ADF and call that ADF pipeline from Fabric, or re-implement the logic with
Copy, Dataflow Gen2 or a notebook. Do not expect a conversion.

### activity.usql
`DataLakeAnalyticsU-SQL`. **removed** (Azure Data Lake Analytics is retired), blocker (listed only).
Do: re-implement the U-SQL job as a Fabric notebook (Spark) or Dataflow Gen2. The data in ADLS Gen1 must move to Gen2 first.

### activity.aml_classic
`AzureMLBatchExecution`, `AzureMLUpdateResource`. **deprecated**, blocker (listed only). Not available in Fabric.
Do: publish the model as an Azure Machine Learning pipeline or batch endpoint and call it with the Fabric Azure Machine
Learning activity.

### activity.wrangling_dataflow
`ExecuteWranglingDataflow` (Power Query in ADF). **deprecated**, difference (emitted).
Do: re-author the Power Query in a Dataflow Gen2 item, then bind the Dataflow item id the generated activity asks for.

### activity.mapping_dataflow
`ExecuteDataFlow` (Mapping Data Flow). **preview**, difference (emitted). Runs in Fabric as Mapping Data Flow transforms in
Dataflow Gen2. Not supported there: flowlets, the Data Flow library, user-defined functions, managed virtual networks;
execution only through a pipeline Dataflow activity; not full parity. The data flow definition is a separate ADF artifact,
not part of the pipeline JSON, so the converter cannot check it.
Do: check the data flow for flowlets, UDFs and library use first; then use the built-in upgrade (Dataflow Gen2 with MDF
transforms) or re-implement as a notebook or Power Query. Bind the resulting item id.

### activity.synapse_spark
`SynapseNotebook`, `SparkJob`. **not_available** as ADF activities in Fabric (listed only; the converter emits a Fabric
notebook / Spark job definition scaffold). Do: move the notebook or Spark job definition into the Fabric workspace and bind it.

## Factory artifacts that are not pipeline activities

### artifact.adf_cdc
An ADF **change data capture** resource (`Microsoft.DataFactory/factories/adfcdcs`; the Azure intake retrieves these into
`source/changeDataCapture/`). **replaced**, manual (listed only). Fabric has no CDC artifact: Microsoft's comparison says
Fabric manages incremental data movement through **Copy jobs** instead of CDC artifacts. The finding names the source and
target linked services, the tables and the mapping count so a person can recreate it.
Do: create a Fabric Copy job with the same source and destination connections and tables, in the incremental mode that
fits, and recreate the column mappings; keep the CDC resource running in ADF until the Copy job is verified. The converter
does **not** generate a Copy job: its item definition is not a documented schema the project can validate.

### Azure Synapse Link (not detectable)
Not a catalog entry, because it is configured in Synapse and the source database, not in an ADF or pipeline export, so no
conversion input contains it. Microsoft's guidance
([Synapse Link overview](https://learn.microsoft.com/en-us/azure/synapse-analytics/synapse-link/sql-synapse-link-overview))
is "instead of Azure Synapse Link, use Fabric Mirroring". If the estate relies on Synapse Link, plan Mirroring for those
databases separately; the converter cannot see it.

## Connectors (linked services)

Microsoft's connector timeline drives these. Three patterns:

### connector.<type>.removed
Connectors Microsoft lists as **Removed** with no replacement version. **Blocker** (emitted when an activity uses it).
Removal date 2024-12-31 unless noted: `AzureMariaDB`, `Concur`, `Couchbase`, `Drill`, `HBase`, `Magento`, `Marketo`,
`Eloqua` (Oracle Eloqua), `Responsys` (Oracle Responsys), `OracleServiceCloud`, `PayPal`, `Phoenix`,
`SalesforceMarketingCloud`, `Zoho`; `AmazonMWS` (no date given).
Pipelines using these cannot run in ADF either. Do: move the data with a different supported connector (REST, ODBC,
file export) or keep the step outside Fabric; check Fabric's connector overview first.

### connector.<type>.v1 (separate V1 type name)
The V1 type was **Removed 2025-09-30**; the current type has a V2 name. **Manual** (emitted). Types: `GoogleBigQuery`
(use `GoogleBigQueryV2`), `PostgreSql` (`PostgreSqlV2`), `Salesforce` (`SalesforceV2`), `SalesforceServiceCloud`
(`SalesforceServiceCloudV2`), `ServiceNow` (`ServiceNowV2`), `Snowflake` (`SnowflakeV2`).
Do: upgrade the ADF linked service to the V2 type, or create the Fabric connection from the current connector's settings.
Never copy V1 settings across; the property names differ.

### connector.<type>.v1 (version 1.0 of a versioned connector)
`typeProperties.version` is `1.0` **or not stated**; version 2.0 is current. **Manual** (emitted). The converter does not
assume an unstated version is 1.0: the finding says "may be the removed 1.0 - set or confirm typeProperties.version".
Removal dates: `Cassandra`, `Greenplum`, `MariaDB`, `MySql`, `Presto`, `QuickBooks`, `Vertica` 2025-09-30; `Hive`, `Impala`,
`Netezza`, `Spark`, `Teradata` 2025-10-31; `Square` 2025-11-15; `HubSpot`, `Shopify` 2025-11-22; `AmazonRdsForOracle`,
`Oracle` 2026-03-31; `AmazonRedshift`, `Jira`, `Xero` 2026-04-30.
Do: upgrade the linked service to 2.0 (set `typeProperties.version`) or create the Fabric connection from the current
connector's settings.

Not flagged (no end-of-support date given): Azure SQL Database, Azure SQL Managed Instance, Azure Synapse Analytics, SQL
Server and Azure Database for PostgreSQL version 1.0 ("to be determined").

## Failure classes that are NOT in the catalog

These are reported by other parts of the converter; see [`../../docs/conversion-failure-modes.md`](../../docs/conversion-failure-modes.md):
unsupported expression functions, the 120-activity limit, parameterized linked services, self-hosted / managed-VNet
runtimes (gateway prerequisites), triggers (schedules are generated where they map exactly; event triggers are not), incomplete sources (`reconstruct`), and runtime-only failures
such as a reserved word used as a SQL alias, which only a pipeline run can reveal.
