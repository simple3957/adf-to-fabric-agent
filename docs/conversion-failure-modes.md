# Conversion failure modes

When an ADF-to-Fabric conversion does not come out clean, and what the agent does about it. Written from the converter's
code and Microsoft's pages as read on 2026-10-13 (sources: the
[Fabric vs ADF comparison](https://learn.microsoft.com/en-us/fabric/data-factory/compare-fabric-data-factory-and-azure-data-factory),
the [ADF connector timelines](https://learn.microsoft.com/en-us/azure/data-factory/connector-release-stages-and-timelines)
and the [Mapping Data Flow transforms page](https://learn.microsoft.com/en-us/fabric/data-factory/dataflow-gen2-mapping-data-flows-transforms)).
Operating instructions: [`skills/conversion/`](../skills/conversion/SKILL.md).

A conversion can fail in six ways. Each is reported, never hidden: no activity is dropped, and *converted* never means
*runnable*.

## 1. Retired, deprecated or unavailable components

Covered by the **compatibility catalog** (`agent/conversion/compatibility.py`), which cites Microsoft's page and date.

| Component | Microsoft's status | Agent behavior |
|---|---|---|
| SSIS package activity / Azure-SSIS runtime | not in Fabric yet | blocker, fail-fast placeholder, alternatives |
| U-SQL / Data Lake Analytics | retired | blocker, alternatives |
| ML Batch Execution, ML Update Resource | deprecated, not in Fabric | blocker, alternatives |
| Wrangling Data Flow (Power Query in ADF) | deprecated | scaffold bound to a Dataflow Gen2 item; dated finding |
| Mapping Data Flow | Dataflow Gen2 MDF transforms, **preview**; no flowlets, data flow library, UDFs or managed VNet | scaffold + alternatives; finding lists the limits |
| Synapse notebook / Spark job definition activities | not available as ADF activities in Fabric | scaffold for a Fabric notebook / Spark job definition |
| ~15 connectors removed outright (e.g. Concur, Couchbase, Magento, Marketo, Zoho) | removed 2024-12-31 | blocker when an activity uses the linked service |
| 6 V1 connector types removed 2025-09-30 (Salesforce, Snowflake, PostgreSql, ServiceNow, GoogleBigQuery, SalesforceServiceCloud) | removed | manual: upgrade the linked service or recreate the connection |
| Version 1.0 of 20 versioned connectors (Oracle, MySql, Teradata, Hive, Xero, ...) | removed 2025-09 to 2026-04 | manual; an unstated version is flagged "may be 1.0", not assumed |
| ADLS Gen1, Data Lake Analytics linked services | retired | blocker (`connector_unsupported`) |

## 2. Activity types the converter has no rule for

Any type outside the registry (a newer ADF activity, `SqlPoolStoredProcedure`, anything misspelled) becomes a blocker with a
fail-fast placeholder (`no_conversion_rule`). Fix: add a handler plus a test, or rebuild the step by hand.

## 3. Expressions and parameters

Unsupported functions, accessors, system variables and dataset references stay **verbatim** and the activity becomes
`manual_required` (`expression_not_converted`). Global parameters become `gp_*` pipeline parameters. Fabric has no
`true()` function and `pipeline().Pipeline` is an id there; these are handled in the expression converter.

## 4. Structure and limits

120 activities per pipeline including nested ones (`activity_limit_exceeded`): the converter **does not split**; split the
source into child pipelines. Dependency cycles, duplicate names, undeclared variables/parameters and invalid policy values
are errors. A source-versus-generated graph check (`graph_*` codes) fails the pipeline if any dependency or condition changed.

## 5. Infrastructure the pipeline JSON cannot carry

Self-hosted runtimes need an on-premises data gateway; runtimes in a managed virtual network need a virtual network data
gateway (Microsoft lists managed VNets and private endpoints as "to be determined" in Fabric). Plain Azure runtimes need
nothing. Schedule and tumbling-window triggers are turned into Fabric schedule definitions where they map exactly (`deploy --schedules`); event triggers and recurrences Fabric cannot express are reported as manual. Connections, workspace items and credentials
must be bound before anything runs. Key Vault-held secrets are re-created in the Fabric connection.

## 6. Failures only a run can reveal

Static conversion cannot see these. They surfaced in a real run and are found by deploying and reading the activity-level
results:

- **Reserved word as a SQL alias.** `SELECT COUNT_BIG(*) AS RowCount` fails in Synapse (`Incorrect syntax near 'AS'`)
  because `ROWCOUNT` is reserved. The query is carried over unchanged, so the bug travels with it. Fix in the source query.
- Permissions, firewalls and credentials on the data source.
- Missing source files or tables.

## Known gaps (not yet detected statically)

- The **contents** of a data flow (flowlets, UDFs, library use, unsupported sources and sinks) are separate ADF artifacts the
  parser does not read, so the Mapping Data Flow finding lists the limits without checking the flow itself.
- SQL queries are not linted for reserved-word aliases or dialect problems.
- Parameterized linked services have no single address to turn into a Fabric connection; this is reported only through
  unsupported `linkedService()` expressions.
- `SqlPoolStoredProcedure` and other Synapse-only activity types fall under "no rule".
- **Azure Synapse Link** (replaced by Fabric Mirroring) is configured outside any ADF export, so it cannot be detected. ADF **change data capture** resources are detected and reported, but no Fabric Copy job is generated.
