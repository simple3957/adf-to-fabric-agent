# Master compatibility catalog and the project's activity mapping

The migrator keeps everything it knows about converting between platforms in **one file**,
[`src/adf2fabric/catalog/compatibility.json`](../src/adf2fabric/catalog/compatibility.json), built on 2026-11-17 from Microsoft's
documentation and the tool's live checks. It is the intermediate layer: a *capability* (`copy`, `lookup`, `execute_pipeline`,
`databricks_job` ...) is platform-neutral, each platform names its own types for it, and `directions` lists the conversions the code
implements (`adf->fabric` today; the platform slot is ready for others).

## What is in it

| Section | Holds | Read by |
|---|---|---|
| `sources` | Every Microsoft page used: URL, title, the page's last-updated date, the date it was read (`live` = checked in the user's tenant) | every section cites these ids |
| `capabilities` | Per capability: ADF types, Fabric activities (with their doc page), strategy, what is carried / not carried, manual steps, validation; per ADF type: Microsoft's comparison wording, every conversion **option** (condition, Fabric activities, status, code, switch), workarounds, lifecycle entries | `registry.rule_from_catalog` (rule descriptions), `mapping_catalog`, `compatibility` |
| `switches` | The choices a person may make per activity: `storage` (connection / lakehouse), `spark` (databricks / fabric), `dataflow` (notebook / dataflow_gen2) | `project_mapping`, the handlers |
| `rules_for_every_activity` | What applies to all activities (unregistered type, nesting, missing dataset / linked service, connectors, expressions, global parameters, reconstruction, model drafts, graph check) | `mapping_catalog.GENERAL` |
| `artifacts`, `concepts` | Factory-level items (CDC) and concepts: datasets, linked services, integration runtimes, managed VNet, triggers (schedule, tumbling window, storage event, custom event), global parameters, data flows, Synapse Link; Microsoft's words and what the tool does | `compatibility`, documentation |
| `connectors` | Per ADF linked-service type: verdict, Fabric connector, Stored procedure / Script support; Fabric's connector table (Dataflow Gen2, pipeline Copy, Copy job source / destination); connector lifecycle (removed, V1); which connections the tool creates | `connectors`, `compatibility` |
| `fabric_schema` | Every Fabric activity's documented `typeProperties`, envelope, limits, expression functions and system variables, what is inferred, and Fabric-only activities | `fabric_schema` |

The converter's handlers still build the Fabric JSON in code; every fact they rely on is read from this file. When Microsoft's
documentation changes, edit the JSON and the source's `read_on` date, not the code. `tests/test_master_catalog.py` checks that every
activity type discovery reads has exactly one capability, every option has a valid status and names only documented Fabric
activities, every cited source exists with a date, and that the modules read the file.

A workaround's `source` says where it comes from: `compatibility` (Microsoft's comparison pages), `handler` (what the converter
reports), `manual` (a person does it) or `tool` (the tool does it by itself; since 2026-11-17 the entries that the code already
automates say so, instead of describing a manual step: Databricks JAR / Python / job / notebook, HDInsight Spark, Synapse Spark job
and notebook, wrangling data flow, data flow steps, U-SQL scripts, SSIS packages, the 120-activity split).

## The project's activity mapping

`adf2fabric mapping-export --project P` writes `projects/P/conversion/activity-mapping.json`, and **Convert writes it again before every
conversion** (new activities added, choices kept). One row per real activity:

```json
{"id": "PL_Copy_Test/Copy data1", "pipeline": "PL_Copy_Test", "name": "Copy data1", "adf_type": "Copy", "capability": "copy",
 "microsoft_comparison": "Copy activity",
 "options": [{"option": 0, "fabric": ["Copy"], "status": "converted", "when": "...", "switch": "connection"},
             {"option": 1, "fabric": ["Copy"], "status": "converted", "when": "...", "switch": "lakehouse"}, ...],
 "workarounds": ["Use a Fabric Copy job ..."],
 "switch": "storage", "allowed": ["connection", "lakehouse"], "chosen": null}
```

`chosen` null follows the run's normal decision (the brief, the Convert options, what is approved and bound). A person sets it where a
switch is offered; Convert follows it:

| Switch | Value | What Convert does |
|---|---|---|
| `storage` | `connection` | The activity keeps its storage connection, even when the run repoints to a lakehouse |
| `storage` | `lakehouse` | The activity reads / writes the Lakehouse through the OneLake shortcut, even when the run keeps connections (needs the storage map and a named lakehouse, else manual `storage_choice_unavailable`) |
| `spark` | `databricks` | Databricks Notebook: the Fabric Azure Databricks activity, even when a migrated notebook exists. Databricks Job: blocked with the portal Job task as the workaround (Fabric documents only its portal settings) |
| `spark` | `fabric` | The migrated Fabric notebook(s); blocked as pending until approved and in the workspace, never falling back to Databricks |
| `dataflow` | `notebook` | The PySpark notebook translated from the data flow script |
| `dataflow` | `dataflow_gen2` | A Dataflow Gen2 refresh with a placeholder, to upgrade with MDF transforms (preview) or re-author |

A value the catalog does not allow fails the conversion with the activity named; it is never dropped. After converting, every
activity's Fabric output is checked against the options the catalog lists for its type; a mismatch is a schema note on the conversion.
