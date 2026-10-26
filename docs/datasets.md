# Datasets

Code: `src/adf2fabric/agent/conversion/handlers/_common.py` (`dataset_settings`).

Fabric Data Factory pipelines have no dataset objects. Each ADF dataset is read at conversion time and its type, schema, location and linked service are inlined into the activity as `datasetSettings`. Dataset parameters are substituted, at the expression-tree level, with the arguments the activity passed (or the dataset's declared defaults).

## What became of each dataset (the ledger)

Microsoft's guidance is that Fabric defines dataset properties inline in each activity ([upgrade paths](https://learn.microsoft.com/fabric/data-factory/upgrade-paths)):
there is no dataset item to recreate in the workspace. So the conversion record carries a `dataset_ledger` and the report a **Datasets** section
that accounts for every source dataset:

| Treatment | Meaning |
|---|---|
| `inlined` | Copied into each listed activity (`Pipeline/Activity`) |
| `repointed_to_lakehouse` | Inlined, with its location moved to the Lakehouse through the OneLake shortcut (`convert --lakehouse`) |
| `not_converted` | Used only by a pipeline left out of this run |
| `used_by_data_flow` | Used only by a data flow, which is not converted |
| `not_used` | No activity uses it: nothing to inline it into, so it is not carried (it stays in the source) |

What can be reproduced in the workspace is the **storage** a file dataset points at, not the dataset: `storage-map` proposes OneLake shortcuts and
Lakehouse paths (`docs/storage-map.md`), `create-shortcut` makes them, and `convert --lakehouse` inlines the Lakehouse location.

## Blocked, never guessed

- A referenced dataset that is not in the source (see [partial-source.md](partial-source.md)).
- An inline endpoint with no dataset, or a dataset with no type.
- A dataset parameter with no argument and no default (`dataset_parameter_unresolved`: manual).
- A dataset with no linked service is manual (`dataset_without_linked_service`), not blocked.

## Datasets that pass arguments to a linked service

An ADF linked service can take parameters (database, server, URL) and each dataset passes its own. A Fabric connection is fixed, so:

- Datasets passing **different literal values** to one linked service get **one connection each**, bound as `LS?Param=value` (for example
  `LS_SQLP?DBName=SalesDB`); the generated resolutions template lists each one. They are never collapsed onto one connection
  (`linked_service_arguments_split`).
- **One value everywhere**: the plain linked-service binding is kept, with a note that the connection is fixed to that value
  (`linked_service_arguments_fixed`).
- A value **computed at run time** (an expression) cannot be bound: manual (`linked_service_argument_dynamic`). Create a connection per
  possible value, or split the pipeline.
- Arguments an activity passes straight to a linked service (Script, Stored procedure, Databricks, ...) are handled the same way, and datasets and
  activities that use one linked service are counted together.

## Known limits

- The schema comes from the dataset's `schema`. For the older `structure` property only the column names are carried: its type names
  (`Int32`, ...) are ADF's own and their Fabric equivalents are not verified.
- **Live-verified dataset types** (the converted form has been run in Fabric and matched ADF): `AzureSqlDWTable`, `DelimitedText` and
  `Parquet` on ADLS Gen2, and the Lakehouse Files repointing. Every other type (Json, Excel, Xml, Avro, Orc, REST, SQL Server, Oracle,
  Cosmos, ...) is converted from its ADF shape and carries an info finding `dataset_type_not_live_verified`; `connector_unverified`
  is the separate finding about the linked service's connector. Run the converted pipeline in a test workspace and `verify` it.
- Dataset folders, descriptions and the argument values themselves are not carried into the pipeline; the values select the connection.

The linked service behind a dataset is resolved to a Fabric connection by the connections layer. See `docs/connections.md`.
