# Azure Databricks items (external compute for PL_Spark_Test)

The source of the notebooks that run on Azure Databricks workspace `dbw-adf-migration`, kept in Git. Architecture, setup
and costs: [docs/compute-and-databricks.md](../docs/compute-and-databricks.md).

| Workspace path | File here | What it does |
|---|---|---|
| `/Shared/adf-migration/NB_Read_Raw_Data` | [notebooks/NB_Read_Raw_Data.py](notebooks/NB_Read_Raw_Data.py) | Creates the sample data (4 customers, 5 sales) as CSV and Parquet |
| `/Shared/adf-migration/NB_Transform_Cab_Data` | [notebooks/NB_Transform_Cab_Data.py](notebooks/NB_Transform_Cab_Data.py) | Joins sales with customers and totals `Amount` by `City` and `Product` |
| `/Shared/adf-migration/tools/billing_check` | [tools/billing_check.py](tools/billing_check.py) | Reads measured DBU usage per job from `system.billing.usage`; used to confirm DBUs per node-hour |

The notebooks are the Synapse originals, with the same logic as the Fabric copies in `fabric/notebooks/`. Only the storage
path differs: they read and write `abfss://migrationcontainer@datamigrationadftofabric.dfs.core.windows.net/migration-test/spark-test`
through the Unity Catalog external location `migration_test_spark`.

To restore a notebook, import the file into the workspace path above as a Python notebook (Workspace API `import`,
format `SOURCE`).
