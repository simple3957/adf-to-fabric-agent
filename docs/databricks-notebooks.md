# Databricks notebooks

ADF's Databricks notebook activity holds only a `notebookPath`; the code is in the Databricks workspace. `fetch-databricks-notebooks` exports it so it can be read and checked for Fabric. Code: `src/adf2fabric/agent/discovery/databricks_notebooks.py`.

```
adf2fabric fetch-databricks-notebooks --project <name> --linked-service <ADF linked service> --token-env DBX_TOKEN
```

- **Without `--token-env`** it only reads the linked service from the project source and lists the notebook paths the pipelines use. It prints the workspace URL (`typeProperties.domain`), how ADF authenticates (access token, managed identity or Key Vault) and the cluster type. It never prints a secret, and ADF does not return the access token anyway.
- **With `--token-env`** it names an environment variable holding a Databricks personal access token, or an Entra token for the Azure Databricks resource. The token is sent only to the workspace host and is never written to a file or message.
- **`--path`** fetches specific notebooks instead of the ones the pipelines use. A path that is an expression (`@{...}`) cannot be resolved without running the pipeline, so it is listed as skipped.

Notebooks are written under `projects/<name>/databricks/notebooks/`, in the same folder layout as the workspace (`/Shared/etl/clean` becomes `Shared/etl/clean.py`). `projects/<name>/databricks/retrieval.json` records each notebook's hash, which activities use it, any error, and the findings below.

It is read-only: nothing in Databricks is created or changed.

## Errors

Each failure has a stable code and what to do: `not_found`, `unauthorized`, `forbidden`, `too_large` (10 MB export limit), `not_a_notebook`, `rate_limited`, `unreachable`, `bad_workspace_url`, `no_token`, `unsafe_path`, `api_error`. One failing notebook does not stop the others; the command exits 2 if any failed.

## Fabric compatibility scan

A textual scan lists Databricks-specific constructs with their line numbers: `dbutils.secrets`, `dbutils.widgets`, `dbutils.notebook`, `dbutils.fs`, other `dbutils`, DBFS and `/FileStore` paths, `/mnt` mount points, `spark.databricks.*` settings, direct ADLS (`abfss://`) paths, `%run`, Unity Catalog names. Each is marked **rewrite** or **review**. It says what it saw and why it matters; it does not rewrite code, and code with no findings is not guaranteed to run on Fabric.

## Verified live

On 2026-10-16 both notebooks `PL_Spark_Test` runs (`/Shared/adf-migration/NB_Read_Raw_Data` and `NB_Transform_Cab_Data`) were exported from a real workspace with an Entra token obtained by `az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d` (the Azure Databricks resource), and the scan found nothing to change. The scan first missed that the notebooks reach ADLS through a Databricks Unity Catalog external location: the same code, run as Fabric notebooks, failed with HTTP 401 reading the storage account, because Fabric uses the running identity's access. The scan now flags every direct `abfss://` path (`adls_direct_path`). Fix: give the identity that runs the notebook access to the account, or read the data through a OneLake shortcut.

The converter binds a Databricks notebook activity to a Fabric notebook only after `deploy-notebook --resolutions` has recorded it (`notebook:<last part of notebookPath>`), and only when the activity has no `baseParameters` or `libraries`.

## End to end, verified live (2026-10-16)

`PL_Spark_Test` (two Databricks notebook activities) was run as Fabric notebooks:

1. `fetch-databricks-notebooks` exported both notebooks.
2. A lakehouse with a OneLake shortcut to the storage container was created (`Files/migrationcontainer`, ADLS Gen2, through an existing Fabric connection). This was first done with direct API calls and is now `adf2fabric create-shortcut` (see `docs/deploy.md`).
3. The one `storage = ...` line in each notebook was changed to the lakehouse path (the tool does not rewrite notebook code). The derived files are in `projects/<name>/databricks/fabric-notebooks/`, next to the untouched exports.
4. `deploy-notebook --resolutions` created the notebooks and recorded their ids; `convert --resolutions` produced Fabric notebook activities (status converted, runtime ready); `deploy` and `run` completed with both activities succeeded, and the CSV, Parquet and transformed output were present in storage.

Without step 2 and 3 the run failed with HTTP 401 on the storage account, because Fabric uses the running identity's access, not Databricks' external-location credential.
