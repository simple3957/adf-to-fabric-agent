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

A textual scan lists Databricks-specific constructs with their line numbers: `dbutils.secrets`, `dbutils.widgets`, `dbutils.notebook`, `dbutils.fs`, other `dbutils`, DBFS and `/FileStore` paths, `/mnt` mount points, `spark.databricks.*` settings, `%run`, Unity Catalog names. Each is marked **rewrite** or **review**. It says what it saw and why it matters; it does not rewrite code, and code with no findings is not guaranteed to run on Fabric.

## Not verified live

Tested against a fake workspace only. The export API shapes follow docs.databricks.com (checked 2026-10-16).
