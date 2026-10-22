# Deploy

Code: `src/adf2fabric/agent/deployment/fabric_deploy.py` (pipelines, schedules) and `compute_deploy.py` (pools, environments). CLI: `adf2fabric deploy`, `adf2fabric compute-deploy`.

## Behaviour

- **Dry run by default.** `--apply` is required to write to the workspace.
- **Ordering.** Child pipelines deploy first so each `ExecutePipeline` parent gets the real item id.
- **Placeholders.** Workspace item ids (notebook, dataflow, Spark job) stay unbound after conversion. Deploy refuses unbound placeholders unless `--allow-unbound`.
- **Schedules.** `--schedules` creates Fabric Job Scheduler schedules from converted triggers. They are created disabled and never duplicated. `--schedules-only` creates them for pipelines that are already in the workspace, without creating or overwriting any pipeline. Fabric requires an `endDateTime` (verified live 2026-10-16); when the ADF trigger has none, ten years out is used and the report says so.
- **Compute.** `compute-deploy` creates custom Spark pools and environments and publishes them.

## Not deployed

Spark job definitions, dataflows and connections. Connections go through the separate connection service (`connections/`, `scripts/create_fabric_connection.py`).

## Deploying a notebook

`adf2fabric deploy-notebook --workspace-id <guid> --file <nb.py|nb.ipynb> --name <Fabric name> [--source-name <name in ADF>] [--resolutions <file>] [--apply]` creates the notebook, or replaces the content of one with the same name. It never deletes. A `.py` file becomes one code cell; an `.ipynb` file is sent as it is. With `--resolutions` it records `ItemToId notebook:<source-name>` and the workspace id, so the next `convert --resolutions <file>` binds the pipeline's notebook activity to it and no longer asks for a manual migration. Code: `agent/deployment/notebook_deploy.py`.

The notebook's content is not changed: storage paths and the attached lakehouse must already be right for Fabric. Reading the notebook out of Synapse is not built yet; today the file comes from you.

Verified live on 2026-10-16: a notebook was created in `adf-migration-test`, its content read back intact, and the id bound through a resolutions file.

A long-running operation may come back with a `Location` on a regional host. That host is never followed; the operation is polled at `https://api.fabric.microsoft.com/v1/operations/{id}` (found live, fixed in `fabric_deploy._operation_url`).

## Giving notebooks access to storage (lakehouse and shortcut)

`adf2fabric create-shortcut --workspace-id <guid> --lakehouse <name> --name <shortcut> --storage-account https://<account>.dfs.core.windows.net --subpath /<container> --connection-id <guid> [--apply]` creates the lakehouse if it is missing, and an ADLS Gen2 OneLake shortcut in it, then prints the `abfss://` path a notebook reads it from. The connection must already exist in Fabric; no storage key is handled. It is idempotent and never deletes or overwrites: a shortcut with the same name and path that points elsewhere is reported (exit code 2). Code: `agent/deployment/shortcut_deploy.py`.

Why: a Databricks notebook that reads ADLS through a Unity Catalog external location fails in Fabric with HTTP 401, because Fabric uses the running identity's access (see `docs/databricks-notebooks.md`).

In Git Bash on Windows, set `MSYS_NO_PATHCONV=1` first, or `/container` is rewritten into a Windows path and the tool refuses it.

## Running a deployed pipeline

`adf2fabric run --workspace-id <guid> --pipeline <name> [--param name=value] [--apply]` runs a pipeline through the Job Scheduler API and waits for the result (`Completed`, `Failed`, `Cancelled`, `Deduped`, or `Timeout` if it outlasts `--timeout`; a timed-out run keeps running in Fabric). It prints the failure reason and each activity's outcome. Without `--apply` it only checks the pipeline exists. Code: `agent/deployment/fabric_run.py`.

Verified live on 2026-10-16: `PL_SQL_Data_Validation` completed with its three Lookups succeeded.

Endpoints and the create/update/list/long-running-operation flow follow the Fabric REST API. Verify against the official docs before changing them.

## Committing to the workspace's Git branch (`git-commit`)

`adf2fabric git-commit --project <name> --workspace-id <guid> [--item <name>] [--all-changes] [--comment ...] [--apply]` reads the
workspace's Git connection and status (`GET .../git/connection`, `GET .../git/status`, long-running when Fabric says so) and commits
with `POST .../git/commitToGit` in `Selective` mode: only the uncommitted workspace changes of the migration's pipelines (and any
`--item`), unless `--all-changes`. Without `--apply` nothing is committed.

It refuses when the workspace is not connected and initialised, and when the branch has changes the workspace does not have (update
the workspace from Git in Fabric first). Conflicting items are listed, never committed. Connecting the workspace to a repository
and branch, and the caller's Git credentials in Fabric, are set up by hand once. Built to Microsoft's REST reference (2026-10-22);
not yet run against a Git-connected workspace.
