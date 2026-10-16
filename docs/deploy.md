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

## Running a deployed pipeline

`adf2fabric run --workspace-id <guid> --pipeline <name> [--param name=value] [--apply]` runs a pipeline through the Job Scheduler API and waits for the result (`Completed`, `Failed`, `Cancelled`, `Deduped`, or `Timeout` if it outlasts `--timeout`; a timed-out run keeps running in Fabric). It prints the failure reason and each activity's outcome. Without `--apply` it only checks the pipeline exists. Code: `agent/deployment/fabric_run.py`.

Verified live on 2026-10-16: `PL_SQL_Data_Validation` completed with its three Lookups succeeded.

Endpoints and the create/update/list/long-running-operation flow follow the Fabric REST API. Verify against the official docs before changing them.
