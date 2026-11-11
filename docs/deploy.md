# Deploy

Code: `src/adf2fabric/agent/deployment/fabric_deploy.py` (pipelines, schedules) and `compute_deploy.py` (pools, environments). CLI: `adf2fabric deploy`, `adf2fabric compute-deploy`.

## Behaviour

- **Dry run by default.** `--apply` is required to write to the workspace.
- **Ordering.** Child pipelines deploy first so each `ExecutePipeline` parent gets the real item id.
- **Placeholders.** Workspace item ids (notebook, dataflow, Spark job) stay unbound after conversion. Deploy refuses unbound placeholders unless `--allow-unbound`.
- **Generated notebooks the workspace lacks (API / UI deploy).** Before deploying, the deploy stage looks for generated notebooks the pipelines in scope run that are approved (a person approved them at least once; an approval resets when the code changes) and have no id in the chosen workspace (`lifecycle.notebooks_to_create`). The **dry run** asks Fabric what would happen, converts again for this workspace (project files only) and reports the pipelines that run them as `pending` ("deploys once its notebook(s) ... are created"), which does not make the dry run unclean. The **real deploy** creates them (a copy pointed at this workspace's lakehouse, as the Create button does; the deploying person is recorded as accepting them, so it needs `provision:approve`), converts again with their ids and deploys every pipeline in one action. A notebook with the same name already in the workspace is never replaced: the pipelines that run it are refused until it is bound (Deploy step, Bind). Added 2026-11-10: before, a second workspace needed Create on each notebook, then Convert, then Deploy. The CLI `deploy` does not do this (use `workloads-deploy`).
- **Schedules.** `--schedules` creates Fabric Job Scheduler schedules from converted triggers. They are created disabled and never duplicated: an existing schedule counts as the same when every field the tool sets has the same
value, read the way Fabric echoes it back (fractional seconds, a trailing `Z` and letter case ignored, fields Fabric adds ignored), and a
schedule created earlier in the same run counts too. `--schedules-only` creates them for pipelines that are already in the workspace, without creating or overwriting any pipeline. Fabric requires an `endDateTime` (verified live 2026-10-16); when the ADF trigger has none, ten years out is used and the report says so.
- **Compute.** `compute-deploy` creates custom Spark pools and environments and publishes them.
- **The deploy record.** Every deploy, from the CLI or the API stage, writes `deployment/deployment-record.json`: the last dry run (workspace, pipelines, refused count) and, for a real deploy, each pipeline's last action and the workspace it went to (and, when it failed or was refused, what was said, redacted, so
`diagnose` can explain it; see [troubleshooting.md](troubleshooting.md)). The Report, the inventory, the waves and the UI's Deploy step read it, and count a pipeline as deployed **only for the workspace it was deployed to**: choosing another workspace shows it as converted, with the reason "deployed to another Fabric workspace". (Before 2026-11-04 the CLI did not write this record and the views ignored the chosen workspace, so a CLI deploy to a new workspace showed as not deployed while the old workspace still showed as deployed.)
- **A deploy makes earlier runs stale.** A recorded run older than the pipeline's latest deploy no longer vouches for it (`verify.current_record`), so after a redeploy run `verify --apply` again.

## Not deployed

Notebooks, Spark job definitions, dataflows and connections are not pushed by `deploy`. Notebooks go through `deploy-notebook` (a file you hold) or `workloads-deploy` (an approved generated artifact, whose id is then recorded in the workload manifest). Connections go through the separate connection service (`connections/`, `scripts/create_fabric_connection.py`).

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

## Read-back after deploy

Since 2026-11-11 a real deploy from the API (`deploy_apply`) is followed by a read of the workspace: the API and deployment checks
of the validation engine (`engine.record(readback=True)`, read-only). It lists every pipeline (its definition is fetched with
`getDefinition` and compared with the generated file), every item and connection the pipelines use, and records them with the time,
the workspace and a hash of the generated files. The Deploy step shows that list ("What is in Fabric (read back)"; [ui.md](ui.md)).
The result is used by the verdict only while it still describes the workspace: a new conversion or a later deploy means a new
read-back. A read that fails is reported in the deploy result (`readback.ok: false`) and nothing is claimed. The CLI's `deploy` does
not read back; `adf2fabric validate --stage api --stage deployment` does the same read.

## Results after deploy

`agent/deployment/run_report.py` builds the results the UI shows on Deploy and Report ("Migration results"; [ui.md](ui.md)): per
pipeline the deploy (to the chosen workspace only), the latest Fabric run (a run older than the latest deploy is not counted), the
baseline it was compared with, the verdict, the checks matched, and the rows and files the Copy activities read and wrote, Fabric beside
the baseline, down to each activity. It reads the deploy record, `verification/verification.json` and the output checks; it calls
nothing. The baseline's figures come from `baseline_activities`, which `verify` records with each comparison.

**Choosing the baseline.** `verify` compares with the pipeline's last successful ADF run, or with an earlier Fabric run when there is
none. When the ADF run is known not to be comparable (it ran on other data, or hit the Lookup row limit), `verify --pipeline NAME
--no-adf` compares that pipeline with earlier Fabric runs and records the choice (`baseline_choice: fabric`). Every later `verify`,
including the UI's *Compare latest runs*, keeps it, so reading the runs again never swaps it for a newer ADF run; the results show
"(chosen)". `verify --pipeline NAME --use-adf` undoes it. Reading the same Fabric run again with nothing new to compare reports the
comparison already recorded for it.

## Committing to the workspace's Git branch (`git-commit`)

`adf2fabric git-commit --project <name> --workspace-id <guid> [--item <name>] [--all-changes] [--comment ...] [--apply]` reads the
workspace's Git connection and status (`GET .../git/connection`, `GET .../git/status`, long-running when Fabric says so) and commits
with `POST .../git/commitToGit` in `Selective` mode: only the uncommitted workspace changes of the migration's pipelines (and any
`--item`), unless `--all-changes`. Without `--apply` nothing is committed.

It refuses when the workspace is not connected and initialised, and when the branch has changes the workspace does not have (update
the workspace from Git in Fabric first). Conflicting items are listed, never committed. Connecting the workspace to a repository
and branch, and the caller's Git credentials in Fabric, are set up by hand once. Built to Microsoft's REST reference (2026-10-22);
not yet run against a Git-connected workspace.
