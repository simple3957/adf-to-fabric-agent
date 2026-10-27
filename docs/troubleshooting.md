# If pipelines or services are not converted, deployed or running in Fabric

Start here:

```
adf2fabric diagnose --project P --workspace-id W      # read-only; writes projects/P/diagnosis/diagnosis.md
```

It finds, for every pipeline, the **first** thing in its way, why, and what to do; ranks the fixes so the one that unblocks the most pipelines comes first; lists
every service (linked service and the Fabric connection it needs) with its state; and names pipelines that are in the workspace but not in this project (it
never deletes anything: remove stale ones in the portal). Without `--workspace-id` it covers conversion and binding only.

## The ladder (each stage needs the one before)

| Stage | What it means | What to do |
|---|---|---|
| **not converted** | The converter left an activity for a person (manual or blocked). | Open the conversion report. Each activity says why; `diagnose` gives the fix for each code (below). Re-run `convert`. |
| **unbound** | Converted, but a connection, gateway or item it needs has no real id yet. | Create it in Fabric, then bind it in the resolutions file; `convert --resolutions ...`. |
| **not deployed** | Not in the workspace. | `deploy --with-children` (a dry run) then `--apply`. It refuses unbound placeholders unless you say so. |
| **deployed, differs** | What is in Fabric is not what was generated. | Deploy the current file again; `pipeline-report` shows the difference. |
| **never run** | Deployed but never started (or only runs when its caller does). | `verify --apply`. |
| **run failed** | The latest run did not complete. | `diagnose` matches the message to a cause (below); open the run in Fabric Monitor for the activity's error. |
| **differs from baseline** | It ran, but the result is not what ADF gave. | Find out which is right: a changed source, a different parameter, a data race. Fix the cause, run again. |
| **proven** | Deployed, run, and matched its ADF baseline. | Done for that pipeline. `validate` says whether the migration as a whole can be called verified. |

## Services: what makes a connection work

1. **It exists.** The tool can create SQL/Synapse, ADLS Gen2, Blob, REST and Databricks connections (Connect step, or `connections apply`). Everything else is created
   by hand in Fabric (Settings > Manage connections and gateways).
2. **It is bound.** The pipeline holds the connection's id. Bind with `LinkedServiceToConnectionId` (key = the linked service) in the resolutions file; a linked service
   that is not in the source can still be bound this way (the activity then converts with a visible difference). Datasets that pass different values to one linked
   service need one connection each (`LS?Param=value`).
3. **Its credential is entered and tested in Fabric.** Nothing is copied from ADF (secrets are never stored in project files).
4. **The identity can reach the data.** For ADLS: Storage Blob Data Contributor. Reading ADLS through a Databricks-style external location fails with 401: use a
   OneLake shortcut (`storage-map`, `create-shortcut`) or grant the running identity.
5. **A gateway is online** when the source is on-premises or private (a self-hosted integration runtime in ADF): install it, register it, share it, create the connection through it.
6. **The source is awake and reachable.** A paused Synapse pool, a firewall, or a private network is the most common reason a correct pipeline fails.

## What `diagnose` tells you for the usual conversion findings

| Finding | Usual fix |
|---|---|
| `linked_service_not_found` | Complete the source (`linkedService/`, the ARM export, or `intake --factory ... --artifact-kind linkedservice`), or create the connection and bind it. |
| `dataset_not_found` | Add the `dataset/` folder to the source. A dataset cannot be bound by hand. |
| `synapse_notebook_migrate`, `spark_job_migrate` | Migrate the notebook or job (`workloads-assess`, `-generate`, `-approve`, `-deploy`), bind its id, convert again. |
| `databricks_cluster` | Supply the cluster (`--compute-sources`), or move the notebook to Fabric. |
| `unsupported_trigger`, `unsupported_triggerbody`, `web_dynamic_url` | Pass the value as a pipeline parameter with a default (the optional model-assisted drafts can propose this: `docs/assisted-drafts.md`). |
| `ssis_unsupported`, `usql_retired`, `aml_deprecated`, `dataflow_reauthor`, `hdinsight_program_settings`, `databricks_jar_python`, `no_conversion_rule` | No automatic conversion exists. Rebuild the logic (Fabric notebook, Dataflow Gen2, Spark job) and call it from the pipeline. |
| `stored_procedure_linked_service_not_in_source`, `script_store_unsupported` | The linked service must be in the source (its store type decides what Fabric supports). |

## What `diagnose` tells you for a failed run

| The message says | Usual cause | First thing to do |
|---|---|---|
| 401 / 403 / unauthorized / login failed | The connection's credential or permissions | Re-enter the credential in the Fabric connection and Test; grant the identity access to the source and target |
| capacity, throttled, 429 | The Fabric capacity is paused or overloaded | Resume or scale it, wait, run again |
| cannot open server, paused, firewall, network | The source is unreachable | Resume the source; allow Fabric through (gateway or managed private endpoint) |
| incorrect syntax near, invalid column/object | The SQL does not run on the target | Fix the query in the ADF source (a reserved word as an alias needs [brackets]) and reconvert |
| expression, InvalidTemplate, cannot be evaluated | An expression fails in Fabric | Rewrite it with documented functions |
| connection not found / invalid | The id does not exist or is not shared | Check the resolutions id; share the connection with whoever runs it |
| gateway | The on-premises gateway is offline or not shared | Start it; check it is Online; share it |
| notebook, livy, spark | The notebook failed | Open its run in Monitor (a 401 on ADLS means: use a shortcut or grant the identity) |
| not found, does not exist, 404 | A file, folder or table is missing | Check the shortcut path and the data; run `storage-map` and create the shortcut |
| timeout | The run exceeded its limit | Find what was slow in Monitor; raise the timeout |

A message the table does not recognise is shown raw, with the failed activity's name, and the advice is to read it in Monitor. It never invents a cause.

## A checklist for a pipeline that runs perfectly

1. The source is complete (pipelines, datasets, linked services, triggers, runtimes) and discovery found no unresolved references.
2. `convert` leaves nothing manual or blocked, and the source checks (reserved SQL aliases, missing artifacts) are fixed in the ADF source.
3. Every connection exists in Fabric, its credential is entered and tested, and its id is bound; gateways are online; shortcuts exist for storage you repoint.
4. The capacity is active and the workspace is on it; whoever runs has the permissions.
5. `deploy` dry run is clean, then `--apply` (children first). `pipeline-report` shows the deployed structure equals ADF's.
6. `verify --apply` ran it and the result matched ADF's last successful run. Branches that did not run (the report lists them) are exercised with inputs that take them.
7. `validate --stage all --runtime` has no failed or blocked required check. Only `MIGRATION_VERIFIED` may be called a verified migration.
