# Workload migration and compatibility assessment

Moves Spark workloads (Databricks notebooks, Python files and JARs; Synapse notebooks and Spark job definitions) toward Fabric
compute. **It is opt-in, approval-gated, and never the default**: without it `convert` keeps calling Azure Databricks
(`reuse_externally`) exactly as before.

```
adf2fabric workloads-assess   --project P [--databricks-token-env NAME | --source /Shared/x=file.py ...]   # read-only analysis
adf2fabric workloads-generate --project P [--secret-map SCOPE=https://vault.vault.azure.net/] [--artifact KEY=abfss://...] [--lakehouse-id GUID]
adf2fabric workloads-approve  --project P --workload <id|path|name> --by "<name>"                          # a person decides
adf2fabric workloads-deploy   --project P --workspace-id GUID [--resolutions file] [--apply]               # approved only; dry run by default
adf2fabric workloads-record-run --project P --workspace-id GUID --pipeline NAME --workload <id>            # evidence from a real run
```

Code: `src/adf2fabric/agent/workloads/` (`source.py` retrieval, `databricks_jobs.py` Databricks job structure, `notebook_format.py`, `analyzer.py`, `transform.py`, `engine.py`,
`lifecycle.py`, `report.py`), `agent/deployment/sjd_deploy.py`, persisted contract `project/workloads.py`
(`projects/<name>/workloads/manifest.yaml` + `report.md` + `artifacts/<name>/`).

## Pipeline, step by step

1. **Inventory.** The compute plan (`agent/conversion/compute_plan.py`) gives each Databricks / Synapse Spark activity and the
   compute it runs on. One manifest entry per distinct workload (notebook path, Python file, main class, Synapse item).
2. **Environment.** Platform, workspace host, cluster mode, runtime and Spark version, libraries, Spark setting NAMES, and the
   suggested Fabric runtime. What cannot be resolved is listed in `environment.gaps`.
3. **Code, only as authorised.** Nothing is fetched unless a credential was offered (`--databricks-token-env NAME`, or
   `--allow-az-tokens` for the Databricks audience): an `az` login does not grant Databricks access, so no token means
   `missing_credential` plus the permission needed (for example *Databricks workspace: CAN READ on the notebook*). Code already
   in the project (an earlier export, a Synapse notebook from the imported repository, a file passed with `--source`) is used
   without any call. A denied/not-found/unreachable call reports its status and the permission category, never the token. A
   JAR has no source (`binary`); a script in a cloud store is `unsupported_location` and names the storage role needed.
4. **Analysis.** Python is parsed with `ast`; other languages by line patterns. Findings carry a construct name and line, never
   the line itself (it can hold a credential). Categories: language, runtime, library, spark_config, data_access,
   authentication, networking, external_dependency, filesystem, api, contract, source.
5. **Classification** (worst finding wins): `directly_convertible`, `convertible_with_changes`, `requires_alternative`
   (Delta Live Tables, Auto Loader, `dbutils.jobs.taskValues`, `synapsesql` against a dedicated pool, ADLS Gen1),
   `blocked` (an input is missing: code, a dynamic path, an incomplete Spark job definition, a JAR with no upload location),
   `unsupported` (.NET for Spark).
6. **Generation** (`workloads-generate` only). Notebook: an `.ipynb` with a parameter cell and the rewrites below. Spark job:
   a `SparkJobDefinitionV1` definition. `requires_alternative`, `blocked` and `unsupported` workloads get **no artifact**: they
   get alternatives with their limitations (keeping the source compute is always offered for Databricks), never a placeholder.
   A cell that cannot run in Fabric (`%%csharp`, `%sh`, `%fs`) is kept as comments followed by `raise NotImplementedError`.
7. **Manifest.** Per workload: environment, code status, findings, rewrites, libraries (resolved to Environment public/custom
   library or flagged), parameter contract, output contract, artifact, required resources, open issues, static checks,
   approval, execution evidence.

## Automatic rewrites (only where the Fabric target is documented and the meaning is unchanged)

| Source | Fabric |
|---|---|
| `dbutils.widgets.text/dropdown/...` + `dbutils.widgets.get("x")` | a variable `x` in the notebook's parameter cell, default kept (a pipeline parameter named `x` overrides it) |
| `dbutils.notebook.exit(v)` | `notebookutils.notebook.exit(v)` |
| `dbutils.fs.{ls,mkdirs,cp,mv,rm,put,head,append}` | `notebookutils.fs.*` (`mv` only with two arguments) |
| `dbutils.secrets.get(scope, key)` | `notebookutils.credentials.getSecret("<vault URL>", key)` only with `--secret-map scope=url` |
| `mssparkutils.{fs,notebook,runtime}`, `credentials.{getToken,getSecret,putSecret,isValidToken}` | `notebookutils.*` (alias is "to be retired") |
| `spark.conf.set("spark.databricks.*" / "spark.synapse.*", ...)` | `pass` with a comment naming the setting |
| `%md` / `%sql` / `%scala` / `%r` cells | markdown / `%%sql` / `%%spark` / `%%sparkr` |

Everything else stays as written and is listed as an open issue (mounts, DBFS, volumes, `%run`, `dbutils.notebook.run`,
credentials in code, `getSecretWithLS`, ...).

## Creating the notebook in Fabric from the UI

On the Convert step, "Things you need to set" offers **Create it in Fabric from the generated notebook** for each notebook a pipeline runs
that the tool generated (`lifecycle.create_notebook`). Without confirmation it only says whether Fabric would create a notebook or replace
the content of the one with that name. Confirmed, it creates (or replaces) it in the chosen workspace from the generated file (refused if
the file changed since generation), records the person as accepting the generated notebook, records the id on the workload and as
`ItemToId notebook:<name>` in `conversion/resolutions.json`; the next Convert points the pipeline's notebook activity at it. A notebook
whose content was replaced keeps no earlier run as evidence. Same Fabric calls as `workloads-deploy --apply`.

A Databricks notebook (or a Databricks job's notebook task) is referred to by the converted pipeline only once it exists in Fabric, so
it never appears in that Convert list. For those, the Deploy step's **Notebooks the pipelines run** offers the same **Create it in
Fabric from the generated notebook** for every generated notebook that is not bound yet (added 2026-11-10; before that only
`workloads-deploy` could create them). Then Convert again.

Since 2026-11-10 the UI's **Deploy** also does this for you: approved generated notebooks the chosen workspace lacks are created there first, Convert runs again with their ids, and the pipelines deploy in the same action (the dry run lists them; see [deploy.md](deploy.md)). On the Deploy step, **Bind** is pre-filled only with an id from the workspace being deployed to: another workspace's notebook id is never offered (found live: Bind in a second workspace was pre-filled with the first one's notebook and could only be refused).

## One notebook per workspace

A generated notebook's storage paths name a workspace and a lakehouse by id (`abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/<shortcut>/...`).
Creating or checking it in another workspace uses a copy whose OneLake paths point at that workspace's lakehouse of the same name (the
storage map's), written to `workloads/artifacts/<name>/workspaces/<workspace>.ipynb`; everything below the lakehouse stays the same,
because the plan makes the shortcuts with the same names. When that workspace has no such lakehouse yet, Create says so and points to
Plan target. The id of each workspace's notebook is kept per workspace (`artifact.fabric_item_ids`), so binding or creating it in one
workspace does not unbind it in another; the generated file itself is never changed.

## Binding a notebook that is already deployed

A notebook that reached the workspace another way (`deploy-notebook`, by hand, an earlier `workloads-deploy`) can be recorded as the
workload's item without `workloads-approve` + `workloads-deploy`: the UI's Deploy step lists the generated notebooks the pipelines run, with
the id each pipeline runs filled in (`GET /projects/{id}/workloads/notebooks`).

- **Check** (`POST .../workloads/notebooks/bind`, `plans:generate`) reads the notebook from the chosen workspace (`getDefinition`, ipynb)
  and compares its code cells with the generated notebook. Read-only.
- **Bind** (the same with `confirm: true`, also `provision:approve`) does the same check and, only if the code is equal, records the id
  in the manifest and `ItemToId notebook:<name>` in `conversion/resolutions.json`, and records the signed-in person as accepting the
  generated notebook (the approval, with the id in its note). Audited as `workload.bind`. Nothing in Fabric is changed.
- An id that is not a notebook in the workspace, or a notebook whose code differs, is refused. Static validation then passes the
  notebook check (`compute.fabric_compute`). Binding also takes the execution evidence from the run-and-compare record when a current,
  completed Fabric run of a pipeline that runs this notebook has the step bound to that id and succeeded (`lifecycle.recorded_run`; the
  run id and the pipeline are in its detail); otherwise execution evidence is `workloads-record-run`'s.
- Code: `agent/workloads/lifecycle.py` (`check_deployed`, `bind_deployed`), `agent/deployment/notebook_deploy.read_cells`. Live-checked on
  2026-11-04 against the two notebooks in `Practise` (equal) and a mismatched id (refused).

## Running generate again

- Each `workloads-generate` run rewrites the workloads it covers. A new artifact needs a new `workloads-approve`, and its `execution` and
  `fabric_item_id` start empty until `workloads-deploy --apply` and `workloads-record-run`.
- Run without the code (no `--source`, no `--fetch`, nothing cached under `databricks/notebooks/`), or with code that cannot be reached
  this time (`not_requested`, `missing_credential`, `denied`, `not_found`, `unreachable`, `error`), a workload whose code was read before
  and which has an artifact keeps its earlier record whole: artifact, approval, Fabric id and run evidence. The manifest gets a note
  naming it, and the CLI prints the note. Pass the code again to regenerate it. (Fixed 2026-11-05; before that the record was replaced
  with `blocked; code not_requested`.)
- In Git Bash on Windows, `--source /Shared/x=file.py` has its `/Shared/...` rewritten into a Windows path and is silently not matched.
  Set `MSYS_NO_PATHCONV=1`, or key by the notebook's name alone: `--source x=file.py`.

## Generated is not verified

| Evidence | Meaning |
|---|---|
| `generation.status` | the artifact was written and passed **static** checks (valid ipynb, Python cells parse, no `dbutils` left, parameters declared, size). `generated_with_open_issues` when something remains open. |
| `approval` | a person accepted the CURRENT artifact (tied to its hash; changed output resets it). |
| `artifact.fabric_item_id` | it was deployed. Not proof that it runs. |
| `execution.status` | **only** `workloads-record-run` sets it, from a real Fabric pipeline run whose activity is bound to the deployed item. |

The report prints "Verified executing in Fabric: N" separately from "generated".

## Pipeline conversion

`convert` binds a Databricks notebook activity to a Fabric notebook only when the notebook was deployed (resolutions file).
With `baseParameters` it additionally requires an approved, deployed workload whose parameters the generated notebook reads;
passed-but-unread parameters are dropped visibly. Downstream expressions that read `runOutput` get a manual-step warning with
the Fabric location of the exit value. Libraries still keep the activity on Databricks.

## Databricks Job activities

An ADF `DatabricksJob` activity holds only a `jobId`: what the job runs is defined in Databricks. Fabric's Azure Databricks
activity has a Job type, but Microsoft documents only its portal settings, not its JSON, so without migration the activity stays a
Fail placeholder (`databricks_job`). With migration:

1. **Read the job, only when allowed.** `workloads-assess` / `workloads-generate` with a Databricks credential
   (`--databricks-token-env NAME` or `--allow-az-tokens`) call `GET /api/2.2/jobs/get` for each literal `jobId` (every page of
   tasks) and keep only the job's structure in `projects/<p>/databricks/jobs/<id>.json` (`agent/workloads/databricks_jobs.py`):
   task keys and types, notebook paths, dependencies, `run_if`, and the NAMES of parameters and libraries. No parameter values, no
   cluster or compute settings, no secrets. A job read once is reused from that file; without a credential nothing is called and the
   job is `not_requested` / `missing_credential` with the permission it needs (*CAN VIEW on job &lt;id&gt;*). A `jobId` that is an
   expression is `dynamic`, never guessed. `/Workspace/Shared/x` and `/Shared/x` are one notebook.
2. **Notebook tasks become workloads.** When Fabric notebooks can run the whole job (below), each notebook task becomes an ordinary
   `databricks_notebook` workload (code export, analysis, generation, approval, deploy, run evidence are unchanged), `used_by` the
   Fabric activity that will run it. Its compute is `job_defined` in the compute plan: the job, not the linked service, sizes it,
   so the size is reported as not available. Otherwise the job is ONE `databricks_job` entry with no artifact: `blocked` when it was
   not read, `unsupported` with each reason when it was. Its notebooks are not half-migrated.
3. **Conversion.** `convert` replaces the activity only when the job's definition is in the project and every task's notebook is
   deployed and bound (`notebook:<name>`, as for a notebook activity). One task: a Fabric Notebook activity with the activity's own
   name. Several tasks: one Notebook activity per task named `<activity>_<task key>`, wired as the job wires them
   (`ALL_SUCCESS` -> `Succeeded`, `ALL_DONE` -> `Completed` on each upstream task); the activity's dependencies attach to the tasks
   that start the job, and what depended on the activity waits for the tasks that end it. If one of those names is already used in
   the pipeline, the job stays blocked (it is never renamed, because run evidence is matched by that name). Policy and state are
   copied to every task activity. A downstream expression that reads the job activity's output becomes a manual step.

**Fabric cannot run the job as notebooks** (each is a stated reason, and the placeholder stays) when it has: a non-notebook task
(SQL, Python file, JAR, wheel, dbt, pipeline, run-job, condition, for-each ...), a notebook from Git, task `base_parameters`, job
parameters, `jobParameters` on the activity, task libraries, a `run_if` other than `ALL_SUCCESS` / `ALL_DONE`, a dependency on a
condition outcome or on a missing task, or a dependency cycle.

## What Microsoft documents, and what is still unverified

Verified against Microsoft Learn (2026-10-22): notebookutils `fs`, `notebook.run/exit`, `credentials.getSecret(<vault URL>, name)`,
`getToken`, `runtime.context`; `mssparkutils` alias; `%run <name>` (same workspace, depth 5); `%%configure`; `%pip`; runtimes
1.3 (Spark 3.5, Scala 2.12.17, Java 11) and 2.0 (Spark 4.1, Scala 2.13.16, Java 21); Spark Job Definition `SparkJobDefinitionV1`
definition and `POST /workspaces/{id}/items`; every SJD needs a lakehouse; no .NET for Spark; linked services, DMTS-in-jobs and
Synapse linked-service Spark settings are removed; `synapsesql` is not compatible with dedicated SQL pools.

**Not documented, but confirmed by a live run (2026-10-22, workspace `adf-migration-test`).** A notebook whose first cell is tagged
`parameters` (`p = "default"`, `q = "default"`) and whose last cell is `notebookutils.notebook.exit(p + "|" + q)` was run from a
pipeline Notebook activity passing `p = {"value": "override", "type": "string"}` and `q = {"value": {"value": "@pipeline().parameters.x",
"type": "Expression"}, "type": "string"}`. The activity output was `result.exitValue = "override|from_expression"`. So:

* the `parameters` cell tag marks the parameter cell;
* `{value, type}` is the parameter shape, and an Expression value works inside it;
* the notebook's exit value is `activity('<name>').output.result.exitValue` (replaces Databricks `output.runOutput`).

Probe items left in the workspace for you to delete in the portal: notebook `zz_wl_probe_nb`, pipeline `zz_wl_probe_pl`.

**Still not documented, so unverified** (surfaced, never asserted):

* the `ipynb` metadata keys that attach a default lakehouse / Environment (so none are generated: the lakehouse is a required resource);
* `language` values for R Spark jobs (none is generated);
* a Databricks-to-Fabric dbutils mapping (Microsoft has no such guide: the rewrite table is limited to documented names);
* running a Spark Job Definition created by `workloads-deploy` (its definition follows the documented format but no job was run).

## Limits

* Library compatibility is not checked against the Fabric runtime's package list (only: PyPI libraries go to the Environment,
  JAR/wheel files need uploading, Maven coordinates need review, `dlt`/`dbruntime` do not exist).
* A JAR is binary: its code is not scanned, and its Scala version must match the runtime.
* Spark job code in storage is read only if you pass it with `--source` (the exact storage role is reported otherwise).
* DBFS retrieval (`dbfs/read`) and the Databricks export call are exercised with fakes in tests; the export call matches the
  one proven live for notebooks, the DBFS call has not run against a real workspace.
* Databricks Job activities: job and task parameters are not mapped, and only notebook tasks migrate (see
  [Databricks Job activities](#databricks-job-activities)). The `jobs/get` call is tested with fakes; its first live use is noted
  in [status-briefing.md](status-briefing.md) once it has run.
