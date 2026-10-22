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

Code: `src/adf2fabric/agent/workloads/` (`source.py` retrieval, `notebook_format.py`, `analyzer.py`, `transform.py`, `engine.py`,
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
