# Spark compute plan

Which external Spark compute an ADF estate runs on, how much of it, and the Fabric compute that would replace it.
It is part of `convert`: the plan lands in the **Spark compute plan** section of `conversion-report.md`, in
`conversion.yaml` (`compute_plan`) and as `compute-plan.json` beside the report.

**The plan is read-only** (creation is the separate `compute-deploy` step below). Each proposed pool carries the exact request body of the documented
[Create Workspace Custom Pool](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool)
API (`POST /v1/workspaces/{workspaceId}/spark/pools`, workspace Admin role) so a later, explicit apply step can send it
unchanged. Creating a pool is free in Fabric; capacity is consumed when a Spark session runs.

## What it reads

| Source | What is read | Where |
|---|---|---|
| Databricks linked service (new cluster) | node type, driver node type, workers (`"4"` fixed or `"2:8"` autoscale), runtime version, Spark conf (secret-looking settings masked), init-script count, custom-tag and env-var *names* | the ADF source (discovery) |
| Databricks activity | `libraries` | the ADF source |
| Synapse notebook / Spark job activity | pool name, `executorSize`, `driverSize`, `numExecutors` | the ADF source |
| Databricks **existing cluster** or instance pool | nothing: ADF holds only the id | the **compute sources file** |
| Synapse **Spark pool** | nothing: ADF holds only the name | the **compute sources file** |
| Triggers | which triggers start each pipeline, through parent pipelines too | the ADF source |

A value authored as an expression or a linked-service parameter is **not read** and is listed as unresolved. A size that
is not available is never guessed: the proposal is `needs_input` and says what to supply.

### The compute sources file

`adf2fabric convert --project X --compute-sources compute-sources.json`

```json
{
  "databricks_clusters": {
    "0912-101500-abc123": { "...": "a Databricks Clusters API 'get' response: node_type_id, driver_node_type_id, num_workers or autoscale{min_workers,max_workers}, spark_version, runtime_engine, spark_conf, init_scripts" }
  },
  "synapse_spark_pools": {
    "sparkpool01": { "properties": { "nodeSize": "Medium", "sparkVersion": "3.4", "autoScale": { "enabled": true, "minNodeCount": 3, "maxNodeCount": 10 } } }
  }
}
```

Keys are the Databricks cluster id and the Synapse pool name. A Synapse pool may be given as the full ARM resource or
just its `properties`. A broken file is reported in the plan; it never fails the conversion. A runnable example:
[`tests/fixtures/spark_estate/`](../tests/fixtures/spark_estate/).

## Where each activity would run (`targets`)

Every Databricks and Synapse Spark activity gets a target record in `compute-plan.json` and a table in the report:

| Field | Meaning |
|---|---|
| `conversion_today` | what `convert` does with the activity now (`reuse_externally`: a Databricks notebook keeps calling Databricks; `replace`: a Synapse notebook is a scaffold). **Pipeline conversion is not changed by the plan.** |
| `target_execution` | `FabricNotebook` (notebook activities) or `FabricSparkJobDefinition` (Databricks JAR/Python, Synapse Spark job) |
| `spark_pool`, `environment` | the proposed pool and environment for its compute |
| `source_item`, `parameters` | notebook path / name / main class, and the parameter **names** it passes (never values) |
| `requires_notebook_migration`, `requires_job_definition`, `requires_compatibility_review` | what is still needed; review reasons say why (the code is not scanned yet) |
| `blockers` | e.g. GPU nodes, or a compute whose size is not known |
| `provisioning_status` | `pending_approval`, `needs_input`, `not_possible`, then `exists` / `created` after `compute-deploy --apply` |

## How it sizes

| Rule | Basis |
|---|---|
| Node size = the smallest Fabric node with at least the source node's vCores: Small 4, Medium 8, Large 16, X-Large 32, XX-Large 64. Rounded-up sizes are stated; above 64 is `not_possible`. | Fabric Spark compute page |
| Nodes = workers + 1 driver (a Fabric pool has one head node, executors 1:1 with nodes). Autoscale min/max follow the source's min/max workers. | Fabric Spark compute page |
| A fixed-size cluster is a pool with autoscale min = max. | The API does not say which field holds the count when autoscale is off |
| `dynamicExecutorAllocation` is on only when the pool scales; executors = nodes - 1. | Create Workspace Custom Pool |
| Peak capacity units = peak Spark vCores / 2, **before burst**. Not an SKU recommendation. | 1 capacity unit = 2 Spark vCores |
| vCPUs of an Azure VM size are read from its name (`Standard_E8ds_v5` = 8; legacy D/DS v1/v2 by size index; `E8-4` = 4). Anything else is `node_type_unresolved`. | Azure naming convention - **not independently verified** |
| Fabric runtime: Spark 3.5 -> 1.3, 4.x -> 2.0, older or unknown -> stated with a warning. Runtime 1.3 is **EOSA** and 2.0 is the GA runtime (Spark 4.1) in Microsoft's table, so every 1.3 proposal carries that warning. | Fabric runtime page |
| Databricks Runtime -> Spark version uses Microsoft's supported-releases table only (14.3, 15.4, 16.4, 17.3, 18, 19). Others are "unknown". | Databricks release notes |

Source pools with the same sizing share one proposed pool; an Environment groups computes with the same pool, runtime,
Spark properties and libraries.

## What it cannot carry over (reported under *Needs a person*)

| Code | Meaning |
|---|---|
| `sizing_unavailable` / `sizing_incomplete` | ADF only references the compute; supply it in the sources file |
| `unresolved_cluster_setting` | a setting is parameterized in ADF |
| `node_type_unresolved` / `exceeds_largest_node` | vCPUs unreadable / larger than XX-Large |
| `gpu_not_available` | the documented pool API offers only the MemoryOptimized family |
| `driver_larger_than_workers` | a pool has one node size |
| `platform_spark_conf` | `spark.databricks.*` (and `spark.master`) are not carried |
| `secret_in_spark_conf` | a secret or `{{secrets/...}}` reference in a Spark property: use Key Vault in the notebook |
| `init_scripts` | not carried; re-express as Environment libraries, Spark properties or notebook code |
| `custom_library` | non-PyPI libraries (jar, Maven, CRAN, wheel, egg) |
| `synapse_session_override` | per-activity executor/driver sizing has no Fabric Notebook-activity equivalent |

## Creating the compute in Fabric (`compute-deploy`)

```
adf2fabric compute-deploy --project X --workspace-id <guid>                       # dry run: lists what exists, changes nothing
adf2fabric compute-deploy --project X --workspace-id <guid> --pool ADF-Large-n3-9 --apply
adf2fabric compute-deploy --project X --workspace-id <guid> --all --apply          # approve everything proposed
adf2fabric compute-deploy ... --all --apply --publish                              # also publish the new environments
adf2fabric compute-deploy ... --all --apply --reuse                                # reuse compatible existing pools
```

**You approve what gets created.** `--apply` is refused unless you name something (`--pool`, `--environment`) or pass
`--all`; nothing unnamed is created. It needs `az login` with a workspace **Admin** (pool creation) on a supported capacity.

| Rule | Why |
|---|---|
| Dry run by default; it only lists the workspace's pools and environments. | read-only |
| Never modifies or deletes. A pool with the same name and settings is `exists`; same name, different settings is `conflict`. An existing environment is left unchanged. | safe re-runs |
| **Existing pools are reused before anything is created.** A pool with identical settings is reused whatever it is called. With `--reuse`, a pool with the *same node size* whose node range covers the proposal's (and with dynamic allocation on) is reused too, even though it may scale further than planned. Without `--reuse` such a pool is only mentioned, so the choice is yours. Different node size, or a range that does not cover the proposal's, is never reused. | share compute instead of one pool per pipeline |
| After `--apply` the Fabric ids and each activity's `provisioning_status` are written into `compute-plan.json`, and the outcome into `compute-provisioning.json`. | a record of what was created or reused |
| A pool is created first, then an environment that points at its id (`instance_pool_id`). An environment is refused unless its pool was approved too or already exists. | the environment needs the pool |
| A proposal that is `needs_input` or `not_possible` is never created. | no guessed sizes |
| Pool bodies are checked against the documented schema before sending, so the plan file may be edited by hand (change a node size, rename a pool) and is re-validated. `convert` rewrites `compute-plan.json`, so apply edits before converting again. | |
| **Publishing is separate** (`--publish`): it runs as a Spark job and uses capacity, and Fabric allows one publish at a time. | cost |

The environment is created with `Setting/Sparkcompute.yml` (runtime, pool, cores/memory from the pool's node size, dynamic
allocation, Spark properties) and, for PyPI libraries, `Libraries/PublicLibraries/environment.yml`. Native execution engine is
written **off**. Endpoints: List/Create Workspace Custom Pool, List/Create Environment, Publish Environment (`beta=false`).

Verified against Microsoft's reference on 2026-10-14. Verified by a read-only dry run against the test workspace (it
listed pools and environments successfully). **Creation itself has only been tested against a fake REST session**, not yet
against a live workspace.

## Not built yet

* Reading the Databricks workspace / Synapse workspace live (today: the sources file).
* Retrieving, scanning and converting notebook **code**, and remapping the Databricks activity to a Fabric Notebook
  activity. Databricks notebooks are still reused externally.
* Custom (non-PyPI) libraries in the environment: the jar/wheel files are not available to the tool.
* Whether an environment created with a definition needs `--publish` before its settings apply is assumed from Microsoft's
  save-and-publish description, not confirmed. Single-node pools: the executor maximum equals the node count, which the
  definition article says must be lower than the pool's maximum; check such a pool in Fabric.
* HDInsight Spark sizing.
* Validated only on the synthetic fixture estate: no real ADF source with Databricks or Synapse compute exists in this
  project yet.
