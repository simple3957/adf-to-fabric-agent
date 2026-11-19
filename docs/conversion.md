# Conversion (ADF -> Microsoft Fabric pipelines)

`adf2fabric convert --project <name>` turns the persisted **semantic representation**
(`discovery/adf-semantic.yaml`) into Fabric pipeline definitions. It is deterministic and uses no LLM.
It never reads or modifies `source/`, never calls Azure or Fabric, and writes only under
`projects/<id>/conversion/`:

```text
conversion/
├── conversion.yaml                     # the record (per-activity strategy/status/mappings/validation)
├── conversion-report.md                # what to read first
├── resolutions.template.json           # every unbound connection / item, ready to fill with GUIDs
└── pipelines/<name>.DataPipeline/pipeline-content.json   # Fabric definition part ({"properties": ...})
```

## Outcomes are never "JSON was generated"

Each source activity gets **one** record with four independent facts:

| Fact | Meaning |
|---|---|
| `strategy` | A `direct`, B `transformed`, C `composite`, D `alternative`, E `manual` (blocker) |
| `status` | `converted` (behavior preserved) · `converted_with_differences` · `manual_required` · `blocked` |
| `structural_valid` | generated JSON is consistent with the documented Fabric schema |
| `runtime_ready` | structurally valid **and** every connection / item / gateway is bound |

Status is derived from recorded facts (differences, manual steps, unsupported expressions, validation
errors) — a structurally invalid activity can never be `converted`. A **blocked** activity is replaced by a
`Fail` placeholder so the dependency graph stays intact and a run stops loudly instead of silently skipping
the operation. Coverage proves every source activity, operation and dynamic expression is accounted for.

## Rules (the registry)

**Everything the converter knows is in one file:** the master compatibility catalog
([master-catalog.md](master-catalog.md), `src/adf2fabric/catalog/compatibility.json`) holds, for each of the 41 ADF activity types,
every way it can be converted (condition, Fabric activities, status, code), what is and is not carried, manual steps, workarounds,
Microsoft's own comparison and lifecycle, plus connectors, triggers, concepts and the Fabric schema, each with its source page. The
rule registry, the compatibility catalog, connector verdicts and the schema below are read from it. Each project gets its own
`conversion/activity-mapping.json` (every activity, its options, and a per-activity choice of storage route, Spark platform and data
flow target) that Convert writes and follows.

`agent/conversion/registry.py` holds one `ConversionRule` per activity family; each declares ADF types and
variants, Fabric targets, property / input-output / expression / dependency / parameter-variable rules,
unsupported properties, manual configuration, validation rules and runtime disposition
(`replace` · `reuse_externally` · `redesign`). Handlers live in `agent/conversion/handlers/`.

| ADF | Fabric | Strategy |
|---|---|---|
| Copy | Copy (datasets inlined, connection per linked service) | B |
| Lookup, GetMetadata, Delete | same (inline `datasetSettings`) | B |
| Validation | Until + GetMetadata + If + Wait (Microsoft's documented pattern) | C |
| ForEach, If, Switch, Until, Filter, Wait, Fail, Set/AppendVariable | same | A |
| ExecutePipeline | InvokePipeline | B |
| Stored procedure, Script | same, only on documented stores; else blocked with alternatives | B / E |
| Synapse SQL pool stored procedure (`SqlPoolStoredProcedure`) | Fabric Stored procedure on a connection bound by name (`connection:sqlpool:<pool>`); the pool itself does not move | B |
| Web, WebHook | same (URL split into connection + `relativeUrl`; dynamic URL -> manual) | B |
| AzureFunction, Databricks notebook, HDInsight Hive/Pig, Custom (Batch), Azure ML pipeline, Kusto | same Fabric activity, runtime reused (Databricks: the linked service's cluster is copied to the activity, under the ADF names Fabric stores it with) | B |
| Synapse notebook, Spark job, Wrangling data flow | Notebook / Spark job definition / Dataflow Gen2 **scaffold** (item must be migrated) | D |
| Mapping data flow (Execute data flow) | Notebook activity running the notebook translated from the data flow's script (fixed rules; [workload-migration.md](workload-migration.md#mapping-data-flows)), once approved and in the workspace | C |
| SSIS, U-SQL, ML Studio classic, Databricks JAR/Python/Job, HDInsight MapReduce/Streaming/Spark, unknown types | blocked + documented alternatives (Databricks Job: Fabric has the task type but documents only its portal settings) | E |
| Databricks Job whose notebook and Python file tasks were migrated (opt-in) | one Fabric Notebook activity per task, wired as the job's tasks, with job parameters, base parameters and Python arguments (`argv_N`) passed as notebook parameters; only when the job's definition was read and every task's notebook is deployed and bound, else the blocked row above ([workload-migration.md](workload-migration.md#databricks-job-activities)) | C |

## Expressions

Fabric's expression language is nearly identical, so expressions pass through **verbatim** unless a named
rule fires: `pipeline().Pipeline` -> `PipelineName` (ADF: name, Fabric: ID); `globalParameters.X` -> the factory's
Variable Library (see below) or `pipeline().parameters.gp_X`; `dataset().P` -> the activity's argument or the dataset default (datasets are inlined).
`pipeline().DataFactory` is kept but flagged (workspace ID in Fabric). `TriggerType`, `trigger*()`,
`linkedService()` and any function outside Fabric's documented list are **unsupported**: kept as written,
flagged, never replaced by a "similar" function.

## Global parameters: the factory's Variable Library

Fabric has no global parameters; Microsoft's guidance moves them to a **Variable Library**
(`agent/conversion/variable_library.py`, since 2026-11-16). One library per factory, `<factory>_globals`, holds every global
parameter a pipeline can read from it: **String**, **Int** (library type Integer) and **Bool** (Boolean) with a literal value. Each
pipeline that reads one declares it and reads it itself; nothing is passed to child pipelines:

```json
"libraryVariables": {"gp_env": {"type": "String", "variableName": "env", "libraryName": "adf2fabricv1_globals"}}
...  "@concat(pipeline().libraryVariables.gp_env, '-x')"
```

The definition (`variables.json`, `settings.json`) is written to `conversion/variable-library/<name>.VariableLibrary/`. The shape is
what Fabric's getDefinition returned for a pipeline set up that way, and a run of that probe pipeline returned the library's values
(live, 2026-11-16). Then the converted `PL_GlobalParams_Test` was deployed and run twice. In one workspace, deploy created the library;
in another, it used the library that was already there. Both runs returned the ADF baseline (`dev-50000`, `true`). It is not on the DataPipeline definition page, so `libraryVariables` is listed in `fabric_schema.INFERRED`.

Float, Array and Object have no library type a pipeline can read (Microsoft, "Variable library integration with pipelines",
2026-06-22: "Number types aren't supported in pipelines"; there is no array or object type). They go to the library as a **String
holding their compact JSON text** (`0.75`, `["EU","US"]`, `{"mode":"full","retries":3}`: what ADF's `string()` prints), and every
reference is read back through `float(...)` or `json(...)`, so accessors still work:

| ADF | Fabric |
|---|---|
| `@string(mul(pipeline().globalParameters.ratio, 2))` | `@string(mul(float(pipeline().libraryVariables.gp_ratio), 2))` |
| `@pipeline().globalParameters.regions[0]` | `@json(pipeline().libraryVariables.gp_regions)[0]` |
| `@pipeline().globalParameters.config.mode` | `@json(pipeline().libraryVariables.gp_config).mode` |

Run live on 2026-11-18 (`PL_GlobalParams_Types_Test`, eight expressions: arithmetic, index, length, property, `string()` of an
array and an object, interpolation): all eight values equal the ADF run, also with a trailing newline a person typed after the
object's JSON in the library UI. Each value set of the library must keep the value valid for `float()` / `json()`.

An expression value keeps the earlier rule: a `gp_<name>` pipeline parameter with the factory's value as its default, passed down
to child pipelines, as does a global with no value. ADF itself refuses a SecureString global parameter (`UnsupportedTypeSecureString`,
seen 2026-11-18), so none reaches the converter.

`deploy` creates the library before the pipelines. If a library of that name already exists, deploy reads it and uses it when it
has every variable the pipelines read with the same type; its values are the workspace's own and are never changed. If a variable
is missing or has another type, the pipelines that read it are refused. The package carries the library folder too.

## Binding connections and items

Generated JSON carries tokens such as `<connection:LS_SQL>` and `<item:pipeline:PL_Child>`. Fill
`resolutions.template.json` and re-run with `--resolutions file.json`. `LinkedServiceToConnectionId` is
Microsoft's documented upgrader resolution type; `ItemToId` (`"pipeline:Name"`, `"notebook:Name"`,
`"dataflow:Name"`, `"connection:invoke-pipeline"`) and `WorkspaceId` (`"*"`) are this tool's own.

## What validation does and does not prove

The schema table (`fabric_schema.py`) is encoded from Microsoft's documentation (DataPipeline definition,
expression language, activity overview, upgrade guide). "Valid" means consistent with that table, **not**
accepted by a Fabric service. Parts the documentation does not list are declared in `fabric_schema.INFERRED`
and printed in every report. Triggers become Fabric schedule definitions where they map exactly (see below); storage and custom event triggers become an Event Grid subscription + Logic App (an event bridge),
generated when their files now land in a lakehouse and otherwise created from the listed wizard steps ([triggers.md](triggers.md)).

## Integration runtimes and gateways

Semantic discovery reads each integration runtime's own definition and records its kind on the linked services
that use it (`integration_runtime_kind`). Conversion then decides:

| ADF runtime | Fabric requirement |
|---|---|
| none / `AutoResolveIntegrationRuntime` / `Managed` (Azure) | none - a Fabric cloud connection replaces it |
| `SelfHosted` | on-premises data gateway (a `gateway` prerequisite) |
| `Managed` inside a managed virtual network | virtual network data gateway or managed private endpoints (prerequisite) |
| not found, or Azure-SSIS | cautious "verify" prerequisite - never silently ignored |

## Pipeline semantics and graph verification

Activity conversion is only half of accuracy; the other half is how activities interact
(`pipeline_semantics.py`):

* **Outcome notes** (in each pipeline's `notes`): which leaf activities decide the pipeline result, and which
  activities use the documented Try-Catch / Do-If-Else / Do-If-Skip-Else / Upon-Completion patterns. Rule used (ADF
  docs, *Pipeline failure and error message*): every leaf is evaluated, a skipped leaf through its parent, and the
  pipeline succeeds iff all evaluated nodes succeed. Whether Fabric applies the same rule is not verified here.
* **Graph verification**: after assembly, the generated JSON is compared with the source `dependsOn` graph. Errors
  (`graph_activity_missing`, `graph_edge_missing`, `graph_condition_changed`, `graph_edge_added`) make the pipeline
  structurally invalid, so a dropped or re-routed dependency can never be reported as "converted".

## Triggers become schedules (where they map exactly)

Each ADF trigger is turned into Fabric **item schedule** definitions (`POST /v1/workspaces/{ws}/items/{id}/jobs/Pipeline/
schedules`; verified against Microsoft's "Create Item Schedule" page on 2026-10-13) and written to
`conversion/schedules/<trigger>.json`:

| ADF recurrence | Fabric schedule |
|---|---|
| Minute n / Hour n (no schedule block) | `Cron`, interval n / n*60 minutes |
| Hour 1 with `minutes` | `Daily`, times = every hour at those minutes (at most 100) |
| Day 1 (start time, or `hours`/`minutes`) | `Daily`, `times` |
| Day n > 1, no hours/minutes | `Cron`, interval n*1440 |
| Week 1, `weekDays` | `Weekly` (`weekdays`, `times`) |
| Month n, `monthDays` / `monthlyOccurrences` | one `Monthly` schedule per day (`DayOfMonth` or `OrdinalWeekday` First..Fifth) |
| Tumbling window | `Cron` interval, **with a stated difference**, plus portal steps for an interval-based schedule; window parameters read `pipeline()?.TriggerEvent?.windowStartTimeUtc` ([triggers.md](triggers.md#tumbling-window-triggers)) |
| Event triggers | not schedules: an event bridge, Event Grid subscription + Logic App ([triggers.md](triggers.md#storage-and-custom-event-triggers---the-event-bridge-event-grid---logic-app---fabric)) |

Anything else is `manual` with the reason, never approximated: Weekly every N weeks, Daily times on an interval above 1,
"last day of the month" / negative `monthDays`, a `-1` (last) occurrence, more than 100 times, more than 20 schedules per
item. `deploy --schedules` creates them **disabled** (`--enable-schedules` enables only schedules whose ADF trigger was
started), skips one that already exists unchanged, and never deletes anything. Fabric runs a schedule immediately when its
start time is in the past; the notes say so. Trigger parameters are not carried; re-enter them.

## ADF change data capture

An ADF CDC resource (`Microsoft.DataFactory/factories/adfcdcs`) is retrieved by Azure intake (best effort: a factory without
the feature is fine; a real error is reported), described by semantic discovery and listed in the report's compatibility
section with its source, target and tables. Fabric has no CDC artifact: Microsoft's comparison says incremental data
movement moves to **Copy jobs**, which the converter does not generate. Azure Synapse Link is replaced by Fabric Mirroring
per Microsoft, but it is not part of any ADF export, so it cannot be detected (see the playbook).

## Deploying: child pipelines first

`deploy` is the only stage that writes to Fabric (dry run unless `--apply`). An `InvokePipeline` carries
`<item:pipeline:Child>` and `<workspace-id>` placeholders, so the deploy:

1. orders pipelines so every child exists before its parent (transitively; cycles are refused, accurately: members
   as "part of a cycle", callers as "invokes a pipeline that is part of a cycle");
2. creates/updates each child, reads back its Fabric item id, and substitutes it (and the target workspace id) into
   the parent before the parent is sent;
3. refuses a parent whose child is neither selected nor already in the workspace, or whose child was refused or
   failed - other pipelines still deploy;
4. never rewrites a child that already exists in the workspace unless it is selected too.

`--pipeline PL_Parent --with-children` selects a parent plus everything it invokes. Item and workspace ids are
therefore **not** needed in the convert-stage resolutions file for deployment; connections still are (for example
`connection:invoke-pipeline`), because Fabric connections cannot be created by this tool. Anything still unbound
after substitution blocks that pipeline unless `--allow-unbound`.

The Invoke pipeline connection is an OAuth connection of type FabricDataPipelines. It belongs to the tenant, not to a workspace, and
Fabric's API cannot create it. The UI asks for it on the **Connect** step, listing the tenant's connections of that type. Connect's apply
binds it by itself when exactly one exists, and never picks between several (`project/bindings.py`, since 2026-11-16).

## Over Fabric's activity limit: child pipelines (2026-11-17)

Fabric allows 120 activities per pipeline, nested ones included. ADF's own limit is lower, so only conversion pushes a pipeline over it
(a Validation becomes four activities, a Databricks job one notebook per task ...). Then `agent/conversion/split.py` moves the largest
self-contained inner block of a container (a ForEach's or Until's activities, an If branch, a Switch case or default) to a child
pipeline `<pipeline>__<container>__<branch>`, and an Invoke pipeline activity with wait on completion takes its place, until the
pipeline fits. What the block reads from the parent is passed at the call: `item()` (as `item`), `activity('X').output` (as
`out_X`), `variables('v')` (as `var_v`, typed from the variable) and the pipeline parameters it reads (same name, same declaration);
the Variable Library declaration is copied. A block that sets a pipeline variable, reads a run-level value (`pipeline().RunId`,
trigger values), reads `activity('X')` other than its output, or is itself over the limit is not moved, and the
`activity_limit_exceeded` error stays with the reason in the record's notes. The child carries the annotation
`adf2fabric:split-from:<pipeline>`; the record lists it under `split_children`; Deploy always sends it with its parent, children
first. Validation compares the source with the pipeline as authored (`split.merge` puts the child back, parameters replaced by what the
call passed) and the deployment check compares the deployed parent with the file as split, and checks the child is in the workspace.
No real estate reaches the limit yet (largest real pipeline: 19 activities); the Invoke pipeline shape is the one already run live.

## Copy activity log (2026-11-17)

ADF `logSettings` on a Copy activity is carried: `logLocationSettings.linkedServiceName` becomes
`logLocationSettings.externalReferences.connection`, the rest as authored. This is the mapping of Microsoft's own ADF-to-Fabric
upgrade module (github.com/microsoft/FabricPipelineUpgrade, `CopyActivityUpgrader.AddLogSettings`), not a Learn page, so it was run
live before relying on it: a removable pipeline `zz_copy_log_check` in the test workspace copied 10 rows and Fabric wrote the log to
`migrationcontainer/zz-logcheck/logs/copyactivity-logs/...`. `redirectIncompatibleRowSettings`, Copy's `logStorageSettings` /
`sessionLog`, Delete's `logStorageSettings` and Script's `logSettings` stay manual steps: Microsoft's pipeline definition names them
with no inner shape, and the upgrade module does not map them.

## Validation with a dynamic minimumSize (2026-11-17)

A Validation activity's `minimumSize` given as one expression (or one whole-string `@{...}` interpolation, read back with `int()`)
goes through the expression converter and into the Until loop's readiness check (`validation_dynamic_minimum_size_converted`, a
difference to test once). Text around or between interpolations is still a manual step.

## Known limits

* Dataflow / notebook / Spark-job *contents* are not converted — only the pipeline scaffold that calls them.
* Trigger-to-pipeline parameter mappings are not in the semantic record.
* Copy incompatible-row redirection and session log, Delete logging and Script logging must be re-created (undocumented Fabric structure); the Copy activity log itself is carried (above).
* Output field names of Copy, Script and InvokePipeline that downstream expressions read are flagged for verification.
