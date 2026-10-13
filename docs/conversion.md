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
| Web, WebHook | same (URL split into connection + `relativeUrl`; dynamic URL -> manual) | B |
| AzureFunction, Databricks notebook, HDInsight Hive/Pig, Custom (Batch), Azure ML pipeline, Kusto | same Fabric activity, runtime reused | B |
| Synapse notebook, Spark job, Mapping/Wrangling data flow | Notebook / Spark job definition / Dataflow Gen2 **scaffold** (item must be migrated) | D |
| SSIS, U-SQL, ML Studio classic, Databricks JAR/Python, HDInsight MapReduce/Streaming/Spark, unknown types | blocked + documented alternatives | E |

## Expressions

Fabric's expression language is nearly identical, so expressions pass through **verbatim** unless a named
rule fires: `pipeline().Pipeline` -> `PipelineName` (ADF: name, Fabric: ID); `globalParameters.X` ->
`pipeline().parameters.gp_X` (global parameters do not exist; defaults are carried and passed down to child
pipelines); `dataset().P` -> the activity's argument or the dataset default (datasets are inlined).
`pipeline().DataFactory` is kept but flagged (workspace ID in Fabric). `TriggerType`, `trigger*()`,
`linkedService()` and any function outside Fabric's documented list are **unsupported**: kept as written,
flagged, never replaced by a "similar" function.

## Binding connections and items

Generated JSON carries tokens such as `<connection:LS_SQL>` and `<item:pipeline:PL_Child>`. Fill
`resolutions.template.json` and re-run with `--resolutions file.json`. `LinkedServiceToConnectionId` is
Microsoft's documented upgrader resolution type; `ItemToId` (`"pipeline:Name"`, `"notebook:Name"`,
`"dataflow:Name"`, `"connection:invoke-pipeline"`) and `WorkspaceId` (`"*"`) are this tool's own.

## What validation does and does not prove

The schema table (`fabric_schema.py`) is encoded from Microsoft's documentation (DataPipeline definition,
expression language, activity overview, upgrade guide). "Valid" means consistent with that table, **not**
accepted by a Fabric service. Parts the documentation does not list are declared in `fabric_schema.INFERRED`
and printed in every report. Triggers become Fabric schedule definitions where they map exactly (see below); event triggers are recreated by hand.

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
| Tumbling window | `Cron` interval, **with a stated difference** (no window start/end, backfill, dependencies) |
| Event triggers | not generated (Activator rules have no schedule API) |

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

## Known limits

* Dataflow / notebook / Spark-job *contents* are not converted — only the pipeline scaffold that calls them.
* Trigger-to-pipeline parameter mappings are not in the semantic record.
* Copy logging / fault-tolerance logging and Script logging must be re-created (undocumented Fabric structure).
* Output field names of Copy, Script and InvokePipeline that downstream expressions read are flagged for verification.
