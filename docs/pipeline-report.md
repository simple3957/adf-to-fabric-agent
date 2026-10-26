# Pipeline report

`adf2fabric pipeline-report --project <name> --workspace-id <GUID> [--workers 8]` answers three questions for every pipeline, read-only
(it only GETs, plus the read-only `getDefinition`; nothing is started or changed). Output: `projects/<name>/pipeline-report/pipeline-report.md`
and `.json`.

1. **What does it consist of?** The ADF source is read iteratively (an explicit stack, never recursion, so any nesting depth is fine):
   every activity with its type, parent container, `dependsOn` edges and conditions, the **pipelines it starts** (`ExecutePipeline`), the
   datasets, linked services and items (notebooks, data flows) it uses. The estate-wide **call graph** lists every pipeline-to-pipeline call.
2. **Does the Fabric twin have the same things?** The deployed pipeline's definition is read back from the workspace and read the same way
   (`InvokePipeline` ids are resolved to pipeline names, inline datasets and connection ids are listed). `agent/structure/compare.py` checks, by
   activity name: every activity present, its parent container, dependency edges with conditions, pipelines started, datasets carried inline,
   the Fabric type is the expected one (from the conversion record), every connection is a real id (no `<connection:...>` placeholder),
   parameters. Differences the converter documents (`ExecutePipeline` -> `InvokePipeline`, datasets inlined, linked services became connections)
   are listed as *documented*; anything else is a *difference*. Verdicts: `equivalent`, `equivalent with documented differences`, `differs`.
3. **How did it behave in the latest Fabric run?** Status, duration, every activity's status, duration, rows read and copied and any error;
   which activities did not execute (a branch not taken); slowest activities across the estate; failed activities. A pipeline another one calls
   once per item (for example a ForEach child) is read as the batch of runs that started together, as `verify` does.

The first section is **Progress**: pipelines deployed, structure equal, latest run completed, matches its ADF baseline (from `verify`), and the
validation engine's overall status.

## What it does not claim

A matching structure says the pieces are carried, not that the pipeline runs correctly: only a run compared with its baseline (`verify`) says that,
and only the validation engine's overall status may say the migration is verified. Names are the identity (ADF requires them to be unique within a
pipeline), so a renamed activity shows as missing plus added. The discovery parser itself still reads nesting to 30 levels and blocks anything
deeper (`nesting_too_deep`); this report's own walkers have no limit. The Fabric reads run in parallel (one definition and one run history per
pipeline, each worker with its own client); the report never writes to Fabric.

Tests: `tests/test_pipeline_structure.py` (iterative order equals the recursive reference, 5000-level nesting, nine kinds of drift each caught and
named, an end-to-end run through a fake workspace including the long-running `getDefinition`, an undeployed pipeline reported, no write calls).
