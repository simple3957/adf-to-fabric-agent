# Triggers

Code: `src/adf2fabric/agent/conversion/triggers.py` (schedules), `agent/conversion/event_triggers.py` and
`agent/deployment/event_bridge.py` (event triggers). Tests: `tests/test_triggers.py`, `tests/test_event_triggers.py`.

| ADF trigger | Result |
|---|---|
| Schedule | Fabric Job Scheduler schedule (Cron, Daily, Weekly, Monthly) |
| Tumbling window | Disabled Cron schedule plus the exact portal steps for an interval-based schedule (preview); the pipeline reads the window from it (see below) |
| Storage event (`BlobEventsTrigger`) | An **event bridge**: an Event Grid subscription with the same filters + a Logic App that starts the Fabric pipeline with the same parameters (ARM template from `deploy --triggers`) |
| Custom event (`CustomEventsTrigger`) | The same event bridge, on the custom topic |
| Others | `manual` |

Recurrence shapes that cannot be expressed are refused as manual, for example weekly intervals above 1 and negative month days.

`deploy --schedules` creates the schedules disabled and does not duplicate existing ones. See `docs/deploy.md`. Schedules are created
from the command line only: the web UI's Deploy step deploys pipelines, not schedules.

## Storage and custom event triggers -> the event bridge (Event Grid -> Logic App -> Fabric)

An ADF storage or custom event trigger **is** an Event Grid subscription with filters that delivers to ADF
([storage](https://learn.microsoft.com/en-us/azure/data-factory/how-to-create-event-trigger),
[custom](https://learn.microsoft.com/en-us/azure/data-factory/how-to-create-custom-event-trigger)). The bridge keeps that and changes only the
last hop (the user's decision, 2026-11-19):

```
event -> Event Grid subscription (the ADF trigger's filters) -> Logic App -> Fabric Run On Demand Item Job -> pipeline (the same parameters)
```

- **Filters, exactly.** Storage: `includedEventTypes`, `subjectBeginsWith` `/blobServices/default/containers/<container>/blobs/<path>`
  (from `blobPathBeginsWith` `/<container>/blobs/<path>`), `subjectEndsWith`, the `data.api` operations ADF's own subscription lists, and `ignoreEmptyBlobs` as the advanced filter
  `data.contentLength` > 0. Custom: event types, subject begins/ends with, and the advanced filters (single-value operators take
  `value`, list operators `values`). Event Grid evaluates them, as it did for ADF.
- **Parameters, exactly.** The Logic App computes what ADF passed: `@triggerBody().fileName` / `folderPath` from the event subject,
  `@triggerBody().event.data.<key>` and `.subject` / `.eventType` / `.id` from the event, literals as typed (cast to the pipeline
  parameter's type). The pipeline does not change. An expression it cannot compute (for example `@trigger().startTime`) is named and not
  passed. As in ADF, when a data key a parameter reads is missing, the run fails and no pipeline starts.
- **The call.** `POST {Fabric}/v1/workspaces/{ws}/items/{pipeline}/jobs/instances?jobType=Pipeline` with `executionData.parameters`
  ([Run On Demand Item Job](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job): users, service
  principals and managed identities), one per pipeline the trigger started, retried with back-off. The Logic App answers Event Grid's
  validation handshake itself.
- **Switched off until cutover.** `runPipelines` is false: the Logic App receives and records each event but starts nothing. At cutover
  deploy again with `runPipelines=true` and stop the ADF trigger, so the pipeline does not run twice.
- **Sign-in.** `--bridge-identity managed_identity` (default; Azure and Fabric in one Entra tenant, no secret; give the Logic App's
  identity Contributor on the workspace) or `service_principal` with `--bridge-client-id` (a service principal of Fabric's tenant; its
  secret is a Key Vault reference in the parameters file, never written by the tool). Fabric's tenant setting "Service principals can use
  Fabric APIs" must allow it.

Conversion writes the plan to `conversion/event-triggers/<trigger>.json`; `deploy --triggers` (or `--triggers-only`) writes, per trigger,
`deployment/event-bridges/<workspace>/<trigger>/template.json`, `parameters.json` and a README with the `az deployment group create`
command for the storage account's or topic's resource group. **The tool never deploys it**: the Event Grid subscription and the Logic App
are the client's Azure resources. Validation checks that the plan carries the ADF filters and pipelines.

**Live, 2026-11-19** (`PL_EventParams_Test`, service principal mode, `projects/event-trigger-test/verification/event-bridge`): ADF's own subscriptions were read first and the generated filters match them (the storage one also carries ADF's `data.api` list: CopyBlob, PutBlob, PutBlockList, FlushWithClose, DeleteBlob, CreateFile, DeleteFile). With the bridge off a new file reached the Logic App and started nothing; switched on, a file in `sub folder` gave `folderPath` `migrationcontainer/migration-test/bridge/sub folder` as ADF did, and of two custom events only the one passing `data.amount > 100` ran, with `orderId` and `subj` identical to ADF. The deploy found two Logic App rules now built in: an HTTP action's `retryPolicy` sits inside `inputs`, and a `Terminate` cannot be inside a `For each` (the Logic App handles the one event of each call, `maxEventsPerBatch` 1). Not run live: the missing-data-key branch, and managed identity mode.

Differences that remain: the run shows in Fabric as started on demand by the Logic App's identity; Event Grid delivers at least once (as
for ADF), so an event can rarely arrive twice.

### Seeing it in Fabric: the `Trigger_event` activity

The bridge is not a Fabric item, so Fabric's UI does not list it as a trigger (its definition and history are in the Azure portal: the
storage account's or topic's **Events** blade and the Logic App's **Run history**). So that each run shows in Fabric which trigger and
event started it (the user's request, 2026-11-19), the converter adds to every pipeline a bridged event trigger starts:

- four String parameters `triggerName`, `triggerEventType`, `triggerEventSubject`, `triggerEventTime` (the Logic App fills them),
- a variable and a **`Trigger_event`** Set variable activity with no dependencies (the pipeline's own order is unchanged), marked
  `userProperties` `adf2fabric: trigger_event`. Its value is `<trigger> | <event type> | <subject> | event time <time>`, or "Started by
  hand or by another pipeline (no trigger)".

Validation reports it as a tool addition (transformed), not an extra activity, and `verify` does not count it against the ADF run.
Not added when the pipeline already has one of those parameters or would exceed Fabric's 120 activities. Live 2026-11-19: a file gave
`TR_Blob_Params_Test | Microsoft.Storage.BlobCreated | /blobServices/default/containers/migrationcontainer/blobs/migration-test/bridge/sub folder/zz_bridge_trigger_event_20261119061539.csv | event time 2026-11-19T06:15:42.7421215Z`.

### Why not Activator (tried 2026-11-18, replaced 2026-11-19)

Fabric's own pipeline Trigger button creates an Activator rule. It was built and run live (a OneLake file ran the pipeline in 3 s), but:
OneLake raises no event for files written to ADLS behind a shortcut (tested); the Azure Blob events source lists only subscriptions of
Fabric's tenant (our storage was in another); an Event Grid custom topic is not a source at all; and the pipeline had to change to read
`pipeline()?.TriggerEvent`. The bridge has none of these limits.

## Tumbling window triggers

An ADF tumbling window trigger keeps its timing directly in `typeProperties` (`frequency` Minute/Hour/Month, `interval`,
`startTime`, always UTC) and runs ONE pipeline, under `pipeline` (singular). Until 2026-11-18 the parser read neither, so a real
tumbling trigger was attached to no pipeline; both are read now, with `delay`, `maxConcurrency`, `retryPolicy` and `dependsOn`.

Fabric has **interval-based schedules (preview)** that give the pipeline the window
([Microsoft](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-runs)). Read live on 2026-11-18:

- They are portal-only. Create Item Schedule documents Cron/Daily/Weekly/Monthly only, the pipeline definition's `.schedules`
  part leaves them out, and List Item Schedules answers `400 InvalidConfiguration` for a pipeline that has one.
- They pass no parameters ("Parameters passed: None configured"). The pipeline reads the window itself:
  `@pipeline()?.TriggerEvent?.windowStartTimeUtc` / `windowEndTimeUtc` (what the expression builder's Trigger parameters wrote).

So the tool:

- generates the **disabled Cron schedule** as before, with notes naming what is lost (backfill, window dependencies, delay,
  retries, concurrency above 1) and the exact portal steps for the interval-based schedule (every N, start, end, UTC, max
  concurrency) to create at cutover instead;
- rewrites each use of a parameter the trigger filled with `@trigger().outputs.windowStartTime` / `windowEndTime` to
  `coalesce(pipeline()?.TriggerEvent?.windowStartTimeUtc, pipeline().parameters.<p>)` (end likewise): a run of the interval
  schedule reads the window, a run started by hand still uses the parameter (rerunning a window);
- `deploy --schedules` skips, with the reason, a pipeline whose schedules cannot be listed because it has an interval schedule
  (no duplicate can be ruled out).

Live, 2026-11-18 (`PL_TumblingWindow_Test`, converted by the tool): a 5-minute interval schedule's run gave
`2026-11-18T10:05:00Z` / `2026-11-18T10:10:00Z` (ADF's format; the run fires at the window's end), and a run by hand with the ADF
baseline window gave exactly the ADF values.

## What the report tells a person to recreate

Discovery records, for each trigger, the parameters it passes to each pipeline and, for event triggers, what it listens to (scope, events, blob path or subject filters, ignore-empty-blobs, advanced filters). The conversion report prints them under the trigger. A value that looks like secret material is left out.

A parameter set from `@trigger()` or `@triggerOutputs()` on a schedule is flagged: those have no Fabric equivalent; Fabric's `pipeline().TriggerTime` and `pipeline().TriggerName` system variables may cover part of it. For event triggers the bridge computes `@triggerBody()` values itself.

Discovery records saved before this was added have no parameter or event details; re-run `discovery-adf-semantic` to fill them in.

## Verified live

On 2026-10-16 a schedule was created in a real workspace (disabled; a rerun left it unchanged). That found a bug the fakes could not: Fabric rejects a schedule without `endDateTime`. See `docs/deploy.md`.

History (the replaced Activator form): on 2026-11-18, in `fabric-demo`, a OneLake events trigger made in the portal was read back; an Activator created from code in that shape
was accepted (201) and read back unchanged; its rule, on a shortcut folder, did not fire for a file written to the ADLS account behind
the shortcut, while the portal rule fired 4 seconds after a file was written to OneLake. Switching rules off by updating the definition
worked. The tool's own path then ran live the same day: `TR_BlobCreated_Test` read from the factory, `convert --onelake-trigger`,
`deploy --triggers-only --apply` created the Activator switched off (a rerun reported `exists`), the read-back matched what was sent,
and with the rule switched on a new `.csv` in the lakehouse folder ran `PL_GlobalParams_Test` 3 seconds later (Completed); the rule was
switched off again. Not checked: the `pipeline()?.TriggerEvent` property names, the empty-file filter, the Azure Blob source.
