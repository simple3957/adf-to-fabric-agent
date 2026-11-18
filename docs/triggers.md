# Triggers

Code: `src/adf2fabric/agent/conversion/triggers.py` (schedules), `agent/conversion/event_triggers.py` and
`agent/deployment/activator_deploy.py` (storage events). Tests: `tests/test_triggers.py`, `tests/test_event_triggers.py`.

| ADF trigger | Result |
|---|---|
| Schedule | Fabric Job Scheduler schedule (Cron, Daily, Weekly, Monthly) |
| Tumbling window | Disabled Cron schedule plus the exact portal steps for an interval-based schedule (preview); the pipeline reads the window from it (see below) |
| Storage event (`BlobEventsTrigger`), files now land in a lakehouse | A generated Fabric **Activator** with a OneLake events rule that runs the pipeline (`convert --onelake-trigger`, `deploy --triggers`) |
| Storage event, files still land in Azure storage | `manual`: the exact Fabric Trigger wizard steps (Azure Blob Storage events), plus read-only readiness checks at deploy |
| Custom event (`CustomEventsTrigger`) | `manual`: the two Fabric routes (Event Hub or Event Grid namespace -> Eventstream -> Activator rule) with the exact Event Grid filter that carries the ADF filters |
| Others | `manual` |

Recurrence shapes that cannot be expressed are refused as manual, for example weekly intervals above 1 and negative month days.

`deploy --schedules` creates the schedules disabled and does not duplicate existing ones. See `docs/deploy.md`. Schedules are created
from the command line only: the web UI's Deploy step deploys pipelines, not schedules.

## Storage event triggers -> Activator rules

Fabric starts a pipeline on a file event through an **Activator** (Reflex) rule ([Microsoft: run or trigger a
pipeline](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-runs), updated 2026-09-21). Its definition,
`ReflexEntities.json`, is documented by [Reflex definition](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/reflex-definition)
(updated 2026-02-26); the exact entities a pipeline trigger uses were read back from one made in the Fabric portal on 2026-11-18
(`tests/fixtures/activator/portal_onelake_trigger.json`, ids masked).

**Where do the files land?** That is a person's decision, per trigger:

- **In a lakehouse.** `convert --onelake-trigger TRIGGER=LAKEHOUSE[:Files/<folder>]` (default folder `Files/<container>`). Conversion writes
  `conversion/activators/<trigger>.json` (the plan: OneLake event types, Subject filters, pipelines, notes). `deploy --triggers` (or
  `--triggers-only` for pipelines already deployed) builds the Activator in the workspace: a container, a OneLake events source on the
  lakehouse, an event view, and per pipeline a "run pipeline" action and a rule. Dry run unless `--apply`; the rules are created
  **switched off** (`--enable-triggers` switches on only those whose ADF trigger was started); an Activator with the trigger's name is
  never changed.
- **Still in Azure storage.** `manual`: the report lists the Fabric Trigger wizard steps (Azure Blob Storage events, subscription, account,
  event types, Subject filters), and `deploy --triggers` runs read-only checks: the storage subscription is in the workspace's Entra
  tenant (the wizard lists no other), the account is StorageV2 / BlockBlobStorage / BlobStorage, `Microsoft.EventGrid` is registered,
  and the workspace's capacity region supports Azure Blob events. A read that fails is `unknown`. The Azure Blob events source entity is
  not generated: it is not documented and could not be read here (the test storage was in another tenant). Reading one, once, from a
  same-tenant setup would let the tool generate it too.

`convert --lakehouse` (datasets read through OneLake shortcuts) does **not** move a trigger to OneLake: OneLake raises no event for a file
written to ADLS behind a shortcut (tested 2026-11-18: no run in 6 minutes; a file written to OneLake itself ran the pipeline in 4 seconds).

| ADF | Fabric Activator (OneLake) | Fabric wizard (Azure Blob) |
|---|---|---|
| `Microsoft.Storage.BlobCreated` / `BlobDeleted` | `Microsoft.Fabric.OneLake.FileCreated` / `FileDeleted` | the same event names |
| `blobPathBeginsWith` `/<container>/blobs/<path>` | Subject begins with `/Files/<container>/<path>` | Subject begins with `/blobServices/default/containers/<container>/blobs/<path>` |
| `blobPathEndsWith` | Subject ends with (a `/<container>/blobs/` form also narrows the begins filter) | the same |
| `ignoreEmptyBlobs` | a stated difference: the rule fires for empty files too (`data.contentLength` exists, a filter on it was not verified) | the same |
| Parameters from `@triggerBody().fileName` | a stated difference: the pipeline reads `@pipeline()?.TriggerEvent?.FileName` (documented) | the same |
| Parameters from `@triggerBody().folderPath`, literals | a stated difference: the folder is in the expression builder's Trigger parameters tab (name not checked); literals become defaults | the same |

Validation counts a generated OneLake rule as transformed (it watches OneLake, not Azure storage) and a wizard one as `BLOCKED` until a
person creates it. The live read-back does not list Activators yet.

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

## Custom event triggers -> Eventstream + Activator rule (manual, exact steps)

An ADF custom event trigger listens to an **Event Grid custom topic**. Fabric has no source for a custom topic: its Event Grid source
reads Event Grid **namespace** topics only ([Microsoft](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/add-source-azure-event-grid),
updated 2026-06-30). The conversion report and `conversion/activators/<trigger>.json` give, per trigger:

- **Route A** (the publishers do not change): add an Event Grid event subscription on the topic that delivers to an **Azure Event Hub**,
  with the printed filter. ADF's own trigger is an Event Grid subscription with the same filters, so event types, subject begins/ends
  with and advanced filters carry over exactly (single-value operators such as `NumberGreaterThan` use `value`, list operators
  `values`). In Fabric an Eventstream reads the Event Hub ([Event Hubs source](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/add-source-azure-event-hubs):
  Shared Access Key or workspace identity, JSON).
- **Route B**: move the publishers to an Event Grid namespace topic and use the Eventstream's Event Grid namespace source.
- Then an Activator rule "on each event" on the Eventstream runs the pipeline(s), passing event fields as the parameters the ADF
  trigger set from `@triggerBody().event.data.<key>` / `subject` / `eventType`. Keep it switched off until cutover, then stop the ADF
  trigger so the pipeline does not run twice.

Nothing is generated: the Event Hub, the subscription and the Eventstream are the client's Azure decisions, and an Activator rule on
an Eventstream source has not been read from a portal-made sample yet. Validation keeps the trigger `BLOCKED` until a person creates it.

## What the report tells a person to recreate

Discovery records, for each trigger, the parameters it passes to each pipeline and, for event triggers, what it listens to (scope, events, blob path or subject filters, ignore-empty-blobs). The conversion report prints them under the trigger, so a person can rebuild the Fabric rule or schedule without opening the ADF source. A value that looks like secret material is left out.

A parameter set from `@trigger()`, `@triggerBody()` or `@triggerOutputs()` is flagged: those have no Fabric equivalent. Fabric's `pipeline().TriggerTime` and `pipeline().TriggerName` system variables may cover part of it; for a storage event trigger the pipeline reads the event through `pipeline()?.TriggerEvent`. That is a hint, not a conversion.

Discovery records saved before this was added have no parameter or event details; re-run `discovery-adf-semantic` to fill them in.

## Verified live

On 2026-10-16 a schedule was created in a real workspace (disabled; a rerun left it unchanged). That found a bug the fakes could not: Fabric rejects a schedule without `endDateTime`. See `docs/deploy.md`.

On 2026-11-18, in `fabric-demo`: a OneLake events trigger made in the portal was read back; an Activator created from code in that shape
was accepted (201) and read back unchanged; its rule, on a shortcut folder, did not fire for a file written to the ADLS account behind
the shortcut, while the portal rule fired 4 seconds after a file was written to OneLake. Switching rules off by updating the definition
worked. The tool's own path then ran live the same day: `TR_BlobCreated_Test` read from the factory, `convert --onelake-trigger`,
`deploy --triggers-only --apply` created the Activator switched off (a rerun reported `exists`), the read-back matched what was sent,
and with the rule switched on a new `.csv` in the lakehouse folder ran `PL_GlobalParams_Test` 3 seconds later (Completed); the rule was
switched off again. Not checked: the `pipeline()?.TriggerEvent` property names, the empty-file filter, the Azure Blob source.
