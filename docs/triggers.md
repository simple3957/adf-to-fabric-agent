# Triggers

Code: `src/adf2fabric/agent/conversion/triggers.py`. Tests: `tests/test_triggers.py`.

| ADF trigger | Result |
|---|---|
| Schedule | Fabric Job Scheduler schedule (Cron, Daily, Weekly, Monthly) |
| Tumbling window | Interval schedule, with a note that window semantics (dependencies, backfill) are lost |
| Event, blob event, custom event, others | `not_supported` or `manual` |

Recurrence shapes that cannot be expressed are refused as manual, for example weekly intervals above 1 and negative month days.

`deploy --schedules` creates the schedules disabled and does not duplicate existing ones. See `docs/deploy.md`.

## What the report tells a person to recreate

Discovery records, for each trigger, the parameters it passes to each pipeline and, for event triggers, what it listens to (scope, events, blob path or subject filters, ignore-empty-blobs). The conversion report prints them under the trigger, so a person can rebuild the Fabric rule or schedule without opening the ADF source. A value that looks like secret material is left out.

A parameter set from `@trigger()`, `@triggerBody()` or `@triggerOutputs()` is flagged: those have no Fabric equivalent. Fabric's `pipeline().TriggerTime` and `pipeline().TriggerName` system variables may cover part of it. That is a hint, not a conversion.

Discovery records saved before this was added have no parameter or event details; re-run `discovery-adf-semantic` to fill them in.

## Verified live

On 2026-10-16 a schedule was created in a real workspace (disabled; a rerun left it unchanged). That found a bug the fakes could not: Fabric rejects a schedule without `endDateTime`. See `docs/deploy.md`.
