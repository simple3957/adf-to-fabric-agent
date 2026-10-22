# Migration validation

`adf2fabric validate --project <name> [--stage static|api|deployment|runtime|all] [--workspace-id GUID] [--runtime]` checks the
GENERATED artifacts against the SOURCE. It does not use the converter's success response: it reads the raw source JSON and the raw
generated JSON itself (`agent/validation/views.py`), and it never writes to Fabric (every call is a GET, plus the read-only
`getDefinition`). Output: `projects/<name>/validation/validation.yaml` and `report.md`.

## Results

Every applicable check ends in exactly one of **PASS**, **FAIL**, **BLOCKED** (something it needs is unbound, unreadable or not yet
produced) or **NOT_RUN** (not attempted: no credentials, stage not requested). Each carries evidence and remediation. Informational
checks are marked and never decide a stage or the overall status.

## Stages (kept apart)

| Stage | Needs | Proves | Does not prove |
|---|---|---|---|
| static | files only | the generated files are consistent with the source and the documented schema | that Fabric accepts them, or that they run |
| api | `--workspace-id` + read access | the target answers; the workspace is readable and on a capacity; bound connection and item ids exist | that they work |
| deployment | same | each pipeline is deployed and its definition, read back with `getDefinition`, equals the generated file (what deployment itself substitutes is applied first); schedules are present | behaviour |
| runtime | `--runtime` and a recorded run | a real run completed and matched its baseline (status, copy counts, lookup results) | anything about pipelines never run |

Runtime never starts a run. It reads the run-and-compare record written by `adf2fabric verify` / `run --apply`, and refuses a run
older than the generated file it is supposed to vouch for.

## The ten categories

1 source inventory (readable, manifest, an independent cross-check of discovery's counts); 2 reference integrity (source
references resolve; generated pipelines hold no unbound placeholders and resolve their dependencies, parameters, variables and
`activity()` reads); 3 target schema (documented Fabric schema, schedules); 4 parameters, variables and expressions (global
parameters normalise to `gp_<name>`; dropped expressions fail, rewritten ones are listed; a dataset parameter the dataset never
reads is ignored); 5 activity count, nesting and dependency-graph equivalence (conditions included; a placeholder `Fail` stand-in is
BLOCKED, never matched); 6 linked services, datasets, connections and secrets (plaintext secrets in generated files fail without
printing the value; each used linked service must be bound); 7 compute (a generated Fabric notebook / job must have a migration
record, no open issues and an approval; provisioning verification); 8 execution semantics (retry/timeout, ForEach, Switch, Copy
types, triggers); 9 deployment; 10 runtime.

## Inventory comparison

Stable ids: `pipeline:P`, `pipeline:P/activity:A`, `dataset:D`, `linked_service:L`, `trigger:T`, `global_parameter:G`,
`integration_runtime:I`, `dataflow:F`. Statuses: **matched**, **transformed** (differs in a documented way, for example
`ExecutePipeline -> InvokePipeline`, a dataset inlined, a linked service became a connection), **altered** (undocumented
difference), **missing**, **extra** (for example a stale generated pipeline whose source was removed), **unverified**,
**not_applicable**. Diffs are flattened and sorted (key order is not a difference) and redacted by key name and by value shape.

## Overall status

`FAILED` (a required check failed, or a source component is missing) > `BLOCKED` > `NOT_VALIDATED` (a required check not run, a
component altered / unverified / extra, a stage not requested, or no runtime check passed) > `MIGRATION_VERIFIED`. Only the last sets
`full_success_claimed`, and only when static, API, deployment and runtime all ran and every required check passed.

## Tests

`tests/test_migration_validation.py`: a cleanly converted baseline validates statically; mutation tests edit the generated files
(drop an activity or expression or parameter, change a dependency, loop setting or schedule, inject a secret, unbind a connection,
swap in a placeholder) and assert the right check turns FAIL / BLOCKED; a fake API records every request; runtime evidence cases;
overall-status rules. Opt-in live, read-only: `ADF2FABRIC_LIVE_VALIDATION=1 ADF2FABRIC_LIVE_PROJECT=<id> ADF2FABRIC_LIVE_WORKSPACE=<guid>`.

## Limits

The expression and semantics checks compare text and settings; they cannot prove two expressions mean the same. `getDefinition` for
data pipelines was confirmed live (read-only) on 2026-10-22. Connection credentials are never read: only their type. Event
triggers are BLOCKED by design (no Fabric pipeline schedule). Static identity of activities is by name, which ADF requires to be
unique within a pipeline.
