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

A Fabric run that **fails** is compared with ADF's own failed run around the same time. When ADF failed at the same activity with the same message
(ids, codes and spacing ignored), the verdict is `same_failure` ("fails like ADF"): the migration reproduces the source, and the cause is the source's data or its
own check, not the conversion. It is never proof (the runtime stage is `BLOCKED`, not passed): nothing completed, so nothing was compared. A failure ADF does
not reproduce, or one with a different message, stays `failed`, and the detail says so.

Runtime never starts a run. It reads the run-and-compare record written by `adf2fabric verify` / `run --apply`. Each recorded run carries the
workspace it ran in (`workspace`; a run recorded before that belongs to the workspace its pipeline was deployed to): validating another
workspace reports that pipeline's runtime check as NOT_RUN, "was in another Fabric workspace", never as passed. The results, the pipeline
states, the diagnosis, confidence and the notebook run taken at Bind follow the same rule (`verify.current_record(project_dir, workspace)`).
It also refuses a run
older than the generated file it is supposed to vouch for. Each saved run is pinned to a hash of the generated file it ran (only when
that file was saved before the run ended; `verify` adds the pin to older records too). For a pinned run the content decides, not the
save time: converting again to the same output, such as a consultation answer changed and changed back, keeps the run's evidence,
while any real change to the file still blocks it.

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

## A pipeline with no run to compare with (`output-check`)

`verify` compares a Fabric run with an earlier ADF (or Fabric) run. If the factory never ran the pipeline, or ran it on other data, the verdict is `no_baseline` and the runtime check is BLOCKED. `adf2fabric output-check --project P --pipeline NAME --source <abfss URL> --target <abfss URL>` reads the source data and what Fabric wrote (CSV with a header, or Parquet; one file or a folder; ADLS Gen2 or OneLake) and compares the row count, the column names and a content hash that ignores row, column and file order. It is read-only; the record `verification/output-checks.json` keeps counts and hashes, never a value. A check with the same pipeline
and name replaces the earlier one, and the record is written under a lock, so two checks finishing together keep both. The Report step runs the same check through
`POST /projects/{id}/output-checks` ([ui.md](ui.md)); there only ADLS Gen2 and OneLake hosts are accepted.

- `matches`: same rows, columns and content. `counts_match`: same count and columns but a different hash (a value changed, or a number or date was written in another type): look at a sample; it does not vouch. `differs`: rows or columns differ.
- The runtime check of a `no_baseline` pipeline passes only if its run completed and every output check of that pipeline is `matches` and was made after that run. The message says there was no earlier run and that only the checked outputs are covered. It never turns a `differs` or failed run into a pass.
- Limits: you give the source and target locations (paths built from parameters or run ids are not worked out for you); no API route or UI step yet; a source in another tenant needs a login that can read it; it does not see Excel, JSON, SQL tables or Delta tables.

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
