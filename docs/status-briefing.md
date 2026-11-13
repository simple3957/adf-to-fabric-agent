# Status briefing (2026-11-04)

Replaces the briefing of 2026-10-16, which predated `verify`, `deploy-notebook`, `git-commit`, provisioning, workloads, validation and
`output-check`. Command list: CLAUDE.md. What a person must supply, and where: [manual-inputs.md](manual-inputs.md).

## Proven live

On the real sample project `adf-data-migration-7593` (10 pipelines, 56 activities), moved to the Fabric workspace `Practise` on
2026-11-03/04: all 10 pipelines converted, deployed and run. 8 match their last successful ADF run (status, copy counts, lookup results);
`PL_Rig_Orchestrator` and `PL_Rig_Child_FileToParquet` match their previous Fabric run, because the ADF baseline was taken on other data
and the ADF original now fails on the 5,000-row Lookup cap. Validation: API, deployment and runtime PASS; static BLOCKED on two notebook
records (below). Also live: notebook deploy, schedules, a Spark pool and an environment created by provisioning, OneLake shortcuts,
Lakehouse repointing of CSV/Parquet datasets.

On 2026-11-13, project `adf2fabricv1`, workspace `fabric-demo`: the Databricks job behind `PL_DatabricksJob_Params_Test` (job parameter
`run_date` passed by the ADF activity, a notebook task with base parameter `min_amount`, then a Python file task with two arguments)
was read with `jobs/get` (az login, Entra ID token), its two tasks generated as notebooks, approved by a person, created in the
workspace by the Deploy step and converted to two Notebook activities with their parameters. The Fabric run completed and both
outputs equal the Databricks run's row for row (4 rows; 3 product totals). `verify` reports `no_baseline` for it: the ADF run has one
activity (`Run_Param_Job`) and the Fabric run two, so nothing matches by name; the proof was the outputs, compared by hand.

## Where each area stands

| Area | Status | What is missing |
|---|---|---|
| Discovery (Git, ZIP, live ARM, `adf_publish`) | Done | Sovereign-cloud storage suffixes are public-cloud only, and the endpoints are untested |
| Activities | Done for about 35 types | SSIS, U-SQL, AzureML classic blocked on purpose; `SqlPoolStoredProcedure` converts to a Fabric Stored procedure (connection bound by pool name), other Synapse-only types have no rule |
| Datasets | Done | Inlined as `datasetSettings` |
| Data flows | Scaffold | `ExecuteDataFlow` becomes a Dataflow Gen2 refresh; the flow itself is not read or converted |
| Triggers | Partial | Schedule and tumbling window convert (created disabled); event and custom-event triggers are manual |
| Connections | Partial | Auto-create for SQL/Synapse, ADLS Gen2, Blob, REST, Databricks; about 50 other types get a verdict only; OAuth and gateway are manual |
| Storage | Partial | ADLS Gen2 shortcuts and CSV/Parquet repointing; other stores, `wasbs://` and computed paths stay as written |
| Spark workloads | Built, CLI only | Generate, approve, deploy, record-run for Databricks notebooks, and for the notebook and Python file tasks of a Databricks Job activity (job read with `jobs/get`, live 2026-11-13; job parameters, base parameters and Python arguments mapped to notebook parameters, run live in Fabric 2026-11-13 with outputs equal to Databricks; other task types stay blocked); no custom jar/wheel libraries |
| Run and compare of a split Databricks job | Gap | `verify` matches activities by name, so a job activity converted to several task activities reports `no_baseline` |
| Compute (pools, environments) | Built | `compute-deploy` creation not tested (its fake-only test was removed 2026-11-11; provisioning created the same items live) |
| Provisioning | Partial live | Pool and environment live; workspace, capacity, roles, lakehouse, ARM capacity never run live, and their fake-only tests were removed 2026-11-11 |
| Run and compare (`verify`) | Done, live | Compares by activity facts; needs a baseline run |
| Output check (`output-check`) | Built 2026-11-04; on the Report step since 2026-11-05 | For pipelines with no baseline; CSV/Parquet; the paths are given by hand |
| Validation | Done, live | |
| `git-commit` | Built | Never run against a Git-connected workspace (none exists) |
| CDC | Detected | No Fabric Copy job generated |
| UI | 9 steps | No screen for workloads, compute deploy, shortcuts, notebook deploy, git-commit (see [ui.md](ui.md)); diagnose and output-check are on the Report step |

## Open on the sample project

- The two notebooks in `Practise` were regenerated for its lakehouse (identical code to what is deployed and ran). They are not bound yet:
  a person binds them on the UI's Deploy step (Check / Bind, added 2026-11-04), then the static stage can pass.
- The ADF original of `PL_Rig_Child_FileToParquet` checks a Lookup count against rows copied, which fails above 5,000 rows. The project's
  converted copy compares Copy `rowsRead` with `rowsCopied` instead; ADF itself is unchanged by agreement.

## Known issues

- None open. `workloads-generate` run without the code used to replace a generated, approved record with `blocked`; since 2026-11-05 it
  keeps the earlier record and says so ([workload-migration.md](workload-migration.md)).

## Known test issue

`tests/test_hardening.py::test_finished_jobs_are_still_listed_after_a_restart` failed once in a full run and passes alone; not yet explained.
