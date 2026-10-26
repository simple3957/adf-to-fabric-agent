# CLAUDE.md

ADF -> Microsoft Fabric migration agent. It reads an Azure Data Factory estate (Git repo, ZIP or live ARM), converts pipelines to Fabric Data Factory pipeline JSON, assesses compute, and deploys to a Fabric workspace. Deploy is a dry run unless `--apply` is passed.

Start with [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the stage map and [docs/migration-capability.md](docs/migration-capability.md) for what is in and out of scope. Current gaps are in [docs/status-briefing.md](docs/status-briefing.md).

## Run

```bash
pip install -e ".[api]"                 # plus dev extras for tests
python -m pytest -q                     # ~4.5 min; offline, mocked Azure/Fabric
python -m adf2fabric.api                # API on 127.0.0.1:8765
streamlit run ui/app.py                 # UI on http://localhost:8501 (add ?demo=1 for the bundled sample)
adf2fabric --help                       # CLI
```

CLI commands (`src/adf2fabric/cli.py`): `intake`, `consultation`, `brief`, `brief-answer`, `brief-accept`, `brief-import`, `brief-submit`, `brief-approve`, `discovery`, `discovery-adls`, `discovery-adls-paths`, `discovery-sql`, `discovery-sql-structural`, `discovery-adf`, `discovery-adf-semantic`, `discovery-dependencies`, `storage-map`, `workloads-assess`, `workloads-generate`, `workloads-approve`, `workloads-deploy`, `workloads-record-run`, `provision-plan`, `provision-approve`, `provision-apply`, `provision-verify`, `validate`, `auth-user-create`, `auth-user-list`, `auth-user-update`, `auth-user-rotate`, `audit-verify`, `convert`, `reconstruct`, `deploy`, `deploy-notebook`, `git-commit`, `create-shortcut`, `fetch-databricks-notebooks`, `run`, `verify`, `handover`, `pipeline-report`, `compute-deploy`, `assess-compute`. Each takes `--project <name>`; state lives under `projects/<name>/` (see `project/store.py`).

## Layout (`src/adf2fabric/`)

| Path | Role |
|---|---|
| `cli.py`, `config.py` | Commands; loads `config/config.yaml` (default provider, branch, `projects_root`) |
| `agent/intake/` | Source intake (GitHub/Git, ZIP, Azure), source validation, manifest |
| `agent/consultation/` | Readiness gate (`agent.py`), and the post-discovery migration consultation: grounded questions, recommendations, an approved brief that drives the run (`brief.py`, `brief_record.py`, [docs](docs/consultation.md)) |
| `agent/discovery/` | Repository, ADF, semantic parser, ADLS, SQL discovery |
| `agent/dependencies/` | Dependency inventory and migration order from the semantic record + source manifest; opt-in per-provider access checks ([docs](docs/dependency-inventory.md)) |
| `agent/storage/` | Read-only storage map (ADLS Gen2 datasets to OneLake shortcuts and Lakehouse paths, unmappable ones listed for a decision, each with what Fabric offers for its store: shortcut, Copy support, gateway, alternatives; `agent/storage/stores.py`, docs read 2026-10-26) opt-in routing of literal ADLS paths in generated notebooks, and opt-in `convert --lakehouse` repointing of ADLS datasets (CSV/Parquet; Copy, Lookup, GetMetadata) to the Lakehouse, live-verified ([docs](docs/storage-map.md)) |
| `agent/workloads/` | Spark workload migration: retrieve code (only when authorised), analyze, classify, generate Fabric notebook / Spark job definition, manifest ([docs](docs/workload-migration.md)) |
| `agent/provisioning/` | Approval-gated Fabric target provisioning: requirements, read-only state, plan, apply, verify; separate Fabric and ARM transports ([docs](docs/fabric-provisioning.md)) |
| `agent/structure/` | Iterative pipeline composition (activities, nesting, dependencies, calls, datasets, connections) from ADF source and from the deployed Fabric definition, their comparison, and the post-run behaviour report ([docs](docs/pipeline-report.md)) |
| `agent/validation/` | Independent migration validation (static / API / deployment / runtime) with an honest overall status ([docs](docs/migration-validation.md)) |
| `agent/reconstruction/` | Incomplete-pipeline detection and proposals |
| `agent/conversion/` | Activity handler registry (`handlers/`), expression converter, triggers, validation |
| `agent/assessment/` | Compute and capacity assessment |
| `agent/deployment/` | Fabric REST deploy (`fabric_deploy.py`, `compute_deploy.py`) |
| `connections/` | Connector catalog, credential cards, Key Vault, integration-runtime analysis, providers (az, fab, git, zip) |
| `connectivity/` | Real connectors: Azure, ADLS Gen2, SQL Server, Fabric |
| `repository/` | Git providers, scanner, auth |
| `api/` | FastAPI backend: routes, jobs, stages, demo mode; `api/security/` holds authn, roles, tenancy, request protection, audit ([docs](docs/security.md)) |
| `project/` | Persisted models and on-disk store; compute plan |
| `ui/` (repo root) | Streamlit UI, 9 guided steps ([docs](docs/ui.md)). No migration logic; it calls the API via `ui/api_client.py` |

Per-agent operating instructions are in `skills/`. Per-stage references are in `docs/`.

## Conventions and gotchas

- Fabric has no datasets. They are inlined into each activity as `datasetSettings` (`conversion/handlers/_common.py`). Datasets that pass different literal arguments to one parameterised linked service get one connection each (`LS?Param=value`), never one for all; see [docs/datasets.md](docs/datasets.md).
- An unregistered activity type becomes an explicit blocker, never silently dropped. Keep it that way when adding handlers. Likewise an activity nested deeper than discovery reads (30 levels) is blocked as `nesting_too_deep`, and a pipeline whose independent inventory scan disagrees with the parser gets an error finding `source_activities_unaccounted`.
- Discovery accounts for everything it sees: each activity is a record (checked by an independent raw scan, `AdfPipeline.inventory`) and each source artifact is a record or an `unprocessed_artifacts` entry with a reason. A new activity type needs an `ActivitySpec` taken from Microsoft's schema AND a fixture in `tests/fixtures/adf_activities/` (a test enforces both).
- Fabric compute migration of Spark workloads is opt-in and approval-gated; `convert` keeps Databricks as the default. A generated artifact is not a verified one: only `workloads-record-run` sets `execution.status`. Never generate a placeholder for an unconvertible workload.
- Provisioning never writes during discovery, planning or verification; only approved steps of an unchanged plan are applied, every write re-reads live state first, nothing is deleted or modified, and only `provision-verify` may say a resource exists. A read that fails is `unknown`, never "absent".
- `validate` never trusts the converter's success response: it re-reads source and generated files. Only a recorded real run can satisfy the runtime stage, and only `MIGRATION_VERIFIED` may be reported as full success; a clean static pass or a 200 from an API is never runtime correctness.
- API security: every route declares its permission (`need(...)`) and the server enforces it; projects, jobs, uploads and sessions are scoped to the caller's organisation and another organisation's project is a 404; a client-chosen subscription, tenant, account or workspace must be in the caller's grants; Git URLs go through the SSRF guard. A new route needs a permission, an entry in the role table of `tests/test_api_security.py`, and (if it changes anything) an audit event. Do not call the app production-secure: see the limitations in `docs/security.md`.
- UI/API contract ([docs/ui.md](docs/ui.md)): views are redacted server-side by `api/views.sanitize` (no paths, ids, tokens, traces); real changes need `confirm: true` after a dry run and a typed confirmation in the UI; submissions carry an `Idempotency-Key`; a completion claim comes only from the validation engine's overall status; production mode (`ADF2FABRIC_ENV=production`) refuses demo data. A new view or route follows the security rule above.
- The consultation is **simple by default** (`brief.simple()`): only the run-driving questions (`brief.ESSENTIAL`), pre-answered with recommendations, and saving them is the whole step: no submit, no second approver. `ADF2FABRIC_CONSULT=full` restores the full flow (every question, submit, four-eyes approval); the tests run in full mode unless a test sets simple. Provisioning/workload approvals are separate and unchanged. In full mode:
- The consultation brief asks only about what discovery found and never answers for a person; a recommendation is not an answer until accepted. Only an approved brief whose answers (and facts) are unchanged is followed (`brief_record.approved_settings`); its configuration facts are stored with the brief so every viewer sees the same fingerprint.
- The UI follows the Synapse accelerator's frontend (palette, stat strip, wave rows, option cards): Review has a 'Waves & order' tab (`GET /projects/{id}/waves`, built from the dependency inventory by `views.waves`), and Convert, Validate, Deploy and Run-and-compare have options cards (`ui/options.py`) whose defaults come from the approved brief; a person's choice for a run beats the brief (`convert` takes `lakehouse` / `apply_reconstruction` in the stage body: `""`/`false` are choices, absent means follow the brief).
- Usage problems found by exploratory testing (each has a test): an `Idempotency-Key` is bound to the exact request (another stage, project or body under the same key is a 422, never the old job); an approved brief is bound to the Fabric workspace it was agreed for (`deploy --apply`, `deploy_apply` and real runs are refused for another one; the id never appears in a view); a brief that holds back every pipeline must never reach `convert` as an empty list (= all); stored brief answers are re-validated on every read; text typed by another person is shown escaped in the UI; step confirmations are kept per project on the server (`journey.json`), so a reload does not reset the walk-through; the Migration page shows a job that is already running and lets you watch it.
- A source that lacks linked services or datasets ([docs/partial-source.md](docs/partial-source.md)): `preflight` reports `missing_linked_services` / `missing_datasets` with who needs them; a missing dataset blocks its activities, a missing linked service makes them manual (binding a connection in `--resolutions` clears that, as a difference) and blocks Stored procedure / Script, whose store type only the definition gives. Never guess a connection.
- Deploy defaults to dry run. It refuses unbound placeholders (notebook, dataflow, Spark job ids) unless `--allow-unbound`.
- Triggers are created disabled and never duplicated.
- `git-commit` (`agent/deployment/fabric_git.py`) is a dry run unless `--apply`; it commits Selectively (the migration's items only, unless `--all-changes`), never commits a conflict, refuses when the branch has changes the workspace lacks, and never connects a workspace to Git. Not yet run against a Git-connected workspace (none existed on 2026-10-22).
- Starting real runs from the API (`verify` stage with `run: true`) needs `migration:execute`, `confirm: true` and 1-20 named pipelines; it is audited as `verify.run`.
- `agent/conversion/preflight.py` reports source problems conversion cannot fix (reserved SQL aliases from Microsoft's keyword list, linked-service parameters with no value, Wait-only pipelines). They are findings, never blockers.
- Secrets are referenced by env-var name, never stored in project files.
- `verify` (`agent/deployment/verify.py`) compares a Fabric run with the last successful ADF run (or the previous Fabric run) by activity name: status, Copy counts, Lookup results. It stores numbers and hashes, never data values. Without `--apply` it only reads the latest finished runs. `handover` (`agent/handover.py`) turns conversion, source checks and verify results into one readiness level per pipeline.
- Connection auto-creation covers only SQL/Synapse, ADLS Gen2, Blob, REST and Databricks. Others get a verdict but are created by hand.
- Before claiming a Fabric property is valid, check the Fabric pipeline docs. Do not guess properties.
- Smoke-run new analysis stages on the real `projects/` sample, not only synthetic fixtures.
- Tests are offline (local git repos, fake connectors and REST). No test touches a live Fabric workspace, so a fake can hide a rule only the live API enforces (the schedule `endDateTime` was found that way). Check a new Fabric write live, with something disabled and removable, before trusting it.
- An `adf_publish` source (ARM template only) is extracted to `_adf_publish_extracted/` by `agent/intake/arm_template.py`; the clone itself is never modified. Intake with no `--branch` follows the repo's default branch.
- ARM and Fabric hosts come from `src/adf2fabric/cloud.py` (`ADF2FABRIC_ARM_ENDPOINT`, `ADF2FABRIC_FABRIC_ENDPOINT`). Don't hardcode them in new code.
- Do not commit real tenant, workspace or lakehouse GUIDs. The `fabric/` samples use placeholders; keep it that way.
