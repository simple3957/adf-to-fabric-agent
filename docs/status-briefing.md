# Status briefing (2026-10-16)

Based on a read-only code audit plus one full test run: **1068 passed, 1 skipped** (about 4.5 min). All tests are offline and mocked. No stage was run against a live Fabric workspace in this audit.

## Where we are

| Area | Status | Note |
|---|---|---|
| Dynamic discovery | Mostly done | Subscription, factory, repo URL, branch, token and workspace are all inputs. No tenant, subscription, factory or domain is hardcoded in `src/`. |
| Pipelines | Partial | About 35 activity types handled. SSIS, U-SQL and AzureML classic are blocked on purpose. Some properties are not carried over (Copy `logSettings`, Validation retry). |
| Datasets | Done | Inlined as `datasetSettings`; missing or inline datasets raise blockers. |
| Linked services / connections | Partial | About 55 types have a verdict. Auto-create for 5: SQL/Synapse, ADLS Gen2, Blob, REST, Databricks. OAuth and gateway connections are manual. |
| Triggers | Partial | Schedule and tumbling-window convert. Event and custom-event triggers are manual. Created disabled. |
| Data flows | Partial | Scaffold activity only; the transformation graph is not converted. |
| Integration runtimes, VNet | Analysis only | Classified and advised; nothing provisioned. |
| Compute | Partial | Plan, assessment and pool/environment deploy exist. Notebook and Spark job content are not migrated. |
| Deploy | Partial | Pipelines, schedules and compute only. Notebooks, Spark job definitions, dataflows and connections are not pushed by `deploy`. |
| Post-deploy run check | Missing in the tool | No job-instance trigger or run verification in code. Earlier session notes record nine pipelines deployed and run by hand on 2026-10-12 in the test workspace, so the converter has been proven live, but not through a repeatable command. |

Compute is built but not finished.

## Gaps in dynamic discovery

Fixed on 2026-10-16 (offline, covered by tests; 1091 passed, 1 skipped):

1. `adf_publish` is now read. An `ARMTemplateForFactory.json` (plus `linkedTemplates/`) is extracted into the Git layout in a generated `_adf_publish_extracted/` folder next to it, and that folder becomes the source root. Parameter defaults are applied; secure parameters and other ARM expressions stay unresolved and are reported in the manifest.
2. The branch is no longer assumed to be `main`. With no `--branch` and no `default_branch` in config, intake follows the repository's own default branch (`git ls-remote --symref`).
3. The ARM and Fabric hosts come from `ADF2FABRIC_ARM_ENDPOINT` and `ADF2FABRIC_FABRIC_ENDPOINT` (default: public cloud). Storage endpoint suffixes (`dfs.core.windows.net`) are still public-cloud only.
4. The workspace, lakehouse and notebook GUIDs in `fabric/` are now placeholders.

Still open: GUIDs remain in Git history; a subscription and tenant id in `docs/linked-service-mapping.md` (line 93) were left for you to decide; the sovereign-cloud endpoints themselves are not verified, and none are assumed.

## Documentation state

- Now present: `CLAUDE.md`, `docs/ARCHITECTURE.md`, `deploy.md`, `datasets.md`, `triggers.md`, `auth.md`, `testing.md`, and this file. They were written from the audit plus spot checks, so skim them before relying on them.
- `docs/pipeline-parser.md` is current: `parser_pipelines.py` exists at `agent/discovery/adf_semantic/` (an earlier audit note calling it stale was wrong). The doc only uses the short path.
- Draft: `docs/linked-service-mapping.md` (marked blocked on Fabric access).
- Still without its own doc: the API layer internals.

## What we need

- A Fabric capacity and workspace we can run against repeatedly.
- Permission to create connections (service principal or user).
- A customer-like ADF sample, including one with an `adf_publish` branch.
- Databricks and Synapse access if compute sources are to be read live.
- A decision on event triggers and data flows: convert, or flag as manual.

## Proposed order of work

1. Live end-to-end run: deploy one converted pipeline, trigger it, read back the result.
2. More connector types for auto-create, verified live.
3. Deploy notebooks, Spark job definitions and dataflow scaffolds.
4. Decide triggers and data flows.

Done offline on 2026-10-16: `adf_publish` parsing, default-branch detection, configurable endpoints, GUID cleanup (see above).

## Questions for the call

- Is a live Fabric run in scope for the next milestone, and who provides the capacity?
- Are customer estates expected to use `adf_publish`, or the collaboration branch?
- Is a manual fallback acceptable for OAuth/gateway connections and event triggers?
