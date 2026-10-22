# Dependency inventory

`adf2fabric discovery-dependencies --project <name>` turns the semantic discovery record and the source manifest into one
inventory of everything the estate depends on, saved as `projects/<name>/discovery/dependencies.yaml` plus a readable
`dependency-report.md`. Run `discovery-adf-semantic` first.

## Pieces

| Module | Role |
|---|---|
| `project/dependencies.py` | Persisted contract (`dependency-inventory/1`). Cannot hold a secret or source data. |
| `agent/dependencies/builder.py` | Pure `build_inventory(discovery, manifest, checks)`. No network, no files, no clock but the one passed in. |
| `agent/dependencies/access.py` | Opt-in, per-provider, least-privilege probes. |
| `agent/dependencies/report.py` | Markdown report. |
| `agent/dependencies/discoverer.py` | Loads records, runs checks, saves. |

Input adapters (live Azure through `az`, GitHub/Git with a branch and commit SHA, ZIP) all end as `projects/<name>/source/`
plus a `SourceManifest`; the engine reads only those, so the inventory for the same estate is identical whichever adapter
produced it (tested).

## What each dependency records

Type, name, resolution, defining file, every reference (consumer, role, file, JSON pointer), unique consumers,
authentication (mode and field names only), migration implication (category, actions, whether it blocks), access check,
issues. Types: pipeline, dataset, linked service, integration runtime, data flow, trigger, global/pipeline parameter,
variable, activity output, Key Vault secret, Databricks notebook, Synapse item, SQL object, storage path, REST endpoint,
event source.

Resolution: `resolved`, `unresolved` (no definition: missing file or broken reference), `unavailable` (source could not be
fully retrieved, so it may exist), `dynamic` (computed by an expression), `external`, `circular`.

Findings: `missing_file`, `broken_reference`, `source_unavailable`, `unresolved_expression`, `unsupported_connection_type`,
`inaccessible_external`, `circular_dependency`, `source_gap`. Source gaps name the precise permission when retrieval was
denied (for example `Microsoft.DataFactory/factories/datasets/read`).

## Ordering

`pipeline_order` lists pipelines children first (cycle members share a position), with `ready` false when the pipeline or any
pipeline it calls consumes a blocking dependency. `waves`: foundation, prerequisites outside ADF, connections, definitions,
items to recreate (flowlets first), pipelines by call depth, triggers.

## Access checks

Off by default: nothing external is contacted and every target is `not_checked`. `--check <provider>` (repeatable) probes:

| Provider | Probe | Never does | Token |
|---|---|---|---|
| `databricks` | `workspace/get-status` on the notebook path | export notebook source | env var (default `ADF2FABRIC_DATABRICKS_TOKEN`, or `--databricks-token-env NAME`) |
| `key_vault` | list versions of one secret | read the value | `--keyvault-token-env NAME` |
| `storage` | HEAD container properties | list or read blobs | `--storage-token-env NAME` |
| `network` | DNS + TCP connect to linked-service hosts | send data, log in | none |

An `az` login is not assumed to grant any of these: with no token for a provider the status is `missing_credential` and the
detail names what to set. `--allow-az-tokens` lets a check request an `az` token for that provider's own audience; the
result still reflects what that identity may do. Statuses: accessible, denied, unauthenticated, not_found, unreachable,
missing_credential, missing_dependency, not_configured, error. SQL logins are not attempted (only the network path).

## Limits

Network probes run from this machine, so a host reached through a self-hosted runtime may show `unreachable` here and still
work in ADF. Storage accounts and SQL servers that come from connection strings are not known (those are never read).
