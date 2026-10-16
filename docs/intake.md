# Intake capability

The Intake Agent is the first stage of the ADF → Fabric migration system. It establishes
the **source of truth** for a migration project: it authenticates with the source
provider, obtains the requested repository/branch, stores a canonical local copy, and
records minimal metadata. Then it **stops**.

Intake treats the repository as an **opaque source artifact**. It does not read, count,
parse, classify, or otherwise understand the contents — that is Discovery's job in a
later, separate stage.

## Inputs

| Input           | Required | Notes                                            |
|-----------------|----------|--------------------------------------------------|
| `project_name`  | yes      | Human-readable name; slugified into the project id. |
| `repository_url`| yes      | Source repository URL (GitHub).                  |
| `branch`        | no       | Defaults to `main` (see `config/config.yaml`).   |
| `provider`      | no       | Defaults to `github`.                            |
| `refresh`       | no       | Force a fresh fetch even if a local source exists.|

Authentication is supplied out-of-band via the `GITHUB_TOKEN` environment variable — it
is never a request field.

## Flow

```text
resolve token → select provider → validate (accessibility + branch)
             → clone (or reuse existing) → scrub credentials
             → capture commit SHA → record metadata → STOP
```

1. **Select provider** and normalize the repository URL to a canonical, token-free form.
2. **Reuse check** — if `projects/<id>/source/` already holds this repo + branch (and
   `refresh` is not set), reuse it: no clone, no authentication needed.
3. **Validate** — `git ls-remote` confirms the repository is reachable, the token is
   valid, and the branch exists, before any clone.
4. **Clone** the single branch into `projects/<id>/source/`.
5. **Scrub** the credential from the clone's `.git/config` (reset `origin` to the
   token-free URL).
6. **Capture** the HEAD commit SHA.
7. **Record** metadata to `projects/<id>/project.yaml`.

## Canonical local source

```text
projects/
└── <project-id>/
    ├── project.yaml
    └── source/            ← canonical local copy; the source of truth for later stages
```

Future stages (Discovery onward) consume `projects/<id>/source/` directly. They do not
re-authenticate or re-clone unless a deliberate refresh is requested. This is the whole
point of persisting the source: **one project → one canonical local source repository.**

## Metadata schema

`project.yaml` records only intake-level fields (no secrets):

```yaml
id: acme-orders
name: Acme Orders
created_at: 2026-09-18T12:00:00+00:00
repository:
  provider: github
  repository_url: https://github.com/acme/adf-orders.git   # token-free
  branch: main
  commit_sha: <40-char sha>
  fetched_at: 2026-09-18T12:00:00+00:00
  local_source_path: .../projects/acme-orders/source
```

`local_source_path` is the canonical source consumed by subsequent stages.

## Branch and `adf_publish` sources

With no `--branch` (and no `default_branch` in `config/config.yaml`), intake fetches the repository's own default branch. Name a branch to override it.

An `adf_publish` branch holds `ARMTemplateForFactory.json` (and `linkedTemplates/`) instead of `pipeline/`, `dataset/` and so on. Intake extracts those resources into the Git layout in `_adf_publish_extracted/` beside the template and uses that folder as the source root. The cloned files are not changed. A value that is exactly `[parameters('x')]` takes the template's default when it has one; secure parameters and other ARM expressions stay as written and are reported in the manifest findings.

## Reuse behavior

- **Same project + repo + branch, already fetched** → reused (no clone, no token needed).
- **Different repo/branch for the same project** → the old `source/` is replaced with a
  fresh clone.
- **`refresh=True`** → always re-fetches.

The reuse logic is intentionally simple: it is not a synchronization engine.

## Errors

Intake returns a clear success/failure result (the CLI exits non-zero on failure):

- Missing token → actionable error naming the environment variable.
- Invalid/unsupported provider → "not implemented" for future providers.
- Invalid repository URL → rejected before any network call.
- Repository unreachable / branch not found → validated up front via `git ls-remote`.

## Security

- The token is read only from the environment and used only for the git operation.
- It is **never** written to `project.yaml`, metadata, logs, skill files, or `.git/config`
  (the remote URL is scrubbed to a token-free form after cloning).
- Any command output surfaced in an error is redacted so an embedded token cannot leak.

## Source modes

Intake normalizes every source into the same place — `projects/<id>/source/` in the ADF
Git folder layout (`pipeline/`, `dataset/`, `linkedService/`, `trigger/`, `factory/` ...)
— so Discovery and the single `parser_pipelines` interface never care where it came from.

| Mode | Command | Source of truth |
|------|---------|-----------------|
| B: GitHub | `adf2fabric intake --project P --repo URL [--branch B] [--subdirectory DIR] [--pipeline NAME]...` | the cloned branch; commit SHA recorded |
| A: Azure  | `adf2fabric intake --project P --factory F --resource-group RG [--subscription S] [--pipeline NAME]... [--artifact-kind K]... [--auth azure_cli] [--allow-partial]` | live ARM `GET`s; the export is committed locally and that SHA recorded |

Exactly one of `--repo` / `--factory` is required. Local JSON directories and ZIP inputs
were not provided by the project before this work and are still not supported.

**Azure mode authentication:** `--auth` accepts `azure_cli` (default), `client_secret`, `client_certificate`,
`managed_identity`, `interactive_browser`, `device_code`, `access_token` and `azure_identity`, each with the
`--*-env` options naming the environment variables it reads (see the README's Authentication table). Storage-only
mechanisms (`account_key`, `sas_token`, `connection_string`) are not valid here: ARM needs a Microsoft Entra token.

**Azure mode** reuses your `az login` session (via `azure-identity`, or the `az` CLI
directly when that extra is not installed). It issues only `GET`s: the factory (global
parameters) plus pipelines, datasets, linked services, triggers, data flows and integration
runtimes, following `nextLink` paging and retrying 429/502/503/504 (honouring
`Retry-After`, exponential backoff otherwise). Definitions are stored as Azure returned
them, except secret **values** (SecureString and credential-named fields), which are masked
and listed in the manifest by location. The export is built in a staging directory and only
replaces `source/` when complete, so a failed retrieval never destroys a good source. Any
failed collection fails the intake with an actionable message unless `--allow-partial`.

**Pipeline selection** (`--pipeline`, either mode) keeps the pipelines plus everything they
reference (datasets, linked services, executed pipelines, data flows, integration runtimes)
and the triggers that start them. Discovery honours it.

## Source manifest

`projects/<id>/source-manifest.yaml` records how the source was obtained: `source_type`,
`source_identifier` (token-free URL / `azure://` resource id), `branch`, `subdirectory`,
`version` (commit SHA), `content_sha256`, `retrieved_at`, the Azure factory identity, the
selection, every artifact (path, hash, ARM etag), every retrieval `failure`, every
`redaction` (location only) and integrity `findings`. It never holds a token or secret.

Findings (shared by both modes, each with an `action`): `invalid_json`,
`duplicate_artifact` (names are case-insensitive), `missing_reference`, `no_adf_artifacts`,
`no_pipelines`, `arm_template_only` (warning), `retrieval_failed`. Errors set
`complete: false`; the source is still stored so it can be inspected.

## Scope boundary

Intake does not parse activities, build inventories or dependency graphs, or analyse the
migration. It does now perform a shallow, read-only **integrity check** of the ADF artifact
files (valid JSON, duplicates, dangling references) so an incomplete export is caught at
the door. Everything semantic still belongs to Discovery and later stages.
