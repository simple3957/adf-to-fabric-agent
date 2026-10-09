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

## Scope boundary

After metadata is recorded, Intake **hard-stops**. It does not read files, count files,
parse ADF JSON, inspect pipelines/datasets/linked services/triggers, build inventories or
dependency graphs, or perform any migration analysis. Those belong to Discovery and later
stages.
