# ADF → Microsoft Fabric Migration Agent

An agent that migrates an **Azure Data Factory (ADF)** estate into **Microsoft Fabric
Data Factory**.

The stages that exist today turn an Azure Data Factory estate into Fabric pipeline definitions
(intake -> discovery -> parse -> convert -> deploy). The broader lifecycle below is the product
direction; [Implementation Status](#implementation-status) states exactly what is built.
Everything is deterministic: **no LLM or generative model is used anywhere.**

---

## Product vision (future — not all implemented)

The eventual system follows this migration lifecycle:

```text
New Project → Consultation → Discovery → Assessment → Planning
           → Architecture → Engineering → Generation → Validation → Deployment
```

Each stage has a distinct responsibility:

| Stage         | Responsibility (intended)                                            |
|---------------|----------------------------------------------------------------------|
| New Project   | Register an engagement and its source.                               |
| Consultation  | Capture scope, contacts, and engagement decisions.                   |
| Discovery     | Read the source repository and understand what ADF objects exist.    |
| Assessment    | Classify objects; identify native / hybrid / custom migration paths. |
| Planning      | Sequence the migration work and dependencies.                        |
| Architecture  | Design the target Fabric layout.                                     |
| Engineering   | Build the conversions and supporting components.                     |
| Generation    | Produce the Fabric artifacts.                                        |
| Validation    | Reconcile and verify migrated behavior.                              |
| Deployment    | Roll out to the target Fabric workspace.                             |

### Target architecture (future context)

```text
                PROJECT
                   │
                   ▼
             INTAKE AGENT          ← implemented today
                   │
                   ▼
          SOURCE REPOSITORY        ← canonical local copy (projects/<id>/source/)
                   │
                   ▼
        DISCOVERY ORCHESTRATOR     ← future
       ┌───────────┼────────────┐
       ▼           ▼            ▼
   Repository   ADF Objects   Dependencies
   Discovery    Discovery     Discovery
       └───────────┼────────────┘
                   ▼
   ASSESSMENT → PLANNING → ARCHITECTURE → ENGINEERING
             → GENERATION → VALIDATION → DEPLOYMENT
```

---

## Intake vs Discovery — the important separation

The repository lifecycle is deliberately split so that fetching a source and
understanding it are **two separate stages**:

```text
             SOURCE SYSTEM (e.g. GitHub)
                    │
                    ▼
              INTAKE AGENT
                    │
             authenticate / fetch (once)
                    │
                    ▼
      ┌─────────────────────────────┐
      │ CANONICAL LOCAL SOURCE REPO │  projects/<id>/source/
      └──────────────┬──────────────┘
                     │ reused by future stages
                     ▼
                 DISCOVERY  → ASSESSMENT → …
```

**Intake** answers: *"Where is the source, and can we obtain it?"*
It authenticates, fetches the requested repository/branch, stores a **canonical local
copy**, and records metadata. During Intake the repository is treated as an **opaque
artifact** — its contents are never inspected.

**Discovery** (a later, separate stage) answers: *"What is actually inside the source?"*
It will consume the canonical local copy produced by Intake and understand the ADF
estate. Discovery does **not** need to clone the repository again unless a deliberate
refresh is requested.

---

## Implementation Status

Built stages (each reads the previous stage's saved output under `projects/<id>/`):

| Stage | Command | What it does | Docs |
|---|---|---|---|
| Intake | `intake` | Gets the ADF source from a **live factory** (Azure Resource Manager, reusing `az login`) or a **GitHub repo**; masks secret values; records the commit and a manifest; checks for invalid JSON, duplicates and missing references. | [`docs/intake.md`](docs/intake.md) |
| Consultation | `consultation` | Readiness gate: questionnaire + connectivity checks (Repository, ADF, SQL Server, ADLS Gen2, Fabric). | [`docs/consultation.md`](docs/consultation.md) |
| Repository discovery | `discovery` | Inventories the source files and recognises ADF artifacts. | |
| ADF semantic discovery | `discovery-adf-semantic` | The pipeline parser (`parser_pipelines.py`): every activity, nested ones included, becomes a semantic record (operation, inputs/outputs, expressions, dependencies, summary). Also classifies integration runtimes. | [`docs/pipeline-parser.md`](docs/pipeline-parser.md) |
| Other discovery | `discovery-adf`, `discovery-adls`, `discovery-adls-paths`, `discovery-sql`, `discovery-sql-structural` | Read-only inventories of a live factory, ADLS Gen2 and SQL Server. | |
| Reconstruction | `reconstruct` | Finds incomplete pipelines and proposes evidence-backed completions with explicit confidence. The original is never modified. | [`docs/reconstruction.md`](docs/reconstruction.md) |
| Conversion | `convert` | Registry-driven conversion of every activity into Fabric components, with validation, a graph check against the source dependencies, and a report that separates *converted* from *runnable*. | [`docs/conversion.md`](docs/conversion.md) |
| Deployment | `deploy` | Pushes converted pipelines to a Fabric workspace through the REST API. **Dry run unless `--apply`**; refuses pipelines with unbound placeholders; never deletes. | [`docs/conversion.md`](docs/conversion.md) |

### Important limits

- **Nothing has been proven to run in Fabric.** Validation is against a schema table encoded from
  Microsoft's documentation, not a Fabric service. *Converted* means behavior is preserved by the
  generated components; it does not mean the pipeline can run (connections, workspace items and
  credentials must still be bound; the report lists them).
- Linked services become connection **placeholders** (`<connection:Name>`); real Fabric connection ids
  are supplied through a resolutions file. Credentials are never copied.
- A Fabric gateway is required only for self-hosted or managed-virtual-network integration runtimes; a plain
  Azure runtime needs none.
- Triggers are not generated (the report says how to recreate each one). Activities with no safe Fabric
  equivalent (e.g. Mapping Data Flow, SSIS) are reported as alternatives or blockers.
- The wider lifecycle in the product vision (Assessment, Planning, Architecture) is not built.

---

## Setup

```bash
python -m venv .venv
. .venv/Scripts/activate       # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                 # base + tests
pip install -e ".[dev,connectivity]"    # + real Consultation connectivity validation
```

Requires Python ≥ 3.11 and a `git` executable on `PATH`. The `connectivity` extra
(`pyodbc`, `azure-identity`, `azure-storage-file-datalake`, `msal`, `requests`) is needed for
**Azure intake**, **`deploy`** and the connectivity checks in Consultation; without it those report a
clear `dependency_unavailable` / "install the 'connectivity' extra" message. GitHub intake and the parse,
reconstruct and convert stages work with the base install.

### Authentication

**GitHub:** provide a token with read access via an environment variable. It is used **only** for the git operation
and is never written to disk, logs, or project metadata.

```bash
export GITHUB_TOKEN=ghp_xxx        # Windows PowerShell: $env:GITHUB_TOKEN="ghp_xxx"
```

**Azure (Data Factory, Fabric, ADLS Gen2):** every command that talks to Azure takes `--auth` plus the NAMES of
the environment variables holding its secrets (never the values). Supported mechanisms:

| `--auth` | Needs | Use for |
|---|---|---|
| `azure_cli` (default) | an `az login` session; optional `--tenant-id-env` to pick a tenant | everything, interactive work |
| `client_secret` | `--tenant-id-env --client-id-env --client-secret-env` | service principal (CI) |
| `client_certificate` | `--tenant-id-env --client-id-env --certificate-path-env [--certificate-password-env]` | service principal with a certificate |
| `managed_identity` | optional `--client-id-env` (user-assigned) | running on Azure (VM, App Service, Functions) |
| `interactive_browser`, `device_code` | optional `--tenant-id-env` | sign in without the Azure CLI |
| `access_token` | `--access-token-env` | a bearer token you already hold |
| `azure_identity` | nothing | the SDK's default credential chain |
| `account_key`, `sas_token`, `connection_string` | `--account-key-env` / `--sas-token-env` / `--connection-string-env` | **ADLS Gen2 only** (set in the consultation questionnaire) |

Storage-only mechanisms are refused for Data Factory and Fabric. `scripts/create_fabric_connection.py` creates Fabric
connections (ADLS Gen2 or SQL/Synapse) with key, SAS, basic, service-principal or workspace-identity credentials;
OAuth2 connections must be created in the Fabric UI. Details: [`docs/intake.md`](docs/intake.md) and
[`skills/consultation/questionnaire.md`](skills/consultation/questionnaire.md).

---

## Usage

```bash
adf2fabric intake \
  --project "Acme Orders" \
  --repo https://github.com/acme/adf-orders \
  --branch main
```

On success the agent prints the project id, provider, repository URL, branch, commit SHA,
and the local source path, and writes:

```text
projects/
└── acme-orders/
    ├── project.yaml     # provider, repository_url, branch, commit_sha, fetched_at, local_source_path
    └── source/          # canonical local copy of the fetched repository
```

Intake can instead pull ADF JSON straight from a live factory, reusing your `az login`
session (read-only `GET`s; secret values are masked):

```bash
adf2fabric intake --project "Acme Orders"   --factory adf-orders --resource-group rg-data [--subscription <id>] [--pipeline PL_Main]
```

Both modes write the same layout plus `projects/<id>/source-manifest.yaml` (source type,
identity, version/commit, retrieval time, integrity findings) and feed the same Discovery
stages. See [`docs/intake.md`](docs/intake.md).

Re-running GitHub intake for the same project/repo/branch **reuses** the existing local source
instead of cloning again; pass `--refresh` to force a fresh fetch (`--refresh` applies to `--repo` only;
Azure intake always fetches fresh).

### Consultation

After Intake, run the readiness gate with a structured questionnaire (schema:
[`skills/consultation/questionnaire.md`](skills/consultation/questionnaire.md)):

```bash
adf2fabric consultation \
  --project "Acme Orders" \
  --input consultation-input.yaml
```

It validates access across five domains (Repository, ADF, SQL Server, ADLS Gen2, Fabric),
prints a **READY / BLOCKED** decision (exit 1 when blocked), and writes a secret-free
`projects/<id>/consultation.yaml` handoff for Discovery. Credentials are supplied only by
environment-variable **reference**; no secret is ever persisted. See
[`docs/consultation.md`](docs/consultation.md).

### Reconstruct incomplete pipelines

```bash
adf2fabric reconstruct --project "Acme Orders"
adf2fabric convert --project "Acme Orders" --apply-reconstruction   # opt in to the inferred parts
```

Detects broken/missing/incomplete/unsupported pipeline content and proposes evidence-backed completions with
explicit confidence (HIGH / MEDIUM / LOW). The original source is never modified and inferred content is
always marked. See [`docs/reconstruction.md`](docs/reconstruction.md).

### Convert to Fabric

After Intake, Repository Discovery and ADF Semantic Discovery:

```bash
adf2fabric convert --project "Acme Orders" [--pipeline PL_Main] [--resolutions bindings.json]
```

Deterministic, registry-driven conversion of the semantic record into Fabric pipeline definitions under
`projects/<id>/conversion/`, with a report that separates *converted* from *runnable*. See
[`docs/conversion.md`](docs/conversion.md).

### Parse and deploy

```bash
adf2fabric discovery --project "Acme Orders"
adf2fabric discovery-adf-semantic --project "Acme Orders"
adf2fabric convert --project "Acme Orders"
adf2fabric deploy --project "Acme Orders" --workspace-id <workspace-guid>          # dry run
adf2fabric deploy --project "Acme Orders" --workspace-id <workspace-guid> --apply  # writes to Fabric
```

Child pipelines are built first: `deploy` orders pipelines so every invoked child exists before its parent, then
fills the child's Fabric item id and the workspace id into the parent (`--with-children` adds a parent's children
automatically). See [`docs/conversion.md`](docs/conversion.md).

`deploy` signs in through your `az login` session (the account needs a Contributor role on the workspace)
and stops on any pipeline that still holds `<connection:...>` placeholders unless `--allow-unbound`.

Configuration defaults live in [`config/config.yaml`](config/config.yaml); CLI flags
override them.

---

## Project layout

```text
src/adf2fabric/
├── cli.py                       # all `adf2fabric` commands
├── config.py                    # loads config/config.yaml
├── agent/intake/                # GitHub + Azure intake, source validation, manifest
├── agent/consultation/          # readiness gate
├── agent/discovery/             # repository, ADF, semantic (parser_pipelines.py), ADLS, SQL
├── agent/reconstruction/        # incomplete-pipeline detection + proposals
├── agent/conversion/            # registry, handlers, expressions, validation, pipeline_semantics.py
├── agent/deployment/            # Fabric REST deployment (dry run by default)
├── connectivity/                # connector registry + real connectors
├── repository/                  # providers, git wrappers, auth
└── project/                     # persisted models + on-disk store
config/config.yaml
skills/                      # operating instructions per agent
docs/                        # per-stage references
tests/                       # offline tests (local git repos, fixtures, fake connectors/REST)
```

## Providers

Only **GitHub** is implemented. The provider abstraction leaves room for Azure DevOps,
GitLab, and Bitbucket in the future; requesting them today raises a clear
"not implemented" error.
