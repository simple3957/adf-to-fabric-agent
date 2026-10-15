# ADF → Microsoft Fabric Migration Agent

An agent that migrates an **Azure Data Factory (ADF)** estate into **Microsoft Fabric
Data Factory**.

The stages that exist today turn an Azure Data Factory estate into Fabric pipeline definitions
(intake -> discovery -> parse -> convert -> deploy), plus a connection/authentication layer and a FastAPI backend
for a UI (Streamlit, built separately). The broader lifecycle below is the product
direction; [Implementation Status](#implementation-status) states exactly what is built.
Everything is deterministic: **no LLM or generative model is used anywhere.**

---

## What this project is

Moving from Azure Data Factory (ADF) to Fabric Data Factory is mostly mechanical, but the mechanical parts hide
decisions: which activities have a Fabric equivalent, which are deprecated or changed, how each linked service
becomes a Fabric *connection* (and with which credentials), and what still needs a person. This project does the
mechanical work deterministically and **reports honestly what it could not do**.

It is a Python package (`adf2fabric`) with three ways to drive it:

* a **CLI** (`adf2fabric ...`) for each stage;
* a **FastAPI backend** (`python -m adf2fabric.api`) that a UI calls; a Streamlit front end is built separately;
* a **Demo mode** in that backend, running the same routes on a bundled sample with no network.

### End-to-end flow

```text
 SOURCE                     ANALYSE                      CONVERT                    FABRIC
 ADF via Azure CLI   ─►  Repository discovery   ─►   Compatibility catalog   ─►  Connections (one per
 Git (HTTPS)               ADF semantic parse         Registry of activity          linked service)
 ZIP export                Integration runtimes       handlers + validation      ─► Deploy (dry run first,
        │                  Reconstruction of          Triggers -> schedules         children before parents)
        ▼                  incomplete pipelines       CDC / Synapse Link
 projects/<id>/source/     projects/<id>/discovery    projects/<id>/conversion
```

1. **Intake** obtains the ADF JSON (live factory, Git repository, or an uploaded ZIP) into a canonical local copy
   `projects/<id>/source/`, masks secret values and records where it came from (commit, time, integrity findings).
2. **Discovery** inventories the files and parses every pipeline, activity (nested ones included), dataset,
   linked service, trigger and integration runtime into semantic records: what each does, its inputs and outputs,
   expressions and dependencies.
3. **Reconstruction** (optional) proposes evidence-backed completions for incomplete pipelines, never touching the original.
4. **Conversion** maps each activity to Fabric components through a registry of handlers, checks the result against a
   schema table taken from Microsoft's documentation, compares the graph to the source dependencies, and consults a
   **compatibility catalog** of deprecated or changed ADF components. Schedule triggers become Fabric schedules; CDC and
   Synapse Link resources are detected and reported. The report separates *converted* from *runnable*.
5. **Connections** gives each linked service a card: the Fabric connector it maps to, the authentication methods
   available (SQL login, service principal, workspace identity, Key Vault variants, existing connection, ...), the
   integration runtime it uses and the Fabric path that implies (cloud connection, VNet gateway, on-premises gateway or
   manual), matching existing Fabric connections, and a real test. Applying the choices creates or reuses Fabric
   connections and writes `conversion/resolutions.json`, which the next conversion consumes to replace placeholders.
6. **Deployment** pushes pipelines to a Fabric workspace through the REST API: dry run unless `--apply`, children before
   parents, never deletes, refuses unresolved placeholders.

### Design principles

* **Deterministic, no LLM.** Same input, same output; every decision traces to a rule, a catalog entry or evidence.
* **Evidence, not assumption.** A connection is "tested" only when a credential was actually used; Azure sign-in is not
  proof of Fabric authorization; a self-hosted integration runtime is never assumed reusable in Fabric.
* **Honest reporting.** Anything not converted, not verified or needing a person is listed, not hidden.
* **Secrets stay out.** Sign-in uses the Azure and Fabric CLIs on the backend machine. Secrets are never put in project
  files, reports, generated code, logs or API responses; a secret typed into the UI lives in an in-memory vault that
  expires. Connection strings are never parsed and values are never invented.
* **Safe by default.** Deploy is a dry run, nothing is deleted, ZIP contents are never executed, the API binds to loopback.

### Where things live

| Path | Contents |
|---|---|
| `src/adf2fabric/agent/` | the stages: intake, consultation, discovery, reconstruction, conversion, deployment |
| `src/adf2fabric/connections/` | connector catalog, cards, runtime analysis, testing, providers, redaction, secret vault |
| `src/adf2fabric/api/` | FastAPI backend, jobs, stages, Demo mode |
| `projects/<id>/` | everything produced for one engagement (source, discovery, conversion, connections) |
| `skills/`, `docs/` | operating instructions per agent and per-stage references |
| `tests/` | offline tests (local git repos, fixtures, fake connectors and REST) |

Start with [Setup](#setup) and [Usage](#usage); the status table below says exactly what is built and what is not.

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
| Intake | `intake` (CLI); API for Git and ZIP | Gets the ADF source from a **live factory** (Azure Resource Manager, reusing `az login`), a **GitHub repo**, any **HTTPS Git repository** or an uploaded **ZIP export** (safe extraction, JSON only, nothing executed); masks secret values; records the commit and a manifest; checks for invalid JSON, duplicates and missing references. | [`docs/intake.md`](docs/intake.md) |
| Consultation | `consultation` | Readiness gate: questionnaire + connectivity checks (Repository, ADF, SQL Server, ADLS Gen2, Fabric). | [`docs/consultation.md`](docs/consultation.md) |
| Repository discovery | `discovery` | Inventories the source files and recognises ADF artifacts. | |
| ADF semantic discovery | `discovery-adf-semantic` | The pipeline parser (`parser_pipelines.py`): every activity, nested ones included, becomes a semantic record (operation, inputs/outputs, expressions, dependencies, summary). Also classifies integration runtimes, and records linked-service connection properties (explicit server/database/user only; connection strings are never parsed), parameterized linked services and dataset arguments. | [`docs/pipeline-parser.md`](docs/pipeline-parser.md) |
| Other discovery | `discovery-adf`, `discovery-adls`, `discovery-adls-paths`, `discovery-sql`, `discovery-sql-structural` | Read-only inventories of a live factory, ADLS Gen2 and SQL Server. | |
| Reconstruction | `reconstruct` | Finds incomplete pipelines and proposes evidence-backed completions with explicit confidence. The original is never modified. | [`docs/reconstruction.md`](docs/reconstruction.md) |
| Conversion | `convert` | Registry-driven conversion of every activity into Fabric components, with validation, a graph check against the source dependencies, and a report that separates *converted* from *runnable*. | [`docs/conversion.md`](docs/conversion.md), [`docs/conversion-failure-modes.md`](docs/conversion-failure-modes.md), [`skills/conversion/`](skills/conversion/SKILL.md) |
| Connections | API (`/projects/{id}/connections`) | One credential card per linked service: SQL, ADLS Gen2, Blob, REST and Databricks connectors; auth methods incl. Key Vault variants, service principal, workspace identity and existing-connection reuse; integration-runtime analysis (Azure / managed VNet / self-hosted / SSIS -> Fabric path); real connection tests; creates or reuses Fabric connections and merges `conversion/resolutions.json`; writes `connections-report.{md,json}`. | [`docs/connections.md`](docs/connections.md), [`skills/connections/`](skills/connections/SKILL.md) |
| API | `python -m adf2fabric.api` | FastAPI backend for the UI: sign-in status, source/destination selection and tests, projects, connections, migration stages as jobs, Demo mode. | [`docs/connections.md`](docs/connections.md) |
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
- Schedule triggers become Fabric schedule definitions where they map exactly (`deploy --schedules`); event triggers are recreated by hand. Activities with no safe Fabric
  equivalent (e.g. Mapping Data Flow, SSIS) are reported as alternatives or blockers.
- Conversion also carries a compatibility catalog (deprecated or changed ADF components and their Fabric
  equivalents, with a conversion skill) and CDC / Synapse Link detection; see
  [`docs/conversion-failure-modes.md`](docs/conversion-failure-modes.md).
- Connection limits: OAuth and gateway-backed connections cannot be created through Fabric's API (create them in
  Fabric, then reuse them); a self-hosted integration runtime is never assumed reusable (it maps to an
  on-premises gateway you set up); reachability tests run from this machine only; the Fabric CLI's signed-in
  output is not yet verified live; deploy uses the Azure CLI sign-in. See [`docs/connections.md`](docs/connections.md).
- The wider lifecycle in the product vision (Assessment, Planning, Architecture) is not built.

---

## Setup

```bash
python -m venv .venv
. .venv/Scripts/activate       # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                 # base + tests
pip install -e ".[dev,connectivity]"    # + real Consultation connectivity validation
pip install -e ".[dev,connectivity,api]" # + the FastAPI backend (UI support)
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

`deploy --schedules` also creates the Fabric schedules generated from ADF triggers (disabled unless `--enable-schedules`).

`deploy` signs in through your `az login` session (the account needs a Contributor role on the workspace)
and stops on any pipeline that still holds `<connection:...>` placeholders unless `--allow-unbound`.

Configuration defaults live in [`config/config.yaml`](config/config.yaml); CLI flags
override them.

---

## Compute assessment (Fabric capacity sizing)

Estimates how much Fabric compute the pipelines need and which F capacity fits. ADF bills per run (Copy DIU-hours,
activity runs, Data Flow cores); Fabric draws everything from a capacity bought up front, measured in capacity units (CU).

```bash
adf2fabric assess-compute --project "Acme Orders"                       # from pipeline definitions only
adf2fabric assess-compute --project "Acme Orders" --history \
  --subscription <sub-id> --resource-group <rg> --factory <factory>     # plus measured durations/DIUs from ADF runs
```

Writes `projects/<id>/assessment/compute-report.{md,json}`. For each activity it shows:
- how ADF bills it, and the resources it is configured with (DIUs, parallel copies, Data Flow cores);
- its Fabric equivalent: Copy throughput (ITO), Spark vCores, or an orchestration run;
- the CU-seconds per pipeline run.

Per pipeline, runs per day come from the triggers and flow down to the pipelines they invoke. Pipelines without a
trigger use `--runs-per-day` (default 1). The recommended SKU is the smallest F capacity that covers daily CU-hours ÷ 24
plus 25% headroom: Fabric smooths background jobs such as pipelines over 24 hours.

The rates are in `agent/assessment/compute_rates.py`, each with its Microsoft Learn source and the date it was checked.
Every number in the report says whether it was measured (ADF run history) or assumed (a stated default). Copy is billed
per started minute; this was confirmed in both ADF's and Fabric's run output for `PL_Compute_Test`. The API exposes the
same estimate as the `compute` stage.

---

## API and Demo mode

```bash
python -m adf2fabric.api            # http://127.0.0.1:8765 (loopback; set ADF2FABRIC_API_TOKEN to require a bearer token)
```

The UI talks to this backend. Sign-in is `az login` / `fab auth login` on the machine running it, so no password or CLI
token ever reaches the browser; secrets typed into a card are held in memory only, expire, and are never returned,
logged or written to a project or report. Send `X-Session-Id` to scope selections and secrets; `POST /mode {"mode":"demo"}`
switches that session to Demo mode (canned providers, real parsing on a bundled sample, no network).
`ADF2FABRIC_ALLOWED_ORIGINS` sets the CORS origins (default the local Streamlit ports). Route list and the
implemented / not-implemented matrix: [`docs/connections.md`](docs/connections.md).

### Run the UI

```bash
pip install -e ".[api,ui]"
python -m adf2fabric.api            # terminal 1: the API on 127.0.0.1:8765
streamlit run ui/app.py             # terminal 2: the UI on http://localhost:8501
```

The UI (`ui/`, "ADF → Fabric Studio") holds no migration logic: every page calls the API through `ui/api_client.py`.
It follows the same process as the Synapse → Fabric Migration Accelerator, with its own look: a **Connections** page
(choose the route, connect the ADF source and the Fabric workspace, start) and a **Migration** page with a vertical
stepper (Discover → Connect → Convert → Deploy). A step opens once the one before it is confirmed. There is also an
**Activity** page with job history.
Turn on **Demo** in the top bar, or open `http://localhost:8501/?demo=1&project=demo-estate`, to walk the whole flow
on the bundled sample without Azure.
Set `ADF2FABRIC_API_URL` if the API is not on the default address. Plan and screen-to-endpoint map: [`docs/ui-plan.md`](docs/ui-plan.md).

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
├── connections/                 # connector catalog, cards, runtime analysis, testing, providers (az, fab, git, zip), redaction, secret vault
├── api/                         # FastAPI backend: routes, jobs, stages, Demo mode
├── connectivity/                # connector registry + real connectors
├── repository/                  # providers, git wrappers, auth
└── project/                     # persisted models + on-disk store
config/config.yaml
skills/                      # operating instructions per agent
docs/                        # per-stage references
tests/                       # offline tests (local git repos, fixtures, fake connectors/REST)
```

## Connections and the API

A FastAPI backend (`pip install -e ".[api]"`, then `python -m adf2fabric.api`, loopback `127.0.0.1:8765`) serves the UI:
Azure CLI / Git / ZIP sources, Azure CLI / Fabric CLI destinations, per-linked-service credential cards and a Demo mode.
See [docs/connections.md](docs/connections.md) for what is implemented, tested and deliberately not supported.

## Providers

The CLI `intake --repo` supports **GitHub**; the API additionally accepts any HTTPS Git repository and ZIP exports. The provider abstraction leaves room for Azure DevOps,
GitLab, and Bitbucket in the future; requesting them today raises a clear
"not implemented" error.
