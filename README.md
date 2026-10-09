# ADF → Microsoft Fabric Migration Agent

An agent that migrates an **Azure Data Factory (ADF)** estate into **Microsoft Fabric
Data Factory**.

This repository is developed **incrementally, one stage at a time**. This document
describes the *complete intended product* so the direction is clear, and then states
*exactly what is implemented today*. **The vision sections are context, not a task list.**

> **Read this first:** the lifecycle and architecture below describe where the product is
> going. They do **not** mean every stage is built. Only the **Intake Agent** and the
> **Consultation Agent** are implemented right now — see
> [Implementation Status](#implementation-status).

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

The project is being developed incrementally. The sections above describe the complete
intended ADF → Microsoft Fabric migration lifecycle. They establish product context,
architecture, responsibilities, and future direction. **They are not instructions to
implement every stage immediately.**

### Currently implemented

- **Intake Agent**
- GitHub authentication (token via environment variable)
- Repository validation (URL, provider, accessibility, branch)
- Repository clone / fetch
- Persistent canonical local source (`projects/<id>/source/`)
- Source reuse (idempotent re-runs skip re-cloning)
- Commit SHA capture
- Project metadata capture

- **Consultation Agent** (deterministic readiness gate; see [`docs/consultation.md`](docs/consultation.md))
- Structured questionnaire input (no LLM, no inference, references-only credentials)
- Deterministic capability-fit check (like-for-like only; see [`docs/migration-capability.md`](docs/migration-capability.md))
- Real connectivity validation across five domains (Repository, ADF, SQL Server, ADLS Gen2, Fabric)
  behind an injectable, extensible connector registry
- READY / BLOCKED readiness gate + secret-free Discovery handoff (`projects/<id>/consultation.yaml`)

### Not yet implemented

- Discovery
- Assessment
- Planning
- Architecture
- Engineering
- Generation
- Validation
- Deployment

No migration/generation logic exists yet. Consultation performs **connectivity validation
only** (auth/reachability/permission) — it does not inspect the source estate or create any
Fabric artifact. There are no placeholder files for future stages — a stage receives code
only when it is explicitly authorized.

---

## Setup

```bash
python -m venv .venv
. .venv/Scripts/activate       # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                 # base + tests
pip install -e ".[dev,connectivity]"    # + real Consultation connectivity validation
```

Requires Python ≥ 3.11 and a `git` executable on `PATH`. The `connectivity` extra
(`pyodbc`, `azure-identity`, `azure-storage-file-datalake`, `msal`, `requests`) is optional
and only needed to *validate* ADF/SQL Server/ADLS/Fabric connectivity during Consultation;
without it those connectors report a clear `dependency_unavailable` result.

### Authentication

Provide a GitHub token with read access to the repository via an environment variable.
The token is used **only** for the git operation and is never written to disk, logs, or
project metadata.

```bash
export GITHUB_TOKEN=ghp_xxx        # Windows PowerShell: $env:GITHUB_TOKEN="ghp_xxx"
```

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

Re-running intake for the same project/repo/branch **reuses** the existing local source
instead of cloning again. Pass `--refresh` to force a fresh fetch.

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

Configuration defaults live in [`config/config.yaml`](config/config.yaml); CLI flags
override them.

---

## Project layout

```text
src/adf2fabric/
├── cli.py                 # `adf2fabric intake` + `adf2fabric consultation` commands
├── config.py              # loads config/config.yaml
├── agent/intake/          # IntakeAgent orchestration + request/result models
├── agent/consultation/    # ConsultationAgent, readiness engine, request/result models
├── connectivity/          # injectable connector registry + 5 real connectors
├── repository/            # providers, git wrappers, auth, fetch client
└── project/               # Project/Consultation models + on-disk store
config/config.yaml
skills/intake/             # Intake agent operating instructions
skills/consultation/       # Consultation operating instructions + questionnaire schema
docs/intake.md             # Intake capability reference
docs/consultation.md       # Consultation capability reference
docs/migration-capability.md  # authoritative like-for-like scope
tests/                     # offline tests (local git repos + injected fake connectors)
```

## Providers

Only **GitHub** is implemented. The provider abstraction leaves room for Azure DevOps,
GitLab, and Bitbucket in the future; requesting them today raises a clear
"not implemented" error.
