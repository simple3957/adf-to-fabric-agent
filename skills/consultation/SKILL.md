# Skill: Consultation

Operating instructions for the Consultation Agent — the second stage of the
ADF → Microsoft Fabric migration system, after Intake and before Discovery.

## Purpose

Establish and validate engagement **readiness** before Discovery. Consultation
answers a single question:

> "Do we have the access and information needed to begin Discovery for this
> ADF + SQL Server + ADLS Gen2 → Microsoft Fabric, like-for-like migration?"

It captures an explicit questionnaire, validates connectivity across five
domains, and emits a deterministic **READY / BLOCKED** decision plus a Discovery
handoff contract (`projects/<id>/consultation.yaml`).

## Current scope

Fixed by the product (see [`../../docs/migration-capability.md`](../../docs/migration-capability.md)):

```text
Source ETL tool  : Azure Data Factory
Source warehouse : SQL Server
Storage / sink   : Azure Data Lake Storage Gen2
Target platform  : Microsoft Fabric
Migration mode   : Like-for-like
```

The five current connectivity domains: **Repository, Azure Data Factory, SQL
Server, Azure Data Lake Storage Gen2, Microsoft Fabric.**

## Responsibilities

- Accept a structured questionnaire (project name + input YAML).
- Confirm the fixed migration definition (ADF + SQL Server + ADLS Gen2 → Fabric,
  like-for-like) and record objective, requested scope, success criteria,
  constraints, exclusions.
- Deterministically classify **capability fit** — a request outside like-for-like
  is captured and flagged unsupported, never reinterpreted or executed.
- Validate genuine connectivity for each **required** domain, tracking
  `required / mandatory / provided / validated` distinctly.
- Evaluate readiness (READY/BLOCKED) with explicit blockers.
- Persist a secret-free, machine-readable consultation record for Discovery.
- Never expose or persist credentials.
- Stop after the readiness decision.

## Boundaries

```text
CONSULTATION ESTABLISHES ACCESS, NOT CONTENT.
It may authenticate to and reach a resource; it must NOT inspect what is inside.
Discovery is responsible for understanding the source estate later.
```

Consultation must **not**: read `source/`; inspect ADF JSON; enumerate ADF
pipelines / datasets / linked services / triggers / integration runtimes / data
flows; enumerate SQL schemas, tables, or stored procedures; enumerate or read
ADLS containers, directories, or files; profile data; infer source→sink or
ADF→ADLS dependencies; map artifact names to targets; or create/deploy any
Fabric artifact. All of that belongs to Discovery or later phases.

## Deterministic, minimal reasoning

Authentication, questionnaire validation, connectivity checks, capability-fit
classification, readiness evaluation, and persistence are all deterministic
code. **No LLM** reasoning is used. No assumptions are made about missing
information — a missing required value fails clearly. Connectivity validation
requires network access; nothing else does.

## Success criterion (hard stop)

```text
Load questionnaire → Validate migration definition → Classify capability fit
   → Validate connectivity (5 domains) → Evaluate readiness → Persist → HARD STOP
```

Once the readiness decision is recorded, stop. Do not continue into Discovery or
any later stage.

## Connectivity providers

Connectors live behind an injectable registry
(`adf2fabric.connectivity.registry`). The current engagement registers exactly
five: `repository`, `adf`, `sql_server`, `adls_gen2`, `fabric`. The registry is
the extension point for future ETL tools, warehouses/databases, storage
technologies, source/target platforms, and connectivity providers — added later
without changing the readiness engine. Do not implement future providers now.

The questionnaire schema is documented in
[`questionnaire.md`](questionnaire.md).

## Security

Persist **references only** — env-var names, auth mechanism, and non-secret
connection metadata (server, database, storage account, workspace id). Never
persist passwords, tokens, client secrets, private keys, or raw connection
strings containing credentials. Connectors resolve credentials from the
environment at validation time and redact them from any error text.
