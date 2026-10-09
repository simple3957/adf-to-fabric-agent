# Consultation capability

The Consultation Agent is the second lifecycle stage, after
[Intake](intake.md) and before Discovery. It is a **deterministic readiness
gate**: it establishes and *actually validates* the access needed to begin
Discovery for the current engagement, then emits a **READY / BLOCKED** decision
and a machine-readable handoff contract. Then it **stops**.

Consultation answers: *"Do we have everything required to begin Discovery for
this ADF + SQL Server + ADLS Gen2 → Microsoft Fabric, like-for-like migration?"*

See [migration-capability.md](migration-capability.md) for the authoritative
scope (like-for-like only; five current connectivity domains).

## Deterministic, no LLM

Consultation captures **explicit human-provided input only** and validates it
against strict pydantic schemas. It performs **no** LLM reasoning, **no**
inference, and makes **no** assumptions when information is missing — a missing
required value fails clearly. Connectivity validation *does* use the network
(that is its purpose); nothing else does.

## Inputs

| Input          | Required | Notes                                                     |
|----------------|----------|-----------------------------------------------------------|
| `project_name` | yes      | An existing project (Intake must have run).               |
| `input_path`   | yes      | Path to the structured questionnaire YAML (`--input`).    |

The questionnaire is the canonical interface. Its schema is documented in the
[Consultation questionnaire skill](../skills/consultation/questionnaire.md).
Credentials are supplied only by **reference** (env-var names); actual secrets
are resolved from the environment at validation time and never persisted.

## Flow

```text
load questionnaire → validate migration definition → classify capability fit
   → validate connectivity (5 domains) → evaluate readiness → persist → STOP
```

## The five connectivity domains

| Domain       | What is validated (genuine auth/reachability/permission)          |
|--------------|-------------------------------------------------------------------|
| `repository` | Source repo (from Intake) reachable on the recorded branch.       |
| `adf`        | ADF factory reachable via ARM (auth + permission).                |
| `sql_server` | Connect + `SELECT 1` against the configured database.             |
| `adls_gen2`  | Storage account (and optional filesystem) reachable + accessible. |
| `fabric`     | Target workspace reachable via the Fabric API.                    |

Each connector performs a **narrow** check only. None enumerate, read, inspect,
or profile the source estate — that is Discovery.

Connectors live behind an injectable registry
(`adf2fabric.connectivity.registry`), the extension point for future ETL tools,
warehouses, storage technologies, and targets.

## Required / provided / validated

Each domain independently tracks distinct states (never collapsed):

- **required** — is this access needed for the engagement?
- **mandatory** — does failure block readiness? (optional requirements never block)
- **provided** — were the config + credential references supplied?
- **validated** — did the genuine connectivity check succeed?

## Readiness gate

- A **mandatory** requirement that is not validated → a **blocker**.
- An **optional** required requirement that is not validated → a **warning**.
- A requested migration **mode** outside like-for-like → a **blocker** (recorded,
  never reinterpreted).
- Unsupported *additional* requirements → recorded + flagged as **warnings**.

```yaml
readiness:
  status: blocked
  blockers:
    - requirement: sql_server
      reason: "sql_server connectivity could not be validated. (...)"
```

`READY` only when every mandatory requirement is actually validated.

## Output (`consultation.yaml`) — the Discovery handoff

Persisted at `projects/<id>/consultation.yaml`, secret-free. Contains: project id
+ `source_commit_sha` (provenance pin from Intake), `created_at`/`updated_at`,
the echoed migration definition / requested scope / success criteria /
constraints / exclusions, a deterministic `capability_fit` block, the five access
requirements (with non-secret config + credential references + states), the
per-domain connectivity outcomes, the readiness decision, and `provenance` tags.

Discovery consumes this directly and does not reconstruct it.

## Timestamps & reproducibility

`created_at` is set once. `updated_at` bumps only when the consultation content
changes (timestamps and volatile `validated_at` excluded). Re-running with
identical input yields semantically equivalent output.

## Security

- No secret is ever persisted — only references (env-var names, auth mechanism,
  connection metadata like server/database/storage account/workspace id).
- Connectors resolve credentials from the environment at validation time and
  discard them; any credential is redacted from error text.

## Scope boundary

After the readiness decision is recorded, Consultation **hard-stops**. It does
not read `source/`, does not inspect ADF JSON, does not enumerate ADF artifacts /
SQL schemas or tables / ADLS containers, directories or files, and does not
create Fabric artifacts. Those belong to Discovery and later stages.
