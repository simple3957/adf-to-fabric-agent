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


## The migration consultation (after discovery): how the migration should happen

Readiness (above) answers "can we start?". The migration consultation answers "how should it happen?", after discovery and
before anything moves (`agent/consultation/brief.py`, `brief_record.py`). Ideas taken from the Synapse accelerator's agent mode.

* **Grounded in discovery.** A question is asked only when discovery found what it is about: notebook activities, file
  datasets, self-hosted runtimes, Key Vault secrets, linked-service parameters without a value, schedule or event triggers,
  data flows, activity types with no automatic Fabric route (from the conversion rules, over the whole estate), safe
  reconstruction proposals. Things discovery cannot see (network rules, data volumes, the capacity actually needed) are asked
  and listed as not visible.
* **Read, not asked.** The Fabric workspace chosen on the Connections step, a lakehouse already bound and the Invoke-pipeline
  connection are read from the configuration. That configuration is recorded with the brief, so everyone who looks at it
  sees the same brief (an approver's own session never changes it).
* **Recommendations with reasons.** Each recommendation is computed from the discovered counts and says why. It is used only
  where nothing is answered, shown as a recommendation, and becomes an answer only when someone accepts it.
* **Every question says what its answer does:** `run` (the run follows it), `gate` (the agent stops for a person), or
  `record` (kept in the brief for the team).
* **A versioned, approved brief.** Answers are checked against each question's own options. A brief is submitted (every
  required question answered, no conflicts), then approved by someone holding `provision:approve`, against the fingerprint
  of the answers they read. Changing an answer, or a change in what discovery found, puts it back to draft; the run then
  stops following it until it is approved again. Carrying out the brief (converting its subset) never takes its approval away.

Guards found by testing the consultation against misuse:

* The brief is bound to the Fabric workspace it was agreed for (recorded with it, never shown by id). Deploying, or starting real
  runs, in another workspace is refused until the brief is reviewed and approved for that workspace ("Use the workspace chosen now").
* A stored answer is validated again each time it is read (a rule that tightened, a hand-edited file): an invalid one is ignored,
  its question is open again, and the reason is shown. A lakehouse name must be a name (letters, digits, `_ . -`), not a sentence.
* Holding back every pipeline is a conflict, and an empty scope never turns into "convert everything". A scope that leaves out a
  pipeline the chosen one invokes is a conflict too (deployment would refuse the caller).
* Saving a topic never accepts a recommendation nobody chose: recommendations are shown, not pre-selected.

What the approved brief drives today:

| Answer | Effect |
|---|---|
| Scope (all, selected pipelines, pilot = first dependency wave), unsupported activities held back | the pipelines `convert` takes |
| Storage: repoint to a lakehouse, its name | `convert --lakehouse` (the storage map is refreshed first) |
| Reconstruction proposals | `convert --apply-reconstruction` |
| Validation depth, sensitive-data restriction | validation stages; whether real runs may be started (`verify` with `run` is refused otherwise) |
| Gateways, network, capacity, Invoke-pipeline connection | prerequisites listed for a person |

The API's `convert` stage follows an approved, current brief automatically; the CLI does with `convert --from-brief`.
Everything else is recorded in the brief (Markdown download) for the team.

CLI: `brief` (show; `--markdown`, `--questionnaire` to export), `brief-answer --set ID=VALUE`, `brief-accept`,
`brief-import --file`, `brief-submit`, `brief-approve --fingerprint --by`. API: `GET/PUT /projects/{id}/brief…`
(read: `reports:view`; answer, accept, import, submit: `plans:generate`; approve: `provision:approve`; audited).
UI: the Consult step, between Review and Connect.
