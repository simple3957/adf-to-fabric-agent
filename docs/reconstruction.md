# Reconstruction (incomplete pipeline analysis)

`adf2fabric reconstruct --project <name>` finds what is missing or inconsistent in the ADF pipelines and, **only
where the project itself supplies the evidence**, proposes how to complete it. Deterministic; no LLM. It reads
the ADF JSON to quote it and never writes to `source/`.

```text
reconstruction/
├── reconstruction.yaml            findings, proposals (evidence, rule, confidence...), blockers, patterns
├── reconstruction-report.md       ORIGINAL / PARSED / PROPOSED / GENERATED, kept apart
├── completed/<same layout>        the proposed ADF completion: original JSON + SAFE proposals only
└── adf-semantic.completed.yaml    that completion parsed by the existing semantic parser
```

`adf2fabric convert --apply-reconstruction` then converts the completed pipelines and marks every inferred
element. Without the flag, inferred content never enters the Fabric output.

## Four representations, never merged

| | Where | Holds |
|---|---|---|
| ORIGINAL | `source/` | the ADF JSON as authored. Never modified |
| PARSED | `discovery/adf-semantic.yaml` | what the parser extracted. Never modified |
| PROPOSED | `reconstruction/` | inferences as evidence-backed JSON patches against the original |
| GENERATED | `conversion/` | the Fabric output; inferred components carry `origin: inferred` and a `userProperties` marker |

## What is detected (four different problems)

| Class | Meaning | Examples |
|---|---|---|
| `invalid_json` | the file cannot be parsed | truncated (cut off mid-structure), malformed, empty |
| `missing_artifact` | a reference points at nothing | dataset, linked service, child pipeline, trigger target |
| `incomplete_configuration` | present but missing/unresolved | required properties, Copy source/sink, empty SQL/script, undeclared parameters/variables/global parameters, undefined or implied dependencies, unparsed expressions, inconsistent output contracts, settings Fabric needs (Script database, Databricks cluster, dynamic URL) |
| `unsupported_functionality` | complete, but no Fabric equivalent | SSIS, retired connectors |

## Evidence and confidence

Evidence is consulted in this priority order: 1 explicit property · 2 explicit reference in the same pipeline ·
3 pipeline/factory parameters · 4 referenced datasets/linked services · 5 parent/child pipelines · 6 related
pipelines of matching structure · 7 repeated source-to-sink patterns · 8 names and metadata · 9 SQL objects,
notebooks, schema information.

Confidence is one of three categories produced by **one function** (`evidence.grade`), never a score:

* **LOW** - information is still required, or the evidence supports zero or several candidates, or the only
  support is weak (one peer, one naming hint).
* **HIGH** - exactly one candidate, nothing missing, no material assumption, and explicit evidence (priority 1-2)
  or a uniquely determined mapping from a referenced artifact (priority <= 4).
* **MEDIUM** - exactly one candidate and enough support (priority <= 6, or a pattern confirmed by two independent
  items). A material assumption caps HIGH at MEDIUM.

Only HIGH/MEDIUM proposals with nothing outstanding are `safe_to_generate`. Everything else becomes a blocker
that names the exact information needed (or, for conventions, a template that is never generated).

## Rules (each is tested)

| Rule | Infers | Evidence |
|---|---|---|
| `copy_endpoint_from_dataset` | missing Copy source/sink type (+store/format settings) | the referenced dataset and linked-service types (uniquely determined) |
| `copy_endpoint_from_peers` | a missing source/sink dataset | other Copy activities pairing with the known side (>= 2 agreeing for MEDIUM) |
| `dependency_from_reference` | `dependsOn` implied by reading `activity('X').output` (Failed if it reads `.error`) | the expression itself; refused if it would create a cycle |
| `lookup_shape_from_consumers` | `firstRowOnly` that the consumers' `.output.value` / `.output.firstRow` reads require | the reads; an *explicit* setting is never overridden (blocker instead) |
| `metadata_field_from_reads` | a GetMetadata `fieldList` entry that consumers read | the reads |
| `argument_from_context` | child-pipeline / dataset parameter values: same-named pipeline parameter, the current file in a loop over `childItems`, or peers' agreed value | parameters, loop structure, peers |
| `missing_artifact_reference` | a rename when exactly one artifact matches modulo case/separators AND fits the use | naming + a compatibility check; otherwise LOW |
| `property_from_shape_siblings` | a required property that >= 2 structurally identical pipelines all set the same way | related pipelines; secret-looking values never copied |
| `watermark_update_from_lookups` | the missing watermark write-back of an incremental load | the stored-watermark Lookup query, the MAX Lookup, the filtered Copy; SQL is assembled **only** from those and flagged `derived_sql` |
| `staging_load_from_peers` | the missing load of a staging table | other pipelines' load procedures (name derived in the unqualified part only); SQL discovery corroborates |
| `error_handling_convention` | (template only, never generated) a failure handler that every comparable pipeline has | repeated pattern |

Patterns recognised for the report (never created): incremental load, file ingestion, parent/child
orchestration, parameterized ingestion, staging-to-curated, conditional execution, retry loop, failure handling.

## Safety guarantees

* No SQL, schema, table, credential or business logic is written without supporting evidence; empty script
  bodies and empty queries are blockers.
* An explicit property is never replaced by an inference.
* Proposals that would conflict, or whose patch cannot apply, are demoted with the reason recorded.
* In conversion, any inferred element makes the activity at best `converted_with_differences`; anything below
  HIGH keeps `runtime_ready` false until reviewed.
* Ambiguous pipelines are never reported as executable: `executable_as_is` is false while any gap, required
  Fabric configuration or unsupported feature remains.

## Limits

Reconstruction cannot know undocumented business intent. It does not infer data-flow contents, notebook code,
connection strings or per-environment values, and it recognises patterns only in the shapes listed above.
