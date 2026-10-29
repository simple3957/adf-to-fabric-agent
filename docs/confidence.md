# Migration confidence

`adf2fabric confidence --project P` (also `GET /projects/{id}/confidence`, and the top of the UI's Report step) says how much of a migration is
*established* and how much is still *assumed*. It is read-only and deterministic: no model, no live call. It reads what earlier stages recorded.

## What it is, and what it is not

A **range** per area and per item, built from criteria:

| | Meaning |
|---|---|
| low | the share of the weight whose criteria are **met** (evidenced). Everything else counts against it. |
| high | the share that is **met or unknown**: what it would be if every open question turned out well. |
| checked | the share that is met or **unmet**: how much has actually been looked at. |

The gap between low and high is the part nobody has checked. It narrows only when evidence is recorded (a real run, a reviewed draft, a tested
connection). It is **not a statistical probability**: nothing is sampled, so there is no confidence level in that sense. It is a bound from
evidence, and the report says so.

## Criteria

Each criterion has a weight, a reason it matters, and a state: `met`, `unmet` (evidence against, or an earlier criterion it needs is unmet) or
`unknown` (nothing recorded). A criterion that does not apply to an item is left out, not counted as met.

| Area | Criteria (weight) |
|---|---|
| Pipelines | source read completely (2), nothing inferred or machine-drafted (1.5), every activity converted (3), no documented differences (1), schema valid (2), no source problems (1), connectors exist in Fabric (1.5), no retired components (1.5), connections bound (2), deployed (2), ran to completion (3), matches the ADF run (3) |
| Linked services | Fabric has a connector (3), a connection is bound (3), credentials were checked (1.5); a gateway is bound (3) |
| Triggers | converted to a Fabric schedule (3) |
| Spark workloads | code read (2), convertible (3), generated (2), approved (1), ran in Fabric (4) |
| Source clusters | size known (2), no open gap (2), a pool is sized (2), the pool exists (1) |

Order is a dependency order: deploying, running and comparing cannot be evidenced while conversion, schema or bindings are unmet, so those become `unmet`
rather than `unknown`. A pipeline weighs in its area by its activity count. Areas are weighted pipelines 4, linked services 2.5, compute 2, triggers 1; an
area the estate does not have is not scored (a project with no triggers is not given credit for them, or marked down).

Each item gets a label, the worst thing in its way: `blocked`, `failing` (a run failed or differs), `needs_input`, `unproven` or `proven`.
A criterion made unmet only because something it needs is unmet is *gated*: the cause is counted once, on the criterion that holds it up, so a pipeline with an
open binding is `needs_input`, never `failing` (that word needs a run that failed). A linked service no converted pipeline uses yet is `unproven`, not
`proven`: nothing says it is bound or needed.

## Questions

The open criteria are grouped by cause. Each question carries **why it matters** and **the command that answers it**, and is ranked by how many points it
would raise the range if every item in the group turned out well (recomputed, not estimated). Only the direct effect is counted: items blocked behind a
gate stay unmet in that recomputation, so the figure is a floor.

## Honest limits

- Deployment is shown only when a recorded `verify` run exists; a live workspace read is not made here (use `diagnose --workspace-id`).
- If the source changed after the conversion, every `met` becomes `unknown`.
- Weights are judgement, written down above. They set the ordering of effort, not a probability of success.
- Data flows are scaffold only, so a pipeline holding one has an unconverted activity and shows as blocked.
