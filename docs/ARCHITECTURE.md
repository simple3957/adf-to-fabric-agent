# Architecture

One-page map. Each stage reads the previous stage's output from `projects/<name>/` and writes its own. Detail is in the linked docs.

```text
 Source (GitHub/Git | ZIP | Azure ARM)
        |
   1. Intake ............ agent/intake            docs/intake.md
        |                 fetch, validate, manifest
   2. Consultation ...... agent/consultation      docs/consultation.md
        |                 readiness gate
   3. Discovery ......... agent/discovery         docs/pipeline-parser.md
        |                 ADF artifacts, semantic parse, optional ADLS / SQL probes
   4. Reconstruction .... agent/reconstruction    docs/reconstruction.md
        |                 detect incomplete pipelines, propose fixes
   5. Conversion ........ agent/conversion        docs/conversion.md, conversion-failure-modes.md
        |                 ADF -> Fabric pipeline JSON, triggers -> schedules
   6. Assessment ........ agent/assessment        docs/compute-plan.md
        |                 compute plan, Fabric CU / capacity estimate
   7. Deploy ............ agent/deployment        docs/deploy.md
                          pipelines, compute pools/environments, schedules
```

Cross-cutting:

- **Connections** (`connections/`, `connectivity/`): linked-service verdicts, credential cards, connection creation. See `docs/connections.md`, `docs/linked-service-mapping.md`.
- **Project store** (`project/`): models and on-disk state, so any stage can be re-run.
- **API** (`api/`) wraps the stages as jobs. The Streamlit UI (`ui/`) calls only the API. See `docs/ui-plan.md`.
- **Scope**: `docs/migration-capability.md` is the source of truth for what is supported.

## Conversion model

1. A handler registry maps each ADF activity type to a Fabric activity (`handlers/`: control, data, sql_web, compute).
2. Expressions are rewritten (`expr_convert.py`); global parameters become `pipeline().parameters.gp_<name>`.
3. Dataset and linked-service references are resolved and inlined as `datasetSettings`.
4. Anything not convertible becomes a blocker with a reason. Output is validated (`validation.py`) before deploy.

## Deploy model

Child pipelines deploy first so parent `ExecutePipeline` activities get real item ids. Dry run is the default; `--apply` writes. Unbound workspace item placeholders block the deploy unless overridden. Compute deploy is a separate command (`compute-deploy`).

## Where the code is ahead of or behind the docs

See [status-briefing.md](status-briefing.md).
