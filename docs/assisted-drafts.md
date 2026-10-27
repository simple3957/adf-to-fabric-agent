# Model-assisted drafts (optional)

Some pipelines the deterministic converter cannot finish: an expression that uses `@trigger()`, a value Fabric cannot resolve, an activity whose store
cannot be told. For those, a person can ask a model for a **draft of corrected ADF source**, review it, and accept it. This is optional, **off by
default**, and it does not change the project's principle: **the deterministic converter is still the only thing that produces Fabric JSON.** The model
never writes Fabric files, and nothing it says is used as it comes.

## The flow

```
adf2fabric convert --project P                     # the converter says what it could not do
adf2fabric assist-candidates --project P           # which pipelines, and why (and whether the feature is on)
adf2fabric assist-prompt --project P --pipeline X  # exactly what would be sent. Sends nothing.
ADF2FABRIC_LLM=on adf2fabric assist-draft --project P --pipeline X
adf2fabric assist-show --project P --pipeline X --code
adf2fabric assist-accept --project P --pipeline X [--file edited.json]
adf2fabric convert --project P --assisted          # the converter reads the accepted source instead of the original
adf2fabric assist-discard --project P --pipeline X [--all]
```

The same flow is on the Convert step (a section **Model-assisted drafts (optional)**, shown only when something is left for a person) and in the API
(`/projects/{id}/assist/...`, permissions `reports:view` to read and `plans:generate` to draft, accept and discard; each action is audited).

## What keeps it safe

* **Off by default.** `ADF2FABRIC_LLM=on` switches it on. Until then nothing about a project leaves the machine and `assist-draft` refuses.
* **The model can only answer.** It is reached through the Claude Code CLI in headless mode (`claude -p`) with every tool switched off, no MCP servers, no
  saved session, from an empty temporary folder, and a JSON schema for the answer. It cannot read this project's files, run commands or reach Azure or
  Fabric, and this tool never holds an API key (it uses the sign-in the CLI already has).
* **What is sent.** The pipeline's own JSON with secret-looking values removed (the sanitiser discovery uses), what the converter did with it, the names and
  types of the datasets, linked services and pipelines it uses, and your `prompts/assist/project.md`. Never a linked-service definition, a credential, a
  connection string or any data. `assist-prompt` (and **Preview what is sent**) shows the whole prompt.
* **The draft is source, not Fabric.** It is corrected ADF pipeline JSON that goes back through the converter (the approach is "make it a shape the converter
  accepts"). The model is told what it may change (an unsupported expression, a dynamic value, a store the converter cannot tell) and what it must not
  (names, types, `dependsOn`, existing parameters and variables, reference names), and not to invent a replacement for an activity with no Fabric equivalent.
* **Automatic checks, run by the project's own converter.** The draft must be valid JSON with the same pipeline name; keep every activity, type and dependency;
  not remove or rename a parameter or variable; not refer to anything that is not in the source; hold no secret; and the converter, run on the draft, must not
  make anything worse (an activity that needed no person must not start needing one). A draft with an error cannot be accepted. A draft that does not help
  is flagged. Additions (a new parameter, an extra activity) are warnings to read.
* **A person accepts.** The reviewer sees the problems, the model's notes, what it left for a person, its confidence, the checks, and the draft next to the
  original, and can edit the draft (an edit is checked again). Accepting records who and when.
* **Pinned to its source.** An accepted draft is tied to a hash of the source it was made from. If the source changes, it is ignored (the original is converted)
  and the log says so; a stale draft cannot be accepted.
* **Opt-in again at conversion.** Only `convert --assisted` (or the option on the Convert step) uses accepted drafts. The original file in the repository is
  never modified.
* **Marked everywhere.** The conversion record lists each assisted pipeline (`assisted`), the report has a **Model-assisted pipelines** section, validation
  compares that pipeline with the accepted source and adds an informational check `source_inventory.assisted_source`, and the pipeline report's confidence is
  capped at 85 with the reason. None of this is a success claim: only a run compared with a baseline shows the edit kept the pipeline's behaviour.

## Settings (environment variables for the API or CLI process)

| Variable | Default | Effect |
|---|---|---|
| `ADF2FABRIC_LLM` | `off` | `on` allows drafts to be generated. Accepted drafts are used by `convert --assisted` either way. |
| `ADF2FABRIC_LLM_MODEL` | Claude Code's default | Passed to `claude --model`. |
| `ADF2FABRIC_LLM_TIMEOUT` | `180` | Seconds allowed for one draft. |
| `ADF2FABRIC_LLM_WORKERS` | `2` | Drafts generated at the same time (API). |
| `CLAUDE_CLI` | `claude` on `PATH` | Path to the CLI. |
| `ADF2FABRIC_PROMPTS_DIR` | `prompts/assist` | Where the templates are read from. |

`prompts/assist/base.md` is the frame of every prompt, `pipeline.md` says how to fix a pipeline, and `project.md` holds the migration team's requirements (they
take priority). The files support `{{placeholders}}` (`object.name`, `target.workspace`), `{{#if x}} ... {{else}} ... {{/if}}` and `<!-- comments -->`; edits apply
to the next draft with no restart.

## Where it can fail

* Claude Code must be installed and signed in on the machine running the tool, or generation is unavailable.
* **The pipeline's definition leaves the machine** (to Anthropic, through Claude Code) when a draft is generated. Some clients will not allow that: leave it off.
* The checks test structure and what the converter makes of the draft, not the business logic. The human review is the real safeguard, and a run compared with a
  baseline is the real proof.
* A draft can only fix what ADF source can express. An activity with no Fabric equivalent (SSIS, U-SQL, classic Azure ML, mapping data flows, HDInsight programs,
  Databricks jar/python/job) is left, and listed, for a person.
* Pipelines only. Notebooks and other objects are not drafted by a model here; the Spark workload migration keeps its own deterministic generation and approval.
* Each draft has a time limit (180 s by default) and drafts run a few at a time, so a long pipeline can time out and hundreds of pipelines are slow.
* A repository that is later fixed by hand makes the draft stale. Make the same change in the ADF source so the repository and Fabric agree.

## How this relates to the Synapse accelerator it was modelled on

The concepts come from `maneesha26625/synapse_fabric_migration_project` (its `llm/` package, `api/transform.py` and `prompts/transform/`). What was carried over and
what was changed to fit this project:

| Concept there | Here |
|---|---|
| Claude Code CLI, headless, no tools, empty folder, JSON schema | Same call, off by default (there it is on by default) |
| Prompt from Markdown templates plus per-project `project.md`, rendered without a library | Same, in `prompts/assist/` |
| Prompt carries the source, the rule output, what the rules could not solve, dependencies | Source (secrets removed), the converter's findings, names and types of what it uses |
| Draft returns code, notes, unconverted, confidence | Same shape |
| Automatic checks that block acceptance | Same idea; the project's converter is run on the draft and must not make it worse |
| Output is source-shaped and goes back through the deterministic converter (pipelines) | Always: the draft is ADF source, never Fabric JSON |
| Accepted draft pinned to a hash of its source; stale means the rules are used | Same |
| A run uses accepted drafts, and the run log marks them | Only with `--assisted`; the conversion record, report, validation and confidence all mark them |
| Draft kinds: procedures, views, tables, scripts, pipelines, notebooks | Pipelines only (the objects this project converts with a model-free engine) |
| Demo mode with canned drafts | Not needed: the tests use a fake model |
| Chat assistants that fall back to simple commands without a model | Not built: this project has no chat layer |
