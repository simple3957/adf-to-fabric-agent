# Guided UI: steps, states and safety rules

The Streamlit UI (`ui/`) holds no migration logic. It calls the API through `ui/api_client.py`; everything it shows has been redacted and
authorised by the server (see [security.md](security.md)).

## The nine steps

| # | Step | Backend | Done when |
|---|---|---|---|
| 1 | Discover | `discovery`, `semantic` stages | semantic record exists |
| 2 | Review | `GET /inventory`, `/compatibility`; `dependencies`, `workloads` stages | the person has read the inventory |
| 3 | Consult | `GET/PUT /brief`, submit, approve | the current answers are approved; the run follows the brief |
| 4 | Connect | connection cards, tests, apply | every linked service has a verdict |
| 5 | Plan target | `/provisioning` plan, approve, dry run, apply, verify | a current plan exists; apply only after approval and a dry run |
| 6 | Convert | `convert` stage | pipelines generated |
| 7 | Validate | `validate` stage | a validation result exists |
| 8 | Deploy | `deploy_dry_run`, `deploy_apply` | dry run done; apply confirmed |
| 9 | Report | `GET /report` | always readable; "verified" only if the backend says so |

Sign-in (token mode) and project access come first: no project data is requested before `/me` succeeds. A step stays locked until the
steps before it are confirmed and complete.

## Pipeline states

`discovered` (read and understood), `inferred` (something dynamic or defaulted: confirm it), `unsupported`, `blocked`, `converted`
(generated, nothing more claimed), `deployed` (a real deploy is on record), `runtime_validated` (a recorded run matched its baseline and
completed). A state moves only on evidence; a dry run is not a deployment and a differing run is not validation.

## Waves, execution order and step options (after the Synapse accelerator's UI)

The look and layout follow the Synapse accelerator's frontend (its palette, stat strip, wave rows and stage-card options), kept inside
the existing nine steps and flows.

* **Review, "Waves & order" tab.** A summary strip (objects, dependencies, waves, connected objects, circular groups), then three
  tabs: *Waves* (a row per wave with what it holds, sized to the largest, and the objects in a chosen wave), *Execution order*
  (the order pipelines are deployed in: children before the pipelines that call them, with what each calls, what calls it, whether it
  is ready, what it waits for and its state) and *Explorer* (search and filter by type or wave, pick an object to see what it needs
  and what needs it). Built on the server from the resolved references (`GET /projects/{id}/waves`, `reports:view`); it needs
  'Resolve dependencies' first and says so.
* **Options cards on the steps that have real choices.** Each says where its defaults come from (the approved consultation brief,
  or the tool's own), and a choice made there is for that run only and beats the brief:
  * *Conversion*: which pipelines, keep connections or repoint to a lakehouse (name checked as you type), apply the safe
    reconstruction proposals.
  * *Validation*: which checks run (static, API, deployment, run evidence); API and deployment need a workspace.
  * *Deployment*: which pipelines, include child pipelines, allow unbound connections.
  * *Run and compare*: which pipelines and how long to wait for each run.
  The server checks every choice again (names, convert-only choices, the real-run rules).

## Each question is asked once

- **Pipeline scope.** Convert and Deploy share one scope per project (`options.scope_picker`): the approved brief's scope is the first default, a choice made in
  one step is where the next starts, and an empty scope means all. The picker stays closed unless someone opens it. Running real pipelines is the exception:
  it is never prefilled, because running everything is never the default.
- **Linked services of one type.** On Connect, a sign-in method and details saved for one service are offered to the other not-yet-ready services of the same
  connector type (`panels.share_targets`), ticked by default and listed. Each keeps its own server or account from the source. An existing-connection id is
  never shared (it belongs to one account), and a secret typed once is held for each service that takes the answer.
- **Not merged, on purpose.** The typed confirmations (CREATE, DEPLOY, APPLY, RUN) stay on every step that changes Fabric: they are the protection, not a
  repeated question.

## What the UI does not cover yet (CLI only)

Checked against the API routes and the CLI on 2026-10-29. Everything the API exposes has a UI use except the admin routes; these CLI features have no UI:

| Feature | CLI | Why it matters for "everything intact" |
|---|---|---|
| Generate, approve, deploy and record a run of Spark notebooks and jobs | `workloads-generate`, `workloads-approve`, `workloads-deploy`, `workloads-record-run` | The UI only assesses them (Review > Compute) |
| Deploy proposed Spark pools and environments | `compute-deploy` | Review shows the proposals; creating them is CLI |
| Storage map, OneLake shortcuts | `storage-map`, `create-shortcut` | Needed before Convert can repoint storage |
| Notebook deploy, Git commit | `deploy-notebook`, `git-commit` | Notebooks are not deployed by Deploy |
| Diagnosis (fix-first ranking) | `diagnose` | The Report step shows states, not the ranked fixes |
| Users and audit log | `auth-user-*`, `audit-verify` (the API has admin routes, no UI) | Administration |

## Things that go wrong in use, and what the UI does

* **Reload, new tab, or a long job.** Step confirmations are kept on the server per project, and the Migration page shows a job that is
  already running (with a button to watch it), so nothing is lost and a second job is not started by mistake. A job outlives the
  page's own 30-minute wait.
* **Text from another person** (notes, the owner's name, free-text answers) is shown as text, never as a link, image or HTML.
* **A deploy for another workspace than the approved brief** is refused with a banner and a one-click way to review it again.

## Safety rules

* Dry runs are labelled "nothing is changed"; real runs are labelled "APPLIED". Real provisioning needs a dry run, then typing `APPLY`;
  deploy needs a dry run of the same pipelines, then typing `DEPLOY`; connection creation needs `CREATE`. The API requires `confirm: true`
  as well, so the UI is not the only gate.
* Report → *Run in Fabric and compare* starts REAL runs of up to 20 chosen, deployed pipelines (they read sources and write
  targets) after typing `RUN`; it needs `migration:execute`. A pipeline becomes runtime-validated only when its run completes and
  matches the ADF baseline; a completed run without a baseline is not proof.
* Buttons are disabled while a job runs; each intent carries an `Idempotency-Key`, so a double click starts one job.
* Errors show the server's short message, what to do, and a request reference for 5xx; never a trace or a path.
* Demo mode is labelled on every page and is refused in production mode. Production never shows sample data.
* Completion is the backend's word: the final banner says "Migration verified" only when the validation engine reports `MIGRATION_VERIFIED`
  (a passing runtime stage); otherwise it says the migration is not verified.

## Tests

`tests/test_api_views.py` (authorisation, redaction, failure states, progress, duplicate submissions, production mode),
`tests/test_ui_enterprise.py` (panels with real demo data, typed confirmation, sign-in gate, viewer, production), `tests/test_ui.py`.

## Limits

Provisioning writes and Azure capacity creation have only been exercised against the in-memory demo target, never a live tenant.
