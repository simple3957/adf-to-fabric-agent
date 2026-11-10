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
| 7 | Deploy | `deploy_dry_run`, `deploy_apply` | dry run done; apply confirmed |
| 8 | Validate | `validate` stage | a validation result exists |
| 9 | Report | `GET /report` | always readable; "verified" only if the backend says so |

Ids are kept per workspace. The notebook, child-pipeline and lakehouse ids saved for one workspace are never used in another:
"Things you need to set" shows the chosen workspace's own ids, Convert with a workspace chosen sets the other workspace's ids aside
(`conversion/bindings-by-workspace/<workspace>.json`) and brings back this one's, and a deploy of a conversion made with another
workspace's ids is refused (dry run included) with what to do. Connections are not tied to a workspace and stay.

When the approved brief was agreed for another workspace than the one chosen, the server refuses a real deploy and real runs there; the
Deploy step says so before the dry run (`GET /brief` carries `deploy_refused`), keeps **Deploy** disabled with that reason, and the dry run's
warnings repeat it. Fix: Consult, 'Use the workspace chosen now' and save again; or choose the workspace the brief was agreed for.
Likewise `runs_refused` (that reason, or a brief whose validation depth does not allow real runs) is shown at the top of
**Run in Fabric and compare**, whose pipeline list and **Run and compare** stay disabled, so nobody chooses pipelines and types RUN only to be refused.

Deploy comes before Validate (since 2026-11-06), so validation checks what is deployed (its API and deployment stages read the workspace).
On Convert, "Things you need to set" lists each notebook a pipeline runs; for a notebook the tool generated, **Create it in Fabric from the
generated notebook** first asks Fabric what would happen (a new notebook, or the content of the one with that name replaced), then **Yes,
create** (or **Yes, replace it** when one with that name exists) puts it in the chosen workspace, records the person as accepting it and saves its id; Convert again uses the id
(`POST /projects/{id}/workloads/notebooks/create`, `migration:execute`, and `provision:approve` to confirm; audited as `workload.create`).

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
* **"Why, and what to do" under Convert, Validate and Deploy.** After every convert, deploy, run and validation the server writes the
  diagnosis again from the project's records (no Fabric read). Each step shows, as escaped text in a bordered box, the pipelines whose problem
  it can fix (Convert: not converted, unbound; Validate: those plus open final checks; Deploy: unbound, deploy failed, not deployed,
  differs), each with its *Why:* and *What to do:* lines.
* **Deploy and Report, "Migration results".** After deploy, from the project's records only (`GET /projects/{id}/run-results`,
  `reports:view`, redacted, no run, job or item id; built by `agent/deployment/run_report.py`): figures for deployed, ran, matching
  the baseline, checks matched and rows copied; ADF beside Fabric for rows read and copied and files read and written, over the pipelines
  whose baseline figures are kept; a table per pipeline (deploy time, Fabric run, what it was compared with, result, steps OK, checks,
  differences, rows copied on both sides, output checks) with a CSV download; and, for a chosen pipeline, each activity with its status,
  runs, rows and files as *baseline / Fabric* and the result of the comparison. A deploy to another workspace and a run older than the
  latest deploy do not count. `verify` keeps the baseline run's own figures (`baseline_activities`) since 2026-11-06; a run compared
  before that keeps its result and checks, and its baseline columns are empty.
* **Report, "What to fix first".** Runs `diagnose` as a job (`POST /projects/{id}/diagnosis`, `plans:generate`, read-only, audited;
  with a workspace chosen it also reads what is deployed and the latest runs) and shows the ranked fixes, where each pipeline stops, and
  the connections still to sort out (`GET /projects/{id}/diagnosis`, `reports:view`, redacted). See [troubleshooting.md](troubleshooting.md).
* **Report, "Check a pipeline's output".** For a pipeline with no ADF run to compare with: pick the pipeline, give the source data and
  what the run wrote (abfss locations, CSV or Parquet), and the server compares them as a job (`POST /projects/{id}/output-checks`,
  `plans:generate`, read-only, audited; `GET` lists the verdicts and row counts, never a location). Only an ADLS Gen2 account or OneLake
  is read, a OneLake workspace must be in the caller's grants, and demo mode refuses it. OneLake is read as the Fabric account, ADLS as
  the source's sign-in. Same rules as the CLI's `output-check` ([migration-validation.md](migration-validation.md)).

## Loading and "what is needed from you"

* **Loader.** While the script runs (every click reruns it) the page shows a rotating ring top right and a thin bar across the top, and the old content stays
  steady: Streamlit's fade of stale elements is switched off (`[data-stale]`), and its small status widget is hidden. A read that is not already kept shows
  `Reading <name>…` (`components.read`); jobs keep their `st.status` log. Reduced-motion settings slow the animation.
* **Needed from you.** A strip over every step says in one sentence what that step needs (`journey.needed`): the step to confirm first, what is still to run, what
  to look at before continuing, or nothing. The words come from the step's facts, so they cannot disagree with the step.
* **Overview.** A project that has been worked on (some steps confirmed, not all) opens on one card: steps done, pipelines proven by a real run, what is open and
  a button to continue. Choosing any step replaces it. Result tables with nothing to look at (every pipeline converted and ready) are collapsed.

## Each question is asked once

- **Pipeline scope.** Convert and Deploy share one scope per project (`options.scope_picker`): the approved brief's scope is the first default, a choice made in
  one step is where the next starts, and an empty scope means all. The picker stays closed unless someone opens it. Running real pipelines is the exception:
  it is never prefilled, because running everything is never the default.
- **Linked services of one type.** On Connect, a sign-in method and details saved for one service are offered to the other not-yet-ready services of the same
  connector type (`panels.share_targets`), ticked by default and listed. Each keeps its own server or account from the source. An existing-connection id is
  never shared (it belongs to one account), and a secret typed once is held for each service that takes the answer.
- **Not merged, on purpose.** The confirmations stay on every step that changes Fabric (typed CREATE, APPLY, RUN; a second click for Deploy): they are the
  protection, not a repeated question.

## What the UI does not cover yet (CLI only)

Checked against the API routes and the CLI on 2026-10-29. Everything the API exposes has a UI use except the admin routes; these CLI features have no UI:

| Feature | CLI | Why it matters for "everything intact" |
|---|---|---|
| Generate, approve, deploy and record a run of Spark notebooks and jobs | `workloads-generate`, `workloads-approve`, `workloads-deploy`, `workloads-record-run` | The UI assesses them (Review > Compute) and, on Deploy, binds a notebook already in the workspace to its generated notebook (Check / Bind; see [workload-migration.md](workload-migration.md)) |
| Deploy proposed Spark pools and environments | `compute-deploy` | Review shows the proposals; creating them is CLI |
| Storage map, OneLake shortcuts | `storage-map`, `create-shortcut` | Needed before Convert can repoint storage |
| Notebook deploy, Git commit | `deploy-notebook`, `git-commit` | Notebooks are not deployed by Deploy |
| Users and audit log | `auth-user-*`, `audit-verify` (the API has admin routes, no UI) | Administration |

## Things that go wrong in use, and what the UI does

* **Reload, new tab, or a long job.** Step confirmations are kept on the server per project, and the Migration page shows a job that is
  already running (with a button to watch it), so nothing is lost and a second job is not started by mistake. A job outlives the
  page's own 30-minute wait.
* **Text from another person** (notes, the owner's name, free-text answers) is shown as text, never as a link, image or HTML.
* **A deploy for another workspace than the approved brief** is refused with a banner and a one-click way to review it again.

## Safety rules

* Dry runs are labelled "nothing is changed"; real runs are labelled "APPLIED". Real provisioning needs a dry run, then typing `APPLY`;
  deploy needs a clean dry run of the same pipelines and options, then **Deploy** and **Yes, deploy** (the question names how many
  pipelines; **Cancel** deploys nothing; no word is typed since 2026-11-06, at the user's request); connection creation needs `CREATE`. The API requires `confirm: true`
  as well, so the UI is not the only gate.
* Report → *Run in Fabric and compare* starts REAL runs of up to 20 chosen, deployed pipelines (they read sources and write
  targets) after typing `RUN`; it needs `migration:execute`. A pipeline becomes runtime-validated only when its run completes and
  matches the ADF baseline; a completed run without a baseline is not proof.
* Buttons are disabled while a job runs; each intent carries an `Idempotency-Key`, so a double click starts one job.
* Errors show the server's short message, what to do, and a request reference for 5xx; never a trace or a path.
* Demo mode is labelled on every page and is refused in production mode. Production never shows sample data.
* Completion is the backend's word: the final banner says "Migration verified" only when the validation engine reports `MIGRATION_VERIFIED`
  (a passing runtime stage); otherwise it says the migration is not verified.

## No dead ends (found by running the real workflow, 2026-10-29)

* **Connect**: a matching connection that already exists in Fabric is bound by *Preview / Create* with "reuse" ticked (the default), and the choice is
  saved, so the card reads "ready" and the step can finish. The step opens on the first service that can be set up, not on an unsupported one.
* **Plan target**: creating a Spark pool or environment is optional for pipelines, so unapplied items let the step be passed with a warning; real
  blockers still stop it.
* **Deploy**: a dry run that refuses pipelines (an unbound connection) offers "Leave out the refused and dry-run the other N". A dry run on its own
  lets the step be passed with the headline "Dry run done, nothing deployed yet"; the Report still shows those pipelines as not deployed. The real
  deploy stays locked until a clean dry run of the same pipelines *and options* (the server compares them).
* A deploy made earlier is read back from the server, so a reload does not say "Deploy needs to run again". It counts only for the workspace it went to:
  with another workspace chosen in Connections, Deploy says "Not run yet" and the Report shows those pipelines as converted, "deployed to another Fabric workspace".
* A run older than the pipeline's latest deploy never counts as proof (`verify.current_record`).
* **Notebooks the pipelines run** (Deploy): an id box per generated notebook, filled with the id the pipeline runs, and **Check** / **Bind**.
  Bind needs `provision:approve` and records the person as accepting the notebook; the first check can take half a minute (Fabric reads the
  definition asynchronously).
* **Connection ids** (Connect: "existing connection"; Convert: Invoke-pipeline and lakehouse): a pasted id is normalised (spaces, `{braces}`, capitals,
  `catalog.normalize_guid`). An existing connection must be the type the linked service needs. The Convert step checks a connection against the ones the
  account can see: a wrong type is refused, an id it cannot see is saved and shown as unverified.
* **Bindings for another workspace**: the resolutions file records the workspace the notebook, child-pipeline and lakehouse ids were bound in
  (`WorkspaceId *`). When it is not the workspace chosen now, the bindings list, the deploy result and the validation finding ("ids do not exist") all say so,
  naming only the last six characters of each id and what to do. A deploy is warned, not refused: the person may know.
* **A failure that ADF reproduces** shows as "fails like ADF" in the run-and-compare table (`verify` verdict `same_failure`), not as a plain "run failed".
* **Sign-in page**: drawn before, and outside, the top bar. A lockout or service error is a message, never a traceback; a valid token still signs in during a
  lockout, and a request with no token does not count toward it.

## Tests

`tests/test_api_views.py` (authorisation, redaction, failure states, progress, duplicate submissions, production mode),
`tests/test_ui_enterprise.py` (panels with real demo data, typed confirmation, sign-in gate, viewer, production), `tests/test_ui.py`.

## Limits

Provisioning has created a Spark pool and an environment live (workspace `Practise`, 2026-11-03, verified by reading them back). Its other writes (workspace creation, capacity assignment, role assignments, lakehouses, Azure capacity creation) have only been exercised against the in-memory demo target, never a live tenant.
