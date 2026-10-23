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
