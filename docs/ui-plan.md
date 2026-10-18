# UI Plan: Streamlit front end for the Migration Accelerator API

**Status:** v1 built on branch `feature/bhavana-ui` (`ui/`, tests in `tests/test_ui.py`) · **Based on:** `simple3957/adf-to-fabric-agent` `main` at commit `617fd7f` (13 Oct 2026).
The API is still changing. Anything marked with an endpoint name must be re-checked against the latest `main` before building.

## 1. Goal
A web UI that walks a user through the full ADF → Fabric migration by calling the **existing local API**
(`src/adf2fabric/api/`). The UI contains **no migration logic and no LLM**. It collects inputs, starts
stages and shows results.

**Decisions so far:** Streamlit · full flow in v1 (sign-in → source → discovery → connections → conversion → deploy).

```
Streamlit UI (ui/, port 8501)  ──HTTP + X-Session-Id──▶  API (python -m adf2fabric.api, 127.0.0.1:8765)
                                                           └─ existing agents → projects/<id>/ files
```

The API already allows CORS from `localhost:8501` (the Streamlit default), and its README expects a
separate Streamlit front end. No frontend exists yet.

## 2. Setup
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev,api]"
pip install streamlit requests
python -m adf2fabric.api            # API on 127.0.0.1:8765  (docs at /docs)
streamlit run ui/app.py             # UI on localhost:8501
```

## 3. Files to add (all new, under `ui/`)
- **`ui/api_client.py`**: one thin `requests` wrapper.
  - The base URL comes from `ADF2FABRIC_API_URL` (default `http://127.0.0.1:8765`). Send `Authorization: Bearer` when `ADF2FABRIC_API_TOKEN` is set.
  - Send `X-Session-Id` on **every** call: one UUID per browser session, kept in `st.session_state`.
  - Turn the error envelope `{error, actions, details}` into one exception that pages display (error plus suggested actions).
  - `wait_job(job_id)`: poll `GET /jobs/{id}?since=<next_event>` every 1–2 s, append `events` to a log box, and return when `state` is `succeeded` or `failed`. Polling is the only way to follow a job; there are no websockets.
- **`ui/app.py`**: the entry point.
  - The sidebar shows API health (`GET /health`), a **Live/Demo** toggle (`GET/POST /mode`) and a project picker (`GET /projects`).
- **`ui/components.py`**: state badges, the findings table, the job log box and the "dry run, nothing changed" banner.
- **`ui/pages/`**: one Streamlit page per screen in section 4.
- **`pyproject.toml`**: add the extra `ui = ["streamlit>=1.35", "requests>=2.31"]`. Also add a "Run the UI" section to the README.

## 4. Screens → API calls

| # | Page | API calls | What to show |
|---|---|---|---|
| 1 | **Sign-in** | `GET /auth/{azure_cli\|fabric_cli}/status`; `POST /auth/{p}/login` (device-code option) → poll `GET /auth/{p}/login/{id}`; `GET /auth/azure_cli/tenants` | Account, tenant and subscription; checks list; the device code and URL from `output`. **No password fields.** Sign-in runs on the API machine. |
| 2 | **Source** | Azure: `GET /azure/subscriptions`, `GET /azure/discover?subscription_id=`. ZIP: `PUT /uploads/zip` (raw bytes, `application/octet-stream`, **not** multipart). Git: URL, revision, subdirectory, token. Then `PUT /session/source` → `POST /test/source` → `POST /projects` (job) → `wait_job` | Factory picker; `SourceSummary` preview (flavor, counts, Synapse-only warning, pipeline list, findings); optional pipeline multiselect; import log. |
| 3 | **Destination** | `GET /fabric/workspaces`, `GET /fabric/capacities`, `PUT /session/destination`, `POST /test/destination` | Workspace picker and test report (`verified` / `not_verified`). |
| 4 | **Discovery** | `POST /projects/{pid}/stages/discovery`, then `.../stages/semantic` (jobs); `GET /projects/{pid}`; `GET /projects/{pid}/linked-services` | Counts (pipelines, linked services, integration runtimes) and the linked service and IR tables. |
| 5 | **Connections** | `GET /connectors`; `GET /projects/{pid}/connections` (cards); `PUT .../{ls}/choice`; `PUT .../{ls}/secrets`; `POST .../{ls}/test`; `POST .../connections/apply` (`dry_run` first); `POST .../connections/workspace-identity`; `GET .../connections/report` | One card per linked service: state badge; endpoint fields and where each value came from; runtime → Fabric path; auth-method dropdown, with unavailable options greyed out and the reason shown; existing matches; missing items, next steps and warnings; test checks; apply results; the markdown report. |
| 6 | **Conversion** | `POST /projects/{pid}/stages/convert` (body `pipelines`) → `wait_job` | `status_counts`; per pipeline: status, `structurally_valid`, `runtime_ready`; `unbound` placeholders; compatibility findings. |
| 7 | **Deploy** | `POST .../stages/deploy_dry_run`, then, after explicit confirmation, `.../stages/deploy_apply`. Body: `workspace_id`, `pipelines`, `with_children`, `allow_unbound=false` | Results table (name, action, item_id, detail, children); a large dry-run banner; apply only behind a confirm checkbox. |
| – | **Jobs** | `GET /jobs?project_id=` | Job history. Jobs live in API memory and are lost when the API restarts. |

**Stage order:** `discovery` → `semantic` → (`reconstruct`, optional) → `convert` → `deploy_dry_run` → `deploy_apply`.
The connections screen only works after `semantic`. Before that the API returns **409**, so show a "Run discovery" button instead.

## 5. Rules the UI must follow
- **Secrets**
  - Send secrets **only** through `PUT .../{ls}/secrets`, using `st.text_input(type="password")`. Clear the field after sending.
  - **Never** put secrets in `choice.values`; the API rejects that with 422. Never store or log them.
  - The API only returns field **names** (`secrets_held`), and secrets expire after 30 minutes. Re-read the cards and prompt the user to re-enter a secret when it's gone.
- **Never ask for Azure or Fabric passwords.** Sign-in is `az login` / `fab auth login` on the API machine.
- **Honest status labels**
  - Show "Tested" only when `state == connection_tested`. Always show `not_verified` next to `verified`.
  - "Converted" doesn't mean runnable. Show `runtime_ready` separately.
- **Deploy is safe by default:** dry run first. `allow_unbound` and schedule enabling are off by default.
- **One job per project at a time** (otherwise 409). Disable the run buttons while a job is running.

## 6. Known API gaps (for the code owner; the UI shouldn't work around these)
- **A generic 500** instead of a helpful message for:
  - a rejected ZIP in `/test/source`;
  - an invalid `fabric_cli` login method;
  - a second login while one is already running;
  - a project name with no valid characters.
- **No endpoints for:** creating a workspace (`WorkspaceBody` exists but is unused), listing or deleting projects and uploads, or downloading converted pipeline JSON.
- **Results not stored after the job ends:** deploy results and connection test results only exist in the job response or in memory.
- **Demo mode:** deploy stages always fail. The `--demo` flag mentioned in `api/__main__.py` isn't implemented.

## 7. Work split
- **Bhavana:** the whole UI: `ui/api_client.py`, all Streamlit pages and components, testing every page against the API in demo mode, and the list of API gaps.
- **API / code owner:** reviews this plan, confirms the endpoints in section 4 still match the latest code, and fixes the API gaps in section 6.

## 8. How to test
1. Start the API and the UI (section 2), then switch the sidebar to **Demo**.
2. With the sample project `demo-estate`:
   - run discovery, then semantic;
   - Connections: pick `sql_login` for `LS_Sales_SQL`, enter the password through the secrets field, test it, then apply as a dry run;
   - run convert, then a deploy dry run. A deploy failure is expected in demo mode.
3. Check the rules:
   - a password sent in a choice gets 422;
   - only secret names are ever shown;
   - after an API restart, jobs are gone but projects remain.
4. `pytest` must pass, including `tests/test_api.py`.
5. Live test later: factory `adf-data-migration-7593` (resource group `adf_to_fabric`), once a licensed Fabric account is available.

## 9. Please check
- Does the API still match section 4 after your latest changes? Is anything renamed, added or removed?
- Do you agree with the `ui/` folder layout?
- Should any screen be dropped or merged for v1, for example Destination merged into Deploy?


## Changes on 2026-10-18 (lag, sign-in, simpler Connections)

Measured against a real machine before the change: on the Connections page every click re-ran the sign-in check (2.1-3.5 s), the Azure resource discovery (6.7 s) and the Fabric workspace list (2.5 s), about 12 seconds per click. The Migration page's data calls were fast (about 7 ms); its problem was layout, because all six connection forms were built on every run even while collapsed.

- **Lag.** `ui/cache.py` keeps the slow lookups for a short time per browser session (accounts 20 s, subscriptions 60 s, discovery 5 min, workspaces 60 s) with a refresh button each; a failed lookup is never kept. The API also keeps the account list for 20 s and drops it on any sign-in or sign-out. Each card on Connections is a `st.fragment`, so a click in one does not redraw the other. The page no longer downloads a web font or uses blur effects.
- **Sign-in.** One account dropdown per side; the Fabric destination can name its account (`PUT /session/destination` with `account`), and deploy uses it too. The tenant ID is under Advanced. A failed token now says what Azure said (for example AADSTS530035, security defaults), and a failed factory lookup is shown, not reported as "No data factories".
- **Connections page.** The hero, the step chips and the one-option "route" step are gone; status shows in one place per card plus one line in the launch bar. The page fits in about 560 px instead of 1250.
- **Connect step.** A table of linked services with one editor for the selected one, instead of an expander with a full form for each. Creating connections is Preview, then Create; the rarely used actions are under More.
