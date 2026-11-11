# Testing

Run: `python -m pytest -q` from the repo root.

**No test uses a stand-in (since 2026-11-11).** Every test runs the tool's real code. Tests that replaced Fabric, Azure, Databricks,
SQL, Git or the API with a fake object, a mock or `monkeypatch.setattr` were removed, and so was demo mode. Tests still build real
local git repositories, write real files under `tmp_path`, read the ADF source samples in `tests/fixtures/`, and set environment
variables (`monkeypatch.setenv`).

## Real calls (`tests/test_live.py`)

These run on **every** `pytest` and need the machine's `az login` and the network. They only read: ARM GETs, Fabric GETs and
`getDefinition`. The validation report they build stays in memory and never replaces the project's own.

| Test | What it proves |
|---|---|
| Fabric workspace | an `az` account signed in on this machine reads the workspace, and the workspace is on a capacity |
| Static, API and deployment validation | the real project validates with no failed or blocked required check against the real workspace |
| Items exist | every notebook, pipeline, lakehouse and connection id the generated pipelines use exists there |
| Runtime | a run is counted only when it was made in this workspace, and "verified" is never claimed without one |
| ADF factory | the factory the project was read from answers to `az login`, and its pipelines are exactly the project's source pipelines |

Which project and workspace:
* `ADF2FABRIC_LIVE_PROJECT`: the project under `projects/` (default `adf2fabricv1`).
* `ADF2FABRIC_LIVE_WORKSPACE`: the Fabric workspace id (default: the workspace of the project's last deployment).

The Fabric account is whichever signed-in `az` account can read that workspace. No tenant, workspace or subscription id is
written in the tests.

These tests fail when there is no sign-in, no network, or when the real state is not what the project says. For example, the
ADF test fails when someone changed the factory after the project last read it.

## What is no longer tested

The removed tests were the only checks of these behaviours. Nothing tests them now:

* error handling: 401, 403 and 404 answers, timeouts, retries, a workspace with no capacity;
* writes: deploy, create a notebook, create a connection, provisioning, triggers, `git-commit`.
  Real write tests were not added, because only reads were agreed.
* the API's route-by-role permission table (`test_api_security.py`), the API routes and the UI pages that ran on demo data;
* SQL Server, ADLS and Databricks discovery, and source intake from ARM.

## Before saying a stage works

Run it on the real project under `projects/`, not only on the fixtures in `tests/fixtures/`. Synthetic fixtures alone have missed
false positives.
