# Testing

Run: `python -m pytest -q` from the repo root. Last full run (2026-10-16): 1068 passed, 1 skipped, about 4.5 minutes.

All tests are offline. They use local git repos, fixtures, and fake connectors and REST clients (`tests/conftest.py`). No test calls Azure or a live Fabric workspace.

Main groups under `tests/`:

- Intake and source intake (`test_intake.py`, `test_source_intake.py`), consultation, repository
- Discovery: ADF, ADF semantic, ADLS, ADLS paths, SQL, SQL structural, pipeline parser and semantics
- Reconstruction (`test_reconstruction_*.py`)
- Conversion: agent, handlers, expressions, compatibility, CDC, triggers
- Connections and connectivity, Azure auth (`test_azure_auth.py`), integration runtimes
- Compute: plan, assessment, deploy; Fabric deploy
- API and UI (`test_api.py`, `test_ui.py`)

Known thin spots: integration-runtime analysis (few tests) and anything that needs a live Fabric run, such as pipeline execution after deploy.

Before saying a new analysis stage works, also run it on the real sample under `projects/`. Synthetic fixtures alone have missed false positives.
