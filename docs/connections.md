# Connections and authentication

Everything the Streamlit UI needs to connect a source, connect a Fabric destination and configure one Fabric connection per
ADF linked service. The UI is a thin client of the FastAPI backend (`python -m adf2fabric.api`, loopback `127.0.0.1:8765`).

## Principles

* **Evidence, not assumption.** A state is "tested" only when a credential was actually exercised. Azure login is not proof of
  Fabric authorization; a TCP reach is not proof of a login.
* **The browser never sees a credential.** Sign-in is `az login` / `fab auth login` on the machine running the backend. No Azure
  password or CLI token cache crosses the API. Secrets typed into a card go to an in-memory, time-limited vault
  (`SecretVault`, 30 min), are never returned, logged, persisted or put in a report; validation errors never echo input.
* **No invented values.** Servers, databases and accounts come from explicit ADF properties; connection strings are never parsed.

## What works (implemented and tested)

| Area | Behaviour | Verified |
|---|---|---|
| Source: Azure CLI | list subscriptions/factories (ARM), validate factory, import | live, read-only |
| Source: Git | HTTPS only, token via env header (never in URL/config), commit SHA verified | unit + fake git |
| Source: ZIP | safe extraction (zip-slip, symlinks, bombs, encrypted, size caps), JSON only, nothing executed | unit |
| Destination: Azure CLI | workspaces, capacities, connections, gateways, validate workspace | live, read-only |
| Destination: Fabric CLI | same service over `fab api`; sign-in states | unauthenticated state live; **authenticated output not yet verified** |
| Cards | SQL, ADLS Gen2, Blob, REST, Databricks; parameterized LS; Key Vault refs; existing-connection matching | unit + real projects |
| Auth methods | SQL login / service principal / workspace identity / existing connection (each with Key Vault variant); storage key / SAS / SP / workspace identity | unit with fakes |
| Test | config -> Key Vault read -> TCP reach -> SQL login / storage call | fakes |
| Apply | create or reuse Fabric connections, merge `conversion/resolutions.json`, dry-run | fakes |
| Integration runtimes | azure / managed VNet / self-hosted / SSIS -> Fabric path | unit + parser |
| Report | `conversion/connections-report.{md,json}` | unit |
| Demo mode | same routes, canned providers, real parsing on a bundled sample | tests |

## Not implemented / cannot be (and why)

* Interactive OAuth and gateway-backed connections cannot be created through Fabric's API: create them in Fabric, then choose
  "Use an existing Fabric connection".
* Granting the workspace identity a role on a data source is outside this tool.
* A self-hosted IR is never assumed reusable: those linked services map to an on-premises gateway the user must set up.
* Deploying with the Fabric CLI session is not supported (deploy uses the Azure CLI sign-in).
* Reachability tests run from this machine; they do not prove a gateway or private-endpoint path from Fabric.
* SQL test is authentication only (no query). Write permissions in Fabric are not probed.
* Linked-service types outside the catalog are reported "configure manually".

## API (summary)

`GET /health`, `GET|POST /mode`, `GET /auth/{azure_cli|fabric_cli}/status`, `POST .../login`, `GET .../login/{id}`,
`GET /azure/subscriptions|discover`, `PUT /uploads/zip`, `POST /test/source|destination`, `PUT /session/source|destination`,
`GET /fabric/workspaces|capacities`, `GET|POST /projects`, `GET /projects/{id}/linked-services`, `GET /connectors`,
`GET /projects/{id}/connections`, `PUT .../{ls}/choice`, `PUT .../{ls}/secrets`, `POST .../{ls}/test`,
`POST .../apply`, `POST .../workspace-identity`, `GET .../report`, `POST /projects/{id}/stages/{stage}`, `GET /jobs[/{id}]`.

Headers: `X-Session-Id` (scopes secrets and selections), optional `Authorization: Bearer` when `ADF2FABRIC_API_TOKEN` is set.
CORS allows `ADF2FABRIC_ALLOWED_ORIGINS` (default the local Streamlit origins). Long work (import, stages) is a job; poll `/jobs/{id}`.
