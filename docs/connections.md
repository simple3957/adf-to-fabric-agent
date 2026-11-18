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
| Source: Git | HTTPS only, token via env header (never in URL/config), commit SHA verified | unit (fake-git tests removed 2026-11-11) |
| Source: ZIP | safe extraction (zip-slip, symlinks, bombs, encrypted, size caps), JSON only, nothing executed | unit |
| Destination: Azure CLI | workspaces, capacities, connections, gateways, validate workspace | live, read-only |
| Destination: Fabric CLI | same service over `fab api`; sign-in states | unauthenticated state live; **authenticated output not yet verified** |
| Cards | SQL, ADLS Gen2, Blob, REST, Databricks; parameterized LS; Key Vault refs; existing-connection matching | unit + real projects |
| Auth methods | SQL login / service principal / workspace identity / existing connection (each with Key Vault variant); storage key / SAS / SP / workspace identity | unit (tests with stand-ins removed 2026-11-11) |
| Test | config -> Key Vault read -> TCP reach -> SQL login / storage call | not tested: its stand-in tests were removed 2026-11-11 |
| Apply | create or reuse Fabric connections, merge `conversion/resolutions.json`, dry-run; never creates a name twice (below) | not tested: its stand-in tests were removed 2026-11-11 |
| Integration runtimes | azure / managed VNet / self-hosted / SSIS -> Fabric path | unit + parser |
| Report | `conversion/connections-report.{md,json}` | unit |

## Only what the converted pipelines use, decided by the tool (2026-11-17)

The user's rule: wherever a Fabric notebook or another workaround replaces an activity, the tool shows that and does not ask a person
to create anything. `connections/needs.py` decides, per linked service, whether a converted Fabric activity still uses it: from the
conversion record when it is current, else from the converter run in memory (`ConversionAgent.forecast`, nothing written, about half
a second on the real estate), so Connect knows before Convert has run.

* **Not needed** (state `not_needed`, counted as ready): no converted activity points at it. Its activities became something the tool
  creates (a Fabric notebook, a Spark job definition, a lakehouse read through a OneLake shortcut), are not convertible at all (U-SQL:
  a connection would not help), or nothing uses it. The card says which activity became what, and has no form; an earlier choice stays
  saved but is not shown. Plan does not plan a connection for it and validation does not count it as missing.
* **Set up by the tool** (`card.planned`, origin `auto_matched` / `auto`), when nobody has chosen: exactly one existing Fabric
  connection with the same type and address is reused; ADF's managed identity becomes the workspace identity (no secret; the identity
  still needs a role on the source); ADF's Key Vault secret is read from the same vault with the person's Azure sign-in when Apply runs.
  *Create in Fabric* carries it out and saves it as the choice; *Choose differently* opens the form.
* **Needs you**: none of the above applies. Typically ADF keeps the secret encrypted inside the factory (no tool can read it) and no
  existing connection has the address. Two connections with the same address are never picked between.

On the real estate (adf2fabricv1, 2026-11-17) only `Sathvik_ADF` is needed (Copy, Lookup and Get Metadata activities) and is set to
reuse `storagebrr`; `AzureDatabricks1` (jobs became notebooks), `LS_HDInsight_Test` (became a Spark job definition) and
`ADLA_Retired` (U-SQL, not convertible) are not needed. A Databricks notebook activity left on Databricks (the default without a
Spark choice) still needs its connection.

## Applying again

A create looks up the connection's display name (case-insensitive) in what Fabric lists first. The same name with the same type and
address is reused ("reused", bound, nothing created): a second apply, or one made by hand. The same name with another type or address is
refused, and the person chooses it as the existing connection or gives another display name. A parameterised linked service's variants
(`name-2`, `name-3`…) are checked one by one, so only missing ones are created. A connection created earlier in the same apply counts too.
(Added 2026-11-05; before that every apply posted a new connection.)

## Existing connections offered for a linked service

Each card lists the Fabric connections of the same type that may fit: **exact** (same address) or **partial** (same server, another
database / container / path; reuse only if its credentials cover it). Only exact matches are bound automatically. For a connection
identified by a URL (Databricks, REST) the server is the URL's host. Until 2026-11-10 it was the text before the first `/`, which is
`https:` for every URL, so any Databricks connection was offered for any Databricks linked service (found live: a workspace's
linked service was offered, and bound by hand to, another workspace's connection).

Choosing **Skip** for a linked service also removes its earlier bindings (its own and its variants' `<name>?...` keys) from
`conversion/resolutions.json`, so convert no longer uses a connection the card says is not bound (fixed 2026-11-10; before that the
binding stayed until it was edited by hand).

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

## Coverage today and what the next connectors need

The conversion report has a **Connector coverage** section: for each linked service, whether this tool can create the Fabric connection (`created`), Fabric has a connector but the connection is made by hand (`manual`), there is no Fabric connector (`unsupported`), or the type is not named in the Fabric documentation consulted (`unlisted`, so verify it). The tool creates 5 connector types (SQL/Synapse SQL, ADLS Gen2, Blob, REST, Azure Databricks) covering 9 ADF types, and recognises 53 more as supported by Fabric.

Fabric's own list of connection types (`GET /v1/connections/supportedConnectionTypes`, 287 types, read on 2026-10-16) gives the exact creation method, required parameters and credential types. For the likely next connectors:

| Fabric type | Creation method | Required parameters | Credential types |
|---|---|---|---|
| `PostgreSQL` | `PostgreSql` | server, database | Basic, OAuth2 |
| `MySql` | `MySql` | server, database | Basic |
| `Oracle` | `Oracle` | server | Basic, OAuth2 |
| `Snowflake` | `Snowflake.Databases` | server, warehouse | Basic, OAuth2, KeyPair, ServicePrincipal, WorkspaceIdentity |
| `AmazonS3` | `AmazonS3.Storage` | none (url and roleArn optional) | Basic, OAuth2, ServicePrincipal |
| `SFTP` | `SFTP.Contents` | server | Basic, KeyPair |
| `Web` | `Web` | url | Anonymous, Basic, OAuth2, ServicePrincipal, WorkspaceIdentity |
| `CosmosDB` | `CosmosDB.Contents` | host | Key, OAuth2, WorkspaceIdentity |

Not yet verified for these: how an ADF linked service's properties map onto the parameters (for example whether Fabric's `server` carries the port, and how an Oracle connection string becomes `server`), and creation against a real system. Those need a live target for each. ADF types with no obvious Fabric name in that list (Teradata, ODBC, file server) are not assumed unsupported.
