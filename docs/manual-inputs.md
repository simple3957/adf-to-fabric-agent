# What a person sets by hand, and where

The tool never invents an id, a credential or a decision. This is every input a person supplies, grouped by the step it belongs to, and where the
screen or command is today. **UI** = the Streamlit app, **CLI only** = no screen yet (a person must use the command).

## Before conversion

| Input | Where | How it is asked |
|---|---|---|
| The ADF source: Git URL, branch and token (an env-var name, never the token), a ZIP, or Azure subscription / resource group / factory | UI (Setup) | Tested with a source check before it is used |
| The Fabric destination: signed-in account, tenant, workspace | UI (Setup / Connections) | Listed live from the signed-in account; the workspace's capacity is shown |
| A Fabric capacity assigned to the workspace | Fabric portal or the provisioning plan | Provisioning (UI) plans, asks for approval, applies and verifies; it never changes an existing assignment. See [fabric-provisioning.md](fabric-provisioning.md) |
| The migration brief: scope, held-back pipelines, storage routing, workload choices, approvals | UI (Consult) | Simple mode asks only what changes a run; `ADF2FABRIC_CONSULT=full` asks all of it. See [consultation.md](consultation.md) |

## Connections (per linked service)

| Input | Where | How it is asked |
|---|---|---|
| The sign-in method, endpoint fields and the secret for a connection the tool can create (SQL/Synapse, ADLS Gen2, Blob, REST, Databricks) | UI (Connect) | Secrets are held in memory for 30 minutes, never saved or shown again |
| An existing Fabric connection to reuse (its id) | UI (Connect) | Picked from the connections Fabric lists for that type and address, or pasted. If Fabric cannot be read the step now says why |
| "This one needs no connection" (skip) | UI (Connect) | A decision, recorded |
| Connections for every other type (the tool gives a verdict only) | Fabric portal, then UI (Connect: existing connection) | Created by hand: Settings > Manage connections and gateways |
| An on-premises or virtual-network data gateway for a self-hosted / managed-VNet integration runtime | Fabric portal, then UI (Connect) | The card says which linked services need one; installing and sharing it is manual |
| One connection per distinct argument of a parameterised linked service (`LS?Param=value`) | UI (Connect) | The card lists the values |

## After conversion: ids the converter cannot know

| Input | Where | How it is asked |
|---|---|---|
| The Fabric **notebook**, **Spark job definition** or **dataflow** an activity runs | **UI (Convert: "Things you need to set")** | Pick from the workspace's items (read live) or paste the id; the server refuses an id that is not in the chosen workspace |
| A connection with no linked service behind it: the **Invoke pipeline** connection, a Web activity's base URL | **UI (Convert: "Things you need to set")** | Pick from the connections of the right type or paste the id |
| A child pipeline's id; the workspace id | Automatic | Filled in by `deploy` (children first); nothing to enter |

An answer is stored in `conversion/resolutions.json` (`ItemToId`) and used by the next **Convert**. Linked-service connections are written to the same file
by the Connect step (`LinkedServiceToConnectionId`).

## Spark workloads (opt-in)

| Input | Where | Notes |
|---|---|---|
| Whether notebooks move to Fabric Spark at all | UI (Consult) | Default keeps Databricks |
| Generating the Fabric notebook or Spark job, **approving** it, deploying it, recording its first real run | **CLI only**: `workloads-generate`, `workloads-approve`, `workloads-deploy`, `workloads-record-run` | The UI only *assesses* workloads. A generated artifact is not a verified one |
| The source code of a Databricks notebook (a token, or a file) | **CLI only**: `fetch-databricks-notebooks`, `workloads-generate --source` | Fetched only when authorised |
| The lakehouse / OneLake workspace the notebook writes to | **CLI only**: `workloads-generate --lakehouse-id ...` | |
| A compute sources file describing existing clusters | **CLI only**: `convert --compute-sources` | Otherwise a Databricks activity with an existing cluster stays manual |

## Storage (opt-in)

| Input | Where | Notes |
|---|---|---|
| Repoint ADLS datasets to a lakehouse (name of the lakehouse) | UI (Convert: Storage) | The storage map is built read-only first |
| A different shortcut name than the default; creating the shortcuts | **CLI only**: `storage-map --shortcut`, `create-shortcut` | Unmappable stores (S3, GCS, file shares) are listed with what Fabric offers |

## Running and proving

| Input | Where | Notes |
|---|---|---|
| Start real Fabric runs | UI (Run and compare) | Needs the execute permission, a typed confirmation and 1-20 named pipelines; audited |
| Pipeline parameter values for a run | Not asked | A run uses the pipeline's defaults |
| Enable triggers | Fabric portal | Triggers are created disabled and never enabled by the tool |
| Commit the migrated items to a Git-connected workspace | **CLI only**: `git-commit` | Dry run unless `--apply`; never connects a workspace to Git |
| Accepting a model-assisted draft | UI (Convert) | Optional; off by default |
| Rebuilding what has no conversion (SSIS, U-SQL, classic Azure ML, mapping data flows, HDInsight programs, Databricks jar/python/job) | By hand in Fabric | Listed in Review and in the conversion report |
| Removing stale items from the workspace | Fabric portal | The tool never deletes a Fabric item |

## Gaps still open

The items marked **CLI only** have no screen. The two that matter most for a person working only in the UI: approving and deploying a migrated notebook,
and creating storage shortcuts. Everything in the first four tables and the "ids the converter cannot know" table is asked for in the UI.
