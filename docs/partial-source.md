# A source with missing linked services or datasets

A ZIP or a Git subfolder often holds the pipelines but not every `linkedService/` or `dataset/` file they name. A live ARM intake has
all of them. This page says what is blocked without them and how to fix it. Measured on the `sathvik-df-test` sample (11 pipelines, 36
activities, 6 linked services, 24 datasets), 2026-10-25:

| Source | Pipelines converted | Manual | Blocked |
|---|---|---|---|
| Complete | 10 | 1 (notebooks to migrate) | 0 |
| No `linkedService/` | 3 | 8 | 0 |
| No `linkedService/`, connections bound in `--resolutions` | 10 | 1 (notebooks) | 0 (activities differ: connector not checked) |
| Pipelines only (no linked services, datasets, factory) | 3 | 1 | 7 |

## What each missing artifact costs

| Missing | Effect | Why |
|---|---|---|
| Dataset | Copy, Lookup and GetMetadata activities that use it (and any other activity that names a dataset) are **blocked** (a `Fail` activity stands in) | Fabric has no datasets: each is inlined into the activity, so there is nothing to inline |
| Linked service | Activities that use it are **manual**: the connection cannot be described (type, endpoint, authentication, integration runtime) | The Fabric connection is created by hand |
| Linked service, for Stored procedure and Script | **Blocked**, even with a connection bound | The store type decides whether Fabric supports the activity; only the linked service says what it is |
| Integration runtime, factory | No gateway advice; the factory-level checks are skipped | They only inform the report |

The conversion says this up front as a source check (`missing_linked_services`, `missing_datasets`): the names, which pipelines need
them and the ways to fix it. Nothing is guessed and nothing is dropped.

## How to resolve it, best first

1. **Complete the source.** Any of: a ZIP or Git folder that includes `linkedService/` and `dataset/`; the whole collaboration branch (not a
   subfolder); the factory's `ARMTemplateForFactory.json` export (an ARM-only source is extracted automatically when it is the only content);
   or intake from the live factory: `adf2fabric intake --factory F --resource-group RG --artifact-kind linkedservice`. Then re-run discovery and convert.
2. **Bind connections you created in Fabric.** Create each connection by hand, then give its id in the resolutions file:
   `{"type": "LinkedServiceToConnectionId", "key": "<linked service name>", "value": "<connection GUID>"}`. (`ItemToId` with
   `connection:<name>`, which older templates wrote, is accepted too.) The activities then convert, with a difference stating that the
   connection's connector type was not checked against a linked service. Check in Fabric that the connector and database match.
3. **Datasets cannot be bound.** They must come from the source.

Secrets are never in the source either way: credentials are entered in the Fabric connection.

## Limits

* Binding a connection does not unblock Stored procedure or Script activities (see above).
* The UI's Connections step works on linked services discovery found; for missing ones use the resolutions file (CLI `convert --resolutions`
  or `conversion/resolutions.json` for the API).
