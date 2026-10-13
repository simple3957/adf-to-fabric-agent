# Linked Service → Fabric Connection Mapping (draft for review)

**Owner:** Bhavana · **Status:** decisions recorded 2026-10-12 (section 6); blocked on Fabric access (section 7)
**Source:** live factory `sathvik-df` (`datafactory-rg154`), read with `az datafactory` on 2026-10-11,
cross-checked against the Synapse export in `synapse-migration-project/linkedService/`.

## 1. Key facts

- **Fabric has no linked services.** The equivalent is a **Connection** (Settings → Manage connections
  and gateways). It lives at tenant level, is shared with workspaces, and pipeline activities refer to it by ID.
- **Fabric has no datasets.** Each Copy or Lookup activity holds the connection, table and path inline.
  So datasets aren't converted. The pipeline generator (Sathvik) reads a dataset only to find out
  *which linked service + which table/path* an activity uses.
- **Fabric has no Azure integration runtime.** Cloud compute is managed for you. Only a self-hosted IR (SHIR)
  needs a Fabric equivalent: an **on-premises data gateway** or a VNet data gateway.

## 2. What we actually have

### Integration runtimes (3 + the built-in default)

| IR | Type | Location | Fabric equivalent |
|---|---|---|---|
| `AutoResolveIntegrationRuntime` | Managed (Azure), built-in | AutoResolve | None needed |
| `IR-Azure-Auto` | Managed (Azure) | AutoResolve | None needed |
| `AzureIR` | Managed (Azure) | AutoResolve | None needed (not used by any linked service) |
| `ADFIR` | Managed (Azure) | AutoResolve | None needed |

**No self-hosted IR exists**, so no gateway is needed for this factory. The generator should still detect
`SelfHosted` IRs and flag them as "gateway required", because a client factory will have them.

### Linked services (5, all on Azure IR)

| # | Linked service | ADF type | Endpoint | Auth seen | IR |
|---|---|---|---|---|---|
| 1 | `transportationsynapsemigration-WorkspaceDefaultStorage` | AzureBlobFS (ADLS Gen2) | `adfteststoragebhavana.dfs.core.windows.net` | none stored → managed identity | IR-Azure-Auto |
| 2 | `AzureDataLakeStorage1` | AzureBlobFS (ADLS Gen2) | `adfmigrationstorage.dfs.core.windows.net` | encrypted credential (key) | ADFIR |
| 3 | `Sathvik_ADF` | AzureBlobFS (ADLS Gen2) | `adfmigrationstorage.dfs.core.windows.net` | encrypted credential (key) | ADFIR |
| 4 | `transportationsynapsemigration-WorkspaceDefaultSqlServer` | AzureSqlDW (Synapse dedicated SQL pool) | `transportationsynapsemigration.sql.azuresynapse.net` | connection string, no user/password → managed identity | ADFIR |
| 5 | `LS_Synapse_Workspace` | AzureSynapseArtifacts | `workspacecr.dev.azuresynapse.net` | MSI | IR-Azure-Auto |

## 3. Proposed mapping

| ADF type | Fabric connection type | Connection fields | Auth mapping | Notes |
|---|---|---|---|---|
| AzureBlobFS (ADLS Gen2) | Azure Data Lake Storage Gen2 | server URL (+ optional path) | Account key → Key · Service principal → Service principal · **Managed identity → Workspace identity** or service principal (decision needed) | LS 2 and 3 point to the same account, so they become **one** connection |
| AzureSqlDW (Synapse SQL pool) | Azure Synapse Analytics (SQL DW) / SQL | server + database | SQL auth → Basic · MI → Service principal / OAuth2 (decision needed) | `DBName` is a linked service **parameter**. Fabric connections can't be parameterized this way, so the database moves into the activity settings |
| AzureSqlDatabase | Azure SQL Database | server + database | as above | not in this factory, but expected at the client |
| SqlServer (on-prem) | SQL Server | server + database | Basic / Windows | needs a **gateway** if the linked service uses a SHIR |
| AzureBlobStorage | Azure Blob Storage | account URL | Key / SAS / SP | expected at the client |
| AzureKeyVault | **No equivalent** | – | – | secrets referenced from Key Vault must be re-entered or handled through Fabric's Key Vault support, so flag for manual handling |
| AzureSynapseArtifacts | **No equivalent, so drop** | – | – | only used for Synapse notebook/Spark activities. Those become Fabric notebooks (pipeline side) |

**Never migrate secret values.** The generator outputs the connection definition with *empty* credentials
plus a "credential required" flag. A human or an approved secret source fills them in.

## 4. What the generator would produce (proposal)

For each unique endpoint, one record:

```json
{
  "fabric_connection_name": "adls-adfmigrationstorage",
  "fabric_connection_type": "AzureDataLakeStorage",
  "source_linked_services": ["AzureDataLakeStorage1", "Sathvik_ADF"],
  "server": "https://adfmigrationstorage.dfs.core.windows.net/",
  "database": null,
  "auth": {"adf": "AccountKey", "fabric": "Key", "credential_required": true},
  "gateway_required": false,
  "status": "ready | manual | drop",
  "notes": []
}
```

Sathvik's pipeline generator uses `source_linked_services` to swap each ADF linked service name for the
Fabric connection ID.

## 5. Gaps found in the current code

1. The existing parser (`adf_semantic/parser.py`, `parse_linked_service`) **doesn't capture `connectVia`**,
   so it can't tell a SHIR from an Azure IR yet.
2. **Two input shapes:** Git JSON nests settings under `properties.typeProperties`, while
   `az datafactory ... show` **flattens** them straight into `properties`. The parser reads
   `typeProperties` only, so it would miss the endpoint and auth on live CLI output. The generator must
   handle both, or live discovery should use the REST/ARM shape.
3. Integration runtimes aren't parsed at all yet.

## 6. Decisions (2026-10-12)

| # | Question | Decision |
|---|---|---|
| 1 | Plan only, or also create in Fabric? | **Create** the connections in Fabric through the REST API. |
| 2 | What replaces ADF's managed identity? | **ADLS → workspace identity. Synapse SQL → SQL login** (username + password, Fabric credential type `Basic`). *To be confirmed.* |
| 3 | Synapse SQL pool to test against | Workspace `transportationmigration`, resource group `TransportationMigrationRG`, subscription `00718529-449b-4c92-8213-126f14ddee4e`, tenant `b886bb14-1c62-46be-8466-473864afcd1c`. Resource ID: `/subscriptions/00718529-449b-4c92-8213-126f14ddee4e/resourceGroups/TransportationMigrationRG/providers/Microsoft.Synapse/workspaces/transportationmigration`. SQL endpoint (confirmed 2026-10-12): `transportationmigration.sql.azuresynapse.net`. Dedicated pool `transportationdedicated` (Online). SQL admin login `transportationmk`. |
| 4 | ADLS: connection or OneLake shortcut? | **OneLake shortcut to the whole container** `migrationcontainer`. *Which storage account is still to be confirmed:* most datasets use `adfmigrationstorage` (through `Sathvik_ADF` / `AzureDataLakeStorage1`), and only `DS_Parquet_Sales` uses `adfteststoragebhavana`. |
| 5 | Drop `LS_Synapse_Workspace` and `AzureIR`? | **Yes, drop both.** |
| 6 | Target Fabric workspace | We create a new one (see blockers). |

### What decision 4 (OneLake shortcut) changes

- A shortcut lives **inside a Lakehouse**, so we need a Lakehouse in the target workspace.
- An ADLS shortcut still **needs a Fabric connection** to the storage account (the shortcut refers to
  it by connection ID). So the generator still creates the ADLS connection, then creates the shortcut
  on top (`POST /v1/workspaces/{ws}/items/{lakehouse}/shortcuts`).
- The shortcut points to the **whole container** `migrationcontainer`. Still open: does the second
  storage account `adfmigrationstorage` (used by `AzureDataLakeStorage1` and `Sathvik_ADF`) need one too?
- **Impact on Sathvik's pipelines:** Copy activities that read or write ADLS through a dataset would
  read or write the **Lakehouse** (through the shortcut) instead. He needs to know this.
- Workspace identity needs the workspace to be on a **Fabric capacity**, and the workspace identity
  needs the **Storage Blob Data Reader/Contributor** role on the storage account.

## 7. Blockers

1. **No Fabric license.** The Azure CLI is logged in as `rasoolaiahc@gmail.com`. Every Fabric API call
   returns `UserNotLicensed`, so we can't list connection types, create a workspace or create
   connections. We need a work account with Fabric access (or a Fabric trial/capacity) for the account
   the generator runs as.
2. **Synapse SQL access:** workspace is known (section 6, row 3). Still needed: a SQL login
   (username + password) and who provides it. **Server name mismatch:** the ADF linked service points to
   `transportationsynapsemigration`, but the real workspace is `transportationmigration`. The generator
   needs a server-name override so the Fabric connection points to the right server.
   Synapse is in tenant `b886bb14-…`, not the CLI's current tenant `7f4bef0a-…`, so the Fabric account
   should be in the same tenant as Synapse.
3. **Fabric workspace + Lakehouse:** to be created once blocker 1 is fixed. Needs a capacity for
   workspace identity.
4. **Python is not installed** on this machine, so the generator can't run here yet.
5. **Fabric type names not yet verified** against `GET /v1/connections/supportedConnectionTypes`
   (blocked by 1). The generator will check them before creating anything.
