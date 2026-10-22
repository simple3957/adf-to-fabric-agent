# Storage map: ADF storage to OneLake shortcuts and Lakehouse paths

`adf2fabric storage-map --project <name> [--lakehouse <name>] [--workspace-id <guid>]` reads the ADF semantic record and writes
`projects/<name>/storage/storage-map.json` and `storage-map.md`. It is read-only: it creates nothing and moves no data.

## What it decides

* A shortcut is planned per (storage account, container): `Files/<container>` (suffixed with the account name when the same container
  name exists in two accounts). The shortcut points at the storage that already holds the data; nothing is copied or deleted.
* Each file dataset gets a Lakehouse path: `Files/<shortcut>/<folder>/<file>`.
  * `mapped`: every part is a literal.
  * `mapped_dynamic`: the folder or file is an expression; it is shown as authored (`<dynamic: ...>`), never resolved.
  * `needs_decision`: dynamic or empty container, an account URL that is not a literal `https://<account>.dfs.core.windows.net`, a linked
    service missing from the source, a `..` in a path, or a store without a verified shortcut here (Blob, S3, GCS, SFTP, FTP, file server,
    Azure Files, ADLS Gen1). The report lists each with the reason and what to do; the command prints them as `DECIDE`.
* Datasets that are not files (SQL tables and so on) are counted and left out.
* Each shortcut needs an existing Fabric connection to the account. The report prints a `create-shortcut` command with
  `<connection-id for ...>` left for you; the id is never invented. Run it dry first, then add `--apply`.

## Routing storage paths in code (Spark notebooks)

`workloads-assess` / `workloads-generate` with `--onelake-workspace` and `--onelake-lakehouse` rewrite literal ADLS paths in the
generated notebooks to the shortcut the storage map planned (`agent/storage/routes.py`):

    abfss://c@acct.dfs.core.windows.net/x  ->  abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/<shortcut>/x
    https://acct.dfs.core.windows.net/c/x  ->  https://onelake.dfs.fabric.microsoft.com/<ws>/<lakehouse>/Files/<shortcut>/x

Give both as GUIDs, or both as names (then `<lakehouse>.Lakehouse`), per Microsoft's OneLake URI rules; a mix is refused.
Only a literal (account, container) with a planned shortcut is rewritten, and its `adls_direct_path` finding is marked rewritten.
Paths built from variables, `wasbs://` / `.blob.core.windows.net`, and unplanned containers are left as written and stay open
findings. The shortcuts must exist before the notebook runs. Idea taken from the Synapse accelerator's `storage_remap.py`.

## Repointing pipelines to the Lakehouse (`convert --lakehouse <name>`)

Opt-in. After `storage-map`, `convert --lakehouse <name> --resolutions <file>` rewrites ADLS Gen2 datasets to read and write the
lakehouse through the planned shortcuts. Bind the lakehouse with `{"type": "ItemToId", "key": "lakehouse:<name>", "value": "<guid>"}`
and the workspace with `WorkspaceId`; unbound, they stay placeholders and `deploy` refuses the pipeline.

The JSON emitted is exactly what ran in Fabric on 2026-10-22 (workspace adf-migration-test): Copy CSV -> CSV and Parquet -> Parquet,
Lookup, GetMetadata (childItems, exists), and the converter's own output for `PL_Copy_Test` (10 rows, as in ADF) and
`PL_Rig_Child_FileToParquet` (folder from a parameter; Lookup 4, Copy 4, its row-count check passed):

    datasetSettings.linkedService = {name, properties: {type: Lakehouse, typeProperties: {workspaceId, artifactId, rootFolder: Files}}}
    datasetSettings.typeProperties.location = {type: LakehouseLocation, folderPath: <shortcut>/<folder>, fileName}
    storeSettings.type: AzureBlobFSReadSettings -> LakehouseReadSettings, AzureBlobFSWriteSettings -> LakehouseWriteSettings

A folder written as an expression keeps it, with the shortcut in front (`@concat('<shortcut>/', ...)`). These stay on their storage
connection, with a `storage_not_repointed` note saying why: formats other than DelimitedText and Parquet (not run yet), a dynamic
container, an account without a literal ADLS Gen2 URL, a container without a shortcut, storeSettings that hold a path
(`wildcardFolderPath`, `fileListPath`, `partitionRootPath`, ...), and every Delete activity (proving it live would delete files).

## Not done yet

* Formats other than CSV and Parquet, Lakehouse Tables (Delta) as a target, and Delete are not repointed.
* Data is never copied or deleted.
