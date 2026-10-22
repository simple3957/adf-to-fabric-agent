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

## Not done yet

* Pipelines are **not** rewritten to read the Lakehouse. The shape of a Lakehouse dataset in Fabric pipeline JSON is not verified in this
  project (Microsoft's connector page documents only the portal fields: workspaceId, itemId, rootFolder, folderPath, fileName), and the
  rule here is not to guess properties. Until it is verified, converted pipelines keep using Fabric connections to the original
  storage (which is also what Microsoft's own upgrader does).
* Loading files into Lakehouse Tables (Delta) is a separate decision, and data copy is out of scope.
* Not exercised against a live Fabric workspace.
