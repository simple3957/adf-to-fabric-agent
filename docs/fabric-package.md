# Fabric package (deploy it yourself)

Since 2026-11-15. The migration as a folder a client deploys **without this tool and without the Fabric CLI**: the Fabric equivalent of an
ADF ARM template. Code: `src/adf2fabric/agent/deployment/fabric_package.py`. Get it with `adf2fabric package --project P` (writes
`projects/P/package/`, replacing an earlier one) or on the UI's Deploy step (**Build the package**, then **Download the package (ZIP)**;
`GET /projects/{id}/package`, `reports:view`, audited as `package.build`). Building it reads only the project's records; nothing in
Fabric is read or changed.

## Why not an ARM template

Fabric items (pipelines, notebooks, Environments, dataflows) are not Azure Resource Manager resources; ARM covers only the Fabric
capacity. Their deployable form is the item-definition format Fabric's Git integration reads and writes
([source code format](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/source-code-format)): one folder per item,
`<display name>.<type>`, holding a `.platform` file and the item's definition files.

## What is in it

| Item | Folder | Files |
|---|---|---|
| Every complete pipeline | `<name>.DataPipeline` | `pipeline-content.json`, `.platform` |
| Every approved generated notebook those pipelines run | `<name>.Notebook` | `notebook-content.py` (Fabric's source format, as Fabric itself returns it: `# Fabric notebook source`, `# PARAMETERS CELL` / `# CELL` / `# MARKDOWN` markers, a `# META` block for the attached Environment), `.platform` |
| Every approved Environment those notebooks use | `<name>.Environment` | `Libraries/CustomLibraries/<wheel>`, `Libraries/PublicLibraries/environment.yml`, `.platform` |
| What differs per target | `parameter.yml` | fabric-cicd `find_replace` entries: every Fabric connection id the pipelines use, every OneLake path that names a workspace and lakehouse |
| How to deploy, what was left out | `README.md` | |

`.platform` is `{"$schema": ".../gitIntegration/platformProperties/2.0.0/schema.json", "metadata": {"type", "displayName",
"description"}, "config": {"version": "2.0", "logicalId"}}`. The logicalId is derived from the project and the item's name, so a
rebuild keeps it.

**References between items** are written the way Fabric's own Git export writes them: the referenced item's logicalId in place of
its id (a Notebook activity's `notebookId`, an Invoke pipeline's `pipelineId`, a notebook's `environmentId`) and the all-zero GUID
for the workspace. Git sync and fabric-cicd resolve both to the items in the target workspace
([cross-workspace dependency binding](https://learn.microsoft.com/en-us/fabric/cicd/cross-workspace-dependency-binding)).

**Left out, with the reason** (in the README and the result): a pipeline with an unbound item or connection, with an activity that was
not converted (a fail-fast stand-in), or that runs a notebook that is not an approved generated notebook of this project; a notebook
whose Environment is not generated and approved. Connections and credentials never travel; nor do lakehouse data, tables or shortcuts
(not in this version).

## Deploying it

1. The Fabric connections listed in `parameter.yml` must exist in the target tenant (their credentials are entered there). Set each
   `TARGET` value to the target's connection id; set the OneLake paths to the target workspace and lakehouse names, and create that
   lakehouse with the same shortcuts first.
2. With Microsoft's [fabric-cicd](https://microsoft.github.io/fabric-cicd/latest/) (`pip install -e ".[package]"`):

   ```python
   from azure.identity import AzureCliCredential
   from fabric_cicd import FabricWorkspace, publish_all_items
   ws = FabricWorkspace(workspace_id="<target workspace id>", repository_directory="<the package folder>", environment="TARGET",
                        item_type_in_scope=["Environment", "Notebook", "DataPipeline"], token_credential=AzureCliCredential())
   publish_all_items(ws)
   ```

   It creates the items in dependency order (Environments, which it also publishes, then notebooks, then pipelines, children first),
   replacing logicalIds with the new items' ids.
3. Or commit the folder to the branch of a Git-connected workspace and update the workspace from Git; then publish each Environment in
   Fabric (a Git sync only stages its libraries).

## Checked live (2026-11-15)

The package of `adf2fabricv1` (12 pipelines, 5 notebooks, 1 Environment; the data flow and U-SQL test pipelines left out) was deployed
with fabric-cicd 1.4.0 into a new, empty workspace (`adf2fabric-package-test`, left for you to delete). All 18 items were created and the
Environment published; reading them back, every pipeline's `notebookId` / `pipelineId` and the notebook's `environmentId` pointed at the
new workspace's own items. The notebook format matches what Fabric returned from `getDefinition` for the same notebooks.

## Limits

* Data flows and U-SQL are not in the package until they are converted (Part 2: a model-drafted notebook a person approves).
* Lakehouses and shortcuts are not packaged yet; the notebooks' OneLake paths are parameters instead.
* Notebooks generated with OneLake paths by workspace NAME keep that name until `parameter.yml` changes it (the per-workspace copy
  the Deploy step makes re-points only paths written with workspace ids; found 2026-11-15).
