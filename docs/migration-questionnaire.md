# ADF to Fabric Migration Questionnaire

As of 2026-10-23

These are the questions the ADF → Fabric migration agent needs answered, in the order it asks them. Sections A and B are the formal consultation questionnaire: discovery does not start until every mandatory access check passes. Sections C to H are the decisions asked later, step by step.

## How to use this questionnaire

- Fill in the **Answer** column. A blank answer is never guessed: the agent stops and asks, or records the item as blocked.
- **Never write a secret here.** Passwords, keys, tokens and connection strings are given only as the *name* of an environment variable (for example `SQL_PASSWORD`). The agent reads the value at run time and never stores it.
- Names, server addresses, subscription, workspace and lakehouse IDs are not secrets and can be written in full.
- Nothing is created, changed or run in Azure or Fabric during these sections. Every real change later needs an approval, a dry run first, and a typed confirmation.

## A. Engagement and scope

Only a like-for-like migration is supported today. Any other mode is recorded and flagged unsupported, never reinterpreted.

| # | Question | Why it is asked | Example | Answer |
| --- | --- | --- | --- | --- |
| A1 | What is the objective of the migration, in one sentence? | Recorded with the readiness decision | Move the ADF estate to Fabric as-is | |
| A2 | Which migration mode: like-for-like, or something else (redesign, modernization, optimization, restructuring, renaming, alternative architecture)? | Only like-for-like is supported; others are flagged | like-for-like | |
| A3 | Migrate the whole repository or factory, or only named artifacts? If named, list each name and type. | Defines the scope discovery checks against | Whole repository | |
| A4 | What are the success criteria? | Shown in the final report | All in-scope pipelines deploy and run in Fabric with the same results as ADF | |
| A5 | Which constraints apply (data residency, change windows, compliance)? | Recorded as-is | Data stays in the EU | |
| A6 | Is anything explicitly excluded? | Excluded items are never migrated | Legacy SSIS packages | |
| A7 | Are there requirements beyond like-for-like you want recorded? | Captured and flagged as unsupported, never executed | Redesign the orchestration | |

## B. Source and access

Each of the five areas below is checked with a real sign-in. A mandatory area that fails blocks discovery. An area you do not need must be marked "not required" explicitly.

| # | Question | Why it is asked | Example | Answer |
| --- | --- | --- | --- | --- |
| B1 | Where is the ADF source: a Git repository, a ZIP export, or the live factory in Azure? | Decides how the source is read | Git repository | |
| B2 | Repository URL and branch (blank = the repository's default branch). Is it the collaboration branch or `adf_publish`? | The source is read from exactly this branch and commit | github.com/org/adf-repo, main | |
| B3 | Name of the environment variable holding the repository token; any extra repositories or submodules? | Private repositories need a token | GITHUB_TOKEN | |
| B4 | Live factory: subscription ID, resource group and factory name | Needed to read the factory and its run history | sub 0000…, rg-data, adf-prod | |
| B5 | Is a SQL Server source in scope? If yes: server, database, port, sign-in (SQL login or Windows) and the TLS policy (strict, trust server certificate, or disabled) | TLS is never assumed or weakened | sqlsrv.example.net, WarehouseDB, strict | |
| B6 | Is ADLS Gen2 in scope? If yes: storage account and (optionally) the container to check | Checks the account is reachable | acmedatalake, raw | |
| B7 | Fabric target: tenant and workspace ID | Every deployment goes to this workspace only | contoso.onmicrosoft.com, 1111… | |
| B8 | For each area, how should the agent sign in? Azure CLI, service principal (secret or certificate), managed identity, browser or device code, a pre-acquired token, or the default Azure chain. Storage only: account key, SAS or connection string. | Only environment-variable names are recorded | Azure CLI for ADF and Fabric | |
| B9 | Are ADF and Fabric in different tenants or accounts? | Each side can sign in with its own account | ADF in tenant A, Fabric in tenant B | |
| B10 | Who owns each area (repository, ADF, SQL, storage, Fabric)? | Blockers are routed to the owner | Platform team, DBA team | |

## C. Connections (asked per linked service after discovery)

Every ADF linked service becomes a Fabric connection. The agent can create SQL/Synapse, ADLS Gen2, Blob, REST and Databricks connections; other types get a verdict and are created by hand.

| # | Question | Why it is asked | Example | Answer |
| --- | --- | --- | --- | --- |
| C1 | Does a Fabric connection already exist for this linked service? If yes, its connection ID | An existing connection is reused, never duplicated | LS_Sales_SQL → 6c14… | |
| C2 | If not, how should Fabric sign in: workspace identity, Key Vault reference, SQL login, account key, SAS or service principal? | Synapse and ADF never give up stored secrets | Workspace identity | |
| C3 | For secret-based methods: the name of the environment variable that holds the secret | The secret is sent once and never stored | SQL_PASSWORD | |
| C4 | Linked services that use a self-hosted integration runtime: which on-premises data gateway serves them? | Fabric needs a gateway instead of the runtime | gw-onprem-01 | |
| C5 | Linked services that read secrets from Azure Key Vault: which vault, and may Fabric use a Key Vault reference? | The credential is recreated in Fabric | kv-data-prod | |
| C6 | Linked service parameters without a default value: what value should each have? | Fabric connections are not parameterised | DBName = Sales | |
| C7 | Pipelines that call other pipelines: which Fabric Data Pipelines connection (OAuth) should the Invoke pipeline step use? | Needed by every Execute pipeline step | Fabric-data | |
| C8 | Should the workspace get a workspace identity (needed for identity-based connections)? | Creating it is a separate, approved step | Yes | |

## D. Storage and OneLake

Data is never copied or deleted by the agent. Storage is reached through OneLake shortcuts that point at the existing accounts, and pipelines can be repointed to read and write the lakehouse instead.

| # | Question | Why it is asked | Example | Answer |
| --- | --- | --- | --- | --- |
| D1 | Should pipelines keep using connections to the original storage, or be repointed to a lakehouse through shortcuts? | Repointing is opt-in | Repoint to the lakehouse | |
| D2 | Which lakehouse holds the shortcuts (name and ID)? Create a new one or use an existing one? | A lakehouse name is never invented | LH_Migration | |
| D3 | Do shortcuts already exist? For each storage account and container, the shortcut name to use | Existing shortcuts are reused by name | datamigration/migrationcontainer → migrationcontainer | |
| D4 | For each storage account: the existing Fabric connection the shortcut should use | Shortcuts reach the account through a connection, never a key | Sathvik-ADF | |
| D5 | Datasets the storage map marks "needs a decision" (Blob, S3, SFTP, FTP, file share, ADLS Gen1, dynamic container): where should this data live in Fabric? | These are listed, never guessed | Copy the SFTP drop into Files/landing | |
| D6 | Should any files also be loaded into lakehouse tables (Delta)? | Loading data is a separate decision | No, keep as files | |
| D7 | Is any storage behind a private network or firewall that Fabric must be allowed through? | Network access is set up by hand | Yes, storage firewall on | |

## E. Compute and Spark workloads (Databricks, Synapse)

By default, converted pipelines keep running Databricks notebooks on Databricks. Moving them to Fabric Spark is opt-in and needs an approval per workload. A generated notebook counts as verified only after a recorded real run.

| # | Question | Why it is asked | Example | Answer |
| --- | --- | --- | --- | --- |
| E1 | Keep Databricks / Synapse compute, or move the notebooks and Spark jobs to Fabric Spark? | The default keeps the source compute | Move to Fabric | |
| E2 | May the agent read the notebook code from Databricks or Synapse? If yes, the environment variable holding the token (or permission to use your Azure CLI sign-in) | Code is read only with your authorisation | DATABRICKS_TOKEN | |
| E3 | Or will you supply the code yourself (local files per notebook path)? | An alternative to reading it live | Exported .py files | |
| E4 | Cluster details ADF only refers to by ID (node size, worker count, Spark version, libraries) | Needed to size Fabric Spark compute | Standard_DS3_v2, 2–8 workers, 13.3 LTS | |
| E5 | Databricks secret scopes used in code: which Azure Key Vault URL replaces each? | Secret calls are rewritten only with this map | kv-scope → https://kv-data.vault.azure.net/ | |
| E6 | For JAR or Python Spark jobs: where have the main files been uploaded in OneLake or ADLS? | A Spark job definition only stores a reference | abfss://…/Files/jobs/main.py | |
| E7 | Default lakehouse for generated Spark job definitions | Every Fabric Spark job needs one | LH_Migration | |
| E8 | Should storage paths in notebook code be routed through the lakehouse shortcuts? If yes, the workspace and lakehouse (both names or both IDs) | Paths are rewritten only for planned shortcuts | adf-migration-test / LH_Migration | |
| E9 | Who approves each generated workload before it is deployed? | Deployment needs an approval | Data engineering lead | |

## F. Target provisioning

The agent first plans read-only: what already exists and what is missing. Only steps an approver accepts are applied, after a dry run. Nothing existing is modified or deleted.

| # | Question | Why it is asked | Example | Answer |
| --- | --- | --- | --- | --- |
| F1 | Use an existing Fabric workspace, or create a new one? Its name | The plan targets exactly this workspace | adf-migration-prod | |
| F2 | Which Fabric capacity (and SKU) should the workspace use? Is an Azure capacity to be created? | Spark sizing is checked against the capacity | F64, existing | |
| F3 | Which lakehouses and warehouses are needed, by name? | Names are never invented | LH_Raw, WH_Reporting | |
| F4 | Spark pools and environments to create, if compute moves to Fabric (from E1) | Sized from the source clusters | Medium pool, runtime 1.3 | |
| F5 | Who must have access to the workspace, and with which role (Admin, Member, Contributor, Viewer)? | Role assignments are planned steps | Data team: Contributor | |
| F6 | Who approves the provisioning plan? (An approver cannot also execute it.) | Separation of duties | Platform lead | |
| F7 | Who executes approved steps? | Needs the execute permission | Migration operator | |

## G. Conversion, deployment, validation and Git

Every deployment is a dry run first. A pipeline is called runtime-validated only when a real Fabric run completes and matches its last successful ADF run.

| # | Question | Why it is asked | Example | Answer |
| --- | --- | --- | --- | --- |
| G1 | Incomplete pipelines found in discovery: apply the agent's safe reconstruction proposals, or fix them in the source? | Inferred content is never used without consent | Fix in source | |
| G2 | Which pipelines go first? Accept the dependency-based migration order? | Children are deployed before their callers | Wave 1: ingestion pipelines | |
| G3 | Triggers are created switched off. When, and by whom, may each schedule be switched on? | Prevents double runs while ADF still runs | After cut-over, by the operator | |
| G4 | Event and custom-event triggers: recreate them by hand (for example as Fabric Activator rules)? | They are not converted automatically | Yes, owner: platform team | |
| G5 | Mapping data flows: rebuild in Fabric (Dataflow Gen2 or notebooks), or keep in ADF for now? | Only a placeholder activity is generated | Rebuild as notebooks | |
| G6 | May the agent read ADF run history as the comparison baseline? Which account can read it? | Fabric runs are compared with the last successful ADF run | Reader on the factory | |
| G7 | May the agent start real runs in Fabric to prove pipelines? Which pipelines, and is it safe for them to write their targets? | Real runs read sources and write targets | Yes, test pipelines only | |
| G8 | Is the Fabric workspace connected to a Git repository and branch? Should the migration's items be committed there? | Commits are selective, never deletions, dry run first | GitHub org/fabric, branch dev | |
| G9 | Cut-over: when is ADF switched off, and who confirms it? | The agent never changes or disables the source | After two matching runs | |

## H. People, roles and sign-off

Each person who uses the agent gets their own sign-in token and one or more roles. Each person can only reach the subscriptions, tenants, accounts and workspaces granted to them.

| # | Question | Why it is asked | Example | Answer |
| --- | --- | --- | --- | --- |
| H1 | Who only reads results and reports? (viewer) | Read-only access | Project manager | |
| H2 | Who runs discovery, conversion and plans? (analyst) | Can plan, cannot change Fabric | Migration engineer | |
| H3 | Who approves provisioning plans? (approver) | Cannot also execute | Platform lead | |
| H4 | Who executes approved changes, deployments and real runs? (operator) | Holds the execute permission | Migration operator | |
| H5 | Who manages users and reads the audit log? (admin) | Administers the agent | Security lead | |
| H6 | Which subscriptions, tenants, accounts and workspaces may each person target? | Requests outside the grant are refused | Operator: workspace 1111… only | |

**Sign-off**

- [ ] Sections A and B complete; consultation readiness is READY
- [ ] Connection, storage and compute decisions recorded (C, D, E)
- [ ] Provisioning plan approved (F)
- [ ] Validation and cut-over criteria agreed (G)
- [ ] Roles and grants assigned (H)
