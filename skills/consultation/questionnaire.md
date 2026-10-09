# Consultation: questionnaire

The questionnaire is the authoritative, deterministic input to Consultation. It
is a YAML file passed via `--input`. Every persisted value is traceable to a
field here or to project metadata. Unknown keys are rejected (typos fail loudly).

**Credentials are references only.** Fields named `*_env` hold the *name* of an
environment variable, never a secret value. `username` and connection metadata
(server, database, storage account, subscription, workspace id) are not secrets.

## Full example

```yaml
migration:
  source_tool: azure_data_factory     # fixed
  source_warehouse: sql_server        # fixed
  target_platform: microsoft_fabric   # fixed
  mode: like_for_like                 # only like_for_like is currently supported
  objective: "Migrate the ADF + SQL Server estate to Fabric as-is."

requested_scope:
  whole_repository: true              # true = entire repo; else list artifacts below
  artifacts: []                       # e.g. [{name: Aple, type: pipeline}] (names recorded verbatim)

success_criteria:
  - "All in-scope artifacts deploy and run in Fabric with behavior parity"
constraints:
  - "Data residency: EU only"
exclusions: []

access:
  repository:
    required: true
    mandatory: true
    token_env: GITHUB_TOKEN
    additional_repositories: []
    submodules_expected: false
    owner: "Platform team"

  adf:
    required: true
    mandatory: true
    subscription_id: "00000000-0000-0000-0000-000000000000"   # explicit; never inferred
    resource_group: "rg-data"
    factory_name: "adf-prod"
    environment: prod
    credential:
      auth: azure_cli                 # current local method: uses your `az login` identity
      # auth: client_secret           # alt: tenant_id_env / client_id_env / client_secret_env
      # auth: azure_identity          # alt: ambient DefaultAzureCredential chain
    owner: "Cloud platform team"

  sql_server:
    required: true
    mandatory: true
    server: "sqlsrv.example.net"      # required (hostname or host\instance)
    database: "WarehouseDB"           # required (no default is assumed)
    # Optional — only what you set is used; nothing is assumed:
    port: 1433                        # omit for the driver/server default
    driver: "ODBC Driver 18 for SQL Server"   # local client driver; overridable default
    connect_timeout: 10               # seconds; omit for the connector default
    tls:                              # EXPLICIT TLS policy — never inferred
      mode: strict                    # strict | trust_server_certificate | disabled
    environment: prod
    credential:
      auth: sql_login                 # or 'integrated' (Windows/Trusted_Connection)
      username: "svc_migrate"
      password_env: SQL_PASSWORD      # env-var NAME; the value is never persisted
    owner: "DBA team"

  adls_gen2:
    required: true
    mandatory: true
    storage_account: "acmedatalake"   # required
    filesystem: "raw"                 # optional; validated only if provided
    environment: prod
    credential:
      auth: azure_identity            # or 'client_secret' with the three *_env fields
      tenant_id_env: AZ_TENANT_ID
      client_id_env: AZ_CLIENT_ID
      client_secret_env: AZ_CLIENT_SECRET
    owner: "Cloud platform team"

  fabric:
    required: true
    mandatory: true
    tenant: "contoso.onmicrosoft.com"
    workspace_id: "11111111-1111-1111-1111-111111111111"
    environment: prod
    credential:
      auth: client_secret
      tenant_id_env: FAB_TENANT_ID
      client_id_env: FAB_CLIENT_ID
      client_secret_env: FAB_CLIENT_SECRET
    owner: "Fabric admin"

# Optional: requirements the user explicitly wants recorded that go BEYOND
# like-for-like. Captured verbatim and flagged unsupported — never executed.
additional_requirements: []
  # - description: "Redesign the orchestration in Fabric"
  #   kind: redesign
```

## Rules

- **All five `access` domains must be present.** If a domain is not needed for the
  engagement, set `required: false` explicitly (no assumptions are made).
- `requested_scope` is `whole_repository: true` **XOR** a non-empty `artifacts`
  list. Artifact names are recorded verbatim and are **not** resolved against
  `source/` (that is Discovery).
- Required connection fields per domain (checked when the domain is required):
  - `adf`: `subscription_id`, `resource_group`, `factory_name`
  - `sql_server`: `server`, `database`, and a credential
  - `adls_gen2`: `storage_account` (only); `filesystem` is optional
  - `fabric`: `workspace_id`
  - `repository`: `token_env` (repo identity/branch come from Intake)
- `mode` must be a known value. Only `like_for_like` is currently supported; any
  other known mode is recorded and flagged `currently_supported: false`.
- **SQL Server TLS is an explicit policy.** `sql_server.tls.mode` is one of
  `strict` (Encrypt=yes, TrustServerCertificate=no), `trust_server_certificate`
  (Encrypt=yes, TrustServerCertificate=yes), or `disabled` (Encrypt=no). It is
  **never inferred** (not from hostname, not from SQL Express, not from whether
  SSMS works) and the connector **never** weakens TLS or retries with a weaker
  mode after a failure. When `tls` is omitted, no encryption option is sent and
  the ODBC driver's own defaults apply; if that then fails certificate
  validation, the outcome is `failed` with a remediation asking you to set an
  explicit `tls.mode`.
- A missing required field, an unknown key, or an invalid fixed platform/tool
  value fails the run clearly and writes nothing.
