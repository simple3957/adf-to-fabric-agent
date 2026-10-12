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
      # Any Microsoft Entra mechanism works for ADF, ADLS Gen2 and Fabric (see "Authentication mechanisms" below):
      # auth: client_secret           # tenant_id_env / client_id_env / client_secret_env
      # auth: client_certificate      # tenant_id_env / client_id_env / certificate_path_env [/ certificate_password_env]
      # auth: managed_identity        # optional client_id_env for a user-assigned identity
      # auth: interactive_browser     # optional tenant_id_env (also: device_code)
      # auth: access_token            # access_token_env (a pre-acquired bearer token)
      # auth: azure_identity          # ambient DefaultAzureCredential chain
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
      auth: azure_identity            # any Entra mechanism above, or a storage-only one:
      # auth: account_key             # account_key_env: NAME of an env var holding the storage account key
      # auth: sas_token               # sas_token_env
      # auth: connection_string       # connection_string_env (its AccountKey / SharedAccessSignature is used)
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

## Authentication mechanisms

A `credential:` block names a mechanism (`auth`) and the **names** of the environment variables holding its
secrets; values are never written into the questionnaire or any record.

| `auth` | Reads (env-var name fields) | Works for |
|---|---|---|
| `azure_cli` | optional `tenant_id_env` (pick a tenant when several are logged in) | ADF, Fabric, ADLS |
| `client_secret` | `tenant_id_env`, `client_id_env`, `client_secret_env` | ADF, Fabric, ADLS |
| `client_certificate` | `tenant_id_env`, `client_id_env`, `certificate_path_env`, optional `certificate_password_env` | ADF, Fabric, ADLS |
| `managed_identity` | optional `client_id_env` (user-assigned) | ADF, Fabric, ADLS |
| `interactive_browser`, `device_code` | optional `tenant_id_env` | ADF, Fabric, ADLS |
| `access_token` | `access_token_env` (you own its audience and lifetime) | ADF, Fabric, ADLS |
| `azure_identity` | none (the SDK's default chain) | ADF, Fabric, ADLS |
| `account_key` | `account_key_env` | **ADLS only** |
| `sas_token` | `sas_token_env` | **ADLS only** |
| `connection_string` | `connection_string_env` | **ADLS only** |

Storage-only mechanisms are refused for ADF and Fabric with a message saying so. Service principals, certificates and
managed identities need the matching role on the target (for example Storage Blob Data Reader for ADLS, Reader on the
factory, a workspace role in Fabric).
