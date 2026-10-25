# Security: trust boundaries, roles, authentication and what is not yet covered

**Status: hardened, not certified production-secure.** The controls below are implemented and covered by negative tests
(`tests/test_api_security.py`). The application must not be described as production-secure until the deployment checks at the end
have been completed and an independent review has been done. See "Remaining limitations".

## Trust boundaries

| Boundary | Who is trusted | Enforced by |
|---|---|---|
| Browser -> Streamlit UI | nobody by default: the UI asks each person for their own API token (kept in that browser session only) | UI sign-in gate; the UI holds no migration logic |
| UI / any HTTP client -> API | an authenticated principal, for what its roles allow, in its own organisation | `api/security/*`, `api/app.py` |
| API -> Azure / Fabric / Databricks / Git | the SERVER's own credentials (Azure CLI session, tokens it is given); a client never supplies credentials for them except a one-request Git token | grants, URL guards, per-stage permissions |
| API -> project files | the server process | organisation ownership per project; ids validated; archives extracted by a hardened reader |
| CLI -> everything | the operating-system user who runs it (it already holds the files and the `az` session) | OS permissions; sensitive commands audited |
| Uploaded archives, repositories, ADF JSON | untrusted input | validation before use; nothing in them is executed |

The Azure CLI sign-in is **machine-wide**: it belongs to the server's operating-system user, not to an API user. Anyone allowed to use
it (`azure:use`) acts through that session, restricted to the subscriptions, tenants, accounts and workspaces they were granted.

## Authentication

* Modes (`ADF2FABRIC_AUTH_MODE`): `token` (every request needs a valid bearer token) or `disabled` (the historical local mode: one implicit
  user with every role, **loopback only, one person**). Default: `token` when users exist or `ADF2FABRIC_API_TOKEN` is set, else `disabled`.
* The server **refuses to listen beyond loopback** unless authentication is on.
* Users and their tokens: `adf2fabric auth-user-create --id ann --org acme --role analyst --subscription <id> --workspace <id>` prints the
  token once. Only a SHA-256 hash is stored (`<projects>/_auth/users.yaml`); tokens are 256-bit random values; comparison is constant-time.
  Rotate with `auth-user-rotate`, disable with `auth-user-update --disable`.
* The legacy single `ADF2FABRIC_API_TOKEN` still works and maps to one admin of the `default` organisation. Prefer named users; the shared
  token is not attributable to a person.
* Failed sign-ins lock a client's guesses out after 8 attempts for 5 minutes (per client address hash) and are audited. A valid per-user
  token is never held back by that lockout (it cannot be guessed, and a proxy or office address must not lock out real users); the shared
  legacy token can be guessed, so it stays behind it. A damaged user store refuses everyone (503); it never lets anyone in.
* The Host header must be a plain host (and numeric port) on the allow-list: `127.0.0.1:8765@evil.example` is refused.
* A brief cannot be approved by the person who submitted it (four eyes; the single user of an unauthenticated local install is exempt).
* Brief, journey and notes records are changed under a per-file lock with unique temp files, so concurrent requests do not lose each
  other's changes (one API process per projects root; several processes writing one folder are not covered).
* Bearer tokens travel in the `Authorization` header only. There are **no cookies**, so classic CSRF cannot ride a credential.
* Short-lived credentials: Azure tokens come from the Azure CLI per call and are cached in memory for their lifetime only; Databricks and
  Key Vault tokens are read from environment variables you name, used for one request, never stored.

## Roles and permissions (server-enforced on every route)

| Permission | Allows |
|---|---|
| `projects:view` | list/read projects of your organisation, linked services, connectors, session, mode |
| `reports:view` | conversion summary, hand-over, connections report, job status and results |
| `sources:import` | upload an archive, test a source, import a source into a project |
| `discovery:run` | discovery / semantic / reconstruct stages; read Azure inventory you are granted |
| `plans:generate` | convert, compute assessment, connection choices and secrets, dry-run deploy, verify (read-only), dry-run connection apply |
| `azure:use` | use the server's Azure sign-in within your grants (accounts, subscriptions, tenants, Fabric workspaces) |
| `provision:approve` | approve target changes that create or configure resources (workspace identity) |
| `migration:execute` | change the target: deploy with apply, create connections for real |
| `credentials:manage` | start/end the server's Azure / Fabric CLI sign-in |
| `users:manage` | create, change, disable users of your organisation; read the organisation's audit events |

| Role | Permissions |
|---|---|
| viewer | projects:view, reports:view |
| analyst | viewer + sources:import, discovery:run, plans:generate, azure:use |
| approver | viewer + provision:approve |
| operator | analyst + migration:execute |
| admin | all |

Planning does not imply executing, and approving does not imply executing: an analyst prepares everything; changing the target needs
`migration:execute`; creating a workspace identity needs both `provision:approve` and `migration:execute`. Give one person both only if
your process allows it. `tests/test_api_security.py` holds a hand-written route-by-role table and fails if any role can do more or less.

## Scoping (tenancy)

* Every project belongs to one organisation, recorded at creation (`projects/<id>/access.yaml`). A principal sees and changes only its own
  organisation's projects, **whatever its role**. Another organisation's project is reported as not found (404), identical to a missing one.
* Projects created before ownership existed belong to `ADF2FABRIC_LEGACY_ORG` (default `default`).
* Jobs, uploads and sessions are scoped the same way (and uploads to the uploading user). A session id chosen by a client is namespaced by
  organisation and user, so it cannot reach another principal's session or held secrets.
* An import never overwrites another organisation's project (409). It does disclose that the name is taken.
* **Grants** limit what a client may point the server at: `subscriptions`, `tenants`, `accounts`, `workspaces` (lists, or `*`). A
  client-chosen value is checked at every use (selection, import, stage, connection apply); lists of inventory are filtered by grants.
  Nobody can create a user with more permissions or grants than they hold.

## Input and request protection

* Identifiers that become paths or are passed to tools (project, upload, session, resource names, GUIDs, linked service names, git
  revisions, subdirectories) are validated and rejected, not repaired. Git sources must be `https`, public, port 443, without credentials in
  the URL, not an internal name, and (after DNS resolution) public addresses only; `ADF2FABRIC_ALLOWED_GIT_HOSTS` narrows it further.
* Archives: size-capped while streaming, checked to be a ZIP, owner-bound, at most 5 pending per user, extracted only by the hardened
  reader (rejects traversal, absolute and drive paths, symlinks, encrypted entries, case collisions, bombs) and deleted after use.
* Middleware before every route: Host allow-list (DNS rebinding), Origin check on state-changing requests (CSRF for the no-credential
  local mode), `Content-Type: application/json` required for state changes (the form types a cross-site page can send without a preflight
  are refused), JSON size cap, refusal of undeclared (chunked) JSON, security headers, request id, rate limits (per principal; stricter for
  sensitive operations; a loose ceiling per client).
* CORS: only `ADF2FABRIC_ALLOWED_ORIGINS`, specific methods, no credentials. API documentation endpoints are disabled.
* Errors: validation errors never echo submitted values; unexpected errors are generic; job logs and errors have server paths removed
  and secrets redacted; no stack trace leaves the server.
* SQL / command injection: the API builds no SQL or shell strings from client input; Git and Azure CLI calls take validated arguments.

## Audit

`<projects>/_audit/audit.jsonl`: one JSON object per sensitive action (sign-in failures and lockouts, authorisation and grant denials,
rate limiting, uploads, imports, stage runs, connection applies, workspace identity, credential sign-in/out, user changes, and the CLI
commands that deploy, run, verify, provision, approve or create users). Fields: time, request id, actor, organisation, action, resource,
outcome and a few short redacted details. **Never** secrets, tokens, request bodies, URLs with credentials or client addresses (a failed
sign-in records a short hash of the address). Lines are hash-chained (`adf2fabric audit-verify`): tamper evidence, not proof. Ship the log
to append-only storage for real assurance. Read your organisation's events with `GET /admin/audit` (`users:manage`).

## Provisioning, validation and the guided UI routes

These are exposed over HTTP, each with its own permission (the role table in `tests/test_api_security.py` lists every one):

| Route | Permission |
|---|---|
| `GET .../inventory`, `waves`, `compatibility`, `validation`, `report`, `provisioning` | `reports:view` |
| `POST .../provisioning/plan` (read-only) | `plans:generate` + `azure:use` |
| `POST .../provisioning/approve` | `provision:approve` (not held by analysts or operators) |
| `POST .../provisioning/apply` | `migration:execute`; `apply: true` also needs `confirm: true` and a finished dry run of the same, approved, unchanged plan |
| `POST .../stages/deploy_apply` | `migration:execute`; needs `confirm: true` and a dry run of the same pipelines in the same workspace within the last hour |
| `POST .../stages/dependencies`, `workloads`, `validate` | `discovery:run` / `plans:generate` |
| `POST .../stages/verify` with `run: true` (starts REAL Fabric runs, then compares with ADF) | `plans:generate` + `migration:execute`; needs `confirm: true` and 1-20 named pipelines; audited as `verify.run` |

* Views go through `api/views.sanitize`: keys that can carry paths, tokens, connection strings, code or hashes are dropped, strings are
  scrubbed of paths and bearer tokens and bounded, and job results and errors leave the server the same way. Provisioning steps are shown by an
  opaque `ref` and the workspace by its display name, never by resource id. Redaction is server-side; the UI never masks.
* `Idempotency-Key` (8-64 chars `[A-Za-z0-9_-]`) makes a repeated submission return the first job (`Idempotent-Replay: true`) for 15 minutes,
  per person and only for the exact same project, action and request: the same key for anything else is a 422. Without it, a second job while one runs on the project is refused (409).
* `ADF2FABRIC_ENV=production` refuses demo mode (403) and builds no stand-in Fabric: nothing canned can pass for a real result.
* The UI hides buttons the signed-in person's role cannot use. That is a convenience; the API enforces every permission itself.

The CLI commands (`provision-*`, `workloads-*`, `validate`) still rely on the operating-system user and their own approval gates, and are audited.

## Remaining limitations (do not skip these)

* **No external identity provider.** Tokens are issued and stored by this application. There is no OIDC / Entra sign-in, no MFA, no SSO,
  no token expiry (rotate by policy). Integrating Entra ID (validating signed JWTs against the tenant's keys) is the right next step and
  is not implemented.
* **Local mode is single-user.** `disabled` mode has no identity; its only protections are loopback binding and the Host / Origin /
  Content-Type checks.
* **The Streamlit UI has no authentication of its own beyond asking for a token.** Anyone who can reach the UI can try tokens; the UI
  process must not be given `ADF2FABRIC_API_TOKEN` when shared (that token is then everyone's). Put it behind your own access control.
* **TLS is not terminated here.** Tokens cross the network in clear unless you terminate TLS in front (required beyond loopback).
  Client addresses are taken from the socket, not forwarded headers, so behind a proxy all clients look alike to the lockout and limits.
* **Rate limits and lockouts are per process and in memory.** Multiple instances, or a restart, reset them.
* **The Azure CLI session is shared by everyone the server serves.** Grants limit what each person may target, but all calls run as the
  server's signed-in identity; Azure's audit log shows that identity, not the API user (our audit log does).
* **SSRF is checked at request time.** DNS can change before `git` fetches; use the allow-list where you can.
* **Project files on disk are not encrypted** and rely on filesystem permissions. Source ADF JSON can contain secrets that its authors
  stored there (validation reports them without printing them).
* **Dependencies are not scanned** here; run your usual supply-chain checks on `pip freeze`.
* **No independent penetration test has been done.** A black-box probe of the running API (authn, authz, tenancy, SSRF, uploads, races, secret
  leakage) and a UI probe were run by the developers; its findings are fixed and covered by `tests/test_security_probe.py`. bandit and pip-audit
  were not available, so no static-analysis or CVE scan was done.

## Deployment checks (all must pass before calling a deployment production-secure)

1. `python -m pytest tests/test_api_security.py` passes, and the full suite passes.
2. `ADF2FABRIC_AUTH_MODE=token`, named users created, no `ADF2FABRIC_API_TOKEN` in the UI's environment; `GET /me` without a token is 401.
3. TLS terminates in front of the API; `ADF2FABRIC_ALLOWED_HOSTS` and `ADF2FABRIC_ALLOWED_ORIGINS` list only your real names.
4. Grants are set per user (no `*` except for administrators); `ADF2FABRIC_ALLOWED_GIT_HOSTS` is set.
5. The projects folder (including `_auth` and `_audit`) has restrictive filesystem permissions and is backed up; the audit log is shipped
   elsewhere; `audit-verify` is scheduled.
6. The server's Azure CLI identity has least-privilege Azure RBAC (read-only for discovery; write only where migration needs it).
7. An independent security review and a penetration test have been completed and their findings closed.
