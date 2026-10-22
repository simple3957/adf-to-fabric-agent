# Fabric target provisioning

Decides which Fabric target resources the migration needs, which already exist, which can be reused, and which must be
created, then creates only what a person approved. Code: `src/adf2fabric/agent/provisioning/`, records
`projects/<name>/provisioning/`, contract `project/provisioning.py`.

```
adf2fabric provision-plan    --project P --workspace-name W --capacity-name C [--tenant-id T] [--lakehouse L] ...   # read-only
adf2fabric provision-approve --project P --step <id> [--step ...] | --all-approvable --by "<name>"               # a person decides
adf2fabric provision-apply   --project P [--apply]                                                              # dry run unless --apply
adf2fabric provision-verify  --project P                                                                        # read-only
```

## Four stages, four records

| Stage | Reads | Writes (Azure / Fabric) | Record |
|---|---|---|---|
| discovery (`state.py`) | capacities, workspace, role (via the list-by-role filter), items, pools, role assignments, connections, the Azure capacity | **nothing** (GET only; a test enforces it) | `state.yaml` |
| planning (`requirements.py`, `planner.py`) | state + compute plan, workload manifest, dependency inventory | nothing (the one POST is ARM's `checkNameAvailability`, a documented query that creates nothing) | `target.yaml`, `plan.yaml`, `plan.md` |
| provisioning (`executor.py`) | fresh state | only approved create / additive-configure steps | `approval.yaml`, `run.yaml` (journal, replaced atomically after every step) |
| verification (`verifier.py`) | the target, again | nothing | `verification.yaml`, `verification.md` |

Only verification may say a resource exists: the executor reports `applied (not yet verified)`. Connection tests and every
discovery command create nothing.

## What is required, from what

Only what the migration needs: a Spark pool and Environment per compute-plan proposal (none when the project has no Spark
compute); a notebook / Spark job definition per generated workload; a Fabric connection per linked service the dependency
inventory marks `needs_connection`; a lakehouse when workloads need one (never invented: you name it, otherwise the step is
blocked); and whatever you ask for explicitly (capacity, workspace, identity, role assignments, warehouses).

## Which API does what (none assumed to cover another)

| Resource | API | Operation |
|---|---|---|
| Fabric capacity (new) | Azure Resource Manager `2023-11-01` | `PUT .../Microsoft.Fabric/capacities/{name}` (needs Azure RBAC) |
| Workspace | Fabric REST | `POST /v1/workspaces` (201) |
| Capacity assignment | Fabric REST | `POST /v1/workspaces/{id}/assignToCapacity` (202; polled via `capacityAssignmentProgress`) |
| Workspace identity | Fabric REST | `POST /v1/workspaces/{id}/provisionIdentity` (200 or 202 LRO) |
| Role assignment | Fabric REST | `POST /v1/workspaces/{id}/roleAssignments` (201) |
| Lakehouse / warehouse | Fabric REST | `POST /v1/workspaces/{id}/lakehouses` / `warehouses` (201 or 202) |
| Spark pool | Fabric REST | `POST /v1/workspaces/{id}/spark/pools` |
| Environment | Fabric REST | `POST /v1/workspaces/{id}/environments` (created, **not published**: publishing runs a Spark job and is a separate step) |
| Connections | **delegated** to `connections apply` (credentials) | |
| Notebooks, Spark job definitions | **delegated** to `workloads-deploy` (approved workloads only) | |

## Before anything is provisioned

Checks (each says what it proved and what it did not): Fabric access; tenant (the token's `tid` against `--tenant-id`; a mismatch
blocks); capacity visible, Active and its region; capacity size vs the largest planned pool (1 capacity unit = 2 Spark vCores);
your role in an existing workspace (Admin / Member / Contributor / Viewer); and for a new Azure capacity: subscription readable
and Enabled and in the expected tenant, `Microsoft.Fabric` registered, SKU offered in the region, quota left (best effort),
name available. **Cannot be checked without creating something** (can this account create workspaces?) is reported `unknown`,
never `passed`; a refusal is reported when applied.

Blocking checks stop approval. Blocked, conflicting or unknown steps cannot be approved, and steps depending on them are blocked
or unknown too.

## Idempotency and safety

* Approval names steps and binds to the plan hash. Apply rebuilds the plan from freshly read state; if the hash differs
  (someone created or changed something) it refuses and writes nothing.
* Every write re-reads the live state first; an existing resource is reused, never overwritten. A `409` on a unique name
  between the check and the write re-finds the resource and reuses it.
* Retries: throttling (429, `Retry-After`) is retried (it never took effect). A 5xx / dropped connection on a write is
  *ambiguous*: the live state is re-read, and if the resource now exists it is recovered, not created again. A clear refusal
  (403, 400, a failed operation) is not retried.
* A failure skips only the steps that depend on it; independent steps finish. The journal records every step, and a re-run
  after fixing the cause creates only what is missing.
* Never deleted or modified: a workspace on a different capacity, a role with a different level, a same-named pool with other
  settings, an item name used by another type, an Azure capacity of another size are **conflicts** left for a person.
* No token, secret or credential is written to any record; error text is redacted; each service only receives its own token
  (a Fabric token cannot be sent to ARM, or the reverse).

## Permissions and approvals

| To do | You need |
|---|---|
| read the plan | a Fabric account that can list capacities / workspaces; Viewer in an existing workspace (role detection works for any role) |
| create a workspace | the tenant setting that lets you create workspaces (a service principal also needs "Service principals can create workspaces, connections, and deployment pipelines"); capacity Contributor/Admin to assign it at creation |
| assign / reassign a capacity | capacity Contributor or Admin **and** workspace Admin |
| workspace identity | workspace Admin |
| role assignments | Member or higher (a Member can grant Member or lower) |
| lakehouse, warehouse, environment | Contributor |
| Spark pool | workspace Admin |
| new Azure capacity | `Microsoft.Fabric/capacities/{read,write}` (Contributor or a custom role), `Microsoft.Fabric` registered, quota in the region (Quota Request Operator to raise it), capacity admins who are tenant members (no B2B guests); **a running capacity is billed until paused** |
| approve | a named person (`--by`); the approval is recorded with the plan hash and step ids |

Approvals required: one per plan, naming each create / configure step (`--step`, or `--all-approvable` after reading the plan).
Creating an Azure capacity is a separate named step: there is no flag that creates one implicitly.

## What was verified, and what was not

Verified against Microsoft Learn (2026-10-22): the operations, bodies and scopes above; `capacityAssignmentProgress` on Get
Workspace; `workspaceIdentity` on Get Workspace; the role and capacity enums; 429 + `Retry-After` and the LRO pattern; ARM
`2023-11-01` capacity PUT, SKU list, name check, quota in CUs. Read-only against a real tenant: `provision-plan` was run on
`adf-migration-test` (capacities, workspace, role, items, pools, connections all read; nothing created).

**Not documented / not verified (treated conservatively):** the duplicate-workspace error code (a 409 is handled generically);
role-assignment duplicate behaviour and the list endpoint's exact shape (existing assignments are checked first); the shape of
the ARM `usages` response (an unrecognised shape is `unknown`, not `passed`); that a new capacity's Fabric `displayName` equals
its ARM name (it is looked up by that name and apply stops with a clear message if it does not appear); no non-admin way to read
the "create workspaces" tenant setting; the write paths (create/assign/identity/roles/pools/lakehouses/ARM) have been exercised
against a fake only, never live.
