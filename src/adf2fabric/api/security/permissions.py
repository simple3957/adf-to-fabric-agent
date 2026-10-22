"""Permissions and roles: least privilege, defined once, enforced on the server for every protected operation.

A permission names one kind of action. A role is a named bundle; a principal holds roles and so the union of their permissions.
Nothing in the UI decides what a person may do: the UI may hide a button (a convenience, read from ``GET /me``), but every route
re-checks on the server, and a request that lacks the permission is refused whatever the UI showed.

| Permission          | Allows                                                                                         |
|---------------------|------------------------------------------------------------------------------------------------|
| projects:view       | list and read projects in your organisation                                                    |
| reports:view        | read conversion summaries, hand-over and connection reports, job results                        |
| sources:import      | upload an archive, test a source, import a source into a new project                            |
| discovery:run       | run discovery / semantic / reconstruction; read Azure inventory you are granted                 |
| plans:generate      | convert, assess compute, set connection choices, dry-run deployments, read-only verify         |
| azure:use           | use the server's Azure sign-in for the subscriptions / tenants / accounts you are granted      |
| provision:approve   | approve changes to the target that create or configure resources (workspace identity ...)      |
| migration:execute   | change the target: deploy with apply, create connections for real                               |
| credentials:manage  | start or end the server's Azure / Fabric CLI sign-in (it is machine-wide)                        |
| users:manage        | create, change and disable users of your organisation                                           |
"""

from __future__ import annotations

from enum import Enum


class Permission(str, Enum):
    projects_view = "projects:view"
    reports_view = "reports:view"
    sources_import = "sources:import"
    discovery_run = "discovery:run"
    plans_generate = "plans:generate"
    azure_use = "azure:use"
    provision_approve = "provision:approve"
    migration_execute = "migration:execute"
    credentials_manage = "credentials:manage"
    users_manage = "users:manage"


P = Permission

ROLES: dict[str, frozenset[Permission]] = {
    "viewer": frozenset({P.projects_view, P.reports_view}),
    "analyst": frozenset({P.projects_view, P.reports_view, P.sources_import, P.discovery_run, P.plans_generate, P.azure_use}),
    "approver": frozenset({P.projects_view, P.reports_view, P.provision_approve}),
    "operator": frozenset({P.projects_view, P.reports_view, P.sources_import, P.discovery_run, P.plans_generate, P.azure_use,
                           P.migration_execute}),
    "admin": frozenset(Permission),
}

# Migration execution and approval are deliberately separate from planning: an analyst can prepare everything, and only someone
# with execute (and, for resource creation, approve) can change the target. Separation of duties is a configuration choice: give
# one person both roles only if your process allows it.


def permissions_for(roles: "set[str] | frozenset[str] | list[str]") -> frozenset[Permission]:
    out: set[Permission] = set()
    for r in roles:
        out |= ROLES.get(r, frozenset())
    return frozenset(out)


def valid_role(name: str) -> bool:
    return name in ROLES
