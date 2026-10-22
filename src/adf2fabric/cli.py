"""Command-line entry point for the ADF -> Fabric migration agent.

Currently the only capability is Intake: authenticate with GitHub, fetch a
source repository, and record its metadata. Later lifecycle stages (Discovery
onward) are documented in the README but not implemented.
"""

from __future__ import annotations

import json
import sys

import click

from adf2fabric.agent.consultation import ConsultationAgent, ConsultationRequest
from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.reconstruction.agent import ReconstructionAgent
from adf2fabric.agent.reconstruction.models import ReconstructionRequest
from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf import AdfDiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoveryRequest
from adf2fabric.agent.discovery.adls import AdlsDiscoveryRequest
from adf2fabric.agent.discovery.adls_paths import AdlsPathsDiscoveryRequest
from adf2fabric.agent.discovery.sql import SqlDiscoveryRequest
from adf2fabric.agent.discovery.sql_structural import SqlStructuralDiscoveryRequest
from adf2fabric.agent.intake import AzureIntakeRequest, IntakeAgent, IntakeRequest
from adf2fabric.agent.intake.azure_source import KIND_CHOICES as AZURE_KIND_CHOICES
from adf2fabric.config import load_config, projects_root
from adf2fabric.project.models import (
    AdfAccess,
    AzureCredentialRef,
    SqlCredentialRef,
    SqlServerAccess,
    SqlTlsMode,
    SqlTlsPolicy,
)
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify


def _os_user() -> str:
    import getpass

    try:
        return f"os:{getpass.getuser()}"
    except Exception:  # noqa: BLE001
        return "os:unknown"


def _audit_log():
    import os
    from pathlib import Path

    from adf2fabric.api.security.audit import AuditLog

    return AuditLog(Path(os.environ["ADF2FABRIC_AUDIT_DIR"]) if os.environ.get("ADF2FABRIC_AUDIT_DIR") else projects_root(load_config()))


def _audited(action: str):
    """Record a sensitive CLI command in the audit log: who (the OS user), what, and whether it finished. Never arguments that could hold a secret.

    Commands with an ``--apply`` flag are recorded only when it is set (a dry run changes nothing); approval commands always.
    """
    import functools

    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not kwargs.get("apply_", True):
                return fn(*args, **kwargs)
            try:
                log = _audit_log()
            except Exception:  # noqa: BLE001 - auditing must not stop the command, but the failure is visible
                click.secho("warning: the audit log could not be opened; this action is not being recorded", fg="yellow", err=True)
                return fn(*args, **kwargs)
            detail = {k: kwargs[k] for k in ("project_name", "workspace_id", "workload", "by") if kwargs.get(k)}
            log.event(f"cli.{action}", "requested", actor=_os_user(), org="cli", **detail)
            try:
                result = fn(*args, **kwargs)
            except SystemExit as exc:
                log.event(f"cli.{action}", "ok" if exc.code in (0, None) else "failed", actor=_os_user(), org="cli", **detail)
                raise
            except Exception:
                log.event(f"cli.{action}", "failed", actor=_os_user(), org="cli", **detail)
                raise
            log.event(f"cli.{action}", "ok", actor=_os_user(), org="cli", **detail)
            return result

        return wrapper

    return deco


def _user_store():
    from adf2fabric.api.security import UserStore

    return UserStore(projects_root(load_config()))


# --- shared Azure authentication options ---------------------------------------------------------------
# Every command that talks to Azure accepts every mechanism the credential layer supports. Options name
# environment variables (never values); only the ones the chosen mechanism needs are read.

_AUTH_ENV_OPTIONS = (
    ("tenant_id_env", "the tenant id (azure_cli, client_secret, client_certificate; optional for interactive_browser/device_code)"),
    ("client_id_env", "the client id (client_secret, client_certificate; optional user-assigned id for managed_identity)"),
    ("client_secret_env", "the client secret (client_secret)"),
    ("certificate_path_env", "the path of a PEM/PFX certificate file (client_certificate)"),
    ("certificate_password_env", "the certificate password (client_certificate, optional)"),
    ("access_token_env", "a pre-acquired bearer token (access_token)"),
    ("account_key_env", "the storage account key (account_key; ADLS Gen2 only)"),
    ("sas_token_env", "a storage SAS token (sas_token; ADLS Gen2 only)"),
    ("connection_string_env", "a storage connection string (connection_string; ADLS Gen2 only)"),
)


def azure_auth_options(*, storage: bool = False, default: str = "azure_cli"):
    """Decorator adding ``--auth`` plus the ``--*-env`` options for every supported mechanism."""
    from adf2fabric.connectivity._azure import AUTH_MECHANISMS, ENTRA_MECHANISMS

    choices = AUTH_MECHANISMS if storage else ENTRA_MECHANISMS

    def decorate(fn):
        for name, what in reversed(_AUTH_ENV_OPTIONS):
            if not storage and name in ("account_key_env", "sas_token_env", "connection_string_env"):
                continue
            fn = click.option(f"--{name.replace('_', '-')}", name, default=None,
                              help=f"NAME of the env var holding {what}. Never the value itself.")(fn)
        return click.option(
            "--auth", type=click.Choice(choices), default=default, show_default=True,
            help="Azure authentication mechanism (azure_cli reuses your `az login`).")(fn)

    return decorate


def credential_from(auth: str, **env_names: str | None) -> AzureCredentialRef:
    """An AzureCredentialRef from CLI values (env-var NAMES only)."""
    return AzureCredentialRef(auth=auth, **{k: v for k, v in env_names.items() if v})


@click.group()
def cli() -> None:
    """ADF -> Microsoft Fabric migration agent."""


@cli.command()
@click.option("--project", "project_name", required=True, help="Project/engagement name.")
@click.option("--repo", "repository_url", default=None, help="Mode B: source repository URL (GitHub).")
@click.option("--branch", default=None, help="Branch to fetch (default from config).")
@click.option("--provider", default=None, help="Source provider (default from config).")
@click.option("--refresh", is_flag=True, help="Force a fresh fetch even if a local source exists.")
@click.option("--subdirectory", default=None, help="Mode B: repository subdirectory holding the ADF folders.")
@click.option("--factory", "factory_name", default=None, help="Mode A: Azure Data Factory name.")
@click.option("--resource-group", "resource_group", default=None, help="Mode A: resource group of the factory.")
@click.option(
    "--subscription", "subscription_id", default=None,
    help="Mode A: Azure subscription id (default: the active `az login` subscription).",
)
@click.option(
    "--pipeline", "pipelines", multiple=True,
    help="Only this pipeline (and what it references). Repeatable.",
)
@click.option(
    "--artifact-kind", "artifact_kinds", multiple=True,
    type=click.Choice(AZURE_KIND_CHOICES, case_sensitive=False),
    help="Mode A: restrict retrieved artifact kinds (pipelines are always included). Repeatable.",
)
@azure_auth_options()
@click.option("--allow-partial", is_flag=True, help="Mode A: keep the source even if some artifacts could not be retrieved.")
def intake(
    project_name: str,
    repository_url: str | None,
    branch: str | None,
    provider: str | None,
    refresh: bool,
    subdirectory: str | None,
    factory_name: str | None,
    resource_group: str | None,
    subscription_id: str | None,
    pipelines: tuple[str, ...],
    artifact_kinds: tuple[str, ...],
    allow_partial: bool,
    auth: str,
    **auth_env: str | None,
) -> None:
    """Obtain ADF source: from a GitHub repo (--repo) or a live factory (--factory)."""
    if bool(repository_url) == bool(factory_name):
        raise click.UsageError(
            "Choose exactly one source: --repo <url> (GitHub) or --factory <name> "
            "(live Azure Data Factory, with --resource-group)."
        )
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = IntakeAgent(store, token_env=config["github_token_env"])

    if factory_name:
        if not resource_group:
            raise click.UsageError("--resource-group is required with --factory.")
        if subdirectory or branch or provider or refresh:
            raise click.UsageError(
                "--branch/--provider/--refresh/--subdirectory apply to --repo only."
            )
        result = agent.run_azure(
            AzureIntakeRequest(
                project_name=project_name,
                resource_group=resource_group,
                factory_name=factory_name,
                subscription_id=subscription_id,
                pipelines=list(pipelines),
                artifact_kinds=list(artifact_kinds),
                credential=credential_from(auth, **auth_env),
                allow_partial=allow_partial,
            )
        )
    else:
        if resource_group or subscription_id or artifact_kinds or allow_partial:
            raise click.UsageError(
                "--resource-group/--subscription/--artifact-kind/--allow-partial apply to --factory only."
            )
        result = agent.run(
            IntakeRequest(
                project_name=project_name,
                repository_url=repository_url,
                branch=branch or config["default_branch"],  # None: the repository's own default branch
                provider=provider or config["default_provider"],
                refresh=refresh,
                subdirectory=subdirectory,
                pipelines=list(pipelines),
            )
        )

    if not result.success:
        click.secho(f"Intake failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    meta = result.project.repository
    verb = "Reused" if result.reused else ("Retrieved" if factory_name else "Fetched")
    click.secho(f"Intake succeeded ({verb.lower()}).", fg="green")
    click.echo(f"  project:      {result.project.id}")
    click.echo(f"  provider:     {meta.provider}")
    click.echo(f"  source:       {meta.repository_url}")
    click.echo(f"  branch:       {meta.branch}")
    click.echo(f"  commit:       {meta.commit_sha}")
    click.echo(f"  local source: {meta.local_source_path}")
    _echo_manifest(result.manifest, store, result.project.id)


def _echo_manifest(manifest, store: ProjectStore, project_id: str) -> None:
    """Print the source manifest summary plus every failure/finding (never silent)."""
    if manifest is None:
        return
    kinds: dict[str, int] = {}
    for a in manifest.artifacts:
        kinds[a.kind] = kinds.get(a.kind, 0) + 1
    summary = ", ".join(f"{n} {k}" for k, n in sorted(kinds.items())) or "no artifacts"
    click.echo(f"  artifacts:    {summary}")
    if manifest.selection.pipelines:
        click.echo(f"  selection:    {', '.join(manifest.selection.pipelines)}")
    if manifest.redactions:
        click.echo(f"  redacted:     {len(manifest.redactions)} secret value(s) masked")
    click.echo(f"  manifest:     {store.source_manifest_path(project_id)}")
    for f in manifest.failures:
        click.secho(f"  FAILED [{f.kind}]: {f.error} {f.action}", fg="red")
    for f in manifest.findings:
        if f.code == "retrieval_failed":
            continue  # already shown above
        colour = "red" if f.severity.value == "error" else "yellow"
        click.secho(f"  {f.severity.value.upper()} [{f.code}]: {f.message} {f.action}", fg=colour)
    if not manifest.complete:
        click.secho("  source is INCOMPLETE; see the items above.", fg="yellow")


@cli.command()
@click.option("--project", "project_name", required=True, help="Project/engagement name (Intake must have run).")
@click.option(
    "--input",
    "input_path",
    required=True,
    type=click.Path(exists=False),
    help="Path to the consultation questionnaire YAML.",
)
def consultation(project_name: str, input_path: str) -> None:
    """Validate engagement readiness (access + connectivity) before Discovery."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = ConsultationAgent(store)

    result = agent.run(
        ConsultationRequest(project_name=project_name, input_path=input_path)
    )

    if not result.success:
        click.secho(f"Consultation failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    c = result.consultation
    m = c.migration
    click.echo(
        "Migration:  ADF -> Fabric   "
        f"source warehouse: SQL Server   storage: ADLS Gen2   mode: {m.mode.value}"
    )

    click.echo("Access:")
    for req in c.access:
        if not req.required:
            state = "not required"
        elif req.validated:
            state = "validated"
        else:
            status = req.connectivity.status.value if req.connectivity else "unknown"
            state = f"NOT validated ({status})"
        click.echo(f"  {req.domain.value:<12} {state}")

    ready = c.readiness.status.value == "ready"
    click.secho(
        f"Readiness:  {c.readiness.status.value.upper()}",
        fg="green" if ready else "red",
    )
    if not ready:
        for blocker in c.readiness.blockers:
            click.echo(f"  blocked: {blocker.requirement} — {blocker.reason}")
    for warning in c.readiness.warnings:
        click.secho(f"  warning: {warning}", fg="yellow")

    click.echo(f"  record: {store.consultation_path(c.project_id)}")

    if not ready:
        sys.exit(1)


@cli.command()
@click.option(
    "--project",
    "project_name",
    required=True,
    help="Project/engagement name (Intake must have run).",
)
def discovery(project_name: str) -> None:
    """Inventory the source repository (read-only, deterministic, factual)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = DiscoveryAgent(store)

    result = agent.discover_repository(DiscoveryRequest(project_name=project_name))

    if not result.success:
        click.secho(f"Repository Discovery failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    d = result.discovery
    s = d.structure
    click.secho("Repository Discovery complete.", fg="green")
    click.echo(f"  project:     {d.project_id}")
    click.echo(f"  commit:      {d.source_commit_sha}")
    click.echo(f"  files:       {s.total_files}   directories: {s.total_directories}")
    click.echo(f"  total size:  {s.total_size_bytes} bytes")

    if d.artifact_kind_counts:
        click.echo("  ADF candidates:")
        for kind, count in d.artifact_kind_counts.items():
            click.echo(f"    {kind:<22} {count}")
    else:
        click.echo("  ADF candidates: none identified")

    if not d.complete:
        click.secho(
            f"  incomplete: {d.skipped_count} skipped, {d.failed_count} failed",
            fg="yellow",
        )

    click.echo(f"  record: {store.repository_discovery_path(d.project_id)}")


@cli.command(name="discovery-adls")
@click.option(
    "--project",
    "project_name",
    required=True,
    help="Project/engagement name (Intake must have run).",
)
@click.option(
    "--storage-account",
    "storage_account",
    required=True,
    help="Explicit target ADLS Gen2 storage account (e.g. 'adfstorage299').",
)
def discovery_adls(project_name: str, storage_account: str) -> None:
    """Enumerate the filesystems/containers in an ADLS Gen2 account (read-only)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = DiscoveryAgent(store)

    result = agent.discover_adls(
        AdlsDiscoveryRequest(
            project_name=project_name, storage_account=storage_account
        )
    )

    d = result.discovery
    if d is not None:
        click.echo(f"Storage account:  {d.storage_account}")
        click.echo("Authentication:   AzureCliCredential")
        click.echo(f"Status:           {d.status.value}")

    if not result.success:
        click.secho(f"ADLS Discovery failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    click.secho("ADLS Discovery complete.", fg="green")
    click.echo(f"Filesystem count: {d.filesystem_count}")
    if d.filesystem_count == 0:
        click.echo("No filesystems were returned for the authenticated identity.")
    else:
        click.echo("Filesystems:")
        for i, fs in enumerate(d.filesystems, 1):
            lm = f"  last_modified={fs.last_modified}" if fs.last_modified else ""
            click.echo(f"  {i}. {fs.name}{lm}")
    click.echo(f"  record: {store.adls_discovery_path(d.project_id)}")


@cli.command(name="discovery-adls-paths")
@click.option(
    "--project",
    "project_name",
    required=True,
    help="Project/engagement name (ADLS Phase-1 discovery must have run).",
)
def discovery_adls_paths(project_name: str) -> None:
    """Enumerate directories and files inside the Phase-1 filesystem(s) (read-only)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = DiscoveryAgent(store)

    result = agent.discover_adls_paths(
        AdlsPathsDiscoveryRequest(project_name=project_name)
    )

    d = result.discovery
    if d is not None:
        click.echo(f"Storage account:  {d.storage_account}")
        click.echo("Authentication:   AzureCliCredential")
        click.echo(f"Status:           {d.status.value}")

    if not result.success:
        click.secho(f"ADLS path discovery failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    click.secho("ADLS path discovery complete.", fg="green")
    click.echo(
        f"  filesystems: {d.filesystem_count}   paths: {d.total_path_count}   "
        f"directories: {d.total_directory_count}   files: {d.total_file_count}   "
        f"total size: {d.total_size_bytes} bytes"
    )
    for fs in d.filesystems:
        flag = "  (TRUNCATED)" if fs.truncated else ""
        click.echo(
            f"    {fs.name}: {fs.directory_count} dir(s), {fs.file_count} file(s){flag}"
        )
    click.echo("reads = 0   downloads = 0   writes = 0   deletes = 0")
    click.echo(f"  record: {store.adls_paths_discovery_path(d.project_id)}")


@cli.command(name="discovery-sql")
@click.option(
    "--project",
    "project_name",
    required=True,
    help="Project/engagement name (Intake must have run).",
)
@click.option("--server", required=True, help="Explicit SQL Server instance (e.g. 'HOST\\SQLEXPRESS').")
@click.option("--database", required=True, help="Explicit target database (e.g. 'T20').")
@click.option(
    "--auth",
    type=click.Choice(["integrated", "sql_login"]),
    default="integrated",
    show_default=True,
    help="Authentication mode. 'integrated' needs no password.",
)
@click.option("--username", default=None, help="SQL login username (sql_login only).")
@click.option(
    "--password-env",
    "password_env",
    default=None,
    help="NAME of the environment variable holding the password (sql_login only). "
    "The value is never read into logs, output, or the persisted record.",
)
@click.option("--driver", default=None, help="Local ODBC driver name (overrides the default).")
@click.option(
    "--tls-mode",
    "tls_mode",
    required=True,
    type=click.Choice(["strict", "trust_server_certificate", "disabled"]),
    help="Explicit TLS policy. Required — never inferred.",
)
def discovery_sql(
    project_name: str,
    server: str,
    database: str,
    auth: str,
    username: str | None,
    password_env: str | None,
    driver: str | None,
    tls_mode: str,
) -> None:
    """Inventory a SQL Server database's structural estate (read-only catalog metadata)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = DiscoveryAgent(store)

    access = SqlServerAccess(
        required=True,
        server=server,
        database=database,
        driver=driver,
        tls=SqlTlsPolicy(mode=SqlTlsMode(tls_mode)),
        credential=SqlCredentialRef(
            auth=auth, username=username, password_env=password_env
        ),
    )

    result = agent.discover_sql(
        SqlDiscoveryRequest(project_name=project_name, access=access)
    )

    d = result.discovery
    if d is not None:
        click.echo("SQL SERVER")
        click.echo(f"  Server:         {d.server}")
        click.echo(f"  Database:       {d.database}")
        click.echo(f"  Authentication: {d.authentication}")
        click.echo(f"  TLS:            {tls_mode}")
        click.echo(f"  Status:         {d.status.value}")

    if not result.success:
        click.secho(f"SQL Discovery failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    click.secho("SQL Discovery complete.", fg="green")
    _echo_sql_objects("SCHEMAS", d.schema_count, [f"{name}" for name in d.schemas])
    _echo_sql_objects(
        "TABLES", d.table_count, [f"{o.schema}.{o.name}" for o in d.tables]
    )
    _echo_sql_objects("VIEWS", d.view_count, [f"{o.schema}.{o.name}" for o in d.views])
    _echo_sql_objects(
        "STORED PROCEDURES",
        d.procedure_count,
        [f"{o.schema}.{o.name}" for o in d.procedures],
    )

    click.echo(f"metadata queries performed: {len(d.metadata_queries)}")
    click.echo("business-data queries = 0   writes = 0   DDL = 0   EXEC = 0")
    click.echo(f"  record: {store.sql_discovery_path(d.project_id)}")


def _echo_sql_objects(title: str, count: int, names: list[str]) -> None:
    click.echo(f"{title} ({count})")
    for name in names:
        click.echo(f"  {name}")


@cli.command(name="discovery-sql-structural")
@click.option("--project", "project_name", required=True, help="Project/engagement name (Intake must have run).")
@click.option("--server", required=True, help="Explicit SQL Server instance (e.g. 'HOST\\SQLEXPRESS').")
@click.option("--database", required=True, help="Explicit target database (e.g. 'T20').")
@click.option(
    "--auth",
    type=click.Choice(["integrated", "sql_login"]),
    default="integrated",
    show_default=True,
    help="Authentication mode. 'integrated' needs no password.",
)
@click.option("--username", default=None, help="SQL login username (sql_login only).")
@click.option(
    "--password-env",
    "password_env",
    default=None,
    help="NAME of the environment variable holding the password (sql_login only). "
    "The value is never read into logs, output, or the persisted record.",
)
@click.option("--driver", default=None, help="Local ODBC driver name (overrides the default).")
@click.option(
    "--tls-mode",
    "tls_mode",
    required=True,
    type=click.Choice(["strict", "trust_server_certificate", "disabled"]),
    help="Explicit TLS policy. Required — never inferred.",
)
def discovery_sql_structural(
    project_name: str,
    server: str,
    database: str,
    auth: str,
    username: str | None,
    password_env: str | None,
    driver: str | None,
    tls_mode: str,
) -> None:
    """Discover a SQL database's structural metadata: columns, keys, relationships (read-only)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = DiscoveryAgent(store)

    access = SqlServerAccess(
        required=True,
        server=server,
        database=database,
        driver=driver,
        tls=SqlTlsPolicy(mode=SqlTlsMode(tls_mode)),
        credential=SqlCredentialRef(auth=auth, username=username, password_env=password_env),
    )

    result = agent.discover_sql_structural(
        SqlStructuralDiscoveryRequest(project_name=project_name, access=access)
    )

    d = result.discovery
    if d is not None:
        click.echo("SQL SERVER (structural / Phase 2)")
        click.echo(f"  Server:         {d.server}")
        click.echo(f"  Database:       {d.database}")
        click.echo(f"  Authentication: {d.authentication}")
        click.echo(f"  TLS:            {tls_mode}")
        click.echo(f"  Status:         {d.status.value}")

    if not result.success:
        click.secho(f"SQL structural discovery failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    click.secho("SQL structural discovery complete.", fg="green")
    click.echo(
        f"  tables: {d.table_count}   columns: {d.column_count}   "
        f"primary keys: {d.primary_key_count}   foreign keys: {d.foreign_key_count}   "
        f"relationships: {len(d.relationships)}"
    )
    for t in d.tables:
        pk = ",".join(c.column for c in t.primary_key.columns) if t.primary_key else "-"
        click.echo(
            f"    {t.schema}.{t.table}: {len(t.columns)} cols   pk=[{pk}]   "
            f"fks={len(t.foreign_keys)}"
        )
    click.echo(f"metadata queries performed: {len(d.metadata_queries)}")
    click.echo("business-data queries = 0   writes = 0   DDL = 0   EXEC = 0")
    click.echo(f"  record: {store.sql_structural_discovery_path(d.project_id)}")


@cli.command(name="discovery-adf")
@click.option(
    "--project",
    "project_name",
    required=True,
    help="Project/engagement name (Intake must have run).",
)
@click.option("--subscription", "subscription_id", required=True, help="Azure subscription id.")
@click.option("--resource-group", "resource_group", required=True, help="Resource group of the factory.")
@click.option("--factory", "factory_name", required=True, help="Data Factory name.")
@azure_auth_options()
def discovery_adf(
    project_name: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    auth: str,
    **auth_env: str | None,
) -> None:
    """Inventory a live Data Factory's artifacts via ARM (read-only, name + kind only)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = DiscoveryAgent(store)

    access = AdfAccess(
        required=True,
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name,
        credential=credential_from(auth, **auth_env),
    )

    result = agent.discover_adf(
        AdfDiscoveryRequest(project_name=project_name, access=access)
    )

    d = result.discovery
    if d is not None:
        click.echo("AZURE DATA FACTORY")
        click.echo(f"  Subscription:   {d.subscription_id}")
        click.echo(f"  Resource group: {d.resource_group}")
        click.echo(f"  Factory:        {d.factory_name}")
        click.echo(f"  Authentication: {d.authentication}")
        click.echo(f"  Status:         {d.status.value}")

    if not result.success:
        click.secho(f"ADF Discovery failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    click.secho("ADF Discovery complete.", fg="green")
    _echo_sql_objects("PIPELINES", d.pipeline_count, [a.name for a in d.pipelines])
    _echo_sql_objects("DATASETS", d.dataset_count, [a.name for a in d.datasets])
    _echo_sql_objects(
        "LINKED SERVICES", d.linked_service_count, [a.name for a in d.linked_services]
    )
    _echo_sql_objects("TRIGGERS", d.trigger_count, [a.name for a in d.triggers])
    _echo_sql_objects("DATA FLOWS", d.dataflow_count, [a.name for a in d.dataflows])
    _echo_sql_objects(
        "INTEGRATION RUNTIMES",
        d.integration_runtime_count,
        [a.name for a in d.integration_runtimes],
    )

    click.echo(f"ARM collection GETs performed: {len(d.collections_queried)}")
    click.echo("writes = 0   non-GET requests = 0")
    click.echo(f"  record: {store.adf_discovery_path(d.project_id)}")


def _kv_pairs(items: tuple, what: str) -> dict:
    out: dict = {}
    for item in items:
        key, sep, val = item.partition("=")
        if not sep or not key.strip() or not val.strip():
            raise click.UsageError(f"{what} must look like NAME=VALUE, got '{item}'.")
        out[key.strip()] = val.strip()
    return out


def _workload_options(generate, compute_sources, fetch, databricks_token_env, allow_az_tokens, sources, artifacts,
                      secret_maps, lakehouse_id, pipelines):
    from adf2fabric.agent.dependencies.access import TokenSource
    from adf2fabric.agent.workloads.engine import Options

    env = {"databricks": databricks_token_env} if databricks_token_env else {}
    return Options(compute_sources=compute_sources, tokens=TokenSource(env_names=env, allow_az=allow_az_tokens),
                   fetch=bool(fetch or databricks_token_env or allow_az_tokens), local_files=_kv_pairs(sources, "--source"),
                   artifacts=_kv_pairs(artifacts, "--artifact"), secret_map=_kv_pairs(secret_maps, "--secret-map"),
                   lakehouse_id=lakehouse_id, pipelines={p.lower() for p in pipelines} or None, generate=generate)


def _workload_options_decorator(fn):
    for opt in reversed([
        click.option("--project", "project_name", required=True, help="Project name (`discovery-adf-semantic` must have run)."),
        click.option("--compute-sources", "compute_sources", type=click.Path(exists=True, dir_okay=False), default=None,
                     help="Compute sources file (cluster / Synapse pool details the ADF source does not hold)."),
        click.option("--databricks-token-env", default=None, help="NAME of the env var holding a Databricks token. Offering one lets code be retrieved."),
        click.option("--allow-az-tokens", is_flag=True, help="Let retrieval ask `az` for a token for the Databricks audience."),
        click.option("--fetch", is_flag=True, help="Retrieve code from the source platform (needs a credential)."),
        click.option("--source", "sources", multiple=True, help="SOURCE_PATH=LOCAL_FILE: code you already hold. Repeatable."),
        click.option("--artifact", "artifacts", multiple=True, help="SOURCE_FILE_OR_MAIN_CLASS=abfss://...: where you uploaded a file. Repeatable."),
        click.option("--secret-map", "secret_maps", multiple=True, help="DATABRICKS_SECRET_SCOPE=https://<vault>.vault.azure.net/. Repeatable."),
        click.option("--lakehouse-id", default=None, help="Default lakehouse (GUID) for generated Spark job definitions."),
        click.option("--pipeline", "pipelines", multiple=True, help="Only workloads used by this pipeline. Repeatable."),
        click.option("--onelake-workspace", "onelake_workspace", default=None,
                     help="Route ADLS paths in code through the storage map's OneLake shortcuts: the workspace (GUID, or a name without spaces)."),
        click.option("--onelake-lakehouse", "onelake_lakehouse", default=None,
                     help="The lakehouse that holds the shortcuts (GUID with a workspace GUID, or a name with a workspace name)."),
    ]):
        fn = opt(fn)
    return fn


def _attach_storage_routes(store, project_name: str, opts, workspace: Optional[str], lakehouse: Optional[str]) -> None:
    """Opt-in: rewrite literal ADLS paths in generated code to the OneLake shortcuts `storage-map` planned."""
    if not workspace and not lakehouse:
        return
    if not (workspace and lakehouse):
        raise click.UsageError("Give both --onelake-workspace and --onelake-lakehouse to route storage paths.")
    from adf2fabric.agent.storage.routes import RouteError, Routes, Target

    path = store.project_dir(project_name) / "storage" / "storage-map.json"
    if not path.exists():
        raise click.UsageError(f"No storage map yet. Run `adf2fabric storage-map --project {project_name}` first.")
    try:
        opts.storage_routes = Routes.from_storage_map(json.loads(path.read_text(encoding="utf-8")), Target(workspace, lakehouse))
    except RouteError as exc:
        raise click.UsageError(str(exc)) from None
    if not opts.storage_routes:
        click.secho("The storage map plans no shortcut, so no path is routed.", fg="yellow")
        opts.storage_routes = None


def _print_manifest(store, m) -> None:
    s = m.summary
    click.echo(f"  workloads: {s.total}   {s.by_classification}")
    click.echo(f"  generated: {s.generated}   with open issues: {s.generated_with_open_issues}   not generated: {s.not_generated}   "
               f"approved: {s.approved}   verified executing in Fabric: {s.executed_in_fabric}")
    for w in m.workloads:
        click.echo(f"  - {w.source_item}: {w.classification.value}; code {w.code.status.value}; {w.generation.status}; approval {w.approval.status}")
    click.echo(f"  manifest: {store.workload_manifest_path(m.project_id)}")
    click.echo(f"  report:   {store.workload_report_path(m.project_id)}")


@cli.command(name="workloads-assess")
@_workload_options_decorator
def workloads_assess(project_name, compute_sources, databricks_token_env, allow_az_tokens, fetch, sources, artifacts, secret_maps,
                     lakehouse_id, pipelines, onelake_workspace, onelake_lakehouse) -> None:
    """Assess Spark workloads for Fabric (read-only; writes only the manifest and any code you authorised retrieving)."""
    from adf2fabric.agent.workloads.engine import build_manifest
    from adf2fabric.agent.workloads.report import render_report

    store = ProjectStore(projects_root(load_config()))
    opts = _workload_options(False, compute_sources, fetch, databricks_token_env, allow_az_tokens, sources, artifacts, secret_maps,
                             lakehouse_id, pipelines)
    _attach_storage_routes(store, project_name, opts, onelake_workspace, onelake_lakehouse)
    m = build_manifest(store, project_name, opts)
    store.save_workload_manifest(project_name, m, render_report(m))
    click.echo("WORKLOAD ASSESSMENT (nothing was generated or sent to Fabric)")
    _print_manifest(store, m)


@cli.command(name="workloads-generate")
@_workload_options_decorator
def workloads_generate(project_name, compute_sources, databricks_token_env, allow_az_tokens, fetch, sources, artifacts, secret_maps,
                       lakehouse_id, pipelines, onelake_workspace, onelake_lakehouse) -> None:
    """Generate Fabric artifacts (notebooks, Spark job definitions) locally. Explicit opt-in; nothing is sent to Fabric."""
    from adf2fabric.agent.workloads.engine import build_manifest
    from adf2fabric.agent.workloads.report import render_report

    store = ProjectStore(projects_root(load_config()))
    opts = _workload_options(True, compute_sources, fetch, databricks_token_env, allow_az_tokens, sources, artifacts, secret_maps,
                             lakehouse_id, pipelines)
    _attach_storage_routes(store, project_name, opts, onelake_workspace, onelake_lakehouse)
    m = build_manifest(store, project_name, opts)
    store.save_workload_manifest(project_name, m, render_report(m))
    click.echo("WORKLOAD GENERATION (files written under workloads/artifacts; static checks only, NOT run in Fabric)")
    _print_manifest(store, m)


@cli.command(name="workloads-approve")
@click.option("--project", "project_name", required=True)
@click.option("--workload", "workload", required=True, help="Workload id, source item or name (see the manifest).")
@click.option("--by", "by", required=True, help="Who is approving.")
@click.option("--reject", is_flag=True, help="Record a rejection instead.")
@click.option("--note", default="", help="Why (optional).")
@_audited("workloads.approve")
def workloads_approve(project_name: str, workload: str, by: str, reject: bool, note: str) -> None:
    """Approve (or reject) a workload's CURRENT generated artifact for deployment to Fabric."""
    from adf2fabric.agent.workloads.lifecycle import LifecycleError, approve

    store = ProjectStore(projects_root(load_config()))
    try:
        w = approve(store, project_name, workload, by=by, approve=not reject, note=note)
    except (LifecycleError, ProjectNotFoundError) as exc:
        click.secho(str(exc), fg="red", err=True)
        sys.exit(1)
    click.echo(f"{w.id}: {w.approval.status} by {w.approval.by}")
    if w.unresolved and not reject:
        click.secho(f"  note: {len(w.unresolved)} open issue(s) remain; approval accepts them. See the report.", fg="yellow")


@cli.command(name="workloads-deploy")
@click.option("--project", "project_name", required=True)
@click.option("--workspace-id", "workspace_id", required=True, help="Target Fabric workspace id (GUID).")
@click.option("--workload", "workloads", multiple=True, help="Only these workloads (default: every approved one).")
@click.option("--resolutions", "resolutions_path", type=click.Path(dir_okay=False), default=None,
              help="With --apply: record notebook ids here so `convert --resolutions` binds the pipeline to them.")
@azure_auth_options()
@click.option("--apply", "apply_", is_flag=True, help="Actually create or update the Fabric items. Without it this only checks.")
@_audited("workloads.deploy")
def workloads_deploy(project_name, workspace_id, workloads, resolutions_path, apply_, auth, **auth_env) -> None:
    """Deploy APPROVED generated artifacts to Fabric (dry run unless --apply). Never deletes anything."""
    from pathlib import Path

    from adf2fabric.agent.deployment.fabric_deploy import FABRIC_SCOPE, FabricClient
    from adf2fabric.agent.workloads.lifecycle import LifecycleError, deploy
    from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential

    store = ProjectStore(projects_root(load_config()))
    try:
        credential, _ = build_credential(credential_from(auth, **auth_env))
        client = FabricClient(credential.get_token(FABRIC_SCOPE).token)
        results = deploy(store, project_name, client, workspace_id, list(workloads), apply=apply_,
                         resolutions=Path(resolutions_path) if resolutions_path else None)
    except (LifecycleError, ProjectNotFoundError, CredentialUnavailable) as exc:
        click.secho(f"Workload deploy failed: {exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - message carries no token
        click.secho(f"Workload deploy failed: could not get a Fabric token or reach Fabric ({type(exc).__name__}).", fg="red", err=True)
        sys.exit(1)
    click.echo(("DEPLOY WORKLOADS" if apply_ else "DEPLOY WORKLOADS (dry run - nothing changed; add --apply)") + f"  workspace: {workspace_id}")
    for w, msg in results:
        click.echo(f"  - {w.source_item}: {msg}")
    click.echo("  Deployed is not verified: run the pipeline, then `workloads-record-run`.")


@cli.command(name="workloads-record-run")
@click.option("--project", "project_name", required=True)
@click.option("--workspace-id", "workspace_id", required=True)
@click.option("--pipeline", "pipeline", required=True, help="The Fabric pipeline whose latest finished run exercised the workload.")
@click.option("--workload", "workload", required=True)
@azure_auth_options()
def workloads_record_run(project_name, workspace_id, pipeline, workload, auth, **auth_env) -> None:
    """Record execution evidence from the latest finished Fabric run of a pipeline (read-only against Fabric)."""
    from adf2fabric.agent.deployment.fabric_deploy import FABRIC_SCOPE, FabricClient
    from adf2fabric.agent.deployment.fabric_run import latest_run
    from adf2fabric.agent.workloads.lifecycle import LifecycleError, record_run
    from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential

    store = ProjectStore(projects_root(load_config()))
    try:
        credential, _ = build_credential(credential_from(auth, **auth_env))
        client = FabricClient(credential.get_token(FABRIC_SCOPE).token)
        run = latest_run(client, workspace_id, pipeline)
        if run is None:
            raise LifecycleError(f"Pipeline '{pipeline}' has no finished run in the workspace.")
        w = record_run(store, project_name, workload, run)
    except (LifecycleError, ProjectNotFoundError, CredentialUnavailable) as exc:
        click.secho(f"Record failed: {exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        click.secho(f"Record failed: could not reach Fabric ({type(exc).__name__}).", fg="red", err=True)
        sys.exit(1)
    click.echo(f"{w.id}: execution {w.execution.status} (run {w.execution.run_id}) {w.execution.detail}")


def _provision_gateways(auth: str, auth_env: dict, need_arm: bool):
    """Fabric and (when an Azure capacity is involved) ARM gateways. Each gets a token provider for its own audience."""
    from adf2fabric import cloud
    from adf2fabric.agent.provisioning.transport import ArmGateway, FabricGateway
    from adf2fabric.connectivity._azure import build_credential

    credential, _ = build_credential(credential_from(auth, **auth_env))
    fab = FabricGateway(lambda: credential.get_token(cloud.FABRIC_SCOPE).token)
    arm = ArmGateway(lambda: credential.get_token(cloud.ARM_SCOPE).token) if need_arm else None
    return fab, arm


@cli.command(name="provision-plan")
@click.option("--project", "project_name", required=True)
@click.option("--tenant-id", default=None, help="The tenant the migration must land in; checked against the token actually used.")
@click.option("--workspace-name", default=None, help="Use this workspace, or plan to create it.")
@click.option("--workspace-id", default=None, help="An existing workspace id (GUID).")
@click.option("--capacity-id", default=None, help="An existing Fabric capacity id (from `GET /capacities`).")
@click.option("--capacity-name", default=None, help="An existing Fabric capacity, by display name.")
@click.option("--create-capacity", is_flag=True, help="Plan a NEW Azure Fabric capacity (billable while running). Needs the --azure-* options.")
@click.option("--azure-subscription", default=None)
@click.option("--azure-resource-group", default=None)
@click.option("--azure-capacity-name", default=None)
@click.option("--azure-region", default=None, help="Azure location, for example westeurope.")
@click.option("--azure-sku", default=None, help="F2 ... F2048.")
@click.option("--capacity-admin", "capacity_admins", multiple=True, help="UPN of a capacity administrator. Repeatable.")
@click.option("--provision-identity", is_flag=True, help="Plan a workspace identity.")
@click.option("--role", "roles", multiple=True, help="PRINCIPAL_ID:TYPE:ROLE, for example 0000...:User:Contributor. Repeatable.")
@click.option("--lakehouse", "lakehouses", multiple=True, help="A lakehouse to create or reuse. Repeatable.")
@click.option("--lakehouse-schemas", is_flag=True, help="Create lakehouses with schemas enabled (preview).")
@click.option("--warehouse", "warehouses", multiple=True, help="A warehouse to create or reuse. Repeatable.")
@azure_auth_options()
def provision_plan(project_name, tenant_id, workspace_name, workspace_id, capacity_id, capacity_name, create_capacity, azure_subscription,
                   azure_resource_group, azure_capacity_name, azure_region, azure_sku, capacity_admins, provision_identity, roles, lakehouses,
                   lakehouse_schemas, warehouses, auth, **auth_env) -> None:
    """Discover what exists and plan what the migration needs in Fabric. Read-only: nothing is created."""
    from adf2fabric.agent.provisioning import service
    from adf2fabric.project.provisioning import TargetSpec

    store = ProjectStore(projects_root(load_config()))
    role_list = []
    for r in roles:
        parts = r.split(":")
        if len(parts) != 3:
            raise click.UsageError(f"--role must look like PRINCIPAL_ID:TYPE:ROLE, got '{r}'.")
        role_list.append({"principal_id": parts[0], "principal_type": parts[1], "role": parts[2]})
    given = any([tenant_id, workspace_name, workspace_id, capacity_id, capacity_name, create_capacity, azure_subscription, provision_identity,
                 roles, lakehouses, warehouses])
    spec = service.load_spec(store, project_name) if not given else None
    if spec is None:
        spec = TargetSpec(tenant_id=tenant_id, workspace_name=workspace_name, workspace_id=workspace_id, capacity_id=capacity_id, capacity_name=capacity_name,
                          create_capacity=create_capacity, azure_subscription_id=azure_subscription, azure_resource_group=azure_resource_group,
                          azure_capacity_name=azure_capacity_name, azure_region=azure_region, azure_sku=azure_sku, capacity_admins=list(capacity_admins),
                          provision_workspace_identity=provision_identity, role_assignments=role_list, lakehouse_names=list(lakehouses),
                          warehouse_names=list(warehouses), lakehouse_schemas_enabled=lakehouse_schemas)
    try:
        fab, arm = _provision_gateways(auth, auth_env, need_arm=bool(spec.create_capacity or spec.azure_subscription_id))
        p = service.plan(store, project_name, spec, fab, arm)
    except Exception as exc:  # noqa: BLE001 - messages carry no token
        click.secho(f"Provisioning plan failed: {type(exc).__name__}: {exc}", fg="red", err=True)
        sys.exit(1)
    s = p.summary
    click.echo("PROVISIONING PLAN (read-only: nothing was created)")
    click.echo(f"  steps: {s.total}   create: {s.to_create}   configure: {s.to_configure}   reuse: {s.reused}   delegated: {s.delegated}   "
               f"blocked: {s.blocked}   conflicts: {s.conflicts}   unknown: {s.unknown}")
    for st in p.steps:
        click.echo(f"  - {st.action.value:9} {st.type.value} '{st.name}'" + (f"  [{'; '.join(st.blockers)[:110]}]" if st.blockers else ""))
    for c in p.checks:
        click.echo(f"  check {c.name}: {c.status.value}" + (f" ({c.detail[:100]})" if c.detail else ""))
    for b in p.blockers:
        click.secho(f"  BLOCKER {b}", fg="red")
    click.echo(f"  plan: {store.provisioning_dir(project_name) / 'plan.md'}   hash {p.plan_hash[:16]}")


@cli.command(name="provision-approve")
@click.option("--project", "project_name", required=True)
@click.option("--step", "steps", multiple=True, help="A step id from the plan (for example workspace:my-ws). Repeatable.")
@click.option("--all-approvable", is_flag=True, help="Approve every create/configure step that is not blocked.")
@click.option("--by", required=True, help="Who is approving.")
@click.option("--note", default="")
@_audited("provision.approve")
def provision_approve(project_name: str, steps: tuple, all_approvable: bool, by: str, note: str) -> None:
    """Approve named steps of the CURRENT plan. Nothing is created by approving."""
    from adf2fabric.agent.provisioning import service
    from adf2fabric.agent.provisioning.executor import ApplyRefused

    store = ProjectStore(projects_root(load_config()))
    if not steps and not all_approvable:
        raise click.UsageError("Name the steps to approve with --step, or pass --all-approvable.")
    try:
        a = service.approve(store, project_name, None if all_approvable else list(steps), by, note)
    except (ApplyRefused, ProjectNotFoundError) as exc:
        click.secho(str(exc), fg="red", err=True)
        sys.exit(1)
    click.echo(f"Approved {len(a.step_ids)} step(s) of plan {a.plan_hash[:16]} by {a.approved_by}:")
    for i in a.step_ids:
        click.echo(f"  - {i}")
    click.echo("Nothing was created. Run provision-apply (dry run) and then provision-apply --apply.")


@cli.command(name="provision-apply")
@click.option("--project", "project_name", required=True)
@click.option("--apply", "apply_", is_flag=True, help="Actually create / configure the approved steps. Without it nothing is written.")
@azure_auth_options()
@_audited("provision.apply")
def provision_apply(project_name: str, apply_: bool, auth: str, **auth_env) -> None:
    """Apply the approved steps (dry run unless --apply), then read the target back to verify. Never deletes."""
    from adf2fabric.agent.provisioning import service
    from adf2fabric.agent.provisioning.executor import ApplyRefused
    from adf2fabric.project.provisioning import ProvisioningPlan

    store = ProjectStore(projects_root(load_config()))
    try:
        p = store.load_provisioning(project_name, "plan", ProvisioningPlan)
        fab, arm = _provision_gateways(auth, auth_env, need_arm=bool(p.target.create_capacity or p.target.azure_subscription_id))
        run, report = service.apply(store, project_name, fab, arm, do_apply=apply_)
    except (ApplyRefused, ProjectNotFoundError) as exc:
        click.secho(str(exc), fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        click.secho(f"Provisioning failed: {type(exc).__name__}: {exc}", fg="red", err=True)
        sys.exit(1)
    click.echo(("PROVISIONING APPLIED" if apply_ else "PROVISIONING (dry run - nothing was changed; add --apply)") + f"  plan {run.plan_hash[:16]}")
    for r in run.results:
        click.echo(f"  - {r.step_id}: {r.status.value}" + (f" ({r.attempts} attempt(s))" if r.attempts else "") + f" {r.detail[:140]}")
    if report is not None:
        click.echo(f"  verification: {'ALL VERIFIED' if report.all_verified else 'NOT all verified: see provisioning/verification.md'}")
    failed = [r for r in run.results if r.status.value in ("failed", "mismatch")]
    if failed:
        sys.exit(2)


@cli.command(name="provision-verify")
@click.option("--project", "project_name", required=True)
@azure_auth_options()
def provision_verify(project_name: str, auth: str, **auth_env) -> None:
    """Read the target back and compare it with the plan (read-only). The only command that may say a resource exists."""
    from adf2fabric.agent.provisioning import service
    from adf2fabric.project.provisioning import ProvisioningPlan

    store = ProjectStore(projects_root(load_config()))
    try:
        p = store.load_provisioning(project_name, "plan", ProvisioningPlan)
        fab, arm = _provision_gateways(auth, auth_env, need_arm=bool(p.target.create_capacity or p.target.azure_subscription_id))
        report = service.verify(store, project_name, fab, arm)
    except ProjectNotFoundError as exc:
        click.secho(str(exc), fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        click.secho(f"Verification failed: {type(exc).__name__}: {exc}", fg="red", err=True)
        sys.exit(1)
    click.echo(f"VERIFICATION {'ALL VERIFIED' if report.all_verified else 'NOT all verified'}")
    for r in report.results:
        click.echo(f"  - {r.step_id}: {r.status.value} {r.detail[:120]}")
    if not report.all_verified:
        sys.exit(2)


@cli.command(name="validate")
@click.option("--project", "project_name", required=True)
@click.option("--stage", "stages", multiple=True, type=click.Choice(["static", "api", "deployment", "runtime", "all"]),
              help="Stages to run (repeatable). Default: static. `all` = static, api, deployment, runtime.")
@click.option("--workspace-id", default=None, help="The Fabric workspace to read for the api and deployment stages (read-only).")
@click.option("--runtime", "runtime_flag", is_flag=True,
              help="Judge runtime from the recorded run-and-compare results (`verify`). Never starts a run itself.")
@azure_auth_options()
def validate_cmd(project_name: str, stages: tuple, workspace_id, runtime_flag: bool, auth: str, **auth_env) -> None:
    """Independently validate the generated artifacts against the source. Read-only; never claims success the evidence does not support."""
    from adf2fabric.agent.validation import engine
    from adf2fabric.agent.validation.report import render

    chosen = set(stages) or {"static"}
    if "all" in chosen:
        chosen = set(engine.ALL_STAGES)
    if runtime_flag:
        chosen.add("runtime")
    store = ProjectStore(projects_root(load_config()))
    fab = None
    if chosen & {"api", "deployment"} and workspace_id:
        try:
            from adf2fabric import cloud
            from adf2fabric.agent.provisioning.transport import FabricGateway
            from adf2fabric.connectivity._azure import build_credential

            credential, _ = build_credential(credential_from(auth, **auth_env))
            fab = FabricGateway(lambda: credential.get_token(cloud.FABRIC_SCOPE).token)
        except Exception as exc:  # noqa: BLE001 - no token in the message
            click.secho(f"Could not sign in to Fabric ({type(exc).__name__}); the api / deployment stages will be NOT_RUN.", fg="yellow", err=True)
    try:
        report = engine.validate(store, project_name, stages=chosen, fab=fab, workspace=workspace_id)
    except ProjectNotFoundError as exc:
        click.secho(str(exc), fg="red", err=True)
        sys.exit(1)
    d = store.project_dir(project_name) / "validation"
    d.mkdir(parents=True, exist_ok=True)
    store.save_validation_report(project_name, report)
    (d / "report.md").write_text(render(report), encoding="utf-8")
    click.echo(f"MIGRATION VALIDATION: {report.overall}")
    for r in report.overall_reasons:
        click.echo(f"  {r}")
    for s in report.stages:
        click.echo(f"  {s.stage.value:10} {s.status.value:8} pass {s.passed}  fail {s.failed}  blocked {s.blocked}  not run {s.not_run}")
    click.echo("  components: " + ", ".join(f"{v} {k}" for k, v in report.inventory.by_status.items()))
    for x in [x for x in report.checks if x.result.value == "FAIL" and x.required][:8]:
        click.secho(f"  FAIL {x.id} {x.component or ''}: {x.summary}", fg="red")
    click.echo(f"  report: {d / 'report.md'}")
    if report.overall in ("FAILED",):
        sys.exit(2)


@cli.command(name="auth-user-create")
@click.option("--id", "user_id", required=True, help="The user's id (letters, digits, _ . @ -).")
@click.option("--org", required=True, help="The organisation that user belongs to. Projects and jobs are scoped to it.")
@click.option("--role", "roles", multiple=True, required=True, help="viewer | analyst | approver | operator | admin (repeatable).")
@click.option("--subscription", "subscriptions", multiple=True, help="An Azure subscription id the user may target, or *. Repeatable.")
@click.option("--tenant", "tenants", multiple=True, help="A tenant id the user may target, or *. Repeatable.")
@click.option("--account", "accounts", multiple=True, help="A signed-in Azure CLI account the user may act as, or *. Repeatable.")
@click.option("--workspace", "workspaces", multiple=True, help="A Fabric workspace id the user may target, or *. Repeatable.")
def auth_user_create(user_id, org, roles, subscriptions, tenants, accounts, workspaces) -> None:
    """Create an API user and print its token ONCE. Run on the server as the operating-system user that owns the data."""
    from adf2fabric.api.security import Grants, UserError

    try:
        rec, token = _user_store().create(user_id, org, list(roles), Grants(subscriptions=list(subscriptions), tenants=list(tenants), accounts=list(accounts),
                                                                         workspaces=list(workspaces)), created_by=_os_user())
    except UserError as exc:
        click.secho(str(exc), fg="red", err=True)
        sys.exit(1)
    _audit_log().event("cli.user.create", "ok", actor=_os_user(), org="cli", resource="user", resource_id=rec.id, roles=",".join(rec.roles), user_org=org)
    click.echo(f"Created {rec.id} (org {rec.org}, roles {', '.join(rec.roles)}).")
    click.echo("Token (shown once; store it in a secret manager, send it as 'Authorization: Bearer <token>'):")
    click.echo(token)


@cli.command(name="auth-user-list")
def auth_user_list() -> None:
    """List API users (never their tokens)."""
    for u in _user_store().list():
        click.echo(f"{u.id}  org={u.org}  roles={','.join(u.roles)}  {'DISABLED' if u.disabled else 'active'}  token ends ...{u.token_hint}")


@cli.command(name="auth-user-update")
@click.option("--id", "user_id", required=True)
@click.option("--role", "roles", multiple=True, help="Replace the roles.")
@click.option("--disable/--enable", "disable", default=None)
def auth_user_update(user_id, roles, disable) -> None:
    """Change a user's roles or disable / enable it."""
    from adf2fabric.api.security import UserError

    try:
        rec = _user_store().update(user_id, roles=list(roles) or None, disabled=disable)
    except UserError as exc:
        click.secho(str(exc), fg="red", err=True)
        sys.exit(1)
    _audit_log().event("cli.user.update", "ok", actor=_os_user(), org="cli", resource="user", resource_id=rec.id, roles=",".join(rec.roles), disabled=rec.disabled)
    click.echo(f"{rec.id}: roles {', '.join(rec.roles)}; {'disabled' if rec.disabled else 'active'}.")


@cli.command(name="auth-user-rotate")
@click.option("--id", "user_id", required=True)
def auth_user_rotate(user_id) -> None:
    """Issue a new token for a user (the old one stops working) and print it ONCE."""
    from adf2fabric.api.security import UserError

    try:
        rec, token = _user_store().rotate(user_id)
    except UserError as exc:
        click.secho(str(exc), fg="red", err=True)
        sys.exit(1)
    _audit_log().event("cli.user.rotate_token", "ok", actor=_os_user(), org="cli", resource="user", resource_id=rec.id)
    click.echo(f"New token for {rec.id} (shown once):")
    click.echo(token)


@cli.command(name="audit-verify")
def audit_verify() -> None:
    """Check the audit log's hash chain. Tamper EVIDENCE only: see docs/security.md."""
    broken = _audit_log().verify()
    if broken is None:
        click.echo("Audit log chain intact.")
    else:
        click.secho(f"Audit log chain BROKEN at line {broken}: a line was edited, removed or inserted.", fg="red", err=True)
        sys.exit(2)


@cli.command(name="storage-map")
@click.option("--project", "project_name", required=True, help="Project name (`discovery-adf-semantic` must have run).")
@click.option("--lakehouse", "lakehouse", default=None, help="Target lakehouse NAME, shown in the plan. It is never invented: leave it out if undecided.")
@click.option("--workspace-id", "workspace_id", default=None, help="Fabric workspace id, only to fill the printed create-shortcut commands.")
def storage_map_cmd(project_name: str, lakehouse: Optional[str], workspace_id: Optional[str]) -> None:
    """Map ADF storage datasets to OneLake shortcuts and Lakehouse paths. Read-only: creates nothing, moves no data."""
    from adf2fabric.agent.storage.mapping import build_storage_map, render_markdown

    store = ProjectStore(projects_root(load_config()))
    if not store.has_adf_semantic_discovery(project_name):
        click.secho(f"No ADF semantic discovery for '{project_name}'. Run `adf2fabric discovery-adf-semantic --project {project_name}` first.", fg="red", err=True)
        sys.exit(1)
    m = build_storage_map(store.load_adf_semantic_discovery(project_name), lakehouse)
    out = store.project_dir(project_name) / "storage"
    out.mkdir(parents=True, exist_ok=True)
    (out / "storage-map.json").write_text(json.dumps(m.to_dict(), indent=2, default=str), encoding="utf-8")
    (out / "storage-map.md").write_text(render_markdown(m, workspace_id or "<workspace-id>"), encoding="utf-8")
    s = m.summary
    click.echo("STORAGE MAP (read-only)")
    click.echo(f"  file datasets: {s['file_datasets']}   mapped: {s['mapped']}   mapped with expression: {s['mapped_dynamic']}   need a decision: {s['needs_decision']}")
    click.echo(f"  shortcuts to create: {s['shortcuts']}   not-file datasets left out: {s['not_file_datasets']}")
    for sc in m.shortcuts:
        click.echo(f"  {sc.lakehouse_path:<40} <- {sc.account_url}{sc.subpath}  ({sc.datasets} dataset(s))")
    for d in m.to_decide[:15]:
        click.secho(f"  DECIDE {d['dataset']}: {d['reason']}", fg="yellow")
    click.echo(f"  report: {out / 'storage-map.md'}")


@cli.command(name="discovery-dependencies")
@click.option("--project", "project_name", required=True, help="Project name (`discovery-adf-semantic` must have run).")
@click.option("--check", "checks", multiple=True, type=click.Choice(["databricks", "key_vault", "storage", "network"]),
              help="Probe this provider's external targets (repeatable). Default: contact nothing.")
@click.option("--databricks-token-env", default=None, help="NAME of the env var holding a Databricks token.")
@click.option("--keyvault-token-env", default=None, help="NAME of the env var holding a Key Vault token.")
@click.option("--storage-token-env", default=None, help="NAME of the env var holding a Storage token.")
@click.option("--allow-az-tokens", is_flag=True, help="Let a check ask `az` for a token for THAT provider's audience.")
def discovery_dependencies(project_name: str, checks: tuple, databricks_token_env: Optional[str],
                           keyvault_token_env: Optional[str], storage_token_env: Optional[str], allow_az_tokens: bool) -> None:
    """Dependency inventory: what the estate depends on, whether it resolves, and the migration order."""
    from adf2fabric.agent.dependencies import DependencyDiscoveryError, discover_dependencies
    from adf2fabric.agent.dependencies.access import TokenSource

    store = ProjectStore(projects_root(load_config()))
    env = {k: v for k, v in (("databricks", databricks_token_env), ("key_vault", keyvault_token_env),
                             ("storage", storage_token_env)) if v}
    try:
        inv = discover_dependencies(store, project_name, checks, TokenSource(env_names=env, allow_az=allow_az_tokens))
    except DependencyDiscoveryError as exc:
        click.secho(str(exc), fg="red", err=True)
        sys.exit(1)
    s = inv.summary
    click.echo("DEPENDENCY INVENTORY")
    click.echo(f"  project: {inv.project_id}   source: {inv.source.source_type}   version: {inv.source.version[:12]}")
    click.echo(f"  dependencies: {s.total}   blocking: {s.blocking}   errors: {s.findings_error}   warnings: {s.findings_warning}")
    click.echo(f"  resolution: {s.by_resolution}")
    click.echo(f"  migration:  {s.by_category}")
    click.echo(f"  access:     {s.by_access or 'not checked (nothing external was contacted)'}")
    click.echo(f"  cycles: {len(inv.cycles)}   source gaps: {len(inv.source_gaps)}   waves: {len(inv.waves)}")
    for f in [f for f in inv.findings if f.severity.value == "error"][:10]:
        click.secho(f"  ERROR {f.code}: {f.message}", fg="red")
    click.echo(f"  record: {store.dependency_inventory_path(inv.project_id)}")
    click.echo(f"  report: {store.dependency_report_path(inv.project_id)}")


@cli.command(name="discovery-adf-semantic")
@click.option(
    "--project",
    "project_name",
    required=True,
    help="Project/engagement name (Intake + Repository Discovery must have run).",
)
def discovery_adf_semantic(project_name: str) -> None:
    """Extract ADF artifact definitions + references from the repository source (read-only)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = DiscoveryAgent(store)

    result = agent.discover_adf_semantic(
        AdfSemanticDiscoveryRequest(project_name=project_name)
    )

    d = result.discovery
    if d is not None:
        click.echo("ADF SEMANTIC DISCOVERY (repository-based)")
        click.echo(f"  project:      {d.project_id}")
        click.echo(f"  commit:       {d.source_commit_sha}")
        click.echo(f"  status:       {d.status.value}")

    if not result.success:
        click.secho(f"ADF Semantic Discovery failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    click.secho("ADF Semantic Discovery complete.", fg="green")
    click.echo(
        f"  pipelines: {d.pipeline_count}   datasets: {d.dataset_count}   "
        f"linked services: {d.linked_service_count}   triggers: {d.trigger_count}   "
        f"factory: {d.factory_count}"
    )
    click.echo(
        f"  external references: {len(d.external_references)}   "
        f"relationships: {len(d.relationships)}   "
        f"unresolved: {len(d.unresolved_references)}"
    )
    click.echo(
        f"  parsed: {d.parsed_count}   skipped: {d.skipped_count}   failed: {d.failed_count}"
    )
    click.echo(
        f"  activities: {d.activity_count}   data flows: {d.dataflow_count}   "
        f"dependency cycles: {sum(len(p.dependency_graph.cycles) for p in d.pipelines if p.dependency_graph)}"
    )
    if d.unknown_activity_types:
        click.secho(f"  unknown activity type(s), preserved but not interpreted: {', '.join(d.unknown_activity_types)}",
                    fg="yellow")
    if d.inventory_mismatch_count:
        bad = [p.name for p in d.pipelines if p.inventory is not None and not p.inventory.matched]
        click.secho(f"  ACTIVITY INVENTORY MISMATCH in {d.inventory_mismatch_count} pipeline(s): {', '.join(bad)} "
                    "(the source holds activities the record does not)", fg="red")
    for u in d.unprocessed_artifacts:
        click.secho(f"  not in the record: {u.path} [{u.kind}] {u.reason}" + (f" - {u.detail}" if u.detail else ""),
                    fg="yellow" if u.reason != "kind_not_parsed" else None)
    if d.failed_count or d.detail:
        click.secho(f"  note: {d.detail}", fg="yellow")
    click.echo(f"  record: {store.adf_semantic_discovery_path(d.project_id)}")


@cli.command(name="convert")
@click.option(
    "--project", "project_name", required=True,
    help="Project/engagement name (Intake + ADF Semantic Discovery must have run).",
)
@click.option("--pipeline", "pipelines", multiple=True, help="Convert only this pipeline. Repeatable.")
@click.option(
    "--resolutions", "resolutions_path", type=click.Path(exists=True, dir_okay=False), default=None,
    help="JSON file binding linked services / items to real Fabric ids (see conversion/resolutions.template.json).",
)
@click.option(
    "--apply-reconstruction", "apply_reconstruction", is_flag=True,
    help="Convert the completed pipelines (original + SAFE inferred proposals from `reconstruct`); "
         "every inferred element is marked. Off by default.",
)
@click.option(
    "--compute-sources", "compute_sources_path", type=click.Path(exists=True, dir_okay=False), default=None,
    help="JSON file describing Spark compute ADF only references (Databricks clusters by id, Synapse Spark pools by "
         "name) so the Spark compute plan can size it. See docs/compute-plan.md.",
)
def convert(project_name: str, pipelines: tuple[str, ...], resolutions_path: str | None,
            apply_reconstruction: bool, compute_sources_path: str | None) -> None:
    """Convert the discovered ADF pipelines to Fabric pipeline definitions (deterministic, no LLM)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    result = ConversionAgent(store).run(
        ConversionRequest(project_name=project_name, pipelines=list(pipelines), resolutions_path=resolutions_path,
                          apply_reconstruction=apply_reconstruction, compute_sources_path=compute_sources_path)
    )
    if not result.success:
        click.secho(f"Conversion failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    c = result.conversion
    click.echo("ADF -> FABRIC CONVERSION")
    click.echo(f"  project:    {c.project_id}")
    click.echo(f"  commit:     {c.source_commit_sha}")
    click.echo(f"  outcome:    {c.status.value}")
    click.echo(f"  pipelines:  {c.pipeline_count}   activities: {c.activity_count}")
    colours = {"converted": "green", "converted_with_differences": "yellow", "manual_required": "yellow", "blocked": "red"}
    for status, n in c.status_counts.items():
        click.secho(f"    {status}: {n}", fg=colours[status] if n else None)
    for p in c.pipelines:
        valid = "valid" if p.structural_valid else "INVALID"
        click.echo(f"  - {p.source_pipeline}: {p.status.value} ({valid}; runtime ready: {'yes' if p.runtime_ready else 'no'})")
        for a in p.activities:
            if a.status.value in ("manual_required", "blocked"):
                click.secho(f"      {a.status.value.upper()}: {a.source_name} ({a.source_type}) - {a.summary}", fg=colours[a.status.value])
    for problem in result.problems:
        click.secho(f"  resolution file: {problem}", fg="yellow")
    if c.reconstruction_applied:
        click.secho(f"  inferred elements applied: {', '.join(c.reconstruction_applied)} (marked in the output; review them)",
                    fg="yellow")
    if c.reconstruction_open:
        click.secho(f"  {len(c.reconstruction_open)} reconstruction blocker(s) remain unresolved.", fg="yellow")
    if c.compatibility:
        by_status: dict[str, int] = {}
        for f in c.compatibility:
            by_status[f.status] = by_status.get(f.status, 0) + 1
        click.secho("  compatibility: " + ", ".join(f"{n} {k}" for k, n in sorted(by_status.items()))
                    + " component(s) retired/deprecated/preview/unavailable; see the report.", fg="yellow")
    plan = c.compute_plan
    if plan is not None:
        proposed = [p for p in plan.pools if p.status == "proposed"]
        click.echo(f"  spark compute: {len(plan.inventory)} in use -> {len(proposed)} Fabric pool(s) proposed, "
                   f"largest {plan.max_single_pool_vcores} Spark vCores at peak")
        for p in plan.pools:
            click.secho(f"    {p.name}: {p.status}" + (f" ({p.min_nodes}-{p.max_nodes} x {p.node_size})" if p.node_size else ""),
                        fg="green" if p.status == "proposed" else "yellow")
        if plan.gaps:
            click.secho(f"    {len(plan.gaps)} item(s) need a person; see the report.", fg="yellow")
        for prob in plan.sources_problems:
            click.secho(f"    compute sources file: {prob}", fg="yellow")
    for f in c.preflight:
        click.secho(f"  source check ({f.severity}): {f.message}",
                    fg={"error": "red", "warning": "yellow"}.get(f.severity))
        click.echo(f"      fix: {f.fix}")
    unbound = [q for q in [*c.connections, *c.workspace_items] if not q.bound]
    # a pipeline's own Fabric id only exists once it is deployed: deploy fills it in, nobody supplies it
    owed = [q for q in unbound if not q.name.startswith("pipeline:")]
    if owed:
        click.secho(f"  {len(owed)} binding(s) still to supply before anything can run: "
                    + ", ".join(q.name for q in owed) + "; see the report.", fg="yellow")
    if len(unbound) > len(owed):
        click.echo(f"  {len(unbound) - len(owed)} pipeline id(s) are filled in by `deploy` once the pipelines exist.")
    base = store.conversion_dir(c.project_id)
    click.echo(f"  report:     {base / 'conversion-report.md'}")
    click.echo(f"  record:     {store.conversion_path(c.project_id)}")
    click.echo(f"  resolutions template: {base / 'resolutions.template.json'}")


@cli.command(name="reconstruct")
@click.option(
    "--project", "project_name", required=True,
    help="Project/engagement name (Intake, Repository Discovery and ADF Semantic Discovery must have run).",
)
def reconstruct(project_name: str) -> None:
    """Find incomplete pipelines and propose evidence-backed completions (deterministic, no LLM)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    result = ReconstructionAgent(store).run(ReconstructionRequest(project_name=project_name))
    if not result.success:
        click.secho(f"Reconstruction failed: {result.error}", fg="red", err=True)
        sys.exit(1)
    r = result.reconstruction
    click.echo("RECONSTRUCTION (original source is never modified)")
    click.echo(f"  project:   {r.project_id}")
    click.echo(f"  commit:    {r.source_commit_sha}")
    click.echo(f"  outcome:   {r.status.value}")
    for cls, n in r.finding_counts.items():
        click.echo(f"  gaps ({cls}): {n}")
    for conf, n in r.proposal_counts.items():
        click.echo(f"  proposals ({conf}): {n}")
    click.echo(f"  safe to generate: {r.safe_proposal_count}   blockers: {r.blocker_count}")
    colours = {"complete": "green", "reconstructable": "yellow", "partially_reconstructable": "yellow",
               "blocked": "red", "unusable": "red"}
    for p in r.pipelines:
        click.secho(f"  - {p.pipeline}: {p.completeness.value}"
                    f"{'' if p.executable_as_is else ' (not executable as authored)'}", fg=colours[p.completeness.value])
    base = store.reconstruction_dir(r.project_id)
    click.echo(f"  report:    {base / 'reconstruction-report.md'}")
    click.echo(f"  record:    {store.reconstruction_path(r.project_id)}")
    if r.completed_files:
        click.echo(f"  proposed completion: {base / 'completed'}  (apply with `convert --apply-reconstruction`)")


@cli.command(name="deploy")
@click.option("--project", "project_name", required=True, help="Project name (`convert` must have run).")
@click.option("--workspace-id", "workspace_id", required=True, help="Target Fabric workspace id (GUID).")
@click.option("--pipeline", "pipelines", multiple=True, help="Deploy only this converted pipeline. Repeatable.")
@click.option("--with-children", is_flag=True,
              help="Also deploy the pipelines a selected pipeline invokes (transitively), children first.")
@click.option("--allow-unbound", is_flag=True,
              help="Push even with <connection:...> placeholders left (bind them in Fabric afterwards).")
@click.option("--schedules", "schedules", is_flag=True,
              help="Also create the Fabric schedules generated from ADF triggers (created DISABLED; never duplicated).")
@click.option("--schedules-only", "schedules_only", is_flag=True,
              help="Only create schedules, for pipelines that are already in the workspace. No pipeline is created or "
                   "overwritten. Implies --schedules.")
@click.option("--enable-schedules", "enable_schedules", is_flag=True,
              help="With --schedules: create schedules enabled when the ADF trigger was started. A start time in the "
                   "past makes Fabric run the pipeline immediately.")
@azure_auth_options()
@click.option("--apply", "apply_", is_flag=True,
              help="Actually create/overwrite pipelines in Fabric. Without it this is a dry run (read-only).")
@_audited("deploy")
def deploy_cmd(project_name: str, workspace_id: str, pipelines: tuple[str, ...], with_children: bool,
               allow_unbound: bool, schedules: bool, schedules_only: bool, enable_schedules: bool, apply_: bool,
               auth: str, **auth_env: str | None) -> None:
    """Deploy converted pipelines to a Fabric workspace (dry run unless --apply)."""
    import json as _json

    schedules = schedules or schedules_only

    from adf2fabric.agent.deployment.fabric_deploy import (
        DeployError, DeployItem, FabricClient, child_refs, deploy,
    )
    from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential
    from adf2fabric.project.models import AzureCredentialRef

    store = ProjectStore(projects_root(load_config()))
    try:
        conversion = store.load_conversion(project_name)
    except Exception as exc:  # noqa: BLE001
        click.secho(f"Deploy failed: {exc}", fg="red", err=True)
        sys.exit(1)
    base = store.conversion_dir(project_name)
    available: dict[str, DeployItem] = {}
    for p in conversion.pipelines:
        rel = p.artifact_path
        if rel and (base / rel).exists():
            path = base / rel
            available[p.source_pipeline.lower()] = DeployItem(
                p.fabric_pipeline_name, _json.loads(path.read_text(encoding="utf-8")), str(path), p.structural_valid)
    wanted = [n.lower() for n in pipelines] or list(available)
    unknown = [n for n in pipelines if n.lower() not in available]
    if unknown:
        click.secho(f"Deploy failed: no converted pipeline named {', '.join(unknown)}. "
                    f"Available: {', '.join(sorted(available)) or '(none)'}.", fg="red", err=True)
        sys.exit(1)
    if with_children:  # transitively add every pipeline a selected one invokes
        seen: list[str] = []
        queue = [n for n in wanted if n in available]
        while queue:
            n = queue.pop(0)
            if n in seen:
                continue
            seen.append(n)
            queue += [c.lower() for c in child_refs(available[n].content) if c.lower() in available]
        wanted = seen
    items = [available[n] for n in wanted if n in available]
    if not items:
        click.secho("Nothing to deploy (run `convert` first).", fg="yellow")
        sys.exit(1)
    try:
        credential, _ = build_credential(credential_from(auth, **auth_env))
        from adf2fabric.agent.deployment.fabric_deploy import FABRIC_SCOPE
        client = FabricClient(credential.get_token(FABRIC_SCOPE).token)
        results = [] if schedules_only else deploy(client, workspace_id, items, apply=apply_, allow_unbound=allow_unbound)
    except (DeployError, CredentialUnavailable) as exc:
        click.secho(f"Deploy failed: {getattr(exc, 'detail', None) or exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - e.g. az login missing/expired; message carries no token
        click.secho(f"Deploy failed: could not get a Fabric token or reach Fabric ({type(exc).__name__}). "
                    "Check `az login` with an account that has access to the workspace.", fg="red", err=True)
        sys.exit(1)
    if schedules_only:
        click.echo(("SCHEDULES ONLY - no pipeline is created or changed" if apply_ else
                    "SCHEDULES ONLY (dry run - nothing changed; add --apply)") + f"  workspace: {workspace_id}")
    else:
        click.echo(("DEPLOY TO FABRIC" if apply_ else "DEPLOY TO FABRIC (dry run - nothing changed; add --apply)")
                   + f"  workspace: {workspace_id}")
    colours = {"create": "green", "update": "yellow", "refused": "red", "failed": "red"}
    for r in results:
        click.secho(f"  - {r.name}: {r.action}{f' [{r.item_id}]' if r.item_id else ''} - {r.detail}", fg=colours[r.action])
        if r.children:
            click.echo(f"      invokes: {', '.join(r.children)} (deployed first; their Fabric ids are filled in)")
    failed_schedules = False
    if schedules:
        from adf2fabric.agent.deployment.fabric_deploy import ScheduleItem, deploy_schedules, list_pipelines_by_name

        deployed = ({i.name for i in items} if schedules_only
                    else {r.name for r in results if r.action in ("create", "update")})
        sched_items = [
            ScheduleItem(p.fabric_pipeline_name, t.name, body)
            for p in conversion.pipelines if p.fabric_pipeline_name in deployed
            for t in p.triggers if t.schedules for body in t.schedules]
        click.echo("SCHEDULES (generated from ADF triggers; created disabled unless --enable-schedules)")
        if not sched_items:
            click.echo("  none: no deployed pipeline has a generated schedule.")
        else:
            try:
                ids = list_pipelines_by_name(client, workspace_id)
                sres = deploy_schedules(client, workspace_id, sched_items, ids, apply=apply_, enable=enable_schedules)
            except DeployError as exc:
                click.secho(f"  schedules failed: {exc}", fg="red", err=True)
                sys.exit(1)
            scolours = {"create": "green", "exists": "yellow", "skipped": "yellow", "failed": "red"}
            for r in sres:
                click.secho(f"  - {r.pipeline} / {r.trigger}: {r.action}{f' [{r.schedule_id}]' if r.schedule_id else ''}"
                            f" - {r.detail}", fg=scolours[r.action])
            failed_schedules = any(r.action == "failed" for r in sres)
    if failed_schedules or any(r.action in ("refused", "failed") for r in results):
        sys.exit(2)


@cli.command(name="create-shortcut")
@click.option("--workspace-id", "workspace_id", required=True, help="Target Fabric workspace id (GUID).")
@click.option("--lakehouse", "lakehouse", required=True, help="Lakehouse name. Created if it does not exist.")
@click.option("--name", "name", required=True, help="Shortcut name, for example the container name.")
@click.option("--path", "path", default="Files", show_default=True, help="Where in the lakehouse the shortcut goes (Files or Tables).")
@click.option("--storage-account", "account", required=True,
              help="The ADLS Gen2 account URL: https://<account>.dfs.core.windows.net")
@click.option("--subpath", "subpath", required=True, help="Container and optional folder, for example /migrationcontainer")
@click.option("--connection-id", "connection_id", required=True,
              help="GUID of an existing Fabric connection to that storage account (never a key).")
@azure_auth_options()
@click.option("--apply", "apply_", is_flag=True,
              help="Actually create what is missing. Without it this only reads. Nothing is deleted or overwritten.")
@_audited("create_shortcut")
def create_shortcut_cmd(workspace_id: str, lakehouse: str, name: str, path: str, account: str, subpath: str,
                        connection_id: str, apply_: bool, auth: str, **auth_env: str | None) -> None:
    """Create a lakehouse and an ADLS Gen2 shortcut so Fabric notebooks can read storage (dry run unless --apply)."""
    from adf2fabric.agent.deployment.fabric_deploy import FABRIC_SCOPE, DeployError, FabricClient
    from adf2fabric.agent.deployment.shortcut_deploy import ShortcutSpec, create_lakehouse_shortcut
    from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential

    try:
        credential, _ = build_credential(credential_from(auth, **auth_env))
        client = FabricClient(credential.get_token(FABRIC_SCOPE).token)
        r = create_lakehouse_shortcut(client, workspace_id, lakehouse,
                                      ShortcutSpec(name, path, account, subpath, connection_id), apply=apply_)
    except (DeployError, CredentialUnavailable) as exc:
        click.secho(f"Shortcut failed: {getattr(exc, 'detail', None) or exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - e.g. az login missing/expired; message carries no token
        click.secho(f"Shortcut failed: could not get a Fabric token or reach Fabric ({type(exc).__name__}). "
                    "Check `az login` with an account that has access to the workspace.", fg="red", err=True)
        sys.exit(1)
    click.echo(("SHORTCUT" if apply_ else "SHORTCUT (dry run - nothing changed; add --apply)") + f"  workspace: {workspace_id}")
    click.echo(f"  lakehouse: {r.lakehouse}: {r.lakehouse_action}{f' [{r.lakehouse_id}]' if r.lakehouse_id else ''}")
    click.secho(f"  shortcut:  {path}/{r.shortcut}: {r.shortcut_action} - {r.detail}",
                fg="red" if r.shortcut_action == "differs" else ("green" if apply_ else None))
    if r.onelake_path:
        click.echo(f"  read it from a notebook at: {r.onelake_path}")
    if r.shortcut_action == "differs":
        sys.exit(2)


@cli.command(name="fetch-databricks-notebooks")
@click.option("--project", "project_name", required=True, help="Project name (`discovery-adf-semantic` must have run).")
@click.option("--linked-service", "linked_service", default=None,
              help="Name of the ADF Databricks linked service in the project source; its 'domain' is the workspace URL.")
@click.option("--workspace-url", "workspace_url", default=None, help="The workspace URL, if you do not name a linked service.")
@click.option("--token-env", "token_env", default=None,
              help="NAME of the env var holding a Databricks personal access token, or an Entra token for the Azure "
                   "Databricks resource. Never the value itself. Without it, only the linked service is inspected.")
@click.option("--path", "paths", multiple=True,
              help="Fetch this notebook path instead of the ones the pipelines use. Repeatable.")
def fetch_databricks_notebooks_cmd(project_name: str, linked_service: str | None, workspace_url: str | None,
                                   token_env: str | None, paths: tuple[str, ...]) -> None:
    """Export the Databricks notebooks the pipelines run (read-only), and report what Fabric will need changed."""
    import json as _json
    import os

    from adf2fabric.agent.discovery.databricks_notebooks import (
        DatabricksClient, DatabricksError, fetch_notebooks, inspect_linked_service, notebook_paths_in_pipelines,
        results_to_json,
    )

    store = ProjectStore(projects_root(load_config()))
    try:
        info = None
        if linked_service:
            ls_file = next((p for p in (store.source_root(project_name) / "linkedService").glob("*.json")
                            if p.stem.lower() == linked_service.lower()), None)
            if ls_file is None:
                raise DatabricksError("linked_service_not_found", f"No linked service '{linked_service}' in the project source.",
                                      ["Check the name, or pass --workspace-url."])
            info = inspect_linked_service(_json.loads(ls_file.read_text(encoding="utf-8-sig")))
            click.echo("DATABRICKS LINKED SERVICE")
            click.echo(f"  name:           {info.name or linked_service}")
            click.echo(f"  workspace url:  {info.workspace_url or '(none)'}")
            click.echo(f"  authentication: {info.authentication}")
            click.echo(f"  cluster:        {info.cluster}" + (f" ({info.existing_cluster_id})" if info.existing_cluster_id else ""))
            for n in info.notes:
                click.echo(f"  note: {n}")
        url = workspace_url or (info.workspace_url if info else None)
        found: list[tuple[str, str, str]] = []
        skipped: list[str] = []
        if not paths:
            found, skipped = notebook_paths_in_pipelines(store.load_adf_semantic_discovery(project_name).pipelines)
        wanted = list(paths) or [p for _, _, p in found]
        used_by: dict[str, list[str]] = {}
        for pl, act, p in found:
            used_by.setdefault(p, []).append(f"{pl}/{act}")
        click.echo(f"NOTEBOOKS THE PIPELINES RUN: {len(set(wanted))}")
        for p in sorted(set(wanted)):
            click.echo(f"  - {p}" + (f"   used by {', '.join(sorted(used_by[p]))}" if p in used_by else ""))
        for s in skipped:
            click.secho(f"  skipped: {s}", fg="yellow")
        if not token_env:
            click.echo("No --token-env given: nothing was fetched.")
            return
        if not url:
            raise DatabricksError("bad_workspace_url", "No workspace URL: pass --linked-service or --workspace-url.")
        token = os.environ.get(token_env, "")
        if not token:
            raise DatabricksError("no_token", f"The environment variable {token_env} is empty or not set.")
        out = store.project_dir(project_name) / "databricks" / "notebooks"
        results = fetch_notebooks(DatabricksClient(url, token), wanted, out, used_by)
    except DatabricksError as exc:
        click.secho(f"Databricks retrieval failed [{exc.code}]: {exc}", fg="red", err=True)
        for a in exc.actions:
            click.echo(f"  - {a}", err=True)
        sys.exit(1)
    (out.parent / "retrieval.json").write_text(results_to_json(results, url), encoding="utf-8")
    failed = False
    for r in results:
        if r.status == "ok":
            counts = {s: sum(1 for f in r.findings if f.severity == s) for s in ("rewrite", "review")}
            click.secho(f"  ok     {r.workspace_path} -> {r.local_file}  ({counts['rewrite']} rewrite, "
                        f"{counts['review']} review)", fg="green")
            for f in r.findings[:8]:
                click.echo(f"           line {f.line}: [{f.severity}] {f.why}")
            if len(r.findings) > 8:
                click.echo(f"           ... {len(r.findings) - 8} more in retrieval.json")
        else:
            failed = True
            click.secho(f"  FAILED {r.workspace_path} [{r.error['code']}]: {r.error['message']}", fg="red")
            for a in r.error["actions"]:
                click.echo(f"           - {a}")
    click.echo(f"  notebooks: {out}")
    click.echo(f"  record:    {out.parent / 'retrieval.json'}")
    if failed:
        sys.exit(2)


@cli.command(name="deploy-notebook")
@click.option("--workspace-id", "workspace_id", required=True, help="Target Fabric workspace id (GUID).")
@click.option("--file", "file_path", required=True, type=click.Path(exists=True, dir_okay=False),
              help="The notebook to deploy: a .py file (one code cell) or an .ipynb file.")
@click.option("--name", "name", required=True, help="Display name of the notebook in Fabric. An existing notebook with "
                                                    "this name has its content replaced; nothing is ever deleted.")
@click.option("--source-name", "source_name", default=None,
              help="The notebook name the ADF/Synapse activity refers to (default: --name). Used for the binding.")
@click.option("--resolutions", "resolutions_path", type=click.Path(dir_okay=False), default=None,
              help="With --apply: record the new notebook id (and the workspace id) in this resolutions file, so the next "
                   "`convert --resolutions` binds the pipeline's notebook activity to it.")
@azure_auth_options()
@click.option("--apply", "apply_", is_flag=True,
              help="Actually create or update the notebook. Without it this only checks (read-only).")
@_audited("deploy_notebook")
def deploy_notebook_cmd(workspace_id: str, file_path: str, name: str, source_name: str | None,
                        resolutions_path: str | None, apply_: bool, auth: str, **auth_env: str | None) -> None:
    """Create or update a Fabric notebook from a local file (dry run unless --apply)."""
    from pathlib import Path

    from adf2fabric.agent.deployment.fabric_deploy import FABRIC_SCOPE, DeployError, FabricClient
    from adf2fabric.agent.deployment.notebook_deploy import bind_resolutions, build_definition, deploy_notebook
    from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential

    try:
        definition = build_definition(Path(file_path))
        credential, _ = build_credential(credential_from(auth, **auth_env))
        client = FabricClient(credential.get_token(FABRIC_SCOPE).token)
        result = deploy_notebook(client, workspace_id, name, definition, apply=apply_)
        changes = (bind_resolutions(Path(resolutions_path), source_name or name, result.item_id, workspace_id)
                   if apply_ and resolutions_path and result.item_id else [])
    except (DeployError, CredentialUnavailable) as exc:
        click.secho(f"Notebook deploy failed: {getattr(exc, 'detail', None) or exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - e.g. az login missing/expired; message carries no token
        click.secho(f"Notebook deploy failed: could not get a Fabric token or reach Fabric ({type(exc).__name__}). "
                    "Check `az login` with an account that has access to the workspace.", fg="red", err=True)
        sys.exit(1)
    click.echo(("DEPLOY NOTEBOOK" if apply_ else "DEPLOY NOTEBOOK (dry run - nothing changed; add --apply)")
               + f"  workspace: {workspace_id}")
    click.secho(f"  - {result.name}: {result.action}{f' [{result.item_id}]' if result.item_id else ''} - {result.detail}",
                fg="green" if apply_ else None)
    for c in changes:
        click.echo(f"  resolutions: {c}")
    if apply_ and not resolutions_path:
        click.echo("  (pass --resolutions <file> to record this id for `convert`)")


@cli.command(name="run")
@click.option("--workspace-id", "workspace_id", required=True, help="Fabric workspace id (GUID) that holds the pipeline.")
@click.option("--pipeline", "pipelines", multiple=True, required=True, help="Pipeline to run, by its Fabric name. Repeatable (run one after another).")
@click.option("--param", "params", multiple=True, help="Pipeline parameter as name=value (the value may be JSON, e.g. an array). Repeatable.")
@click.option("--no-wait", "no_wait", is_flag=True, help="Start the run and return without waiting for the result.")
@click.option("--timeout", "timeout_seconds", type=int, default=1800, show_default=True, help="Seconds to wait for each run.")
@azure_auth_options()
@click.option("--apply", "apply_", is_flag=True,
              help="Actually run the pipeline(s). A run executes the pipeline and uses capacity. Without it this only "
                   "checks the pipelines exist (read-only).")
@_audited("run")
def run_cmd(workspace_id: str, pipelines: tuple[str, ...], params: tuple[str, ...], no_wait: bool, timeout_seconds: int,
            apply_: bool, auth: str, **auth_env: str | None) -> None:
    """Run deployed pipelines on demand and report each result (dry run unless --apply)."""
    import json as _json

    from adf2fabric.agent.deployment.fabric_deploy import FABRIC_SCOPE, DeployError, FabricClient
    from adf2fabric.agent.deployment.fabric_run import run_pipeline
    from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential

    parameters: dict = {}
    for item in params:
        key, sep, raw = item.partition("=")
        if not sep or not key.strip():
            raise click.UsageError(f"--param must look like name=value, got '{item}'.")
        try:
            parameters[key.strip()] = _json.loads(raw)
        except ValueError:
            parameters[key.strip()] = raw
    failed = False
    try:
        credential, _ = build_credential(credential_from(auth, **auth_env))
        client = FabricClient(credential.get_token(FABRIC_SCOPE).token)
        existing = client.list_pipelines(workspace_id)
        missing = [n for n in pipelines if n.lower() not in existing]
        if missing:
            click.secho(f"Run failed: no pipeline named {', '.join(missing)} in the workspace. "
                        f"Present: {', '.join(sorted(existing)) or '(none)'}.", fg="red", err=True)
            sys.exit(1)
        if not apply_:
            click.echo(f"RUN IN FABRIC (dry run - nothing started; add --apply)  workspace: {workspace_id}")
            for n in pipelines:
                click.echo(f"  - {n}: would run" + (f" with {', '.join(sorted(parameters))}" if parameters else ""))
            return
        click.echo(f"RUN IN FABRIC  workspace: {workspace_id}")
        for n in pipelines:
            r = run_pipeline(client, workspace_id, n, parameters=parameters or None, wait=not no_wait,
                             timeout_seconds=timeout_seconds)
            colour = "green" if r.ok else ("yellow" if r.status in ("InProgress", "Timeout") else "red")
            click.secho(f"  - {n}: {r.status} [job {r.job_id}]" + (f" - {r.detail}" if r.detail else ""), fg=colour)
            if r.started or r.ended:
                click.echo(f"      started {r.started or '?'}   ended {r.ended or '?'}")
            if r.failure:
                click.secho(f"      failure: {r.failure}", fg="red")
            for a in r.activities:
                click.echo(f"      {a.status or '?':<10} {a.name} ({a.type})" + (f" - {a.error}" if a.error else ""))
            failed = failed or r.status not in ("Completed", "InProgress")
    except (DeployError, CredentialUnavailable) as exc:
        click.secho(f"Run failed: {getattr(exc, 'detail', None) or exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - e.g. az login missing/expired; message carries no token
        click.secho(f"Run failed: could not get a Fabric token or reach Fabric ({type(exc).__name__}). "
                    "Check `az login` with an account that has access to the workspace.", fg="red", err=True)
        sys.exit(1)
    if failed:
        sys.exit(1)


@cli.command(name="verify")
@click.option("--project", "project_name", required=True, help="Project name (`convert` and `deploy` must have run).")
@click.option("--workspace-id", "workspace_id", required=True, help="Fabric workspace id (GUID) that holds the pipelines.")
@click.option("--pipeline", "pipelines", multiple=True, help="Verify only this pipeline. Repeatable. Default: every converted one.")
@click.option("--no-adf", "no_adf", is_flag=True, help="Do not read the ADF factory's run history (compare with earlier Fabric runs only).")
@click.option("--timeout", "timeout_seconds", type=int, default=1800, show_default=True, help="Seconds to wait for each run.")
@azure_auth_options()
@click.option("--apply", "apply_", is_flag=True,
              help="Start a new Fabric run of each pipeline, then compare. Without it nothing is started: the latest finished "
                   "Fabric run of each pipeline is compared instead (read-only).")
@_audited("verify")
def verify_cmd(project_name: str, workspace_id: str, pipelines: tuple[str, ...], no_adf: bool, timeout_seconds: int,
               apply_: bool, auth: str, **auth_env: str | None) -> None:
    """Prove converted pipelines: compare each Fabric run with the last successful ADF run (or the previous Fabric run)."""
    from adf2fabric.agent.deployment.fabric_deploy import FABRIC_SCOPE, DeployError, FabricClient
    from adf2fabric.agent.deployment.verify import AdfRuns, Factory, load_record, render_report, save_record, verify_all
    from adf2fabric.agent.deployment.verify import callers as verify_callers
    from adf2fabric.agent.deployment.verify import record_path as verify_record_path
    from adf2fabric.connections.providers.azure_cli import AzureCliProvider, ProviderError
    from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential

    config = load_config()
    store = ProjectStore(projects_root(config))
    pid = slugify(project_name)
    if not store.has_conversion(pid):
        click.secho(f"Verify failed: project '{pid}' has no conversion. Run `adf2fabric convert` first.", fg="red", err=True)
        sys.exit(1)
    conv = store.load_conversion(pid)
    names = list(pipelines) or [p.source_pipeline for p in conv.pipelines]
    called = verify_callers(store.conversion_dir(pid), conv.pipelines)
    record_path = verify_record_path(store.project_dir(pid))
    previous = load_record(record_path)
    adf = None
    factory = Factory.from_source(store.load(pid).repository.repository_url if store.load(pid).repository else None)
    if factory and not no_adf:
        try:  # the factory's own subscription picks the signed-in account that owns it; the CLI default is not changed
            adf = AdfRuns(factory, AzureCliProvider().token(subscription=factory.subscription))
        except ProviderError as exc:
            click.secho(f"  ADF run history not available ({exc}); comparing with earlier Fabric runs only.", fg="yellow")
    try:
        credential, _ = build_credential(credential_from(auth, **auth_env))
        fabric_token = credential.get_token(FABRIC_SCOPE).token
        click.echo(f"RUN AND COMPARE ({'new runs' if apply_ else 'latest finished runs; nothing started, add --apply to run'})"
                   f"  workspace: {workspace_id}")
        results = verify_all(lambda: FabricClient(fabric_token), workspace_id, names, start=apply_, adf=adf,
                             previous=previous, called=called, timeout_seconds=timeout_seconds)
        for n, v in zip(names, results):
            colour = {"matches": "green", "partial": "yellow", "differs": "yellow", "no_baseline": None, "failed": "red",
                      "not_run": None}[v.verdict]
            ran = f" - Fabric run {v.ran_at}" if v.ran_at else ""
            base = f", {v.matched} fact(s) matched vs {v.baseline} run {v.baseline_at}" if v.baseline else ""
            click.secho(f"  - {n}: {v.verdict}{ran}{base}", fg=colour)
            for d in v.differences[:5]:
                click.echo(f"      {d['activity']} {d['fact']}: baseline {d['baseline']} -> Fabric {d['fabric']}")
            if v.detail:
                click.echo(f"      {v.detail}")
    except (DeployError, CredentialUnavailable) as exc:
        click.secho(f"Verify failed: {getattr(exc, 'detail', None) or exc}", fg="red", err=True)
        sys.exit(1)
    record = save_record(record_path, results)
    report = record_path.with_name("verification-report.md")
    report.write_text(render_report(record), encoding="utf-8")
    click.echo(f"  record: {record_path}")
    click.echo(f"  report: {report}")
    if any(v.verdict in ("failed", "differs") for v in results):
        sys.exit(1)


@cli.command(name="handover")
@click.option("--project", "project_name", required=True, help="Project name (`convert` must have run).")
def handover_cmd(project_name: str) -> None:
    """Write the handover report: each pipeline's readiness and next step, from what earlier stages recorded."""
    from adf2fabric.agent.handover import assess, render
    from adf2fabric.agent.deployment.verify import load_record, record_path

    store = ProjectStore(projects_root(load_config()))
    pid = slugify(project_name)
    if not store.has_conversion(pid):
        click.secho(f"Handover failed: project '{pid}' has no conversion. Run `adf2fabric convert` first.", fg="red", err=True)
        sys.exit(1)
    path = store.project_dir(pid) / "handover.md"
    path.write_text(render(store, pid), encoding="utf-8")
    click.echo(f"HANDOVER  project: {pid}")
    for r in assess(store.load_conversion(pid), load_record(record_path(store.project_dir(pid)))):
        click.echo(f"  - {r.pipeline}: {r.label}" + (f" ({r.why[:120]})" if r.why else ""))
    click.echo(f"  report: {path}")


@cli.command(name="compute-deploy")
@click.option("--project", "project_name", required=True, help="Project name (`convert` must have run).")
@click.option("--workspace-id", "workspace_id", required=True, help="Target Fabric workspace id (GUID).")
@click.option("--pool", "pools", multiple=True, help="Approve this proposed pool by name. Repeatable.")
@click.option("--environment", "environments", multiple=True, help="Approve this proposed environment by name. Repeatable.")
@click.option("--all", "all_", is_flag=True, help="Approve every proposed pool and environment.")
@click.option("--reuse", "reuse", is_flag=True,
              help="Reuse an existing pool with the same node size whose node range covers the proposal's, instead of "
                   "creating one. Pools with identical settings are always reused.")
@click.option("--publish", "publish", is_flag=True,
              help="Also publish the environments created in this run. Publishing runs as a Spark job and uses capacity.")
@azure_auth_options()
@click.option("--apply", "apply_", is_flag=True,
              help="Actually create what you approved. Without it this is a dry run (read-only). Requires --pool, "
                   "--environment or --all: nothing is created that you did not name.")
@_audited("compute_deploy")
def compute_deploy_cmd(project_name: str, workspace_id: str, pools: tuple[str, ...], environments: tuple[str, ...],
                       all_: bool, reuse: bool, publish: bool, apply_: bool, auth: str, **auth_env: str | None) -> None:
    """Create the Fabric Spark pools and environments the compute plan proposes (dry run unless --apply)."""
    from adf2fabric.agent.deployment.compute_deploy import deploy_compute
    from adf2fabric.agent.deployment.fabric_deploy import FABRIC_SCOPE, DeployError, FabricClient
    from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential
    from adf2fabric.project.compute import ComputePlan

    store = ProjectStore(projects_root(load_config()))
    plan_file = store.conversion_dir(project_name) / "compute-plan.json"
    try:
        if plan_file.exists():
            plan = ComputePlan.model_validate_json(plan_file.read_text(encoding="utf-8"))
        else:
            plan = store.load_conversion(project_name).compute_plan
    except Exception as exc:  # noqa: BLE001
        click.secho(f"Compute deploy failed: {exc}", fg="red", err=True)
        sys.exit(1)
    if plan is None or not plan.pools:
        click.secho("Nothing to create: the conversion found no external Spark compute (run `convert` first).", fg="yellow")
        sys.exit(1)
    known_pools = {p.name.lower() for p in plan.pools}
    known_envs = {e.name.lower() for e in plan.environments}
    unknown = [n for n in pools if n.lower() not in known_pools] + [n for n in environments if n.lower() not in known_envs]
    if unknown:
        click.secho(f"Compute deploy failed: not in the plan: {', '.join(unknown)}. Pools: "
                    f"{', '.join(p.name for p in plan.pools if p.status == 'proposed') or '(none)'}; environments: "
                    f"{', '.join(e.name for e in plan.environments) or '(none)'}.", fg="red", err=True)
        sys.exit(1)
    selected = bool(pools or environments or all_)
    if apply_ and not selected:
        click.secho("Nothing was created. --apply needs you to approve what to create: --pool NAME, --environment NAME "
                    "or --all. Run without --apply to see the proposals.", fg="red", err=True)
        sys.exit(1)
    try:
        credential, _ = build_credential(credential_from(auth, **auth_env))
        client = FabricClient(credential.get_token(FABRIC_SCOPE).token)
        results = deploy_compute(client, workspace_id, plan, apply=apply_,
                                 pools=None if (all_ or not selected) else set(pools),
                                 environments=None if (all_ or not selected) else set(environments), publish=publish,
                                 reuse_compatible=reuse)
    except (DeployError, CredentialUnavailable) as exc:
        click.secho(f"Compute deploy failed: {getattr(exc, 'detail', None) or exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - e.g. az login missing/expired; message carries no token
        click.secho(f"Compute deploy failed: could not get a Fabric token or reach Fabric ({type(exc).__name__}). "
                    "Check `az login` with an account that is an Admin of the workspace.", fg="red", err=True)
        sys.exit(1)
    click.echo(("SPARK COMPUTE IN FABRIC" if apply_ else "SPARK COMPUTE IN FABRIC (dry run - nothing changed; approve with "
                "--pool/--environment/--all and --apply)") + f"  workspace: {workspace_id}")
    colours = {"create": "green", "publish": "green", "reuse": "green", "exists": "yellow", "conflict": "red", "not_selected": None,
               "skipped": "yellow", "refused": "red", "failed": "red"}
    for r in results:
        click.secho(f"  - {r.kind} {r.name}: {r.action}{f' [{r.item_id}]' if r.item_id else ''} - {r.detail}",
                    fg=colours[r.action])
    if apply_:  # keep a record: Fabric ids, and each activity's provisioning status, beside the plan
        import json as _json
        from datetime import datetime, timezone

        from adf2fabric.agent.deployment.compute_deploy import record_results

        updated = record_results(plan, results)
        base = store.conversion_dir(project_name)
        (base / "compute-plan.json").write_text(updated.model_dump_json(indent=2) + "\n", encoding="utf-8")
        (base / "compute-provisioning.json").write_text(_json.dumps({
            "workspace_id": workspace_id, "applied_at": datetime.now(timezone.utc).isoformat(),
            "results": [r.__dict__ for r in results]}, indent=2) + "\n", encoding="utf-8")
        click.echo(f"  record: {base / 'compute-provisioning.json'} (plan updated with Fabric ids and statuses)")
    if any(r.action in ("refused", "failed", "conflict") for r in results):
        sys.exit(2)



@cli.command(name="assess-compute")
@click.option("--project", "project_name", required=True,
              help="Project/engagement name (discovery and semantic must have run).")
@click.option("--runs-per-day", "runs_per_day", type=float, default=1.0, show_default=True,
              help="Runs per day assumed for pipelines that have no trigger.")
@click.option("--skip-stopped-triggers", is_flag=True, help="Ignore triggers that are Stopped in ADF.")
@click.option("--history/--no-history", "use_history", default=False,
              help="Read measured durations and DIUs from the factory's run history (read-only ARM calls).")
@click.option("--live", "use_live", is_flag=True,
              help="Read the pipeline definitions from the live factory instead of the stored Git record.")
@click.option("--prices", "price_mode", type=click.Choice(["live", "recorded"]), default="live", show_default=True,
              help="Databricks prices: live from the Azure Retail Prices API, or the table recorded in compute_rates.")
@click.option("--subscription", "subscription_id", default=None, help="Azure subscription id (for --history).")
@click.option("--resource-group", "resource_group", default=None, help="Resource group of the factory (for --history).")
@click.option("--factory", "factory_name", default=None, help="Data Factory name (for --history).")
@click.option("--days", type=int, default=30, show_default=True, help="How far back to read run history.")
@azure_auth_options()
def assess_compute(project_name: str, runs_per_day: float, skip_stopped_triggers: bool, use_history: bool,
                   use_live: bool, price_mode: str, subscription_id: str | None, resource_group: str | None,
                   factory_name: str | None, days: int, auth: str, **auth_env: str | None) -> None:
    """Estimate the Fabric compute (CU) the pipelines need and the capacity size that fits (deterministic)."""
    from adf2fabric.agent.assessment.agent import ComputeAgent, ComputeRequest

    store = ProjectStore(projects_root(load_config()))
    fetcher = loader = None
    if (use_history or use_live) and not (subscription_id and resource_group and factory_name):
        click.secho("--history and --live need --subscription, --resource-group and --factory.", fg="red", err=True)
        sys.exit(2)
    prices = workspace = None
    if price_mode == "live":
        import requests

        from adf2fabric.agent.assessment.azure_prices import LivePrices

        prices = LivePrices(requests)
    if subscription_id:
        def workspace(resource_id: str):
            import requests

            from adf2fabric.agent.assessment.azure_prices import workspace_info
            from adf2fabric.connections.providers.azure_cli import AzureCliProvider

            return workspace_info(requests, AzureCliProvider().token(subscription=subscription_id), resource_id)
    if use_live:
        def loader():
            import requests

            from adf2fabric.agent.warehouse.adf_sources import fetch_live, to_semantic
            from adf2fabric.connections.providers.azure_cli import AzureCliProvider

            raw = fetch_live(requests, subscription_id, resource_group, factory_name,
                             AzureCliProvider().token(subscription=subscription_id))
            return to_semantic(raw, project_name, f"live:{factory_name}")
    if use_history:
        from adf2fabric.agent.assessment import adf_history
        from adf2fabric.agent.discovery.adf.arm import ARM_SCOPE, default_transport
        from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential

        try:
            if auth == "azure_cli":
                # Token for the factory's subscription: the CLI picks the signed-in account that owns it, even when
                # another account (for example the Fabric one) is the current default.
                from adf2fabric.connections.providers.azure_cli import AzureCliProvider, ProviderError

                try:
                    token = AzureCliProvider().token(subscription=subscription_id)
                except ProviderError as exc:
                    click.secho(f"Cannot get an Azure token for subscription {subscription_id}: {exc}", fg="red", err=True)
                    sys.exit(1)
            else:
                credential, _secret = build_credential(credential_from(auth, **auth_env))
                token = credential.get_token(ARM_SCOPE).token
        except CredentialUnavailable as exc:
            click.secho(f"Cannot sign in to Azure: {exc.detail}", fg="red", err=True)
            sys.exit(1)

        def fetcher(names):
            return adf_history.fetch(default_transport(), token, subscription_id, resource_group, factory_name, names,
                                     days=days, log=lambda m: click.echo(f"  history: {m}"))

    result = ComputeAgent(store).run(ComputeRequest(project_name=project_name, default_runs_per_day=runs_per_day,
                                                    include_stopped_triggers=not skip_stopped_triggers),
                                     history_fetcher=fetcher, log=lambda m: click.echo(f"  {m}"),
                                     definitions_loader=loader, prices=prices, databricks_workspace=workspace)
    if not result.success:
        click.secho(f"Compute assessment failed: {result.error}", fg="red", err=True)
        sys.exit(1)
    a = result.assessment
    click.secho("COMPUTE ASSESSMENT", fg="green")
    click.echo(f"  pipelines:          {len(a.pipelines)}   run history: {'yes' if a.history_used else 'no'}")
    click.echo(f"  Fabric CU-hours/day: {a.cu_hours_per_day:g}   average CU: {a.average_cu_needed:g}")
    click.echo(f"  recommended:        {a.recommended_sku}")
    for p in a.pipelines:
        click.echo(f"    {p.pipeline:<34} {p.cu_seconds_per_run:>10g} CU-s/run  x {p.runs_per_day:g}/day")
    click.echo(f"  report: {result.report_markdown}")


@cli.command(name="warehouse-assess")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, dir_okay=False),
              help="Warehouse migration config (YAML; see config/warehouse-migration.example.yaml).")
@click.option("--no-target", is_flag=True, help="Skip the read-only inspection of the target Warehouse.")
def warehouse_assess(config_path: str, no_target: bool) -> None:
    """Discover a source warehouse, map ADF usage, plan the Fabric Warehouse objects (dry-run DDL) and validate
    an existing target. Read-only: nothing is deployed or loaded."""
    from pathlib import Path

    from adf2fabric.agent.warehouse import agent as wh_agent
    from adf2fabric.agent.warehouse.config import load as load_wh_config

    try:
        cfg = load_wh_config(Path(config_path))
    except ValueError as exc:
        click.secho(f"Invalid config: {exc}", fg="red", err=True)
        sys.exit(2)
    root = projects_root(load_config())
    out = root / cfg.project / "warehouse"
    try:
        rep = wh_agent.run(cfg, ProjectStore(root), out, inspect_target=not no_target, log=lambda m: click.echo(f"  {m}"))
    except Exception as exc:  # noqa: BLE001 - surface any failure as a CLI error
        click.secho(f"Warehouse assessment failed: {exc}", fg="red", err=True)
        sys.exit(1)
    click.secho("WAREHOUSE ASSESSMENT", fg="green")
    for k, v in rep.catalog_summary.items():
        click.echo(f"  {k:<32} {v}")
    for e in rep.manifest:
        click.echo(f"    {e.source_key:<42} {e.kind.value:<15} {e.adf_usage:<10} {e.action:<7} {e.validation}")
    click.echo(f"  report: {out / 'report.md'}")


@cli.command(name="warehouse-migrate")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, dir_okay=False),
              help="Warehouse migration config (see config/warehouse-migration.example.yaml).")
@click.option("--apply", "apply_", is_flag=True,
              help="Create and load in the target. Without it, everything is discovered and planned but nothing is written.")
@click.option("--create-warehouse", is_flag=True,
              help="Create the target Warehouse if it does not exist (also needs target.create_if_missing: true).")
@click.option("--no-load", is_flag=True, help="Create objects only; do not load data.")
def warehouse_migrate(config_path: str, apply_: bool, create_warehouse: bool, no_load: bool) -> None:
    """Migrate a source warehouse into a Fabric Warehouse: discover the source, read ADF (live and Git), keep the
    objects ADF relates to (plus their dependencies), create and load them, validate, and report."""
    from pathlib import Path

    from adf2fabric.agent.warehouse import agent as wh_agent
    from adf2fabric.agent.warehouse.config import load as load_wh_config

    try:
        cfg = load_wh_config(Path(config_path))
    except ValueError as exc:
        click.secho(f"Invalid config: {exc}", fg="red", err=True)
        sys.exit(2)
    root = projects_root(load_config())
    out = root / cfg.project / "warehouse"
    try:
        rep = wh_agent.migrate(cfg, ProjectStore(root), out, apply=apply_, create_warehouse=create_warehouse,
                               load=not no_load, log=lambda m: click.echo(f"  {m}"))
    except Exception as exc:  # noqa: BLE001 - surface any failure as a CLI error
        click.secho(f"Warehouse migration failed: {exc}", fg="red", err=True)
        sys.exit(1)
    status = (rep.deploy or {}).get("status", "planned")
    click.secho(f"WAREHOUSE MIGRATION ({'applied' if apply_ else 'dry run'}): {status}",
                fg={"succeeded": "green", "planned": "yellow"}.get(status, "red"))
    for e in rep.manifest:
        click.echo(f"    {e.source_key:<42} {e.kind.value:<15} {e.adf_usage:<10} {e.action:<8} {e.validation}")
    click.echo(f"  report: {out / 'report.md'}")
    if status in ("refused", "failed"):
        sys.exit(2)


@cli.command(name="warehouse-repoint")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, dir_okay=False),
              help="Warehouse migration config with a repoint section.")
@click.option("--apply", "apply_", is_flag=True, help="Update the pipelines. Without it, only the changes are shown.")
def warehouse_repoint(config_path: str, apply_: bool) -> None:
    """Point Fabric pipelines that read the old source warehouse at the migrated Fabric Warehouse.
    Each pipeline is backed up before it is changed."""
    from pathlib import Path

    from adf2fabric.agent.warehouse import agent as wh_agent
    from adf2fabric.agent.warehouse.config import load as load_wh_config

    try:
        cfg = load_wh_config(Path(config_path))
    except ValueError as exc:
        click.secho(f"Invalid config: {exc}", fg="red", err=True)
        sys.exit(2)
    out = projects_root(load_config()) / cfg.project / "warehouse" / "repoint"
    try:
        res = wh_agent.repoint_pipelines(cfg, out, apply=apply_, log=lambda m: click.echo(f"  {m}"))
    except Exception as exc:  # noqa: BLE001 - surface any failure as a CLI error
        click.secho(f"Repoint failed: {exc}", fg="red", err=True)
        sys.exit(1)
    click.secho(f"REPOINT ({'applied' if apply_ else 'dry run'}) -> {res['warehouse']}", fg="green")
    for name, e in res["pipelines"].items():
        click.echo(f"    {name:<34} {e['status']}")
    if any(e["status"] == "not found" for e in res["pipelines"].values()):
        sys.exit(2)


if __name__ == "__main__":
    cli()
