"""Command-line entry point for the ADF -> Fabric migration agent.

Currently the only capability is Intake: authenticate with GitHub, fetch a
source repository, and record its metadata. Later lifecycle stages (Discovery
onward) are documented in the README but not implemented.
"""

from __future__ import annotations

import sys

import click

from adf2fabric.agent.consultation import ConsultationAgent, ConsultationRequest
from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf import AdfDiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoveryRequest
from adf2fabric.agent.discovery.adls import AdlsDiscoveryRequest
from adf2fabric.agent.discovery.adls_paths import AdlsPathsDiscoveryRequest
from adf2fabric.agent.discovery.sql import SqlDiscoveryRequest
from adf2fabric.agent.discovery.sql_structural import SqlStructuralDiscoveryRequest
from adf2fabric.agent.intake import IntakeAgent, IntakeRequest
from adf2fabric.config import load_config, projects_root
from adf2fabric.project.models import (
    AdfAccess,
    AzureCredentialRef,
    SqlCredentialRef,
    SqlServerAccess,
    SqlTlsMode,
    SqlTlsPolicy,
)
from adf2fabric.project.store import ProjectStore


@click.group()
def cli() -> None:
    """ADF -> Microsoft Fabric migration agent."""


@cli.command()
@click.option("--project", "project_name", required=True, help="Project/engagement name.")
@click.option("--repo", "repository_url", required=True, help="Source repository URL.")
@click.option("--branch", default=None, help="Branch to fetch (default from config).")
@click.option("--provider", default=None, help="Source provider (default from config).")
@click.option("--refresh", is_flag=True, help="Force a fresh fetch even if a local source exists.")
def intake(
    project_name: str,
    repository_url: str,
    branch: str | None,
    provider: str | None,
    refresh: bool,
) -> None:
    """Fetch a source repository and record it as a project."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    agent = IntakeAgent(store, token_env=config["github_token_env"])

    request = IntakeRequest(
        project_name=project_name,
        repository_url=repository_url,
        branch=branch or config["default_branch"],
        provider=provider or config["default_provider"],
        refresh=refresh,
    )

    result = agent.run(request)

    if not result.success:
        click.secho(f"Intake failed: {result.error}", fg="red", err=True)
        sys.exit(1)

    meta = result.project.repository
    verb = "Reused" if result.reused else "Fetched"
    click.secho(f"Intake succeeded ({verb.lower()}).", fg="green")
    click.echo(f"  project:      {result.project.id}")
    click.echo(f"  provider:     {meta.provider}")
    click.echo(f"  repository:   {meta.repository_url}")
    click.echo(f"  branch:       {meta.branch}")
    click.echo(f"  commit:       {meta.commit_sha}")
    click.echo(f"  local source: {meta.local_source_path}")


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
@click.option(
    "--auth",
    type=click.Choice(["azure_cli", "client_secret", "azure_identity"]),
    default="azure_cli",
    show_default=True,
    help="Azure authentication mechanism.",
)
@click.option("--tenant-id-env", "tenant_id_env", default=None, help="NAME of env var holding the tenant id (client_secret only).")
@click.option("--client-id-env", "client_id_env", default=None, help="NAME of env var holding the client id (client_secret only).")
@click.option(
    "--client-secret-env",
    "client_secret_env",
    default=None,
    help="NAME of env var holding the client secret (client_secret only). "
    "The value is never read into logs, output, or the persisted record.",
)
def discovery_adf(
    project_name: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    auth: str,
    tenant_id_env: str | None,
    client_id_env: str | None,
    client_secret_env: str | None,
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
        credential=AzureCredentialRef(
            auth=auth,
            tenant_id_env=tenant_id_env,
            client_id_env=client_id_env,
            client_secret_env=client_secret_env,
        ),
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
    if d.failed_count:
        click.secho(f"  note: {d.detail}", fg="yellow")
    click.echo(f"  record: {store.adf_semantic_discovery_path(d.project_id)}")


if __name__ == "__main__":
    cli()
