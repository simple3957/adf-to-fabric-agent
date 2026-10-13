"""Command-line entry point for the ADF -> Fabric migration agent.

Currently the only capability is Intake: authenticate with GitHub, fetch a
source repository, and record its metadata. Later lifecycle stages (Discovery
onward) are documented in the README but not implemented.
"""

from __future__ import annotations

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
from adf2fabric.project.store import ProjectStore


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
                branch=branch or config["default_branch"],
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
def convert(project_name: str, pipelines: tuple[str, ...], resolutions_path: str | None,
            apply_reconstruction: bool) -> None:
    """Convert the discovered ADF pipelines to Fabric pipeline definitions (deterministic, no LLM)."""
    config = load_config()
    store = ProjectStore(projects_root(config))
    result = ConversionAgent(store).run(
        ConversionRequest(project_name=project_name, pipelines=list(pipelines), resolutions_path=resolutions_path,
                          apply_reconstruction=apply_reconstruction)
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
    unbound = [q for q in [*c.connections, *c.workspace_items] if not q.bound]
    if unbound:
        click.secho(f"  {len(unbound)} binding(s) still to supply before anything can run; see the report.", fg="yellow")
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
@click.option("--enable-schedules", "enable_schedules", is_flag=True,
              help="With --schedules: create schedules enabled when the ADF trigger was started. A start time in the "
                   "past makes Fabric run the pipeline immediately.")
@azure_auth_options()
@click.option("--apply", "apply_", is_flag=True,
              help="Actually create/overwrite pipelines in Fabric. Without it this is a dry run (read-only).")
def deploy_cmd(project_name: str, workspace_id: str, pipelines: tuple[str, ...], with_children: bool,
               allow_unbound: bool, schedules: bool, enable_schedules: bool, apply_: bool, auth: str,
               **auth_env: str | None) -> None:
    """Deploy converted pipelines to a Fabric workspace (dry run unless --apply)."""
    import json as _json

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
        results = deploy(client, workspace_id, items, apply=apply_, allow_unbound=allow_unbound)
    except (DeployError, CredentialUnavailable) as exc:
        click.secho(f"Deploy failed: {getattr(exc, 'detail', None) or exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - e.g. az login missing/expired; message carries no token
        click.secho(f"Deploy failed: could not get a Fabric token or reach Fabric ({type(exc).__name__}). "
                    "Check `az login` with an account that has access to the workspace.", fg="red", err=True)
        sys.exit(1)
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

        deployed = {r.name for r in results if r.action in ("create", "update")}
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


if __name__ == "__main__":
    cli()
