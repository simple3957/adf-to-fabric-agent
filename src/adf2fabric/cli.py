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
@click.option("--schedules-only", "schedules_only", is_flag=True,
              help="Only create schedules, for pipelines that are already in the workspace. No pipeline is created or "
                   "overwritten. Implies --schedules.")
@click.option("--enable-schedules", "enable_schedules", is_flag=True,
              help="With --schedules: create schedules enabled when the ADF trigger was started. A start time in the "
                   "past makes Fabric run the pipeline immediately.")
@azure_auth_options()
@click.option("--apply", "apply_", is_flag=True,
              help="Actually create/overwrite pipelines in Fabric. Without it this is a dry run (read-only).")
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
@click.option("--subscription", "subscription_id", default=None, help="Azure subscription id (for --history).")
@click.option("--resource-group", "resource_group", default=None, help="Resource group of the factory (for --history).")
@click.option("--factory", "factory_name", default=None, help="Data Factory name (for --history).")
@click.option("--days", type=int, default=30, show_default=True, help="How far back to read run history.")
@azure_auth_options()
def assess_compute(project_name: str, runs_per_day: float, skip_stopped_triggers: bool, use_history: bool,
                   subscription_id: str | None, resource_group: str | None, factory_name: str | None, days: int,
                   auth: str, **auth_env: str | None) -> None:
    """Estimate the Fabric compute (CU) the pipelines need and the capacity size that fits (deterministic)."""
    from adf2fabric.agent.assessment.agent import ComputeAgent, ComputeRequest

    store = ProjectStore(projects_root(load_config()))
    fetcher = None
    if use_history:
        if not (subscription_id and resource_group and factory_name):
            click.secho("--history needs --subscription, --resource-group and --factory.", fg="red", err=True)
            sys.exit(2)
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
                                     history_fetcher=fetcher, log=lambda m: click.echo(f"  {m}"))
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

if __name__ == "__main__":
    cli()
