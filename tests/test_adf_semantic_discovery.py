"""ADF Semantic / Deep Discovery (Phase 1) tests — fully offline, deterministic.

A synthetic ADF source tree is written into a temp project; real Repository
Discovery runs to produce the candidate inventory; then Semantic Discovery runs
against the immutable source. No Azure, no network, no live services.

The suite proves the secret-safety guarantee: connection-string values, password
parameter defaults, and SAS signatures placed in the source must NEVER appear in
the persisted output.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import subprocess
from pathlib import Path

import adf2fabric.agent.discovery.adf_semantic.parser as parser_mod
import adf2fabric.agent.discovery.adf_semantic.discoverer as discoverer_mod
from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import (
    AdfSemanticDiscoverer,
    AdfSemanticDiscoveryRequest,
    AdfSemanticDiscoveryResult,
)
from adf2fabric.project.discovery import (
    AdfArtifactKind,
    AdfSemanticDiscovery,
    AdfSemanticStatus,
    ExternalResourceKind,
)
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.store import ProjectStore

# Secret sentinels planted in the source; none may reach persisted output.
CONN_SENTINEL = "SENTINEL_CONNSTRING_SHOULD_NOT_LEAK"
SAS_SENTINEL = "SENTINEL_SAS_SHOULD_NOT_LEAK"
PWD_SENTINEL = "PWDSENTINEL_SHOULD_NOT_LEAK"
GP_SENTINEL = "SENTINEL_GP_SHOULD_NOT_LEAK"
ALL_SENTINELS = (CONN_SENTINEL, SAS_SENTINEL, PWD_SENTINEL, GP_SENTINEL)


# --- synthetic source -------------------------------------------------------

SOURCE: dict[str, dict] = {
    "pipeline/PL_Main.json": {
        "name": "PL_Main",
        "properties": {
            "activities": [
                {
                    "name": "Lookup Auth",
                    "type": "Lookup",
                    "dependsOn": [],
                    "typeProperties": {
                        "source": {
                            "type": "AzureSqlSource",
                            "sqlReaderStoredProcedureName": "[dbo].[usp_Get]",
                        },
                        "dataset": {"referenceName": "DS_Sql", "type": "DatasetReference"},
                    },
                },
                {
                    "name": "For Each",
                    "type": "ForEach",
                    "dependsOn": [
                        {"activity": "Lookup Auth", "dependencyConditions": ["Succeeded"]}
                    ],
                    "typeProperties": {
                        "activities": [
                            {
                                "name": "Exec Child",
                                "type": "ExecutePipeline",
                                "dependsOn": [],
                                "typeProperties": {
                                    "pipeline": {
                                        "referenceName": "PL_Child",
                                        "type": "PipelineReference",
                                    }
                                },
                            }
                        ]
                    },
                },
                {
                    "name": "Merge",
                    "type": "SqlServerStoredProcedure",
                    "dependsOn": [
                        {"activity": "For Each", "dependencyConditions": ["Completed"]}
                    ],
                    "typeProperties": {"storedProcedureName": "[dbo].[usp_Merge]"},
                    "linkedServiceName": {
                        "referenceName": "LS_Sql",
                        "type": "LinkedServiceReference",
                    },
                },
            ],
            "parameters": {
                "GoodParam": {"type": "String", "defaultValue": "hello"},
                "BadParam": {"type": "String", "defaultValue": f"Password={PWD_SENTINEL}"},
            },
            "variables": {"ContainerName": {"type": "String", "defaultValue": "raw"}},
            "folder": {"name": "Main"},
            "annotations": [],
        },
    },
    "pipeline/PL_Child.json": {
        "name": "PL_Child",
        "properties": {
            "activities": [
                {
                    "name": "Copy RestToAdls",
                    "type": "Copy",
                    "dependsOn": [],
                    "typeProperties": {
                        "source": {"type": "RestSource"},
                        "sink": {"type": "JsonSink"},
                    },
                    "inputs": [{"referenceName": "DS_Rest", "type": "DatasetReference"}],
                    "outputs": [{"referenceName": "DS_Adls", "type": "DatasetReference"}],
                },
                {
                    "name": "Exec Missing",
                    "type": "ExecutePipeline",
                    "dependsOn": [
                        {"activity": "Copy RestToAdls", "dependencyConditions": ["Succeeded"]}
                    ],
                    "typeProperties": {
                        "pipeline": {"referenceName": "PL_Missing", "type": "PipelineReference"}
                    },
                },
            ],
            "annotations": [],
        },
    },
    "dataset/DS_Sql.json": {
        "name": "DS_Sql",
        "properties": {
            "linkedServiceName": {"referenceName": "LS_Sql", "type": "LinkedServiceReference"},
            "folder": {"name": "F"},
            "type": "AzureSqlTable",
            "schema": [{"name": "Id", "type": "int"}, {"name": "Name", "type": "nvarchar"}],
            "typeProperties": {"schema": "Staging", "table": "Authorities"},
        },
    },
    "dataset/DS_Adls.json": {
        "name": "DS_Adls",
        "properties": {
            "linkedServiceName": {"referenceName": "LS_Adls", "type": "LinkedServiceReference"},
            "parameters": {"FolderName": {"type": "string"}, "FileName": {"type": "string"}},
            "type": "Json",
            "typeProperties": {
                "location": {
                    "type": "AzureBlobFSLocation",
                    "fileName": {"value": "@dataset().FileName", "type": "Expression"},
                    "folderPath": {"value": "@dataset().FolderName", "type": "Expression"},
                    "fileSystem": "raw",
                }
            },
            "schema": {},
        },
    },
    "dataset/DS_Rest.json": {
        "name": "DS_Rest",
        "properties": {
            "linkedServiceName": {"referenceName": "LS_Rest", "type": "LinkedServiceReference"},
            "type": "RestResource",
            "typeProperties": {"relativeUrl": "Authorities"},
            "schema": [],
        },
    },
    "linkedService/LS_Sql.json": {
        "name": "LS_Sql",
        "properties": {
            "type": "AzureSqlDatabase",
            "typeProperties": {
                "connectionString": (
                    "Integrated Security=False;Data Source=srv.database.windows.net;"
                    f"Initial Catalog=db;Password={CONN_SENTINEL}"
                )
            },
        },
        "type": "Microsoft.DataFactory/factories/linkedservices",
    },
    "linkedService/LS_Adls.json": {
        "name": "LS_Adls",
        "properties": {
            "type": "AzureBlobFS",
            "typeProperties": {
                "url": "https://adlsdemo.dfs.core.windows.net/",
                "accountKey": {
                    "type": "AzureKeyVaultSecret",
                    "store": {"referenceName": "LS_Akv", "type": "LinkedServiceReference"},
                    "secretName": "adls-key",
                },
            },
        },
    },
    "linkedService/LS_AdlsSas.json": {
        "name": "LS_AdlsSas",
        "properties": {
            "type": "AzureBlobFS",
            "typeProperties": {"url": f"https://sasacct.dfs.core.windows.net/?sig={SAS_SENTINEL}"},
        },
    },
    "linkedService/LS_Akv.json": {
        "name": "LS_Akv",
        "properties": {
            "type": "AzureKeyVault",
            "typeProperties": {"baseUrl": "https://kvdemo.vault.azure.net/"},
        },
    },
    "linkedService/LS_Rest.json": {
        "name": "LS_Rest",
        "properties": {
            "type": "RestService",
            "typeProperties": {
                "url": "https://api.example.com/",
                "authenticationType": "Anonymous",
            },
        },
    },
    "trigger/TRG.json": {
        "name": "TRG",
        "properties": {
            "runtimeState": "Stopped",
            "pipelines": [
                {"pipelineReference": {"referenceName": "PL_Main", "type": "PipelineReference"}}
            ],
            "type": "ScheduleTrigger",
            "typeProperties": {
                "recurrence": {
                    "frequency": "Day",
                    "interval": 1,
                    "timeZone": "UTC",
                    "startTime": "2020-08-01T12:00:00Z",
                }
            },
        },
    },
    "factory/factory.json": {
        "name": "factory",
        "properties": {
            "globalParameters": {
                "environment": {"type": "string", "value": "dev"},
                "SecretGP": {"type": "string", "value": f"AccountKey={GP_SENTINEL}"},
            }
        },
        "location": "northeurope",
    },
}


# --- helpers ----------------------------------------------------------------


def _write_source(store: ProjectStore, pid: str, files: dict[str, dict]) -> None:
    source_dir = store.source_dir(pid)
    for rel, content in files.items():
        full = source_dir / rel
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(json.dumps(content, indent=2), encoding="utf-8")


def _git_commit(path: Path) -> str:
    """Initialise a git repo in ``path`` and commit everything; return HEAD sha.

    Repository Discovery verifies the source is a git repo whose HEAD matches the
    commit recorded by Intake, so the synthetic source must be a real repo.
    """
    def g(*args, check=True):
        return subprocess.run(
            ["git", "-C", str(path), *args], capture_output=True, text=True, check=check
        )

    g("init", "-q")
    g("add", "-A")
    g("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "init")
    return g("rev-parse", "HEAD").stdout.strip()


def _make_project(tmp_path, files: dict[str, dict], run_repo: bool = True):
    store = ProjectStore(tmp_path / "projects")
    pid = "test-proj"
    _write_source(store, pid, files)
    sha = _git_commit(store.source_dir(pid))
    store.save(
        Project(
            id=pid,
            name="Test Proj",
            repository=RepositoryMetadata(
                provider="github",
                repository_url="https://example.invalid/x.git",
                branch="main",
                commit_sha=sha,
                local_source_path=str(store.source_dir(pid)),
            ),
        )
    )
    if run_repo:
        DiscoveryAgent(store).discover_repository(DiscoveryRequest(project_name="test-proj"))
    return store, pid


def _run(store) -> AdfSemanticDiscoveryResult:
    return AdfSemanticDiscoverer(store).run(
        AdfSemanticDiscoveryRequest(project_name="test-proj")
    )


def _by_name(items):
    return {i.name: i for i in items}


# --- extraction: pipelines + nested activities ------------------------------


def test_pipeline_and_nested_activity_extraction(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE)
    d = _run(store).discovery
    assert d.status == AdfSemanticStatus.discovered
    pipes = _by_name(d.pipelines)
    main = pipes["PL_Main"]
    assert main.folder == "Main"
    assert main.activity_count == 3
    assert [a.name for a in main.activities] == ["Lookup Auth", "For Each", "Merge"]
    foreach = main.activities[1]
    assert foreach.type == "ForEach"
    # nested activity captured
    assert [a.name for a in foreach.activities] == ["Exec Child"]
    assert foreach.activities[0].pipeline_refs == ["PL_Child"]
    # dependsOn ordering preserved
    merge = main.activities[2]
    assert merge.depends_on[0].activity == "For Each"
    assert merge.depends_on[0].conditions == ["Completed"]
    # pipeline→pipeline dependency derived from nested ExecutePipeline
    assert main.pipeline_dependencies == ["PL_Child"]
    # stored procedure + dataset refs captured
    lookup = main.activities[0]
    assert lookup.dataset_refs == ["DS_Sql"]
    assert "[dbo].[usp_Get]" in lookup.stored_procedures
    assert merge.linked_service_refs == ["LS_Sql"]


def test_pipeline_parameters_and_variables(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE)
    d = _run(store).discovery
    main = _by_name(d.pipelines)["PL_Main"]
    params = _by_name(main.parameters)
    assert params["GoodParam"].default_value == "hello"
    # secret-looking default dropped by the value guard
    assert params["BadParam"].default_value is None
    variables = _by_name(main.variables)
    assert variables["ContainerName"].default_value == "raw"


# --- extraction: datasets ---------------------------------------------------


def test_dataset_extraction_with_columns_and_asset(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE)
    d = _run(store).discovery
    ds = _by_name(d.datasets)
    sql = ds["DS_Sql"]
    assert sql.type == "AzureSqlTable"
    assert sql.linked_service_ref.name == "LS_Sql" and sql.linked_service_ref.resolved
    assert [(c.name, c.type) for c in sql.columns] == [("Id", "int"), ("Name", "nvarchar")]
    assert sql.asset.resource_kind == ExternalResourceKind.sql_object
    assert sql.asset.sql_schema == "Staging" and sql.asset.sql_table == "Authorities"
    # ADLS dataset: expression-driven location
    adls = ds["DS_Adls"]
    assert adls.asset.resource_kind == ExternalResourceKind.adls_path
    assert adls.asset.adls_filesystem == "raw"
    assert adls.asset.is_expression_driven is True
    assert [p.name for p in adls.parameters] == ["FileName", "FolderName"]
    # REST dataset
    rest = ds["DS_Rest"]
    assert rest.asset.resource_kind == ExternalResourceKind.rest_endpoint
    assert rest.asset.rest_relative_url == "Authorities"


# --- extraction: linked services (safe metadata only) -----------------------


def test_linked_service_safe_metadata(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE)
    d = _run(store).discovery
    ls = _by_name(d.linked_services)
    sql = ls["LS_Sql"]
    assert sql.type == "AzureSqlDatabase"
    assert sql.has_connection_string is True
    assert sql.endpoint is None  # never derived from a connection string
    assert sql.credential_present is True
    assert sql.connection_field_names == ["connectionString"]  # key NAME only
    # No server/database fields exist on the model at all
    assert not hasattr(sql, "server") and not hasattr(sql, "database")

    adls = ls["LS_Adls"]
    assert adls.endpoint == "https://adlsdemo.dfs.core.windows.net/"
    assert adls.credential_present is True
    assert len(adls.key_vault_refs) == 1
    assert adls.key_vault_refs[0].store_linked_service == "LS_Akv"
    assert adls.key_vault_refs[0].secret_name == "adls-key"

    # SAS-bearing url is dropped by the value guard
    sas = ls["LS_AdlsSas"]
    assert sas.endpoint is None

    rest = ls["LS_Rest"]
    assert rest.endpoint == "https://api.example.com/"
    assert rest.authentication_type == "Anonymous"


# --- extraction: triggers + factory -----------------------------------------


def test_trigger_and_factory_extraction(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE)
    d = _run(store).discovery
    trg = _by_name(d.triggers)["TRG"]
    assert trg.type == "ScheduleTrigger"
    assert trg.runtime_state == "Stopped"
    assert trg.pipeline_refs == ["PL_Main"]
    assert trg.recurrence.frequency == "Day" and trg.recurrence.interval == 1
    assert trg.recurrence.time_zone == "UTC"

    factory = d.factory[0]
    gp = _by_name(factory.global_parameters)
    assert gp["environment"].default_value == "dev"
    # secret-looking global parameter value dropped
    assert gp["SecretGP"].default_value is None


# --- external references + relationships + resolution -----------------------


def test_external_references(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE)
    d = _run(store).discovery
    sql_procs = {
        r.sql_stored_procedure
        for r in d.external_references
        if r.resource_kind == ExternalResourceKind.sql_object and r.sql_stored_procedure
    }
    assert {"[dbo].[usp_Get]", "[dbo].[usp_Merge]"} <= sql_procs
    kinds = {r.resource_kind for r in d.external_references}
    assert ExternalResourceKind.adls_path in kinds
    assert ExternalResourceKind.rest_endpoint in kinds
    assert ExternalResourceKind.key_vault in kinds
    accounts = {r.adls_account for r in d.external_references if r.adls_account}
    assert "adlsdemo.dfs.core.windows.net" in accounts


def test_relationships_and_resolution(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE)
    d = _run(store).discovery
    edges = {
        (e.relationship.value, e.source_name, e.target_name): e.resolved
        for e in d.relationships
    }
    assert edges[("trigger_triggers_pipeline", "TRG", "PL_Main")] is True
    assert edges[("pipeline_executes_pipeline", "PL_Main", "PL_Child")] is True
    # dangling ExecutePipeline is recorded but unresolved
    assert edges[("pipeline_executes_pipeline", "PL_Child", "PL_Missing")] is False
    assert edges[("pipeline_uses_dataset", "PL_Main", "DS_Sql")] is True
    assert edges[("dataset_uses_linked_service", "DS_Sql", "LS_Sql")] is True
    # unresolved references surfaced
    unresolved = {(r.kind, r.name) for r in d.unresolved_references}
    assert (AdfArtifactKind.pipeline, "PL_Missing") in unresolved


# --- SECRET SAFETY (the core guarantee) -------------------------------------


def test_no_secret_material_in_output(tmp_path):
    store, pid = _make_project(tmp_path, SOURCE)
    result = _run(store)
    serialized = result.discovery.model_dump_json()
    text = store.adf_semantic_discovery_path(pid).read_text(encoding="utf-8")
    for sentinel in ALL_SENTINELS:
        assert sentinel not in serialized, f"{sentinel} leaked into serialized model"
        assert sentinel not in text, f"{sentinel} leaked into persisted YAML"
    # no secret-VALUE markers (the key NAME 'connectionString' in
    # connection_field_names is allowed — it is a name, never a value).
    low = text.lower()
    for marker in ("password=", "accountkey=", "sig=", "data source=", "initial catalog="):
        assert marker not in low


def test_connection_string_value_never_read(tmp_path):
    # Static guard: the parser never reads connectionString VALUES nor parses them.
    src = inspect.getsource(parser_mod).lower()
    assert 'get("connectionstring")' not in src
    assert "data source" not in src
    assert "initial catalog" not in src
    assert ".split(" not in src.replace("value.split", "")  # no conn-string splitting


# --- status paths -----------------------------------------------------------


def test_missing_repository_discovery(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE, run_repo=False)
    result = _run(store)
    assert result.success is False
    assert result.discovery.status == AdfSemanticStatus.missing_repository_discovery


def test_no_artifacts(tmp_path):
    # A source with no ADF artifacts -> repository discovery finds no candidates.
    store, _ = _make_project(tmp_path, {"README.md": {"note": "no adf here"}}, run_repo=True)
    result = _run(store)
    assert result.success is True
    assert result.discovery.status == AdfSemanticStatus.no_artifacts
    assert result.discovery.pipeline_count == 0


def test_malformed_artifact_is_partial(tmp_path):
    store, pid = _make_project(tmp_path, SOURCE)
    # Corrupt one source file and mark its candidate 'discovered' so semantic tries it.
    (store.source_dir(pid) / "pipeline" / "PL_Main.json").write_text("{ not json", encoding="utf-8")
    repo = store.load_repository_discovery(pid)
    for c in repo.adf_candidates:
        if c.path.endswith("PL_Main.json"):
            c.parse_status = c.parse_status.__class__.discovered
    store.save_repository_discovery(pid, repo)
    result = _run(store)
    assert result.discovery.status == AdfSemanticStatus.partial
    assert result.discovery.failed_count == 1
    assert result.success is True  # partial is still a completed run
    assert "PL_Main.json" in result.discovery.detail


# --- determinism + persistence ----------------------------------------------


def test_deterministic_repeatable(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE)
    first = _run(store).discovery
    second = _run(store).discovery
    assert discoverer_mod._semantic(first) == discoverer_mod._semantic(second)
    assert second.created_at == first.created_at
    assert second.updated_at == first.updated_at


def test_persisted_round_trips(tmp_path):
    store, pid = _make_project(tmp_path, SOURCE)
    _run(store)
    loaded = store.load_adf_semantic_discovery(pid)
    assert loaded.schema_version == "adf-semantic-discovery/1"
    assert loaded.source_commit_sha == store.load(pid).repository.commit_sha
    assert len(loaded.source_commit_sha) == 40
    assert _by_name(loaded.pipelines)["PL_Main"].activity_count == 3


# --- delegation + layering + reuse ------------------------------------------


def test_discovery_agent_delegates(tmp_path):
    store, _ = _make_project(tmp_path, SOURCE, run_repo=False)
    sentinel = AdfSemanticDiscoveryResult(success=True, message="delegated")

    class Fake:
        def __init__(self):
            self.calls = []

        def run(self, request):
            self.calls.append(request)
            return sentinel

    fake = Fake()
    agent = DiscoveryAgent(store, adf_semantic_discoverer=fake)
    req = AdfSemanticDiscoveryRequest(project_name="test-proj")
    out = agent.discover_adf_semantic(req)
    assert out is sentinel and fake.calls == [req]


def test_persisted_models_only_in_project_layer():
    import adf2fabric.agent.discovery.adf_semantic.models as sem_models

    for model in (AdfSemanticDiscovery, AdfArtifactKind):
        assert model.__module__ == "adf2fabric.project.discovery"
    for name in ("AdfSemanticDiscovery", "AdfPipeline", "AdfDataset"):
        assert name not in sem_models.__dict__ or (
            getattr(sem_models, name).__module__ == "adf2fabric.project.discovery"
        )


def test_project_layer_does_not_import_agent():
    import adf2fabric.project.discovery as disc
    import adf2fabric.project.store as store_mod

    for mod in (disc, store_mod):
        assert "adf2fabric.agent" not in inspect.getsource(mod)


def test_reuses_shared_artifact_kind_no_duplicate():
    # Semantic discovery must reuse the shared AdfArtifactKind, not define its own.
    assert AdfArtifactKind.__module__ == "adf2fabric.project.discovery"
    assert "class AdfArtifactKind" not in inspect.getsource(parser_mod)
    assert "class AdfArtifactKind" not in inspect.getsource(discoverer_mod)


def test_package_imports_cleanly():
    import importlib

    for name in (
        "adf2fabric.agent.discovery.adf_semantic",
        "adf2fabric.agent.discovery.adf_semantic.parser",
        "adf2fabric.agent.discovery.adf_semantic.references",
        "adf2fabric.agent.discovery.adf_semantic.discoverer",
        "adf2fabric.agent.discovery.adf_semantic.models",
    ):
        assert importlib.import_module(name) is not None


# --- immutability: source + repository.yaml + adf.yaml untouched ------------


def _dir_fingerprint(root: Path) -> dict[str, str]:
    fp: dict[str, str] = {}
    for p in sorted(root.rglob("*")):
        rel = p.relative_to(root)
        if p.is_file() and ".git" not in rel.parts:  # ADF source only, not git internals
            fp[str(rel)] = hashlib.sha256(p.read_bytes()).hexdigest()
    return fp


def test_source_and_other_records_unchanged(tmp_path):
    store, pid = _make_project(tmp_path, SOURCE)
    source_before = _dir_fingerprint(store.source_dir(pid))
    repo_before = store.repository_discovery_path(pid).read_bytes()

    _run(store)

    assert _dir_fingerprint(store.source_dir(pid)) == source_before  # source immutable
    assert store.repository_discovery_path(pid).read_bytes() == repo_before  # repository.yaml intact
    assert store.has_adf_discovery(pid) is False  # semantic never creates adf.yaml
