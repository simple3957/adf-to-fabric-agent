"""Repository Discovery tests — fully offline and deterministic.

Real local git repositories are built in ``tmp_path`` (no network), and the
DiscoveryAgent inspects them read-only. These tests assert the factual-inventory
behavior and, importantly, the boundaries: no migration interpretation, no
network/Azure/SQL/ADLS/Fabric access, no repository-file execution, and no
mutation of the source.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import subprocess
from pathlib import Path

import pytest

import adf2fabric.agent.discovery.agent as agent_mod
import adf2fabric.agent.discovery.repository.artifacts as artifacts_mod
import adf2fabric.agent.discovery.repository.discoverer as repo_discoverer_mod
import adf2fabric.agent.discovery.repository.hashing as hashing_mod
import adf2fabric.agent.discovery.repository.scanner as scanner_mod
from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.project.discovery import (
    AdfArtifactKind,
    AdfCandidate,
    DiscoveryStatus,
    FileRecord,
    KindEvidence,
    RepositoryDiscovery,
)
from adf2fabric.project.models import Project, RepositoryMetadata
from adf2fabric.project.store import ProjectStore


# --- helpers ----------------------------------------------------------------


def _git(args: list[str], cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _make_git_source(source: Path, files: dict[str, str], *, allow_empty: bool = False) -> str:
    source.mkdir(parents=True, exist_ok=True)
    _git(["init", "-b", "main"], source)
    _git(["config", "user.email", "test@example.com"], source)
    _git(["config", "user.name", "Test"], source)
    for rel, content in files.items():
        p = source / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    if files:
        _git(["add", "-A"], source)
        _git(["commit", "-m", "initial"], source)
    elif allow_empty:
        _git(["commit", "--allow-empty", "-m", "empty"], source)
    return subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def _setup(
    tmp_path: Path,
    files: dict[str, str],
    *,
    allow_empty: bool = False,
    commit_override: str | None = None,
    create_source: bool = True,
) -> tuple[ProjectStore, str]:
    store = ProjectStore(tmp_path / "projects")
    project_id = "test-proj"
    source = store.source_dir(project_id)
    head = "0" * 40
    if create_source:
        head = _make_git_source(source, files, allow_empty=allow_empty)
    meta = RepositoryMetadata(
        provider="github",
        repository_url="https://example.invalid/x.git",
        branch="main",
        commit_sha=commit_override or head,
        local_source_path=str(source),
    )
    store.save(Project(id=project_id, name="Test Proj", repository=meta))
    return store, "test-proj"


def _run(store: ProjectStore, project_id: str):
    # Route through the phase orchestrator's repository delegation.
    return DiscoveryAgent(store).discover_repository(
        DiscoveryRequest(project_name=project_id)
    )


ADF_FILES: dict[str, str] = {
    "pipeline/PL_One.json": json.dumps(
        {
            "name": "PL_One",
            "properties": {
                "activities": [{"name": "a1"}, {"name": "a2"}],
                "parameters": {"p1": {"type": "String"}},
                "variables": {"v1": {"type": "String"}},
                "annotations": ["tag"],
            },
        }
    ),
    "pipeline/PL_Two.json": json.dumps({"name": "PL_Two", "properties": {"activities": []}}),
    "dataset/DS_One.json": json.dumps(
        {
            "name": "DS_One",
            "properties": {
                "type": "AzureSqlTable",
                "linkedServiceName": {
                    "referenceName": "LS_One",
                    "type": "LinkedServiceReference",
                },
                "schema": [],
                "parameters": {"dsp": {"type": "String"}},
            },
        }
    ),
    "linkedService/LS_One.json": json.dumps(
        {
            "name": "LS_One",
            "type": "Microsoft.DataFactory/factories/linkedservices",
            "properties": {
                "type": "AzureSqlDatabase",
                "typeProperties": {"connectionString": "Server=x;Password=SUPERSECRET;"},
            },
        }
    ),
    "trigger/TRG_One.json": json.dumps(
        {"name": "TRG_One", "properties": {"type": "ScheduleTrigger"}}
    ),
    "factory/adf-demo.json": json.dumps(
        {"name": "adf-demo", "properties": {"globalParameters": {}}, "location": "northeurope"}
    ),
    "ARMTemplateForFactory.json": json.dumps({"resources": []}),
    "arm-template-parameters-definition.json": json.dumps(
        {"Microsoft.DataFactory/factories/pipelines": {}}
    ),
    "azure-pipelines.yml": "trigger: none\n",
    "build/package.json": json.dumps({"name": "build"}),
    "scripts/deploy.ps1": "Write-Host 'hi'\n",
    "README.md": "# demo\n",
    "images/pl.png": "PNGDATA",
    "nested/deep/leaf.json": json.dumps({"foo": "bar"}),
}


def _by_path(discovery: RepositoryDiscovery) -> dict:
    return {c.path: c for c in discovery.adf_candidates}


# --- 1. empty repository ----------------------------------------------------


def test_empty_repository(tmp_path):
    store, pid = _setup(tmp_path, {}, allow_empty=True)
    result = _run(store, pid)
    assert result.success
    d = result.discovery
    assert d.structure.total_files == 0
    assert d.structure.total_directories == 0
    assert d.adf_candidates == []
    assert d.artifact_kind_counts == {}
    assert d.complete is True


# --- 2. normal repository + 7. candidates -----------------------------------


def test_normal_repository_candidates(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    d = _run(store, pid).discovery
    by = _by_path(d)

    assert by["pipeline/PL_One.json"].artifact_kind == AdfArtifactKind.pipeline
    assert by["pipeline/PL_One.json"].kind_evidence == KindEvidence.directory_name
    assert by["pipeline/PL_One.json"].activity_count == 2
    assert by["pipeline/PL_Two.json"].activity_count == 0
    assert by["dataset/DS_One.json"].artifact_kind == AdfArtifactKind.dataset
    assert by["trigger/TRG_One.json"].artifact_kind == AdfArtifactKind.trigger
    assert by["factory/adf-demo.json"].artifact_kind == AdfArtifactKind.factory

    ls = by["linkedService/LS_One.json"]
    assert ls.artifact_kind == AdfArtifactKind.linked_service
    assert ls.kind_evidence == KindEvidence.top_level_type_field
    assert ls.resource_type == "Microsoft.DataFactory/factories/linkedservices"

    # non-artifacts are inventoried as files but never candidates
    assert "build/package.json" not in by
    assert "nested/deep/leaf.json" not in by
    assert "README.md" not in by

    assert d.artifact_kind_counts == {
        "armTemplate": 1,
        "armTemplateParameters": 1,
        "dataset": 1,
        "factory": 1,
        "linkedService": 1,
        "pipeline": 2,
        "trigger": 1,
    }


# --- 3. nested directories --------------------------------------------------


def test_nested_directories(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    d = _run(store, pid).discovery
    assert "nested" in d.structure.directories
    assert "nested/deep" in d.structure.directories
    # excludes .git entirely
    assert all(not x.startswith(".git") for x in d.structure.directories)
    assert d.structure.excluded_paths == [".git/"]


# --- 4. deterministic ordering ----------------------------------------------


def test_deterministic_ordering(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    d = _run(store, pid).discovery
    assert [f.path for f in d.files] == sorted(f.path for f in d.files)
    assert [c.path for c in d.adf_candidates] == sorted(c.path for c in d.adf_candidates)
    assert list(d.structure.extension_counts) == sorted(d.structure.extension_counts)
    assert list(d.artifact_kind_counts) == sorted(d.artifact_kind_counts)


# --- 5. deterministic hashing -----------------------------------------------


def test_deterministic_hashing(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    d = _run(store, pid).discovery
    rec = next(f for f in d.files if f.path == "README.md")
    # Hash the actual on-disk bytes (avoids platform newline-translation surprises)
    actual_bytes = (store.source_dir(pid) / "README.md").read_bytes()
    assert rec.sha256 == hashlib.sha256(actual_bytes).hexdigest()


# --- 6. extension inventory -------------------------------------------------


def test_extension_inventory(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    d = _run(store, pid).discovery
    ec = d.structure.extension_counts
    # 10 .json files across the tree
    assert ec[".json"] == 10
    assert ec[".yml"] == 1
    assert ec[".ps1"] == 1
    assert ec[".md"] == 1
    assert ec[".png"] == 1


# --- 8 & 9. ARM template / parameter identification -------------------------


def test_arm_identification(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    by = _by_path(_run(store, pid).discovery)
    arm = by["ARMTemplateForFactory.json"]
    assert arm.artifact_kind == AdfArtifactKind.arm_template
    assert arm.kind_evidence == KindEvidence.file_name
    params = by["arm-template-parameters-definition.json"]
    assert params.artifact_kind == AdfArtifactKind.arm_template_parameters
    assert params.kind_evidence == KindEvidence.file_name


# --- 10. commit mismatch -> HARD STOP ---------------------------------------


def test_commit_mismatch_hard_stop(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES, commit_override="deadbeef" * 5)
    result = _run(store, pid)
    assert result.success is False
    assert "mismatch" in result.error.lower()
    assert not store.has_repository_discovery(pid)


# --- 11. missing source directory -------------------------------------------


def test_missing_source_dir(tmp_path):
    store, pid = _setup(tmp_path, {}, create_source=False)
    result = _run(store, pid)
    assert result.success is False
    assert "source directory not found" in result.error.lower()


# --- 12. unreadable file behavior -------------------------------------------


def test_unreadable_file(tmp_path, monkeypatch):
    store, pid = _setup(tmp_path, ADF_FILES)
    real = hashing_mod.sha256_file

    def flaky(path: Path):
        if path.name == "README.md":
            raise PermissionError("denied")
        return real(path)

    monkeypatch.setattr(hashing_mod, "sha256_file", flaky)
    d = _run(store, pid).discovery
    rec = next(f for f in d.files if f.path == "README.md")
    assert rec.status == DiscoveryStatus.failed
    assert rec.sha256 is None
    assert d.complete is False
    assert d.failed_count >= 1


# --- 13. malformed candidate JSON -------------------------------------------


def test_malformed_candidate_json(tmp_path):
    files = dict(ADF_FILES)
    files["pipeline/PL_Bad.json"] = "{ this is : not valid json "
    store, pid = _setup(tmp_path, files)
    d = _run(store, pid).discovery
    by = _by_path(d)
    bad = by["pipeline/PL_Bad.json"]
    # still a candidate (directory evidence), but structural parse failed
    assert bad.artifact_kind == AdfArtifactKind.pipeline
    assert bad.kind_evidence == KindEvidence.directory_name
    assert bad.parse_status == DiscoveryStatus.failed
    assert "unparseable" in bad.note.lower()
    # the file itself is still inventoried with a hash
    frec = next(f for f in d.files if f.path == "pipeline/PL_Bad.json")
    assert frec.status == DiscoveryStatus.discovered
    assert frec.sha256 is not None


# --- 14. secret redaction ---------------------------------------------------


def test_secret_redaction(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    d = _run(store, pid).discovery
    ls = _by_path(d)["linkedService/LS_One.json"]
    assert ls.credential_present is True
    # the secret value never appears anywhere in the serialized output
    serialized = d.model_dump_json()
    assert "SUPERSECRET" not in serialized
    # and the persisted YAML is likewise clean
    path = store.repository_discovery_path(pid)
    assert "SUPERSECRET" not in path.read_text(encoding="utf-8")


# --- 15. repository remains unchanged ---------------------------------------


def _tree_fingerprint(root: Path) -> list[tuple[str, str]]:
    out = []
    for p in sorted(root.rglob("*")):
        if p.is_file() and ".git" not in p.parts:
            out.append((p.relative_to(root).as_posix(), hashlib.sha256(p.read_bytes()).hexdigest()))
    return out


def test_source_unchanged(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    source = store.source_dir(pid)
    before = _tree_fingerprint(source)
    _run(store, pid)
    after = _tree_fingerprint(source)
    assert before == after


# --- 16. no execution of repository files -----------------------------------


def test_no_repo_file_execution(tmp_path):
    sentinel = tmp_path / "SENTINEL_SHOULD_NOT_EXIST"
    files = dict(ADF_FILES)
    files["scripts/evil.py"] = f"open(r'{sentinel}', 'w').write('executed')\n"
    store, pid = _setup(tmp_path, files)
    result = _run(store, pid)
    assert result.success
    assert not sentinel.exists()


# --- 18. repeat discovery is identical --------------------------------------


def test_repeat_discovery_identical(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    first = _run(store, pid).discovery
    second = _run(store, pid).discovery
    assert repo_discoverer_mod._semantic(first) == repo_discoverer_mod._semantic(second)
    # created_at preserved; unchanged content leaves updated_at untouched
    assert second.created_at == first.created_at
    assert second.updated_at == first.updated_at


# --- boundary: no forbidden imports / no assessment fields ------------------


def test_no_network_or_platform_imports():
    # Repository-domain modules must stay fully offline / no platform SDKs.
    for mod in (repo_discoverer_mod, scanner_mod, artifacts_mod, hashing_mod):
        src = inspect.getsource(mod)
        for forbidden in ("requests", "azure", "pyodbc", "socket", "connectivity", "urllib"):
            assert forbidden not in src, f"{mod.__name__} must not reference {forbidden!r}"


def test_phase_agent_is_orchestration_only():
    """DiscoveryAgent delegates; it holds no traversal/hashing/SDK/extraction logic."""
    src = inspect.getsource(agent_mod)
    for forbidden in (
        "os.walk", "hashlib", "DataLakeServiceClient", "list_file_systems",
        "json.load", "scan_repository", "extract_candidates", "get_service_properties",
    ):
        assert forbidden not in src, f"phase agent must not contain {forbidden!r}"


def test_phase_agent_delegates_to_injected_discoverers():
    class _FakeDiscoverer:
        def __init__(self):
            self.calls = []

        def run(self, request):
            self.calls.append(request)
            return f"handled:{request}"

    repo, adls = _FakeDiscoverer(), _FakeDiscoverer()
    agent = DiscoveryAgent(
        store=None, repository_discoverer=repo, adls_discoverer=adls
    )
    assert agent.discover_repository("REQ_R") == "handled:REQ_R"
    assert agent.discover_adls("REQ_A") == "handled:REQ_A"
    assert repo.calls == ["REQ_R"] and adls.calls == ["REQ_A"]


def test_no_assessment_fields():
    field_names = set()
    field_names.update(RepositoryDiscovery.model_fields)
    field_names.update(AdfCandidate.model_fields)
    field_names.update(FileRecord.model_fields)
    forbidden = {
        "compatible", "incompatible", "supported", "unsupported",
        "migration_ready", "migration_complexity", "complexity",
        "migration_risk", "risk", "effort", "target_mapping",
        "recommended_action", "recommendation",
    }
    assert field_names.isdisjoint(forbidden)


# --- artifact-level individual inventory -------------------------------------


def test_every_artifact_individually_represented(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    d = _run(store, pid).discovery
    from collections import Counter

    kinds = Counter(c.artifact_kind for c in d.adf_candidates)
    assert kinds[AdfArtifactKind.pipeline] == 2
    assert kinds[AdfArtifactKind.dataset] == 1
    assert kinds[AdfArtifactKind.linked_service] == 1
    assert kinds[AdfArtifactKind.trigger] == 1
    assert kinds[AdfArtifactKind.factory] == 1
    # every candidate carries its own path (individual inventory item)
    assert all(c.path for c in d.adf_candidates)
    # exactly one inventory list (no parallel per-kind collection)
    assert not hasattr(d, "pipelines")


def test_name_extraction(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    by = _by_path(_run(store, pid).discovery)
    assert by["pipeline/PL_One.json"].name == "PL_One"
    assert by["pipeline/PL_Two.json"].name == "PL_Two"
    assert by["dataset/DS_One.json"].name == "DS_One"
    assert by["linkedService/LS_One.json"].name == "LS_One"
    assert by["trigger/TRG_One.json"].name == "TRG_One"
    assert by["factory/adf-demo.json"].name == "adf-demo"
    # ARM files carry no deterministic artifact name -> null (never guessed)
    assert by["ARMTemplateForFactory.json"].name is None
    assert by["arm-template-parameters-definition.json"].name is None


def test_pipeline_structural_facts(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    p = _by_path(_run(store, pid).discovery)["pipeline/PL_One.json"]
    assert p.activity_count == 2
    assert p.has_parameters is True
    assert p.parameter_names == ["p1"]
    assert p.has_variables is True
    assert p.has_annotations is True
    assert p.connection_field_names == []  # not a linked service


def test_dataset_structural_facts(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    ds = _by_path(_run(store, pid).discovery)["dataset/DS_One.json"]
    assert ds.structural_type == "AzureSqlTable"
    assert ds.has_schema is True
    assert ds.linked_service_ref_present is True  # presence only, not the target
    assert ds.has_parameters is True
    assert ds.parameter_names == ["dsp"]


def test_linked_service_structural_facts(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    ls = _by_path(_run(store, pid).discovery)["linkedService/LS_One.json"]
    assert ls.structural_type == "AzureSqlDatabase"
    assert ls.resource_type == "Microsoft.DataFactory/factories/linkedservices"
    assert ls.connection_field_names == ["connectionString"]  # key NAME only
    assert ls.credential_present is True


def test_trigger_structural_facts(tmp_path):
    store, pid = _setup(tmp_path, ADF_FILES)
    trg = _by_path(_run(store, pid).discovery)["trigger/TRG_One.json"]
    assert trg.structural_type == "ScheduleTrigger"
