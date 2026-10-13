"""Unified ADF source intake: Mode A (live Azure) and Mode B (GitHub), offline.

Mode A uses an injected fake ARM transport + token provider (no Azure, no
network, no real token). Mode B clones real local git repositories. Both modes are
then driven through the EXISTING Discovery stages to prove they feed the same
``parser_pipelines`` interface.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from adf2fabric import cli as cli_mod
from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import parser_pipelines
from adf2fabric.agent.intake import AzureIntakeRequest, IntakeAgent, IntakeRequest
from adf2fabric.agent.intake import agent as agent_mod
from adf2fabric.agent.intake.azure_source import (
    AzureSourceIntake,
    TokenUnavailable,
    redact_artifact,
)
from adf2fabric.project.source import SourceType
from adf2fabric.project.store import ProjectStore
from tests.conftest import _git, repo_head
from tests.test_intake import LocalPathProvider

SUB, RG, FACTORY = "sub-1111", "rg-demo", "adf-demo"
BASE = (
    f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}"
    f"/providers/Microsoft.DataFactory/factories/{FACTORY}"
)
TOKEN = "TOKEN_ABC_999"
SECRET_CONN = "SECRET_PW_123"
SECRET_SECURE = "SECRET_SECURE_456"


# --- ADF fixtures --------------------------------------------------------------


def ref(kind: str, name: str) -> dict:
    return {"referenceName": name, "type": f"{kind}Reference"}


def pipeline(name: str, *, dataset_in=None, dataset_out=None, child=None) -> dict:
    acts = []
    if dataset_in:
        acts.append({
            "name": f"Copy_{name}", "type": "Copy", "dependsOn": [],
            "typeProperties": {"source": {"type": "AzureSqlSource"}, "sink": {"type": "ParquetSink"}},
            "inputs": [ref("Dataset", dataset_in)], "outputs": [ref("Dataset", dataset_out)],
        })
    if child:
        acts.append({
            "name": f"Run_{child}", "type": "ExecutePipeline",
            "dependsOn": [{"activity": acts[0]["name"], "dependencyConditions": ["Succeeded"]}] if acts else [],
            "typeProperties": {"pipeline": ref("Pipeline", child), "waitOnCompletion": True},
        })
    return {"name": name, "properties": {"activities": acts, "parameters": {"p": {"type": "String"}}}}


def dataset(name: str, ls: str) -> dict:
    return {"name": name, "properties": {
        "type": "AzureSqlTable", "linkedServiceName": ref("LinkedService", ls),
        "typeProperties": {"schema": "dbo", "table": name}}}


def linked_service(name: str, secret: bool = False) -> dict:
    tp: dict = {"url": f"https://{name.lower()}.dfs.core.windows.net"}
    if secret:
        tp = {
            "connectionString": f"Server=tcp:x;Password={SECRET_CONN};",
            "password": {"type": "SecureString", "value": SECRET_SECURE},
        }
    return {"name": name, "properties": {"type": "AzureSqlDatabase" if secret else "AzureBlobFS",
                                         "typeProperties": tp}}


def trigger(name: str, pipeline_name: str) -> dict:
    return {"name": name, "properties": {
        "type": "ScheduleTrigger", "runtimeState": "Started",
        "pipelines": [{"pipelineReference": ref("Pipeline", pipeline_name)}],
        "typeProperties": {"recurrence": {"frequency": "Day", "interval": 1}}}}


def estate() -> dict[str, list[dict]]:
    """A small factory: PL_Main -> PL_Child, plus an unrelated pipeline."""
    return {
        "pipelines": [
            pipeline("PL_Main", dataset_in="DS_In", dataset_out="DS_Out", child="PL_Child"),
            pipeline("PL_Child"),
            pipeline("PL_Unrelated", dataset_in="DS_Other", dataset_out="DS_Other"),
        ],
        "datasets": [dataset("DS_In", "LS_Sql"), dataset("DS_Out", "LS_Lake"),
                     dataset("DS_Other", "LS_Other")],
        "linkedservices": [linked_service("LS_Sql", secret=True), linked_service("LS_Lake"),
                           linked_service("LS_Other")],
        "triggers": [trigger("TR_Main", "PL_Main"), trigger("TR_Other", "PL_Unrelated")],
        "dataflows": [],
        "integrationruntimes": [],
    }


def arm_item(segment: str, body: dict) -> dict:
    return {
        "id": f"/subscriptions/{SUB}/.../{body['name']}",
        "name": body["name"],
        "type": f"Microsoft.DataFactory/factories/{segment}",
        "etag": f"etag-{body['name']}",
        "properties": body["properties"],
    }


# --- fake ARM transport --------------------------------------------------------


class Resp:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._payload


class FakeArm:
    """GET-only. Routes by full URL, then by URL path. Write verbs are illegal."""

    def __init__(self, routes):
        self.routes = {k: list(v) if isinstance(v, list) else [v] for k, v in routes.items()}
        self.gets: list[str] = []
        self.auth_headers: set[str] = set()

    def get(self, url, headers=None, timeout=None):
        self.gets.append(url)
        self.auth_headers.add((headers or {}).get("Authorization", ""))
        seq = self.routes.get(url) or self.routes.get(url.split("?", 1)[0])
        if seq is None:
            return Resp(404, {"error": {"code": "ResourceNotFound"}})
        item = seq.pop(0) if len(seq) > 1 else seq[0]
        if isinstance(item, Exception):
            raise item
        return item

    def _illegal(self, *a, **k):
        raise AssertionError("write verb used against Azure")

    post = put = patch = delete = request = _illegal


def ok_routes(data=None, *, factory_json=None) -> dict:
    data = data if data is not None else estate()
    routes: dict = {
        BASE: Resp(200, factory_json or {
            "name": FACTORY, "type": "Microsoft.DataFactory/factories",
            "properties": {"globalParameters": {"env": {"type": "String", "value": "dev"}}}}),
    }
    for segment, bodies in data.items():
        routes[f"{BASE}/{segment}"] = Resp(200, {"value": [arm_item(segment, b) for b in bodies]})
    return routes


# --- harness -------------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ProjectStore(tmp_path / "projects")


@pytest.fixture
def sleeps():
    return []


def azure_agent(store, transport, sleeps, *, token_provider=None, subscription="resolved-sub"):
    azure = AzureSourceIntake(
        store,
        transport_factory=lambda: transport,
        token_provider=token_provider or (lambda cred: (TOKEN, "")),
        subscription_resolver=lambda: subscription,
        sleep=sleeps.append,
        max_retries=3,
    )
    return IntakeAgent(store, azure_intake=azure)


def azure_request(**kw) -> AzureIntakeRequest:
    kw.setdefault("project_name", "demo")
    kw.setdefault("resource_group", RG)
    kw.setdefault("factory_name", FACTORY)
    kw.setdefault("subscription_id", SUB)
    return AzureIntakeRequest(**kw)


def run_azure(store, transport, sleeps, **kw):
    return azure_agent(store, transport, sleeps).run_azure(azure_request(**kw))


def all_text(root: Path) -> str:
    parts = []
    for p in sorted(root.rglob("*")):
        if p.is_file() and ".git" not in p.parts:
            parts.append(p.read_text(encoding="utf-8", errors="ignore"))
    return "\n".join(parts)


# =============================================================================
# Mode A: live Azure Data Factory
# =============================================================================


def test_azure_retrieves_adf_layout_and_records_manifest(store, sleeps):
    transport = FakeArm(ok_routes())
    result = run_azure(store, transport, sleeps)

    assert result.success, result.error
    src = store.source_dir("demo")
    assert (src / "pipeline" / "PL_Main.json").exists()
    assert (src / "dataset" / "DS_In.json").exists()
    assert (src / "linkedService" / "LS_Sql.json").exists()
    assert (src / "trigger" / "TR_Main.json").exists()
    assert (src / "factory" / f"{FACTORY}.json").exists()

    m = store.load_source_manifest("demo")
    assert m.source_type == SourceType.azure_adf
    assert m.azure.factory_name == FACTORY and m.azure.resource_group == RG
    assert m.source_identifier.startswith(f"azure://subscriptions/{SUB}/")
    assert m.branch == "adf-live"
    assert m.retrieved_at is not None
    assert m.complete and not m.failures
    assert len([a for a in m.artifacts if a.kind == "pipeline"]) == 3
    assert {a.etag for a in m.artifacts if a.name == "PL_Main"} == {"etag-PL_Main"}
    # The local export is a verifiable commit, recorded in project + manifest.
    project = store.load("demo")
    assert project.repository.provider == "azure-adf"
    assert project.repository.commit_sha == repo_head(src) == m.version


def test_azure_source_is_preserved_not_rewritten(store, sleeps):
    run_azure(store, FakeArm(ok_routes()), sleeps)
    stored = json.loads((store.source_dir("demo") / "pipeline" / "PL_Main.json").read_text("utf-8"))
    assert stored == arm_item("pipelines", estate()["pipelines"][0])


def test_azure_source_feeds_existing_discovery(store, sleeps):
    run_azure(store, FakeArm(ok_routes()), sleeps)
    agent = DiscoveryAgent(store)
    assert agent.discover_repository(DiscoveryRequest(project_name="demo")).success
    sem = agent.discover_adf_semantic(AdfSemanticDiscoveryRequest(project_name="demo"))
    assert sem.success, sem.error
    d = sem.discovery
    assert d.pipeline_count == 3 and d.dataset_count == 3
    assert d.linked_service_count == 3 and d.trigger_count == 2 and d.factory_count == 1
    main = next(p for p in d.pipelines if p.name == "PL_Main")
    assert main.pipeline_dependencies == ["PL_Child"]
    assert [g.name for g in d.factory[0].global_parameters] == ["env"]
    assert d.unresolved_references == []


def test_azure_secrets_and_token_never_stored(store, sleeps):
    transport = FakeArm(ok_routes())
    result = run_azure(store, transport, sleeps)
    assert result.success

    text = all_text(store.project_dir("demo"))
    for secret in (SECRET_CONN, SECRET_SECURE, TOKEN):
        assert secret not in text
    assert "[REDACTED]" in (store.source_dir("demo") / "linkedService" / "LS_Sql.json").read_text("utf-8")
    # Masking is reported (where + why), never silent, and holds no value.
    m = store.load_source_manifest("demo")
    reasons = {(r.artifact_path, r.reason) for r in m.redactions}
    assert ("linkedService/LS_Sql.json", "credential_field") in reasons
    assert ("linkedService/LS_Sql.json", "secure_string") in reasons
    # The token is only ever sent to ARM, as a bearer header.
    assert transport.auth_headers == {f"Bearer {TOKEN}"}


def test_redact_artifact_leaves_key_vault_refs_and_masks_alone():
    body = {"properties": {"typeProperties": {
        "password": {"type": "AzureKeyVaultSecret", "store": ref("LinkedService", "KV"), "secretName": "pw"},
        "accountKey": "**********", "sasToken": "sv=2020&sig=abc"}}}
    out, recs = redact_artifact(body, "linkedService/X.json")
    tp = out["properties"]["typeProperties"]
    assert tp["password"]["secretName"] == "pw"  # reference, not a secret
    assert tp["accountKey"] == "**********"       # already masked: untouched
    assert tp["sasToken"] == "[REDACTED]"
    assert [r.field_path for r in recs] == ["properties.typeProperties.sasToken"]


def test_azure_only_get_requests(store, sleeps):
    transport = FakeArm(ok_routes())
    run_azure(store, transport, sleeps)  # any write verb raises AssertionError -> failure
    assert transport.gets and all(u.startswith("https://management.azure.com/") for u in transport.gets)
    assert len(transport.gets) == 8  # factory + six collections + the change data capture (adfcdcs) collection
    assert sum(u.split('?')[0].endswith('/adfcdcs') for u in transport.gets) == 1


def test_azure_pagination_followed(store, sleeps):
    data = estate()
    routes = ok_routes(data)
    page1 = [arm_item("pipelines", data["pipelines"][0])]
    page2 = [arm_item("pipelines", b) for b in data["pipelines"][1:]]
    next_url = f"{BASE}/pipelines?api-version=2018-06-01&$skiptoken=2"
    routes[f"{BASE}/pipelines"] = Resp(200, {"value": page1, "nextLink": next_url})
    routes[next_url] = Resp(200, {"value": page2})
    result = run_azure(store, FakeArm(routes), sleeps)
    assert result.success
    assert len(list((store.source_dir("demo") / "pipeline").glob("*.json"))) == 3


def test_azure_refuses_foreign_next_link(store, sleeps):
    routes = ok_routes()
    routes[f"{BASE}/pipelines"] = Resp(200, {"value": [], "nextLink": "https://evil.example.com/steal"})
    transport = FakeArm(routes)
    result = run_azure(store, transport, sleeps)
    assert not result.success
    assert all("evil.example.com" not in u for u in transport.gets)


def test_azure_throttling_is_retried_with_retry_after(store, sleeps):
    routes = ok_routes()
    routes[f"{BASE}/datasets"] = [
        Resp(429, headers={"Retry-After": "7"}),
        Resp(503),
        Resp(200, {"value": [arm_item("datasets", b) for b in estate()["datasets"]]}),
    ]
    result = run_azure(store, FakeArm(routes), sleeps)
    assert result.success, result.error
    assert sleeps == [7.0, 2.0]  # Retry-After honoured, then exponential backoff


def test_azure_throttling_exhausted_is_reported_not_skipped(store, sleeps):
    routes = ok_routes()
    routes[f"{BASE}/datasets"] = Resp(429)
    result = run_azure(store, FakeArm(routes), sleeps)
    assert not result.success
    assert "429" in result.error and "datasets" in result.error
    assert len(sleeps) == 3  # max_retries
    assert not store.source_dir("demo").exists()  # nothing half-written


@pytest.mark.parametrize("status,needle", [
    (404, "not found"), (403, "Reader"), (401, "az login"),
])
def test_azure_factory_errors_are_actionable(store, sleeps, status, needle):
    routes = ok_routes()
    routes[BASE] = Resp(status, {"error": {"code": "SomeCode", "message": "ignored"}})
    result = run_azure(store, FakeArm(routes), sleeps)
    assert not result.success
    assert needle in result.error
    if status == 404:
        assert FACTORY in result.error and RG in result.error
    assert not store.exists("demo")


def test_azure_inaccessible_collection_fails_loudly_and_keeps_old_source(store, sleeps):
    assert run_azure(store, FakeArm(ok_routes()), sleeps).success
    before = store.load("demo").repository.commit_sha

    routes = ok_routes()
    routes[f"{BASE}/linkedservices"] = Resp(403, {"error": {"code": "AuthorizationFailed"}})
    result = run_azure(store, FakeArm(routes), sleeps)

    assert not result.success
    assert "linkedservices" in result.error and "403" in result.error and "--allow-partial" in result.error
    assert store.load("demo").repository.commit_sha == before
    assert (store.source_dir("demo") / "linkedService" / "LS_Sql.json").exists()
    assert not (store.project_dir("demo") / "source.staging").exists()


def test_azure_allow_partial_records_failures(store, sleeps):
    routes = ok_routes()
    routes[f"{BASE}/linkedservices"] = Resp(403, {"error": {"code": "AuthorizationFailed"}})
    result = run_azure(store, FakeArm(routes), sleeps, allow_partial=True)

    assert result.success
    m = result.manifest
    assert not m.complete
    assert [(f.kind, f.status_code, f.error_code) for f in m.failures] == [
        ("linkedService", 403, "AuthorizationFailed")]
    assert m.failures[0].action
    codes = {f.code for f in m.findings}
    assert "retrieval_failed" in codes and "missing_reference" in codes  # datasets point at no LS


def test_azure_pipeline_selection_pulls_in_dependencies_only(store, sleeps):
    result = run_azure(store, FakeArm(ok_routes()), sleeps, pipelines=["pl_main"])  # case-insensitive
    assert result.success, result.error
    src = store.source_dir("demo")
    names = lambda d: sorted(p.stem for p in (src / d).glob("*.json"))  # noqa: E731
    assert names("pipeline") == ["PL_Child", "PL_Main"]
    assert names("dataset") == ["DS_In", "DS_Out"]
    assert names("linkedService") == ["LS_Lake", "LS_Sql"]
    assert names("trigger") == ["TR_Main"]
    assert (src / "factory" / f"{FACTORY}.json").exists()  # global parameters stay
    assert result.manifest.complete
    assert "linkedService/LS_Sql.json" in result.manifest.selection.resolved_paths
    assert all("PL_Unrelated" not in r.artifact_path for r in result.manifest.redactions)

    # Discovery sees only the selection.
    agent = DiscoveryAgent(store)
    agent.discover_repository(DiscoveryRequest(project_name="demo"))
    d = agent.discover_adf_semantic(AdfSemanticDiscoveryRequest(project_name="demo")).discovery
    assert [p.name for p in d.pipelines] == ["PL_Child", "PL_Main"]


def test_azure_unknown_pipeline_lists_available_and_stores_nothing(store, sleeps):
    result = run_azure(store, FakeArm(ok_routes()), sleeps, pipelines=["PL_Nope"])
    assert not result.success
    assert "PL_Nope" in result.error and "PL_Main" in result.error
    assert not store.exists("demo")
    assert not (store.project_dir("demo") / "source.staging").exists()


def test_azure_kind_restriction_reports_missing_references(store, sleeps):
    result = run_azure(store, FakeArm(ok_routes()), sleeps, artifact_kinds=["pipeline"])
    assert result.success
    missing = [f for f in result.manifest.findings if f.code == "missing_reference"]
    assert missing and any("dataset" in f.action for f in missing)
    assert not result.manifest.complete


def test_azure_unknown_artifact_kind_rejected(store, sleeps):
    result = run_azure(store, FakeArm(ok_routes()), sleeps, artifact_kinds=["bogus"])
    assert not result.success and "bogus" in result.error


def test_azure_item_without_name_is_a_failure(store, sleeps):
    routes = ok_routes()
    routes[f"{BASE}/triggers"] = Resp(200, {"value": [{"properties": {}}]})
    result = run_azure(store, FakeArm(routes), sleeps)
    assert not result.success and "no name" in result.error


def test_azure_subscription_defaults_to_cli_session(store, sleeps):
    transport = FakeArm({k.replace(SUB, "resolved-sub"): v for k, v in ok_routes().items()})
    result = azure_agent(store, transport, sleeps).run_azure(azure_request(subscription_id=None))
    assert result.success, result.error
    assert store.load_source_manifest("demo").azure.subscription_id == "resolved-sub"


def test_azure_no_login_session_is_actionable(store, sleeps):
    def no_token(cred):
        raise TokenUnavailable("The Azure CLI has no usable login session. Run `az login` and retry.")

    result = azure_agent(store, FakeArm(ok_routes()), sleeps, token_provider=no_token).run_azure(azure_request())
    assert not result.success and "az login" in result.error


def test_azure_network_failure_is_reported(store, sleeps):
    routes = ok_routes()
    routes[BASE] = ConnectionError(f"boom {TOKEN}")
    result = run_azure(store, FakeArm(routes), sleeps)
    assert not result.success
    assert "network" in result.error.lower() and TOKEN not in result.error


def test_azure_names_cannot_escape_the_source_dir(store, sleeps):
    data = estate()
    data["pipelines"].append(pipeline("../../evil"))
    result = run_azure(store, FakeArm(ok_routes(data)), sleeps)
    assert result.success
    assert not (store.project_dir("demo").parent / "evil.json").exists()
    assert all((store.source_dir("demo") / a.path).resolve().is_relative_to(store.source_dir("demo").resolve())
               for a in result.manifest.artifacts)


# =============================================================================
# Mode B: GitHub repository
# =============================================================================


@pytest.fixture
def local_provider(monkeypatch):
    monkeypatch.setattr(agent_mod, "get_provider", lambda name: LocalPathProvider())


@pytest.fixture
def token_env(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_TESTTOKEN123")


def make_adf_repo(path: Path, files: dict[str, object], branch: str = "main") -> Path:
    path.mkdir(parents=True, exist_ok=True)
    _git(["init", "-b", branch], path)
    _git(["config", "user.email", "t@example.com"], path)
    _git(["config", "user.name", "T"], path)
    for rel, content in files.items():
        f = path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(content if isinstance(content, str) else json.dumps(content, indent=2), encoding="utf-8")
    _git(["add", "-A"], path)
    _git(["commit", "-m", "adf"], path)
    return path


def git_layout(prefix: str = "") -> dict[str, object]:
    data = estate()
    files: dict[str, object] = {}
    for segment, folder in (("pipelines", "pipeline"), ("datasets", "dataset"),
                            ("linkedservices", "linkedService"), ("triggers", "trigger")):
        for body in data[segment]:
            files[f"{prefix}{folder}/{body['name']}.json"] = body
    files[f"{prefix}factory/{FACTORY}.json"] = {
        "name": FACTORY, "properties": {"globalParameters": {"env": {"type": "String", "value": "dev"}}}}
    return files


def run_github(store, repo, *, name="demo", **kw):
    return IntakeAgent(store).run(
        IntakeRequest(project_name=name, repository_url=str(repo), branch="main", **kw))


def test_github_manifest_and_discovery(store, tmp_path, local_provider, token_env):
    repo = make_adf_repo(tmp_path / "origin", git_layout())
    result = run_github(store, repo)

    assert result.success, result.error
    m = store.load_source_manifest("demo")
    assert m.source_type == SourceType.github
    assert m.branch == "main" and m.subdirectory is None
    assert m.version == repo_head(repo) == store.load("demo").repository.commit_sha
    assert m.complete and not m.findings
    assert len(m.artifacts) == 3 + 3 + 3 + 2 + 1

    agent = DiscoveryAgent(store)
    assert agent.discover_repository(DiscoveryRequest(project_name="demo")).success
    d = agent.discover_adf_semantic(AdfSemanticDiscoveryRequest(project_name="demo")).discovery
    assert d.pipeline_count == 3 and d.unresolved_references == []


def test_github_subdirectory_is_the_discovery_root(store, tmp_path, local_provider, token_env):
    files = git_layout(prefix="adf/")
    files["README.md"] = "docs"
    files["other/pipeline/Stray.json"] = pipeline("Stray")  # outside the subdirectory
    repo = make_adf_repo(tmp_path / "origin", files)
    result = run_github(store, repo, subdirectory="adf")

    assert result.success, result.error
    assert result.manifest.subdirectory == "adf"
    assert store.source_root("demo") == store.source_dir("demo") / "adf"
    agent = DiscoveryAgent(store)
    repo_d = agent.discover_repository(DiscoveryRequest(project_name="demo")).discovery
    assert all(c.path.startswith(("pipeline/", "dataset/", "linkedService/", "trigger/", "factory/"))
               for c in repo_d.adf_candidates)
    d = agent.discover_adf_semantic(AdfSemanticDiscoveryRequest(project_name="demo")).discovery
    assert sorted(p.name for p in d.pipelines) == ["PL_Child", "PL_Main", "PL_Unrelated"]


@pytest.mark.parametrize("sub", ["nope", "../escape", "/abs"])
def test_github_bad_subdirectory_is_rejected(store, tmp_path, local_provider, token_env, sub):
    repo = make_adf_repo(tmp_path / "origin", git_layout())
    result = run_github(store, repo, subdirectory=sub)
    assert not result.success
    assert "ubdirectory" in result.error


def test_github_pipeline_selection(store, tmp_path, local_provider, token_env):
    repo = make_adf_repo(tmp_path / "origin", git_layout())
    result = run_github(store, repo, pipelines=["PL_Main"])
    assert result.success, result.error
    assert result.manifest.complete
    paths = set(result.manifest.selection.resolved_paths)
    assert "pipeline/PL_Child.json" in paths and "trigger/TR_Main.json" in paths
    assert "pipeline/PL_Unrelated.json" not in paths

    agent = DiscoveryAgent(store)
    agent.discover_repository(DiscoveryRequest(project_name="demo"))
    d = agent.discover_adf_semantic(AdfSemanticDiscoveryRequest(project_name="demo")).discovery
    assert [p.name for p in d.pipelines] == ["PL_Child", "PL_Main"]
    assert [t.name for t in d.triggers] == ["TR_Main"]


def test_github_unknown_pipeline_is_actionable(store, tmp_path, local_provider, token_env):
    repo = make_adf_repo(tmp_path / "origin", git_layout())
    result = run_github(store, repo, pipelines=["PL_Nope"])
    assert not result.success and "PL_Nope" in result.error and "PL_Main" in result.error


def test_github_integrity_findings(store, tmp_path, local_provider, token_env):
    files = git_layout()
    files["pipeline/pl_main_copy.json"] = pipeline("pl_main")           # duplicate (case-insens.)
    files["pipeline/Broken.json"] = "{ not json"                         # invalid JSON
    files["pipeline/PL_Dangling.json"] = pipeline("PL_Dangling", child="PL_Ghost")  # missing ref
    repo = make_adf_repo(tmp_path / "origin", files)
    result = run_github(store, repo)

    assert result.success  # the source IS retrieved; the problems are reported, not hidden
    m = result.manifest
    assert not m.complete
    by_code = {f.code: f for f in m.findings}
    assert {"duplicate_artifact", "invalid_json", "missing_reference"} <= set(by_code)
    assert "pipeline/Broken.json" in by_code["invalid_json"].message
    assert "PL_Ghost" in by_code["missing_reference"].message
    assert all(f.action for f in m.findings)


def test_github_without_adf_folders_is_flagged(store, tmp_path, local_provider, token_env):
    repo = make_adf_repo(tmp_path / "origin", {"README.md": "hi"})
    m = run_github(store, repo).manifest
    assert [f.code for f in m.findings] == ["no_adf_artifacts"] and not m.complete


def test_github_arm_template_only_is_flagged(store, tmp_path, local_provider, token_env):
    repo = make_adf_repo(tmp_path / "origin", {"ARMTemplateForFactory.json": {"resources": []}})
    m = run_github(store, repo).manifest
    assert [f.code for f in m.findings] == ["arm_template_only"]
    assert m.complete  # a warning, not an error


def test_github_reuse_still_records_manifest(store, tmp_path, local_provider, token_env):
    repo = make_adf_repo(tmp_path / "origin", git_layout())
    run_github(store, repo)
    again = run_github(store, repo)
    assert again.success and again.reused and again.manifest.version == repo_head(repo)


# =============================================================================
# Both modes -> one parser interface
# =============================================================================


def test_both_sources_parse_identically_through_parser_pipelines(store, tmp_path, sleeps, local_provider, token_env):
    run_azure(store, FakeArm(ok_routes()), sleeps)
    gh_store = ProjectStore(tmp_path / "gh-projects")
    run_github(gh_store, make_adf_repo(tmp_path / "origin", git_layout()))

    for name in ("PL_Main", "PL_Child", "PL_Unrelated"):
        rel = f"pipeline/{name}.json"
        az = parser_pipelines.parse_pipeline_file(store.source_root("demo"), rel)
        gh = parser_pipelines.parse_pipeline_file(gh_store.source_root("demo"), rel)
        assert az == gh
        assert az.name == name


def test_azure_arm_item_and_git_item_both_accepted_by_parser():
    body = pipeline("PL_Main", dataset_in="DS_In", dataset_out="DS_Out", child="PL_Child")
    from_git = parser_pipelines.parse_pipeline(body, "pipeline/PL_Main.json")
    from_arm = parser_pipelines.parse_pipeline(arm_item("pipelines", body), "pipeline/PL_Main.json")
    assert from_git == from_arm and from_git.activity_count == 2


# =============================================================================
# CLI wiring
# =============================================================================


def test_cli_requires_exactly_one_source():
    runner = CliRunner()
    neither = runner.invoke(cli_mod.cli, ["intake", "--project", "x"])
    both = runner.invoke(cli_mod.cli, ["intake", "--project", "x", "--repo", "r", "--factory", "f"])
    assert neither.exit_code != 0 and "exactly one source" in neither.output
    assert both.exit_code != 0 and "exactly one source" in both.output


def test_cli_rejects_cross_mode_options():
    runner = CliRunner()
    a = runner.invoke(cli_mod.cli, ["intake", "--project", "x", "--factory", "f"])
    b = runner.invoke(cli_mod.cli, ["intake", "--project", "x", "--factory", "f", "--resource-group", "g",
                                    "--branch", "main"])
    c = runner.invoke(cli_mod.cli, ["intake", "--project", "x", "--repo", "r", "--resource-group", "g"])
    assert "--resource-group is required" in a.output
    assert "--repo only" in b.output
    assert "--factory only" in c.output


def test_cli_azure_end_to_end(tmp_path, monkeypatch, sleeps):
    projects = tmp_path / "projects"
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: projects)
    store = ProjectStore(projects)
    transport = FakeArm(ok_routes())
    azure = AzureSourceIntake(
        store, transport_factory=lambda: transport, token_provider=lambda c: (TOKEN, ""),
        sleep=sleeps.append)
    real = cli_mod.IntakeAgent
    monkeypatch.setattr(cli_mod, "IntakeAgent",
                        lambda s, token_env=None: real(s, token_env=token_env, azure_intake=azure))

    out = CliRunner().invoke(cli_mod.cli, [
        "intake", "--project", "demo", "--factory", FACTORY, "--resource-group", RG,
        "--subscription", SUB, "--pipeline", "PL_Main"])
    assert out.exit_code == 0, out.output
    assert "azure-adf" in out.output and "manifest:" in out.output and "redacted:" in out.output
    assert TOKEN not in out.output and SECRET_CONN not in out.output
    yaml.safe_load(store.source_manifest_path("demo").read_text("utf-8"))


# --- change data capture (adfcdcs): a tolerant extra collection -----------------------------------------------


def _cdc_item(name="CDC_One"):
    return {"id": f"/subscriptions/{SUB}/.../{name}", "name": name, "type": "Microsoft.DataFactory/factories/adfcdcs",
            "etag": "e1", "properties": {"policy": {"mode": "Microbatch"}, "sourceConnectionsInfo": [],
                                          "targetConnectionsInfo": []}}


def test_azure_intake_retrieves_cdc_resources_into_their_own_folder(store, sleeps):
    routes = ok_routes()
    routes[f"{BASE}/adfcdcs"] = Resp(200, {"value": [_cdc_item()]})
    result = run_azure(store, FakeArm(routes), sleeps)
    assert result.success
    assert (store.source_dir("demo") / "changeDataCapture" / "CDC_One.json").exists()
    assert any(a.kind == "changeDataCapture" and a.path.endswith("CDC_One.json") for a in result.manifest.artifacts)


def test_a_factory_without_cdc_support_is_not_a_failure(store, sleeps):
    for status in (404, 400):
        routes = ok_routes()
        routes[f"{BASE}/adfcdcs"] = Resp(status, {"error": {"code": "ResourceNotFound"}})
        result = run_azure(store, FakeArm(routes), sleeps)
        assert result.success and not result.manifest.failures
        assert not (store.source_dir("demo") / "changeDataCapture").exists()


def test_a_real_cdc_retrieval_failure_is_reported_not_swallowed(store, sleeps):
    routes = ok_routes()
    routes[f"{BASE}/adfcdcs"] = Resp(403, {"error": {"code": "AuthorizationFailed"}})
    result = run_azure(store, FakeArm(routes), sleeps)
    assert not result.success and "adfcdcs" in (result.error or "")


def test_cdc_is_not_requested_when_the_artifact_kinds_are_restricted(store, sleeps):
    transport = FakeArm(ok_routes())
    result = run_azure(store, transport, sleeps, artifact_kinds=["pipeline"])
    assert result.success and not any(u.split("?")[0].endswith("/adfcdcs") for u in transport.gets)
    wanted = FakeArm(ok_routes())
    run_azure(store, wanted, sleeps, artifact_kinds=["pipeline", "changeDataCapture"])
    assert any(u.split("?")[0].endswith("/adfcdcs") for u in wanted.gets)
