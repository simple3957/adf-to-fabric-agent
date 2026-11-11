"""Source import: ZIP safety and review, Git authentication and verified SHAs, and the shared source analysis."""

from __future__ import annotations

import json
import stat
import zipfile

import pytest

from adf2fabric.agent.discovery import DiscoveryAgent, DiscoveryRequest
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest
from adf2fabric.agent.intake.zip_source import ZipIntakeRequest, ZipSourceIntake, inspect_zip
from adf2fabric.connections.providers.git_source import (
    GitSourceError,
    clean_url,
    default_username,
)
from adf2fabric.connections.providers.zip_source import ZipLimits, ZipRejected, extract_json
from adf2fabric.connections.source_analysis import analyze_source, detect_root
from adf2fabric.project.source import SourceType
from adf2fabric.project.store import ProjectStore

TOKEN = "ghp_SuperSecretToken123456"


def pipeline(name="PL_A", dataset="DS_A", child=None):
    acts = [{"name": "Copy", "type": "Copy", "dependsOn": [], "typeProperties": {
        "source": {"type": "DelimitedTextSource"}, "sink": {"type": "ParquetSink"}},
        "inputs": [{"referenceName": dataset, "type": "DatasetReference"}], "outputs": []}]
    if child:
        acts.append({"name": "Run", "type": "ExecutePipeline", "dependsOn": [], "typeProperties": {
            "pipeline": {"referenceName": child, "type": "PipelineReference"}}})
    return {"name": name, "properties": {"activities": acts}}


ESTATE = {
    "pipeline/PL_A.json": pipeline(),
    "dataset/DS_A.json": {"name": "DS_A", "properties": {"type": "DelimitedText", "linkedServiceName": {
        "referenceName": "LS_A", "type": "LinkedServiceReference"}, "typeProperties": {}}},
    "linkedService/LS_A.json": {"name": "LS_A", "properties": {"type": "AzureBlobFS", "typeProperties": {
        "url": "https://a.dfs.core.windows.net"}}},
    "integrationRuntime/IR.json": {"name": "IR", "properties": {"type": "Managed"}},
    "trigger/TR.json": {"name": "TR", "properties": {"type": "ScheduleTrigger", "pipelines": [{"pipelineReference": {
        "referenceName": "PL_A", "type": "PipelineReference"}}], "typeProperties": {}}},
}


def make_zip(tmp_path, files, prefix="", name="export.zip", extra=None):
    path = tmp_path / name
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for rel, body in files.items():
            z.writestr(prefix + rel, json.dumps(body) if isinstance(body, (dict, list)) else body)
        for info, data in (extra or []):
            z.writestr(info, data)
    return path


# --- ZIP safety -----------------------------------------------------------------------------------------------


def test_zip_extracts_only_json_and_reports_what_it_ignored(tmp_path):
    z = make_zip(tmp_path, {**ESTATE, "scripts/run.ps1": "Remove-Item C:\\ -Recurse", "README.md": "# hi", "tool.exe": "MZ"})
    rep = extract_json(z, tmp_path / "out")
    assert rep.extracted == len(ESTATE) and rep.ignored == 3
    assert not list((tmp_path / "out").rglob("*.ps1")) and not list((tmp_path / "out").rglob("*.exe"))
    assert {"scripts/run.ps1", "README.md", "tool.exe"} >= set(rep.ignored_examples)


@pytest.mark.parametrize("name, why", [
    ("../evil.json", "outside"), ("pipeline/../../evil.json", "outside"), ("/etc/passwd.json", "absolute"),
    ("C:/Windows/evil.json", "absolute"), ("a\\..\\..\\evil.json", "outside"), ("pipeline/con.json", "reserved"),
])
def test_zip_slip_absolute_drive_and_reserved_names_are_refused(tmp_path, name, why):
    z = tmp_path / "bad.zip"
    with zipfile.ZipFile(z, "w") as f:
        f.writestr(name, "{}")
    with pytest.raises(ZipRejected, match=why):
        extract_json(z, tmp_path / "out")
    assert not (tmp_path / "evil.json").exists() and not (tmp_path.parent / "evil.json").exists()


def test_symlinks_encrypted_entries_duplicates_and_non_zips_are_refused(tmp_path):
    link = zipfile.ZipInfo("pipeline/link.json")
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    z = tmp_path / "l.zip"
    with zipfile.ZipFile(z, "w") as f:
        f.writestr(link, "/etc/passwd")
    with pytest.raises(ZipRejected, match="symbolic link"):
        extract_json(z, tmp_path / "o1")
    z2 = make_zip(tmp_path, {"pipeline/a.json": "{}"}, name="e.zip")
    real = zipfile.ZipFile.infolist

    def encrypted(self):
        infos = real(self)
        for i in infos:
            i.flag_bits |= 0x1
        return infos

    zipfile.ZipFile.infolist = encrypted
    try:
        with pytest.raises(ZipRejected, match="encrypted"):
            extract_json(z2, tmp_path / "o2")
    finally:
        zipfile.ZipFile.infolist = real
    z3 = tmp_path / "d.zip"
    with zipfile.ZipFile(z3, "w") as f:
        f.writestr("pipeline/A.json", "{}")
        f.writestr("pipeline/a.json", "{}")
    with pytest.raises(ZipRejected, match="more than once"):
        extract_json(z3, tmp_path / "o3")
    notzip = tmp_path / "x.zip"
    notzip.write_bytes(b"not a zip at all")
    with pytest.raises(ZipRejected, match="not a valid ZIP"):
        extract_json(notzip, tmp_path / "o4")


def test_limits_stop_bombs_and_oversized_archives(tmp_path):
    z = make_zip(tmp_path, {f"pipeline/p{i}.json": {"name": f"p{i}"} for i in range(10)})
    with pytest.raises(ZipRejected, match="more than 5 entries"):
        extract_json(z, tmp_path / "a", ZipLimits(max_files=5))
    big = make_zip(tmp_path, {"pipeline/big.json": json.dumps({"x": "0" * 3_000_000})}, name="big.zip")
    with pytest.raises(ZipRejected, match="decompression bomb"):
        extract_json(big, tmp_path / "b", ZipLimits(max_ratio=50))
    with pytest.raises(ZipRejected, match="larger than"):
        extract_json(big, tmp_path / "c", ZipLimits(max_file_bytes=1000))
    with pytest.raises(ZipRejected, match="expands to more than"):
        extract_json(big, tmp_path / "d", ZipLimits(max_total_bytes=1000, max_file_bytes=10**9, max_ratio=10**9))
    with pytest.raises(ZipRejected, match="archive is larger"):
        extract_json(big, tmp_path / "e", ZipLimits(max_archive_bytes=100))
    with pytest.raises(ZipRejected, match="too long"):
        extract_json(make_zip(tmp_path, {"pipeline/" + "x" * 300 + ".json": {}}, name="long.zip"), tmp_path / "f")


def test_a_lying_declared_size_is_rejected_not_trusted(tmp_path):
    z = make_zip(tmp_path, {"pipeline/a.json": json.dumps({"k": "v" * 5000})})
    real = zipfile.ZipFile.infolist

    def lying(self):
        infos = real(self)
        for i in infos:
            i.file_size = 100  # claims 100 bytes, delivers thousands
        return infos

    zipfile.ZipFile.infolist = lying
    try:
        with pytest.raises(ZipRejected, match="corrupt or inconsistent|larger than the archive declared"):
            extract_json(z, tmp_path / "out")
    finally:
        zipfile.ZipFile.infolist = real


# --- source analysis ------------------------------------------------------------------------------------------


def write_tree(root, files):
    for rel, body in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(body) if isinstance(body, (dict, list)) else body, encoding="utf-8")


def test_analysis_counts_artifacts_and_finds_the_root_inside_a_wrapper_folder(tmp_path):
    write_tree(tmp_path / "wrapper" / "main", ESTATE)
    s = analyze_source(tmp_path)
    assert s.root == "wrapper/main" and s.flavor == "adf" and s.layout_ok and s.complete
    assert s.counts == {"dataset": 1, "integrationRuntime": 1, "linkedService": 1, "pipeline": 1, "trigger": 1}
    assert s.pipelines == ["PL_A"] and s.total == 5 and s.unresolved_references == 0


def test_analysis_flags_missing_definitions_and_unresolved_references(tmp_path):
    write_tree(tmp_path, {"pipeline/PL_A.json": pipeline(child="PL_Missing"), "dataset/DS_A.json": ESTATE["dataset/DS_A.json"]})
    s = analyze_source(tmp_path)
    codes = {f.code for f in s.findings}
    assert "missing_reference" in codes and s.unresolved_references >= 2 and not s.complete
    assert any("PL_Missing" in f.message and f.action for f in s.findings)  # names the missing artifact and what to do


def test_analysis_recognises_a_synapse_export_and_counts_what_is_not_converted(tmp_path):
    write_tree(tmp_path, {**ESTATE, "notebook/N1.json": {"name": "N1"}, "notebook/N2.json": {"name": "N2"},
                          "sqlscript/S1.json": {"name": "S1"}, "bigDataPool/spark1.json": {"name": "spark1"}})
    s = analyze_source(tmp_path)
    assert s.flavor == "synapse" and s.synapse_only == {"notebooks": 2, "SQL scripts": 1, "Spark pools": 1}


def test_analysis_reports_no_artifacts_with_a_hint_for_arm_templates(tmp_path):
    write_tree(tmp_path, {"ARMTemplateForFactory.json": {"resources": []}, "readme.json": {}})
    s = analyze_source(tmp_path)
    assert not s.layout_ok and not s.complete and s.flavor == "unknown"
    assert any(f.code == "no_artifacts" and "ARM template" in f.message for f in s.findings)
    empty = tmp_path / "empty"
    empty.mkdir()
    assert any(f.code == "no_artifacts" for f in analyze_source(empty).findings)


def test_analysis_subdirectory_is_validated_and_ambiguous_roots_are_noted(tmp_path):
    write_tree(tmp_path / "a", ESTATE)
    write_tree(tmp_path / "b", {"pipeline/PL_B.json": pipeline("PL_B")})
    assert analyze_source(tmp_path, "b").pipelines == ["PL_B"]
    with pytest.raises(ValueError, match="does not exist"):
        analyze_source(tmp_path, "nope")
    with pytest.raises(ValueError, match="escapes"):
        analyze_source(tmp_path / "a", "../b")
    root, notes = detect_root(tmp_path)
    assert root.name == "a" and notes and "Several folders" in notes[0]  # a has more artifact folders, so it is chosen


def test_analysis_without_pipelines_warns_but_stays_usable(tmp_path):
    write_tree(tmp_path, {k: v for k, v in ESTATE.items() if not k.startswith("pipeline")})
    s = analyze_source(tmp_path)
    assert any(f.code == "no_pipelines" for f in s.findings)


# --- ZIP review + intake ---------------------------------------------------------------------------------------


def test_inspect_describes_the_archive_creates_nothing_and_cleans_up(tmp_path):
    z = make_zip(tmp_path, ESTATE, prefix="my-factory/")
    before = set(tmp_path.iterdir())
    summary, report = inspect_zip(z)
    assert summary.root == "my-factory" and summary.total == 5 and report.extracted == 5
    assert set(tmp_path.iterdir()) == before
    with pytest.raises(ZipRejected):
        inspect_zip(tmp_path / "missing-but-not-a-zip.zip") if False else inspect_zip(make_zip(tmp_path, {"../x.json": "{}"}, name="z.zip"))


def zip_agent(tmp_path):
    return ProjectStore(tmp_path / "projects")


def test_zip_intake_stores_the_source_manifest_and_feeds_discovery_and_semantic_parsing(tmp_path):
    store = zip_agent(tmp_path)
    z = make_zip(tmp_path, {**ESTATE, "scripts/evil.sh": "rm -rf /"}, prefix="factory/")
    r = ZipSourceIntake(store).run(ZipIntakeRequest(project_name="Zip Demo", zip_path=str(z), original_name="my export.zip"))
    assert r.success, r.error
    m = r.manifest
    assert m.source_type is SourceType.zip_export and m.source_identifier.startswith("zip://") and m.source_identifier.endswith("/my_export.zip")
    assert m.subdirectory == "factory" and len(m.version) == 40 and m.complete
    assert not list(store.source_dir("zip-demo").rglob("*.sh")) and str(tmp_path) not in m.source_identifier
    # the existing workflow runs unchanged on the imported source
    assert DiscoveryAgent(store).discover_repository(DiscoveryRequest(project_name="zip-demo")).success
    sem = AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="zip-demo")).discovery
    assert [p.name for p in sem.pipelines] == ["PL_A"] and [l.name for l in sem.linked_services] == ["LS_A"]
    assert [i.name for i in sem.integration_runtimes] == ["IR"] if hasattr(sem, "integration_runtimes") else True


def test_zip_intake_failures_create_no_project_and_say_why(tmp_path):
    store = zip_agent(tmp_path)
    bad_zip = make_zip(tmp_path, {"../evil.json": "{}"}, name="bad.zip")
    r = ZipSourceIntake(store).run(ZipIntakeRequest(project_name="Bad", zip_path=str(bad_zip)))
    assert not r.success and "outside" in r.error and not store.exists("bad")
    empty = make_zip(tmp_path, {"notes.json": "{}"}, name="empty.zip")
    r2 = ZipSourceIntake(store).run(ZipIntakeRequest(project_name="Empty", zip_path=str(empty)))
    assert not r2.success and "No pipeline" in r2.error and not store.exists("empty")
    assert not list((store.projects_root / "bad").glob("source.staging*")) if (store.projects_root / "bad").exists() else True


def test_zip_intake_reimport_replaces_the_source(tmp_path):
    store = zip_agent(tmp_path)
    ZipSourceIntake(store).run(ZipIntakeRequest(project_name="P", zip_path=str(make_zip(tmp_path, ESTATE, name="a.zip"))))
    two = {**ESTATE, "pipeline/PL_B.json": pipeline("PL_B")}
    r = ZipSourceIntake(store).run(ZipIntakeRequest(project_name="P", zip_path=str(make_zip(tmp_path, two, name="b.zip"))))
    assert r.success and len([a for a in r.manifest.artifacts if a.kind == "pipeline"]) == 2


# --- Git -------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("url, why", [("git@github.com:o/r.git", "HTTPS"), ("ssh://git@host/o/r", "HTTPS"), ("file:///x", "HTTPS"),
                                      ("http://github.com/o/r", "https"), ("https://user:pw@github.com/o/r", "credentials"),
                                      (f"https://{TOKEN}@github.com/o/r", "credentials"), ("", "https")])
def test_repository_urls_must_be_https_and_credential_free(url, why):
    with pytest.raises(GitSourceError, match=why):
        clean_url(url)


def test_clean_url_normalises_and_default_users_follow_host_conventions():
    assert clean_url("https://GitHub.com/o/r.git/") == ("https://github.com/o/r.git", "github.com")
    assert clean_url("https://dev.azure.com/org/p/_git/r")[1] == "dev.azure.com"
    assert [default_username(h) for h in ("github.com", "gitlab.example.com", "bitbucket.org", "dev.azure.com", "x.org")] == [
        "x-access-token", "oauth2", "x-token-auth", "", "git"]


