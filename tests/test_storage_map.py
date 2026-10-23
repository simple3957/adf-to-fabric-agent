"""Storage map: ADF storage datasets -> OneLake shortcuts and Lakehouse paths. Read-only; it never guesses what it cannot read."""

from __future__ import annotations

import json
from types import SimpleNamespace as NS

from click.testing import CliRunner

from adf2fabric.agent.storage.mapping import build_storage_map, render_markdown

ACC = "https://acctone.dfs.core.windows.net/"


def ls(name, endpoint=ACC):
    return NS(name=name, endpoint=endpoint)


def ds(name, loc, ls_name="LS1", fmt="Parquet"):
    return NS(name=name, type=fmt, linked_service_ref=NS(name=ls_name) if ls_name else None, type_properties={"location": loc} if loc is not None else {"table": "t"})


def adls(container="c1", folder=None, file=None):
    loc = {"type": "AzureBlobFSLocation", "fileSystem": container}
    if folder is not None:
        loc["folderPath"] = folder
    if file is not None:
        loc["fileName"] = file
    return loc


def expr(v):
    return {"value": v, "type": "Expression"}


def act(name, inputs=(), outputs=(), children=()):
    return NS(name=name, inputs=[NS(kind="dataset", name=i) for i in inputs], outputs=[NS(kind="dataset", name=o) for o in outputs], activities=list(children))


def disc(datasets, linked=None, pipelines=None):
    return NS(project_id="p", datasets=datasets, linked_services=[ls("LS1")] if linked is None else linked, pipelines=pipelines or [])


def row(m, name):
    return next(r for r in m.datasets if r.dataset == name)


def test_literal_adls_locations_map_exactly_and_share_one_shortcut():
    m = build_storage_map(disc([ds("A", adls("c1", "in/sales", "s.csv")), ds("B", adls("c1", "/out/", None))]), "lh")
    assert [s.lakehouse_path for s in m.shortcuts] == ["Files/c1"] and m.shortcuts[0].datasets == 2
    assert m.shortcuts[0].account_url == "https://acctone.dfs.core.windows.net" and m.shortcuts[0].subpath == "/c1"
    assert row(m, "A").status == "mapped" and row(m, "A").lakehouse_path == "Files/c1/in/sales/s.csv"
    assert row(m, "B").lakehouse_path == "Files/c1/out"  # slashes normalised, no file part
    assert m.summary["needs_decision"] == 0 and m.to_decide == []


def test_an_expression_in_folder_or_file_is_kept_as_authored_not_resolved():
    m = build_storage_map(disc([ds("A", adls("c1", "in", expr("@dataset().f"))), ds("B", adls("c1", expr("@concat('a/',dataset().d)"), "x"))]))
    a, b = row(m, "A"), row(m, "B")
    assert a.status == "mapped_dynamic" and a.dynamic == ["file"] and a.lakehouse_path == "Files/c1/in/<dynamic: @dataset().f>"
    assert b.dynamic == ["folder"] and "<dynamic: @concat('a/',dataset().d)>" in b.lakehouse_path
    assert m.summary["mapped_dynamic"] == 2 and len(m.shortcuts) == 1  # the container is literal, so a shortcut can still be named


def test_what_cannot_be_read_is_a_decision_with_a_reason_never_a_guess():
    cases = {
        "dynamic_container": ds("D1", adls(expr("@dataset().c"), "f")),
        "no_container": ds("D2", {"type": "AzureBlobFSLocation"}),
        "blob": ds("D3", {"type": "AzureBlobStorageLocation", "container": "c", "folderPath": "f"}),
        "s3": ds("D4", {"type": "AmazonS3Location", "bucketName": "b"}),
        "unknown": ds("D5", {"type": "SomethingNewLocation"}),
        "no_ls": ds("D6", adls("c1"), ls_name=None),
        "dotdot": ds("D7", adls("c1", "../x")),
    }
    m = build_storage_map(disc(list(cases.values())))
    assert m.shortcuts == [] and m.summary["needs_decision"] == len(cases)
    for r in m.datasets:
        assert r.status == "needs_decision" and r.reason and r.action and r.lakehouse_path is None, r.dataset
    assert {d["dataset"] for d in m.to_decide} == {"D1", "D2", "D3", "D4", "D5", "D6", "D7"}


def test_a_linked_service_whose_account_cannot_be_read_is_a_decision():
    for endpoint in (None, "", "https://x.blob.core.windows.net/", "@{linkedService().url}", "https://acct.dfs.core.windows.net.evil.example"):
        m = build_storage_map(disc([ds("A", adls("c1"))], linked=[ls("LS1", endpoint)]))
        assert row(m, "A").status == "needs_decision" and m.shortcuts == [], endpoint
    m = build_storage_map(disc([ds("A", adls("c1"))], linked=[]))  # the linked service is not in the source at all
    assert row(m, "A").status == "needs_decision" and "not found" in row(m, "A").reason


def test_non_file_datasets_are_counted_and_left_out():
    m = build_storage_map(disc([ds("T", None, fmt="AzureSqlTable"), ds("A", adls("c1"))]))
    assert m.summary["not_file_datasets"] == 1 and [r.dataset for r in m.datasets] == ["A"]


def test_the_same_container_name_in_two_accounts_gets_distinct_shortcuts():
    linked = [ls("LS1", ACC), ls("LS2", "https://accttwo.dfs.core.windows.net")]
    m = build_storage_map(disc([ds("A", adls("data", "x")), ds("B", adls("data", "y"), "LS2")], linked=linked))
    assert sorted(s.shortcut for s in m.shortcuts) == ["data-acctone", "data-accttwo"]
    assert row(m, "A").lakehouse_path == "Files/data-acctone/x" and row(m, "B").lakehouse_path == "Files/data-accttwo/y"


def test_pipelines_and_activities_using_a_dataset_are_listed_including_nested_ones():
    inner = act("Inner", outputs=["A"])
    pipes = [NS(name="P1", activities=[act("Copy", ["A"], ["B"]), act("Loop", children=[inner])])]
    m = build_storage_map(disc([ds("A", adls("c1")), ds("B", adls("c1"))], pipelines=pipes))
    assert {(u.pipeline, u.activity, u.role) for u in row(m, "A").used_by} == {("P1", "Copy", "input"), ("P1", "Inner", "output")}
    assert [(u.activity, u.role) for u in row(m, "B").used_by] == [("Copy", "output")]


def test_the_report_names_what_the_user_must_decide_and_claims_nothing_is_done():
    m = build_storage_map(disc([ds("A", adls("c1", "f")), ds("D3", {"type": "AzureBlobStorageLocation", "container": "c"})], pipelines=[NS(name="P1", activities=[act("Copy", ["D3"])])]), "lh_main")
    md = render_markdown(m, "WS")
    assert "For you to deal with" in md and "| D3 | P1 |" in md and "create-shortcut --workspace-id WS --lakehouse lh_main" in md
    assert "No data is moved" in md and "not verified until the shortcut exists" in md and "not yet rewritten" in md
    assert "--connection-id <connection-id for acctone>" in md  # the connection id is never invented
    none = render_markdown(build_storage_map(disc([])), "WS")
    assert "None." in none and "<lakehouse>" not in none.split("## For you")[0].replace("pass --lakehouse", "")


def test_the_lakehouse_name_is_only_shown_when_given():
    assert "lakehouse `lh`" in render_markdown(build_storage_map(disc([ds("A", adls("c1"))]), "lh"))
    assert "no lakehouse named yet" in render_markdown(build_storage_map(disc([ds("A", adls("c1"))])))


def test_the_command_is_read_only_and_writes_the_report_into_the_project(tmp_path, monkeypatch):
    from adf2fabric import cli as cli_mod
    from adf2fabric.project.store import ProjectStore

    store = ProjectStore(tmp_path / "projects")
    d = disc([ds("A", adls("c1", "f", "x.csv")), ds("B", {"type": "AzureBlobStorageLocation", "container": "c"})])
    monkeypatch.setattr(cli_mod, "ProjectStore", lambda root: store)
    monkeypatch.setattr(ProjectStore, "has_adf_semantic_discovery", lambda self, pid: pid == "p")
    monkeypatch.setattr(ProjectStore, "load_adf_semantic_discovery", lambda self, pid: d)
    r = CliRunner().invoke(cli_mod.cli, ["storage-map", "--project", "p", "--lakehouse", "lh"])
    assert r.exit_code == 0, r.output
    assert "mapped: 1" in r.output and "need a decision: 1" in r.output and "DECIDE B" in r.output
    out = store.project_dir("p") / "storage"
    data = json.loads((out / "storage-map.json").read_text(encoding="utf-8"))
    assert data["summary"]["shortcuts"] == 1 and (out / "storage-map.md").exists()
    missing = CliRunner().invoke(cli_mod.cli, ["storage-map", "--project", "nope"])
    assert missing.exit_code == 1 and "discovery-adf-semantic" in missing.output


def test_a_shortcut_name_can_be_chosen_and_bad_choices_are_refused():
    import pytest

    from adf2fabric.agent.storage.mapping import StorageMapError

    linked = [ls("LS1", ACC), ls("LS2", "https://accttwo.dfs.core.windows.net")]
    d = disc([ds("A", adls("data", "x")), ds("B", adls("data", "y"), "LS2")], linked=linked)
    m = build_storage_map(d, names={("ACCTONE", "data"): "existing"})  # case-insensitive keys
    assert sorted(s.shortcut for s in m.shortcuts) == ["data-accttwo", "existing"] and row(m, "A").lakehouse_path == "Files/existing/x"
    for bad, why in (({("acctone", "data"): "data-accttwo"}, "share"), ({("nope", "x"): "y"}, "No mapped dataset"),
                     ({("acctone", "data"): "a/b"}, "cannot be"), ({("acctone", "data"): "a'b"}, "cannot be")):
        with pytest.raises(StorageMapError, match=why):
            build_storage_map(d, names=bad)


def test_a_container_written_in_two_cases_is_one_container():
    m = build_storage_map(disc([ds("A", adls("Raw", "x")), ds("B", adls("raw", "y"))]))
    assert [s.shortcut for s in m.shortcuts] == ["raw"] and m.shortcuts[0].datasets == 2
    assert row(m, "A").lakehouse_path == "Files/raw/x"
