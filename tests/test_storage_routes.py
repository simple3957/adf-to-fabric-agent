"""Routing storage paths in code through OneLake shortcuts: only literal, planned locations; everything else left and reported."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from adf2fabric.agent.storage.routes import RouteError, Routes, Target
from adf2fabric.agent.workloads import analyzer, engine, notebook_format, transform

WS, LH = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
MAP = {"shortcuts": [{"account": "acctone", "container": "raw", "shortcut": "raw"},
                     {"account": "accttwo", "container": "raw", "shortcut": "raw-accttwo"}]}


def routes(ws=WS, lh=LH):
    return Routes.from_storage_map(MAP, Target(ws, lh))


def test_literal_abfss_and_https_paths_go_through_the_shortcut():
    code = ('df = spark.read.parquet("abfss://raw@acctone.dfs.core.windows.net/sales/2024")\n'
            "url = 'https://acctone.dfs.core.windows.net/raw/x.csv'\n"
            'other = "abfss://raw@accttwo.dfs.core.windows.net/y"\n')
    r = routes().rewrite(code)
    assert r.routed == 3 and r.left == [] and r.routed_locations == ["acctone/raw", "accttwo/raw"]
    assert f'"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{LH}/Files/raw/sales/2024"' in r.text
    assert f"'https://onelake.dfs.fabric.microsoft.com/{WS}/{LH}/Files/raw/x.csv'" in r.text
    assert f"/{LH}/Files/raw-accttwo/y" in r.text  # each account keeps its own shortcut
    assert "core.windows.net" not in r.text


def test_names_use_the_documented_item_type_extension():
    r = routes("SalesWS", "lh_main").rewrite("p = 'abfss://raw@acctone.dfs.core.windows.net/a'")
    assert "abfss://SalesWS@onelake.dfs.fabric.microsoft.com/lh_main.Lakehouse/Files/raw/a" in r.text


@pytest.mark.parametrize("ws,lh", [(WS, "lh_main"), ("Sales WS", "lh"), ("ws", "lh main"), ("", LH), ("ws", "")])
def test_a_mix_of_guid_and_name_or_an_unsafe_name_is_refused(ws, lh):
    with pytest.raises(RouteError):
        Target(ws, lh)


def test_what_is_not_planned_or_not_literal_is_left_as_written_and_reported():
    code = ('a = "abfss://other@acctone.dfs.core.windows.net/x"\n'  # a container without a shortcut
            'b = f"abfss://{container}@acctone.dfs.core.windows.net/x"\n'  # built from a variable
            'c = "wasbs://raw@acctone.blob.core.windows.net/x"\n'  # Blob driver
            'd = "https://acctone.blob.core.windows.net/raw/x"\n')
    r = routes().rewrite(code)
    assert r.routed == 0 and r.text == code
    whys = [w for _, w in r.left]
    assert any("no shortcut" in w for w in whys) and any("variable" in w for w in whys) and sum("Blob" in w for w in whys) == 2


def test_lookalike_hosts_are_not_rewritten():
    code = 'x = "abfss://raw@acctone.dfs.core.windows.net.evil.example/x"\ny = "https://acctone.dfs.core.windows.netx/raw"\n'
    assert routes().rewrite(code).routed == 0


def _transformed(code: str):
    cells, _ = notebook_format.load(code, "nb.py")
    an = analyzer.analyze(cells, "databricks")
    return transform.transform(cells, an, "databricks"), list(an.findings)


def test_generated_notebook_cells_are_routed_and_their_findings_marked():
    tr, findings = _transformed('df = spark.read.csv("abfss://raw@acctone.dfs.core.windows.net/in.csv")\n'
                                'df2 = spark.read.csv("abfss://keep@acctone.dfs.core.windows.net/in.csv")\n')
    assert {f.construct for f in findings if f.code == "adls_direct_path"} == {"abfss://raw@acctone", "abfss://keep@acctone"}
    engine._route_storage(tr, findings, routes())
    code = "".join("".join(c["source"]) for c in tr.cells if c["cell_type"] == "code")
    assert f"/{LH}/Files/raw/in.csv" in code and "abfss://keep@acctone.dfs.core.windows.net/in.csv" in code
    done = {f.construct: f.rewritten for f in findings if f.code == "adls_direct_path"}
    assert done == {"abfss://raw@acctone": True, "abfss://keep@acctone": False}  # the unplanned one stays open work
    assert [r.code for r in tr.rewrites if r.code == "storage_routed"] == ["storage_routed"]


def test_nothing_changes_without_routes():
    tr, findings = _transformed('df = spark.read.csv("abfss://raw@acctone.dfs.core.windows.net/in.csv")\n')
    before = json.dumps(tr.cells)
    engine._route_storage(tr, findings, Routes([], Target(WS, LH)))
    assert json.dumps(tr.cells) == before and not any(f.rewritten for f in findings if f.code == "adls_direct_path")


def test_the_cli_needs_a_storage_map_and_both_onelake_options(tmp_path, monkeypatch):
    from adf2fabric import cli as cli_mod
    from adf2fabric.project.store import ProjectStore

    store = ProjectStore(tmp_path / "projects")
    monkeypatch.setattr(cli_mod, "ProjectStore", lambda root: store)
    one = CliRunner().invoke(cli_mod.cli, ["workloads-assess", "--project", "p", "--onelake-workspace", WS])
    assert one.exit_code != 0 and "both" in one.output
    none = CliRunner().invoke(cli_mod.cli, ["workloads-assess", "--project", "p", "--onelake-workspace", WS, "--onelake-lakehouse", LH])
    assert none.exit_code != 0 and "storage-map" in none.output
    (store.project_dir("p") / "storage").mkdir(parents=True)
    (store.project_dir("p") / "storage" / "storage-map.json").write_text(json.dumps(MAP), encoding="utf-8")
    mixed = CliRunner().invoke(cli_mod.cli, ["workloads-assess", "--project", "p", "--onelake-workspace", WS, "--onelake-lakehouse", "lh"])
    assert mixed.exit_code != 0 and "GUIDs" in mixed.output


@pytest.mark.parametrize("code", ['x = "abfss://raw@acctone.dfs.core.windows.net:443/x"', 'x = "abfss://raw@acctone.dfs.core.windows.net#f"'])
def test_forms_that_are_not_rewritten_are_reported_not_silently_skipped(code):
    r = routes().rewrite(code)
    assert r.routed == 0 and r.text == code and r.left and "not rewritten" in r.left[0][1]


def test_a_url_with_a_query_string_is_never_sent_to_onelake():
    code = 'u = "https://acctone.dfs.core.windows.net/raw/x.csv?sv=2024&sig=SECRET"'
    r = routes().rewrite(code)
    assert r.routed == 0 and r.text == code and "query string" in r.left[0][1]


def test_a_finding_stays_open_when_the_same_location_is_also_written_in_a_form_not_rewritten():
    tr, findings = _transformed('a = spark.read.csv("abfss://raw@acctone.dfs.core.windows.net/in.csv")\n'
                                'b = spark.read.csv("abfss://raw@acctone.dfs.core.windows.net:443/in.csv")\n')
    engine._route_storage(tr, findings, routes())
    flags = [f.rewritten for f in findings if f.code == "adls_direct_path"]
    assert flags and not any(flags)  # a direct reference to that location remains, so none is marked done
