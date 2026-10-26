"""Every source dataset is accounted for after conversion: inlined (where), repointed, held back or not carried at all."""

from __future__ import annotations

from tests.conversion_support import dataset
from tests.test_conversion_agent import FILES
from tests.test_partial_source import _convert

UNUSED = dataset("DS_Unused", "AzureSqlTable", "LS_SQL", {"schema": "dbo", "table": "Old"})


def _ledger(conv):
    return {e.name: e for e in conv.dataset_ledger}


def test_inlined_datasets_say_which_activities_got_them_and_an_unused_one_is_listed_with_the_reason(tmp_path):
    _, _, conv = _convert(tmp_path, {**FILES, "dataset/DS_Unused.json": UNUSED})
    led = _ledger(conv)
    assert set(led) == {"DS_SQL", "DS_LAKE", "DS_Unused"}  # nothing silently missing
    assert led["DS_SQL"].treatment == "inlined" and led["DS_SQL"].used_by and all("/" in u for u in led["DS_SQL"].used_by)
    assert led["DS_Unused"].treatment == "not_used" and "nothing to inline" in led["DS_Unused"].note and not led["DS_Unused"].used_by


def test_a_dataset_used_only_by_a_pipeline_not_converted_in_this_run_is_not_called_unused(tmp_path):
    _, _, conv = _convert(tmp_path, FILES, pipelines=["PL_Child"])
    led = _ledger(conv)
    assert led["DS_SQL"].treatment == "not_converted" and "PL_Main" in led["DS_SQL"].note


def test_the_report_lists_the_datasets_section(tmp_path):
    store, pid, _ = _convert(tmp_path, {**FILES, "dataset/DS_Unused.json": UNUSED})
    text = (store.conversion_dir(pid) / "conversion-report.md").read_text("utf-8")
    assert "## Datasets" in text and "`DS_Unused`" in text and "not used" in text


def test_the_ledger_survives_the_record_round_trip(tmp_path):
    store, pid, _ = _convert(tmp_path, FILES)
    assert {e.name for e in store.load_conversion(pid).dataset_ledger} == {"DS_SQL", "DS_LAKE"}
