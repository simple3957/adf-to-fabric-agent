"""Output check: a pipeline with no earlier run to compare with is judged by its data (rows, columns, content hash)."""
import io

import pytest

from adf2fabric.agent.deployment import output_check as oc


def csv(*rows, header="id,name,amt"):
    return ("\n".join([header, *rows]) + "\n").encode()


def parquet(cols: dict):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(pa.table(cols), buf)
    return buf.getvalue()


def test_the_same_rows_in_any_order_and_any_split_into_files_match():
    a = oc.summarize([("a.csv", csv("1,x,10", "2,y,20", "3,z,30"))])
    b = oc.summarize([("p1.csv", csv("3,z,30", "1,x,10")), ("p2.csv", csv("2,y,20"))])
    assert oc.judge(a, b)[0] == oc.MATCHES and a.rows == 3 and b.files == 2


def test_columns_in_another_order_still_match():
    a = oc.summarize([("a.csv", csv("1,x,10"))])
    b = oc.summarize([("a.csv", csv("10,1,x", header="amt,id,name"))])
    assert oc.judge(a, b)[0] == oc.MATCHES


def test_a_lost_extra_or_changed_row_is_not_a_match():
    base = oc.summarize([("a.csv", csv("1,x,10", "2,y,20"))])
    assert oc.judge(base, oc.summarize([("a.csv", csv("1,x,10"))]))[0] == oc.DIFFERS  # lost
    assert oc.judge(base, oc.summarize([("a.csv", csv("1,x,10", "2,y,20", "2,y,20"))]))[0] == oc.DIFFERS  # duplicated
    verdict, detail = oc.judge(base, oc.summarize([("a.csv", csv("1,x,10", "2,y,21"))]))  # same count, a value changed
    assert verdict == oc.COUNTS_MATCH and "hash differs" in detail
    assert oc.judge(base, oc.summarize([("a.csv", csv("1,x", header="id,name"))]))[0] == oc.DIFFERS  # a column is gone


def test_csv_text_equals_parquet_values_of_the_same_data():
    c = oc.summarize([("a.csv", csv("1,x,10", "2,y,20"))])
    p = oc.summarize([("part-0.parquet", parquet({"id": [1, 2], "name": ["x", "y"], "amt": [10.0, 20.0]}))])
    assert oc.judge(c, p)[0] == oc.MATCHES


def test_markers_are_not_data_and_no_data_is_an_error():
    with pytest.raises(oc.OutputCheckError):
        oc.summarize([("_SUCCESS", b"")])


def test_locations():
    loc = oc.parse_location("abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/a/b")
    assert (loc.filesystem, loc.path, loc.account_url) == ("ws", "lh/Files/a/b", "https://onelake.dfs.fabric.microsoft.com")
    for bad in ("", "https://x/y", "abfss://nofilesystem.dfs.core.windows.net/p"):
        with pytest.raises(oc.OutputCheckError):
            oc.parse_location(bad)


def test_the_record_keeps_counts_and_hashes_never_values(tmp_path):
    s = oc.summarize([("a.csv", csv("1,SECRET-NAME,10"))])
    r = {"name": "n", "pipeline": "PL", "verdict": oc.MATCHES, "detail": "", "source_summary": s.__dict__, "target_summary": s.__dict__,
         "checked_at": "2999-01-01T00:00:00+00:00"}
    assert "SECRET-NAME" not in oc.save(tmp_path, r).read_text()


def test_evidence_needs_every_check_matching_and_made_after_the_run(tmp_path):
    from datetime import datetime, timezone

    ran = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert oc.evidence(tmp_path, "PL", ran)[0] is False  # none
    oc.save(tmp_path, {"name": "a", "pipeline": "PL", "verdict": oc.MATCHES, "detail": "", "checked_at": "2026-02-01T00:00:00+00:00"})
    assert oc.evidence(tmp_path, "pl", ran)[:2] == (True, 1)
    oc.save(tmp_path, {"name": "b", "pipeline": "PL", "verdict": oc.COUNTS_MATCH, "detail": "x", "checked_at": "2026-02-01T00:00:00+00:00"})
    assert oc.evidence(tmp_path, "PL", ran)[0] is False
    oc.save(tmp_path, {"name": "b", "pipeline": "PL", "verdict": oc.MATCHES, "detail": "", "checked_at": "2025-12-01T00:00:00+00:00"})
    ok, n, bad = oc.evidence(tmp_path, "PL", ran)
    assert not ok and "before the recorded run" in bad[0]
