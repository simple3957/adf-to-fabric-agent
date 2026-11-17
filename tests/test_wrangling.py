"""ADF wrangling data flow (Power Query) -> Fabric Dataflow Gen2 definition (agent/workloads/wrangling.py, 2026-11-17).

The script below is in the shape ADF stores (Microsoft.DataFactory dataflows, type WranglingDataFlow: typeProperties.script with the source
queries ADF generates from the datasets and the UserQuery); the definition parts are Fabric's documented ones (queryMetadata.json with
formatVersion 202502 and queriesMetadata, mashup.pq)."""

from __future__ import annotations

import json

import pytest

from adf2fabric.agent.workloads import wrangling

SCRIPT = '''section Section1;
shared DS_CSV_Sales = let AdfDoc = AzureStorage.DataLakeContents("https://acct.dfs.core.windows.net/data/sales.csv"),
  Csv = Csv.Document(AdfDoc, [Delimiter = ",", Encoding = TextEncoding.Utf8, QuoteStyle = QuoteStyle.Csv]),
  Promoted = Table.PromoteHeaders(Csv, [PromoteAllScalars = true]) in Promoted;
shared #"User Query" = let Source = DS_CSV_Sales, Kept = Table.SelectRows(Source, each [Amount] <> null) in Kept;'''
TP = {"script": SCRIPT, "documentLocale": "en-us", "sources": [{"name": "DS_CSV_Sales", "dataset": {"referenceName": "DS_CSV_Sales"}}]}


def test_the_script_becomes_the_mashup_and_each_query_is_described():
    parts, todo = wrangling.definition("WDF_Sales", TP)
    assert parts["mashup.pq"].startswith("section Section1;") and "Table.SelectRows" in parts["mashup.pq"]  # carried unchanged
    meta = json.loads(parts["queryMetadata.json"])
    assert meta["formatVersion"] == "202502" and meta["name"] == "WDF_Sales" and meta["documentLocale"] == "en-us"
    q = meta["queriesMetadata"]
    assert list(q) == ["DS_CSV_Sales", "User Query"]  # a quoted #"..." name is read without its quotes
    assert q["DS_CSV_Sales"]["loadEnabled"] is False and q["User Query"]["loadEnabled"] is True  # source queries only feed the user query
    assert q["User Query"]["queryId"] == json.loads(wrangling.definition("WDF_Sales", TP)[0]["queryMetadata.json"])["queriesMetadata"]["User Query"]["queryId"]
    assert len(todo) == 2 and "destination" in todo[1]  # never guessed: a person sets sign-in and destination


def test_a_script_without_a_section_header_gets_one_and_an_empty_one_is_refused():
    assert wrangling.mashup("shared Q = 1;").startswith("section Section1;")
    with pytest.raises(ValueError, match="no Power Query script"):
        wrangling.definition("X", {"script": "  "})
    with pytest.raises(ValueError, match="no shared query"):
        wrangling.definition("X", {"script": "section Section1;"})
