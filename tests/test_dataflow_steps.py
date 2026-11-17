"""Model-drafted code for a data flow step the fixed rules cannot translate (agent/assist/dataflow_steps.py, 2026-11-17).

No model is called here: the drafts are files a person would hold after `assist-dataflow --draft`, written the way the module stores them.
What is checked is the part that must never go wrong: only an accepted draft of an unchanged script is used, it is marked in the notebook,
and without one the step still raises."""

from __future__ import annotations

import json

import pytest

from adf2fabric.agent.assist import dataflow_steps as DS
from adf2fabric.agent.workloads import dataflow as DF

SCRIPT = "source(allowSchemaDrift: true) ~> Sales\nSales pivot(groupBy(Product), pivotBy(Region), Total = sum(Amount)) ~> P"
CODE = "df_P = df_Sales.groupBy('Product').pivot('Region').agg(F.sum('Amount').alias('Total'))"


def _store(tmp_path, script=SCRIPT, unconverted=()):
    d = DS._dir(tmp_path, "DF_Pivot")
    d.mkdir(parents=True)
    (d / "P.json").write_text(json.dumps({"dataflow": "DF_Pivot", "step": "P", "status": "draft", "code": CODE, "notes": [],
                                          "unconverted": list(unconverted), "confidence": "medium", "script_sha256": DS._hash(script)}))


def test_the_untranslated_step_is_a_candidate_with_its_variables(tmp_path):
    [c] = DS.candidates(tmp_path, "DF_Pivot", SCRIPT)
    assert c["step"] == "P" and c["inputs"] == ["df_Sales"] and c["output"] == "df_P" and c["draft"] is None


def test_a_draft_is_used_only_after_acceptance_and_is_marked(tmp_path):
    _store(tmp_path)
    assert DS.accepted(tmp_path, "DF_Pivot", SCRIPT) == {}  # a draft alone changes nothing
    tr = DF.translate(SCRIPT, {}, {}, {}, DS.accepted(tmp_path, "DF_Pivot", SCRIPT))
    assert any("pivot" in u for u in tr.untranslated) and tr.drafted == []
    DS.accept(tmp_path, "DF_Pivot", SCRIPT, "P", "reviewer")
    tr = DF.translate(SCRIPT, {}, {}, {}, DS.accepted(tmp_path, "DF_Pivot", SCRIPT))
    code = "".join("".join(c["source"]) for c in tr.cells)
    assert tr.drafted == ["P (pivot)"] and not any(u.startswith("P ") for u in tr.untranslated)
    assert "MODEL-DRAFTED" in code and "accepted by reviewer" in code and CODE in code
    assert "data flow step P (pivot) is not translated" not in code


def test_a_changed_script_or_an_incomplete_draft_is_refused(tmp_path):
    _store(tmp_path)
    changed = SCRIPT.replace("sum(Amount)", "max(Amount)")
    with pytest.raises(DS.StepDraftError, match="changed since"):
        DS.accept(tmp_path, "DF_Pivot", changed, "P", "reviewer")
    DS.accept(tmp_path, "DF_Pivot", SCRIPT, "P", "reviewer")
    assert DS.accepted(tmp_path, "DF_Pivot", changed) == {}  # pinned to the script it was made for
    (DS._dir(tmp_path, "DF_Pivot") / "P.json").unlink()
    d = DS._dir(tmp_path, "DF_Pivot")
    (d / "P.json").write_text(json.dumps({"step": "P", "status": "draft", "code": CODE, "unconverted": ["Total alias"],
                                          "script_sha256": DS._hash(SCRIPT)}))
    with pytest.raises(DS.StepDraftError, match="not converted"):
        DS.accept(tmp_path, "DF_Pivot", SCRIPT, "P", "reviewer")


def test_drafting_is_refused_while_model_assistance_is_off(tmp_path):
    import os
    if os.environ.get("ADF2FABRIC_LLM", "").lower() == "on":
        pytest.skip("model assistance is on in this environment")
    with pytest.raises(DS.StepDraftError, match="ADF2FABRIC_LLM=on"):
        DS.draft(tmp_path, "DF_Pivot", SCRIPT, "P")
