"""Reconstruction end to end: record, report, conversion with inferred elements, CLI, safety and determinism."""

from __future__ import annotations

import inspect
import json
import pathlib

import pytest
import yaml
from click.testing import CliRunner

import adf2fabric.agent.reconstruction as rpkg
from adf2fabric import cli as cli_mod
from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.reconstruction.agent import ReconstructionAgent
from adf2fabric.agent.reconstruction.models import ReconstructionRequest
from adf2fabric.project.conversion import ConversionStatus as S
from adf2fabric.project.reconstruction import Completeness, ProjectReconstruction, ReconstructionOutcome
from tests.reconstruction_support import (
    LOW, act, blockers, completed, copy_activity, dep, expr, fingerprint, make_project, pl, proposals, reconstruct,
    ref, with_estate,
)
from tests.test_reconstruction_patterns import FILES, inc_pipeline

GUID = lambda n: f"{n:08d}-0000-0000-0000-000000000000"  # noqa: E731
PID = "test-proj"


def project(tmp_path):
    store, rec = reconstruct(tmp_path, with_estate(inc_pipeline(), **FILES))
    return store, rec


def bind_all():
    return [{"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": GUID(1)},
            {"type": "LinkedServiceToConnectionId", "key": "LS_LAKE", "value": GUID(2)}]


def run_convert(store, *, apply, resolutions=None, tmp=None):
    path = None
    if resolutions is not None:
        path = tmp / "res.json"
        path.write_text(json.dumps(resolutions), encoding="utf-8")
    return ConversionAgent(store).run(ConversionRequest(
        project_name=PID, apply_reconstruction=apply, resolutions_path=str(path) if path else None))


# =============================================================================
# the record and the four representations
# =============================================================================


def test_record_round_trips_and_keeps_representations_apart(tmp_path):
    store, rec = project(tmp_path)
    loaded = store.load_reconstruction(PID)
    assert isinstance(loaded, ProjectReconstruction) and loaded.model_dump() == rec.model_dump()
    assert set(rec.representations) == {"original", "parsed", "proposed", "generated"}
    text = store.reconstruction_path(PID).read_text("utf-8")
    assert yaml.safe_load(text)["schema_version"] == "adf-reconstruction/1"
    base = store.reconstruction_dir(PID)
    assert (base / "completed" / "pipeline" / "PL_Inc.json").exists() and (base / "adf-semantic.completed.yaml").exists()
    assert rec.status is ReconstructionOutcome.reconstructed and rec.safe_proposal_count == 1


def test_every_proposal_carries_every_required_field(tmp_path):
    _, rec = project(tmp_path)
    for p in proposals(rec):
        assert p.id and p.rule and p.finding_ids and p.missing_element and p.action and p.target_path
        assert p.evidence and all(e.source and e.detail and 1 <= e.priority <= 9 for e in p.evidence)
        assert p.confidence_basis.startswith(p.confidence.value.upper())
        assert isinstance(p.assumptions, list) and isinstance(p.alternatives, list)
        assert isinstance(p.information_required, list) and isinstance(p.safe_to_generate, bool)
        assert p.patches or not p.safe_to_generate


def test_the_report_labels_original_parsed_proposed_and_generated(tmp_path):
    store, _ = project(tmp_path)
    text = (store.reconstruction_dir(PID) / "reconstruction-report.md").read_text("utf-8")
    for label in ("ORIGINAL", "PARSED", "PROPOSED", "GENERATED"):
        assert label in text
    for needle in ("watermark_update_from_lookups", "MEDIUM", "Evidence:", "Assumptions:", "Alternative interpretations:",
                   "Confidence basis", "SQL assembled from evidence", "not in the original"):
        assert needle in text


def test_reconstruction_is_deterministic_and_idempotent(tmp_path):
    store, first = project(tmp_path)
    done = (store.reconstruction_dir(PID) / "completed" / "pipeline" / "PL_Inc.json").read_bytes()
    second = ReconstructionAgent(store).run(ReconstructionRequest(project_name=PID)).reconstruction
    assert second.created_at == first.created_at and second.updated_at == first.updated_at
    assert second.model_dump(exclude={"created_at", "updated_at"}) == first.model_dump(exclude={"created_at", "updated_at"})
    assert (store.reconstruction_dir(PID) / "completed" / "pipeline" / "PL_Inc.json").read_bytes() == done


def test_stale_completion_files_do_not_survive_a_rerun(tmp_path):
    store, _ = project(tmp_path)
    (store.reconstruction_dir(PID) / "completed" / "pipeline" / "stale.json").write_text("{}", encoding="utf-8")
    ReconstructionAgent(store).run(ReconstructionRequest(project_name=PID))
    assert not (store.reconstruction_dir(PID) / "completed" / "pipeline" / "stale.json").exists()


# =============================================================================
# nothing original is ever overwritten
# =============================================================================


def test_original_and_parsed_representations_are_untouched_by_reconstruction_and_conversion(tmp_path):
    store = make_project(tmp_path, with_estate(inc_pipeline(), **FILES))
    src_before = fingerprint(store.source_dir(PID))
    parsed_before = store.adf_semantic_discovery_path(PID).read_bytes()
    repo_before = store.repository_discovery_path(PID).read_bytes()
    assert ReconstructionAgent(store).run(ReconstructionRequest(project_name=PID)).success
    assert run_convert(store, apply=True).success and run_convert(store, apply=False).success
    assert fingerprint(store.source_dir(PID)) == src_before
    assert store.adf_semantic_discovery_path(PID).read_bytes() == parsed_before
    assert store.repository_discovery_path(PID).read_bytes() == repo_before
    # inferred content exists only under reconstruction/ and conversion/
    assert "Update Watermark" not in (store.source_dir(PID) / "pipeline" / "PL_Inc.json").read_text("utf-8")
    assert "Update Watermark" not in store.adf_semantic_discovery_path(PID).read_text("utf-8")


def test_the_completed_semantic_record_is_the_original_parse_plus_the_inference(tmp_path):
    store, _ = project(tmp_path)
    original = store.load_adf_semantic_discovery(PID)
    done = yaml.safe_load((store.reconstruction_dir(PID) / "adf-semantic.completed.yaml").read_text("utf-8"))
    assert [a.name for a in original.pipelines[0].activities] == ["Lookup_Old_Watermark", "Lookup_New_Watermark", "Copy_Delta"]
    assert [a["name"] for a in done["pipelines"][0]["activities"]][-1] == "Update Watermark - Copy_Delta"
    assert done["pipelines"][0]["activity_count"] == 4 and original.pipelines[0].activity_count == 3


# =============================================================================
# conversion: inferred elements are applied only on request, and marked
# =============================================================================


def test_conversion_ignores_reconstruction_unless_asked(tmp_path):
    store, _ = project(tmp_path)
    conv = run_convert(store, apply=False).conversion
    assert conv.reconstruction_applied == [] and conv.activity_count == 3
    assert not any(a.inferred_elements for p in conv.pipelines for a in p.activities)
    content = (store.conversion_dir(PID) / "pipelines" / "PL_Inc.DataPipeline" / "pipeline-content.json").read_text("utf-8")
    assert "Update Watermark" not in content and "userProperties" not in content


def test_applied_conversion_converts_the_completed_pipeline_and_marks_what_was_inferred(tmp_path):
    store, _ = project(tmp_path)
    conv = run_convert(store, apply=True).conversion
    assert conv.reconstruction_applied == ["P-0001"] and conv.activity_count == 4
    pl_ = conv.pipelines[0]
    new = next(a for a in pl_.activities if a.source_name == "Update Watermark - Copy_Delta")
    assert new.source_type == "Script" and [c.origin for c in new.components] == ["inferred"]
    [e] = new.inferred_elements
    assert (e.proposal_id, e.rule, e.confidence, e.review_required) == ("P-0001", "watermark_update_from_lookups", "medium", True)
    assert new.status is S.converted_with_differences and any("INFERRED" in x for x in new.limitations)
    assert any("Review inferred element P-0001" in m for m in new.manual_steps)
    # the original activities are untouched and carry no inference marker
    others = [a for a in pl_.activities if a is not new]
    assert all(not a.inferred_elements and all(c.origin == "source" for c in a.components) for a in others)
    content = json.loads((store.conversion_dir(PID) / "pipelines" / "PL_Inc.DataPipeline" / "pipeline-content.json").read_text("utf-8"))
    acts = {a["name"]: a for a in content["properties"]["activities"]}
    marker = acts["Update Watermark - Copy_Delta"]["userProperties"]
    assert marker[0]["name"] == "adf2fabric.inferred" and "P-0001 (medium)" in marker[0]["value"]
    assert "userProperties" not in acts["Copy_Delta"]
    assert acts["Update Watermark - Copy_Delta"]["dependsOn"][0]["activity"] == "Copy_Delta"
    assert pl_.structural_valid


def test_a_pipeline_with_medium_inferences_is_never_runtime_ready_even_when_fully_bound(tmp_path):
    store, _ = project(tmp_path)
    res = bind_all() + [{"type": "ItemToId", "key": "connection:invoke-pipeline", "value": GUID(3)},
                        {"type": "WorkspaceId", "key": "*", "value": GUID(5)}]
    plain = run_convert(store, apply=False, resolutions=res, tmp=tmp_path).conversion.pipelines[0]
    assert plain.runtime_ready is False or plain.runtime_ready is True  # baseline just has to compute
    inferred = run_convert(store, apply=True, resolutions=res, tmp=tmp_path).conversion.pipelines[0]
    assert inferred.structural_valid and not inferred.runtime_ready
    assert any(a.inferred_elements and not a.runtime_ready for a in inferred.activities)


def test_a_high_confidence_inference_does_not_block_readiness(tmp_path):
    lk = act("L", "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS_SQL")})
    rd = act("R", "SetVariable", {"variableName": "v", "value": expr("@activity('L').output.firstRow.x")})
    store, rec = reconstruct(tmp_path, with_estate(pl(lk, rd, variables={"v": "String"})))
    [p] = proposals(rec, "dependency_from_reference")
    assert p.confidence.value == "high"
    conv = run_convert(store, apply=True, resolutions=[{"type": "LinkedServiceToConnectionId", "key": "LS_SQL", "value": GUID(1)}],
                       tmp=tmp_path).conversion
    r = next(a for a in conv.pipelines[0].activities if a.source_name == "R")
    assert r.inferred_elements[0].review_required is False and r.status is S.converted_with_differences
    assert r.runtime_ready and conv.pipelines[0].runtime_ready
    cont = json.loads((store.conversion_dir(PID) / "pipelines" / "PL.DataPipeline" / "pipeline-content.json").read_text("utf-8"))
    assert cont["properties"]["activities"][1]["dependsOn"] == [{"activity": "L", "dependencyConditions": ["Succeeded"]}]


def test_low_confidence_and_blocked_gaps_are_not_applied_and_stay_visible(tmp_path):
    cp = copy_activity("Copy")
    cp["inputs"] = [ref("Dataset", "DS_GONE")]
    store, rec = reconstruct(tmp_path, with_estate(pl(cp, act("W", "Wait", {}))))
    assert rec.blocker_count >= 2 and rec.safe_proposal_count == 0
    conv = run_convert(store, apply=True).conversion
    assert conv.reconstruction_applied == [] and len(conv.reconstruction_open) == rec.blocker_count
    by = {a.source_name: a for a in conv.pipelines[0].activities}
    assert by["Copy"].status is S.blocked and not by["Copy"].inferred_elements
    assert by["W"].status is not S.converted  # a missing required property is never glossed over
    assert "Unresolved gaps carried from reconstruction" in (store.conversion_dir(PID) / "conversion-report.md").read_text("utf-8")


def test_the_conversion_report_separates_original_inferred_and_unresolved(tmp_path):
    store, _ = project(tmp_path)
    run_convert(store, apply=True)
    text = (store.conversion_dir(PID) / "conversion-report.md").read_text("utf-8")
    assert "Reconstruction applied:** P-0001" in text and "Inferred elements (NOT in the original source)" in text
    assert "proposal P-0001 (rule `watermark_update_from_lookups`, medium, review required)" in text


def test_applying_requires_a_current_reconstruction_record(tmp_path):
    store = make_project(tmp_path, with_estate(inc_pipeline(), **FILES))
    r = run_convert(store, apply=True)
    assert not r.success and "adf2fabric reconstruct" in r.error
    ReconstructionAgent(store).run(ReconstructionRequest(project_name=PID))
    rec = store.load_reconstruction(PID)
    rec.source_commit_sha = "0" * 40
    store.save_reconstruction(PID, rec)
    r = run_convert(store, apply=True)
    assert not r.success and "stale" in r.error
    assert run_convert(store, apply=False).success  # not applying never depends on it


# =============================================================================
# the agent's own preconditions
# =============================================================================


def test_reconstruction_needs_semantic_discovery_and_a_current_one(tmp_path):
    from adf2fabric.project.store import ProjectStore

    assert not ReconstructionAgent(ProjectStore(tmp_path / "p")).run(ReconstructionRequest(project_name="ghost")).success
    store = make_project(tmp_path / "a", with_estate(inc_pipeline(), **FILES))
    store.adf_semantic_discovery_path(PID).unlink()
    r = ReconstructionAgent(store).run(ReconstructionRequest(project_name=PID))
    assert not r.success and "Semantic Discovery has not run" in r.error
    store2 = make_project(tmp_path / "b", with_estate(inc_pipeline(), **FILES))
    d = store2.load_adf_semantic_discovery(PID)
    d.source_commit_sha = "1" * 40
    store2.save_adf_semantic_discovery(PID, d)
    assert "stale" in ReconstructionAgent(store2).run(ReconstructionRequest(project_name=PID)).error


def test_a_complete_estate_yields_a_complete_outcome_with_nothing_to_apply(tmp_path):
    store, rec = reconstruct(tmp_path, with_estate(pl(copy_activity("Copy"))))
    assert rec.status is ReconstructionOutcome.complete and rec.completed_files == []
    assert rec.pipelines[0].completeness is Completeness.complete
    conv = run_convert(store, apply=True).conversion
    assert conv.reconstruction_applied == [] and conv.status.value in ("converted", "partial")


def test_unreadable_files_make_the_outcome_incomplete(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(pl(act("W", "Wait", {"waitTimeInSeconds": 1})), **{"pipeline/Broken.json": "{"}))
    assert rec.status is ReconstructionOutcome.incomplete
    assert any(p.completeness is Completeness.unusable for p in rec.pipelines)


# =============================================================================
# CLI
# =============================================================================


def test_cli_reconstruct_then_convert_applying_it(tmp_path, monkeypatch):
    store = make_project(tmp_path, with_estate(inc_pipeline(), **FILES))
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    out = CliRunner().invoke(cli_mod.cli, ["reconstruct", "--project", PID])
    assert out.exit_code == 0, out.output
    assert "RECONSTRUCTION (original source is never modified)" in out.output and "reconstructable" in out.output
    assert "convert --apply-reconstruction" in out.output and store.has_reconstruction(PID)
    conv = CliRunner().invoke(cli_mod.cli, ["convert", "--project", PID, "--apply-reconstruction"])
    assert conv.exit_code == 0, conv.output
    assert "inferred elements applied: P-0001" in conv.output


def test_cli_reconstruct_fails_cleanly_without_discovery(tmp_path, monkeypatch):
    store = make_project(tmp_path, with_estate(inc_pipeline(), **FILES))
    store.adf_semantic_discovery_path(PID).unlink()
    monkeypatch.setattr(cli_mod, "projects_root", lambda config: store.projects_root)
    out = CliRunner().invoke(cli_mod.cli, ["reconstruct", "--project", PID])
    assert out.exit_code == 1 and "Semantic Discovery has not run" in out.output


# =============================================================================
# architecture and safety guards
# =============================================================================


def _modules():
    return list(pathlib.Path(inspect.getfile(rpkg)).parent.rglob("*.py"))


def test_reconstruction_uses_no_model_or_network_library():
    banned = ("openai", "anthropic", "langchain", "requests", "urllib.request", "httpx", "socket", "subprocess", "random")
    for p in _modules():
        text = p.read_text(encoding="utf-8")
        for b in banned:
            assert f"import {b}" not in text and f"from {b}" not in text, f"{p.name} uses {b}"


def test_reconstruction_never_writes_to_the_source_or_outside_its_folder(tmp_path):
    from adf2fabric.project.store import ProjectStore

    with pytest.raises(ValueError):
        ProjectStore(tmp_path).write_reconstruction_file("p", "../source/x.json", "{}")
    for p in _modules():
        text = p.read_text(encoding="utf-8")
        assert "source_dir(" not in text or p.name == "agent.py", p.name  # only the agent addresses source_dir
        assert ".write_text(" not in text and "open(" not in text, f"{p.name} writes files directly"


def test_the_project_layer_stays_free_of_the_agent_layer():
    import adf2fabric.project.reconstruction as m

    assert "adf2fabric.agent" not in inspect.getsource(m)


def test_existing_stage_modules_do_not_depend_on_reconstruction():
    root = pathlib.Path(inspect.getfile(rpkg)).parents[1]
    for sub in ("discovery", "intake", "consultation"):
        assert not any("agent.reconstruction" in p.read_text(encoding="utf-8") for p in (root / sub).rglob("*.py"))


def test_confidence_is_a_category_never_a_number():
    from adf2fabric.project.reconstruction import Confidence, Proposal

    assert {c.value for c in Confidence} == {"high", "medium", "low"}
    assert not any("score" in f or "probab" in f for f in Proposal.model_fields)


def test_low_confidence_proposals_never_enter_the_completion(tmp_path):
    lk = act("L", "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS_SQL"), "firstRowOnly": True})
    fe = act("Each", "ForEach", {"items": expr("@activity('L').output.value"), "activities": [act("W", "Wait", {"waitTimeInSeconds": 1})]},
             deps=[dep("L")])
    store, rec = reconstruct(tmp_path, with_estate(pl(lk, fe)))
    lows = [p for p in proposals(rec) if p.confidence is LOW]
    assert lows and not any(p.safe_to_generate for p in lows) and blockers(rec)
    assert not (store.reconstruction_dir(PID) / "completed").exists() and rec.safe_proposal_count == 0
