"""The structural reconstruction rules: what each infers, from which evidence, at which confidence."""

from __future__ import annotations

import json

import pytest

from adf2fabric.agent.reconstruction.analyzer import _dry_run_patches
from adf2fabric.project.reconstruction import (
    Completeness, Confidence, Evidence, EvidenceKind, Patch, PatchOp, Proposal)
from tests.reconstruction_support import (
    DS_LAKE, DS_SQL, ESTATE, HIGH, LOW, MEDIUM, SQL_LS, act, blockers, completed, copy_activity, dataset, dep, expr,
    findings, fingerprint, linked_service, make_project, original, pipeline_of, pl, proposals, reconstruct, ref,
    with_estate,
)

WAIT = {"waitTimeInSeconds": 1}
SRC = {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT 1"}


def lake_ref(**args):
    return ref("Dataset", "DS_LAKE", folder="x", **args)


def copy_missing_sink(name="Copy"):
    return act(name, "Copy", {"source": SRC}, inputs=[ref("Dataset", "DS_SQL")], outputs=[lake_ref()])


# =============================================================================
# Copy endpoints
# =============================================================================


def test_missing_sink_is_derived_from_the_dataset_and_linked_service_types(tmp_path):
    store, rec = reconstruct(tmp_path, with_estate(pl(copy_missing_sink())))
    [p] = proposals(rec, "copy_endpoint_from_dataset")
    assert p.confidence is HIGH and p.safe_to_generate and p.action == "set_property"
    assert p.proposed == {"type": "ParquetSink", "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                          "formatSettings": {"type": "ParquetWriteSettings"}}
    assert {e.kind.value for e in p.evidence} == {"explicit_property", "referenced_artifact"}
    assert p.patches[0].file == "pipeline/PL.json" and p.patches[0].ops[0].path.endswith("/typeProperties/sink")
    # the completion has the sink; the ORIGINAL file does not
    assert completed(store, "pipeline/PL.json")["properties"]["activities"][0]["typeProperties"]["sink"]["type"] == "ParquetSink"
    assert "sink" not in original(store, "pipeline/PL.json")["properties"]["activities"][0]["typeProperties"]


def test_an_existing_sink_object_keeps_its_settings_and_only_gains_the_type(tmp_path):
    c = act("Copy", "Copy", {"source": SRC, "sink": {"writeBehavior": "insert", "tableOption": "autoCreate"}},
            inputs=[ref("Dataset", "DS_LAKE", folder="x")], outputs=[ref("Dataset", "DS_SQL")])
    _, rec = reconstruct(tmp_path, with_estate(pl(c)))
    [p] = [p for p in proposals(rec, "copy_endpoint_from_dataset") if "sink" in p.target_path]
    assert p.proposed == {"writeBehavior": "insert", "tableOption": "autoCreate", "type": "AzureSqlSink"}


def test_unmapped_store_leaves_information_required_and_nothing_is_generated(tmp_path):
    odd = linked_service("LS_ODD", "SomeStore")
    ds = dataset("DS_ODD", "Parquet", "LS_ODD", {"location": {"type": "X"}})
    c = act("Copy", "Copy", {"source": SRC}, inputs=[ref("Dataset", "DS_SQL")], outputs=[ref("Dataset", "DS_ODD")])
    store, rec = reconstruct(tmp_path, with_estate(pl(c), **{"linkedService/LS_ODD.json": odd, "dataset/DS_ODD.json": ds}))
    [p] = proposals(rec, "copy_endpoint_from_dataset")
    assert p.confidence is LOW and not p.safe_to_generate and "store settings type" in p.information_required[0]
    assert p.not_generated_reason and not (store.reconstruction_dir("test-proj") / "completed").exists()


def test_unknown_dataset_type_becomes_a_blocker_not_a_guess(tmp_path):
    ds = dataset("DS_X", "MysteryTable", "LS_SQL", {})
    c = act("Copy", "Copy", {"source": SRC}, inputs=[ref("Dataset", "DS_SQL")], outputs=[ref("Dataset", "DS_X")])
    _, rec = reconstruct(tmp_path, with_estate(pl(c), **{"dataset/DS_X.json": ds}))
    assert not proposals(rec, "copy_endpoint_from_dataset")
    assert any("MysteryTable" in " ".join(b.information_required) for b in blockers(rec))


def _peer(name, sink="DS_LAKE"):
    return pl(act("Copy", "Copy", {"source": SRC, "sink": {"type": "ParquetSink"}},
                  inputs=[ref("Dataset", "DS_SQL")], outputs=[ref("Dataset", sink, folder="x")]), name=name)


def _no_sink():
    return pl(act("Copy", "Copy", {"source": SRC}, inputs=[ref("Dataset", "DS_SQL")]), name="PL_Gap")


def test_missing_sink_dataset_from_two_agreeing_peers_is_medium_and_generated(tmp_path):
    store, rec = reconstruct(tmp_path, with_estate(_no_sink(), _peer("PL_P1"), _peer("PL_P2")))
    [p] = proposals(rec, "copy_endpoint_from_peers")
    assert p.confidence is MEDIUM and p.safe_to_generate
    assert [e.kind.value for e in p.evidence] == ["repeated_pattern", "repeated_pattern"]
    done = completed(store, "pipeline/PL_Gap.json")["properties"]["activities"][0]
    assert done["outputs"][0]["referenceName"] == "DS_LAKE" and done["typeProperties"]["sink"]["type"] == "ParquetSink"


def test_a_single_peer_is_not_enough(tmp_path):
    store, rec = reconstruct(tmp_path, with_estate(_no_sink(), _peer("PL_P1")))
    [p] = proposals(rec, "copy_endpoint_from_peers")
    assert p.confidence is LOW and not p.safe_to_generate
    assert pipeline_of(rec, "PL_Gap").completeness in (Completeness.blocked, Completeness.partially_reconstructable)


def test_conflicting_peers_yield_alternatives_and_no_choice(tmp_path):
    other = dataset("DS_LAKE2", "Parquet", "LS_LAKE", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "r"}})
    files = with_estate(_no_sink(), _peer("PL_P1"), _peer("PL_P2", "DS_LAKE2"), **{"dataset/DS_LAKE2.json": other})
    _, rec = reconstruct(tmp_path, files)
    [p] = proposals(rec, "copy_endpoint_from_peers")
    assert p.confidence is LOW and not p.safe_to_generate
    assert sorted(a.description for a in p.alternatives) == ["use dataset 'DS_LAKE'", "use dataset 'DS_LAKE2'"]


def test_a_copy_with_no_dataset_on_either_side_is_a_blocker(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(pl(act("Copy", "Copy", {}))))
    assert not proposals(rec) and any("no dataset on either side" in b.reason for b in blockers(rec))


# =============================================================================
# dependencies implied by references
# =============================================================================


def _lookup(name="L"):
    return act(name, "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS_SQL")})


def _reader(name, text, deps=None, **kw):
    return act(name, "SetVariable", {"variableName": "v", "value": expr(text)}, deps=deps or [], **kw)


def test_reading_an_output_implies_a_succeeded_dependency(tmp_path):
    p = pl(_lookup(), _reader("R", "@activity('L').output.firstRow.x"), variables={"v": "String"})
    store, rec = reconstruct(tmp_path, with_estate(p))
    [pr] = proposals(rec, "dependency_from_reference")
    assert pr.confidence is HIGH and pr.safe_to_generate and pr.activity_id == "PL/R"
    assert pr.proposed == {"activity": "L", "dependencyConditions": ["Succeeded"]}
    assert pr.evidence[0].kind is EvidenceKind.same_pipeline_reference and "activity('L').output.firstRow.x" in pr.evidence[0].detail
    assert completed(store, "pipeline/PL.json")["properties"]["activities"][1]["dependsOn"] == [pr.proposed]
    assert original(store, "pipeline/PL.json")["properties"]["activities"][1]["dependsOn"] == []


def test_reading_the_error_implies_a_failed_condition_and_both_implies_completed(tmp_path):
    p = pl(act("Risky", "Wait", WAIT),
           _reader("OnErr", "@activity('Risky').error.message"),
           _reader("Either", "@concat(activity('Risky').error.message, activity('Risky').output.x)"),
           variables={"v": "String"})
    _, rec = reconstruct(tmp_path, with_estate(p))
    by = {pr.activity_id: pr for pr in proposals(rec, "dependency_from_reference")}
    assert by["PL/OnErr"].proposed["dependencyConditions"] == ["Failed"]
    assert by["PL/Either"].proposed["dependencyConditions"] == ["Completed"]


def test_a_nested_read_adds_the_dependency_to_the_container(tmp_path):
    inner = _reader("Inner", "@activity('L').output.firstRow.x")
    each = act("Each", "ForEach", {"items": expr("@createArray(1)"), "activities": [inner]})
    store, rec = reconstruct(tmp_path, with_estate(pl(_lookup(), each, variables={"v": "String"})))
    [pr] = proposals(rec, "dependency_from_reference")
    assert pr.activity_id == "PL/Each" and "dependsOn" in pr.target_path  # the loop waits, not the child
    assert completed(store, "pipeline/PL.json")["properties"]["activities"][1]["dependsOn"][0]["activity"] == "L"


def test_existing_transitive_dependencies_need_nothing(tmp_path):
    p = pl(_lookup("A"), act("B", "Wait", WAIT, deps=[dep("A")]),
           _reader("C", "@activity('A').output.firstRow.x", deps=[dep("B")]), variables={"v": "String"})
    _, rec = reconstruct(tmp_path, with_estate(p))
    assert not findings(rec, "dependency_missing_for_reference") and not proposals(rec, "dependency_from_reference")


def test_a_dependency_that_would_create_a_cycle_is_refused(tmp_path):
    a = act("A", "Wait", WAIT, deps=[dep("B")])
    b = _reader("B", "@activity('A').output.x")
    _, rec = reconstruct(tmp_path, with_estate(pl(a, b, variables={"v": "String"})))
    assert not proposals(rec, "dependency_from_reference")
    assert any("cycle" in b_.reason for b_ in blockers(rec))


def test_reading_an_undefined_activity_is_a_blocker(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(pl(_reader("R", "@activity('Ghost').output.x"), variables={"v": "String"})))
    assert findings(rec, "activity_reference_unresolved") and any("Ghost" in " ".join(b.information_required) for b in blockers(rec))
    assert not proposals(rec, "dependency_from_reference")


# =============================================================================
# output contracts
# =============================================================================


def _foreach_over(lookup_name="L"):
    return act("Each", "ForEach", {"items": expr(f"@activity('{lookup_name}').output.value"),
                                   "activities": [act("W", "Wait", WAIT)]}, deps=[dep(lookup_name)])


def test_consumers_reading_value_fix_an_implicit_first_row_only_with_high_confidence(tmp_path):
    store, rec = reconstruct(tmp_path, with_estate(pl(_lookup(), _foreach_over())))
    [p] = proposals(rec, "lookup_shape_from_consumers")
    assert p.confidence is HIGH and p.proposed is False and p.safe_to_generate
    assert completed(store, "pipeline/PL.json")["properties"]["activities"][0]["typeProperties"]["firstRowOnly"] is False


def test_an_explicit_setting_that_contradicts_its_consumers_is_not_overridden(tmp_path):
    lk = _lookup()
    lk["typeProperties"]["firstRowOnly"] = True
    store, rec = reconstruct(tmp_path, with_estate(pl(lk, _foreach_over())))
    [p] = proposals(rec, "lookup_shape_from_consumers")
    assert p.confidence is LOW and not p.safe_to_generate and len(p.alternatives) == 1
    assert blockers(rec) and not (store.reconstruction_dir("test-proj") / "completed").exists()


def test_consumers_that_disagree_about_the_shape_are_a_blocker(tmp_path):
    first = _reader("First", "@activity('L').output.firstRow.x", deps=[dep("L")])
    _, rec = reconstruct(tmp_path, with_estate(pl(_lookup(), _foreach_over(), first, variables={"v": "String"})))
    assert findings(rec, "contract_lookup_shape") and not proposals(rec, "lookup_shape_from_consumers")


def test_first_row_reads_against_an_explicit_false_are_flagged(tmp_path):
    lk = _lookup()
    lk["typeProperties"]["firstRowOnly"] = False
    first = _reader("First", "@activity('L').output.firstRow.x", deps=[dep("L")])
    _, rec = reconstruct(tmp_path, with_estate(pl(lk, first, variables={"v": "String"})))
    [p] = proposals(rec, "lookup_shape_from_consumers")
    assert p.proposed is True and p.confidence is LOW and not p.safe_to_generate


def test_metadata_fields_that_consumers_read_are_added_to_the_field_list(tmp_path):
    gm = act("GM", "GetMetadata", {"dataset": ref("Dataset", "DS_LAKE", folder="x"), "fieldList": ["exists"]})
    use = act("Each", "ForEach", {"items": expr("@activity('GM').output.childItems"), "activities": [act("W", "Wait", WAIT)]},
              deps=[dep("GM")])
    ok = _reader("Ok", "@activity('GM').output.exists", deps=[dep("GM")])
    store, rec = reconstruct(tmp_path, with_estate(pl(gm, use, ok, variables={"v": "String"})))
    [p] = proposals(rec, "metadata_field_from_reads")
    assert p.proposed == "childItems" and p.confidence is HIGH
    assert completed(store, "pipeline/PL.json")["properties"]["activities"][0]["typeProperties"]["fieldList"] == ["exists", "childItems"]


def test_default_metadata_fields_and_dynamic_field_lists_need_nothing(tmp_path):
    gm = act("GM", "GetMetadata", {"dataset": ref("Dataset", "DS_LAKE", folder="x"), "fieldList": expr("@variables('f')")})
    use = _reader("U", "@activity('GM').output.size", deps=[dep("GM")])
    gm2 = act("GM2", "GetMetadata", {"dataset": ref("Dataset", "DS_LAKE", folder="x")})
    use2 = _reader("U2", "@activity('GM2').output.itemName", deps=[dep("GM2")])
    _, rec = reconstruct(tmp_path, with_estate(pl(gm, use, gm2, use2, variables={"v": "String", "f": "Array"})))
    assert not proposals(rec, "metadata_field_from_reads")


# =============================================================================
# arguments
# =============================================================================

CHILD = pl(act("W", "Wait", WAIT), name="PL_Child", parameters=["env"])


def test_a_missing_child_argument_is_taken_from_the_same_named_parent_parameter(tmp_path):
    parent = pl(act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Child")}), name="PL_Parent", parameters=["env"])
    store, rec = reconstruct(tmp_path, with_estate(parent, CHILD))
    [p] = proposals(rec, "argument_from_context")
    assert p.confidence is MEDIUM and p.safe_to_generate and p.proposed == {"value": "@pipeline().parameters.env", "type": "Expression"}
    kinds = {e.kind.value for e in p.evidence}
    assert kinds == {"parameter", "parent_child"} and p.assumptions
    done = completed(store, "pipeline/PL_Parent.json")["properties"]["activities"][0]["typeProperties"]
    assert done["parameters"]["env"]["value"] == "@pipeline().parameters.env"


def test_no_matching_parameter_means_a_blocker_that_names_the_value_needed(tmp_path):
    parent = pl(act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Child")}), name="PL_Parent")
    _, rec = reconstruct(tmp_path, with_estate(parent, CHILD))
    assert not proposals(rec, "argument_from_context")
    assert any("child parameter 'env'" in " ".join(b.information_required) for b in blockers(rec))


def test_a_child_parameter_with_a_default_is_not_a_gap(tmp_path):
    child = pl(act("W", "Wait", WAIT), name="PL_Child", parameters=["env"])
    child["properties"]["parameters"]["env"]["defaultValue"] = "dev"
    parent = pl(act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL_Child")}), name="PL_Parent")
    _, rec = reconstruct(tmp_path, with_estate(parent, child))
    assert not findings(rec, "child_parameter_not_supplied")


def test_a_missing_dataset_argument_comes_from_the_same_named_pipeline_parameter(tmp_path):
    c = act("Copy", "Copy", {"source": SRC, "sink": {"type": "ParquetSink"}}, inputs=[ref("Dataset", "DS_SQL")],
            outputs=[ref("Dataset", "DS_LAKE")])
    store, rec = reconstruct(tmp_path, with_estate(pl(c, parameters=["folder"])))
    [p] = proposals(rec, "argument_from_context")
    assert p.proposed == {"value": "@pipeline().parameters.folder", "type": "Expression"} and p.confidence is MEDIUM
    done = completed(store, "pipeline/PL.json")["properties"]["activities"][0]["outputs"][0]
    assert done["parameters"] == {"folder": p.proposed}


FILE_DS = dataset("DS_IN", "DelimitedText", "LS_LAKE", {"location": {
    "type": "AzureBlobFSLocation", "fileSystem": "raw", "fileName": expr("@dataset().FileName")}},
    params={"FileName": {"type": "String"}})


def _file_pipeline(*, via_filter=False, param="FileName"):
    gm = act("GetFiles", "GetMetadata", {"dataset": ref("Dataset", "DS_LAKE", folder="in"), "fieldList": ["childItems"]})
    src = "GetFiles"
    mid = []
    if via_filter:
        mid = [act("OnlyFiles", "Filter", {"items": expr("@activity('GetFiles').output.childItems"),
                                           "condition": expr("@equals(item().type, 'File')")}, deps=[dep("GetFiles")])]
        src = "OnlyFiles"
    items = "@activity('OnlyFiles').output.Value" if via_filter else "@activity('GetFiles').output.childItems"
    inner = act("CopyOne", "Copy", {"source": {"type": "DelimitedTextSource"}, "sink": {"type": "AzureSqlSink"}},
                inputs=[ref("Dataset", "DS_IN")], outputs=[ref("Dataset", "DS_SQL")])
    loop = act("Each", "ForEach", {"items": expr(items), "activities": [inner]}, deps=[dep(src)])
    return pl(gm, *mid, loop)


def test_a_file_loop_binds_the_current_item_to_the_file_name_parameter(tmp_path):
    store, rec = reconstruct(tmp_path, with_estate(_file_pipeline(), **{"dataset/DS_IN.json": FILE_DS}))
    [p] = proposals(rec, "argument_from_context")
    assert p.proposed == {"value": "@item().name", "type": "Expression"} and p.confidence is MEDIUM
    assert {e.kind.value for e in p.evidence} == {"same_pipeline_reference", "referenced_artifact", "naming_convention"}
    assert any("name of the file" in a for a in p.assumptions) and p.alternatives
    nested = completed(store, "pipeline/PL.json")["properties"]["activities"][1]["typeProperties"]["activities"][0]
    assert nested["inputs"][0]["parameters"]["FileName"]["value"] == "@item().name"
    assert [pt.name for pt in pipeline_of(rec, "PL").patterns].count("file_ingestion") == 1


def test_the_file_loop_is_followed_through_a_filter(tmp_path):
    _, rec = reconstruct(tmp_path, with_estate(_file_pipeline(via_filter=True), **{"dataset/DS_IN.json": FILE_DS}))
    assert [p.proposed["value"] for p in proposals(rec, "argument_from_context")] == ["@item().name"]


def test_a_parameter_not_named_like_a_file_is_never_bound_to_the_item(tmp_path):
    ds = dataset("DS_IN", "DelimitedText", "LS_LAKE", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "raw",
                 "folderPath": expr("@dataset().Region")}}, params={"Region": {"type": "String"}})
    _, rec = reconstruct(tmp_path, with_estate(_file_pipeline(), **{"dataset/DS_IN.json": ds}))
    assert not proposals(rec, "argument_from_context") and any("Region" in " ".join(b.information_required) for b in blockers(rec))


def test_peers_that_agree_supply_a_dataset_argument_but_conflicting_peers_do_not(tmp_path):
    def user(name, folder):
        return pl(act("Copy", "Copy", {"source": SRC, "sink": {"type": "ParquetSink"}}, inputs=[ref("Dataset", "DS_SQL")],
                      outputs=[ref("Dataset", "DS_LAKE", folder=folder)]), name=name)
    gap = pl(act("Copy", "Copy", {"source": SRC, "sink": {"type": "ParquetSink"}}, inputs=[ref("Dataset", "DS_SQL")],
                 outputs=[ref("Dataset", "DS_LAKE")]), name="PL_Gap")
    _, agree = reconstruct(tmp_path / "a", with_estate(gap, user("P1", "raw"), user("P2", "raw")))
    [p] = proposals(agree, "argument_from_context")
    assert p.proposed == "raw" and p.confidence is MEDIUM
    _, split = reconstruct(tmp_path / "b", with_estate(gap, user("P1", "raw"), user("P2", "curated")))
    assert not proposals(split, "argument_from_context")
    assert any("different values" in b.reason for b in blockers(split))


# =============================================================================
# renamed references
# =============================================================================


def test_a_separator_only_difference_is_corroborated_by_how_the_dataset_is_used(tmp_path):
    c = copy_activity("Copy")
    c["inputs"] = [ref("Dataset", "DS-SQL")]
    store, rec = reconstruct(tmp_path, with_estate(pl(c)))
    [p] = proposals(rec, "missing_artifact_reference")
    assert p.proposed == "DS_SQL" and p.confidence is MEDIUM and p.safe_to_generate
    assert {e.kind.value for e in p.evidence} == {"naming_convention", "referenced_artifact"}
    done = completed(store, "pipeline/PL.json")["properties"]["activities"][0]["inputs"][0]["referenceName"]
    assert done == "DS_SQL" and original(store, "pipeline/PL.json")["properties"]["activities"][0]["inputs"][0]["referenceName"] == "DS-SQL"


def test_a_name_match_without_corroboration_is_not_applied(tmp_path):
    lk = act("L", "Lookup", {"source": {"type": "AzureSqlSource"}, "dataset": ref("Dataset", "DS-SQL")})
    _, rec = reconstruct(tmp_path, with_estate(pl(lk)))
    [p] = proposals(rec, "missing_artifact_reference")
    assert p.confidence is LOW and not p.safe_to_generate and blockers(rec)


def test_several_name_matches_are_never_chosen_between(tmp_path):
    c = copy_activity("Copy")
    c["inputs"] = [ref("Dataset", "DS-SQL")]
    twin = dataset("ds.sql", "AzureSqlTable", "LS_SQL", {"table": "t"})
    _, rec = reconstruct(tmp_path, with_estate(pl(c), **{"dataset/ds.sql.json": twin}))
    [p] = proposals(rec, "missing_artifact_reference")
    assert p.confidence is LOW and "2 plausible candidates" in p.confidence_basis and not p.safe_to_generate


def test_a_renamed_child_pipeline_is_matched_when_its_parameters_fit(tmp_path):
    parent = pl(act("Run", "ExecutePipeline", {"pipeline": ref("Pipeline", "PL-Child"),
                                               "parameters": {"env": "dev"}}), name="PL_Parent")
    store, rec = reconstruct(tmp_path, with_estate(parent, CHILD))
    [p] = proposals(rec, "missing_artifact_reference")
    assert p.proposed == "PL_Child" and p.confidence is MEDIUM and p.safe_to_generate
    assert completed(store, "pipeline/PL_Parent.json")["properties"]["activities"][0]["typeProperties"]["pipeline"]["referenceName"] == "PL_Child"


def test_a_genuinely_missing_artifact_gets_no_substitute(tmp_path):
    c = copy_activity("Copy")
    c["inputs"] = [ref("Dataset", "DS_TOTALLY_OTHER")]
    _, rec = reconstruct(tmp_path, with_estate(pl(c)))
    assert not proposals(rec, "missing_artifact_reference") and blockers(rec)


# =============================================================================
# related pipelines of matching structure
# =============================================================================


def _shape(name, items=None):
    fe = act("Each", "ForEach", {"activities": [act("W", "Wait", WAIT)]} if items is None else
             {"items": expr(items), "activities": [act("W", "Wait", WAIT)]})
    return pl(act("Pause", "Wait", WAIT), fe, name=name)


def test_two_identical_siblings_supply_a_missing_required_property(tmp_path):
    files = with_estate(_shape("PL_Gap"), _shape("PL_S1", "@createArray(1, 2)"), _shape("PL_S2", "@createArray(1, 2)"))
    store, rec = reconstruct(tmp_path, files)
    [p] = [p for p in proposals(rec, "property_from_shape_siblings") if p.pipeline == "PL_Gap"]
    assert p.confidence is MEDIUM and p.safe_to_generate
    assert p.proposed == {"value": "@createArray(1, 2)", "type": "Expression"}
    assert {e.kind.value for e in p.evidence} == {"related_pipeline"}
    assert completed(store, "pipeline/PL_Gap.json")["properties"]["activities"][1]["typeProperties"]["items"]["value"] == "@createArray(1, 2)"


def test_one_sibling_or_disagreeing_siblings_are_not_enough(tmp_path):
    one = reconstruct(tmp_path / "a", with_estate(_shape("PL_Gap"), _shape("PL_S1", "@createArray(1)")))[1]
    [p] = [x for x in proposals(one, "property_from_shape_siblings") if x.pipeline == "PL_Gap"]
    assert p.confidence is LOW and not p.safe_to_generate
    two = reconstruct(tmp_path / "b", with_estate(_shape("PL_Gap"), _shape("PL_S1", "@createArray(1)"),
                                                   _shape("PL_S2", "@createArray(2)")))[1]
    [q] = [x for x in proposals(two, "property_from_shape_siblings") if x.pipeline == "PL_Gap"]
    assert q.confidence is LOW and len(q.alternatives) == 2 and not q.safe_to_generate


def test_secret_looking_sibling_values_are_never_copied(tmp_path):
    def web(name, with_url=True):
        tp = {"method": "GET", "headers": {"Authorization": "Bearer abc"}}
        if with_url:
            tp["url"] = "https://h/x"
        return pl(act("Call", "WebActivity", tp), act("W", "Wait", WAIT), name=name)
    sib = web("PL_S1", True), web("PL_S2", True)
    _, rec = reconstruct(tmp_path, with_estate(web("PL_Gap", False), *sib))
    assert not [p for p in proposals(rec, "property_from_shape_siblings") if "Authorization" in json.dumps(p.proposed)]


# =============================================================================
# safety nets
# =============================================================================


def test_overlapping_proposals_cannot_both_be_applied():
    doc = {"properties": {"activities": [{"name": "A", "typeProperties": {"x": 1}}]}}

    class FakeEv:
        def doc(self, f):
            return doc

    def prop(pid, value):
        return Proposal(id=pid, rule="r", pipeline="P", missing_element="m", action="set_property", target_path="t",
                        confidence=Confidence.high, confidence_basis="HIGH: x", safe_to_generate=True,
                        patches=[Patch(file="f", ops=[PatchOp(op="replace", path="/properties/activities/0/typeProperties/x", value=value)])])

    a, b = prop("P-0001", 2), prop("P-0002", 3)
    _dry_run_patches(FakeEv(), [a, b])
    assert a.safe_to_generate and not b.safe_to_generate and "already changed by P-0001" in b.not_generated_reason


def test_a_patch_that_cannot_apply_is_demoted_not_dropped():
    class FakeEv:
        def doc(self, f):
            return {"properties": {}}

    p = Proposal(id="P-0001", rule="r", pipeline="P", missing_element="m", action="set_property", target_path="t",
                 confidence=Confidence.high, confidence_basis="HIGH: x", safe_to_generate=True,
                 patches=[Patch(file="f", ops=[PatchOp(op="replace", path="/properties/nope/x", value=1)])])
    _dry_run_patches(FakeEv(), [p])
    assert not p.safe_to_generate and "cannot be applied" in p.not_generated_reason


def test_the_original_source_is_never_modified_by_any_stage(tmp_path):
    files = with_estate(copy_missing_sink() and pl(copy_missing_sink()), _peer("PL_P1"), _peer("PL_P2"))
    store = make_project(tmp_path, files)
    before = fingerprint(store.source_dir("test-proj"))
    from adf2fabric.agent.reconstruction.agent import ReconstructionAgent
    from adf2fabric.agent.reconstruction.models import ReconstructionRequest

    r = ReconstructionAgent(store).run(ReconstructionRequest(project_name="test-proj"))
    assert r.success and proposals(r.reconstruction)
    assert fingerprint(store.source_dir("test-proj")) == before
    assert not any("reconstruction" in p.parts for p in store.source_dir("test-proj").rglob("*"))
