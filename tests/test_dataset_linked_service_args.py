"""Datasets that pass arguments to a parameterised linked service: a Fabric connection is fixed, so different values need different connections."""

from __future__ import annotations

from tests.conversion_support import act, by_name, convert_one, dataset, find, linked_service, pipe, ref

GUID = "11111111-1111-1111-1111-111111111111"


def _ls():
    ls = linked_service("LS_SQLP", "AzureSqlDatabase")
    ls["properties"]["parameters"] = {"DBName": {"type": "String"}}
    return ls


def _ds(name, db):
    d = dataset(name, "AzureSqlTable", "LS_SQLP", {"schema": "dbo", "table": "T"})
    d["properties"]["linkedServiceName"]["parameters"] = {"DBName": db}
    return d


def _copy(name, src, dst):
    return act(name, "Copy", {"source": {"type": "AzureSqlSource"}, "sink": {"type": "AzureSqlSink"}}, inputs=[ref("Dataset", src)], outputs=[ref("Dataset", dst)])


def _conn(content, activity):
    return find(content, activity)["typeProperties"]["source"]["datasetSettings"]["externalReferences"]["connection"]


def test_datasets_passing_different_databases_get_different_connections_never_one_for_all():
    rec, c = convert_one(pipe(_copy("C1", "DS1", "DS_OUT"), _copy("C2", "DS2", "DS_OUT")),
                         datasets=[_ds("DS1", "SalesDB"), _ds("DS2", "HRDB"), _ds("DS_OUT", "Lake")], linked_services=[_ls()])
    assert _conn(c, "C1") == "<connection:LS_SQLP?DBName=SalesDB>" and _conn(c, "C2") == "<connection:LS_SQLP?DBName=HRDB>"
    assert {"LS_SQLP?DBName=SalesDB", "LS_SQLP?DBName=HRDB", "LS_SQLP?DBName=Lake"} <= {p.name for p in rec.prerequisites}
    acts = by_name(rec)
    assert "linked_service_arguments_split" in {w.code for w in acts["C1"].warnings}
    assert not rec.runtime_ready  # three connections still to bind


def test_each_value_is_bound_on_its_own():
    _, c = convert_one(pipe(_copy("C1", "DS1", "DS_OUT"), _copy("C2", "DS2", "DS_OUT")),
                       datasets=[_ds("DS1", "SalesDB"), _ds("DS2", "HRDB"), _ds("DS_OUT", "Lake")], linked_services=[_ls()],
                       resolutions=[{"type": "LinkedServiceToConnectionId", "key": "LS_SQLP?DBName=SalesDB", "value": GUID}])
    assert _conn(c, "C1") == GUID and _conn(c, "C2") == "<connection:LS_SQLP?DBName=HRDB>"


def test_one_value_everywhere_keeps_the_plain_binding_and_says_the_connection_is_fixed_to_it():
    rec, c = convert_one(pipe(_copy("C1", "DS1", "DS2")), datasets=[_ds("DS1", "SalesDB"), _ds("DS2", "SalesDB")], linked_services=[_ls()])
    assert _conn(c, "C1") == "<connection:LS_SQLP>"
    assert "linked_service_arguments_fixed" in {w.code for w in by_name(rec)["C1"].warnings}


def test_a_value_computed_at_run_time_cannot_be_bound_and_is_manual():
    d = dataset("DS1", "AzureSqlTable", "LS_SQLP", {"schema": "dbo", "table": "T"})
    d["properties"]["linkedServiceName"]["parameters"] = {"DBName": {"value": "@pipeline().parameters.db", "type": "Expression"}}
    rec, _ = convert_one(pipe(_copy("C1", "DS1", "DS2")), datasets=[d, _ds("DS2", "X")], linked_services=[_ls()])
    a = by_name(rec)["C1"]
    assert "linked_service_argument_dynamic" in {w.code for w in a.warnings} and a.status.value == "manual_required"


def test_a_dataset_without_arguments_is_unchanged():
    d = dataset("DS1", "AzureSqlTable", "LS_SQLP", {"schema": "dbo", "table": "T"})
    rec, c = convert_one(pipe(_copy("C1", "DS1", "DS1")), datasets=[d], linked_services=[_ls()])
    assert _conn(c, "C1") == "<connection:LS_SQLP>" and not {w.code for w in by_name(rec)["C1"].warnings} & {"linked_service_arguments_split", "linked_service_arguments_fixed"}


def _sp(name, db):
    a = act(name, "SqlServerStoredProcedure", {"storedProcedureName": "dbo.p"})
    a["type"] = "SqlServerStoredProcedure"
    a["linkedServiceName"] = ref("LinkedService", "LS_SQLP", DBName=db)
    return a


def _connection_of(content, activity):
    return find(content, activity)["externalReferences"]["connection"]


def test_activities_passing_different_databases_straight_to_the_linked_service_get_different_connections():
    rec, c = convert_one(pipe(_sp("P1", "SalesDB"), _sp("P2", "HRDB")), linked_services=[_ls()])
    assert _connection_of(c, "P1") == "<connection:LS_SQLP?DBName=SalesDB>" and _connection_of(c, "P2") == "<connection:LS_SQLP?DBName=HRDB>"
    assert "linked_service_arguments_split" in {w.code for w in by_name(rec)["P1"].warnings}


def test_a_dataset_and_an_activity_count_towards_the_same_linked_service():
    rec, c = convert_one(pipe(_copy("C1", "DS1", "DS1"), _sp("P1", "HRDB")), datasets=[_ds("DS1", "SalesDB")], linked_services=[_ls()])
    assert _conn(c, "C1") == "<connection:LS_SQLP?DBName=SalesDB>" and _connection_of(c, "P1") == "<connection:LS_SQLP?DBName=HRDB>"


def test_an_activity_with_a_computed_argument_is_manual():
    a = _sp("P1", "x")
    a["linkedServiceName"]["parameters"] = {"DBName": {"value": "@pipeline().parameters.db", "type": "Expression"}}
    rec, _ = convert_one(pipe(a), linked_services=[_ls()])
    assert "linked_service_argument_dynamic" in {w.code for w in by_name(rec)["P1"].warnings}


def _lookup_ds(type_, ls_type, tp):
    d = dataset("DS_X", type_, "LS_X", tp)
    l = linked_service("LS_X", ls_type, {"url": "https://x"})
    a = act("Look", "GetMetadata", {"dataset": ref("Dataset", "DS_X"), "fieldList": ["exists"]})
    return convert_one(pipe(a), datasets=[d], linked_services=[l])


def test_a_dataset_type_that_has_not_run_in_fabric_says_so_and_a_verified_one_does_not():
    rec, _ = _lookup_ds("Json", "AzureBlobStorage", {"location": {"type": "AzureBlobStorageLocation", "container": "c"}})
    assert "dataset_type_not_live_verified" in {w.code for w in by_name(rec)["Look"].warnings}
    rec, _ = _lookup_ds("Parquet", "AzureBlobFS", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "c"}})
    assert "dataset_type_not_live_verified" not in {w.code for w in by_name(rec)["Look"].warnings}


def test_the_legacy_structure_property_carries_column_names_only():
    d = dataset("DS_S", "AzureSqlTable", "LS_SQL", {"schema": "dbo", "table": "T"})
    d["properties"]["structure"] = [{"name": "Id", "type": "Int32"}, {"name": "Name", "type": "String"}]
    a = act("C", "Copy", {"source": {"type": "AzureSqlSource"}, "sink": {"type": "AzureSqlSink"}}, inputs=[ref("Dataset", "DS_S")], outputs=[ref("Dataset", "DS_S")])
    _, c = convert_one(pipe(a), datasets=[d], linked_services=[linked_service("LS_SQL", "AzureSqlDatabase")])
    assert find(c, "C")["typeProperties"]["source"]["datasetSettings"]["schema"] == [{"name": "Id"}, {"name": "Name"}]
