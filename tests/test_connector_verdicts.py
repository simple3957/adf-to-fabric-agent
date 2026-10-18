"""The ADF linked-service type table: what the web check on 2026-10-16 changed, and what must not regress."""

from __future__ import annotations

import pytest

from adf2fabric.agent.conversion import connectors as cn


@pytest.mark.parametrize("adf_type", ["CosmosDbMongoDbApi", "MongoDbV2", "MongoDbAtlas", "AzureSearch", "Office365",
                                      "Lakehouse", "Warehouse", "SapOpenHub", "AzureDatabricks"])
def test_connectors_that_fabric_has_but_the_table_missed_are_now_supported(adf_type):
    kind, name = cn.verdict(adf_type)
    assert kind == "supported" and name


def test_sap_bw_via_mdx_is_no_longer_reported_as_supported():
    # SapBw (SAP BW via MDX) was mapped to "SAP BW Open Hub" by mistake; Open Hub is SapOpenHub, and MDX is not in Fabric
    # pipelines per Microsoft's comparison. Matching is case-insensitive: the ARM reference spells it SapBW.
    for spelling in ("SapBw", "SapBW"):
        kind, why = cn.verdict(spelling)
        assert kind == "unlisted" and "SAP BW via MDX" in why and "not available in Fabric pipelines" in why


@pytest.mark.parametrize("adf_type", ["Hive", "Impala", "Netezza", "Spark", "Sybase", "GoogleAdWords", "Hubspot", "Jira",
                                      "QuickBooks", "SapCloudForCustomer", "SapEcc", "Shopify", "Square", "Web", "Xero"])
def test_types_marked_not_available_for_pipelines_say_so_without_being_called_unsupported(adf_type):
    kind, why = cn.verdict(adf_type)
    assert kind == "unlisted"  # a Dataflow Gen2 connector may exist: verify, do not refuse
    assert "2026-07-08" in why and "Dataflow Gen2" in why


def test_retired_sources_are_still_unsupported_and_unknown_types_are_still_unlisted():
    assert cn.verdict("AzureDataLakeStore")[0] == "unsupported"
    assert cn.verdict("AzureDataLakeAnalytics")[0] == "unsupported"
    kind, why = cn.verdict("SomethingNew")
    assert kind == "unlisted" and "not named" in why
    assert cn.verdict(None)[0] == "unlisted"


def test_the_tables_do_not_overlap_so_a_type_has_one_verdict():
    assert not set(cn.SUPPORTED) & set(cn.NOT_IN_PIPELINES)
    assert not set(cn.SUPPORTED) & set(cn.UNSUPPORTED)
    assert not set(cn.NOT_IN_PIPELINES) & set(cn.UNSUPPORTED)


def test_the_ones_the_tool_can_create_are_all_recognised_as_supported():
    from adf2fabric.connections.catalog import CONNECTORS

    # 'azuresynapseanalytics' is in the connector catalog but is not a type string Microsoft's reference confirms: Synapse
    # dedicated pools are AzureSqlDW. It is exempt here until someone verifies it, not silently added to the table.
    unconfirmed = {"azuresynapseanalytics"}
    for c in CONNECTORS:
        for t in c.adf_types:
            if t not in unconfirmed:
                assert cn.verdict(t)[0] == "supported", t
