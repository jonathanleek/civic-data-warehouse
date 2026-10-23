"""End-to-end tests of the staging -> current transforms on synthetic data.

See fixtures.py for the scenario each account represents.
"""

import pytest

from .conftest import load_staging, query, reset_database, run_layers, scalar

ALL_LAYERS = ["reference", "entities", "spine", "events", "history", "qa"]


@pytest.fixture(scope="module")
def loaded(pg):
    reset_database(pg)
    load_staging(pg)
    run_layers(pg, ALL_LAYERS)
    return pg


# --- reference ---------------------------------------------------------------


def test_zoning_class_includes_codes_missing_from_decode(loaded):
    rows = dict(query(loaded, "SELECT code, zoning_class FROM current.zoning_class"))
    assert rows == {
        "A": "Single-Family Dwelling District",
        "H": "Area Commercial District",
        "Z": None,
    }


def test_lookup_keys_are_namespaced(loaded):
    ids = [
        r[0] for r in query(loaded, "SELECT zoning_class_id FROM current.zoning_class")
    ]
    assert all(i.startswith("840.29510.zoning_class.") for i in ids)
    assert scalar(loaded, "SELECT municipality_id FROM current.municipality") == (
        "840.29510.municipality.1"
    )


def test_use_type_unions_decode_and_building_codes(loaded):
    rows = dict(query(loaded, "SELECT code, use_type FROM current.use_type"))
    assert rows["1110"] == "SINGLE FAMILY"
    assert rows["6000"] is None  # section use code with no decode row
    assert rows["3997"] is None


def test_permit_type_per_feed(loaded):
    rows = set(query(loaded, "SELECT category, code FROM current.permit_type"))
    assert rows == {
        ("building", "NEW CONSTRUCTION"),
        ("building", None),
        ("electrical", "SERVICE UPGRADE"),
    }


def test_decode_reject_is_logged_and_reconciled(loaded):
    assert (
        scalar(
            loaded,
            "SELECT count(*) FROM current.exclusion_log "
            "WHERE source_table = 'prclcode_cdzoning' AND disposition = 'rejected'",
        )
        == 1
    )
    assert (
        scalar(loaded, "SELECT bool_and(balanced) FROM etl.feed_reconciliation") is True
    )


def test_missing_optional_sources_are_reported(loaded):
    gaps = {
        r[0]
        for r in query(
            loaded,
            "SELECT view_name FROM etl.source_contract_gap WHERE target_alias IS NULL",
        )
    }
    assert "src_permit_plumbing" in gaps


# --- entities ----------------------------------------------------------------


def test_owner_dedup_across_accounts(loaded):
    # A1, A3 and condo unit 52 share owner + mailing address -> one entity.
    assert (
        scalar(
            loaded,
            "SELECT count(*) FROM current.legal_entity WHERE name = 'SMITH JOHN'",
        )
        == 1
    )


def test_owner_mailing_line_kept_verbatim(loaded):
    row = query(
        loaded,
        "SELECT a.raw_line, a.city, a.state, a.zip, a.street_name, e.name2 "
        "FROM current.legal_entity e JOIN current.address a USING (address_id) "
        "WHERE e.name = 'ACME LLC'",
    )
    assert row == [("PO BOX 5", "CHICAGO", "IL", "60601", None, "C/O JANE DOE")]


def test_situs_addresses_componentized(loaded):
    rows = set(
        query(
            loaded,
            "SELECT street_number, street_name_prefix, street_name, street_name_suffix "
            "FROM current.address WHERE street_name = 'OAK'",
        )
    )
    assert rows == {(100, "N", "OAK", "AV"), (102, "N", "OAK", "AV")}


# --- spine -------------------------------------------------------------------


def test_one_parcel_per_real_estate_footprint(loaded):
    handles = [
        r[0]
        for r in query(
            loaded,
            "SELECT municipal_parcel_id FROM current.municipal_parcel_id_mapping "
            "ORDER BY 1",
        )
    ]
    assert handles == [
        "10010000100",
        "10010000200",
        "10010000300",
        "10010000400",
        "10020000500",
        "10030000600",
        "10040000800",
    ]


def test_parcel_ids_follow_cdw_id_standard(loaded):
    rows = dict(
        query(
            loaded,
            "SELECT m.municipal_parcel_id, p.cdw_id FROM current.parcel p "
            "JOIN current.municipal_parcel_id_mapping m ON m.parcel_id = p.cdw_id",
        )
    )
    assert rows["10010000100"] == "840.29510.00000001.0000.00000"


def test_parcel_attributes_verbatim(loaded):
    row = query(
        loaded,
        "SELECT p.ward, p.zip_code, p.census_block, p.frontage, z.code "
        "FROM current.parcel p JOIN current.municipal_parcel_id_mapping m "
        "ON m.parcel_id = p.cdw_id JOIN current.zoning_class z USING (zoning_class_id) "
        "WHERE m.municipal_parcel_id = '10010000200'",
    )
    ward, zip_code, census_block, frontage, zoning = row[0]
    assert (ward, zip_code, census_block, zoning) == (5, "63101", "1001", "H")
    assert str(frontage) == "78.529999"  # numeric, not rounded to int


def _units_by_handle(conn, handle):
    return query(
        conn,
        "SELECT u.cdw_id, u.number_of_units FROM current.unit u "
        "JOIN current.municipal_parcel_id_mapping m "
        "ON m.parcel_id = split_part(u.cdw_id, '.', 1) || '.' || split_part(u.cdw_id, '.', 2) "
        "|| '.' || split_part(u.cdw_id, '.', 3) || '.0000.00000' "
        "WHERE m.municipal_parcel_id = %s ORDER BY 1",
        (handle,),
    )


def test_simple_buildings_and_sections(loaded):
    units = [u[0] for u in _units_by_handle(loaded, "10010000200")]
    assert units == [
        "840.29510.00000002.0001.00001",  # building 1: single unit
        "840.29510.00000002.0002.00001",  # building 2: one unit per section
        "840.29510.00000002.0002.00002",
        "840.29510.00000002.0002.00003",
    ]


def test_multifamily_is_one_unit_with_count(loaded):
    assert _units_by_handle(loaded, "10010000300") == [
        ("840.29510.00000003.0001.00001", 4)
    ]


def test_vacant_lot_has_no_building(loaded):
    assert _units_by_handle(loaded, "10010000400") == []
    assert (
        scalar(
            loaded,
            "SELECT count(*) FROM current.building WHERE parcel_cdw_id = "
            "'840.29510.00000004.0000.00000'",
        )
        == 0
    )


def test_condo_synthesized_building_and_units(loaded):
    units = [u[0] for u in _units_by_handle(loaded, "10020000500")]
    assert units == [
        "840.29510.00000005.0001.00001",
        "840.29510.00000005.0001.00002",
        "840.29510.00000005.0001.00003",
    ]
    secondary = query(
        loaded,
        "SELECT a.secondary_designator FROM current.unit u "
        "JOIN current.address a USING (address_id) "
        "WHERE u.building_cdw_id = '840.29510.00000005.0001.00000' ORDER BY u.cdw_id",
    )
    assert [s[0] for s in secondary] == ["1A", "1B", "2A"]


def test_every_building_has_a_unit(loaded):
    assert (
        scalar(
            loaded,
            "SELECT count(*) FROM current.building b WHERE NOT EXISTS "
            "(SELECT 1 FROM current.unit u WHERE u.building_cdw_id = b.cdw_id)",
        )
        == 0
    )


def test_residual_account_becomes_secondary_owner(loaded):
    rows = query(
        loaded,
        "SELECT s.parcel_id, e.name FROM current.secondary_owner s "
        "JOIN current.legal_entity e ON e.legal_entity_id = s.owner_id",
    )
    assert rows == [("840.29510.00000006.0000.00000", "JONES ROBERT")]


def test_account_dispositions(loaded):
    rows = set(
        query(
            loaded,
            "SELECT source_natural_key, disposition, reason FROM current.exclusion_log "
            "WHERE source_table = 'prcl_prcl'",
        )
    )
    assert rows == {
        ("1001.00/10/0", "rejected", "duplicate account row"),
        ("1001.00/21/0", "excluded", "non-real-estate land use 3997"),
        ("1003.00/70/0", "excluded", "non-real-estate land use 6312"),
        ("1003.00/71/0", "excluded", "footprint account is non-real-estate"),
        ("1005.00/90/0", "rejected", "account has no handle"),
        ("1005.00/95/0", "rejected", "no footprint account for handle"),
        ("1005.00/97/0", "rejected", "handle has leading/trailing whitespace"),
    }


def test_mixed_condo_footprint_building_rejected(loaded):
    assert query(
        loaded,
        "SELECT reason FROM current.exclusion_log WHERE source_table = 'prcl_bldgall' "
        "AND source_natural_key = '1004.00/80/0#1'",
    ) == [("condo footprint has its own building (mixed footprint)",)]


def test_capture_all_balances_for_every_feed(loaded):
    unbalanced = query(
        loaded, "SELECT feed FROM etl.feed_reconciliation WHERE NOT balanced"
    )
    assert unbalanced == []
    feeds = {r[0] for r in query(loaded, "SELECT feed FROM etl.feed_reconciliation")}
    assert {"prcl_prcl", "prcl_bldgall", "prcl_bldgres", "prcl_bldgsect"} <= feeds


# --- events ------------------------------------------------------------------


def test_assessment_linked_and_dispositioned(loaded):
    rows = set(
        query(
            loaded,
            "SELECT parcel_id, assessment_amount, land_value, improvement_value "
            "FROM current.assessment",
        )
    )
    assert ("840.29510.00000002.0000.00000", 91310, 15100, 76210) in rows
    assert len(rows) == 3  # billboard account excluded, no-handle account rejected
    assert set(
        query(
            loaded,
            "SELECT disposition FROM current.exclusion_log "
            "WHERE source_table = 'prcl_prclasmt'",
        )
    ) == {("excluded",), ("rejected",)}


def test_tax_delinquency_only_for_balances_due(loaded):
    rows = set(
        query(
            loaded,
            "SELECT e.name, t.amount_delinquent FROM current.tax_delinquency t "
            "JOIN current.legal_entity e USING (legal_entity_id)",
        )
    )
    assert rows == {("ACME LLC", 1500.25), ("SMITH JOHN", 250.0)}


def test_permits_standalone_with_types_and_dates(loaded):
    rows = set(
        query(
            loaded,
            "SELECT t.category, t.code, p.application_date::text, p.cost "
            "FROM current.permit p JOIN current.permit_type t USING (permit_type_id)",
        )
    )
    assert rows == {
        ("building", "NEW CONSTRUCTION", "2026-09-01", 25000.0),
        ("building", None, "2026-09-03", None),  # '1,200' does not cast
        ("electrical", "SERVICE UPGRADE", "2026-09-10", 3000.5),
    }


def test_inspection_links_building_only_when_unambiguous(loaded):
    rows = dict(
        query(
            loaded,
            "SELECT inspection_date::text, building_id FROM current.inspection",
        )
    )
    assert rows == {
        "2026-08-01": "840.29510.00000001.0001.00000",  # one building on parcel
        "2026-08-02": None,  # two buildings: ambiguous
        "2026-08-03": None,  # unknown handle
    }


def test_condemnation_status_decoded(loaded):
    assert query(
        loaded,
        "SELECT letter_date::text, condemnation_type, status FROM current.condemnation",
    ) == [("2026-07-04", "EXTERIOR", "Condemned for demolition")]


def test_services_linked_to_parcel(loaded):
    rows = set(
        query(
            loaded,
            "SELECT s.parcel_id, d.code, t.code FROM current.service s "
            "JOIN current.division d USING (division_id) "
            "JOIN current.service_type t USING (service_type_id)",
        )
    )
    assert rows == {
        ("840.29510.00000004.0000.00000", "FORESTRY", "TREE TRIM"),
        ("840.29510.00000004.0000.00000", "LRA", "SIDE LOT"),
    }


# --- qa / history ------------------------------------------------------------


def test_qa_reports_warnings_without_errors(loaded):
    rows = dict(
        query(
            loaded,
            "SELECT check_name, failing_rows FROM etl.qa_result WHERE failing_rows > 0",
        )
    )
    assert rows.get("cast_loss_permit_cost") == 1
    assert (
        scalar(
            loaded,
            "SELECT count(*) FROM etl.qa_result WHERE severity = 'error' AND failing_rows > 0",
        )
        == 0
    )


def test_history_has_first_versions(loaded):
    assert (
        scalar(
            loaded,
            "SELECT count(*) FROM history.record_version WHERE table_name = 'parcel'",
        )
        == 7
    )
