"""Regression tests for source shapes the main fixture does not cover:
empty optional feeds, keys that only collide after casting, and accounts with
missing key parts. Each must be loaded or logged, never crash a layer or
vanish."""

import copy

import pytest

from . import fixtures
from .conftest import load_staging, query, reset_database, run_layers, scalar

LAYERS = ["reference", "entities", "spine", "events", "history", "qa"]


def _edge_tables():
    t = copy.deepcopy(fixtures.STAGING_TABLES)
    # Optional feeds present but empty (a quiet 30-day permit window, etc.).
    for name in [
        "building_permits_last_30_days",
        "electrical_permits_last_30_days",
        "hcd_hcdinsp",
        "condemn",
        "cndcodes",
        "forestry_maintenance_properties",
        "lra_public",
    ]:
        cols = {c for r in t[name] for c in r}
        t[name] = [dict.fromkeys(cols)]  # keeps the columns; row removed below
    prcl = t["prcl_prcl"]
    a2 = next(r for r in prcl if r["parcel"] == "20")
    # Residual account with no owner at all -> rejected, not silently dropped.
    prcl.append(
        dict(
            a2,
            parcel="22",
            gisparcel="20",
            ownername=None,
            ownername2=None,
            asrparcelid="00100100220",
            asrlanduse1="1110",
        )
    )
    # Condo unit with no asrparcelid -> still a unit.
    condo = next(r for r in prcl if r["parcel"] == "51")
    prcl.append(
        dict(
            condo,
            parcel="54",
            asrparcelid=None,
            stdunitnum="3A",
            ownername="NULL APN OWNER",
        )
    )
    # Account whose key is incomplete -> rejected.
    prcl.append(dict(a2, parcel="23", ownercode=None, gisparcel="20"))
    # Situs street number that is a range -> kept verbatim in raw_line.
    for r in prcl:
        if r["parcel"] == "30":
            r["lowaddrnum"] = "300-302"
    # bldgnum '01' duplicates bldgnum '1' on A1 once cast.
    t["prcl_bldgall"].append(
        dict(
            cityblock="1001.00",
            parcel="10",
            ownercode="0",
            bldgnum="01",
            bldgusecode="1110",
        )
    )
    return t


@pytest.fixture(scope="module")
def edge(pg):
    reset_database(pg)
    tables = _edge_tables()
    load_staging(pg, tables)
    # Empty the "empty" feeds, keeping their columns.
    for name in [
        "building_permits_last_30_days",
        "electrical_permits_last_30_days",
        "hcd_hcdinsp",
        "condemn",
        "cndcodes",
        "forestry_maintenance_properties",
        "lra_public",
    ]:
        with pg.cursor() as cur:
            cur.execute(f"DELETE FROM staging.{name}")
    pg.commit()
    run_layers(pg, LAYERS)
    return pg


def test_empty_feeds_load_nothing_without_failing(edge):
    for table in ["permit", "inspection", "condemnation", "service"]:
        assert scalar(edge, f"SELECT count(*) FROM current.{table}") == 0


def test_all_feeds_balance(edge):
    assert (
        query(edge, "SELECT feed FROM etl.feed_reconciliation WHERE NOT balanced") == []
    )


def test_ownerless_residual_and_incomplete_key_are_rejected(edge):
    rows = dict(
        query(
            edge,
            "SELECT source_natural_key, reason FROM current.exclusion_log "
            "WHERE source_table = 'prcl_prcl' AND source_natural_key IN "
            "('1001.00/22/0', '1001.00/23/')",
        )
    )
    assert rows == {
        "1001.00/22/0": "residual co-owner account has no owner name",
        "1001.00/23/": "account key (cityblock, parcel, ownercode) incomplete",
    }


def test_condo_unit_without_asrparcelid_still_loads(edge):
    names = [
        r[0]
        for r in query(
            edge,
            "SELECT e.name FROM current.unit u JOIN current.legal_entity e "
            "ON e.legal_entity_id = u.owner_id "
            "WHERE u.building_cdw_id = '840.29510.00000005.0001.00000' ORDER BY u.cdw_id",
        )
    ]
    assert len(names) == 4 and "NULL APN OWNER" in names


def test_cast_equal_bldgnum_is_logged_not_a_pk_error(edge):
    assert query(
        edge,
        "SELECT reason FROM current.exclusion_log WHERE source_table = 'prcl_bldgall' "
        "AND source_natural_key LIKE '1001.00/10/0#%' ",
    ) == [("duplicate (account, bldgnum)",)]


def test_range_street_number_kept_in_raw_line(edge):
    assert query(
        edge,
        "SELECT a.raw_line, a.street_number FROM current.unit u "
        "JOIN current.address a USING (address_id) "
        "WHERE u.cdw_id = '840.29510.00000003.0001.00001'",
    ) == [("300-302 MAIN ST", None)]
