"""Identity stability and history across repeated loads.

The crosswalk is persistent state: re-running the pipeline, or loading a newer
extract, must never renumber an existing parcel, unit or entity.
"""

import copy

import pytest

from . import fixtures
from .conftest import load_staging, query, reset_database, run_layers, scalar

LAYERS = ["reference", "entities", "spine", "events", "history", "qa"]

SPINE_IDS = """
    SELECT 'parcel', cdw_id FROM current.parcel
    UNION ALL SELECT 'building', cdw_id FROM current.building
    UNION ALL SELECT 'unit', cdw_id FROM current.unit
    UNION ALL SELECT 'legal_entity', legal_entity_id || ':' || name FROM current.legal_entity
    UNION ALL SELECT 'address', address_id FROM current.address
"""


def handle_to_parcel(conn):
    return dict(
        query(
            conn,
            "SELECT municipal_parcel_id, parcel_id FROM current.municipal_parcel_id_mapping",
        )
    )


@pytest.fixture(scope="module")
def first_load(pg):
    reset_database(pg)
    load_staging(pg)
    run_layers(pg, LAYERS)
    return {
        "ids": set(query(pg, SPINE_IDS)),
        "parcels": handle_to_parcel(pg),
    }


def test_rerun_is_identical_and_history_unchanged(pg, first_load):
    run_layers(pg, LAYERS)
    assert set(query(pg, SPINE_IDS)) == first_load["ids"]
    last = query(
        pg,
        "SELECT sum(inserted + changed + deleted + revived) FROM history.snapshot_log "
        "WHERE taken_at = (SELECT max(taken_at) FROM history.snapshot_log s "
        "                  WHERE s.table_name = snapshot_log.table_name)",
    )
    assert last == [(0,)]


def test_new_extract_keeps_existing_ids_and_records_history(pg, first_load):
    tables = copy.deepcopy(fixtures.STAGING_TABLES)
    prcl = tables["prcl_prcl"]
    # 1. A new parcel whose handle sorts BEFORE every existing one.
    new = dict(
        prcl[0],
        cityblock="1000.00",
        parcel="1",
        handle="10000000010",
        giscityblock="1000.00",
        gisparcel="1",
        asrparcelid="00100000010",
        ownername="NEW OWNER",
    )
    # 2. A new condo unit whose asrparcelid sorts before the existing units.
    condo_new = dict(
        next(r for r in prcl if r["parcel"] == "51"),
        parcel="49",
        asrparcelid="00000000001",
        stdunitnum="0A",
        ownername="EARLY UNIT",
    )
    # 3. A1 is sold (owner changes); 4. the vacant lot A4 disappears.
    for r in prcl:
        if r["parcel"] == "10":
            r["ownername"] = "SMITH JANE"
    tables["prcl_prcl"] = [r for r in prcl if r["parcel"] != "40"] + [new, condo_new]
    tables["prcl_bldgall"] = tables["prcl_bldgall"] + [
        dict(
            cityblock="1002.00",
            parcel="49",
            ownercode="0",
            bldgnum="1",
            bldgusecode="1115",
        )
    ]

    load_staging(pg, tables)
    run_layers(pg, LAYERS)

    parcels = handle_to_parcel(pg)
    for handle, parcel_id in first_load["parcels"].items():
        if handle != "10010000400":
            assert parcels[handle] == parcel_id, handle
    assert parcels["10000000010"] == "840.29510.00000008.0000.00000"
    assert "10010000400" not in parcels

    # Existing condo units keep their numbers; the new one is appended.
    units = query(
        pg,
        "SELECT u.cdw_id, e.name FROM current.unit u JOIN current.legal_entity e "
        "ON e.legal_entity_id = u.owner_id "
        "WHERE u.building_cdw_id = '840.29510.00000005.0001.00000' ORDER BY 1",
    )
    assert [u[1] for u in units] == [
        "DOE JANE",
        "SMITH JOHN",
        "ROE RICHARD",
        "EARLY UNIT",
    ]

    # History: A1's parcel changed (new owner), A4's parcel is dead.
    assert (
        scalar(
            pg,
            "SELECT count(*) FROM history.changed_records WHERE table_name = 'parcel' "
            "AND record_id = '840.29510.00000001.0000.00000'",
        )
        == 1
    )
    assert query(
        pg, "SELECT record_id FROM history.dead_records WHERE table_name = 'parcel'"
    ) == [("840.29510.00000004.0000.00000",)]


def test_dead_parcel_revives_with_same_id(pg, first_load):
    load_staging(pg)
    run_layers(pg, LAYERS)
    assert handle_to_parcel(pg)["10010000400"] == first_load["parcels"]["10010000400"]
    assert query(
        pg, "SELECT record_id FROM history.dead_records WHERE table_name = 'parcel'"
    ) == [("840.29510.00000008.0000.00000",)]
