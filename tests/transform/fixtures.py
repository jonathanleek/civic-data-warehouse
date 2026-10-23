"""Synthetic staging data for the staging -> current transform tests.

Shaped like the real city extracts (all columns TEXT, lower-cased names, as
staging_table_prep loads them) and small enough to reason about by hand. Each
account exercises one rule from documentation/pipeline_architecture/
staging_to_current.md; the expectations in test_transforms.py are written
against these comments.

Accounts in prcl_prcl (cityblock, parcel, ownercode -> handle):

  A1  1001.00/10/0  H 10010000100  simple single-family, 1 building, 1 section
  A1' duplicate of A1 (identical row)                     -> rejected
  A2  1001.00/20/0  H 10010000200  simple, 2 buildings; bldg 2 has 3 sections
  A2b 1001.00/21/0  H 10010000200  billboard account (land use 3997) -> excluded
  A3  1001.00/30/0  H 10010000300  multi-family (4 units), same owner as A1
  A4  1001.00/40/0  H 10010000400  vacant lot; zoning code missing from decode
  A5  1002.00/50/0  H 10020000500  condo footprint (common element)
  A5u 1002.00/51..53/0             3 condo unit accounts on A5's handle
  A6  1003.00/60/0  H 10030000600  simple, plus residual co-owner account 61
  A7  1003.00/70/0  H 10030000700  footprint is outdoor advertising (6312)
                                   -> parcel excluded, account 71 excluded too
  A8  1004.00/80/0  H 10040000800  condo footprint WITH its own building
                                   (mixed case -> that bldgall row rejected),
                                   one unit account 81
  A9  1005.00/90/0  no handle                               -> rejected
  A10 1005.00/95/0  H 10050000950  no footprint on its handle -> rejected
  A12 1005.00/97/0  ' 10050000970' untrimmed handle         -> rejected
"""

PRCL_DEFAULTS = {
    "condominium": "0",
    "ownercity": "ST LOUIS",
    "ownerstate": "MO",
    "ownerzip": "63101",
    "asrlanduse1": "1110",
    "ward20": "5",
    "nbrhd": "12",
    "zip": "63101",
    "censblock20": "1001",
    "frontage": "30.6",
    "zoning": "A",
    "taxbaldue": "0.0000",
    "billyear": "2025",
    "nbrofunits": "1",
    "stname": "MAIN",
    "sttype": "ST",
}


def _account(cityblock, parcel, handle, gisparcel=None, **kw):
    row = dict(PRCL_DEFAULTS)
    row.update(
        cityblock=cityblock,
        parcel=parcel,
        ownercode="0",
        handle=handle,
        giscityblock=cityblock,
        gisparcel=gisparcel if gisparcel is not None else parcel,
        gisownercode="0",
        asrparcelid=(cityblock.replace(".", "") + parcel.zfill(4) + "0").zfill(11),
        parcel9=(cityblock.replace(".", "") + parcel.zfill(3)),
    )
    row.update(kw)
    return row


A1 = _account(
    "1001.00",
    "10",
    "10010000100",
    ownername="SMITH JOHN",
    owneraddr="123 MAIN ST",
    lowaddrnum="123",
)

PRCL_PRCL = [
    A1,
    dict(A1),  # A1': exact duplicate row
    _account(
        "1001.00",
        "20",
        "10010000200",
        ownername="ACME LLC",
        ownername2="C/O JANE DOE",
        owneraddr="PO BOX 5",
        ownercity="CHICAGO",
        ownerstate="IL",
        ownerzip="60601",
        zoning="H",
        taxbaldue="1500.2500",
        lowaddrnum="100",
        stname="OAK",
        sttype="AV",
        stpredir="N",
        frontage="78.529999",
    ),
    _account(
        "1001.00",
        "21",
        "10010000200",
        gisparcel="20",
        ownername="OUTFRONT MEDIA",
        owneraddr="1 SIGN WAY",
        asrlanduse1="3997",
    ),
    _account(
        "1001.00",
        "30",
        "10010000300",
        ownername="SMITH JOHN",
        owneraddr="123 MAIN ST",
        asrlanduse1="1130",
        nbrofunits="4",
        lowaddrnum="300",
    ),
    _account(
        "1001.00",
        "40",
        "10010000400",
        ownername="LAND REUTILIZATION AUTHORITY",
        owneraddr="1520 MARKET ST",
        asrlanduse1="0",
        zoning="Z",
        lowaddrnum="400",
    ),
    _account(
        "1002.00",
        "50",
        "10020000500",
        condominium="1",
        ownername="LOFT CONDO ASSN",
        owneraddr="500 WASHINGTON AV",
        asrlanduse1="1115",
        lowaddrnum="500",
        stname="WASHINGTON",
        sttype="AV",
    ),
    *[
        _account(
            "1002.00",
            str(51 + i),
            "10020000500",
            gisparcel="50",
            condominium="1",
            ownername=owner,
            owneraddr=addr,
            asrlanduse1="1115",
            lowaddrnum="500",
            stname="WASHINGTON",
            sttype="AV",
            stdunitnum=unit,
            taxbaldue=due,
        )
        for i, (owner, addr, unit, due) in enumerate(
            [
                ("DOE JANE", "500 WASHINGTON AV #1A", "1A", "0.0000"),
                ("SMITH JOHN", "123 MAIN ST", "1B", "250.0000"),
                ("ROE RICHARD", "500 WASHINGTON AV #2A", "2A", "0.0000"),
            ]
        )
    ],
    _account(
        "1003.00",
        "60",
        "10030000600",
        ownername="JONES MARY",
        owneraddr="600 ELM ST",
        lowaddrnum="600",
        stname="ELM",
    ),
    _account(
        "1003.00",
        "61",
        "10030000600",
        gisparcel="60",
        ownername="JONES ROBERT",
        owneraddr="602 ELM ST",
    ),
    _account(
        "1003.00",
        "70",
        "10030000700",
        ownername="BILLBOARD CO",
        owneraddr="9 AD LN",
        asrlanduse1="6312",
    ),
    _account(
        "1003.00",
        "71",
        "10030000700",
        gisparcel="70",
        ownername="BILLBOARD CO",
        owneraddr="9 AD LN",
    ),
    _account(
        "1004.00",
        "80",
        "10040000800",
        condominium="1",
        ownername="MIXED CONDO ASSN",
        owneraddr="800 PINE ST",
        lowaddrnum="800",
        stname="PINE",
    ),
    _account(
        "1004.00",
        "81",
        "10040000800",
        gisparcel="80",
        condominium="1",
        ownername="UNIT OWNER",
        owneraddr="800 PINE ST #1",
        lowaddrnum="800",
        stname="PINE",
        stdunitnum="1",
    ),
    _account("1005.00", "90", None, ownername="NO HANDLE"),
    _account(
        "1005.00",
        "95",
        "10050000950",
        gisparcel="94",
        ownername="ORPHAN ACCOUNT",
    ),
    _account("1005.00", "97", " 10050000970", ownername="UNTRIMMED"),
]


def _bldg(cityblock, parcel, bldgnum, **kw):
    return dict(
        cityblock=cityblock, parcel=parcel, ownercode="0", bldgnum=bldgnum, **kw
    )


PRCL_BLDGALL = [
    _bldg("1001.00", "10", "1", bldgusecode="1110", totalarea="1500", yearbuilt="1910"),
    _bldg("1001.00", "20", "1", bldgusecode="1110", totalarea="1800", yearbuilt="1925"),
    _bldg("1001.00", "20", "2", bldgusecode="5000", totalarea="4000", yearbuilt="1950"),
    _bldg("1001.00", "21", "1", bldgusecode="3997", totalarea="0", yearbuilt="0"),
    _bldg("1001.00", "30", "1", bldgusecode="1130", totalarea="3600", yearbuilt="1905"),
    _bldg("1002.00", "51", "1", bldgusecode="1115", totalarea="900", yearbuilt="2004"),
    _bldg("1002.00", "52", "1", bldgusecode="1115", totalarea="950", yearbuilt="2004"),
    _bldg("1002.00", "53", "1", bldgusecode="1115", totalarea="1200", yearbuilt="2004"),
    _bldg("1004.00", "80", "1", bldgusecode="1110", totalarea="700", yearbuilt="1890"),
    _bldg("1004.00", "81", "1", bldgusecode="1115", totalarea="650", yearbuilt="1890"),
    _bldg("1009.00", "1", "1", bldgusecode="1110", totalarea="1000", yearbuilt="1900"),
]

PRCL_BLDGRES = [
    _bldg(
        "1001.00",
        "10",
        "1",
        stories="2",
        fullbaths="1",
        halfbaths="1",
        aircondcentral="1",
        aircondwindow="0",
        garage1="1",
    ),
    _bldg(
        "1001.00",
        "20",
        "1",
        stories="1.5",
        fullbaths="2",
        halfbaths="0",
        aircondcentral="0",
        aircondwindow="2",
        garage1="0",
    ),
    _bldg(
        "1001.00",
        "30",
        "1",
        stories="3",
        fullbaths="4",
        halfbaths="0",
        aircondcentral="1",
        aircondwindow="0",
        garage1="0",
        nbrofunits="4",
    ),
    _bldg("1002.00", "51", "1", stories="1", fullbaths="1", halfbaths="0"),
    _bldg("1002.00", "52", "1", stories="1", fullbaths="1", halfbaths="1"),
    _bldg("1002.00", "53", "1", stories="2", fullbaths="2", halfbaths="0"),
    _bldg("1004.00", "81", "1", stories="1", fullbaths="1", halfbaths="0"),
]

PRCL_BLDGSECT = [
    _bldg("1001.00", "10", "1", sectnum="1"),
    _bldg("1001.00", "20", "2", sectnum="1", sectusecode="5000"),
    _bldg("1001.00", "20", "2", sectnum="2", sectusecode="5000"),
    _bldg("1001.00", "20", "2", sectnum="3", sectusecode="6000"),
]

PRCL_PRCLADDR = [
    dict(
        cityblock="1001.00",
        parcel="10",
        ownercode="0",
        lowaddrnum="123",
        stname="MAIN",
        sttype="ST",
        zip="63101",
    ),
    dict(
        cityblock="1001.00",
        parcel="20",
        ownercode="0",
        lowaddrnum="100",
        stpredir="N",
        stname="OAK",
        sttype="AV",
        zip="63101",
    ),
    dict(
        cityblock="1001.00",
        parcel="20",
        ownercode="0",
        lowaddrnum="102",
        stpredir="N",
        stname="OAK",
        sttype="AV",
        zip="63101",
    ),
    dict(
        cityblock="1002.00",
        parcel="51",
        ownercode="0",
        lowaddrnum="500",
        stname="WASHINGTON",
        sttype="AV",
        stdunitnum="1A",
        zip="63101",
    ),
    dict(
        cityblock="1002.00",
        parcel="52",
        ownercode="0",
        lowaddrnum="500",
        stname="WASHINGTON",
        sttype="AV",
        stdunitnum="1B",
        zip="63101",
    ),
    dict(cityblock="1005.00", parcel="99", ownercode="0"),  # no components -> rejected
]

PRCL_PRCLASMT = [
    dict(
        cityblock="1001.00",
        parcel="10",
        ownercode="0",
        asdtotal="19000.0000",
        asdland="3000.0000",
        asdimprove="16000.0000",
    ),
    dict(
        cityblock="1001.00",
        parcel="20",
        ownercode="0",
        asdtotal="91310.0000",
        asdland="15100.0000",
        asdimprove="76210.0000",
    ),
    dict(
        cityblock="1001.00",
        parcel="21",
        ownercode="0",
        asdtotal="500.0000",
        asdland="0.0000",
        asdimprove="500.0000",
    ),
    dict(
        cityblock="1002.00",
        parcel="51",
        ownercode="0",
        asdtotal="12000.0000",
        asdland="1000.0000",
        asdimprove="11000.0000",
    ),
    dict(
        cityblock="1005.00",
        parcel="90",
        ownercode="0",
        asdtotal="1.0000",
        asdland="1.0000",
        asdimprove="0.0000",
    ),
]

PRCLCODE_CDZONING = [
    dict(zoningcode="A", descr="Single-Family Dwelling District"),
    dict(zoningcode="H", descr="Area Commercial District"),
    dict(zoningcode=None, descr="orphan decode row"),
]

PRCLCODE_CDLANDUSE = [
    dict(landuse="1110", descr="SINGLE FAMILY"),
    dict(landuse="1115", descr="CONDOMINIUM"),
    dict(landuse="1130", descr="MULTI-FAMILY"),
    dict(landuse="5000", descr="RETAIL TRADE"),
]

BUILDING_PERMITS = [
    dict(
        permitnumber="BP-1",
        projecttype="NEW CONSTRUCTION",
        applicationdate="2026-09-01",
        issuedate="2026-09-15",
        applicationdescription="Build garage",
        estprojectcost="25000",
    ),
    dict(
        permitnumber="BP-2",
        projecttype=None,
        applicationdate="09/03/2026",
        issuedate=None,
        applicationdescription="Porch repair",
        estprojectcost="1,200",
    ),
]

ELECTRICAL_PERMITS = [
    dict(
        permitnumber="EP-1",
        projecttype="SERVICE UPGRADE",
        applicationdate="2026-09-10",
        issuedate="2026-09-11",
        applicationdescription="200A service",
        estprojectcost="3000.50",
    ),
]

HCD_HCDINSP = [
    dict(
        handle="10010000100",
        inspectionnum="I-1",
        inspectdate="08/01/26 00:00:00",
        inspecttype="COMPLAINT",
    ),
    dict(
        handle="10010000200",
        inspectionnum="I-2",
        inspectdate="08/02/26 00:00:00",
        inspecttype="ROUTINE",
    ),
    dict(
        handle="99999999999",
        inspectionnum="I-3",
        inspectdate="08/03/26 00:00:00",
        inspecttype="ROUTINE",
    ),
]

CONDEMN = [
    dict(
        handle="10010000400",
        condlettersent="07/04/26 00:00:00",
        inspecttype="EXTERIOR",
        status="C",
    ),
]

CNDCODES = [dict(code="C", descr="Condemned for demolition")]

FORESTRY = [
    dict(
        handle="10010000400",
        parcelid="x",
        category="TREE TRIM",
        servicedate="2026-06-01",
    )
]

LRA = [dict(handle="10010000400", parcelid="y", usage="SIDE LOT")]

STAGING_TABLES = {
    "prcl_prcl": PRCL_PRCL,
    "prcl_bldgall": PRCL_BLDGALL,
    "prcl_bldgres": PRCL_BLDGRES,
    "prcl_bldgsect": PRCL_BLDGSECT,
    "prcl_prcladdr": PRCL_PRCLADDR,
    "prcl_prclasmt": PRCL_PRCLASMT,
    "prclcode_cdzoning": PRCLCODE_CDZONING,
    "prclcode_cdlanduse": PRCLCODE_CDLANDUSE,
    "building_permits_last_30_days": BUILDING_PERMITS,
    "electrical_permits_last_30_days": ELECTRICAL_PERMITS,
    "hcd_hcdinsp": HCD_HCDINSP,
    "condemn": CONDEMN,
    "cndcodes": CNDCODES,
    "forestry_maintenance_properties": FORESTRY,
    "lra_public": LRA,
}
