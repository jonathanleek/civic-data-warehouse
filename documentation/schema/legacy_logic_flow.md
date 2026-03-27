Legacy Parcel, Building, Unit Logic Flow
=========================================

This document describes the legacy ETL logic for transforming raw St. Louis Assessor's data (`prcl.mdb`) into normalized REDB (Real Estate Database) tables. It is based on the diagram found in `tmp/LEGACY - Pacel, Building, Unit Logic Flow.jpg` and validated against the actual source data.

Overview
--------

The legacy process runs six SQL scripts in order:

1. `bf_insert_legal_entity_address.sql` (Q1)
2. `bf_insert_legal_entity.sql` (Q2)
3. `bf_insert_specparceltype.sql` (Q3)
4. `jf_insert_subparceltype.sql` (Q4)
5. `dwp_insert_parcel.sql` (Q5)
6. `bf_insert_building.sql` (Q6)

These scripts transform four primary source tables from `prcl.mdb` into the REDB `parcel`, `building`, `unit`, `legal_entity`, and `address` tables, plus two reference tables (`SpecParcelType`, `SubParcelType`).

Source Data
-----------

The source file `prcl.zip` contains a single Microsoft Access database (`prcl.mdb`) with the following tables relevant to this logic:

| Table | Row Count | Purpose |
|-------|-----------|---------|
| `Prcl` | ~135,889 | Master parcel records with owner, address, assessment, and geographic data |
| `BldgAll` | ~491,442 | Building summary records for all types (includes empty/placeholder rows) |
| `BldgCom` | ~11,762 | Detailed commercial building attributes |
| `BldgRes` | ~98,748 | Detailed residential building attributes |
| `BldgSect` | ~12,884 | Building sections (sub-unit detail, primarily for commercial) |
| `PrclAddr` | ~156,618 | Address records linked to parcels |

Additional tables in `prcl.mdb` not directly referenced in this logic flow: `BldgResImp`, `CdAttrTypeNum`, `CxPrclCnBlk10`, `CxPrclCnBlk20`, `PrclAddrLRMS`, `PrclAttr`, `PrclAttrAsr`, `PrclDate`, `PrclImp`, `PrclREAR`, `PrclSBD`, `PrclAsmt`.

### Key Join Keys

All source tables share a composite key of `CityBlock + Parcel + OwnerCode`. Building tables add `BldgNum` to this key. `BldgSect` further adds `SectNum`.

### Key Abbreviations (from the diagram)

- **CB** = CityBlock
- **OC** = OwnerCode
- **PRCL** = Parcel (the field, not the table)
- **GISPRCL** = GisParcel (GIS-validated parcel number)
- **GSOC** = GisOwnerCode (GIS-validated owner code)

Q1 & Q2 — Address and Legal Entity Creation
--------------------------------------------

**Q1** creates address records and IDs from `PrclAddr` (156,618 rows). Each row contains full street address components (`StPreDir`, `StName`, `StType`, `StSufDir`, `LowAddrNum`, `HighAddrNum`, `UnitNum`, `ZIP`).

**Q2** creates `legal_entity` records from the `Prcl` table's owner fields:
- `OwnerName`, `OwnerName2` — entity name
- `OwnerAddr`, `OwnerCity`, `OwnerState`, `OwnerZIP` — entity address

Q2 includes a **duplicate check** — matching on name and linking to the REDB address ID created in Q1. This is necessary because the same owner appears across many parcels.

Q3 & Q4 — Reference Table Creation
-----------------------------------

**Q3** creates the `SpecParcelType` reference table from the single-character `SpecParcelType` column on `Prcl`. Known values include:
- `"C"` — 566 parcels
- `"S"` — 27 parcels
- Blank/numeric — majority of records

**Q4** creates the `SubParcelType` reference table from the `SubParcelType` column on `Prcl`. Known values include:
- `"C"` (1,972), `"Q"` (216), `"K"` (196), `"E"` (195), `"G"` (152), `"X"` (42), `"A"` (35), `"S"` (19)

Q5 — Parcel Filtering and Insertion
------------------------------------

Q5 applies a validation filter before inserting parcels into the REDB `parcel` table:

```
IF (Parcel = GisParcel AND OwnerCode = GisOwnerCode AND OwnerCode != 8)
    → INSERT into REDB parcel table
```

### Filter Results (from actual data)

| Condition | Count | % of Total |
|-----------|-------|------------|
| Passes Q5 filter | ~44,440 | 33% |
| Excluded: OwnerCode = 8 | ~299 | <1% |
| Excluded: GIS mismatch (Parcel != GisParcel or OC != GisOC) | ~91,449 | 67% |

**Why this filter exists:** The `Prcl` table contains both the Assessor's parcel/owner codes and GIS-validated equivalents (`GisCityBlock`, `GisParcel`, `GisOwnerCode`). When these don't match, it indicates a data quality issue — the assessor's records and GIS records disagree on parcel identity. The filter ensures only validated, reconciled parcels enter the REDB.

**OwnerCode = 8** is excluded as a special ownership category (likely non-standard or system-use records).

The two code paths after the filter likely correspond to standard parcels vs. parcels with special types (routed through `SpecParcelType`/`SubParcelType` lookups).

Q6 — Building Creation
-----------------------

Q6 constructs REDB building records from three sources, UNIONed together:

### Source 1: Synthetic buildings for high-number parcels
```
SELECT * FROM Prcl WHERE Parcel > 8000
→ Add '1' as BldgNum
```
This creates building records for ~597 parcels with Parcel numbers > 8000. These are likely parcels that have structures but no corresponding `BldgCom` or `BldgRes` records.

### Source 2: Commercial buildings
```
INNER JOIN BldgCom ON (CityBlock, Parcel, OwnerCode, BldgNum)
```
Produces **11,762 commercial building records**. Confirmed: `BldgAll` has exactly 11,762 records with `BldgUseCode = 'C'`.

### Source 3: Residential buildings
```
INNER JOIN BldgRes ON (CityBlock, Parcel, OwnerCode, BldgNum)
```
Produces **98,748 residential building records**. Confirmed: `BldgAll` has exactly 98,748 records with `BldgUseCode = 'R'`.

### Validation via BldgAll

`BldgAll` serves as a summary table with `BldgUseCode` categorizing each record:

| BldgUseCode | Count | Meaning |
|-------------|-------|---------|
| `R` | 98,748 | Residential (matches `BldgRes` exactly) |
| `C` | 11,762 | Commercial (matches `BldgCom` exactly) |
| `O` | 3,215 | Outbuildings (not in `BldgCom` or `BldgRes`) |
| (blank) | 377,715 | Parcels without buildings / placeholder rows |

Q7 — Unit Creation
-------------------

Q7 completes the spine by creating `current.unit` records for every building produced by Q6. The CDW schema requires that **every building has at least one unit** — even single-use structures. Units are created via three paths, each targeting a distinct building type. Together, these paths are exhaustive: every Q6 building receives exactly one path's treatment, and any record not captured is an error.

### Target Table: `current.unit`

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| `unit_id` | int PK | Generated | Auto-incrementing surrogate key |
| `building_id` | int FK | Q6 output | FK to `current.building`; resolved via composite key join |
| `owner_id` | int FK | Q2 output | FK to `current.legal_entity`; inherited from the parcel's owner |
| `use_type_id` | int FK | `BldgUseCode` | FK to `current.use_type`; derived from `BldgAll.BldgUseCode` (`R`, `C`, `O`) |
| `address_id` | int FK | Q1 output | FK to `current.address`; linked via `PrclAddr` (see Address Linkage below) |
| `ground_floor` | bool | Derived | `TRUE` if unit includes ground level (see derivation rules per path) |
| `stories` | float | Source-dependent | Residential: `ResStoriesCode`; Commercial: `LevelTo - LevelFrom + 1`; Outbuilding: `NULL` |
| `windows_ac` | int | Source-dependent | Count of window AC units; `NULL` for outbuildings |
| `central_ac` | int | Source-dependent | Central AC indicator; `NULL` for outbuildings |
| `full_bath` | int | `BldgRes` only | `NULL` for commercial and outbuilding units |
| `half_bath` | int | `BldgRes` only | `NULL` for commercial and outbuilding units |
| `garage` | int | `BldgRes` only | `NULL` for commercial and outbuilding units |

### Path 1: Residential Units (~98,748 records)

**One unit per `BldgRes` record.** Every residential building from Q6 Source 3 gets exactly one unit.

```
SELECT
    nextval('unit_id_seq')          AS unit_id,
    b.building_id                   AS building_id,
    p.owner_id                      AS owner_id,
    ut.use_type_id                  AS use_type_id,   -- where BldgUseCode = 'R'
    addr.address_id                 AS address_id,     -- see Address Linkage
    TRUE                            AS ground_floor,   -- single-unit residential always includes ground
    r.ResStoriesCode                AS stories,
    r.AirCondWindow                 AS windows_ac,
    r.AirCondCentral                AS central_ac,
    r.FullBaths                     AS full_bath,
    r.HalfBaths                     AS half_bath,
    COALESCE(r.NbrOfGarages, 0)
        + CASE WHEN r.Garage1 IS NOT NULL AND r.Garage1 != '' THEN 1 ELSE 0 END
        + CASE WHEN r.Garage2 IS NOT NULL AND r.Garage2 != '' THEN 1 ELSE 0 END
                                    AS garage
FROM BldgRes r
INNER JOIN current.building b
    ON (r.CityBlock, r.Parcel, r.OwnerCode, r.BldgNum)
     = (b.city_block, b.parcel, b.owner_code, b.bldg_num)
```

**Field mapping:**

| Source (`BldgRes`) | Target (`current.unit`) | Transform |
|--------------------|------------------------|-----------|
| `FullBaths` (Byte) | `full_bath` | Direct |
| `HalfBaths` (Byte) | `half_bath` | Direct |
| `AirCondCentral` (Boolean) | `central_ac` | Cast bool → int (0/1) |
| `AirCondWindow` (Boolean) | `windows_ac` | Cast bool → int (0/1) |
| `Garage1`, `Garage2`, `NbrOfGarages` | `garage` | `NbrOfGarages + (1 if Garage1 non-empty) + (1 if Garage2 non-empty)` |
| `ResStoriesCode` (Byte) | `stories` | Direct (code represents story count as float, e.g. 1.5) |
| — | `ground_floor` | Always `TRUE` (single-unit residential) |

### Path 2: Commercial Units (~12,884 records)

**One unit per `BldgSect` record** (not per `BldgCom`). A single commercial building may have multiple sections spanning different floor ranges, each becoming its own unit. This captures the section-level granularity that `BldgCom` alone does not provide.

```
SELECT
    nextval('unit_id_seq')          AS unit_id,
    b.building_id                   AS building_id,
    p.owner_id                      AS owner_id,
    ut.use_type_id                  AS use_type_id,   -- where BldgUseCode = 'C'
    addr.address_id                 AS address_id,     -- see Address Linkage
    (s.LevelFrom = 1)              AS ground_floor,   -- TRUE if section starts at level 1
    (s.LevelTo - s.LevelFrom + 1)  AS stories,
    s.AirCondWindow                 AS windows_ac,
    s.AirCondCentral                AS central_ac,
    NULL                            AS full_bath,
    NULL                            AS half_bath,
    NULL                            AS garage
FROM BldgSect s
INNER JOIN current.building b
    ON (s.CityBlock, s.Parcel, s.OwnerCode, s.BldgNum)
     = (b.city_block, b.parcel, b.owner_code, b.bldg_num)
```

**Field mapping:**

| Source (`BldgSect`) | Target (`current.unit`) | Transform |
|---------------------|------------------------|-----------|
| `LevelFrom` (Byte) | `ground_floor` | `TRUE` if `LevelFrom = 1` |
| `LevelFrom`, `LevelTo` (Byte) | `stories` | `LevelTo - LevelFrom + 1` |
| `AirCondCentral` (Boolean) | `central_ac` | Cast bool → int (0/1) |
| `AirCondWindow` (Boolean) | `windows_ac` | Cast bool → int (0/1) |
| `Area` (Long) | — | Available for downstream analytics but no direct `current.unit` column |
| — | `full_bath`, `half_bath`, `garage` | `NULL` (not applicable to commercial sections) |

**Edge case — Commercial buildings with no `BldgSect` records:** If a `BldgCom` building has no matching rows in `BldgSect`, it would receive zero units, violating the "every building has at least one unit" constraint. To handle this:

```
INSERT INTO current.unit (building_id, owner_id, use_type_id, address_id, ground_floor, stories, ...)
SELECT
    b.building_id, p.owner_id, ut.use_type_id, addr.address_id,
    TRUE,                           -- assume ground floor
    c.NbrOfStories,                 -- fall back to BldgCom.NbrOfStories
    NULL, NULL, NULL, NULL, NULL    -- no section-level detail available
FROM BldgCom c
INNER JOIN current.building b
    ON (c.CityBlock, c.Parcel, c.OwnerCode, c.BldgNum)
     = (b.city_block, b.parcel, b.owner_code, b.bldg_num)
LEFT JOIN BldgSect s
    ON (c.CityBlock, c.Parcel, c.OwnerCode, c.BldgNum)
     = (s.CityBlock, s.Parcel, s.OwnerCode, s.BldgNum)
WHERE s.SectNum IS NULL             -- no sections exist for this building
```

### Path 3: Outbuilding / Synthetic Units (~3,812 records)

**One unit per building** for two sub-populations that lack detail records in `BldgCom` or `BldgRes`:

**3a. Outbuildings (~3,215 records):** Buildings from `BldgAll` where `BldgUseCode = 'O'`. These are structures like detached garages, sheds, and billboards that have no corresponding row in `BldgCom` or `BldgRes`.

**3b. Synthetic buildings (~597 records):** Buildings created by Q6 Source 1 (`Prcl WHERE Parcel > 8000`). These parcels have structures but no typed building records.

```
-- 3a: Outbuildings
SELECT
    nextval('unit_id_seq')          AS unit_id,
    b.building_id                   AS building_id,
    p.owner_id                      AS owner_id,
    ut.use_type_id                  AS use_type_id,   -- where BldgUseCode = 'O'
    addr.address_id                 AS address_id,
    NULL                            AS ground_floor,
    NULL                            AS stories,
    NULL                            AS windows_ac,
    NULL                            AS central_ac,
    NULL                            AS full_bath,
    NULL                            AS half_bath,
    NULL                            AS garage
FROM BldgAll a
INNER JOIN current.building b
    ON (a.CityBlock, a.Parcel, a.OwnerCode, a.BldgNum)
     = (b.city_block, b.parcel, b.owner_code, b.bldg_num)
WHERE a.BldgUseCode = 'O'

UNION ALL

-- 3b: Synthetic buildings (Parcel > 8000)
SELECT
    nextval('unit_id_seq')          AS unit_id,
    b.building_id                   AS building_id,
    p.owner_id                      AS owner_id,
    NULL                            AS use_type_id,   -- no BldgUseCode available
    addr.address_id                 AS address_id,
    NULL                            AS ground_floor,
    NULL                            AS stories,
    NULL                            AS windows_ac,
    NULL                            AS central_ac,
    NULL                            AS full_bath,
    NULL                            AS half_bath,
    NULL                            AS garage
FROM Prcl pr
INNER JOIN current.building b
    ON (pr.CityBlock, pr.Parcel, pr.OwnerCode, '1')
     = (b.city_block, b.parcel, b.owner_code, b.bldg_num)
WHERE pr.Parcel > 8000
```

These are minimal unit records — mostly NULLs — that exist solely to satisfy the "every building has at least one unit" constraint.

### Address Linkage

Units are linked to addresses via the `PrclAddr` table (~156,618 rows), which connects to parcels through the composite key `(CityBlock, Parcel, OwnerCode)`.

```
LEFT JOIN PrclAddr pa
    ON (source.CityBlock, source.Parcel, source.OwnerCode)
     = (pa.CityBlock, pa.Parcel, pa.OwnerCode)
LEFT JOIN current.address addr
    ON addr.address_id = Q1_lookup(pa.*)   -- resolved via Q1-generated address IDs
```

**Multi-address parcels:** When a parcel has multiple `PrclAddr` records (e.g., a corner lot with two street addresses, or a multi-unit building), each unit receives the address that best matches its context:
- **Single-unit buildings** (residential, outbuilding, synthetic): Use the *primary* `PrclAddr` record (lowest `AddrNum` or first record by sort order). If only one address exists, it is used directly.
- **Multi-unit buildings** (commercial sections): Where possible, match `BldgSect` records to specific `PrclAddr` entries. When no deterministic match exists, all units on the building share the primary address.

The `LEFT JOIN` ensures that units are still created even if no `PrclAddr` record exists — `address_id` will be `NULL` in those cases, rather than dropping the unit entirely.

### Exhaustiveness Check

The three paths must collectively cover every building from Q6 with **no gaps and no duplicates**.

| Path | Source | Expected Count | Join Key |
|------|--------|---------------|----------|
| Path 1 — Residential | `BldgRes` | ~98,748 | `(CB, Parcel, OC, BldgNum)` |
| Path 2 — Commercial (sections) | `BldgSect` | ~12,884 | `(CB, Parcel, OC, BldgNum, SectNum)` |
| Path 2 — Commercial (fallback) | `BldgCom` w/o sections | ~0 (verify) | `(CB, Parcel, OC, BldgNum)` |
| Path 3a — Outbuildings | `BldgAll` where `O` | ~3,215 | `(CB, Parcel, OC, BldgNum)` |
| Path 3b — Synthetics | `Prcl` where `Parcel > 8000` | ~597 | `(CB, Parcel, OC, '1')` |
| **Total units** | | **~115,444** | |

**Verification against Q6 buildings:**
- Q6 Source 1 (synthetics): 597 buildings → 597 units (Path 3b) ✓
- Q6 Source 2 (commercial): 11,762 buildings → 12,884 section-units + fallback units (Path 2) ✓
- Q6 Source 3 (residential): 98,748 buildings → 98,748 units (Path 1) ✓
- Outbuildings: 3,215 `BldgAll` records with `BldgUseCode = 'O'` → 3,215 units (Path 3a) ✓

**Note:** Commercial buildings produce *more* units than buildings (12,884 sections across 11,762 buildings) because some buildings have multiple sections. The total unit count exceeds the total building count, which is expected.

**Duplicate prevention:** The three paths are mutually exclusive by construction:
- Path 1 joins on `BldgRes` (only `BldgUseCode = 'R'` buildings)
- Path 2 joins on `BldgSect`/`BldgCom` (only `BldgUseCode = 'C'` buildings)
- Path 3a filters `BldgAll` to `BldgUseCode = 'O'` (disjoint from `R` and `C`)
- Path 3b filters `Prcl` to `Parcel > 8000` (these parcels lack `BldgCom`/`BldgRes` records by definition)

**Error handling:** After all three paths execute, run a validation query to find any Q6 building with zero units:

```
SELECT b.building_id
FROM current.building b
LEFT JOIN current.unit u ON b.building_id = u.building_id
WHERE u.unit_id IS NULL
```

Any rows returned indicate a gap in the logic — these should be flagged as errors for manual review, matching the original diagram's "ERROR" fallback state. The intent is **zero rows** from this query.

Source Column Reference
-----------------------

### Prcl (key columns)

| Column | Type | Notes |
|--------|------|-------|
| `CityBlock` | Double | Part of composite key |
| `Parcel` | Double | Part of composite key |
| `OwnerCode` | Byte | Part of composite key; 8 = excluded |
| `GisCityBlock` | Double | GIS-validated city block |
| `GisParcel` | Double | GIS-validated parcel number |
| `GisOwnerCode` | Byte | GIS-validated owner code |
| `OwnerName` / `OwnerName2` | Text | Owner name fields → `legal_entity` |
| `OwnerAddr` / `OwnerCity` / `OwnerState` / `OwnerZIP` | Text | Owner address → `address` |
| `SpecParcelType` | Text(1) | Special parcel type code |
| `SubParcelType` | Text(1) | Sub-parcel type code |
| `Ward20` | Byte | Ward number (2020 boundaries) |
| `Nbrhd` | Byte | Neighborhood code |
| `ZIP` | Double | ZIP code |
| `Frontage` | Single | Lot frontage |
| `Zoning` | Text(4) | Zoning classification |
| `NbrOfBldgsRes` / `NbrOfBldgsCom` | Byte | Building counts |

### BldgCom (key columns)

| Column | Type | Notes |
|--------|------|-------|
| `CityBlock` / `Parcel` / `OwnerCode` / `BldgNum` | — | Composite key |
| `BldgCategory` | Text(2) | Building category code |
| `BldgType` | Integer | Building type code |
| `GroundFloorArea` | Long | Ground floor sq footage |
| `TotalArea` | Long | Total sq footage |
| `YearBuilt` | Integer | Year constructed |
| `NbrOfStories` | Byte | Number of stories |
| `TotApts` / `NbrOfApts*` | Integer | Apartment counts by bedroom |

### BldgRes (key columns)

| Column | Type | Notes |
|--------|------|-------|
| `CityBlock` / `Parcel` / `OwnerCode` / `BldgNum` | — | Composite key |
| `ResOccType` | Byte | Occupancy type |
| `LivingAreaTotal` | Long | Total living area |
| `FullBaths` / `HalfBaths` | Byte | Bathroom counts |
| `AirCondCentral` / `AirCondWindow` | Boolean | AC presence |
| `Garage1` / `Garage2` / `NbrOfGarages` | — | Garage info |
| `YearBuilt` | Integer | Year constructed |
| `ResStoriesCode` | Byte | Stories code |

### BldgSect (key columns)

| Column | Type | Notes |
|--------|------|-------|
| `CityBlock` / `Parcel` / `OwnerCode` / `BldgNum` / `SectNum` | — | Composite key |
| `SectCategory` | Text(2) | Section category |
| `LevelFrom` / `LevelTo` | Byte | Floor range |
| `Area` | Long | Section area |
| `Heating` / `AirCondCentral` / `AirCondWindow` | Boolean | HVAC |
| `Elevator` / `FireProtection` / `Electricity` | Boolean | Building services |
