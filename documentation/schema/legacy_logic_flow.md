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

Unfinished Logic — Unit Creation
---------------------------------

The legacy diagram marks this section as **"Unfinished Logic"**, and it was never fully implemented. The intended flow was:

### Path 1: Commercial building sections → UNIT records
```
IF Parcel-11 & BldgNum listed in BldgSect table
    → Create REDB UNIT Record (building section)
```
`BldgSect` has **12,884 rows** with section-level detail for commercial buildings: floor levels, area, heating, AC, elevator, fire protection, etc. Each section would become a unit record.

### Path 2: Non-building parcels → UNIT records
```
IF Parcel-11 NOT listed in BldgCom or BldgRes tables
    → Create REDB UNIT Record (all others, e.g. billboard)
```
This handles the **3,215 outbuilding** records in `BldgAll` (`BldgUseCode = 'O'`), plus potentially other non-standard structures.

### Path 3: Filter already-handled parcels
A `NOT LEFT JOIN on #Com&#Res` step removes parcels already handled by the commercial and residential paths, preventing duplicate unit creation.

### Fallback: ERROR
Any records not captured by Paths 1-3 fall through to an **ERROR** state. The diagram notes: *"Should Capture all Records"* — indicating the logic was designed to be exhaustive but was never completed.

### What's Missing for Unit Creation

To complete the unit creation logic, the following transformations are needed:

1. **Residential units** — Map `BldgRes` fields directly to `current.unit`:
   - `FullBaths` → `full_bath`
   - `HalfBaths` → `half_bath`
   - `AirCondCentral` → `central_ac`
   - `AirCondWindow` → `windows_ac`
   - `Garage1`/`Garage2`/`NbrOfGarages` → `garage`
   - `ResStoriesCode` → `stories`
   - Every residential building must have at least one unit (per schema design)

2. **Commercial units** — Map `BldgSect` rows to `current.unit`:
   - Each section becomes a unit record
   - `Area` → sq footage
   - `AirCondCentral`, `AirCondWindow` → AC fields
   - `LevelFrom`/`LevelTo` → floor/story information

3. **Outbuildings** — Create minimal unit records for `BldgAll` records with `BldgUseCode = 'O'`

4. **Address linkage** — Connect units to `PrclAddr` records via the parcel key to populate `address_id`

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
