# Useful Findings From The Legacy Mapping Workbook

Workbook source:

- [LEGACY - Mapping.xlsx](../data_sources/LEGACY%20-%20Mapping.xlsx)

This note captures the parts of the legacy workbook that still look directly useful to the current CDW data-dictionary work.

## 1. Legacy ID Rules Worth Preserving

Source:

- `ID Types` sheet, especially rows `4` through `24`

Findings:

- `CityBlock`
  - legacy sheet treats it as a float-like identifier with schema `#.## to ####.##`
- `Parcel`
  - legacy sheet notes that if it reaches four digits it can act as a condo-building placeholder
- `OwnerCode`
  - legacy sheet treats this as a taxing-status / owner-type component in the parcel identifier stack
- `ParcelId (Parcel11)`
  - legacy rule: construct from `CityBlock`, `Parcel`, and `OwnerCode`
  - legacy schema: `BBBBbbPPPO`
- `Parcel9`
  - legacy rule: construct from `CityBlock` and `Parcel`
  - legacy schema: `BBBBbbPPP`
  - legacy note: "Root of all taxable info"
- `GisCityBlock`
  - legacy note says it "Should be same as CityBlock"
- `Handle`
  - legacy sheet describes it as parcel-and-building level
  - legacy note says it is built from:
    - `GisCityBlock` with decimal removed
    - `Condominium` as `[0 OR 8]`
    - `GisParcel` with zero padding
    - `GisOwnerCode`
  - caution:
    - the prose description is more detailed than the abbreviated printed schema in the sheet, so treat this as a strong clue rather than a final spec

Current implication:

- the CDW parcel-ID strategy should explicitly distinguish at least these legacy concepts:
  - `Parcel11` / public parcel ID
  - `Parcel9`
  - `Handle`
  - GIS-derived components

## 2. Legacy Type-Mapping Rule That Still Fits CDW

Source:

- `Type Mapping` sheet, rows `1` through `19`

Findings:

- The old workflow translated source-system types through a text landing layer first.
- Examples from the sheet:
  - Access `Yes/No` -> transit `VARCHAR` -> target `BOOLEAN`
  - Access `Date/Time` -> transit `VARCHAR` -> target `DATE`
  - Access `Long Integer` -> transit `VARCHAR` -> target `INT`
  - DBF `Logical` -> transit `VARCHAR` -> target `BOOLEAN`
  - DBF `Date` -> transit `VARCHAR` -> target `DATE`

Current implication:

- This aligns with the current CDW staging design, where raw staging columns are kept as text and typed later in transformation logic.
- The legacy workbook therefore reinforces the current "lossless raw landing first, typed model later" approach.

## 3. Legacy Source Priority Hints

Source:

- `All Databases` sheet, first populated rows

Findings:

- `prcl.mdb` was marked `#1 - Main Data`
- The code/lookup databases from `codes.zip` were marked `#2 - Critical`
  - `PrclCode.mdb`
  - `corcode.mdb`
  - `bincode.mdb`
  - `cndcode.mdb`
- Several permit/inspection/conservation databases were marked `#3 - Important`
  - `prmemp.mdb`
  - `prmbdo.mdb`
  - `bldinsp.mdb`
  - `hcd.mdb`

Current implication:

- This is a reasonable starting point for documentation priority and issue-triage priority:
  - parcel + code tables first
  - then permits / inspections / conservation

## 4. Legacy Field-Definition Aggregation

Source:

- `All Field Defs` sheet, first populated rows

Findings:

- The old project already relied on external field-definition workbooks and aggregated them into one sheet.
- Example file referenced there:
  - `occupancy-permits-field-definitions.xlsx`

Current implication:

- The current `documentation/data_dictionary/source_field_definitions/` approach is a cleaner replacement for that old aggregation pattern.
- If more legacy field-definition files still exist outside the repo, they may be worth ingesting the same way.
