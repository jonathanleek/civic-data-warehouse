# Data Dictionary

This directory is the project-specific data dictionary for the Civic Data Warehouse (CDW).

The City of St. Louis publishes useful field definitions and controlled vocabularies, but those sources do not answer the CDW-specific questions:

- which raw field should map to which CDW table and column
- which fields are direct copies vs lookups vs derived transforms
- where the current schema is incomplete or lossy

This folder is the place to record those answers.

## Current Scope

The current seed covers the parcel spine plus its immediate fan-out into:

- `building`
- `assessment`
- `legal_entity`
- `address`

Relevant source references:

- `prcl.zip` from `https://www.stlouis-mo.gov/data/upload/data-files/prcl.zip`
- City distribution `189` ("2017 - 2023 Parcels")
- City distribution `195` ("Parcel Joining Data")
- Controlled vocabularies used by those fields, especially:
  - `1` Neighborhood
  - `23` Assessor Class Code
  - `24` Assessor Land Use
  - `39` Parcel Attribute Type
  - `53` Zoning Code
  - `58` Building Exterior Wall Type
  - `65` Street Prefix Direction
  - `66` Street Type
  - `69` Multi Parcel Ind
  - `96` Number Of Units Source

The raw parcel archive currently ingested by this repo was confirmed on April 19, 2026, and the HTTP `Last-Modified` header for `prcl.zip` was also April 19, 2026.

## Canonical Files

- [parcel_building_dictionary.csv](./parcel_building_dictionary.csv)
  - One row per source field or mapping decision.
- [refresh_stl_reference_data.py](./refresh_stl_reference_data.py)
  - Refreshes local snapshots of city field definitions and vocabularies.
- [reference_manifest.json](./reference_manifest.json)
  - Download metadata for the local reference snapshots.
- [assessment_legal_entity_address_notes.md](./assessment_legal_entity_address_notes.md)
  - Grain, deduping, and modeling notes for the next transformation pass.
- [legacy_mapping_workbook_sheet_inventory.md](./legacy_mapping_workbook_sheet_inventory.md)
  - Sheet-by-sheet inventory of the legacy Excel workbook.
- [legacy_workbook_useful_findings.md](./legacy_workbook_useful_findings.md)
  - The subset of legacy workbook findings that still appear useful now.
- [source_field_definitions](./source_field_definitions/)
  - Machine-readable field definitions from the city's metadata endpoints.
- [controlled_vocabularies](./controlled_vocabularies/)
  - Machine-readable vocabulary snapshots used by parcel/building modeling.

## Dictionary Columns

`parcel_building_dictionary.csv` uses these columns:

- `entity`: CDW entity the source field is most relevant to.
- `source_status`: `ingested` if the repo currently pulls the source, `reference_only` if it is only being used to document semantics for now.
- `source_distribution`: city dataset/distribution name.
- `source_table_or_file`: raw file or logical source table.
- `source_field`: exact source field name.
- `source_label`: city-published human label.
- `staging_table` / `staging_column`: current CDW staging landing target when known.
- `current_table` / `current_column`: target CDW table and column when one exists.
- `mapping_status`: current state of the mapping decision.
- `vocabulary`: related controlled vocabulary when applicable.
- `notes`: CDW-specific interpretation, caveats, or next action.
- `source_url`: exact city page used for the definition.

## Mapping Status Values

- `candidate_direct`: likely direct mapping after typing/casting cleanup.
- `needs_lookup`: source code should resolve through a lookup table.
- `needs_parse`: text needs structured parsing before loading.
- `requires_row_generation`: source is aggregated and must be exploded into entity rows.
- `needs_decision`: modeling choice is still open.
- `not_modeled`: source field is real and useful, but there is no current target column yet.
- `business_rule`: source field mainly affects transform logic instead of landing in a target column.
- `proposed`: target mapping is likely correct, but the current ETL path does not implement it yet.

## Important Modeling Notes

- The implemented DDL in [include/sql/create_current.sql](../../include/sql/create_current.sql) and the conceptual model in [documentation/schema/schema.dbml](../schema/schema.dbml) do not fully agree.
  - Example: `current.parcel` is denormalized in SQL (`county`, `neighborhood`) but normalized in `schema.dbml` (`county_id`, `neighborhood_id`).
  - Example: `zip_code` is `int` in SQL even though ZIP codes behave like identifiers and should stay text.
- The legacy workbook's `ID Types` sheet is still useful context for the parcel-ID problem:
  - legacy `Parcel11` / `ParcelId` was documented as `CityBlock + Parcel + OwnerCode`
  - legacy `Parcel9` was documented as `CityBlock + Parcel`
  - legacy `Handle` was documented as a GIS-derived parcel/building identifier with condominium signaling
  - these should be treated as strong historical clues, not unquestioned truth, until they are revalidated against live source data
- The parcel source supports `ZONING1` through `ZONING3`, but `current.parcel` only allows one `zoning_class_id`.
- The raw parcel source is mostly parcel-grain. `current.building` and `current.unit` will require row-generation logic because fields like `NUMBLDGS`, `BDG1AREA`, and `NUMUNITS` describe buildings indirectly.
- The schema narrative in [documentation/schema/schema.md](../schema/schema.md) says addresses belong at the `unit` grain, but the current source material is still parcel-centric. The dictionary keeps that tension explicit instead of hiding it.
- The same physical `address` table will have to serve at least two semantic roles:
  - owner mailing addresses linked from `legal_entity.address_id`
  - site addresses linked to property entities
- `current.assessment.assessment_date` is not cleanly sourced from the parcel extract today. The `UPDATED` field may be usable as a record timestamp, but it is not clearly an official assessment date.

## Useful Legacy Workbook Findings

The old workbook still contains a few findings worth preserving.

Source:

- [LEGACY - Mapping.xlsx](../data_sources/LEGACY%20-%20Mapping.xlsx)
- summarized in [legacy_workbook_useful_findings.md](./legacy_workbook_useful_findings.md)

The most useful carry-overs so far are:

- From `ID Types`:
  - `Parcel11` / `ParcelId` was documented as `CityBlock + Parcel + OwnerCode`
  - `Parcel9` was documented as `CityBlock + Parcel`
  - `Handle` was documented as a GIS-derived parcel/building identifier that also signals condominium handling
  - `Parcel` reaching four digits was called out as a condo-building placeholder case
- From `Type Mapping`:
  - the legacy workflow already used a "land as text first, type later" pattern
  - that matches the current CDW staging approach and gives it historical justification
- From `All Databases`:
  - `prcl.mdb` was treated as the main source
  - the `codes.zip` databases were treated as critical supporting lookup sources
- From `All Field Defs`:
  - the old project already had a pattern of aggregating external field-definition files
  - the current `source_field_definitions/` folder is a cleaner successor to that idea

## Suggested Expansion Order

1. Decide the parcel ID strategy:
   - CDW surrogate `parcel_id`
   - source `HANDLE`
   - public-facing `ParcelId`
2. Decide whether zoning and land use stay single-valued or move to bridge tables.
3. Add an explicit classification target for `ASRCLASS*` owner-class codes.
4. Document raw `PrclCode_*` tables exported from `prcl.mdb`, especially parcel attribute and improvement code tables.
5. After the property spine is stable, fan out into permits, inspections, condemnations, and sales.

## Refreshing Reference Snapshots

Run:

```powershell
python documentation/data_dictionary/refresh_stl_reference_data.py
```

That command refreshes the local CSV snapshots and rewrites `reference_manifest.json`.
