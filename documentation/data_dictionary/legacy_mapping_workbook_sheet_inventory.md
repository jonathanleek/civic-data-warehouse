# Legacy Mapping Workbook Sheet Inventory

Workbook profiled:

- [LEGACY - Mapping.xlsx](../data_sources/LEGACY%20-%20Mapping.xlsx)

This inventory is a sheet-by-sheet description of the old workbook so the useful parts can later be migrated into the repo in a more durable form.

Method:

- workbook structure and previews were inspected programmatically with `openpyxl`
- `max_rows` / `max_cols` are workbook extents, not necessarily populated rows and columns
- `non_empty_cells` and `formula_cells` are better indicators of whether a sheet is truly active
- this pass does not attempt to decode every fill color, only to note that some sheets are heavily color-driven

Sortable companion file:

- [legacy_mapping_workbook_sheet_inventory.csv](./legacy_mapping_workbook_sheet_inventory.csv)

## Sheet-by-Sheet Summary

| # | Sheet | What It Has | Current State |
|---|---|---|---|
| 1 | `README` | Intro, authorship/history, how-to-use notes, and reference links for the old REDB mapping workbook. | Useful historical context. |
| 2 | `ID Types` | Color-coded explanation of city ID fields and signifiers, with types, examples, construction rules, and notes about what each ID means. | One of the highest-value sheets. |
| 3 | `Dimensional Table Sources` | Only a two-column header for dimension-table-to-source mapping. | Empty placeholder. |
| 4 | `Process Tracking` | Early process implementation tracker for Talend-era steps like downloads, staging, and core loads. | Not empty, but mostly an outdated partial stub. |
| 5 | `All Databases` | Prioritized inventory of city source files and databases, with URLs, access steps, sizes, freshness, inclusion flags, counts, notes, and progress columns. | High-value source inventory. |
| 6 | `Schema Tracking` | Early schema-planning tracker with only a few populated rows. | Outdated partial stub. |
| 7 | `All Tables ` | Inventory of city tables by file, with field counts, record counts, notes, main type, and key-ID presence flags. | High-value table inventory. |
| 8 | `City_to_REDB` | Main master mapping from city file.table.field rows into REDB staging/core targets, plus descriptions, examples, include flags, transformations, checks, and SQL/XML helper columns. | Highest-value archaeology sheet. |
| 9 | `REDB_only` | REDB-side field inventory showing what still needed maps, counts of occurrences, constraints, and helper columns for generators. | Useful for old-schema gap analysis. |
| 10 | `SQL & XML Generators` | Formula-driven helper tab for emitting SQL/XML snippets from mapping data. | Generator artifact with some broken references. |
| 11 | `DBDiagram.io Generator` | Formula sheet that assembles dbdiagram.io table definitions from upstream mapping rows. | Useful only if the old diagram workflow matters. |
| 12 | `DBDiagram Export V2` | Parsed/annotated working sheet derived from dbdiagram SQL export, broken into commands, table names, field names, flags, and metadata. | Useful schema archaeology. |
| 13 | `Type Mapping` | Compact mapping from source/Access data types to transit and target warehouse types. | High-value reusable reference. |
| 14 | `Task Backlog` | Tiny backlog with a few concrete follow-up tasks, such as LRA parcel IDs and census mapping. | Small but potentially useful historical crumb trail. |
| 15 | `Testing Pivot` | No visible content in the current workbook. | Empty pivot artifact. |
| 16 | `Pivot on City Table` | No visible content in the current workbook. | Empty pivot artifact. |
| 17 | `Pivot on File Name` | No visible content in the current workbook. | Empty pivot artifact. |
| 18 | `All Field Defs` | Compiled field definitions with names, labels, types, vocabularies, and originating definition files. | High-value field-definition inventory. |
| 19 | `Pivot of Tables ` | No visible content in the current workbook. | Empty pivot artifact. |
| 20 | `StoT Mapping (Depreciated)` | Older deprecated source-to-target mapping sheet from city fields into REDB staging/core, with transformations and notes. | Superseded, but still useful for ambiguity checks. |

## High-Value Tabs To Mine Later

- `ID Types`
  - Best candidate for turning the old identifier untangling work into durable documentation.
- `All Databases`
  - Best candidate for source-system inventory, provenance notes, and issue creation.
- `All Tables `
  - Best candidate for source-table cataloging.
- `City_to_REDB`
  - Best candidate for field-level mapping history, transform logic notes, and unresolved design issues.
- `Type Mapping`
  - Best candidate for contributor-guide typing rules and ETL conventions.
- `All Field Defs`
  - Best candidate for seeding the new project data dictionary.

## Tabs That Look Mostly Archival

- `Process Tracking`
- `Schema Tracking`
- `Task Backlog`
- `StoT Mapping (Depreciated)`

These are not worthless, but they read more like historical planning artifacts than living source-of-truth tabs.

## Tabs That Look Generated or Disposable

- `SQL & XML Generators`
- `DBDiagram.io Generator`
- `DBDiagram Export V2`
- `Testing Pivot`
- `Pivot on City Table`
- `Pivot on File Name`
- `Pivot of Tables `

These appear to support old generation/reporting workflows rather than contain primary business knowledge.

## One Important Correction To The Initial Hunch

`Process Tracking` is not actually empty. It has a handful of populated rows covering early Talend workflow steps, but it is still fair to call it an outdated stub because the sheet is mostly unmaintained after those first rows.
