Staging → Current Transformation
================================

This document describes how the City of St. Louis source data in the `staging` schema is
restructured into the normalized CDW model in the `current` schema. It is the design reference for
the transformation DAGs (the layer after `cdw_creation`, `govt_file_download`, and
`staging_table_prep`).

Guiding principles
==================
1. **Restructure, don't alter.** Source values are carried through verbatim and only re-shaped
   into the new model. Where a target type would force a lossy cast, the column is widened rather
   than the value mangled. The one sanctioned exception is filtering out non-real-estate items
   (see [schema.md](../schema/schema.md)).
2. **Deterministic & idempotent.** Each transform is a full truncate-and-reload of its target.
   Surrogate keys are minted through a persisted crosswalk so they are stable across re-runs.
3. **FK-dependency ordering.** Build order respects the foreign keys in `create_current.sql`:
   reference/lookups → address → legal_entity → parcel → building → unit → events.
4. **Capture all records.** Every source row is loaded, filtered (to the exclusion log), or
   rejected (to a reject table) — never silently lost. Reconciliation: `loaded + excluded +
   rejected = source` per feed.
5. **Identity follows the CDW_ID standard.** See [cdw_id.md](../schema/cdw_id.md).

Source inventory
================
Join keys: `handle` (assessor parcel handle = plot of land) and the composite
`(cityblock, parcel, ownercode)` (assessment account) are the universal parcel keys. `condemn`,
`hcd_*`, `forestry`, and `lra` also carry `handle`/`parcelid`, so they join cleanly. Only the six
permit CSV feeds are address-only.

The `prclcode_*`, `corcode_*`, `comcode_*`, `bincode_*`, and `cndcode*` staging tables are decode
dictionaries used during transformation, not direct load targets.

Source → target mapping
=======================
| `current` table | Primary source(s) | Notes |
|---|---|---|
| **parcel** | `prcl_prcl`, footprint rows (`parcel=gisparcel AND ownercode=gisownercode AND cityblock=giscityblock`) | Real-estate only. ward←`ward20`, neighborhood←`nbrhd`, zip←`zip`, census_block←`censblock20`, frontage←`frontage`, zoning_class_id←`zoning` |
| **building** | `prcl_bldgall` / `bldgcom` / `bldgres` | per (account, `bldgnum`); use_type←`bldgusecode`, sq_footage←`totalarea`, year_built←`yearbuilt` |
| **unit** | `prcl_bldgsect`, condo accounts, single-unit derivation | detailed attributes from `bldgres`; address_id→address |
| **legal_entity** | owners in `prcl_prcl` (`ownername…ownerzip`), `condemn`, `hcd_hcdinsp` | dedup by (name, address) |
| **address** | `prcl_prcladdr` (situs, componentized), owner mailing addresses | `raw_line` preserves un-componentized source verbatim |
| **assessment** | `prcl_prclasmt` | amount←`asdtotal`, land_value←`asdland`, improvement_value←`asdimprove` |
| **tax_delinquency** | `prcl_prcl` (`taxbaldue`, `billyear`) | amount_delinquent←`taxbaldue` |
| **permit** + **permit_type** | 6 `*_permits_last_30_days` feeds | loaded standalone (schema has no spine FK) |
| **inspection** + **inspection_type** | `hcd_hcdinsp` | inspection_date←`inspectdate` |
| **condemnation** | `condemn` (+ `cndcodes` decode) | letter_date←`condlettersent`, status←`status` |
| **service** + **service_type** + **division** | `forestry_maintenance_properties`, `lra_public_*` | parcel_id via `handle`/`parcelid` |
| **zoning_class** | `prclcode_cdzoning` | direct decode load |
| **use_type** | `prclcode_cdlanduse` / `bldgusecode` | |
| **municipality** + **municipal_parcel_id_mapping** | static (`St. Louis City` / FIPS 29510) + `prcl_prcl` (`handle`) | municipality_id = FIPS region code; mapping preserves `handle` |

**Out of scope:** `prclsale_prclsale` (property sales) has no target table in the current model and
is not transformed.

Spine transformation (parcel → building → unit)
===============================================
Verified grain of the source:
- `handle` = plot of land → **parcel**.
- The assessment account `(cityblock, parcel, ownercode)` is **unit-grain**. A footprint has one
  account (a normal parcel) or many (a condo: one common-element footprint account + N unit
  accounts, all sharing the footprint's `handle`).
- `bldgall` / `bldgres` / `bldgcom` are keyed per `(account, bldgnum)` — one row per building per
  account. `bldgsect` is finer: per `(account, bldgnum, sectnum)`.
- Each condo unit-account carries its own `bldgall` + `bldgres` row; the common-element footprint
  account carries none.

Two footprint shapes drive building/unit generation:

- **Simple footprint** (single account = footprint; the large majority of parcels):
  - **building** ← each `bldgall` row on the account.
  - **unit** ← per building: residential or single-section → one unit carrying that building's
    detail; multi-section (`bldgsect`) → one unit per section. Vacant land (no building) → no unit.
  - Multi-family residential yields **one** unit carrying building-level detail with `nbrofunits`
    retained — CDW does not fabricate per-apartment rows the city does not provide.
- **Condo footprint** (footprint account + N unit accounts):
  - **unit** ← each unit-account → one unit (its own `bldgres`/`bldgall` detail; address from
    `prcl_prcladdr` / `stdunitnum`).
  - **building** ← one synthesized structure per footprint, with all its units linked to it.

Identity & crosswalk
====================
- Parcel `cdw_id` = `840.29510.<parcel_seq>.0000.00000`, where `parcel_seq` is minted from `handle`
  via `crosswalk.parcel_xref` (the authoritative per-region registry). `handle` is preserved in
  `current.municipal_parcel_id_mapping`.
- `crosswalk.parcel_xref` is persistent pipeline state. It survives current-schema rebuilds so an
  existing native `handle` keeps the same `parcel_seq` on every run.
- Building = `…<bldgnum:4>.00000`; unit = `…<bldgnum:4>.<unitseq:5>`. Condo buildings synthesize
  `…0001.00000`; condo units order deterministically by assessor parcel number.
- Non-spine surrogates are namespaced TEXT (`840.29510.<entity>.<seq>`). See
  [cdw_id.md](../schema/cdw_id.md).

Key modeling decisions
======================
- **Types:** codes/identifiers are stored as TEXT; only true measured quantities (area, value,
  counts, year) are numeric. `zip_code`, `census_block`, and `use_type.use_type` are TEXT.
- **Addresses:** componentized situs addresses map 1:1; freeform owner street lines are not parsed
  — stored verbatim in `address.raw_line` alongside structured city/state/zip.
- **Permits:** loaded standalone; the current `permit` table has no parcel/building link.
- **No-source fields** (`assessment_date`, `tax_delinquency.last_paid_year`,
  `inspection.number_of_violations`, permit `completion_date`/`cancel_date`/`contractor_id`) are
  null-filled, never fabricated.
- **Co-owners:** `parcel.owner_id` is the footprint owner; residual non-condo multi-owner accounts
  are logged to an exclusion/secondary-owner table.

Orchestration
=============
Each layer is a DAG (`dags/transform_<layer>.py`) that runs one `SQLExecuteQueryOperator` task per
SQL file (`conn_id="cdw-dev"`). The file list and order live in one place,
`include/transform_layers.py`, which both the DAGs and the tests read. Layers are chained by
Airflow Assets: `cdw_transform_reference` is triggered manually after `staging_table_prep`, and
each later layer is scheduled on the Asset of the one before it. Any layer can also be triggered
on its own.

| Order | DAG | SQL (`include/sql/current/`) | Loads |
|---|---|---|---|
| 1 | `cdw_transform_reference` | `reference/*.sql` | municipality, division, zoning_class, use_type, permit_type, inspection_type, service_type |
| 2 | `cdw_transform_entities` | `entities/*.sql` | address, legal_entity (+ `etl.w_account`) |
| 3 | `cdw_transform_spine` | `spine/*.sql` | parcel, municipal_parcel_id_mapping, building, unit, secondary_owner |
| 4 | `cdw_transform_events` | `events/*.sql` | assessment, tax_delinquency, permit, inspection, condemnation, service |
| 5 | `cdw_transform_history` | `history/snapshot.sql` | `history.record_version` |
| 6 | `cdw_transform_qa` | `qa/checks.sql` | `etl.qa_result` (fails on any error check) |

Every layer starts with two tasks: `set_namespace` (writes the `CDW-COUNTRY-ID` / `CDW-REGION-ID`
/ `CDW-REGION-NAME` Airflow Variables to `crosswalk.active_namespace`) and `source_views`
(rebuilds the `etl.src_*` views). Every layer ends with `assert_balanced`, which fails the layer,
before it emits its Asset, if any feed's capture-all counts do not balance. Each SQL file runs in a
single transaction. Diagnostics (`etl.feed_reconciliation`, `etl.qa_result`) are written by one
task and asserted by a later one, so a failed run leaves its evidence committed.

Run the layers one at a time. Two overlapping chains can deadlock on the cascading truncates
(Postgres aborts one; its retry recovers). `max_active_runs=1` only applies per DAG, so a shared
Airflow pool of size 1 for the `transform` DAGs is the stricter guard.

`cdw_transform_history` refuses to snapshot when a feed is unbalanced, or when `current.parcel`
is empty while staging has accounts. A partial load would otherwise tombstone every record.

Because a reload truncates its targets with `CASCADE`, re-running an upstream layer also clears
the tables that reference it. The Asset chain then reloads them.

Implementation
==============

Schemas
-------
| Schema | Created by | Lifetime |
|---|---|---|
| `crosswalk` | `create_crosswalk.sql` | **Persistent.** `parcel_xref` (handle → parcel_seq), `entity_xref` (non-spine natural key → seq), `active_namespace`. Never truncated. |
| `etl` | `create_etl.sql` | Rebuildable. Source views, safe-cast and capture-all helpers, config seeds, `etl.w_*` work tables, `feed_reconciliation`, `qa_result`. |
| `current` | `create_current.sql` | Truncate-and-reload target. |
| `history` | `create_history.sql` | **Persistent.** Versioned copies of `current` rows. |

Source contract
---------------
`include/sql/current/source_views.sql` is the only file that names raw staging tables or
columns. It exposes each one as an `etl.src_*` view with stable aliases (all text, `''` → NULL,
plus `_payload` = the raw row as jsonb). A missing *required* column fails the layer and names
the column. A missing *optional* column reads as NULL and is recorded in
`etl.source_contract_gap`, which QA reports. Columns marked `UNVERIFIED` there are best guesses
at the city's field names that have not yet been checked against a real staging load (the
`prcl_bldgres` detail columns, the `prcl_bldgsect` use code, permit numbers, and most
hcd / condemn / forestry / LRA fields). Correcting a guess means editing one line.

Identifiers
-----------
- Parcels: `crosswalk.parcel_xref`. New handles are inserted in handle order and the trigger mints
  the next `parcel_seq`; existing handles keep theirs, including handles that drop out of an
  extract and come back.
- Everything else: `crosswalk.mint_entity_keys(entity, [scopes,] natural_keys)`. New keys are
  numbered after the current maximum in byte order (`COLLATE "C"`), so numbering does not depend on
  the database locale. Key = `<country>.<region>.<entity>.<seq>`.
- Natural keys: lookups → source code; address → all address columns; legal_entity →
  (ownername, ownername2, mailing address); secondary_owner / tax_delinquency → account
  `(cityblock/parcel/ownercode)`; condemnation → (handle, letter date, type); service →
  (feed, handle, type, date); permit / inspection → source number when the feed has one, else
  row hash. An ordinal disambiguates rows with the same key.
- Condo `unit_seq`: `entity_xref` with entity `condo_unit`, scope = handle, natural key =
  `asrparcelid|account`. The first load numbers units in assessor-parcel order, as specified, and later
  loads keep each unit's number when units are added.

Account classification (spine)
------------------------------
`spine/account_class.sql` gives every `prcl_prcl` row exactly one role:

| Role | Rule | Becomes |
|---|---|---|
| footprint | own key = GIS key; one per handle (lowest `asrparcelid` if several qualify) | parcel |
| condo_unit | non-footprint account on a footprint with `condominium = '1'` | unit under the synthesized building |
| residual | non-footprint account on any other footprint | secondary_owner |
| excluded | `asrlanduse1` in `etl.non_real_estate_land_use`, or its footprint is excluded | exclusion_log |
| rejected | duplicate row, incomplete account key, missing or untrimmed handle, no footprint on its handle, residual account with no owner | exclusion_log |

Building / unit rules not spelled out above:
- A condo footprint that has unit accounts *and* its own `prcl_bldgall` row is the mixed case
  (#69). The footprint's own building row is rejected (logged for review); the units still load
  under the synthesized building.
- A building on a residual account is rejected (logged): it has no place on the footprint's
  building numbering.
- `prcl_bldgres` detail attaches to single-unit and condo units. Detail for a sectioned building
  is logged rather than attached to an arbitrary section.
- `inspection.building_id` is set only when the parcel has exactly one building.

Capture-all and QA
------------------
Every feed's transform logs excluded and rejected rows to `current.exclusion_log`
(`disposition`, `reason`, raw `_payload`). It then calls `etl.reconcile`, which records the counts in
`etl.feed_reconciliation`. The layer's final `assert_balanced` task fails unless
`loaded + excluded + rejected = source` for every feed. For `prcl_prcl`, "loaded" is counted from the
rows actually written to parcel, unit and secondary_owner. Duplicate detection uses the cast value
that becomes an ID segment (bldgnum `'1'` and `'01'` collide), so duplicates are logged rather than
failing a primary key.
`qa/checks.sql` re-asserts the balance and adds spine invariants (one parcel per footprint, every
building has a unit, every parcel has its native reference and crosswalk entry), lookup
resolution, and a no-mutation audit of verbatim columns. It also reports cast loss (non-empty
source values that did not parse as a number or date) and source-contract gaps as warnings.

Testing
-------
`tests/transform/` runs every SQL file, in DAG order, against synthetic staging data
(`fixtures.py`, one account per rule above) in a scratch Postgres. It covers ID stability across
re-runs and changed extracts, plus history change, death and revival. CI runs it in the
`Transform SQL tests` job; locally:

```bash
CDW_TEST_DATABASE_URL=postgresql://postgres@localhost:5432/cdw pytest tests/transform
```
