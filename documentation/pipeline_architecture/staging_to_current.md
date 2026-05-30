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
Each layer adds its `include/sql/current/*.sql` transforms plus a domain DAG (manual trigger,
`conn_id="cdw-dev"`, templated SQL via `SQLExecuteQueryOperator`), domain DAGs linked by Airflow
Assets so a layer can be re-run in isolation. Validation per layer: parse check (`af dags errors`),
row reconciliation against distinct source keys, FK-integrity pre-checks, and the capture-all guard.
