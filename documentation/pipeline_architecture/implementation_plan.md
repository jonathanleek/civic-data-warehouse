Staging → Current: Implementation Plan
======================================

The phased build-out of the transformation layer described in
[staging_to_current.md](staging_to_current.md). Work is decomposed into branches; wave boundaries
are hard dependencies, and branches within a wave are independent and can proceed in parallel.

Open follow-ups carried by individual branches
==============================================
- Enumerate the exact non-real-estate land-use codes to exclude (from the assessor Land Use
  vocabulary); parking and condo garage spaces are retained.
- Confirm condo footprint accounts never carry their own buildings (so the simple/condo cases are
  disjoint); handle any mixed footprints.
- Verify the grain of multi-building condo developments before finalizing the synthesized building
  rule.

Branches
========

**Wave 0 — independent, can start immediately**
- `fix/staging-decode-none` — fix the staging load so decode tables (`prclcode_*`, `corcode_*`,
  `cndcodes`, etc.) retain their real descriptions instead of loading as literal `None`.
  Prerequisite for every reference/decode join; self-contained.

**Wave 1 — foundation (must land before the rest)**
- `schema/cdw-id-and-current-ddl` — rewrite `include/sql/create_current.sql` to the CDW_ID design:
  namespaced TEXT primary keys, generated `cdw_id` on the spine, explicit parent FK columns, type
  widening (`zip_code`, `census_block`, `use_type.use_type`), `address.raw_line`, and the
  exclusion / secondary-owner tables. Keep the schema docs in sync.

**Wave 2 — Phase 0 infrastructure (after Wave 1)**
- `xform/crosswalk-and-id-minting` — `crosswalk` schema and `parcel_xref`
  (region, native reference `UNIQUE` → parcel sequence via per-region DB sequence), region/namespace
  config (country 840 / FIPS 29510), idempotent surrogate minting for spine and non-spine entities,
  exclusion-log table, and the capture-all reconciliation guard.
- `xform/reference-loads` — load `zoning_class`, `use_type`, `municipality`, `division`,
  `permit_type`, `inspection_type`, `service_type` from decode tables and seeds.
  *(Depends on `fix/staging-decode-none` and the new DDL.)*

**Wave 3 — entities (after Wave 2)**
- `xform/address-legal-entity` — `address` (situs + owner + `raw_line`) and `legal_entity` (dedup),
  with namespaced keys via the crosswalk.

**Wave 4 — spine (after Wave 3)**
- `xform/spine-parcel-building-unit` — the core: real-estate-only filter, parcel from footprints,
  the simple/condo building+unit logic, deterministic ids. Largest and highest-risk branch.

**Wave 5 — events (after Wave 4; mutually independent → parallel)**
- `xform/parcel-events` — `assessment`, `tax_delinquency`, `municipal_parcel_id_mapping`.
- `xform/inspections-condemnation` — `inspection`, `condemnation` (+ `cndcodes` decode).
- `xform/services` — `forestry` + `lra` → `service` / `service_type` / `division`.
- `xform/permits` — the six permit feeds → `permit` (standalone). Only needs Waves 1–2, so it can
  start as early as Wave 3.

**Wave 6 — finalize**
- `xform/history-and-qa` — populate `history` snapshots and the data-quality / reconciliation test
  suite.

Validation approach
===================
- `af dags errors` after each DAG (parse gate).
- Row reconciliation: target count vs. count of distinct source keys.
- FK-integrity pre-checks before load.
- Capture-all guard: `loaded + excluded + rejected = source` per feed.
- No-mutation audit: spot-check that loaded values equal source values (no rounding, trimming, or
  recoding beyond documented decode-table joins).
