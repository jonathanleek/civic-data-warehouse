CDW_ID — Global Parcel Identifier Standard
==========================================

Overview
========
The CDW_ID is the primary key of the Civic Data Warehouse spine (parcel, building, unit). It is
designed so that independent CDW deployments — one city or county at a time — can later be
**combined into a single warehouse without key collisions**, enabling CDW to function as an open,
globally interoperable standard.

The identifier must:
1. Scale to every parcel on earth.
2. Convey the geographic location of the entity it identifies.
3. Convey the entity's position in the property hierarchy (region, parcel, building, unit).
4. Never collide across independently built deployments, with no central coordinating authority.

Design: local id within a namespace
===================================
There is no global authority that numbers parcels, and there should not be — a parcel is a legal
object defined by whichever local jurisdiction has authority over it. The recognized international
standards for land administration resolve this the same way:

- **ISO 19152 (LADM)** — the Land Administration Domain Model. Its object identifier is
  `{localId, namespace}`.
- **EU INSPIRE — Cadastral Parcels** — every parcel carries `inspireId = localId + namespace +
  version`, where the namespace makes a locally-unique id globally unique, and the raw local
  reference is preserved as `nationalCadastralReference`.

CDW adopts this model. Global uniqueness is **structural**: the top of the identifier names the
jurisdiction using codes that governments already maintain as globally unique, and the bottom is
minted locally. Two different jurisdictions therefore cannot collide, regardless of how each
numbers its parcels — the same principle behind E.164 phone numbers, GS1 barcodes, and DOIs.

Structure
=========
```
<country>.<region>.<parcel>.<building>.<unit>
   840   . 29510  .  000123  .  0000   . 00000      ← a St. Louis City parcel
```

| Segment   | Authority (guarantees uniqueness)                                              | Notes |
|-----------|-------------------------------------------------------------------------------|-------|
| country   | ISO 3166-1 **numeric** (USA = `840`)                                          | 3 digits, globally unique by treaty |
| region    | Government code of the **parcel-issuing jurisdiction** — US = 5-digit county **FIPS** (state 2 + county 3). St. Louis City = `29510` (independent city = its own region); Shelby County / Memphis = `47157`. | `country.region` together = the LADM/INSPIRE **namespace** and the collision firewall |
| parcel    | A **CDW-minted, per-region surrogate sequence** (the **localId**)              | See "Parcel localId" below |
| building  | Sequence within the parcel                                                     | `0000` = parcel-level record (no building) |
| unit      | Sequence within the building                                                   | `00000` = building-level record (no unit) |

Any level not applicable is zero-filled (a vacant lot = `840.29510.000123.0000.00000`), and the
identifier still joins cleanly by prefix — e.g. all parcels in a region are
`840.29510.%`, all units of a building share its `…<parcel>.<building>.%` prefix.

Parcel localId — minted surrogate, not the city's parcel number
===============================================================
The parcel segment is a **CDW-minted per-region surrogate sequence**, not the city's own parcel
number and not a hash of it.

- The city's native parcel reference (in St. Louis, the assessor `handle`) is mapped to the
  surrogate through the region's crosswalk and **preserved verbatim** in
  `current.municipal_parcel_id_mapping` (this is INSPIRE's `nationalCadastralReference`). It also
  remains the operational join key while transforming the city's data.
- **Why a surrogate sequence and not the raw reference or a hash:**
  - *Collision-free by construction* — a per-region database sequence with a `UNIQUE` constraint
    on the native reference cannot collide within a region; the namespace prevents cross-region
    collisions. (A hash carries a small but non-zero collision probability.)
  - *Stable under churn* — the surrogate survives the city renumbering, splitting, or merging a
    parcel. The raw reference and a hash of it would both change, breaking the global identifier
    and every reference to it.
  - *Decentralized* — each jurisdiction maintains its own crosswalk for its own namespace; no
    central coordination is required.

The crosswalk (`crosswalk.parcel_xref`: region, native reference `UNIQUE`, parcel sequence) is the
**single authoritative registry for its region** — persisted, version-controlled, and never
regenerated. Sequences are collision-free within a region; the one disallowed situation is two
independent crosswalks minting the same region.

Storage
=======
A full CDW_ID is far wider than a 64-bit integer, so it is stored as **TEXT**, not a number.

- Spine tables (parcel, building, unit) store the five segments as their own columns
  (`country_code`, `region_code`, `parcel_seq`, `building_seq`, `unit_seq`) plus a Postgres
  **generated** `cdw_id` (the zero-padded, dot-delimited concatenation) used as the primary key.
  This gives one canonical string for joins/federation and indexable components for filtering.
- Parent relationships are explicit foreign-key columns: `building.parcel_cdw_id` → `parcel`,
  `unit.building_cdw_id` → `building`.
- Non-spine tables (legal_entity, address, the event tables, and all reference/lookup tables) use
  a namespaced TEXT surrogate of the form `<country>.<region>.<entity>.<sequence>`, so they too are
  globally unique and union cleanly across deployments.

Governance (to operate as an open standard)
============================================
- Publish the segment order, widths, delimiter, zero-padding, and the encoding authority for each
  level (ISO 3166-1 numeric → per-country region code system → locally minted localId).
- Maintain a small **region-code registry** mapping each country to its authoritative sub-region
  code system (US → Census FIPS). This is the only shared artifact required.
- Version the scheme so it can evolve.
- Aligns with ISO 19152 (LADM), EU INSPIRE, US FIPS, and the E.164 / GS1 / DOI namespace patterns.
