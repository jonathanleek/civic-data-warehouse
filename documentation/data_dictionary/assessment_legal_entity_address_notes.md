# Assessment, Legal Entity, and Address Notes

This note captures the main modeling decisions implied by the current parcel sources.

## Assessment

Recommended grain:

- one row per parcel per source snapshot

Source fit:

- `ASMTLAND` -> `current.assessment.land_value`
- `ASMTIMPROV` -> `current.assessment.improvement_value`
- `ASMTTOTAL` -> `current.assessment.assessment_amount`

Open issue:

- `current.assessment.assessment_date` does not have a clearly named source field in the parcel extract.
- `UPDATED` looks like a record timestamp, but it is not clearly the legal assessment date.

Pragmatic first pass:

- treat the assessment values as the latest known snapshot in `current`
- let the `history` layer preserve when the warehouse observed the snapshot
- only map `UPDATED` into `assessment_date` if downstream users explicitly want "source record last updated" semantics

## Legal Entity

Recommended grain:

- one deduplicated owner entity reused across parcels when the normalized owner identity matches

Natural source fields:

- `OWNERNAME`
- `OWNERNAME2`
- `OWNERGROUP`
- `ASRCLASS1` through `ASRCLASS4`

Open issues:

- `OWNERNAME2` may be a continuation of the primary name or a second owner line.
- `OWNERGROUP` is not described in the published field metadata.
- `ASRCLASS*` uses the city vocabulary `23` ("Assessor Class Code"), but the current `legal_entity` table has no class/type column.

Pragmatic first pass:

- build the owner display name from `OWNERNAME` plus `OWNERNAME2` when present
- keep the raw source fields available for auditing
- add a future `legal_entity_class_id` or equivalent lookup target before trying to normalize `ASRCLASS*`

## Address

There are two distinct address roles in the parcel sources:

- owner mailing address
- parcel/site address

Owner mailing address source:

- `OWNERADDR`
- `OWNERCITY`
- `OWNERSTATE`
- `OWNERZIP`
- `OWNERCNTRY`

Site address source:

- raw parcel extract: `SITEADDR`
- helper distribution `195`: `LowAddrNum`, `LowAddrSuf`, `HighAddrNum`, `HighAddrSuf`, `StdUnitNum`, `StName`, `StPreDir`, `StType`, `StSufDir`, `ZIP`

Important limitations in the current schema:

- no address-range representation
- no suffix-direction column
- no country column
- no explicit address role column

Pragmatic first pass:

1. Use distribution `195` as the preferred parser-helper for site addresses when available.
2. Parse `OWNERADDR` separately as a mailing address linked from `legal_entity.address_id`.
3. Preserve unsupported fields such as high address numbers and suffix directions in staging or audit outputs until the schema grows.
