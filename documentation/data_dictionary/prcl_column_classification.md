# `prcl` Column Classification

This sheet classifies every column in the live `staging.prcl_prcl` table into one of three buckets:

- `actual_data`: literal values like parcel identifiers, owner names, legal descriptions, dates, counts, or dollar amounts
- `coded_data`: codes, flags, district memberships, lookup values, or controlled-vocabulary style fields
- `other`: empty columns, placeholder-only columns, or fields whose current contents are too sparse or unclear to treat as normal data

Snapshot used:

- table: `staging.prcl_prcl`
- rows profiled: `135894`
- profiled on: `2026-04-19` local project date

Bucket counts:

- `actual_data`: 88
- `coded_data`: 88
- `other`: 11

Primary sheet:

- [prcl_column_classification.csv](./prcl_column_classification.csv)

A few important caveats:

- The staging layer stores everything as `TEXT`, so this sheet is about meaning, not warehouse data types.
- Some fields are clearly coded even when a matching city vocabulary or local lookup table has not been found yet.
- `other` does not mean useless. It usually means one of these: empty in the current snapshot, placeholder-only, or needs manual interpretation.
