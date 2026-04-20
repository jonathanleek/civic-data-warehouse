# Staging Loader Fix Notes

## PR Comment Draft

This PR fixes two staging-loader issues that were discovered during local setup and validation of the parcel staging data.

### What was observed

- In `staging.prcl_prcl`, the important text fields `ownername`, `ownername2`, and `legaldesc1` through `legaldesc5` were showing up as `'None'` in Postgres.
- The original `prcl.mdb` Access file clearly showed those fields populated.
- After fixing that first problem, a rerun of `staging_table_prep` failed with:

```text
StringDataRightTruncation: value too long for type character varying(64)
```

on several mapped `populate_staging_tables` tasks, including code/lookup files and permit data.

### Root causes

1. In `include/staging_table_prep.py`, the loader did:

```python
df[column] = df[column].replace("'", "''", inplace=True)
```

For pandas `Series.replace(..., inplace=True)`, the return value is `None`, so every object/text column was effectively being overwritten with `None`.

2. The staging table builder created every source column as `VARCHAR(64)`. Once text fields were preserved correctly, longer raw source values began failing to insert.

3. The DAG truncated staging tables but did not drop and recreate them, so old `VARCHAR(64)` definitions persisted across reruns.

### Fixes in this PR

- Read staging CSVs as strings with `dtype=str` so text values and leading-zero identifiers are preserved.
- Preserve missing values as real nulls instead of converting them to the literal string `'None'`.
- Replace manual SQL string assembly with `PostgresHook.insert_rows(...)`.
- Rebuild each staging table on each run with `DROP TABLE IF EXISTS ...; CREATE TABLE ...`.
- Define staging columns as `TEXT` instead of `VARCHAR(64)` so the staging layer is not lossy.

### Validation performed

- Confirmed the raw exported `prcl_Prcl.csv` in LocalStack still contained populated owner/legal fields.
- Confirmed the corruption happened in the Python staging loader, not during `mdb-export`.
- Smoke-tested the new insert path with apostrophes and nulls.
- Smoke-tested long-text inserts against rebuilt staging tables.
- Restarted Astro and reran `staging_table_prep`.
- Verified in DBeaver that `staging.prcl_prcl` now contains the previously missing owner and legal description data.

## Detailed Chronology

### 1. Initial setup issue

While setting up the repo locally, the onboarding guide referenced a nonexistent `Dockerfile.local` file:

```text
cp Dockerfile.local Dockerfile
```

The repo already contained a usable root `Dockerfile`, so the documentation was stale.

Related doc updates:

- `documentation/new_contributor_guide.md`
- `documentation/process.md`

### 2. DAG review and local environment validation

The DAG directory was reviewed to understand the local data flow:

- `cdw_creation` creates the `staging`, `current`, and `history` schemas and the table truncation function.
- `govt_file_download` downloads public source files, converts or unwraps them as needed, and uploads CSVs into the local S3 bucket in LocalStack.
- `staging_table_prep` truncates `staging`, lists S3 objects, creates one staging table per file, and populates them.

The local CDW Postgres instance was confirmed to be reachable on:

- host: `localhost`
- port: `5433`
- database: `cdw`
- user: `cdw_user`

### 3. First parcel-data bug report

During inspection of `staging.prcl_prcl`, the columns:

- `ownername`
- `ownername2`
- `legaldesc1`
- `legaldesc2`
- `legaldesc3`
- `legaldesc4`
- `legaldesc5`

appeared to have no meaningful values.

Initial SQL checks showed these were not SQL `NULL`, but the literal string `'None'` across the table.

### 4. Source-of-truth check against the raw Access export

The local staging data was compared against the upstream raw export chain:

1. Original `prcl.mdb` in Access
2. `mdb-export` output
3. `prcl_Prcl.csv` stored in LocalStack/S3
4. rows inserted into `staging.prcl_prcl`

Key finding:

- The raw `prcl_Prcl.csv` object in LocalStack still contained populated owner and legal-description fields.
- Therefore the data loss happened after export, inside the Python staging loader.

### 5. Root-cause analysis of the `'None'` corruption

The problem was traced to `populate_staging_table()` in `include/staging_table_prep.py`.

Old behavior:

```python
df = pd.read_csv(StringIO(obj))
for column in df.columns:
    if df[column].dtype == object:
        df[column] = df[column].replace("'", "''", inplace=True)
df.replace(np.nan, "None", inplace=True)
```

Why this broke:

- `Series.replace(..., inplace=True)` mutates the Series and returns `None`.
- Assigning that return value back into `df[column]` replaced every object/text column with `None`.
- The next line then converted missing values into the literal string `'None'`.

Why some fields still looked correct:

- Numeric-looking columns such as `ParcelId`, `AsdLand`, and `BillTotal` were often inferred as non-object dtypes by pandas, so they skipped the bad branch.

### 6. First code fix

The loader was changed to:

- read CSVs as strings
- preserve nulls as nulls
- stop manual quote escaping
- stop building raw SQL `INSERT` text by hand
- use `PostgresHook.insert_rows(...)`

This addressed:

- missing owner/legal text
- apostrophes inside values
- leading-zero preservation
- the literal `'None'` problem

### 7. New failure after the first fix

Once the loader began preserving text correctly, `staging_table_prep` started failing during mapped `populate_staging_tables` tasks.

The Airflow task logs showed:

```text
StringDataRightTruncation: value too long for type character varying(64)
```

This showed up in files such as:

- `PrclCode_CdAttrTypeNum.csv`
- `PrclCode_CdLandUse.csv`
- `building_permits_last_30_days.csv`

### 8. Root cause of the truncation failure

The staging table creator was still defining every source column as:

```sql
VARCHAR(64)
```

That was too small for:

- long permit descriptions
- longer code descriptions
- long owner/legal text in raw source files

In addition, `staging_table_prep` only truncated existing tables. It did not drop them, and the table DDL used `CREATE TABLE IF NOT EXISTS`, which meant old narrow schemas survived reruns.

### 9. Second code fix

`create_table_in_postgres()` was updated to:

- `DROP TABLE IF EXISTS` before rebuilding each staging table
- recreate staging columns as `TEXT`

This makes the staging layer act like a true raw landing area:

- no arbitrary 64-character cap
- no stale schema from prior runs
- no source-data truncation in staging

### 10. Validation of the final fix

Validation steps included:

- compile check of `include/staging_table_prep.py`
- throwaway insert smoke test using `PostgresHook.insert_rows(...)`
- throwaway long-text insert into a rebuilt staging table
- Astro restart
- rerun of `staging_table_prep`
- manual verification in DBeaver

Final result:

- the DAG completed successfully
- refreshing `staging.prcl_prcl` in DBeaver showed the previously missing owner and legal fields populated correctly

## Screenshot Observations Captured In Text

### Access screenshot

The original `Prcl` table in Access showed visibly populated values for:

- `OwnerName`
- `OwnerName2`
- `OwnerAddr`
- `OwnerCity`
- `OwnerState`
- `OwnerCountry`
- `OwnerZIP`
- `LegalDesc1`
- `LegalDesc2`
- `LegalDesc3`
- `LegalDesc4`
- `LegalDesc5`

This was the key visual evidence that the data existed upstream.

### Airflow screenshot

The Airflow UI showed a failed `staging_table_prep` run after the first code fix. Follow-up inspection showed the failures were in mapped `populate_staging_tables` tasks and were caused by `VARCHAR(64)` truncation.

### DBeaver screenshot

After the final fix and rerun, `staging.prcl_prcl` in DBeaver displayed populated owner and address fields, confirming the text columns were no longer being replaced with `'None'`.

## Files Changed During This Work

Primary functional fix:

- `include/staging_table_prep.py`

Additional doc cleanup performed during local setup:

- `documentation/new_contributor_guide.md`
- `documentation/process.md`
