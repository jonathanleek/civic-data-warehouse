-- etl.w_account_class: what every prcl_prcl row becomes. Exactly one row per
-- source row (row_num distinguishes exact duplicates), one role each:
--
--   footprint   the account whose own (cityblock, parcel, ownercode) equals its
--               GIS footprint (giscityblock, gisparcel, gisownercode); one per
--               handle -> current.parcel. If several qualify, the lowest
--               asrparcelid is the footprint and the rest are treated as
--               non-footprint accounts.
--   condo_unit  non-footprint account on a condo footprint (condominium = '1')
--               -> current.unit under the synthesized condo building
--   residual    non-footprint account on a non-condo footprint
--               -> current.secondary_owner
--   excluded    non-real-estate land use (etl.non_real_estate_land_use), or
--               any account on a handle whose footprint is excluded
--   rejected    duplicate row, missing key component, missing/untrimmed
--               handle, no footprint account on its handle, or a residual
--               account with no owner name (nothing to record)
DROP TABLE IF EXISTS etl.w_account_class;
CREATE TABLE etl.w_account_class AS
WITH src AS (
  SELECT
    etl.account_key(s.cityblock, s.parcel, s.ownercode) AS account_key,
    row_number() OVER (PARTITION BY s.cityblock, s.parcel, s.ownercode
                       ORDER BY s._payload::text COLLATE "C") AS row_num,
    s.*
  FROM etl.src_prcl s
),
footprint_candidate AS (
  SELECT DISTINCT ON (handle) handle, account_key, condominium,
         asrlanduse1 IN (SELECT land_use_code FROM etl.non_real_estate_land_use) AS is_excluded
  FROM src
  WHERE row_num = 1
    AND cityblock IS NOT NULL AND parcel IS NOT NULL AND ownercode IS NOT NULL
    AND handle IS NOT NULL AND handle = btrim(handle)
    AND cityblock = giscityblock AND parcel = gisparcel AND ownercode = gisownercode
  ORDER BY handle, asrparcelid COLLATE "C", account_key COLLATE "C"
)
SELECT
  s.account_key,
  s.row_num,
  s.handle,
  f.account_key AS footprint_account_key,
  (f.condominium = '1') AS is_condo,
  CASE
    WHEN s.row_num > 1 THEN 'rejected'
    WHEN s.cityblock IS NULL OR s.parcel IS NULL OR s.ownercode IS NULL THEN 'rejected'
    WHEN s.handle IS NULL THEN 'rejected'
    WHEN s.handle <> btrim(s.handle) THEN 'rejected'
    WHEN s.asrlanduse1 IN (SELECT land_use_code FROM etl.non_real_estate_land_use) THEN 'excluded'
    WHEN f.handle IS NULL THEN 'rejected'
    WHEN f.is_excluded THEN 'excluded'
    WHEN f.account_key = s.account_key THEN 'footprint'
    WHEN f.condominium = '1' THEN 'condo_unit'
    WHEN s.ownername IS NULL AND s.ownername2 IS NULL THEN 'rejected'
    ELSE 'residual'
  END AS role,
  CASE
    WHEN s.row_num > 1 THEN 'duplicate account row'
    WHEN s.cityblock IS NULL OR s.parcel IS NULL OR s.ownercode IS NULL
      THEN 'account key (cityblock, parcel, ownercode) incomplete'
    WHEN s.handle IS NULL THEN 'account has no handle'
    WHEN s.handle <> btrim(s.handle) THEN 'handle has leading/trailing whitespace'
    WHEN s.asrlanduse1 IN (SELECT land_use_code FROM etl.non_real_estate_land_use)
      THEN 'non-real-estate land use ' || s.asrlanduse1
    WHEN f.handle IS NULL THEN 'no footprint account for handle'
    WHEN f.is_excluded THEN 'footprint account is non-real-estate'
    WHEN f.account_key <> s.account_key AND f.condominium IS DISTINCT FROM '1'
         AND s.ownername IS NULL AND s.ownername2 IS NULL
      THEN 'residual co-owner account has no owner name'
  END AS reason,
  s._payload
FROM src s
LEFT JOIN footprint_candidate f ON f.handle = s.handle;

CREATE INDEX ON etl.w_account_class (account_key);
CREATE INDEX ON etl.w_account_class (handle);

-- Disposition log for prcl_prcl. The capture-all guard runs in
-- secondary_owner.sql, once every role has been written to its target table.
DELETE FROM current.exclusion_log WHERE source_table = 'prcl_prcl';
INSERT INTO current.exclusion_log (exclusion_id, source_table, source_natural_key, disposition, reason, raw_payload)
SELECT crosswalk.ns_key('exclusion', nextval('etl.exclusion_seq')), 'prcl_prcl', account_key, role, reason, _payload
FROM etl.w_account_class
WHERE role IN ('excluded', 'rejected');
