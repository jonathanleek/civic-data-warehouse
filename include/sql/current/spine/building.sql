-- current.building
--   simple footprint: one building per prcl_bldgall row on the footprint
--                     account; building_seq = bldgnum.
--   condo footprint:  (condominium = '1' with at least one unit account)
--                     one synthesized building (building_seq 1) that all the
--                     condo's units hang off; no source row, so its attributes
--                     are NULL apart from the footprint owner.
-- Every prcl_bldgall row is dispositioned in etl.w_bldgall:
--   building     -> loaded as a building
--   condo_unit   -> loaded as detail for that condo unit (unit.sql)
--   excluded     -> its account was excluded
--   rejected     -> bad/duplicate bldgnum, account rejected/unknown, building on
--                   a residual co-owner account, or a condo footprint that also
--                   has its own building (mixed case, issue #69)
DROP TABLE IF EXISTS etl.w_bldgall;
CREATE TABLE etl.w_bldgall AS
SELECT
  etl.account_key(b.cityblock, b.parcel, b.ownercode) AS account_key,
  b.bldgnum,
  etl.try_int(b.bldgnum) AS building_seq,
  -- Duplicates are judged on the value that becomes building_seq, so '1' and
  -- '01' on one account collide here (and are logged) rather than in the PK.
  row_number() OVER (PARTITION BY b.cityblock, b.parcel, b.ownercode,
                                  coalesce(etl.try_int(b.bldgnum)::text, b.bldgnum)
                     ORDER BY b._payload::text COLLATE "C") AS row_num,
  -- A condo unit takes its detail from its lowest-numbered building only.
  row_number() OVER (PARTITION BY b.cityblock, b.parcel, b.ownercode
                     ORDER BY etl.try_int(b.bldgnum) NULLS LAST, b._payload::text COLLATE "C") AS account_bldg_rank,
  b.bldgusecode, b.totalarea, b.yearbuilt, b._payload,
  c.role AS account_role, c.handle,
  -- A condo footprint only gets a synthesized building when it has unit accounts.
  coalesce(c.is_condo, false) AND EXISTS (
    SELECT 1 FROM etl.w_account_class u WHERE u.handle = c.handle AND u.role = 'condo_unit'
  ) AS is_condo
FROM etl.src_bldgall b
LEFT JOIN etl.w_account_class c
  ON c.account_key = etl.account_key(b.cityblock, b.parcel, b.ownercode) AND c.row_num = 1;

ALTER TABLE etl.w_bldgall ADD COLUMN disposition text, ADD COLUMN reason text;
UPDATE etl.w_bldgall SET (disposition, reason) = (
  CASE
    WHEN account_role IS NULL THEN 'rejected'
    WHEN account_role = 'excluded' THEN 'excluded'
    WHEN account_role = 'rejected' THEN 'rejected'
    WHEN building_seq IS NULL OR building_seq NOT BETWEEN 1 AND 9999 THEN 'rejected'
    WHEN row_num > 1 THEN 'rejected'
    WHEN account_role = 'footprint' AND is_condo THEN 'rejected'
    WHEN account_role = 'footprint' THEN 'building'
    WHEN account_role = 'condo_unit' AND account_bldg_rank = 1 THEN 'condo_unit'
    ELSE 'rejected'
  END,
  CASE
    WHEN account_role IS NULL THEN 'no prcl_prcl account'
    WHEN account_role IN ('excluded', 'rejected') THEN 'account ' || account_role
    WHEN building_seq IS NULL OR building_seq NOT BETWEEN 1 AND 9999 THEN 'bldgnum not an integer in 1..9999'
    WHEN row_num > 1 THEN 'duplicate (account, bldgnum)'
    WHEN account_role = 'footprint' AND is_condo THEN 'condo footprint has its own building (mixed footprint)'
    WHEN account_role = 'residual' THEN 'building on a residual co-owner account'
    WHEN account_role = 'condo_unit' AND account_bldg_rank > 1 THEN 'additional building on a condo unit account'
  END);

TRUNCATE current.building CASCADE;
INSERT INTO current.building (
  country_code, region_code, parcel_seq, building_seq, owner_id, use_type_id, sq_footage, year_built)
SELECT crosswalk.ns_country(), crosswalk.ns_region(), p.parcel_seq, b.building_seq,
       o.entity_key, u.use_type_id, etl.try_int(b.totalarea), etl.try_int(b.yearbuilt)
FROM etl.w_bldgall b
JOIN etl.w_parcel p ON p.account_key = b.account_key
JOIN etl.w_account a ON a.account_key = b.account_key
LEFT JOIN crosswalk.entity_keys('legal_entity') o ON o.scope = '' AND o.natural_key = a.owner_nk
LEFT JOIN current.use_type u ON u.code = b.bldgusecode
WHERE b.disposition = 'building'
UNION ALL
SELECT crosswalk.ns_country(), crosswalk.ns_region(), p.parcel_seq, 1,
       o.entity_key, NULL, NULL, NULL
FROM etl.w_parcel p
JOIN etl.w_account a ON a.account_key = p.account_key
LEFT JOIN crosswalk.entity_keys('legal_entity') o ON o.scope = '' AND o.natural_key = a.owner_nk
WHERE p.is_condo
  AND EXISTS (SELECT 1 FROM etl.w_account_class c WHERE c.handle = p.handle AND c.role = 'condo_unit');

DELETE FROM current.exclusion_log WHERE source_table = 'prcl_bldgall';
INSERT INTO current.exclusion_log (exclusion_id, source_table, source_natural_key, disposition, reason, raw_payload)
SELECT crosswalk.ns_key('exclusion', nextval('etl.exclusion_seq')), 'prcl_bldgall',
       account_key || '#' || coalesce(bldgnum, ''), disposition, reason, _payload
FROM etl.w_bldgall
WHERE disposition IN ('excluded', 'rejected');

SELECT etl.reconcile('prcl_bldgall',
  (SELECT count(*) FROM etl.src_bldgall),
  (SELECT count(*) FROM etl.w_bldgall WHERE disposition IN ('building', 'condo_unit')),
  etl.disposition_count('prcl_bldgall', 'excluded'),
  etl.disposition_count('prcl_bldgall', 'rejected'));
