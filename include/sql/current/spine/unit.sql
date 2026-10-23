-- current.unit (every building has at least one unit; schema.md)
--   simple building, 0-1 sections -> one unit (unit_seq 1) carrying the
--       building's prcl_bldgres detail. A multi-family building is still ONE
--       unit, with the source unit count in number_of_units: CDW does not
--       fabricate per-apartment rows the city does not provide.
--   simple building, 2+ sections  -> one unit per prcl_bldgsect row,
--       unit_seq = sectnum
--   condo unit account            -> one unit under the synthesized building;
--       unit_seq minted in crosswalk.entity_xref (entity 'condo_unit', scope =
--       handle, natural key = asrparcelid + '|' + account key), so the first
--       load numbers units in assessor parcel order, later loads keep each
--       unit's number, and the key is unique per account even if asrparcelid
--       is missing or repeated.
-- owner_id = the account's owner; address_id = the account's situs address.

-- Sections ------------------------------------------------------------------
DROP TABLE IF EXISTS etl.w_bldgsect;
CREATE TABLE etl.w_bldgsect AS
SELECT
  etl.account_key(s.cityblock, s.parcel, s.ownercode) AS account_key,
  s.bldgnum, s.sectnum, etl.try_int(s.sectnum) AS unit_seq, s.sect_use_code,
  row_number() OVER (PARTITION BY s.cityblock, s.parcel, s.ownercode, s.bldgnum,
                                  coalesce(etl.try_int(s.sectnum)::text, s.sectnum)
                     ORDER BY s._payload::text COLLATE "C") AS row_num,
  s._payload
FROM etl.src_bldgsect s;

DROP TABLE IF EXISTS etl.w_building_sections;
CREATE TABLE etl.w_building_sections AS
SELECT b.account_key, b.bldgnum, count(s.*) AS sections
FROM etl.w_bldgall b
LEFT JOIN etl.w_bldgsect s
  ON s.account_key = b.account_key AND s.bldgnum = b.bldgnum
 AND s.row_num = 1 AND s.unit_seq BETWEEN 1 AND 99999
WHERE b.disposition = 'building'
GROUP BY b.account_key, b.bldgnum;

-- Residential detail, first row per (account, bldgnum) -------------------------
DROP TABLE IF EXISTS etl.w_bldgres;
CREATE TABLE etl.w_bldgres AS
SELECT
  etl.account_key(r.cityblock, r.parcel, r.ownercode) AS account_key,
  r.*,
  row_number() OVER (PARTITION BY r.cityblock, r.parcel, r.ownercode, r.bldgnum
                     ORDER BY r._payload::text COLLATE "C") AS row_num
FROM etl.src_bldgres r;

-- Condo unit numbering -------------------------------------------------------
SELECT crosswalk.mint_entity_keys('condo_unit', array_agg(c.handle),
                                  array_agg(coalesce(a.asrparcelid, '') || '|' || c.account_key))
FROM etl.w_account_class c
JOIN etl.w_account a ON a.account_key = c.account_key
WHERE c.role = 'condo_unit' AND c.row_num = 1;

-- Unit rows --------------------------------------------------------------------
DROP TABLE IF EXISTS etl.w_unit;
CREATE TABLE etl.w_unit AS
-- simple, single unit (a lone section's use code wins over the building's)
SELECT p.parcel_seq, b.building_seq, 1::bigint AS unit_seq, b.account_key, b.bldgnum,
       coalesce(s1.sect_use_code, b.bldgusecode) AS use_code, true AS takes_bldgres,
       'simple'::text AS kind
FROM etl.w_bldgall b
JOIN etl.w_parcel p ON p.account_key = b.account_key
JOIN etl.w_building_sections bs ON bs.account_key = b.account_key AND bs.bldgnum = b.bldgnum
LEFT JOIN etl.w_bldgsect s1
  ON s1.account_key = b.account_key AND s1.bldgnum = b.bldgnum
 AND s1.row_num = 1 AND s1.unit_seq BETWEEN 1 AND 99999
WHERE b.disposition = 'building' AND bs.sections <= 1
UNION ALL
-- simple, one unit per section
SELECT p.parcel_seq, b.building_seq, s.unit_seq, b.account_key, b.bldgnum,
       coalesce(s.sect_use_code, b.bldgusecode), false, 'section'
FROM etl.w_bldgall b
JOIN etl.w_parcel p ON p.account_key = b.account_key
JOIN etl.w_building_sections bs ON bs.account_key = b.account_key AND bs.bldgnum = b.bldgnum
JOIN etl.w_bldgsect s
  ON s.account_key = b.account_key AND s.bldgnum = b.bldgnum
 AND s.row_num = 1 AND s.unit_seq BETWEEN 1 AND 99999
WHERE b.disposition = 'building' AND bs.sections > 1
UNION ALL
-- condo units (detail from the unit account's lowest-numbered building)
SELECT p.parcel_seq, 1, k.entity_seq, c.account_key, ub.bldgnum, ub.bldgusecode, true, 'condo'
FROM etl.w_account_class c
JOIN etl.w_account a ON a.account_key = c.account_key
JOIN etl.w_parcel p ON p.handle = c.handle
JOIN crosswalk.entity_keys('condo_unit') k
  ON k.scope = c.handle AND k.natural_key = coalesce(a.asrparcelid, '') || '|' || c.account_key
LEFT JOIN LATERAL (
  SELECT b.bldgnum, b.bldgusecode FROM etl.w_bldgall b
  WHERE b.account_key = c.account_key AND b.disposition = 'condo_unit' AND b.row_num = 1
  ORDER BY b.building_seq NULLS LAST, b.bldgnum COLLATE "C"
  LIMIT 1
) ub ON true
WHERE c.role = 'condo_unit' AND c.row_num = 1;

TRUNCATE current.unit CASCADE;
INSERT INTO current.unit (
  country_code, region_code, parcel_seq, building_seq, unit_seq, owner_id, use_type_id,
  address_id, ground_floor, stories, windows_ac, central_ac, full_bath, half_bath, garage,
  number_of_units)
SELECT crosswalk.ns_country(), crosswalk.ns_region(), w.parcel_seq, w.building_seq, w.unit_seq,
       o.entity_key, u.use_type_id, ad.entity_key,
       NULL,  -- no source column
       etl.try_numeric(r.stories)::float, etl.try_int(r.windows_ac), etl.try_int(r.central_ac),
       etl.try_int(r.full_bath), etl.try_int(r.half_bath), etl.try_int(r.garage),
       etl.try_int(r.nbrofunits)
FROM etl.w_unit w
JOIN etl.w_account a ON a.account_key = w.account_key
LEFT JOIN crosswalk.entity_keys('legal_entity') o ON o.scope = '' AND o.natural_key = a.owner_nk
LEFT JOIN crosswalk.entity_keys('address') ad ON ad.scope = '' AND ad.natural_key = a.situs_address_nk
LEFT JOIN current.use_type u ON u.code = w.use_code
LEFT JOIN etl.w_bldgres r
  ON w.takes_bldgres AND r.account_key = w.account_key AND r.bldgnum = w.bldgnum AND r.row_num = 1;

-- Dispositions: prcl_bldgsect -------------------------------------------------
DELETE FROM current.exclusion_log WHERE source_table IN ('prcl_bldgsect', 'prcl_bldgres');

INSERT INTO current.exclusion_log (exclusion_id, source_table, source_natural_key, disposition, reason, raw_payload)
SELECT crosswalk.ns_key('exclusion', nextval('etl.exclusion_seq')), 'prcl_bldgsect',
       s.account_key || '#' || coalesce(s.bldgnum, '') || '#' || coalesce(s.sectnum, ''),
       CASE WHEN c.role = 'excluded' THEN 'excluded' ELSE 'rejected' END,
       CASE
         WHEN c.role = 'excluded' THEN 'account excluded'
         WHEN s.unit_seq IS NULL OR s.unit_seq NOT BETWEEN 1 AND 99999 THEN 'sectnum not an integer in 1..99999'
         WHEN s.row_num > 1 THEN 'duplicate (account, bldgnum, sectnum)'
         ELSE 'section of a building that was not loaded'
       END,
       s._payload
FROM etl.w_bldgsect s
LEFT JOIN etl.w_account_class c ON c.account_key = s.account_key AND c.row_num = 1
WHERE NOT (s.row_num = 1 AND s.unit_seq BETWEEN 1 AND 99999
           AND EXISTS (SELECT 1 FROM etl.w_building_sections bs
                       WHERE bs.account_key = s.account_key AND bs.bldgnum = s.bldgnum));

SELECT etl.reconcile('prcl_bldgsect',
  (SELECT count(*) FROM etl.src_bldgsect),
  (SELECT count(*) FROM etl.w_bldgsect s
   WHERE s.row_num = 1 AND s.unit_seq BETWEEN 1 AND 99999
     AND EXISTS (SELECT 1 FROM etl.w_building_sections bs
                 WHERE bs.account_key = s.account_key AND bs.bldgnum = s.bldgnum)),
  etl.disposition_count('prcl_bldgsect', 'excluded'),
  etl.disposition_count('prcl_bldgsect', 'rejected'));

-- Dispositions: prcl_bldgres --------------------------------------------------
DROP TABLE IF EXISTS etl.w_bldgres_used;
CREATE TABLE etl.w_bldgres_used AS
SELECT DISTINCT w.account_key, w.bldgnum FROM etl.w_unit w WHERE w.takes_bldgres;

INSERT INTO current.exclusion_log (exclusion_id, source_table, source_natural_key, disposition, reason, raw_payload)
SELECT crosswalk.ns_key('exclusion', nextval('etl.exclusion_seq')), 'prcl_bldgres',
       r.account_key || '#' || coalesce(r.bldgnum, ''),
       CASE WHEN c.role = 'excluded' THEN 'excluded' ELSE 'rejected' END,
       CASE
         WHEN c.role = 'excluded' THEN 'account excluded'
         WHEN r.row_num > 1 THEN 'duplicate (account, bldgnum)'
         WHEN EXISTS (SELECT 1 FROM etl.w_building_sections bs
                      WHERE bs.account_key = r.account_key AND bs.bldgnum = r.bldgnum AND bs.sections > 1)
           THEN 'detail for a sectioned building (units come from sections)'
         ELSE 'no unit for this building'
       END,
       r._payload
FROM etl.w_bldgres r
LEFT JOIN etl.w_account_class c ON c.account_key = r.account_key AND c.row_num = 1
WHERE r.row_num > 1
   OR NOT EXISTS (SELECT 1 FROM etl.w_bldgres_used u
                  WHERE u.account_key = r.account_key AND u.bldgnum = r.bldgnum);

SELECT etl.reconcile('prcl_bldgres',
  (SELECT count(*) FROM etl.src_bldgres),
  (SELECT count(*) FROM etl.w_bldgres r
   WHERE r.row_num = 1
     AND EXISTS (SELECT 1 FROM etl.w_bldgres_used u
                 WHERE u.account_key = r.account_key AND u.bldgnum = r.bldgnum)),
  etl.disposition_count('prcl_bldgres', 'excluded'),
  etl.disposition_count('prcl_bldgres', 'rejected'));
