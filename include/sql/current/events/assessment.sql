-- current.assessment <- prcl_prclasmt, one row per source row, linked to the
-- parcel of the row's account. Values are carried verbatim as numbers.
-- assessment_date has no source column and is NULL (not fabricated).
-- Natural key: account + ordinal among that account's rows (byte order of the
-- raw row), which is stable while the source rows are unchanged.
DROP TABLE IF EXISTS etl.w_assessment;
CREATE TABLE etl.w_assessment AS
SELECT
  etl.account_key(s.cityblock, s.parcel, s.ownercode) AS account_key,
  etl.account_key(s.cityblock, s.parcel, s.ownercode) || '#'
    || row_number() OVER (PARTITION BY s.cityblock, s.parcel, s.ownercode
                          ORDER BY s._payload::text COLLATE "C") AS natural_key,
  s.asdtotal, s.asdland, s.asdimprove, s._payload,
  c.role AS account_role, p.parcel_cdw_id
FROM etl.src_prclasmt s
LEFT JOIN etl.w_account_class c
  ON c.account_key = etl.account_key(s.cityblock, s.parcel, s.ownercode) AND c.row_num = 1
LEFT JOIN etl.w_parcel p ON p.handle = c.handle AND c.role IN ('footprint', 'condo_unit', 'residual');

TRUNCATE current.assessment;
SELECT crosswalk.mint_entity_keys('assessment', array_agg(natural_key))
FROM etl.w_assessment WHERE parcel_cdw_id IS NOT NULL;

INSERT INTO current.assessment (
  assessment_id, parcel_id, assessment_date, assessment_amount, land_value, improvement_value)
SELECT k.entity_key, w.parcel_cdw_id, NULL,
       etl.try_numeric(w.asdtotal), etl.try_numeric(w.asdland), etl.try_numeric(w.asdimprove)
FROM etl.w_assessment w
JOIN crosswalk.entity_keys('assessment') k ON k.scope = '' AND k.natural_key = w.natural_key
WHERE w.parcel_cdw_id IS NOT NULL;

DELETE FROM current.exclusion_log WHERE source_table = 'prcl_prclasmt';
INSERT INTO current.exclusion_log (exclusion_id, source_table, source_natural_key, disposition, reason, raw_payload)
SELECT crosswalk.ns_key('exclusion', nextval('etl.exclusion_seq')), 'prcl_prclasmt', natural_key,
       CASE WHEN account_role = 'excluded' THEN 'excluded' ELSE 'rejected' END,
       CASE WHEN account_role IS NULL THEN 'no prcl_prcl account' ELSE 'account ' || account_role END,
       _payload
FROM etl.w_assessment
WHERE parcel_cdw_id IS NULL;

SELECT etl.reconcile('prcl_prclasmt',
  (SELECT count(*) FROM etl.src_prclasmt),
  (SELECT count(*) FROM current.assessment),
  etl.disposition_count('prcl_prclasmt', 'excluded'),
  etl.disposition_count('prcl_prclasmt', 'rejected'));
