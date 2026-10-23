-- current.use_type <- prclcode_cdlanduse (landuse, descr) plus every building
-- use code seen in prcl_bldgall / prcl_bldgsect (same land-use vocabulary).
-- Codes absent from the decode table load with a NULL description.
DROP TABLE IF EXISTS etl.w_use_type;
CREATE TABLE etl.w_use_type AS
SELECT code AS natural_key, code, min(descr COLLATE "C") AS description
FROM (
  SELECT code, descr FROM etl.src_cdlanduse WHERE code IS NOT NULL
  UNION ALL
  SELECT bldgusecode, NULL FROM etl.src_bldgall WHERE bldgusecode IS NOT NULL
  UNION ALL
  SELECT sect_use_code, NULL FROM etl.src_bldgsect WHERE sect_use_code IS NOT NULL
) s
GROUP BY code;

SELECT etl.load_lookup('use_type', 'etl.w_use_type');

DELETE FROM current.exclusion_log WHERE source_table = 'prclcode_cdlanduse';
SELECT etl.log_disposition('prclcode_cdlanduse', NULL, 'rejected', 'decode row has no code', _payload)
FROM etl.src_cdlanduse WHERE code IS NULL;

SELECT etl.reconcile('prclcode_cdlanduse',
  (SELECT count(*) FROM etl.src_cdlanduse),
  (SELECT count(*) FROM etl.src_cdlanduse s JOIN current.use_type u ON u.code = s.code),
  0,
  etl.disposition_count('prclcode_cdlanduse', 'rejected'));
