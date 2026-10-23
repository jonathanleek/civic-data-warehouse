-- current.zoning_class <- prclcode_cdzoning (zoningcode, descr), plus any code
-- used by an account but missing from the decode table (description NULL), so
-- every parcel.zoning_class_id resolves.
DROP TABLE IF EXISTS etl.w_zoning_class;
CREATE TABLE etl.w_zoning_class AS
SELECT code AS natural_key, code, min(descr COLLATE "C") AS description
FROM (
  SELECT code, descr FROM etl.src_cdzoning WHERE code IS NOT NULL
  UNION ALL
  SELECT zoning, NULL FROM etl.src_prcl WHERE zoning IS NOT NULL
) s
GROUP BY code;

SELECT etl.load_lookup('zoning_class', 'etl.w_zoning_class');

-- Decode rows without a code cannot be keyed.
DELETE FROM current.exclusion_log WHERE source_table = 'prclcode_cdzoning';
SELECT etl.log_disposition('prclcode_cdzoning', NULL, 'rejected', 'decode row has no code', _payload)
FROM etl.src_cdzoning WHERE code IS NULL;

SELECT etl.reconcile('prclcode_cdzoning',
  (SELECT count(*) FROM etl.src_cdzoning),
  (SELECT count(*) FROM etl.src_cdzoning s JOIN current.zoning_class z ON z.code = s.code),
  0,
  etl.disposition_count('prclcode_cdzoning', 'rejected'));
