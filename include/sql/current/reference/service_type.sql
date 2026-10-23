-- current.service_type <- distinct forestry categories and LRA usages.
-- Keyed by division so the same word in two feeds stays two service types.
DROP TABLE IF EXISTS etl.w_service_type;
CREATE TABLE etl.w_service_type AS
SELECT DISTINCT division || ':' || code AS natural_key, code, code AS description
FROM (
  SELECT 'FORESTRY' AS division, category AS code FROM etl.src_forestry
  UNION ALL
  SELECT 'LRA', usage FROM etl.src_lra
) s
WHERE code IS NOT NULL;

SELECT etl.load_lookup('service_type', 'etl.w_service_type');
