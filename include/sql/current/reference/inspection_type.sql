-- current.inspection_type <- distinct inspection types in hcd_hcdinsp.
-- The code is the source value verbatim; the city publishes no decode table
-- for it, so the description repeats the code.
DROP TABLE IF EXISTS etl.w_inspection_type;
CREATE TABLE etl.w_inspection_type AS
SELECT DISTINCT inspecttype AS natural_key, inspecttype AS code, inspecttype AS description
FROM etl.src_hcdinsp
WHERE inspecttype IS NOT NULL;

SELECT etl.load_lookup('inspection_type', 'etl.w_inspection_type');
