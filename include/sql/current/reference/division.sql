-- current.division: the city divisions that deliver a service (seed).
-- Extend the VALUES list when a new service feed is added.
DROP TABLE IF EXISTS etl.w_division;
CREATE TABLE etl.w_division AS
SELECT code AS natural_key, code, description
FROM (VALUES
  ('FORESTRY', 'Forestry Division'),
  ('LRA', 'Land Reutilization Authority')
) AS v(code, description);

SELECT etl.load_lookup('division', 'etl.w_division');
