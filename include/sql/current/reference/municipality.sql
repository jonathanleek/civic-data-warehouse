-- current.municipality: the deployment's own region (static config, not a feed).
-- natural key = region code; name from crosswalk.active_namespace.region_name.
DROP TABLE IF EXISTS etl.w_municipality;
CREATE TABLE etl.w_municipality AS
SELECT crosswalk.ns_region() AS natural_key,
       crosswalk.ns_region() AS code,
       (SELECT region_name FROM crosswalk.active_namespace) AS description;

TRUNCATE current.municipality CASCADE;
SELECT crosswalk.mint_entity_keys('municipality', array_agg(natural_key)) FROM etl.w_municipality;
INSERT INTO current.municipality (municipality_id, municipality)
SELECT k.entity_key, w.description
FROM etl.w_municipality w
JOIN crosswalk.entity_keys('municipality') k ON k.scope = '' AND k.natural_key = w.natural_key;
