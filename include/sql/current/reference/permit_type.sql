-- current.permit_type <- distinct (feed, projecttype) across the six permit
-- feeds. category records the feed; a permit with no projecttype gets its
-- feed's generic type (code NULL).
DROP TABLE IF EXISTS etl.w_permit;
CREATE TABLE etl.w_permit AS
SELECT 'building' AS feed, * FROM etl.src_permit_building
UNION ALL SELECT 'demolition', * FROM etl.src_permit_demolition
UNION ALL SELECT 'electrical', * FROM etl.src_permit_electrical
UNION ALL SELECT 'mechanical', * FROM etl.src_permit_mechanical
UNION ALL SELECT 'occupancy', * FROM etl.src_permit_occupancy
UNION ALL SELECT 'plumbing', * FROM etl.src_permit_plumbing;

DROP TABLE IF EXISTS etl.w_permit_type;
CREATE TABLE etl.w_permit_type AS
SELECT DISTINCT feed || ':' || coalesce(projecttype, '') AS natural_key,
       feed AS category, projecttype AS code
FROM etl.w_permit;

TRUNCATE current.permit_type CASCADE;
SELECT crosswalk.mint_entity_keys('permit_type', array_agg(natural_key)) FROM etl.w_permit_type;
INSERT INTO current.permit_type (permit_type_id, category, code, permit_type)
SELECT k.entity_key, w.category, w.code, coalesce(w.code, w.category || ' permit')
FROM etl.w_permit_type w
JOIN crosswalk.entity_keys('permit_type') k ON k.scope = '' AND k.natural_key = w.natural_key;
