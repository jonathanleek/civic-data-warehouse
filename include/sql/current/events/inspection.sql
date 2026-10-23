-- current.inspection <- hcd_hcdinsp. The model links an inspection to a
-- building only: building_id is set when the inspected handle's parcel has
-- exactly one building, and NULL otherwise (the source does not say which).
-- number_of_violations has no count source and is NULL.
DROP TABLE IF EXISTS etl.w_single_building;
CREATE TABLE etl.w_single_building AS
SELECT parcel_cdw_id, min(cdw_id) AS building_cdw_id
FROM current.building
GROUP BY parcel_cdw_id
HAVING count(*) = 1;
CREATE UNIQUE INDEX ON etl.w_single_building (parcel_cdw_id);

DROP TABLE IF EXISTS etl.w_inspection;
CREATE TABLE etl.w_inspection AS
SELECT s.*,
       coalesce(s.inspection_number, md5(s._payload::text)) || '#'
         || row_number() OVER (PARTITION BY coalesce(s.inspection_number, md5(s._payload::text))
                               ORDER BY s._payload::text COLLATE "C") AS natural_key,
       sb.building_cdw_id
FROM etl.src_hcdinsp s
LEFT JOIN etl.w_parcel p ON p.handle = s.handle
LEFT JOIN etl.w_single_building sb ON sb.parcel_cdw_id = p.parcel_cdw_id;

TRUNCATE current.inspection CASCADE;
SELECT crosswalk.mint_entity_keys('inspection', array_agg(natural_key)) FROM etl.w_inspection;

INSERT INTO current.inspection (
  inspection_id, building_id, inspection_date, completion_date, inspection_type_id, number_of_violations)
SELECT k.entity_key, w.building_cdw_id, etl.try_date(w.inspectdate), etl.try_date(w.completiondate),
       t.inspection_type_id, NULL
FROM etl.w_inspection w
JOIN crosswalk.entity_keys('inspection') k ON k.scope = '' AND k.natural_key = w.natural_key
LEFT JOIN current.inspection_type t ON t.code = w.inspecttype;

SELECT etl.reconcile('hcd_hcdinsp',
  (SELECT count(*) FROM etl.src_hcdinsp), (SELECT count(*) FROM current.inspection), 0, 0);
