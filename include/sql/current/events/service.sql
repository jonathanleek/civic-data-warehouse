-- current.service <- forestry_maintenance_properties + lra_public.
-- parcel_id via handle -> parcel (NULL when the handle is not a loaded parcel);
-- division / service_type from the reference layer. fee has no source column.
-- Natural key = (feed, handle, service type, date), not a row hash, so an edit
-- to another column is a history change rather than a new service.
DROP TABLE IF EXISTS etl.w_service;
CREATE TABLE etl.w_service AS
SELECT feed, division, code, handle, service_date, _payload,
       concat_ws(chr(31), feed, coalesce(handle, ''), coalesce(code, ''), coalesce(service_date, '')) || '#'
         || row_number() OVER (PARTITION BY feed, handle, code, service_date
                               ORDER BY _payload::text COLLATE "C") AS natural_key
FROM (
  SELECT 'forestry' AS feed, 'FORESTRY' AS division, category AS code, handle, service_date, _payload
  FROM etl.src_forestry
  UNION ALL
  SELECT 'lra', 'LRA', usage, handle, NULL, _payload FROM etl.src_lra
) s;

TRUNCATE current.service;
SELECT crosswalk.mint_entity_keys('service', array_agg(natural_key)) FROM etl.w_service;

INSERT INTO current.service (
  service_id, parcel_id, building_id, unit_id, owner_id, division_id, service_date, service_type_id, fee)
SELECT k.entity_key, p.parcel_cdw_id, NULL, NULL, NULL, d.division_id, etl.try_date(w.service_date),
       t.service_type_id, NULL
FROM etl.w_service w
JOIN crosswalk.entity_keys('service') k ON k.scope = '' AND k.natural_key = w.natural_key
LEFT JOIN etl.w_parcel p ON p.handle = w.handle
LEFT JOIN current.division d ON d.code = w.division
LEFT JOIN current.service_type t ON t.code = w.code
  AND t.service_type_id = (SELECT entity_key FROM crosswalk.entity_keys('service_type')
                           WHERE scope = '' AND natural_key = w.division || ':' || w.code);

SELECT etl.reconcile('services',
  (SELECT count(*) FROM etl.src_forestry) + (SELECT count(*) FROM etl.src_lra),
  (SELECT count(*) FROM current.service), 0, 0);
