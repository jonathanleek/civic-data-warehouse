-- current.permit <- the six *_permits_last_30_days feeds, loaded standalone
-- (the model has no permit -> spine link; the feeds carry only addresses).
-- Natural key: feed + permit number, or feed + row hash when the feed has no
-- permit number. completion_date / cancel_date / contractor_id / applicant_id
-- have no verified source column and are NULL.
DROP TABLE IF EXISTS etl.w_permit;
CREATE TABLE etl.w_permit AS
SELECT 'building' AS feed, * FROM etl.src_permit_building
UNION ALL SELECT 'demolition', * FROM etl.src_permit_demolition
UNION ALL SELECT 'electrical', * FROM etl.src_permit_electrical
UNION ALL SELECT 'mechanical', * FROM etl.src_permit_mechanical
UNION ALL SELECT 'occupancy', * FROM etl.src_permit_occupancy
UNION ALL SELECT 'plumbing', * FROM etl.src_permit_plumbing;

DROP TABLE IF EXISTS etl.w_permit_keyed;
CREATE TABLE etl.w_permit_keyed AS
SELECT w.*,
       w.feed || ':' || coalesce(w.permit_number, md5(w._payload::text)) || '#'
         || row_number() OVER (PARTITION BY w.feed, coalesce(w.permit_number, md5(w._payload::text))
                               ORDER BY w._payload::text COLLATE "C") AS natural_key
FROM etl.w_permit w;

TRUNCATE current.permit;
SELECT crosswalk.mint_entity_keys('permit', array_agg(natural_key)) FROM etl.w_permit_keyed;

INSERT INTO current.permit (
  permit_id, permit_type_id, application_date, issue_date, completion_date, cancel_date,
  description, cost, applicant_id, contractor_id)
SELECT k.entity_key, t.permit_type_id, etl.try_date(w.applicationdate), etl.try_date(w.issuedate),
       NULL, NULL, w.applicationdescription, etl.try_numeric(w.estprojectcost)::float, NULL, NULL
FROM etl.w_permit_keyed w
JOIN crosswalk.entity_keys('permit') k ON k.scope = '' AND k.natural_key = w.natural_key
LEFT JOIN current.permit_type t
  ON t.category = w.feed AND t.code IS NOT DISTINCT FROM w.projecttype;

SELECT etl.reconcile('permits_last_30_days',
  (SELECT count(*) FROM etl.w_permit), (SELECT count(*) FROM current.permit), 0, 0);
