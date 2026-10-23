-- current.condemnation <- condemn. status is decoded through cndcodes when the
-- code is in the decode table (a documented decode join), else kept verbatim.
-- inspection_id stays NULL: condemn shares no key with hcd_hcdinsp.
-- Natural key = (handle, letter date, type): a status change updates the same
-- condemnation (a history change) instead of minting a new one.
DROP TABLE IF EXISTS etl.w_condemnation;
CREATE TABLE etl.w_condemnation AS
SELECT s.*,
       concat_ws(chr(31), coalesce(s.handle, ''), coalesce(s.condlettersent, ''),
                 coalesce(s.inspecttype, '')) || '#'
         || row_number() OVER (PARTITION BY s.handle, s.condlettersent, s.inspecttype
                               ORDER BY s._payload::text COLLATE "C") AS natural_key,
       (SELECT min(d.descr COLLATE "C") FROM etl.src_cndcodes d WHERE d.code = s.status) AS status_descr
FROM etl.src_condemn s;

TRUNCATE current.condemnation;
SELECT crosswalk.mint_entity_keys('condemnation', array_agg(natural_key)) FROM etl.w_condemnation;

INSERT INTO current.condemnation (condemnation_id, inspection_id, letter_date, condemnation_type, status)
SELECT k.entity_key, NULL, etl.try_date(w.condlettersent), w.inspecttype, coalesce(w.status_descr, w.status)
FROM etl.w_condemnation w
JOIN crosswalk.entity_keys('condemnation') k ON k.scope = '' AND k.natural_key = w.natural_key;

SELECT etl.reconcile('condemn',
  (SELECT count(*) FROM etl.src_condemn), (SELECT count(*) FROM current.condemnation), 0, 0);
