-- current.address <- three address roles, deduplicated on the full address:
--   * situs addresses from prcl_prcladdr (already componentized, mapped 1:1)
--   * each account's primary situs address on prcl_prcl (what unit.address_id
--     points at)
--   * owner mailing addresses: the freeform owneraddr line is NOT parsed; it
--     is kept verbatim in raw_line beside the structured city/state/zip.
-- Components are carried verbatim. street_name_suffix = sttype + ' ' +
-- stsufdir (the model has one suffix column). Situs rows carry no city/state:
-- the source does not state them and CDW does not fabricate them.
-- Natural key layout (chr(31)-separated, matches entities/account.sql):
--   raw_line, number, number_suffix, prefix, name, suffix, secondary, city, state, zip
DROP TABLE IF EXISTS etl.w_prcladdr;
CREATE TABLE etl.w_prcladdr AS
SELECT
  s.*,
  CASE WHEN coalesce(s.lowaddrnum, s.stname) IS NULL THEN 'address row has no street number or name'
  END AS reject_reason,
  concat_ws(chr(31), '', coalesce(s.lowaddrnum, ''), coalesce(s.lowaddrsuf, ''),
            coalesce(s.stpredir, ''), coalesce(s.stname, ''),
            coalesce(concat_ws(' ', s.sttype, s.stsufdir), ''),
            coalesce(s.stdunitnum, ''), '', '', coalesce(s.zip, '')) AS address_nk
FROM etl.src_prcladdr s;

DROP TABLE IF EXISTS etl.w_address;
CREATE TABLE etl.w_address AS
SELECT DISTINCT ON (address_nk) *
FROM (
  SELECT address_nk FROM etl.w_prcladdr WHERE reject_reason IS NULL
  UNION ALL
  SELECT situs_address_nk FROM etl.w_account WHERE situs_address_nk IS NOT NULL
  UNION ALL
  SELECT owner_address_nk FROM etl.w_account WHERE owner_address_nk IS NOT NULL
) s
CROSS JOIN LATERAL (
  SELECT nullif(p[1], '') AS raw_line, nullif(p[2], '') AS number_text,
         nullif(p[3], '') AS street_number_suffix, nullif(p[4], '') AS street_name_prefix,
         nullif(p[5], '') AS street_name, nullif(p[6], '') AS street_name_suffix,
         nullif(p[7], '') AS secondary_designator, nullif(p[8], '') AS city,
         nullif(p[9], '') AS state, nullif(p[10], '') AS zip
  FROM string_to_array(s.address_nk, chr(31)) AS p
) c
ORDER BY address_nk;

TRUNCATE current.address CASCADE;
SELECT crosswalk.mint_entity_keys('address', array_agg(address_nk)) FROM etl.w_address;
INSERT INTO current.address (
  address_id, raw_line, street_number, street_number_suffix, street_name_prefix,
  street_name, street_name_suffix, secondary_designator, city, state, zip)
SELECT k.entity_key,
       -- A street number that is not an integer (e.g. a range '123-125') cannot
       -- go in street_number; keep the situs line verbatim in raw_line instead.
       coalesce(w.raw_line, CASE WHEN w.number_text IS NOT NULL AND etl.try_int(w.number_text) IS NULL THEN
         concat_ws(' ', w.number_text, w.street_number_suffix, w.street_name_prefix, w.street_name,
                   w.street_name_suffix, w.secondary_designator) END),
       etl.try_int(w.number_text), w.street_number_suffix,
       w.street_name_prefix, w.street_name, w.street_name_suffix,
       w.secondary_designator, w.city, w.state, w.zip
FROM etl.w_address w
JOIN crosswalk.entity_keys('address') k ON k.scope = '' AND k.natural_key = w.address_nk;

DELETE FROM current.exclusion_log WHERE source_table = 'prcl_prcladdr';
SELECT etl.log_disposition('prcl_prcladdr', etl.account_key(cityblock, parcel, ownercode),
                           'rejected', reject_reason, _payload)
FROM etl.w_prcladdr WHERE reject_reason IS NOT NULL;

SELECT etl.reconcile('prcl_prcladdr',
  (SELECT count(*) FROM etl.src_prcladdr),
  (SELECT count(*) FROM etl.w_prcladdr WHERE reject_reason IS NULL),
  0,
  etl.disposition_count('prcl_prcladdr', 'rejected'));
