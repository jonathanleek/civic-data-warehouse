-- current.parcel <- one row per footprint account (one per handle).
-- parcel_seq is minted in crosswalk.parcel_xref (persistent: a handle keeps its
-- seq forever); new handles are numbered in handle order. Codes are verbatim
-- text; ward and frontage are the only casts. county has no source and is NULL.
INSERT INTO crosswalk.parcel_xref (country_code, region_code, native_ref)
SELECT crosswalk.ns_country(), crosswalk.ns_region(), c.handle
FROM etl.w_account_class c
WHERE c.role = 'footprint'
  AND NOT EXISTS (
    SELECT 1 FROM crosswalk.parcel_xref x
    WHERE x.country_code = crosswalk.ns_country() AND x.region_code = crosswalk.ns_region()
      AND x.native_ref = c.handle)
ORDER BY c.handle COLLATE "C";

DROP TABLE IF EXISTS etl.w_parcel;
CREATE TABLE etl.w_parcel AS
SELECT c.handle, c.account_key, c.is_condo, x.parcel_seq,
       crosswalk.ns_country() || '.' || crosswalk.ns_region() || '.'
         || lpad(x.parcel_seq::text, 8, '0') || '.0000.00000' AS parcel_cdw_id
FROM etl.w_account_class c
JOIN crosswalk.parcel_xref x
  ON x.country_code = crosswalk.ns_country() AND x.region_code = crosswalk.ns_region()
 AND x.native_ref = c.handle
WHERE c.role = 'footprint';

CREATE UNIQUE INDEX ON etl.w_parcel (handle);

TRUNCATE current.parcel CASCADE;
INSERT INTO current.parcel (
  country_code, region_code, parcel_seq, county, owner_id, ward, neighborhood,
  zip_code, census_block, frontage, zoning_class_id)
SELECT crosswalk.ns_country(), crosswalk.ns_region(), p.parcel_seq, NULL,
       o.entity_key, etl.try_int(a.ward20), a.nbrhd, a.zip, a.censblock20,
       etl.try_numeric(a.frontage), z.zoning_class_id
FROM etl.w_parcel p
JOIN etl.w_account a ON a.account_key = p.account_key
LEFT JOIN crosswalk.entity_keys('legal_entity') o ON o.scope = '' AND o.natural_key = a.owner_nk
LEFT JOIN current.zoning_class z ON z.code = a.zoning;

-- Native reference preservation (INSPIRE nationalCadastralReference).
INSERT INTO current.municipal_parcel_id_mapping (parcel_id, municipal_parcel_id, municipality_id)
SELECT p.parcel_cdw_id, p.handle, (SELECT municipality_id FROM current.municipality LIMIT 1)
FROM etl.w_parcel p;
