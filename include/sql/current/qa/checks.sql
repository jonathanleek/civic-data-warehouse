-- Data-quality / reconciliation checks over a completed load (issue #80).
-- Results go to etl.qa_result (replaced each run) and are committed; the next
-- task, qa/assert.sql, fails the run if any 'error' check failed. 'warning'
-- checks are reported only.
--   error    capture-all balance, spine invariants, crosswalk coverage,
--            lookup resolution, no-mutation audit of verbatim columns
--   warning  source values that did not cast, optional source columns missing
CREATE TABLE IF NOT EXISTS etl.qa_result (
  check_name text PRIMARY KEY,
  severity text NOT NULL CHECK (severity IN ('error', 'warning')),
  failing_rows bigint NOT NULL,
  detail text,
  checked_at timestamptz NOT NULL DEFAULT now()
);
TRUNCATE etl.qa_result;

INSERT INTO etl.qa_result (check_name, severity, failing_rows, detail)
-- Capture-all ----------------------------------------------------------------
SELECT 'feeds_balanced', 'error', count(*), string_agg(feed, ', ')
FROM etl.feed_reconciliation WHERE NOT balanced
-- Spine ------------------------------------------------------------------------
UNION ALL
SELECT 'parcel_per_footprint', 'error',
       abs((SELECT count(*) FROM current.parcel)
           - (SELECT count(DISTINCT handle) FROM etl.w_account_class WHERE role = 'footprint')),
       'parcel rows vs distinct real-estate footprint handles'
UNION ALL
SELECT 'parcel_has_native_ref', 'error', count(*), 'parcels without municipal_parcel_id_mapping'
FROM current.parcel p
WHERE NOT EXISTS (SELECT 1 FROM current.municipal_parcel_id_mapping m WHERE m.parcel_id = p.cdw_id)
UNION ALL
SELECT 'parcel_in_crosswalk', 'error', count(*), 'parcels whose parcel_seq is not in crosswalk.parcel_xref'
FROM current.parcel p
WHERE NOT EXISTS (
  SELECT 1 FROM crosswalk.parcel_xref x
  WHERE x.country_code = p.country_code AND x.region_code = p.region_code AND x.parcel_seq = p.parcel_seq)
UNION ALL
SELECT 'building_has_unit', 'error', count(*), 'buildings with no unit'
FROM current.building b
WHERE NOT EXISTS (SELECT 1 FROM current.unit u WHERE u.building_cdw_id = b.cdw_id)
-- Lookup resolution: a source code that is present must resolve ---------------
UNION ALL
SELECT 'parcel_zoning_resolves', 'error', count(*), 'accounts with a zoning code but no zoning_class_id'
FROM current.parcel p
JOIN current.municipal_parcel_id_mapping m ON m.parcel_id = p.cdw_id
JOIN etl.w_parcel wp ON wp.handle = m.municipal_parcel_id
JOIN etl.w_account a ON a.account_key = wp.account_key
WHERE a.zoning IS NOT NULL AND p.zoning_class_id IS NULL
UNION ALL
SELECT 'building_use_type_resolves', 'error', count(*), 'buildings with a use code but no use_type_id'
FROM etl.w_bldgall b
JOIN etl.w_parcel p ON p.account_key = b.account_key
JOIN current.building cb ON cb.parcel_seq = p.parcel_seq AND cb.building_seq = b.building_seq
WHERE b.disposition = 'building' AND b.bldgusecode IS NOT NULL AND cb.use_type_id IS NULL
-- No-mutation audit: verbatim columns equal their source ----------------------
UNION ALL
SELECT 'parcel_verbatim', 'error', count(*), 'parcel zip/census_block/neighborhood differ from prcl_prcl'
FROM current.parcel p
JOIN current.municipal_parcel_id_mapping m ON m.parcel_id = p.cdw_id
JOIN etl.w_parcel wp ON wp.handle = m.municipal_parcel_id
JOIN etl.w_account a ON a.account_key = wp.account_key
WHERE p.zip_code IS DISTINCT FROM a.zip
   OR p.census_block IS DISTINCT FROM a.censblock20
   OR p.neighborhood IS DISTINCT FROM a.nbrhd
UNION ALL
SELECT 'owner_verbatim', 'error', count(*), 'parcel owner name differs from prcl_prcl.ownername'
FROM current.parcel p
JOIN current.municipal_parcel_id_mapping m ON m.parcel_id = p.cdw_id
JOIN etl.w_parcel wp ON wp.handle = m.municipal_parcel_id
JOIN etl.w_account a ON a.account_key = wp.account_key
LEFT JOIN current.legal_entity e ON e.legal_entity_id = p.owner_id
WHERE e.name IS DISTINCT FROM a.ownername
-- Cast loss: non-empty source values that did not parse (target left NULL) ----
UNION ALL
SELECT 'cast_loss_' || c.name, 'warning', c.n, c.detail
FROM (
  SELECT 'prcl_prcl_ward20' AS name, count(*) AS n, 'ward20 not an integer' AS detail
  FROM etl.w_account WHERE ward20 IS NOT NULL AND etl.try_int(ward20) IS NULL
  UNION ALL SELECT 'prcl_prcl_frontage', count(*), 'frontage not numeric'
  FROM etl.w_account WHERE frontage IS NOT NULL AND etl.try_numeric(frontage) IS NULL
  UNION ALL SELECT 'prcl_prcl_taxbaldue', count(*), 'taxbaldue not numeric'
  FROM etl.w_account WHERE taxbaldue IS NOT NULL AND etl.try_numeric(taxbaldue) IS NULL
  UNION ALL SELECT 'prcl_prcl_lowaddrnum', count(*), 'situs street number not an integer (line kept in address.raw_line)'
  FROM etl.w_account WHERE lowaddrnum IS NOT NULL AND etl.try_int(lowaddrnum) IS NULL
  UNION ALL SELECT 'prcl_bldgall_totalarea', count(*), 'totalarea not an integer'
  FROM etl.src_bldgall WHERE totalarea IS NOT NULL AND etl.try_int(totalarea) IS NULL
  UNION ALL SELECT 'prcl_bldgall_yearbuilt', count(*), 'yearbuilt not an integer'
  FROM etl.src_bldgall WHERE yearbuilt IS NOT NULL AND etl.try_int(yearbuilt) IS NULL
  UNION ALL SELECT 'prcl_bldgres_detail', count(*), 'bldgres stories/baths/AC/garage/units not numeric'
  FROM etl.src_bldgres
  WHERE (stories IS NOT NULL AND etl.try_numeric(stories) IS NULL)
     OR (full_bath IS NOT NULL AND etl.try_int(full_bath) IS NULL)
     OR (half_bath IS NOT NULL AND etl.try_int(half_bath) IS NULL)
     OR (central_ac IS NOT NULL AND etl.try_int(central_ac) IS NULL)
     OR (windows_ac IS NOT NULL AND etl.try_int(windows_ac) IS NULL)
     OR (garage IS NOT NULL AND etl.try_int(garage) IS NULL)
     OR (nbrofunits IS NOT NULL AND etl.try_int(nbrofunits) IS NULL)
  UNION ALL SELECT 'prcl_prclasmt_values', count(*), 'assessment values not numeric'
  FROM etl.src_prclasmt
  WHERE (asdtotal IS NOT NULL AND etl.try_numeric(asdtotal) IS NULL)
     OR (asdland IS NOT NULL AND etl.try_numeric(asdland) IS NULL)
     OR (asdimprove IS NOT NULL AND etl.try_numeric(asdimprove) IS NULL)
  UNION ALL SELECT 'permit_dates', count(*), 'permit application/issue date not a date'
  FROM etl.w_permit
  WHERE (applicationdate IS NOT NULL AND etl.try_date(applicationdate) IS NULL)
     OR (issuedate IS NOT NULL AND etl.try_date(issuedate) IS NULL)
  UNION ALL SELECT 'permit_cost', count(*), 'estprojectcost not numeric'
  FROM etl.w_permit WHERE estprojectcost IS NOT NULL AND etl.try_numeric(estprojectcost) IS NULL
  UNION ALL SELECT 'hcd_hcdinsp_dates', count(*), 'inspection dates not a date'
  FROM etl.src_hcdinsp
  WHERE (inspectdate IS NOT NULL AND etl.try_date(inspectdate) IS NULL)
     OR (completiondate IS NOT NULL AND etl.try_date(completiondate) IS NULL)
  UNION ALL SELECT 'condemn_letter_date', count(*), 'condlettersent not a date'
  FROM etl.src_condemn WHERE condlettersent IS NOT NULL AND etl.try_date(condlettersent) IS NULL
  UNION ALL SELECT 'forestry_service_date', count(*), 'service date not a date'
  FROM etl.src_forestry WHERE service_date IS NOT NULL AND etl.try_date(service_date) IS NULL
) c
-- Source contract ----------------------------------------------------------------
UNION ALL
SELECT 'source_contract_gaps', 'warning', count(*),
       string_agg(DISTINCT view_name || coalesce('.' || target_alias, ''), ', ')
FROM etl.source_contract_gap;
