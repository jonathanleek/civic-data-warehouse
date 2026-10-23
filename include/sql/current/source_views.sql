-- =============================================================================
-- Source contract: staging tables -> etl.src_* views
-- -----------------------------------------------------------------------------
-- The only place the transforms name raw staging tables and columns. Every
-- layer runs this first, so a re-shaped city extract fails here, by name,
-- rather than deep inside a transform.
--
--   etl.build_source_view(view, candidate_tables, {alias: column}, required_aliases, optional_source)
--
-- Candidate tables are tried in order (mdb exports are named <db>_<table>,
-- plain CSVs by file name). Required aliases must exist or the task fails.
-- Optional aliases that are missing read as NULL and are listed in
-- etl.source_contract_gap; the QA layer reports them.
--
-- Columns marked UNVERIFIED are best guesses at the city's field names that
-- have not yet been checked against a real staging load; they are optional
-- so a wrong guess degrades to NULL plus a gap row instead of a failure.
-- =============================================================================

-- Assessor accounts (one row per (cityblock, parcel, ownercode)). Column names
-- verified against a real load: documentation/data_dictionary/prcl_column_classification.csv
SELECT etl.build_source_view('src_prcl', ARRAY['prcl_prcl'], '{
  "cityblock": "cityblock", "parcel": "parcel", "ownercode": "ownercode",
  "handle": "handle", "parcel9": "parcel9", "asrparcelid": "asrparcelid",
  "parcelid": "parcelid",
  "giscityblock": "giscityblock", "gisparcel": "gisparcel", "gisownercode": "gisownercode",
  "condominium": "condominium",
  "ownername": "ownername", "ownername2": "ownername2", "owneraddr": "owneraddr",
  "ownercity": "ownercity", "ownerstate": "ownerstate", "ownerzip": "ownerzip",
  "ownercountry": "ownercountry",
  "asrlanduse1": "asrlanduse1", "ward20": "ward20", "nbrhd": "nbrhd", "zip": "zip",
  "censblock20": "censblock20", "frontage": "frontage", "zoning": "zoning",
  "taxbaldue": "taxbaldue", "billyear": "billyear", "nbrofunits": "nbrofunits",
  "lowaddrnum": "lowaddrnum", "lowaddrsuf": "lowaddrsuf", "stpredir": "stpredir",
  "stname": "stname", "sttype": "sttype", "stsufdir": "stsufdir",
  "stdunitnum": "stdunitnum"
}'::jsonb, ARRAY['cityblock', 'parcel', 'ownercode', 'handle',
                 'giscityblock', 'gisparcel', 'gisownercode', 'condominium', 'asrparcelid']);

-- Situs addresses, already componentized.
SELECT etl.build_source_view('src_prcladdr', ARRAY['prcl_prcladdr'], '{
  "cityblock": "cityblock", "parcel": "parcel", "ownercode": "ownercode",
  "lowaddrnum": "lowaddrnum", "lowaddrsuf": "lowaddrsuf", "stpredir": "stpredir",
  "stname": "stname", "sttype": "sttype", "stsufdir": "stsufdir",
  "stdunitnum": "stdunitnum", "zip": "zip"
}'::jsonb, ARRAY['cityblock', 'parcel', 'ownercode'], true);

SELECT etl.build_source_view('src_prclasmt', ARRAY['prcl_prclasmt'], '{
  "cityblock": "cityblock", "parcel": "parcel", "ownercode": "ownercode",
  "asdtotal": "asdtotal", "asdland": "asdland", "asdimprove": "asdimprove"
}'::jsonb, ARRAY['cityblock', 'parcel', 'ownercode'], true);

-- Buildings: one row per (account, bldgnum).
SELECT etl.build_source_view('src_bldgall', ARRAY['prcl_bldgall'], '{
  "cityblock": "cityblock", "parcel": "parcel", "ownercode": "ownercode",
  "bldgnum": "bldgnum", "bldgusecode": "bldgusecode",
  "totalarea": "totalarea", "yearbuilt": "yearbuilt"
}'::jsonb, ARRAY['cityblock', 'parcel', 'ownercode', 'bldgnum'], true);

-- Residential building detail. Detail columns are UNVERIFIED.
SELECT etl.build_source_view('src_bldgres', ARRAY['prcl_bldgres'], '{
  "cityblock": "cityblock", "parcel": "parcel", "ownercode": "ownercode",
  "bldgnum": "bldgnum",
  "stories": "stories", "full_bath": "fullbaths", "half_bath": "halfbaths",
  "central_ac": "aircondcentral", "windows_ac": "aircondwindow",
  "garage": "garage1", "nbrofunits": "nbrofunits"
}'::jsonb, ARRAY['cityblock', 'parcel', 'ownercode', 'bldgnum'], true);

-- Building sections: one row per (account, bldgnum, sectnum). sect_use_code UNVERIFIED.
SELECT etl.build_source_view('src_bldgsect', ARRAY['prcl_bldgsect'], '{
  "cityblock": "cityblock", "parcel": "parcel", "ownercode": "ownercode",
  "bldgnum": "bldgnum", "sectnum": "sectnum", "sect_use_code": "sectusecode"
}'::jsonb, ARRAY['cityblock', 'parcel', 'ownercode', 'bldgnum', 'sectnum'], true);

-- Decode tables.
SELECT etl.build_source_view('src_cdzoning', ARRAY['prclcode_cdzoning'], '{
  "code": "zoningcode", "descr": "descr"
}'::jsonb, ARRAY['code'], true);

SELECT etl.build_source_view('src_cdlanduse', ARRAY['prclcode_cdlanduse'], '{
  "code": "landuse", "descr": "descr"
}'::jsonb, ARRAY['code'], true);

-- Permit feeds (address-only, loaded standalone). permit_number UNVERIFIED.
SELECT etl.build_source_view('src_permit_' || feed, ARRAY[feed || '_permits_last_30_days'], '{
  "permit_number": "permitnumber", "projecttype": "projecttype",
  "applicationdate": "applicationdate", "issuedate": "issuedate",
  "applicationdescription": "applicationdescription", "estprojectcost": "estprojectcost"
}'::jsonb, '{}', true)
FROM unnest(ARRAY['building', 'demolition', 'electrical', 'mechanical', 'occupancy', 'plumbing']) AS f(feed);

-- Housing conservation inspections. inspection_number / completiondate UNVERIFIED.
SELECT etl.build_source_view('src_hcdinsp', ARRAY['hcd_hcdinsp'], '{
  "handle": "handle", "inspection_number": "inspectionnum",
  "inspectdate": "inspectdate", "inspecttype": "inspecttype",
  "completiondate": "completiondate"
}'::jsonb, '{}', true);

SELECT etl.build_source_view('src_condemn', ARRAY['condemn', 'condemn_condemn'], '{
  "handle": "handle", "condlettersent": "condlettersent",
  "inspecttype": "inspecttype", "status": "status"
}'::jsonb, '{}', true);

-- Condemnation status decode. Column names UNVERIFIED.
SELECT etl.build_source_view('src_cndcodes', ARRAY['cndcodes', 'condemn_cndcodes'], '{
  "code": "code", "descr": "descr"
}'::jsonb, '{}', true);

-- Services. Column names other than handle/parcelid UNVERIFIED.
SELECT etl.build_source_view('src_forestry', ARRAY['forestry_maintenance_properties'], '{
  "handle": "handle", "parcelid": "parcelid", "category": "category",
  "service_date": "servicedate"
}'::jsonb, '{}', true);

SELECT etl.build_source_view('src_lra', ARRAY['lra_public', 'lra_public_lra_public'], '{
  "handle": "handle", "parcelid": "parcelid", "usage": "usage"
}'::jsonb, '{}', true);
