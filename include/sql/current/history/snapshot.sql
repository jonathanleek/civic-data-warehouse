-- Version every current table into history.record_version (see
-- create_history.sql): new and changed records get a new version, records that
-- disappeared from source get a tombstone. Runs after the events layer, so the
-- snapshot sees a complete, consistent load.

-- Refuse to snapshot a broken or partial load: an unbalanced feed, or an empty
-- spine while staging has accounts, would otherwise tombstone every record.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM etl.feed_reconciliation WHERE NOT balanced) THEN
    RAISE EXCEPTION 'Not snapshotting: etl.feed_reconciliation has unbalanced feeds';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM current.parcel)
     AND to_regclass('etl.src_prcl') IS NOT NULL
     AND EXISTS (SELECT 1 FROM etl.src_prcl) THEN
    RAISE EXCEPTION 'Not snapshotting: current.parcel is empty but staging has accounts (incomplete load?)';
  END IF;
END;
$$;
SELECT history.snapshot(t.table_name, t.key_column)
FROM (VALUES
  ('municipality', 'municipality_id'),
  ('division', 'division_id'),
  ('service_type', 'service_type_id'),
  ('zoning_class', 'zoning_class_id'),
  ('use_type', 'use_type_id'),
  ('permit_type', 'permit_type_id'),
  ('inspection_type', 'inspection_type_id'),
  ('address', 'address_id'),
  ('legal_entity', 'legal_entity_id'),
  ('parcel', 'cdw_id'),
  ('building', 'cdw_id'),
  ('unit', 'cdw_id'),
  ('municipal_parcel_id_mapping', 'parcel_id'),
  ('secondary_owner', 'secondary_owner_id'),
  ('assessment', 'assessment_id'),
  ('tax_delinquency', 'tax_delinquency_id'),
  ('permit', 'permit_id'),
  ('inspection', 'inspection_id'),
  ('condemnation', 'condemnation_id'),
  ('service', 'service_id')
) AS t(table_name, key_column);
