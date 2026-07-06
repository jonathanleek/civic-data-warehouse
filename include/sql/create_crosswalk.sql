-- =============================================================================
-- CDW "crosswalk" schema DDL
-- -----------------------------------------------------------------------------
-- Persistent ID registries used while transforming source records into the CDW
-- spine. Crosswalk tables are authoritative state: they are not staging output
-- and must not be truncated or regenerated during normal pipeline rebuilds.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS crosswalk;

CREATE TABLE IF NOT EXISTS crosswalk.parcel_xref (
  region_code text NOT NULL,
  native_ref text NOT NULL,
  parcel_seq bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT parcel_xref_pkey PRIMARY KEY (region_code, native_ref),
  CONSTRAINT parcel_xref_region_seq_key UNIQUE (region_code, parcel_seq),
  CONSTRAINT parcel_xref_region_code_not_blank CHECK (btrim(region_code) <> ''),
  CONSTRAINT parcel_xref_native_ref_not_blank CHECK (btrim(native_ref) <> ''),
  CONSTRAINT parcel_xref_parcel_seq_positive CHECK (parcel_seq > 0)
);

COMMENT ON SCHEMA crosswalk IS
  'Persistent CDW_ID crosswalks. Authoritative ID registry; do not truncate or regenerate.';

COMMENT ON TABLE crosswalk.parcel_xref IS
  'Authoritative mapping from a region-native parcel reference to the stable CDW parcel sequence.';

COMMENT ON COLUMN crosswalk.parcel_xref.region_code IS
  'Parcel-issuing jurisdiction namespace, e.g. St. Louis City FIPS 29510.';

COMMENT ON COLUMN crosswalk.parcel_xref.native_ref IS
  'Native parcel reference from the source system, e.g. the St. Louis assessor handle.';

COMMENT ON COLUMN crosswalk.parcel_xref.parcel_seq IS
  'CDW-minted parcel localId. Unique within region_code and never reassigned.';

CREATE OR REPLACE FUNCTION crosswalk.parcel_seq_sequence_name(region_code text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT 'parcel_seq_' || regexp_replace(btrim(region_code), '[^A-Za-z0-9_]+', '_', 'g');
$$;

COMMENT ON FUNCTION crosswalk.parcel_seq_sequence_name(text) IS
  'Returns the per-region sequence name used to allocate parcel_xref.parcel_seq values.';

CREATE OR REPLACE FUNCTION crosswalk.next_parcel_seq(region_code text)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  normalized_region text := btrim(region_code);
  sequence_name text;
BEGIN
  IF normalized_region IS NULL OR normalized_region = '' THEN
    RAISE EXCEPTION 'region_code is required to allocate a parcel sequence';
  END IF;

  sequence_name := crosswalk.parcel_seq_sequence_name(normalized_region);

  IF to_regclass(format('%I.%I', 'crosswalk', sequence_name)) IS NULL THEN
    EXECUTE format(
      'CREATE SEQUENCE IF NOT EXISTS %I.%I AS bigint START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1',
      'crosswalk',
      sequence_name
    );
  END IF;

  RETURN nextval(format('%I.%I', 'crosswalk', sequence_name)::regclass);
END;
$$;

COMMENT ON FUNCTION crosswalk.next_parcel_seq(text) IS
  'Allocates the next parcel sequence from the sequence dedicated to the given region_code.';

CREATE OR REPLACE FUNCTION crosswalk.set_parcel_xref_defaults()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.region_code := btrim(NEW.region_code);
  NEW.updated_at := now();

  IF NEW.parcel_seq IS NULL THEN
    NEW.parcel_seq := crosswalk.next_parcel_seq(NEW.region_code);
  END IF;

  RETURN NEW;
END;
$$;

COMMENT ON FUNCTION crosswalk.set_parcel_xref_defaults() IS
  'Assigns parcel_seq from the region-specific sequence when a parcel_xref row omits it.';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'parcel_xref_set_defaults'
      AND tgrelid = 'crosswalk.parcel_xref'::regclass
  ) THEN
    CREATE TRIGGER parcel_xref_set_defaults
    BEFORE INSERT OR UPDATE ON crosswalk.parcel_xref
    FOR EACH ROW
    EXECUTE FUNCTION crosswalk.set_parcel_xref_defaults();
  END IF;
END;
$$;
