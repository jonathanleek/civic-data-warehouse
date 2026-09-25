-- =============================================================================
-- CDW "crosswalk" schema DDL
-- -----------------------------------------------------------------------------
-- Persistent ID registries used while transforming source records into the CDW
-- spine. Crosswalk tables are authoritative state: they are not staging output
-- and must not be truncated or regenerated during normal pipeline rebuilds.
--
-- The namespace is `country_code.region_code` (documentation/schema/cdw_id.md):
-- parcel sequences are minted per namespace from a dedicated Postgres sequence
-- named parcel_seq_<country>_<region>, e.g. parcel_seq_840_29510.
--
-- Invariants enforced here:
--   * parcel_seq is immutable once minted (trigger rejects UPDATEs that change it).
--   * An explicit parcel_seq on INSERT (e.g. restoring a registry from backup)
--     advances the namespace sequence past it, so later minting cannot collide.
--   * native_ref is stored verbatim and must already be trimmed; the transform
--     layer routes untrimmed references to its reject path rather than altering
--     them here.
--   * country_code / region_code / parcel_seq carry the canonical CDW_ID widths
--     (3 / 5 / 8 digits) so they compose into a well-formed cdw_id.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS crosswalk;

-- Earlier revisions keyed the namespace on region_code alone and exposed
-- single-argument helpers. Remove those overloads so exactly one definition of
-- each function exists.
DROP FUNCTION IF EXISTS crosswalk.parcel_seq_sequence_name(text);
DROP FUNCTION IF EXISTS crosswalk.next_parcel_seq(text);

CREATE TABLE IF NOT EXISTS crosswalk.parcel_xref (
  country_code text NOT NULL,
  region_code text NOT NULL,
  native_ref text NOT NULL,
  parcel_seq bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT parcel_xref_pkey PRIMARY KEY (country_code, region_code, native_ref),
  CONSTRAINT parcel_xref_namespace_seq_key UNIQUE (country_code, region_code, parcel_seq),
  CONSTRAINT parcel_xref_country_code_format CHECK (country_code ~ '^[0-9]{3}$'),
  CONSTRAINT parcel_xref_region_code_format CHECK (region_code ~ '^[0-9]{5}$'),
  CONSTRAINT parcel_xref_native_ref_not_blank CHECK (btrim(native_ref) <> ''),
  CONSTRAINT parcel_xref_native_ref_trimmed CHECK (native_ref = btrim(native_ref)),
  CONSTRAINT parcel_xref_parcel_seq_range CHECK (parcel_seq BETWEEN 1 AND 99999999)
);

COMMENT ON SCHEMA crosswalk IS
  'Persistent CDW_ID crosswalks. Authoritative ID registry; do not truncate or regenerate.';

COMMENT ON TABLE crosswalk.parcel_xref IS
  'Authoritative mapping from a namespace-native parcel reference to the stable CDW parcel sequence.';

COMMENT ON COLUMN crosswalk.parcel_xref.country_code IS
  'ISO 3166-1 numeric country code, e.g. USA = 840. With region_code forms the CDW_ID namespace.';

COMMENT ON COLUMN crosswalk.parcel_xref.region_code IS
  'Parcel-issuing jurisdiction code within the country, e.g. St. Louis City FIPS 29510.';

COMMENT ON COLUMN crosswalk.parcel_xref.native_ref IS
  'Native parcel reference from the source system, stored verbatim, e.g. the St. Louis assessor handle.';

COMMENT ON COLUMN crosswalk.parcel_xref.parcel_seq IS
  'CDW-minted parcel localId. Unique within the country/region namespace and never reassigned.';

CREATE OR REPLACE FUNCTION crosswalk.parcel_seq_sequence_name(country_code text, region_code text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT 'parcel_seq_'
      || regexp_replace(btrim(country_code), '[^A-Za-z0-9_]+', '_', 'g')
      || '_'
      || regexp_replace(btrim(region_code), '[^A-Za-z0-9_]+', '_', 'g');
$$;

COMMENT ON FUNCTION crosswalk.parcel_seq_sequence_name(text, text) IS
  'Returns the per-namespace sequence name used to allocate parcel_xref.parcel_seq values.';

CREATE OR REPLACE FUNCTION crosswalk.ensure_parcel_seq_sequence(country_code text, region_code text)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
  normalized_country text := btrim(country_code);
  normalized_region text := btrim(region_code);
  sequence_name text;
BEGIN
  IF coalesce(normalized_country, '') = '' OR coalesce(normalized_region, '') = '' THEN
    RAISE EXCEPTION 'country_code and region_code are required to allocate a parcel sequence';
  END IF;

  sequence_name := crosswalk.parcel_seq_sequence_name(normalized_country, normalized_region);

  IF to_regclass(format('%I.%I', 'crosswalk', sequence_name)) IS NULL THEN
    EXECUTE format(
      'CREATE SEQUENCE IF NOT EXISTS %I.%I AS bigint START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 99999999 CACHE 1',
      'crosswalk',
      sequence_name
    );
  END IF;

  RETURN format('%I.%I', 'crosswalk', sequence_name);
END;
$$;

COMMENT ON FUNCTION crosswalk.ensure_parcel_seq_sequence(text, text) IS
  'Creates the per-namespace parcel sequence if needed and returns its qualified name.';

CREATE OR REPLACE FUNCTION crosswalk.next_parcel_seq(country_code text, region_code text)
RETURNS bigint
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN nextval(crosswalk.ensure_parcel_seq_sequence(country_code, region_code)::regclass);
END;
$$;

COMMENT ON FUNCTION crosswalk.next_parcel_seq(text, text) IS
  'Allocates the next parcel sequence from the sequence dedicated to the given namespace.';

CREATE OR REPLACE FUNCTION crosswalk.set_parcel_xref_defaults()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  sequence_ref text;
  sequence_last bigint;
BEGIN
  NEW.country_code := btrim(NEW.country_code);
  NEW.region_code := btrim(NEW.region_code);
  NEW.updated_at := now();

  IF TG_OP = 'UPDATE' THEN
    IF NEW.parcel_seq IS DISTINCT FROM OLD.parcel_seq THEN
      RAISE EXCEPTION
        'parcel_xref.parcel_seq is immutable once minted (namespace %.%, native_ref %: % -> %)',
        OLD.country_code, OLD.region_code, OLD.native_ref, OLD.parcel_seq, NEW.parcel_seq;
    END IF;
    RETURN NEW;
  END IF;

  IF NEW.parcel_seq IS NULL THEN
    NEW.parcel_seq := crosswalk.next_parcel_seq(NEW.country_code, NEW.region_code);
  ELSE
    -- Explicit value supplied (e.g. restoring a registry). Advance the namespace
    -- sequence past it so later minting cannot hand out the same number.
    sequence_ref := crosswalk.ensure_parcel_seq_sequence(NEW.country_code, NEW.region_code);
    EXECUTE format('SELECT last_value FROM %s', sequence_ref) INTO sequence_last;
    PERFORM setval(sequence_ref::regclass, GREATEST(NEW.parcel_seq, sequence_last), true);
  END IF;

  RETURN NEW;
END;
$$;

COMMENT ON FUNCTION crosswalk.set_parcel_xref_defaults() IS
  'Mints parcel_seq from the namespace sequence when omitted, keeps the sequence ahead of explicit values, and rejects reassignment.';

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
