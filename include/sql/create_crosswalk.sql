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

-- =============================================================================
-- Active namespace
-- -----------------------------------------------------------------------------
-- The deployment's own CDW_ID namespace (country.region). Transform SQL reads
-- it through crosswalk.ns_country() / crosswalk.ns_region() instead of
-- templating codes into every file. The transform DAGs set it from the
-- CDW-COUNTRY-ID / CDW-REGION-ID Airflow Variables at the start of each run.
-- Exactly one row.
-- =============================================================================

CREATE TABLE IF NOT EXISTS crosswalk.active_namespace (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  country_code text NOT NULL CHECK (country_code ~ '^[0-9]{3}$'),
  region_code text NOT NULL CHECK (region_code ~ '^[0-9]{5}$'),
  region_name text,
  updated_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE crosswalk.active_namespace IS
  'The country.region namespace this deployment mints identifiers in (single row).';

CREATE OR REPLACE FUNCTION crosswalk.set_active_namespace(
  p_country_code text, p_region_code text, p_region_name text DEFAULT NULL)
RETURNS void
LANGUAGE sql
AS $$
  INSERT INTO crosswalk.active_namespace (country_code, region_code, region_name)
  VALUES (btrim(p_country_code), btrim(p_region_code), p_region_name)
  ON CONFLICT (singleton) DO UPDATE
    SET country_code = EXCLUDED.country_code,
        region_code = EXCLUDED.region_code,
        region_name = EXCLUDED.region_name,
        updated_at = now();
$$;

CREATE OR REPLACE FUNCTION crosswalk.ns_country()
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v text;
BEGIN
  SELECT country_code INTO v FROM crosswalk.active_namespace;
  IF v IS NULL THEN
    RAISE EXCEPTION 'crosswalk.active_namespace is not set; call crosswalk.set_active_namespace() first';
  END IF;
  RETURN v;
END;
$$;

CREATE OR REPLACE FUNCTION crosswalk.ns_region()
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v text;
BEGIN
  SELECT region_code INTO v FROM crosswalk.active_namespace;
  IF v IS NULL THEN
    RAISE EXCEPTION 'crosswalk.active_namespace is not set; call crosswalk.set_active_namespace() first';
  END IF;
  RETURN v;
END;
$$;

-- Formats a non-spine surrogate key: <country>.<region>.<entity>.<seq>
-- (documentation/schema/cdw_id.md, "Storage").
CREATE OR REPLACE FUNCTION crosswalk.ns_key(p_entity text, p_seq bigint)
RETURNS text
LANGUAGE sql
STABLE
AS $$
  SELECT CASE WHEN p_seq IS NULL THEN NULL
              ELSE crosswalk.ns_country() || '.' || crosswalk.ns_region() || '.' || p_entity || '.' || p_seq::text
         END;
$$;

-- =============================================================================
-- Non-spine entity registry
-- -----------------------------------------------------------------------------
-- Maps an entity's source natural key to a stable per-namespace sequence, the
-- same way parcel_xref does for parcels. `scope` partitions the sequence when
-- numbering restarts under a parent (e.g. condo unit_seq within a parcel);
-- unscoped entities use scope ''. Like parcel_xref this is authoritative state:
-- append-only, never truncated, entity_seq immutable once minted.
-- =============================================================================

CREATE TABLE IF NOT EXISTS crosswalk.entity_xref (
  country_code text NOT NULL,
  region_code text NOT NULL,
  entity text NOT NULL,
  scope text NOT NULL DEFAULT '',
  natural_key text NOT NULL,
  entity_seq bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT entity_xref_pkey PRIMARY KEY (country_code, region_code, entity, scope, natural_key),
  CONSTRAINT entity_xref_seq_key UNIQUE (country_code, region_code, entity, scope, entity_seq),
  CONSTRAINT entity_xref_country_code_format CHECK (country_code ~ '^[0-9]{3}$'),
  CONSTRAINT entity_xref_region_code_format CHECK (region_code ~ '^[0-9]{5}$'),
  CONSTRAINT entity_xref_entity_format CHECK (entity ~ '^[a-z][a-z0-9_]*$'),
  CONSTRAINT entity_xref_seq_positive CHECK (entity_seq >= 1)
);

COMMENT ON TABLE crosswalk.entity_xref IS
  'Authoritative mapping from a non-spine entity natural key to its stable namespaced sequence.';

CREATE OR REPLACE FUNCTION crosswalk.entity_xref_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.entity_seq IS DISTINCT FROM OLD.entity_seq
     OR NEW.natural_key IS DISTINCT FROM OLD.natural_key THEN
    RAISE EXCEPTION 'crosswalk.entity_xref rows are immutable once minted (% % %)',
      OLD.entity, OLD.scope, OLD.natural_key;
  END IF;
  RETURN NEW;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'entity_xref_immutable'
      AND tgrelid = 'crosswalk.entity_xref'::regclass
  ) THEN
    CREATE TRIGGER entity_xref_immutable
    BEFORE UPDATE ON crosswalk.entity_xref
    FOR EACH ROW
    EXECUTE FUNCTION crosswalk.entity_xref_immutable();
  END IF;
END;
$$;

-- Mints sequences for every (scope, natural_key) pair not yet registered for
-- p_entity in the active namespace. Existing keys are untouched, so re-running
-- is a no-op. New keys are numbered after the scope's current maximum in
-- byte order of natural_key (COLLATE "C"), so the result does not depend on
-- the database's locale. Returns the number of keys minted.
CREATE OR REPLACE FUNCTION crosswalk.mint_entity_keys(
  p_entity text, p_scopes text[], p_natural_keys text[])
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  v_country text := crosswalk.ns_country();
  v_region text := crosswalk.ns_region();
  v_minted bigint;
BEGIN
  -- array_agg() over zero rows is NULL: treat as "nothing to mint".
  IF coalesce(cardinality(p_natural_keys), 0) = 0 THEN
    RETURN 0;
  END IF;
  IF coalesce(cardinality(p_scopes), 0) <> cardinality(p_natural_keys) THEN
    RAISE EXCEPTION 'mint_entity_keys: p_scopes and p_natural_keys must be the same length';
  END IF;

  -- Serialize minters so two concurrent runs cannot both claim max(seq) + 1.
  LOCK TABLE crosswalk.entity_xref IN SHARE ROW EXCLUSIVE MODE;

  WITH input AS (
    SELECT DISTINCT coalesce(u.scope, '') AS scope, u.natural_key
    FROM unnest(p_scopes, p_natural_keys) AS u(scope, natural_key)
    WHERE u.natural_key IS NOT NULL
  ),
  fresh AS (
    SELECT i.scope, i.natural_key
    FROM input i
    WHERE NOT EXISTS (
      SELECT 1 FROM crosswalk.entity_xref x
      WHERE x.country_code = v_country AND x.region_code = v_region
        AND x.entity = p_entity AND x.scope = i.scope AND x.natural_key = i.natural_key
    )
  ),
  base AS (
    SELECT x.scope, max(x.entity_seq) AS max_seq
    FROM crosswalk.entity_xref x
    WHERE x.country_code = v_country AND x.region_code = v_region AND x.entity = p_entity
    GROUP BY x.scope
  )
  INSERT INTO crosswalk.entity_xref (country_code, region_code, entity, scope, natural_key, entity_seq)
  SELECT v_country, v_region, p_entity, f.scope, f.natural_key,
         coalesce(b.max_seq, 0)
           + row_number() OVER (PARTITION BY f.scope ORDER BY f.natural_key COLLATE "C")
  FROM fresh f
  LEFT JOIN base b ON b.scope = f.scope;

  GET DIAGNOSTICS v_minted = ROW_COUNT;
  RETURN v_minted;
END;
$$;

CREATE OR REPLACE FUNCTION crosswalk.mint_entity_keys(p_entity text, p_natural_keys text[])
RETURNS bigint
LANGUAGE sql
AS $$
  SELECT crosswalk.mint_entity_keys(
    p_entity, array_fill(''::text, ARRAY[coalesce(cardinality(p_natural_keys), 0)]), p_natural_keys);
$$;

-- Read side: the active namespace's registry for one entity, with the
-- formatted key. Transforms join to this after minting.
CREATE OR REPLACE FUNCTION crosswalk.entity_keys(p_entity text)
RETURNS TABLE (scope text, natural_key text, entity_seq bigint, entity_key text)
LANGUAGE sql
STABLE
AS $$
  SELECT x.scope, x.natural_key, x.entity_seq,
         x.country_code || '.' || x.region_code || '.' || x.entity || '.' || x.entity_seq::text
  FROM crosswalk.entity_xref x
  WHERE x.country_code = crosswalk.ns_country()
    AND x.region_code = crosswalk.ns_region()
    AND x.entity = p_entity;
$$;
