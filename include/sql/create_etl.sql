-- =============================================================================
-- CDW "etl" schema
-- -----------------------------------------------------------------------------
-- Shared machinery for the staging -> current transforms
-- (documentation/pipeline_architecture/staging_to_current.md):
--
--   * Source contract  - etl.build_source_view() exposes each staging table as
--                        an etl.src_* view with stable column aliases, so the
--                        transform SQL never names a raw staging column. Missing
--                        required columns fail loudly; missing optional ones
--                        are recorded in etl.source_contract_gap.
--   * Safe casts       - etl.try_int / try_numeric / try_date return NULL for
--                        values that do not parse, never an error. The QA layer
--                        counts source values that failed to cast.
--   * Capture-all      - etl.log_disposition() writes excluded / rejected rows
--                        to current.exclusion_log; etl.reconcile() asserts
--                        loaded + excluded + rejected = source per feed.
--   * Config seeds     - etl.non_real_estate_land_use.
--
-- Work tables built by the transforms (etl.w_*) also live here. Nothing in this
-- schema is authoritative: it is safe to drop and recreate. Re-runnable.
-- Functions that touch `current` are plpgsql so they bind at call time and this
-- script can run before or after create_current.sql.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS etl;

-- -----------------------------------------------------------------------------
-- Source contract
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.source_contract_gap (
  view_name text NOT NULL,
  source_table text,
  target_alias text,
  source_column text,
  detail text NOT NULL,
  detected_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE etl.source_contract_gap IS
  'Optional source tables/columns that were absent when the etl.src_* views were last built. The aliased column reads as NULL.';

-- Builds etl.<p_view> over the first staging table in p_sources that exists.
--   p_columns   jsonb object {alias: staging_column}
--   p_required  aliases that must resolve to a real column (else error)
--   p_optional_source  when true and no candidate table exists, an empty view
--                      is created instead of raising
-- Every aliased column is text with '' normalized to NULL (COPY loads quoted
-- empty fields as ''). _payload carries the full raw row for the exclusion log.
CREATE OR REPLACE FUNCTION etl.build_source_view(
  p_view text,
  p_sources text[],
  p_columns jsonb,
  p_required text[] DEFAULT '{}',
  p_optional_source boolean DEFAULT false)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
  v_source text;
  v_alias text;
  v_column text;
  v_select text[] := '{}';
  v_missing_required text[] := '{}';
BEGIN
  DELETE FROM etl.source_contract_gap WHERE view_name = p_view;

  SELECT s INTO v_source
  FROM unnest(p_sources) WITH ORDINALITY AS u(s, ord)
  WHERE to_regclass(format('staging.%I', s)) IS NOT NULL
  ORDER BY ord
  LIMIT 1;

  IF v_source IS NULL THEN
    IF NOT p_optional_source THEN
      RAISE EXCEPTION 'etl.%: none of the source tables % exist in schema staging', p_view, p_sources;
    END IF;
    INSERT INTO etl.source_contract_gap (view_name, source_table, detail)
    VALUES (p_view, array_to_string(p_sources, ' | '), 'source table missing; view is empty');
  END IF;

  FOR v_alias, v_column IN SELECT key, value FROM jsonb_each_text(p_columns) ORDER BY key LOOP
    IF v_source IS NOT NULL AND EXISTS (
      SELECT 1 FROM information_schema.columns c
      WHERE c.table_schema = 'staging' AND c.table_name = v_source AND c.column_name = v_column
    ) THEN
      v_select := v_select || format('nullif(s.%I::text, %L) AS %I', v_column, '', v_alias);
    ELSE
      IF v_source IS NOT NULL AND v_alias = ANY (p_required) THEN
        v_missing_required := v_missing_required || format('%s (%s)', v_alias, v_column);
      END IF;
      IF v_source IS NOT NULL THEN
        INSERT INTO etl.source_contract_gap (view_name, source_table, target_alias, source_column, detail)
        VALUES (p_view, v_source, v_alias, v_column, 'column missing; alias reads as NULL');
      END IF;
      v_select := v_select || format('NULL::text AS %I', v_alias);
    END IF;
  END LOOP;

  IF cardinality(v_missing_required) > 0 THEN
    RAISE EXCEPTION 'etl.%: staging.% is missing required column(s): %',
      p_view, v_source, array_to_string(v_missing_required, ', ');
  END IF;

  EXECUTE format('DROP VIEW IF EXISTS etl.%I CASCADE', p_view);
  IF v_source IS NOT NULL THEN
    EXECUTE format(
      'CREATE VIEW etl.%I AS SELECT %s, to_jsonb(s) AS _payload FROM staging.%I s',
      p_view, array_to_string(v_select, ', '), v_source);
  ELSE
    EXECUTE format(
      'CREATE VIEW etl.%I AS SELECT %s, NULL::jsonb AS _payload WHERE false',
      p_view, array_to_string(v_select, ', '));
  END IF;

  RETURN v_source;
END;
$$;

-- -----------------------------------------------------------------------------
-- Assessor account key: cityblock/parcel/ownercode. NULL components become ''
-- so ('1','2',NULL) and ('1',NULL,'2') stay distinct ('1/2/' vs '1//2').
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION etl.account_key(p_cityblock text, p_parcel text, p_ownercode text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT coalesce(p_cityblock, '') || '/' || coalesce(p_parcel, '') || '/' || coalesce(p_ownercode, '');
$$;

-- -----------------------------------------------------------------------------
-- Safe casts (NULL on anything that does not parse; never raise)
-- -----------------------------------------------------------------------------

-- Integer-valued text only: '12', '-3', '12.000' -> int; '12.5', 'abc' -> NULL.
CREATE OR REPLACE FUNCTION etl.try_int(p text)
RETURNS int
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p ~ '^\s*[-+]?[0-9]{1,9}(\.0*)?\s*$' THEN trunc(p::numeric)::int
  END;
$$;

CREATE OR REPLACE FUNCTION etl.try_numeric(p text)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p ~ '^\s*[-+]?([0-9]+(\.[0-9]*)?|\.[0-9]+)\s*$' THEN p::numeric
  END;
$$;

-- Accepts ISO dates (YYYY-MM-DD[ time]) and US dates (MM/DD/YY[YY][ time]),
-- the two shapes the city extracts use. Two-digit years pivot at 70 (Access
-- export convention: 00-69 -> 2000s, 70-99 -> 1900s).
CREATE OR REPLACE FUNCTION etl.try_date(p text)
RETURNS date
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  m text[];
  y int;
BEGIN
  IF p IS NULL THEN
    RETURN NULL;
  END IF;
  m := regexp_match(p, '^\s*([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})([ T].*)?$');
  IF m IS NOT NULL THEN
    RETURN make_date(m[1]::int, m[2]::int, m[3]::int);
  END IF;
  m := regexp_match(p, '^\s*([0-9]{1,2})/([0-9]{1,2})/([0-9]{2}|[0-9]{4})(\s.*)?$');
  IF m IS NOT NULL THEN
    y := m[3]::int;
    IF length(m[3]) = 2 THEN
      y := y + CASE WHEN y < 70 THEN 2000 ELSE 1900 END;
    END IF;
    RETURN make_date(y, m[1]::int, m[2]::int);
  END IF;
  RETURN NULL;
EXCEPTION WHEN others THEN
  -- make_date rejects impossible dates (e.g. 02/30/20).
  RETURN NULL;
END;
$$;

-- -----------------------------------------------------------------------------
-- Capture-all: exclusion / reject logging and reconciliation
-- -----------------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS etl.exclusion_seq;

-- If etl was dropped and recreated while current.exclusion_log still holds
-- rows, move the sequence past them so new exclusion_ids cannot collide.
DO $$
BEGIN
  IF to_regclass('current.exclusion_log') IS NOT NULL THEN
    PERFORM setval('etl.exclusion_seq', greatest(
      (SELECT coalesce(max(split_part(exclusion_id, '.', 4)::bigint), 0)
       FROM current.exclusion_log
       WHERE split_part(exclusion_id, '.', 4) ~ '^[0-9]+$'),
      (SELECT last_value FROM etl.exclusion_seq)));
  END IF;
END;
$$;

-- Appends one row to current.exclusion_log. disposition is 'excluded' (a
-- deliberate filter, e.g. non-real-estate) or 'rejected' (could not be loaded,
-- e.g. a missing key).
CREATE OR REPLACE FUNCTION etl.log_disposition(
  p_source_table text, p_natural_key text, p_disposition text, p_reason text, p_payload jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO current.exclusion_log
    (exclusion_id, source_table, source_natural_key, disposition, reason, raw_payload)
  VALUES (
    crosswalk.ns_key('exclusion', nextval('etl.exclusion_seq')),
    p_source_table, p_natural_key, p_disposition, p_reason, p_payload);
END;
$$;

CREATE TABLE IF NOT EXISTS etl.feed_reconciliation (
  feed text PRIMARY KEY,
  source_rows bigint NOT NULL,
  loaded bigint NOT NULL,
  excluded bigint NOT NULL,
  rejected bigint NOT NULL,
  balanced boolean GENERATED ALWAYS AS (loaded + excluded + rejected = source_rows) STORED,
  checked_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE etl.feed_reconciliation IS
  'Capture-all guard results: every source row is loaded, excluded, or rejected. One row per feed, replaced on each run.';

-- Records the counts for a feed. It does not raise: the row must commit so an
-- imbalance stays visible. Each layer ends with current/assert_balanced.sql,
-- which fails the layer (before it emits its Asset) if any feed is unbalanced.
CREATE OR REPLACE FUNCTION etl.reconcile(
  p_feed text, p_source_rows bigint, p_loaded bigint, p_excluded bigint, p_rejected bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO etl.feed_reconciliation (feed, source_rows, loaded, excluded, rejected)
  VALUES (p_feed, p_source_rows, p_loaded, p_excluded, p_rejected)
  ON CONFLICT (feed) DO UPDATE
    SET source_rows = EXCLUDED.source_rows,
        loaded = EXCLUDED.loaded,
        excluded = EXCLUDED.excluded,
        rejected = EXCLUDED.rejected,
        checked_at = now();

  IF p_loaded + p_excluded + p_rejected <> p_source_rows THEN
    RAISE WARNING
      'Capture-all guard failed for %: loaded % + excluded % + rejected % <> source %',
      p_feed, p_loaded, p_excluded, p_rejected, p_source_rows;
  END IF;
END;
$$;

-- Convenience: counts excluded / rejected rows already logged for a feed.
CREATE OR REPLACE FUNCTION etl.disposition_count(p_source_table text, p_disposition text)
RETURNS bigint
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN (
    SELECT count(*) FROM current.exclusion_log
    WHERE source_table = p_source_table AND disposition = p_disposition);
END;
$$;

-- -----------------------------------------------------------------------------
-- Config: non-real-estate land uses (issue #48)
-- -----------------------------------------------------------------------------
-- The spine holds real-estate entities only. Accounts whose assessor land use
-- (prcl_prcl.asrlanduse1, vocabulary 24) is listed here are excluded from
-- parcel / building / unit and logged to current.exclusion_log. Parking lots,
-- parking garages and condo garage spaces (1116) are real estate and are NOT
-- listed. Codes are the city's vocabulary identifiers, verbatim.

CREATE TABLE IF NOT EXISTS etl.non_real_estate_land_use (
  land_use_code text PRIMARY KEY,
  title text NOT NULL,
  rationale text NOT NULL
);

INSERT INTO etl.non_real_estate_land_use (land_use_code, title, rationale) VALUES
  ('3997', 'SIGNS AND ADVERTISING DISPLAYS',
   'Billboard / sign structure assessed as its own taxable account; not land, a building, or a unit.'),
  ('6312', 'OUTDOOR ADVERTISING SERVICES',
   'Outdoor-advertising (billboard) account; same rationale as 3997.')
ON CONFLICT (land_use_code) DO UPDATE
  SET title = EXCLUDED.title, rationale = EXCLUDED.rationale;

-- -----------------------------------------------------------------------------
-- Lookup loader
-- -----------------------------------------------------------------------------
-- Reloads current.<p_table> (columns <p_table>_id, code, <p_table>) from a work
-- table with columns (natural_key, code, description). Keys are minted in
-- crosswalk.entity_xref under entity = p_table, so a code keeps its id across
-- runs. TRUNCATE ... CASCADE also clears rows that reference the lookup; the
-- downstream layers reload them (they are scheduled on this layer's Asset).
-- Returns the number of rows loaded.
CREATE OR REPLACE FUNCTION etl.load_lookup(p_table text, p_work regclass)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  v_loaded bigint;
BEGIN
  EXECUTE format('TRUNCATE current.%I CASCADE', p_table);
  EXECUTE format(
    'SELECT crosswalk.mint_entity_keys(%L, array_agg(natural_key)) FROM %s', p_table, p_work);
  EXECUTE format(
    'INSERT INTO current.%1$I (%2$I, code, %1$I)
     SELECT k.entity_key, w.code, w.description
     FROM %3$s w
     JOIN crosswalk.entity_keys(%1$L) k ON k.scope = %4$L AND k.natural_key = w.natural_key',
    p_table, p_table || '_id', p_work, '');
  GET DIAGNOSTICS v_loaded = ROW_COUNT;
  RETURN v_loaded;
END;
$$;
