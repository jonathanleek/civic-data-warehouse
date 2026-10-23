-- =============================================================================
-- CDW "history" schema
-- -----------------------------------------------------------------------------
-- `current` is truncated and reloaded on every run, so history is what remembers
-- how a record looked before. After each load, history.snapshot() compares every
-- row of a current table with its open version here:
--
--   new record          -> insert a version
--   changed record      -> close the open version (valid_to), insert a new one
--   unchanged record    -> nothing
--   record gone (dead)  -> close the open version, insert a tombstone
--                          (is_deleted = true, payload = last known row)
--   dead record returns -> close the tombstone, insert a new version
--
-- Rows are stored generically as jsonb keyed by (table_name, record_id), so new
-- current tables need no history DDL. Authoritative and append-only: this
-- script never drops data and is safe to re-run.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS history;

CREATE TABLE IF NOT EXISTS history.record_version (
  table_name text NOT NULL,
  record_id text NOT NULL,
  valid_from timestamptz NOT NULL,
  valid_to timestamptz,
  is_deleted boolean NOT NULL DEFAULT false,
  row_hash text NOT NULL,
  payload jsonb NOT NULL,
  CONSTRAINT record_version_pkey PRIMARY KEY (table_name, record_id, valid_from)
);

-- At most one open version per record.
CREATE UNIQUE INDEX IF NOT EXISTS record_version_open_idx
  ON history.record_version (table_name, record_id) WHERE valid_to IS NULL;

COMMENT ON TABLE history.record_version IS
  'Versioned copies of current-schema rows. Open version = valid_to IS NULL; tombstones mark records that disappeared from source.';

CREATE TABLE IF NOT EXISTS history.snapshot_log (
  table_name text NOT NULL,
  taken_at timestamptz NOT NULL,
  inserted bigint NOT NULL,
  changed bigint NOT NULL,
  deleted bigint NOT NULL,
  revived bigint NOT NULL,
  unchanged bigint NOT NULL,
  CONSTRAINT snapshot_log_pkey PRIMARY KEY (table_name, taken_at)
);

-- Snapshots current.<p_table>, whose primary key column is p_key_column.
CREATE OR REPLACE FUNCTION history.snapshot(p_table text, p_key_column text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_inserted bigint;
  v_changed bigint;
  v_deleted bigint;
  v_revived bigint;
  v_unchanged bigint;
BEGIN
  DROP TABLE IF EXISTS pg_temp.snap_current;
  EXECUTE format(
    'CREATE TEMP TABLE snap_current ON COMMIT DROP AS
     SELECT t.%1$I::text AS record_id, to_jsonb(t) AS payload, md5(to_jsonb(t)::text) AS row_hash
     FROM current.%2$I t',
    p_key_column, p_table);
  CREATE UNIQUE INDEX ON snap_current (record_id);

  DROP TABLE IF EXISTS pg_temp.snap_open;
  CREATE TEMP TABLE snap_open ON COMMIT DROP AS
  SELECT record_id, row_hash, is_deleted, payload
  FROM history.record_version
  WHERE table_name = p_table AND valid_to IS NULL;
  CREATE UNIQUE INDEX ON snap_open (record_id);

  SELECT
    count(*) FILTER (WHERE o.record_id IS NULL),
    count(*) FILTER (WHERE NOT o.is_deleted AND o.row_hash <> c.row_hash),
    count(*) FILTER (WHERE o.is_deleted),
    count(*) FILTER (WHERE NOT o.is_deleted AND o.row_hash = c.row_hash)
  INTO v_inserted, v_changed, v_revived, v_unchanged
  FROM snap_current c LEFT JOIN snap_open o USING (record_id);

  SELECT count(*) INTO v_deleted
  FROM snap_open o
  WHERE NOT o.is_deleted AND NOT EXISTS (SELECT 1 FROM snap_current c WHERE c.record_id = o.record_id);

  -- Close open versions that changed, died, or are tombstones being revived.
  UPDATE history.record_version v
  SET valid_to = v_now
  FROM snap_open o
  LEFT JOIN snap_current c ON c.record_id = o.record_id
  WHERE v.table_name = p_table AND v.valid_to IS NULL AND v.record_id = o.record_id
    AND (c.record_id IS NULL AND NOT o.is_deleted
         OR c.record_id IS NOT NULL AND (o.is_deleted OR o.row_hash <> c.row_hash));

  -- New, changed and revived records.
  INSERT INTO history.record_version (table_name, record_id, valid_from, row_hash, payload)
  SELECT p_table, c.record_id, v_now, c.row_hash, c.payload
  FROM snap_current c
  LEFT JOIN snap_open o ON o.record_id = c.record_id
  WHERE o.record_id IS NULL OR o.is_deleted OR o.row_hash <> c.row_hash;

  -- Tombstones for records that disappeared.
  INSERT INTO history.record_version (table_name, record_id, valid_from, is_deleted, row_hash, payload)
  SELECT p_table, o.record_id, v_now, true, o.row_hash, o.payload
  FROM snap_open o
  WHERE NOT o.is_deleted AND NOT EXISTS (SELECT 1 FROM snap_current c WHERE c.record_id = o.record_id);

  INSERT INTO history.snapshot_log (table_name, taken_at, inserted, changed, deleted, revived, unchanged)
  VALUES (p_table, v_now, v_inserted, v_changed, v_deleted, v_revived, v_unchanged);
END;
$$;

COMMENT ON FUNCTION history.snapshot(text, text) IS
  'Versions current.<table> into history.record_version: inserts new/changed/revived rows, tombstones dead ones.';

-- Every superseded version (the record changed afterwards).
CREATE OR REPLACE VIEW history.changed_records AS
SELECT v.*
FROM history.record_version v
WHERE v.valid_to IS NOT NULL AND NOT v.is_deleted
  AND EXISTS (
    SELECT 1 FROM history.record_version n
    WHERE n.table_name = v.table_name AND n.record_id = v.record_id
      AND n.valid_from = v.valid_to AND NOT n.is_deleted);

-- Records currently absent from source, with their last known contents.
CREATE OR REPLACE VIEW history.dead_records AS
SELECT table_name, record_id, valid_from AS died_at, payload AS last_payload
FROM history.record_version
WHERE valid_to IS NULL AND is_deleted;
