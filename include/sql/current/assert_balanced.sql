-- Last task of every layer: fail (before the layer emits its Asset) if any
-- feed's capture-all counts do not balance. etl.reconcile only records the
-- counts, so the evidence in etl.feed_reconciliation / current.exclusion_log
-- is committed and inspectable when this fails.
DO $$
DECLARE
  failures text;
BEGIN
  SELECT string_agg(format('%s (loaded %s + excluded %s + rejected %s <> source %s)',
                           feed, loaded, excluded, rejected, source_rows), '; ')
  INTO failures
  FROM etl.feed_reconciliation
  WHERE NOT balanced;
  IF failures IS NOT NULL THEN
    RAISE EXCEPTION 'Capture-all guard failed: %', failures;
  END IF;
END;
$$;
