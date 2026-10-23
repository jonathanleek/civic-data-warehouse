-- Fail the QA layer if any error-severity check in etl.qa_result failed.
DO $$
DECLARE
  failures text;
BEGIN
  SELECT string_agg(check_name || ' (' || failing_rows || ')', ', ') INTO failures
  FROM etl.qa_result WHERE severity = 'error' AND failing_rows > 0;
  IF failures IS NOT NULL THEN
    RAISE EXCEPTION 'CDW QA checks failed: %', failures;
  END IF;
END;
$$;
