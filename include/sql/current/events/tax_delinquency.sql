-- current.tax_delinquency <- loaded prcl_prcl accounts with a balance due
-- (taxbaldue > 0). One row per account, linked to the account's owner and the
-- parcel. last_paid_year has no source column and is NULL (not fabricated).
TRUNCATE current.tax_delinquency;

DROP TABLE IF EXISTS etl.w_tax_delinquency;
CREATE TABLE etl.w_tax_delinquency AS
SELECT c.account_key, a.owner_nk, p.parcel_cdw_id, etl.try_numeric(a.taxbaldue) AS amount
FROM etl.w_account_class c
JOIN etl.w_account a ON a.account_key = c.account_key
JOIN etl.w_parcel p ON p.handle = c.handle
WHERE c.row_num = 1
  AND c.role IN ('footprint', 'condo_unit', 'residual')
  AND etl.try_numeric(a.taxbaldue) > 0;

SELECT crosswalk.mint_entity_keys('tax_delinquency', array_agg(account_key)) FROM etl.w_tax_delinquency;

INSERT INTO current.tax_delinquency (tax_delinquency_id, legal_entity_id, parcel_id, last_paid_year, amount_delinquent)
SELECT k.entity_key, o.entity_key, w.parcel_cdw_id, NULL, w.amount
FROM etl.w_tax_delinquency w
JOIN crosswalk.entity_keys('tax_delinquency') k ON k.scope = '' AND k.natural_key = w.account_key
LEFT JOIN crosswalk.entity_keys('legal_entity') o ON o.scope = '' AND o.natural_key = w.owner_nk;
