-- current.legal_entity <- distinct owners on prcl_prcl, deduplicated on
-- (ownername, ownername2, mailing address). name / name2 are verbatim.
-- Owners on condemn / hcd_hcdinsp are not loaded yet: their owner columns are
-- not in the verified source contract.
DROP TABLE IF EXISTS etl.w_legal_entity;
CREATE TABLE etl.w_legal_entity AS
SELECT DISTINCT ON (owner_nk) owner_nk, ownername, ownername2, owner_address_nk
FROM etl.w_account
WHERE owner_nk IS NOT NULL
ORDER BY owner_nk;

TRUNCATE current.legal_entity CASCADE;
SELECT crosswalk.mint_entity_keys('legal_entity', array_agg(owner_nk)) FROM etl.w_legal_entity;
INSERT INTO current.legal_entity (legal_entity_id, name, name2, address_id)
SELECT k.entity_key, w.ownername, w.ownername2, a.entity_key
FROM etl.w_legal_entity w
JOIN crosswalk.entity_keys('legal_entity') k ON k.scope = '' AND k.natural_key = w.owner_nk
LEFT JOIN crosswalk.entity_keys('address') a ON a.scope = '' AND a.natural_key = w.owner_address_nk;
