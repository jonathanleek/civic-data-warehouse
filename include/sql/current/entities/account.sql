-- etl.w_account: one row per distinct assessor account in prcl_prcl, with the
-- natural keys of its owner and addresses. Shared by the entity, spine and
-- event layers. Exact duplicate account rows collapse here; the spine layer
-- logs the extra copies as rejected.
--
-- Address natural keys are the address columns joined with a unit separator
-- (chr(31)), NULLs as empty, so identical addresses from any source collapse
-- to one current.address row.
DROP TABLE IF EXISTS etl.w_account;
CREATE TABLE etl.w_account AS
SELECT DISTINCT ON (account_key)
  account_key,
  a.*,
  CASE WHEN a.ownername IS NOT NULL OR a.ownername2 IS NOT NULL THEN
    concat_ws(chr(31), coalesce(a.ownername, ''), coalesce(a.ownername2, ''), coalesce(owner_address_nk, ''))
  END AS owner_nk,
  owner_address_nk,
  situs_address_nk
FROM etl.src_prcl a
CROSS JOIN LATERAL (
  SELECT
    etl.account_key(a.cityblock, a.parcel, a.ownercode) AS account_key,
    CASE WHEN coalesce(a.owneraddr, a.ownercity, a.ownerstate, a.ownerzip) IS NOT NULL THEN
      concat_ws(chr(31), coalesce(a.owneraddr, ''), '', '', '', '', '', '',
                coalesce(a.ownercity, ''), coalesce(a.ownerstate, ''), coalesce(a.ownerzip, ''))
    END AS owner_address_nk,
    CASE WHEN coalesce(a.lowaddrnum, a.stname) IS NOT NULL THEN
      concat_ws(chr(31), '', coalesce(a.lowaddrnum, ''), coalesce(a.lowaddrsuf, ''),
                coalesce(a.stpredir, ''), coalesce(a.stname, ''),
                coalesce(concat_ws(' ', a.sttype, a.stsufdir), ''),
                coalesce(a.stdunitnum, ''), '', '', coalesce(a.zip, ''))
    END AS situs_address_nk
) k
ORDER BY account_key, a._payload::text COLLATE "C";

CREATE UNIQUE INDEX ON etl.w_account (account_key);
