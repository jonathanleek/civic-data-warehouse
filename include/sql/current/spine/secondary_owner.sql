-- current.secondary_owner <- residual accounts: non-footprint, non-condo
-- accounts on a footprint's handle. parcel.owner_id holds the footprint owner;
-- each residual account's owner is kept here instead of being dropped.
TRUNCATE current.secondary_owner;
SELECT crosswalk.mint_entity_keys('secondary_owner', array_agg(account_key))
FROM etl.w_account_class WHERE role = 'residual';

INSERT INTO current.secondary_owner (secondary_owner_id, parcel_id, owner_id, ownership_role, ownership_share)
SELECT k.entity_key, p.parcel_cdw_id, o.entity_key, 'additional assessor account ' || c.account_key, NULL
FROM etl.w_account_class c
JOIN etl.w_account a ON a.account_key = c.account_key
JOIN etl.w_parcel p ON p.handle = c.handle
JOIN crosswalk.entity_keys('secondary_owner') k ON k.scope = '' AND k.natural_key = c.account_key
JOIN crosswalk.entity_keys('legal_entity') o ON o.scope = '' AND o.natural_key = a.owner_nk
WHERE c.role = 'residual';

-- Capture-all guard for prcl_prcl, counted from what was actually written:
-- parcels (footprints) + condo units + secondary owners.
SELECT etl.reconcile('prcl_prcl',
  (SELECT count(*) FROM etl.src_prcl),
  (SELECT count(*) FROM current.parcel)
    + (SELECT count(*) FROM etl.w_unit w
       JOIN current.unit u ON u.parcel_seq = w.parcel_seq AND u.building_seq = w.building_seq
                          AND u.unit_seq = w.unit_seq
       WHERE w.kind = 'condo')
    + (SELECT count(*) FROM current.secondary_owner),
  etl.disposition_count('prcl_prcl', 'excluded'),
  etl.disposition_count('prcl_prcl', 'rejected'));
