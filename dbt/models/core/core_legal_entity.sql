-- One row per unique property owner extracted from the parcel table.
-- Owners are deduplicated on (name, street_address, city, state) so that
-- a single LLC or individual maps to one legal_entity_id across all parcels.
with stg as (
    select * from {{ ref('stg_prcl__prcl') }}
),

addrs as (
    select * from {{ ref('int_owner_addresses') }}
),

unique_owners as (
    select distinct
        owner_name,
        owner_street_address,
        owner_city,
        owner_state,
        owner_zip
    from stg
    where owner_name is not null
)

-- Two owners can share the same hash (name+street+city+state) but differ on zip;
-- pick one deterministically to ensure legal_entity_id is unique.
select distinct on (legal_entity_id)
    hashtext(
        coalesce(o.owner_name, '')           || '|' ||
        coalesce(o.owner_street_address, '') || '|' ||
        coalesce(o.owner_city, '')           || '|' ||
        coalesce(o.owner_state, '')
    )               as legal_entity_id,
    o.owner_name    as name,
    a.address_id
from unique_owners o
left join addrs a
    on  a.street_address = o.owner_street_address
    and coalesce(a.city, '')  = coalesce(o.owner_city, '')
    and coalesce(a.state, '') = coalesce(o.owner_state, '')
    and coalesce(a.zip, '')   = coalesce(o.owner_zip, '')
order by legal_entity_id, o.owner_name
