-- Deduplicated owner mailing addresses extracted from the parcel table.
-- One row per unique (street_address, city, state, zip) combination.
-- address_id is a deterministic integer surrogate key so downstream models
-- can reference it without a separate lookup join.
with stg as (
    select * from {{ ref('stg_prcl__prcl') }}
),

unique_addresses as (
    select distinct
        owner_street_address,
        owner_city,
        owner_state,
        owner_zip
    from stg
    where owner_street_address is not null
)

select
    hashtext(
        coalesce(owner_street_address, '') || '|' ||
        coalesce(owner_city, '')           || '|' ||
        coalesce(owner_state, '')          || '|' ||
        coalesce(owner_zip, '')
    )                       as address_id,
    owner_street_address    as street_address,
    owner_city              as city,
    owner_state             as state,
    owner_zip               as zip
from unique_addresses
