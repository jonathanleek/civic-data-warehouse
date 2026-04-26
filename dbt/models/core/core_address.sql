-- Owner mailing addresses sourced from the parcel table.
-- The owneraddr field is a single-line string (not pre-parsed), so it is
-- stored in street_name; parsed components will be added in a future pass.
with addrs as (
    select * from {{ ref('int_owner_addresses') }}
)

-- DISTINCT ON guards against the rare hashtext() 32-bit collision.
select distinct on (address_id)
    address_id,
    null::varchar   as street_number,
    null::varchar   as street_name_prefix,
    street_address  as street_name,
    null::varchar   as street_name_suffix,
    null::varchar   as secondary_designator,
    city,
    state,
    zip
from addrs
order by address_id
