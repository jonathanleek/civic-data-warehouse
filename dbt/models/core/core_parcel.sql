with stg as (
    select * from {{ ref('stg_prcl__prcl') }}
),

wards       as (select * from {{ ref('dim_ward') }}),
hoods       as (select * from {{ ref('dim_neighborhood') }}),
zoning      as (select * from {{ ref('dim_zoning_class') }}),
owners      as (select * from {{ ref('core_legal_entity') }})

-- DISTINCT ON guards against the rare hashtext() 32-bit collision across 127K handles.
select distinct on (hashtext(stg.handle))
    hashtext(stg.handle)    as parcel_id,
    null::int               as county_id,   -- county table populated in a future pass
    o.legal_entity_id       as owner_id,
    w.ward_id,
    n.neighborhood_id,
    stg.zip_code,
    stg.census_block,
    stg.frontage::int       as frontage,
    z.zoning_class_id

from stg

left join wards  w on w.ward             = stg.ward_number
left join hoods  n on n.neighborhood     = stg.neighborhood_code
left join zoning z on z.zoning_class     = stg.zoning_class
-- Only join owners when owner_name is present; null owner_name produces a hash
-- that has no corresponding legal_entity_id (entities require a name).
left join owners o
    on stg.owner_name is not null
    and o.legal_entity_id = hashtext(
        coalesce(stg.owner_name, '')           || '|' ||
        coalesce(stg.owner_street_address, '') || '|' ||
        coalesce(stg.owner_city, '')           || '|' ||
        coalesce(stg.owner_state, '')
    )

order by hashtext(stg.handle)
