with source as (
  select * from {{ source('staging', 'parcel') }}
),
renamed as (
  select
    parcel_id::int as parcel_id,
    county,
    owner_id::int as owner_id,
    ward::int as ward,
    neighborhood,
    zip_code::int as zip_code,
    census_block::int as census_block,
    frontage::int as frontage,
    zoning_class_id::int as zoning_class_id
  from source
)
select * from renamed
