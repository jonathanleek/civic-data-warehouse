with source as (
    select * from {{ source('stl_open_data', 'prcl_prcl') }}
),

-- Source data contains duplicate handle values; keep one row per handle.
deduped as (
    select distinct on (handle) *
    from source
    where nullif(trim(handle), '') is not null
    order by handle
),

renamed as (
    select
        -- identifiers
        handle,
        nullif(trim(parcel9), '')                           as parcel9,
        nullif(cityblock, '')::float                        as city_block,
        nullif(parcel, '')::int                             as parcel_number,
        nullif(ownercode, '')::int                          as owner_code,
        nullif(giscityblock, '')::float                     as gis_city_block,
        nullif(gisparcel, '')::int                          as gis_parcel,
        nullif(gisownercode, '')::int                       as gis_owner_code,

        -- parcel street address components
        nullif(trim(lowaddrnum), '')::int                   as street_number_low,
        nullif(trim(highaddrnum), '')::int                  as street_number_high,
        nullif(trim(stpredir), '')                          as street_prefix_dir,
        nullif(trim(stname), '')                            as street_name,
        nullif(trim(sttype), '')                            as street_type,
        nullif(trim(stsufdir), '')                          as street_suffix_dir,
        nullif(trim(stdunitnum), '')                        as unit_number,
        nullif(trim(zip), '')                               as zip_code,

        -- owner / legal entity fields
        nullif(trim(ownername), '')                         as owner_name,
        nullif(trim(ownername2), '')                        as owner_name_2,
        nullif(trim(owneraddr), '')                         as owner_street_address,
        nullif(trim(ownercity), '')                         as owner_city,
        nullif(trim(ownerstate), '')                        as owner_state,
        nullif(trim(ownerzip), '')                          as owner_zip,

        -- parcel geography / jurisdiction
        nullif(frontage, '')::float                         as frontage,
        nullif(trim(zoning), '')                            as zoning_class,
        nullif(ward10, '')::int                             as ward_number,
        nullif(nbrhd, '')::int                              as neighborhood_code,
        nullif(censblock10, '')::int                        as census_block,

        -- assessment values (stored as currency strings)
        nullif(trim(asdland), '')::float                    as assessed_land_value,
        nullif(trim(asdimprove), '')::float                 as assessed_improvement_value,
        nullif(trim(asdtotal), '')::float                   as assessed_total_value

    from deduped
)

select * from renamed
