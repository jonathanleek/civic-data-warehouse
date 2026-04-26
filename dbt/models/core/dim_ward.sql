with source as (
    select distinct ward_number
    from {{ ref('stg_prcl__prcl') }}
    where ward_number is not null
)

select
    hashtext(ward_number::text) as ward_id,
    ward_number                 as ward
from source
