with source as (
    select distinct neighborhood_code
    from {{ ref('stg_prcl__prcl') }}
    where neighborhood_code is not null
)

select
    hashtext(neighborhood_code::text) as neighborhood_id,
    neighborhood_code                 as neighborhood
from source
