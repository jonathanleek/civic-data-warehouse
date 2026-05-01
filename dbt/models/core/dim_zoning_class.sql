with source as (
    select distinct zoning_class
    from {{ ref('stg_prcl__prcl') }}
    where zoning_class is not null
      and trim(zoning_class) != ''
)

select
    hashtext(zoning_class) as zoning_class_id,
    zoning_class
from source
