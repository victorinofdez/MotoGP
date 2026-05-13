{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

select
    category_id,
    name            as category_name,
    legacy_id       as category_legacy_id,
    class_tier
from {{ ref('stg_category') }}