{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

select
    constructor_id,
    constructor_name,
    constructor_legacy_id
from {{ ref('stg_constructor') }}