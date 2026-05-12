
with source as (
    select * from {{ source('raw', 'results') }}
),

renamed as (
    select 
        constructor_id,
        constructor_name,
        constructor_legacy_id
    from source
    where constructor_id is not null
    qualify row_number() over (
        partition by constructor_id
        order by _ingested_at desc
    ) = 1
)

select * from renamed