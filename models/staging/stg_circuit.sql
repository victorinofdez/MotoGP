with
    source as (
        select * from {{ source('raw', 'events') }}
),
renamed as (
    select 
        circuit_id,
        circuit_legacy_id,
        circuit_name,
        circuit_nation,
        circuit_place
    from source
    qualify row_number() over (
            partition by circuit_id
            order by _ingested_at desc
    ) = 1
)

select * from renamed
