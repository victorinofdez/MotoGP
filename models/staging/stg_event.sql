with
    source as (
        select * from {{ source('raw', 'events') }}
),
renamed as (
    select 
        id as event_id,
        circuit_id,
        season_id,
        country_iso,
        short_name,
        sponsored_name,
        year,
        date_end,
        date_start
    from source
    qualify row_number() over (
            partition by id
            order by _ingested_at desc
    ) = 1
)

select * from renamed
