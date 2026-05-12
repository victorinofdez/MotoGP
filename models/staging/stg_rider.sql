with source as (
    select * from {{ source('raw', 'results') }}
),

renamed as (
    select 
        rider_id,
        rider_name,
        rider_surname,
        rider_full_name,
        rider_nickname,
        rider_country_iso,
        rider_country_name,
        rider_legacy_id,
        birth_city,
        birth_date,
        profile_picture_url,
        portrait_picture_url
    from source
    qualify row_number() over (
        partition by rider_id
        order by _ingested_at desc
    ) = 1
)

select * from renamed