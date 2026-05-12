with source as (
    select * from {{ source('raw', 'standings') }}
),

renamed as (
    select 
        classification_id as standing_id,
        rider_full_name,
        rider_country_iso,
        constructor_name,
        year,
        points,
        position,
        _category_id as category_id
    from source
    where classification_id is not null
    qualify row_number() over (
        partition by classification_id
        order by _ingested_at desc
    ) = 1
)

select * from renamed