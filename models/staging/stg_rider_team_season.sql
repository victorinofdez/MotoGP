with source as (
    select * from {{ source('raw', 'results') }}
),

renamed as (
    select 
        rider_team_id,
        rider_id,
        team_id,
        year::int as season_year,
        category_id,
        current_career_step_season,
        rider_in_grid,
        bike_picture_url,
        helmet_picture_url,
        number_picture_url
    from source
    where rider_team_id is not null
    qualify row_number() over (
        partition by rider_team_id
        order by _ingested_at desc
    ) = 1
)

select * from renamed