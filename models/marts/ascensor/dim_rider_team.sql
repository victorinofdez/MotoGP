{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

select
    rider_team_id,
    rider_id,
    team_id,
    category_id,
    season_year,
    current_career_step_season,
    rider_in_grid,
    bike_picture_url,
    helmet_picture_url,  
    dbt_scd_id,
    dbt_valid_from,
    dbt_valid_to,
    case when dbt_valid_to is null then true else false end  as is_current
from {{ ref('snap_rider_team') }}