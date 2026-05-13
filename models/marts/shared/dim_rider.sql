{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

select
    rider_id,
    rider_name,
    rider_surname,
    rider_full_name,
    rider_nickname,
    rider_country_iso,
    rider_country_name,
    birth_city,
    birth_date::date            as birth_date,
    rider_legacy_id,
    profile_picture_url,
    portrait_picture_url
from {{ ref('stg_rider') }}