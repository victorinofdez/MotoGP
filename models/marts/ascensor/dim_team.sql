{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

select
    team_id,
    team_name,
    team_legacy_id
from {{ ref('stg_team') }}