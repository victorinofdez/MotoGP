{{ 
    config(
        materialized='view'
    ) 
}}

with results as (
    select * 
    from {{ ref('stg_result') }}
),

sessions as (
    select * 
    from {{ ref('stg_session') }}
),

joined as (
    select
        -- ids
        r.result_id,
        r.rider_id,
        r.team_id,
        r.constructor_id,
        r.rider_team_id,
        r.session_id,
        r.event_id,
        r.category_id,
        r.status_category,
        r.circuit_id,
        r.year,

        -- contexto de la sesion
        s.type                            as session_type,
        s.session_date,
        s.is_sprint,

        -- resultados
        r.position,
        r.points,
        r.status,
        r.total_laps,
        r.gap_first,
        r.gap_prev,
        r.gap_lap,
        r.average_speed,
        r.top_speed,
        r.best_lap_time,
        r.best_lap_number,
        r.time_text,
        r.file_url,

        -- condiciones meteorológicas
        s.weather_condition,
        s.track_condition,
        s.air_condition,
        s.humidity_condition,
        s.ground_condition

    from results r
    inner join sessions s 
        on r.session_id = s.session_id
)

select *
from joined
