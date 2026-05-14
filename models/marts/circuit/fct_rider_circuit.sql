{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

with race_results as (
    select r.*
    from {{ ref('int_race_results') }} r
    inner join {{ ref('session_type') }} st
        on r.session_type = st.type
    where st.is_race = true
      and r.rider_id is not null
      and r.circuit_id is not null
      and r.year >= 2002
),

aggregated as (
    select
        rider_id,
        circuit_id,
        category_id,
        {{ race_metrics() }},
        round(avg(top_speed), 2)                                        as avg_top_speed,
        max(top_speed)                                                  as max_top_speed

    from race_results
    group by rider_id, circuit_id, category_id
)

select * from aggregated