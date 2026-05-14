{{ 
    config(
        materialized="table", 
        schema="GOLD"
    ) 
}}

with race_results as (
    select r.*
    from {{ ref("int_race_results") }} r
    inner join {{ ref("session_type") }} st 
        on r.session_type = st.type
    where st.is_race = true
      and r.constructor_id is not null
),

aggregated as (
    select
        constructor_id,
        category_id,
        year,

        {{ race_metrics(include_avg_position=false) }},

        count(distinct rider_id)                                        as total_riders,
        round(avg(top_speed), 2)                                        as avg_top_speed,
        max(top_speed)                                                  as max_top_speed

    from race_results
    group by constructor_id, category_id, year
)

select * from aggregated