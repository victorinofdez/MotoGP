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

        -- participaciones
        count(*)                                                        as total_entries,
        count(distinct rider_id)                                        as total_riders,

        -- rendimiento
        sum(points)                                                     as total_points,
        count(case when position = 1 then 1 end)                        as victories,
        count(case when position <= 3 then 1 end)                       as podiums,
        count(case when position <= 10 then 1 end)                      as points_finishes,

        -- tasas de abandono
        count(case when status_category = 'DNF' then 1 end)             as dnf_count,
        round(
            count(case when status_category = 'DNF' then 1 end)::float
            / nullif(count(*), 0) * 100, 2
        )                                                               as dnf_rate_pct,

        -- velocidad
        round(avg(top_speed), 2)                                        as avg_top_speed,
        max(top_speed)                                                  as max_top_speed

    from race_results
    group by constructor_id, category_id, year
)

select * from aggregated