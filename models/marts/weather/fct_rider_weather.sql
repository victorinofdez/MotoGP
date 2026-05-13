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
      and r.track_condition is not null
      and r.year >= 2002
),

categorized as (
    select
        *,
        case
            when track_condition = 'Dry'                        then 'DRY'
            when track_condition = 'Wet'                        then 'WET'
            when track_condition in ('Wet-Dry', 'Dry-Wet')     then 'MIXED'
        end as condition_category
    from race_results
),

aggregated as (
    select
        rider_id,
        category_id,
        circuit_id,
        year,
        condition_category,

        -- volumen
        count(*)                                                        as total_entries,

        -- rendimiento
        sum(points)                                                     as total_points,
        round(avg(position), 2)                                         as avg_position,
        count(case when position = 1 then 1 end)                        as victories,
        count(case when position <= 3 then 1 end)                       as podiums,
        count(case when position <= 10 then 1 end)                      as points_finishes,

        -- fiabilidad
        count(case when status_category = 'DNF' then 1 end)             as dnf_count,
        round(
            count(case when status_category = 'DNF' then 1 end)::float
            / nullif(count(*), 0) * 100, 2
        )                                                               as dnf_rate_pct,

        -- velocidad
        round(avg(top_speed), 2)                                        as avg_top_speed

    from categorized
    group by rider_id, category_id, circuit_id, year, condition_category
)

select * from aggregated