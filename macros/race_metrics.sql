{% macro race_metrics(include_avg_position=true) %}
    -- participaciones
    count(*)                                                        as total_entries,

    -- rendimiento
    sum(points)                                                     as total_points,
    count(case when position = 1 then 1 end)                        as victories,
    count(case when position <= 3 then 1 end)                       as podiums,
    count(case when position <= 10 then 1 end)                      as points_finishes,
    
    {% if include_avg_position %}
    round(avg(position), 2)                                         as avg_position,
    {% endif %}

    -- tasa de abandono
    count(case when status_category = 'DNF' then 1 end)             as dnf_count,
    round(
        count(case when status_category = 'DNF' then 1 end)::float
        / nullif(count(*), 0) * 100, 2
    )                                                               as dnf_rate_pct
{% endmacro %}