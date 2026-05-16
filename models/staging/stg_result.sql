{{
    config(
        materialized        = 'incremental',
        unique_key          = 'result_id',
        incremental_strategy = 'delete+insert',
        on_schema_change    = 'fail'
    )
}}

with source as (
    select * from {{ source('raw', 'results') }}
    {% if is_incremental() %}
    where _ingested_at > (select max(dest._ingested_at) from {{ this }} dest)
    {% endif %}
),
renamed as (
    select
        result_id,
        position::int                                                               as position,
        rider_id,
        team_id,
        constructor_id,
        rider_team_id,
        session_id,
        event_id,
        category_id,
        circuit_id,
        year,
        average_speed::float    as average_speed,
        REPLACE(gap_first, ',', '')::float                                          as gap_first,
        REPLACE(gap_prev, ',', '')::float                                           as gap_prev,
        gap_lap::float                                                              as gap_lap,
        total_laps::int                                                             as total_laps,
        top_speed::float                                                            as top_speed,
        time_text,
        points::int                                                                 as points,
        status,
        -- Normalización del campo status desde Bronze.
        -- INSTND (In Standings) y OUTSTND (Out of Standings) son los estados
        -- de pilotos clasificados.
        -- NOTFINISHFIRST aplica a pilotos que terminaron sin ser primeros.
        -- Los verdaderos abandonos son OUTOFLAPS, NOTONRESTARTGRID y OUTOFTIME.
        case 
            when status in (
                'INSTND', 
                'OUTSTND', 
                'NOTFINISHFIRST', 
                'FINISHEDTHRUPITS'
            )                       then 'FIN'
            when status in (
                'OUTOFLAPS', 
                'NOTONRESTARTGRID', 
                'OUTOFTIME'
            )                       then 'DNF'
            when status in (
                'NOTSTARTED',
                'WILLNOTSTART'
            )                       then 'DNS'
            when status in (
                'DISQUALIFIED',
                'EXCLUDED')         then 'DSQ'
            when status is null     then 'FIN'
            else 'FIN'
        end                                                                         as status_category,
        
        best_lap_number::int                                                        as best_lap_number,
        best_lap_time,
        file                                                                        as file_url,
        _ingested_at
    from source
    where result_id is not null
    qualify row_number() over (
        partition by result_id
        order by _ingested_at desc
    ) = 1
)
select * from renamed