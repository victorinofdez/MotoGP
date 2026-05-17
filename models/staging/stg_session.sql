{{
    config(
        materialized        = 'incremental',
        unique_key          = 'session_id',
        incremental_strategy = 'delete+insert',
        on_schema_change    = 'fail'
    )
}}

with source as (
    select * from {{ source('raw', 'sessions') }}
    {% if is_incremental() %}
        where _ingested_at > (select max(dest._ingested_at) from {{ this }} dest)
    {% endif %}
),
renamed as (
    select
        id                                                                              as session_id,
        event_id,
        category_id,
        circuit_id,
        year,
        type,
        status,
        is_sprint,
        date::timestamp_ntz                                                             as session_date,
        NULLIF(TRIM(track_condition), '')                                               as track_condition,
        NULLIF(REGEXP_REPLACE(TRIM(air_condition), '[^0-9.]', ''), '')::float           as air_condition,
        NULLIF(REGEXP_REPLACE(TRIM(humidity_condition), '[^0-9.]', ''), '')::float      as humidity_condition,
        NULLIF(REGEXP_REPLACE(TRIM(ground_condition), '[^0-9.]', ''), '')::float        as ground_condition,
        NULLIF(TRIM(weather_condition), '')                                             as weather_condition,
        _ingested_at
    from source
    where id is not null
    qualify row_number() over (
        partition by id
        order by _ingested_at desc
    ) = 1
)
select * from renamed