with
    source as (select * from {{ source("raw", "seasons") }}),

    renamed as (
        select
            id                                      as season_id,
            year::int                               as year,
            year::int = year(current_date())        as is_current
        from source
                -- puesto que un mismo id aparece en múltiples años (multi-dataset ingestion),
                -- se particiona por id y nos quedamos con el registro más reciente
        qualify row_number() over (
                partition by id
                order by _ingested_at desc
        ) = 1
    )

select *
from renamed
