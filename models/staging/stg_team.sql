with source as (
    select * from {{ source('raw', 'results') }}
),

renamed as (
    select 
        team_id,
        team_name,
        team_legacy_id
    from source
    where team_id is not null
    qualify row_number() over (
        partition by team_id
        order by _ingested_at desc
    ) = 1
)

select * from renamed